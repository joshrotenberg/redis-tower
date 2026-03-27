use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::SinkExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use redis_tower_protocol::helpers::{array, bulk};
use redis_tower_protocol::{Frame, RespCodec};

use crate::command::Command;
use crate::error::RedisError;
use crate::stream::RedisStream;
use crate::url::{RedisUrl, parse_redis_url};

/// A single Redis connection implementing `tower::Service<Cmd>`.
///
/// This is the foundational building block. It owns a framed TCP/TLS/Unix
/// connection and serializes commands one at a time.
///
/// `RedisConnection` requires `&mut self` for `Service::call`, which is the
/// correct Tower contract for a non-multiplexed connection. For shared access
/// across tasks, wrap with `tower::buffer::Buffer`.
///
/// # Example
///
/// ```ignore
/// use redis_tower_core::RedisConnection;
/// use redis_tower_commands::Get;
/// use tower::Service;
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
/// let response = conn.call(Get::new("my_key")).await?;
/// ```
pub struct RedisConnection {
    framed: Arc<Mutex<Framed<RedisStream, RespCodec>>>,
    /// Optional sender for RESP3 push messages. Set via `subscribe_pushes()`.
    push_tx: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedSender<Frame>>>>,
}

impl RedisConnection {
    /// Create a connection from a framed stream.
    fn from_framed_inner(framed: Framed<RedisStream, RespCodec>) -> Self {
        Self {
            framed: Arc::new(Mutex::new(framed)),
            push_tx: Arc::new(Mutex::new(None)),
        }
    }

    /// Connect to a Redis server over TCP.
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self::from_framed_inner(Framed::new(
            RedisStream::Tcp(stream),
            RespCodec,
        )))
    }

    /// Connect over TLS using the provided configuration.
    ///
    /// Requires either the `tls-native-tls` or `tls-rustls` feature.
    #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
    pub async fn connect_tls(
        addr: &str,
        hostname: &str,
        tls_config: &crate::tls::TlsConfig,
    ) -> Result<Self, RedisError> {
        let tcp = TcpStream::connect(addr).await?;
        tcp.set_nodelay(true)?;
        let stream = tls_config.connect(tcp, hostname).await?;
        Ok(Self::from_framed_inner(Framed::new(stream, RespCodec)))
    }

    /// Connect using a Redis URL.
    ///
    /// Supports `redis://`, `rediss://` (TLS), and `unix://` schemes.
    ///
    /// For `rediss://` URLs, a TLS backend feature must be enabled.
    /// The `tls-rustls` backend is preferred if both are enabled.
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        let parsed = parse_redis_url(url)?;

        let conn = if parsed.unix {
            #[cfg(unix)]
            {
                let path = parsed
                    .path
                    .as_deref()
                    .ok_or_else(|| RedisError::InvalidUrl("unix URL missing path".into()))?;
                let stream = tokio::net::UnixStream::connect(path).await?;
                Self::from_framed_inner(Framed::new(RedisStream::Unix(stream), RespCodec))
            }
            #[cfg(not(unix))]
            {
                return Err(RedisError::InvalidUrl(
                    "unix sockets not supported on this platform".into(),
                ));
            }
        } else if parsed.tls {
            #[cfg(feature = "tls-rustls")]
            {
                let tls_config = crate::tls::TlsConfig::default_rustls();
                let addr = format!("{}:{}", parsed.host, parsed.port);
                Self::connect_tls(&addr, &parsed.host, &tls_config).await?
            }
            #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
            {
                let tls_config = crate::tls::TlsConfig::default_native_tls();
                let addr = format!("{}:{}", parsed.host, parsed.port);
                Self::connect_tls(&addr, &parsed.host, &tls_config).await?
            }
            #[cfg(not(any(feature = "tls-native-tls", feature = "tls-rustls")))]
            {
                return Err(RedisError::InvalidUrl(
                    "TLS requires the tls-native-tls or tls-rustls feature".into(),
                ));
            }
        } else {
            Self::connect(&format!("{}:{}", parsed.host, parsed.port)).await?
        };

        conn.post_connect_setup(&parsed).await?;
        Ok(conn)
    }

    /// Connect to a Redis server and negotiate RESP3 protocol.
    ///
    /// Sends `HELLO 3` after connecting. The server will respond with
    /// RESP3 frames for all subsequent commands.
    pub async fn connect_resp3(addr: &str) -> Result<Self, RedisError> {
        let conn = Self::connect(addr).await?;
        conn.hello(3).await?;
        Ok(conn)
    }

    /// Send HELLO to negotiate protocol version.
    ///
    /// `HELLO 3` switches to RESP3, `HELLO 2` switches back to RESP2.
    pub async fn hello(&self, version: u8) -> Result<Frame, RedisError> {
        let frame = array(vec![bulk("HELLO"), bulk(version.to_string())]);
        let mut framed = self.framed.lock().await;
        framed.send(frame).await.map_err(RedisError::from)?;
        let response = self.read_response(&mut framed).await?;
        if let Frame::Error(ref e) = response {
            return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
        }
        Ok(response)
    }

    /// Wrap an existing stream in a `RedisConnection`.
    pub fn from_stream(stream: RedisStream) -> Self {
        Self::from_framed_inner(Framed::new(stream, RespCodec))
    }

    /// Subscribe to RESP3 push messages.
    ///
    /// Returns a receiver for out-of-band push frames (e.g., invalidation
    /// messages from CLIENT TRACKING). Push frames received during normal
    /// command execution are automatically routed to this channel.
    ///
    /// If nobody subscribes, push frames are silently discarded.
    pub async fn subscribe_pushes(&self) -> tokio::sync::mpsc::UnboundedReceiver<Frame> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut guard = self.push_tx.lock().await;
        *guard = Some(tx);
        rx
    }

    /// Read the next non-push response frame from the stream.
    ///
    /// If a `Frame::Push` is received, it's routed to the push channel
    /// (if subscribed) and the next frame is read. This ensures push
    /// messages don't interfere with command responses.
    async fn read_response(
        &self,
        framed: &mut Framed<RedisStream, RespCodec>,
    ) -> Result<Frame, RedisError> {
        loop {
            let frame = framed
                .next()
                .await
                .ok_or(RedisError::ConnectionClosed)?
                .map_err(RedisError::from)?;

            if let Frame::Push(_) = &frame {
                // Route to push channel if subscribed.
                let guard = self.push_tx.lock().await;
                if let Some(ref tx) = *guard {
                    let _ = tx.send(frame); // Best-effort, drop if receiver is gone.
                }
                continue; // Read the actual command response.
            }

            return Ok(frame);
        }
    }

    /// Send a command and receive the response.
    ///
    /// This is the low-level method. Prefer using the `Service` trait via
    /// `tower::ServiceExt::oneshot` or `Service::call`.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let frame = cmd.to_frame();
        let mut framed = self.framed.lock().await;
        framed.send(frame).await.map_err(RedisError::from)?;
        let response = self.read_response(&mut framed).await?;

        if let Frame::Error(ref e) = response {
            return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
        }

        cmd.parse_response(response)
    }

    /// Send multiple command frames and read all responses in a single roundtrip.
    ///
    /// Used by pipeline and transaction implementations. Holds the connection
    /// lock for the entire batch.
    pub async fn execute_pipeline(&self, frames: Vec<Frame>) -> Result<Vec<Frame>, RedisError> {
        let count = frames.len();
        let mut framed = self.framed.lock().await;

        // Send all frames, buffering writes.
        for (i, frame) in frames.into_iter().enumerate() {
            if i < count - 1 {
                framed.feed(frame).await.map_err(RedisError::from)?;
            } else {
                framed.send(frame).await.map_err(RedisError::from)?;
            }
        }

        // Read all responses, routing push frames to the channel.
        let mut responses = Vec::with_capacity(count);
        for _ in 0..count {
            let response = self.read_response(&mut framed).await?;
            responses.push(response);
        }

        Ok(responses)
    }

    /// Execute a WATCH/MULTI/EXEC transaction sequence.
    ///
    /// Returns `Ok(Some(responses))` on commit, `Ok(None)` if aborted by WATCH.
    pub async fn execute_transaction(
        &self,
        watch_frames: Vec<Frame>,
        command_frames: Vec<Frame>,
    ) -> Result<Option<Vec<Frame>>, RedisError> {
        let mut framed = self.framed.lock().await;

        // Send WATCH keys if any.
        for frame in watch_frames {
            framed.send(frame).await.map_err(RedisError::from)?;
            let response = self.read_response(&mut framed).await?;
            if let Frame::Error(e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned()));
            }
        }

        // Send MULTI.
        framed
            .send(array(vec![bulk("MULTI")]))
            .await
            .map_err(RedisError::from)?;
        let multi_resp = self.read_response(&mut framed).await?;
        if let Frame::Error(e) = multi_resp {
            return Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned()));
        }

        // Send each command, expect QUEUED for each.
        for frame in &command_frames {
            framed.send(frame.clone()).await.map_err(RedisError::from)?;
            let queued_resp = self.read_response(&mut framed).await?;
            match queued_resp {
                Frame::SimpleString(ref s) if &s[..] == b"QUEUED" => {}
                Frame::Error(e) => {
                    // Abort the transaction on error.
                    let _ = framed.send(array(vec![bulk("DISCARD")])).await;
                    let _ = framed.next().await;
                    return Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned()));
                }
                _ => {
                    let _ = framed.send(array(vec![bulk("DISCARD")])).await;
                    let _ = framed.next().await;
                    return Err(RedisError::UnexpectedResponse {
                        expected: "QUEUED",
                        actual: format!("{queued_resp:?}"),
                    });
                }
            }
        }

        // Send EXEC.
        framed
            .send(array(vec![bulk("EXEC")]))
            .await
            .map_err(RedisError::from)?;
        let exec_resp = self.read_response(&mut framed).await?;

        match exec_resp {
            Frame::Array(Some(results)) => Ok(Some(results)),
            Frame::Array(None) | Frame::Null => Ok(None), // WATCH violation
            Frame::Error(e) => Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned())),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    /// Consume this connection and extract the underlying framed stream.
    ///
    /// Fails if the connection's `Arc` has been cloned (e.g., by a
    /// `Service::call` future still in flight). Use a fresh connection
    /// for operations that require exclusive ownership (like pub/sub).
    pub fn into_framed(self) -> Result<Framed<RedisStream, RespCodec>, RedisError> {
        let mutex = Arc::try_unwrap(self.framed).map_err(|_| RedisError::ConnectionInUse)?;
        Ok(mutex.into_inner())
    }

    /// Get a clone of the internal framed Arc.
    ///
    /// Used by FrameService and cluster/sentinel Service impls to share
    /// the underlying connection for async futures.
    pub fn framed_arc(&self) -> Arc<Mutex<Framed<RedisStream, RespCodec>>> {
        Arc::clone(&self.framed)
    }

    /// Get a clone of the push sender Arc.
    pub fn push_tx_arc(&self) -> Arc<Mutex<Option<tokio::sync::mpsc::UnboundedSender<Frame>>>> {
        Arc::clone(&self.push_tx)
    }

    /// Run post-connection setup (AUTH, SELECT) based on URL parameters.
    async fn post_connect_setup(&self, url: &RedisUrl) -> Result<(), RedisError> {
        let mut framed = self.framed.lock().await;

        if let Some(ref password) = url.password {
            let mut auth_args = vec![bulk("AUTH")];
            if let Some(ref username) = url.username {
                auth_args.push(bulk(username.clone()));
            }
            auth_args.push(bulk(password.clone()));

            framed
                .send(array(auth_args))
                .await
                .map_err(RedisError::from)?;
            let response = self.read_response(&mut framed).await?;

            if let Frame::Error(e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned()));
            }
        }

        if let Some(db) = url.database {
            framed
                .send(array(vec![bulk("SELECT"), bulk(db.to_string())]))
                .await
                .map_err(RedisError::from)?;
            let response = self.read_response(&mut framed).await?;

            if let Frame::Error(e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned()));
            }
        }

        Ok(())
    }
}

impl<Cmd: Command> tower_service::Service<Cmd> for RedisConnection {
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Cmd::Response, RedisError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // The Arc<Mutex<>> design means poll_ready always returns Ready.
        // Actual serialization happens inside call() under the lock.
        // For proper backpressure, wrap with tower::buffer::Buffer.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, cmd: Cmd) -> Self::Future {
        let framed = Arc::clone(&self.framed);
        let push_tx = Arc::clone(&self.push_tx);
        Box::pin(async move {
            let frame = cmd.to_frame();
            let mut guard = framed.lock().await;
            guard.send(frame).await.map_err(RedisError::from)?;

            // Read response, routing push frames.
            let response = loop {
                let f = guard
                    .next()
                    .await
                    .ok_or(RedisError::ConnectionClosed)?
                    .map_err(RedisError::from)?;
                if let Frame::Push(_) = &f {
                    let ptx = push_tx.lock().await;
                    if let Some(ref tx) = *ptx {
                        let _ = tx.send(f);
                    }
                    continue;
                }
                break f;
            };

            if let Frame::Error(ref e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
            }

            cmd.parse_response(response)
        })
    }
}
