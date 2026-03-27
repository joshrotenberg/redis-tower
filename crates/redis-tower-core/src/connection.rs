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
}

impl RedisConnection {
    /// Connect to a Redis server over TCP.
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            framed: Arc::new(Mutex::new(Framed::new(RedisStream::Tcp(stream), RespCodec))),
        })
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
        Ok(Self {
            framed: Arc::new(Mutex::new(Framed::new(stream, RespCodec))),
        })
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
                Self {
                    framed: Arc::new(Mutex::new(Framed::new(
                        RedisStream::Unix(stream),
                        RespCodec,
                    ))),
                }
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
        let response = framed
            .next()
            .await
            .ok_or(RedisError::ConnectionClosed)?
            .map_err(RedisError::from)?;
        if let Frame::Error(ref e) = response {
            return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
        }
        Ok(response)
    }

    /// Wrap an existing stream in a `RedisConnection`.
    pub fn from_stream(stream: RedisStream) -> Self {
        Self {
            framed: Arc::new(Mutex::new(Framed::new(stream, RespCodec))),
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
        let response = framed
            .next()
            .await
            .ok_or(RedisError::ConnectionClosed)?
            .map_err(RedisError::from)?;

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

        // Read all responses.
        let mut responses = Vec::with_capacity(count);
        for _ in 0..count {
            let response = framed
                .next()
                .await
                .ok_or(RedisError::ConnectionClosed)?
                .map_err(RedisError::from)?;
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
            let response = framed
                .next()
                .await
                .ok_or(RedisError::ConnectionClosed)?
                .map_err(RedisError::from)?;
            if let Frame::Error(e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned()));
            }
        }

        // Send MULTI.
        framed
            .send(array(vec![bulk("MULTI")]))
            .await
            .map_err(RedisError::from)?;
        let multi_resp = framed
            .next()
            .await
            .ok_or(RedisError::ConnectionClosed)?
            .map_err(RedisError::from)?;
        if let Frame::Error(e) = multi_resp {
            return Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned()));
        }

        // Send each command, expect QUEUED for each.
        for frame in &command_frames {
            framed.send(frame.clone()).await.map_err(RedisError::from)?;
            let queued_resp = framed
                .next()
                .await
                .ok_or(RedisError::ConnectionClosed)?
                .map_err(RedisError::from)?;
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
        let exec_resp = framed
            .next()
            .await
            .ok_or(RedisError::ConnectionClosed)?
            .map_err(RedisError::from)?;

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
            let response = framed
                .next()
                .await
                .ok_or(RedisError::ConnectionClosed)?
                .map_err(RedisError::from)?;

            if let Frame::Error(e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned()));
            }
        }

        if let Some(db) = url.database {
            framed
                .send(array(vec![bulk("SELECT"), bulk(db.to_string())]))
                .await
                .map_err(RedisError::from)?;
            let response = framed
                .next()
                .await
                .ok_or(RedisError::ConnectionClosed)?
                .map_err(RedisError::from)?;

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
        Box::pin(async move {
            let frame = cmd.to_frame();
            let mut guard = framed.lock().await;
            guard.send(frame).await.map_err(RedisError::from)?;
            let response = guard
                .next()
                .await
                .ok_or(RedisError::ConnectionClosed)?
                .map_err(RedisError::from)?;

            if let Frame::Error(ref e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
            }

            cmd.parse_response(response)
        })
    }
}
