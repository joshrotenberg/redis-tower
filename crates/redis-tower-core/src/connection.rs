use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Sink;
use futures::SinkExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use redis_tower_protocol::helpers::{array, bulk};
use redis_tower_protocol::{Frame, RespCodec};

use crate::command::Command;
use crate::error::RedisError;
use crate::stream::RedisStream;
use crate::url::{RedisUrl, parse_redis_url};

/// Read the next non-push response frame, routing push frames to the channel.
async fn read_response_from(
    framed: &mut Framed<RedisStream, RespCodec>,
    push_tx: &Option<tokio::sync::mpsc::UnboundedSender<Frame>>,
) -> Result<Frame, RedisError> {
    loop {
        let frame = framed
            .next()
            .await
            .ok_or(RedisError::ConnectionClosed)?
            .map_err(RedisError::from)?;

        if let Frame::Push(_) = &frame {
            if let Some(ref tx) = *push_tx {
                let _ = tx.send(frame);
            }
            continue;
        }

        return Ok(frame);
    }
}

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
    /// The framed transport. `None` while a `Service::call` future is in flight.
    framed: Option<Framed<RedisStream, RespCodec>>,
    /// Optional sender for RESP3 push messages. Set via `subscribe_pushes()`.
    push_tx: Option<tokio::sync::mpsc::UnboundedSender<Frame>>,
    /// Channel to reclaim the framed transport after a `Service::call` completes.
    inflight: Option<oneshot::Receiver<Framed<RedisStream, RespCodec>>>,
}

impl RedisConnection {
    /// Create a connection from a framed stream.
    fn from_framed_inner(framed: Framed<RedisStream, RespCodec>) -> Self {
        Self {
            framed: Some(framed),
            push_tx: None,
            inflight: None,
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

        let mut conn = if parsed.unix {
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
        let mut conn = Self::connect(addr).await?;
        conn.hello(3).await?;
        Ok(conn)
    }

    /// Send HELLO to negotiate protocol version.
    ///
    /// `HELLO 3` switches to RESP3, `HELLO 2` switches back to RESP2.
    pub async fn hello(&mut self, version: u8) -> Result<Frame, RedisError> {
        let frame = array(vec![bulk("HELLO"), bulk(version.to_string())]);
        let framed = self.framed.as_mut().expect("connection not in flight");
        framed.send(frame).await.map_err(RedisError::from)?;
        let response = read_response_from(framed, &self.push_tx).await?;
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
    pub fn subscribe_pushes(&mut self) -> tokio::sync::mpsc::UnboundedReceiver<Frame> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.push_tx = Some(tx);
        rx
    }

    /// Ensure the framed transport is available, reclaiming it from an
    /// in-flight `Service::call` future if necessary.
    async fn ensure_framed(&mut self) -> Result<(), RedisError> {
        if self.framed.is_none() {
            if let Some(rx) = self.inflight.take() {
                let framed = rx.await.map_err(|_| RedisError::ConnectionClosed)?;
                self.framed = Some(framed);
            } else {
                return Err(RedisError::ConnectionClosed);
            }
        }
        Ok(())
    }

    /// Send a command and receive the response.
    ///
    /// This is the low-level method. Prefer using the `Service` trait via
    /// `tower::ServiceExt::oneshot` or `Service::call`.
    pub async fn execute<Cmd: Command>(&mut self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        self.ensure_framed().await?;
        let frame = cmd.to_frame();
        let framed = self.framed.as_mut().unwrap();
        framed.send(frame).await.map_err(RedisError::from)?;
        let response = read_response_from(framed, &self.push_tx).await?;

        if let Frame::Error(ref e) = response {
            return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
        }

        cmd.parse_response(response)
    }

    /// Send multiple command frames and read all responses in a single roundtrip.
    ///
    /// Used by pipeline and transaction implementations.
    pub async fn execute_pipeline(&mut self, frames: Vec<Frame>) -> Result<Vec<Frame>, RedisError> {
        self.ensure_framed().await?;
        let count = frames.len();
        let framed = self.framed.as_mut().unwrap();

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
            let response = read_response_from(framed, &self.push_tx).await?;
            responses.push(response);
        }

        Ok(responses)
    }

    /// Execute a WATCH/MULTI/EXEC transaction sequence.
    ///
    /// Returns `Ok(Some(responses))` on commit, `Ok(None)` if aborted by WATCH.
    pub async fn execute_transaction(
        &mut self,
        watch_frames: Vec<Frame>,
        command_frames: Vec<Frame>,
    ) -> Result<Option<Vec<Frame>>, RedisError> {
        self.ensure_framed().await?;
        let framed = self.framed.as_mut().unwrap();

        // Send WATCH keys if any.
        for frame in watch_frames {
            framed.send(frame).await.map_err(RedisError::from)?;
            let response = read_response_from(framed, &self.push_tx).await?;
            if let Frame::Error(e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned()));
            }
        }

        // Send MULTI.
        framed
            .send(array(vec![bulk("MULTI")]))
            .await
            .map_err(RedisError::from)?;
        let multi_resp = read_response_from(framed, &self.push_tx).await?;
        if let Frame::Error(e) = multi_resp {
            return Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned()));
        }

        // Send each command, expect QUEUED for each.
        for frame in &command_frames {
            framed.send(frame.clone()).await.map_err(RedisError::from)?;
            let queued_resp = read_response_from(framed, &self.push_tx).await?;
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
        let exec_resp = read_response_from(framed, &self.push_tx).await?;

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
    /// Fails if a `Service::call` future is still in flight.
    pub fn into_framed(mut self) -> Result<Framed<RedisStream, RespCodec>, RedisError> {
        self.framed.take().ok_or(RedisError::ConnectionInUse)
    }

    /// Run post-connection setup (AUTH, SELECT) based on URL parameters.
    async fn post_connect_setup(&mut self, url: &RedisUrl) -> Result<(), RedisError> {
        let framed = self.framed.as_mut().expect("connection not in flight");

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
            let response = read_response_from(framed, &self.push_tx).await?;

            if let Frame::Error(e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned()));
            }
        }

        if let Some(db) = url.database {
            framed
                .send(array(vec![bulk("SELECT"), bulk(db.to_string())]))
                .await
                .map_err(RedisError::from)?;
            let response = read_response_from(framed, &self.push_tx).await?;

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

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // If the framed transport was taken by a previous call(), try to reclaim it.
        if self.framed.is_none() {
            if let Some(ref mut rx) = self.inflight {
                match Pin::new(rx).poll(cx) {
                    Poll::Ready(Ok(framed)) => {
                        self.framed = Some(framed);
                        self.inflight = None;
                    }
                    Poll::Ready(Err(_)) => {
                        self.inflight = None;
                        return Poll::Ready(Err(RedisError::ConnectionClosed));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            } else {
                return Poll::Ready(Err(RedisError::ConnectionClosed));
            }
        }

        // Check that the underlying sink can accept a write.
        let framed = self.framed.as_mut().unwrap();
        Pin::new(framed).poll_ready(cx).map_err(RedisError::from)
    }

    fn call(&mut self, cmd: Cmd) -> Self::Future {
        // Take the framed transport out of self so the future can own it.
        let mut framed = self
            .framed
            .take()
            .expect("call() invoked without successful poll_ready()");
        let push_tx = self.push_tx.clone();

        // Enqueue the frame synchronously (valid after poll_ready returned Ready).
        let frame = cmd.to_frame();
        if let Err(e) = Pin::new(&mut framed).start_send(frame) {
            // Put framed back since we failed before spawning the future.
            self.framed = Some(framed);
            return Box::pin(async move { Err(RedisError::from(e)) });
        }

        // Create the return channel so poll_ready can reclaim the transport.
        let (return_tx, return_rx) = oneshot::channel();
        self.inflight = Some(return_rx);

        Box::pin(async move {
            // Flush the buffered write.
            framed.flush().await.map_err(RedisError::from)?;

            // Read response, routing push frames.
            let response = read_response_from(&mut framed, &push_tx).await?;

            // Return the transport for reuse.
            let _ = return_tx.send(framed);

            if let Frame::Error(ref e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
            }

            cmd.parse_response(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn into_framed_returns_error_when_none() {
        let conn = RedisConnection {
            framed: None,
            push_tx: None,
            inflight: None,
        };
        match conn.into_framed() {
            Err(RedisError::ConnectionInUse) => {}
            Err(other) => panic!("expected ConnectionInUse, got: {other}"),
            Ok(_) => panic!("expected Err(ConnectionInUse), got Ok"),
        }
    }

    #[tokio::test]
    async fn ensure_framed_returns_error_when_no_framed_and_no_inflight() {
        let mut conn = RedisConnection {
            framed: None,
            push_tx: None,
            inflight: None,
        };
        match conn.ensure_framed().await {
            Err(RedisError::ConnectionClosed) => {}
            other => panic!("expected ConnectionClosed, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn ensure_framed_returns_error_when_inflight_sender_dropped() {
        let (tx, rx) = oneshot::channel::<Framed<RedisStream, RespCodec>>();
        drop(tx);

        let mut conn = RedisConnection {
            framed: None,
            push_tx: None,
            inflight: Some(rx),
        };
        match conn.ensure_framed().await {
            Err(RedisError::ConnectionClosed) => {}
            other => panic!("expected ConnectionClosed, got: {other:?}"),
        }
    }

    #[test]
    fn subscribe_pushes_returns_receiver() {
        let mut conn = RedisConnection {
            framed: None,
            push_tx: None,
            inflight: None,
        };
        let _rx = conn.subscribe_pushes();
        assert!(conn.push_tx.is_some());
    }
}
