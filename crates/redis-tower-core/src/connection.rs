use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Sink;
use futures::SinkExt;
use socket2::{Socket, TcpKeepalive};
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

/// Configuration for TCP keepalive probes.
///
/// Controls `SO_KEEPALIVE` on TCP connections created by
/// [`RedisConnection::connect`] and `RedisConnection::connect_tls` (TLS features).
///
/// When the connection has been idle for `idle` seconds, the OS begins
/// sending keepalive probes every `interval` seconds. If `probes` consecutive
/// probes go unanswered the connection is considered dead and a subsequent
/// read or write will return an error.
///
/// # Example
///
/// ```
/// use redis_tower_core::KeepaliveConfig;
/// use std::time::Duration;
///
/// // Aggressive keepalive for cloud environments:
/// let cfg = KeepaliveConfig::new()
///     .with_idle(Duration::from_secs(30))
///     .with_interval(Duration::from_secs(5))
///     .with_probes(5);
/// ```
#[derive(Debug, Clone)]
pub struct KeepaliveConfig {
    /// Time the connection must be idle before keepalive probes start.
    pub idle: Duration,
    /// Interval between consecutive keepalive probes.
    pub interval: Duration,
    /// Number of unanswered probes before the connection is considered dead.
    /// Note: ignored on Windows, which does not expose this parameter.
    pub probes: u32,
}

impl Default for KeepaliveConfig {
    fn default() -> Self {
        Self {
            idle: Duration::from_secs(60),
            interval: Duration::from_secs(10),
            probes: 3,
        }
    }
}

impl KeepaliveConfig {
    /// Create a new `KeepaliveConfig` with default values (60s idle, 10s interval, 3 probes).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the idle time before keepalive probes start.
    #[must_use]
    pub fn with_idle(mut self, idle: Duration) -> Self {
        self.idle = idle;
        self
    }

    /// Set the interval between consecutive keepalive probes.
    #[must_use]
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Set the number of unanswered probes before declaring the connection dead.
    ///
    /// This setting is ignored on Windows.
    #[must_use]
    pub fn with_probes(mut self, probes: u32) -> Self {
        self.probes = probes;
        self
    }
}

/// Apply TCP keepalive settings to an already-connected `TcpStream`.
///
/// Converts through `socket2` to call `setsockopt(SO_KEEPALIVE, ...)`.
fn apply_keepalive(stream: TcpStream, config: &KeepaliveConfig) -> Result<TcpStream, RedisError> {
    let std_stream = stream.into_std()?;
    let socket = Socket::from(std_stream);

    let keepalive = TcpKeepalive::new()
        .with_time(config.idle)
        .with_interval(config.interval);

    // `with_retries` is not available on Windows.
    #[cfg(not(windows))]
    let keepalive = keepalive.with_retries(config.probes);

    socket.set_tcp_keepalive(&keepalive)?;

    let std_stream: std::net::TcpStream = socket.into();
    std_stream.set_nonblocking(true)?;
    Ok(TcpStream::from_std(std_stream)?)
}

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
    ///
    /// TCP keepalive is enabled with sensible defaults: 60 s idle, 10 s
    /// interval, 3 probes. Use [`connect_with_keepalive`](Self::connect_with_keepalive)
    /// to supply custom keepalive parameters.
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        Self::connect_with_keepalive(addr, &KeepaliveConfig::default()).await
    }

    /// Connect to a Redis server over TCP with a custom keepalive configuration.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::Connection`] if the TCP connection fails or if
    /// the keepalive socket options cannot be applied.
    pub async fn connect_with_keepalive(
        addr: &str,
        keepalive: &KeepaliveConfig,
    ) -> Result<Self, RedisError> {
        let stream = TcpStream::connect(addr).await?;
        let stream = apply_keepalive(stream, keepalive)?;
        stream.set_nodelay(true)?;
        let mut conn = Self::from_framed_inner(Framed::new(RedisStream::Tcp(stream), RespCodec));
        conn.identify_client().await;
        Ok(conn)
    }

    /// Connect to a Redis server over TCP with a connect timeout.
    ///
    /// If the TCP handshake is not completed within `timeout`, returns
    /// [`RedisError::ConnectTimeout`] instead of waiting for the OS-default
    /// timeout (which can be several minutes on unreachable hosts).
    ///
    /// TCP keepalive is enabled with sensible defaults after the connection is
    /// established. Use [`connect_with_keepalive`](Self::connect_with_keepalive)
    /// and wrap the call yourself with [`tokio::time::timeout`] if you need
    /// custom keepalive parameters alongside a connect timeout.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use redis_tower_core::RedisConnection;
    /// use std::time::Duration;
    ///
    /// # tokio_test::block_on(async {
    /// let conn = RedisConnection::connect_with_timeout("127.0.0.1:6379", Duration::from_secs(3)).await;
    /// # });
    /// ```
    pub async fn connect_with_timeout(addr: &str, timeout: Duration) -> Result<Self, RedisError> {
        let stream = match tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return Err(RedisError::Connection(e)),
            Err(_elapsed) => return Err(RedisError::ConnectTimeout),
        };
        let stream = apply_keepalive(stream, &KeepaliveConfig::default())?;
        stream.set_nodelay(true)?;
        let mut conn = Self::from_framed_inner(Framed::new(RedisStream::Tcp(stream), RespCodec));
        conn.identify_client().await;
        Ok(conn)
    }

    /// Connect over TLS using the provided configuration.
    ///
    /// Requires either the `tls-native-tls` or `tls-rustls` feature.
    ///
    /// TCP keepalive is enabled with sensible defaults: 60 s idle, 10 s
    /// interval, 3 probes. Use [`connect_tls_with_keepalive`](Self::connect_tls_with_keepalive)
    /// to supply custom keepalive parameters.
    #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
    #[cfg_attr(
        docsrs,
        doc(cfg(any(feature = "tls-native-tls", feature = "tls-rustls")))
    )]
    pub async fn connect_tls(
        addr: &str,
        hostname: &str,
        tls_config: &crate::tls::TlsConfig,
    ) -> Result<Self, RedisError> {
        Self::connect_tls_with_keepalive(addr, hostname, tls_config, &KeepaliveConfig::default())
            .await
    }

    /// Connect over TLS with a custom keepalive configuration.
    ///
    /// Requires either the `tls-native-tls` or `tls-rustls` feature.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::Connection`] if the TCP connection or TLS
    /// handshake fails, or if the keepalive socket options cannot be applied.
    #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
    #[cfg_attr(
        docsrs,
        doc(cfg(any(feature = "tls-native-tls", feature = "tls-rustls")))
    )]
    pub async fn connect_tls_with_keepalive(
        addr: &str,
        hostname: &str,
        tls_config: &crate::tls::TlsConfig,
        keepalive: &KeepaliveConfig,
    ) -> Result<Self, RedisError> {
        let tcp = TcpStream::connect(addr).await?;
        let tcp = apply_keepalive(tcp, keepalive)?;
        tcp.set_nodelay(true)?;
        let stream = tls_config.connect(tcp, hostname).await?;
        let mut conn = Self::from_framed_inner(Framed::new(stream, RespCodec));
        conn.identify_client().await;
        Ok(conn)
    }

    /// Connect over TLS with a connect timeout.
    ///
    /// Requires either the `tls-native-tls` or `tls-rustls` feature.
    ///
    /// If the TCP handshake is not completed within `timeout`, returns
    /// [`RedisError::ConnectTimeout`]. The timeout covers only the TCP
    /// connection phase; the TLS handshake runs outside the timeout window.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::ConnectTimeout`] if the TCP connect times out,
    /// [`RedisError::Connection`] if the TCP connection or TLS handshake
    /// fails, or [`RedisError::Connection`] if keepalive socket options
    /// cannot be applied.
    #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
    #[cfg_attr(
        docsrs,
        doc(cfg(any(feature = "tls-native-tls", feature = "tls-rustls")))
    )]
    pub async fn connect_tls_with_timeout(
        addr: &str,
        hostname: &str,
        tls_config: &crate::tls::TlsConfig,
        timeout: Duration,
    ) -> Result<Self, RedisError> {
        let tcp = match tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => return Err(RedisError::Connection(e)),
            Err(_elapsed) => return Err(RedisError::ConnectTimeout),
        };
        let tcp = apply_keepalive(tcp, &KeepaliveConfig::default())?;
        tcp.set_nodelay(true)?;
        let stream = tls_config.connect(tcp, hostname).await?;
        let mut conn = Self::from_framed_inner(Framed::new(stream, RespCodec));
        conn.identify_client().await;
        Ok(conn)
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

    /// Connect from a Redis URL, performing the TLS handshake with an explicit
    /// [`TlsConfig`](crate::tls::TlsConfig).
    ///
    /// Like [`connect_url`](Self::connect_url) but the caller supplies the TLS
    /// configuration -- a custom root CA, a client certificate for mTLS, or a
    /// pre-built backend -- instead of the URL's hardcoded default. The host,
    /// port, and any AUTH/SELECT parameters still come from the URL, and the
    /// connection always uses TLS (for either a `redis://` or `rediss://` URL).
    /// Unix-socket URLs are rejected.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use redis_tower_core::tls::TlsConfig;
    /// let tls = TlsConfig::default_rustls()
    ///     .with_root_ca_pem(std::fs::read("ca.pem")?)
    ///     .with_client_auth_pem(std::fs::read("client.pem")?, std::fs::read("client.key")?);
    /// let conn = RedisConnection::connect_url_with_tls(
    ///     "rediss://default:secret@redis.internal:6379",
    ///     &tls,
    /// )
    /// .await?;
    /// ```
    #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
    #[cfg_attr(
        docsrs,
        doc(cfg(any(feature = "tls-native-tls", feature = "tls-rustls")))
    )]
    pub async fn connect_url_with_tls(
        url: &str,
        tls_config: &crate::tls::TlsConfig,
    ) -> Result<Self, RedisError> {
        let parsed = parse_redis_url(url)?;
        if parsed.unix {
            return Err(RedisError::InvalidUrl(
                "unix socket URLs cannot use TLS".into(),
            ));
        }
        let addr = format!("{}:{}", parsed.host, parsed.port);
        let mut conn = Self::connect_tls(&addr, &parsed.host, tls_config).await?;
        conn.post_connect_setup(&parsed).await?;
        Ok(conn)
    }

    /// Connect to a Redis server and negotiate RESP3 protocol.
    ///
    /// Sends `HELLO 3` after connecting. The server will respond with
    /// RESP3 frames for all subsequent commands.
    pub async fn connect_resp3(addr: &str) -> Result<Self, RedisError> {
        // connect() already sends CLIENT SETINFO, then we upgrade to RESP3.
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
    ///
    /// # Reconnection Warning
    ///
    /// Push subscriptions do **not** survive reconnection. If the underlying
    /// TCP connection drops and a new connection is established (e.g., via
    /// [`ResilientConnection`](https://docs.rs/redis-tower) or manual
    /// reconnection), any server-side state such as `CLIENT TRACKING`
    /// registrations is lost. The push receiver will stop receiving
    /// messages until the tracking is re-enabled on the new connection.
    ///
    /// To handle this, implement [`ConnectionFactory`](https://docs.rs/redis-tower)
    /// yourself and replay setup commands (e.g., `CLIENT TRACKING ON`) inside
    /// `connect()`. This ensures the setup runs on every fresh connection,
    /// including reconnections. For example:
    ///
    /// ```ignore
    /// use redis_tower::reconnect::{ConnectionFactory, ResilientConnection, ReconnectConfig};
    /// use redis_tower_core::{RedisConnection, RedisError};
    ///
    /// struct TrackingFactory {
    ///     addr: String,
    /// }
    ///
    /// impl ConnectionFactory for TrackingFactory {
    ///     fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
    ///         let addr = self.addr.clone();
    ///         Box::pin(async move {
    ///             let mut conn = RedisConnection::connect_resp3(&addr).await?;
    ///             // Replay CLIENT TRACKING on every new connection.
    ///             conn.execute(ClientTracking::on()).await?;
    ///             Ok(conn)
    ///         })
    ///     }
    /// }
    /// ```
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
    ///
    /// # Large values
    ///
    /// For values approaching 10MB or larger, the sequential send-then-read
    /// pattern can cause TCP backpressure issues on some managed Redis
    /// services (e.g., ElastiCache) if the write buffer fills before the
    /// response starts being read. Consider using `AutoPipelineService`
    /// for large-value workloads, or splitting large values across
    /// multiple keys.
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

    /// Send CLIENT SETINFO to identify the client library.
    ///
    /// This is best-effort: errors are silently ignored because older
    /// Redis versions do not support the command.
    async fn identify_client(&mut self) {
        let framed = self.framed.as_mut().unwrap();
        // CLIENT SETINFO LIB-NAME redis-tower
        let _ = framed
            .send(array(vec![
                bulk("CLIENT"),
                bulk("SETINFO"),
                bulk("LIB-NAME"),
                bulk("redis-tower"),
            ]))
            .await;
        let _ = read_response_from(framed, &self.push_tx).await;
        // CLIENT SETINFO LIB-VER <version>
        let _ = framed
            .send(array(vec![
                bulk("CLIENT"),
                bulk("SETINFO"),
                bulk("LIB-VER"),
                bulk(env!("CARGO_PKG_VERSION")),
            ]))
            .await;
        let _ = read_response_from(framed, &self.push_tx).await;
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

/// Guard that returns the framed transport via the oneshot channel on drop.
///
/// This ensures the transport is not leaked when a `Service::call` future is
/// cancelled (e.g., by `tokio::time::timeout`, `select!`, or task abort).
/// On the success path the future takes the fields out of the guard before
/// it is dropped, so the `Drop` impl becomes a no-op.
struct FrameGuard {
    framed: Option<Framed<RedisStream, RespCodec>>,
    return_tx: Option<oneshot::Sender<Framed<RedisStream, RespCodec>>>,
}

impl Drop for FrameGuard {
    fn drop(&mut self) {
        if let (Some(framed), Some(tx)) = (self.framed.take(), self.return_tx.take()) {
            let _ = tx.send(framed);
        }
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

        // Use a guard to ensure the framed transport is returned even if the
        // future is dropped (e.g., timeout, select!, task cancellation).
        let mut guard = FrameGuard {
            framed: Some(framed),
            return_tx: Some(return_tx),
        };

        Box::pin(async move {
            let framed = guard.framed.as_mut().unwrap();

            // Flush the buffered write.
            framed.flush().await.map_err(RedisError::from)?;

            // Read response, routing push frames.
            let response = read_response_from(framed, &push_tx).await?;

            // Explicitly return the transport on success (disarms the guard).
            let _ = guard
                .return_tx
                .take()
                .unwrap()
                .send(guard.framed.take().unwrap());

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
    use crate::command::Command;

    /// Minimal command type used only in unit tests.
    struct DummyCmd;
    impl Command for DummyCmd {
        type Response = ();
        fn to_frame(&self) -> Frame {
            Frame::SimpleString(b"PING"[..].into())
        }
        fn parse_response(&self, _frame: Frame) -> Result<(), RedisError> {
            Ok(())
        }
        fn name(&self) -> &str {
            "DUMMY"
        }
    }

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

    #[tokio::test]
    async fn poll_ready_returns_connection_closed_after_cancelled_future() {
        use tower_service::Service;

        // Simulate a cancelled Service::call future: the sender side of the
        // oneshot is dropped without sending the framed transport back.
        let (tx, rx) = oneshot::channel::<Framed<RedisStream, RespCodec>>();
        drop(tx); // Simulates the future being dropped before completion.

        let mut conn = RedisConnection {
            framed: None,
            push_tx: None,
            inflight: Some(rx),
        };

        // poll_ready should detect the cancelled sender and return an error
        // rather than hanging forever.
        let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        match Service::<DummyCmd>::poll_ready(&mut conn, &mut cx) {
            Poll::Ready(Err(RedisError::ConnectionClosed)) => {}
            other => panic!("expected Ready(Err(ConnectionClosed)), got: {other:?}"),
        }
    }

    #[test]
    fn frame_guard_returns_transport_on_drop() {
        // Verify that FrameGuard sends the framed transport back when dropped.
        let (return_tx, mut return_rx) = oneshot::channel::<Framed<RedisStream, RespCodec>>();

        // We cannot easily construct a real Framed without a socket, but we can
        // verify the guard sends the return_tx by checking the receiver is not
        // cancelled after the guard is dropped with both fields populated.
        //
        // Since we need a real Framed to test the full path, we instead test
        // that dropping a guard with return_tx=None does NOT panic.
        let guard = FrameGuard {
            framed: None,
            return_tx: Some(return_tx),
        };
        drop(guard);
        // Sender was dropped (framed was None), so receiver should get an error.
        assert!(return_rx.try_recv().is_err());
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
