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
use redis_tower_protocol::{Frame, RespCodec, RespLimits};

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
/// Typed command types such as `Get` live in the `redis-tower-commands` crate,
/// which depends on this one, so the example below stands in a minimal local
/// [`Command`] implementation (hidden) and runs it with
/// [`execute`](Self::execute):
///
/// ```no_run
/// use redis_tower_core::{Command, Frame, RedisConnection, RedisError};
/// # use redis_tower_protocol::helpers::{array, bulk};
/// #
/// # struct Get(String);
/// #
/// # impl Command for Get {
/// #     type Response = Option<String>;
/// #
/// #     fn to_frame(&self) -> Frame {
/// #         array(vec![bulk("GET"), bulk(&self.0)])
/// #     }
/// #
/// #     fn parse_response(&self, frame: Frame) -> Result<Option<String>, RedisError> {
/// #         match frame {
/// #             Frame::BulkString(Some(b)) => Ok(Some(String::from_utf8_lossy(&b).into_owned())),
/// #             _ => Ok(None),
/// #         }
/// #     }
/// #
/// #     fn name(&self) -> &str { "GET" }
/// # }
/// #
/// # async fn example() -> Result<(), RedisError> {
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
/// let value = conn.execute(Get("my_key".to_string())).await?;
/// # let _ = value;
/// # Ok(())
/// # }
/// ```
pub struct RedisConnection {
    /// The framed transport. `None` while a `Service::call` future is in flight.
    framed: Option<Framed<RedisStream, RespCodec>>,
    /// Optional sender for RESP3 push messages. Set via `subscribe_pushes()`.
    push_tx: Option<tokio::sync::mpsc::UnboundedSender<Frame>>,
    /// Channel to reclaim the framed transport after a `Service::call` completes.
    inflight: Option<oneshot::Receiver<Framed<RedisStream, RespCodec>>>,
    /// Whether RESP3 has been negotiated via `HELLO 3`. Defaults to RESP2.
    resp3: bool,
}

/// Which RESP protocol version to negotiate when connecting.
///
/// Used with [`RedisConnection::connect_with_protocol`] to control the
/// handshake explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolVersion {
    /// Try RESP3 via `HELLO 3`, falling back to RESP2 if the server does not
    /// support it (Redis < 6.0, or `HELLO` rejected). The default.
    #[default]
    Auto,
    /// Force RESP2 -- do not send `HELLO 3`.
    Resp2,
    /// Force RESP3 -- send `HELLO 3` and return an error if it is rejected.
    Resp3,
}

/// Configuration shared by Redis connection constructors.
///
/// The default matches [`RedisConnection::connect`]: standard TCP keepalive,
/// no connect timeout, automatic RESP3 negotiation with RESP2 fallback, and
/// [`RespLimits::default`] decode limits. Builder methods can be combined
/// without adding a constructor for every possible option matrix.
///
/// ```
/// use redis_tower_core::{ConnectionConfig, ProtocolVersion, RespLimits};
/// use std::time::Duration;
///
/// let config = ConnectionConfig::new()
///     .with_connect_timeout(Some(Duration::from_secs(3)))
///     .with_protocol(ProtocolVersion::Resp3)
///     .with_resp_limits(RespLimits {
///         max_frame_size: 8 * 1024 * 1024,
///         max_depth: 32,
///     });
/// assert_eq!(config.protocol(), ProtocolVersion::Resp3);
/// ```
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    keepalive: KeepaliveConfig,
    connect_timeout: Option<Duration>,
    protocol: ProtocolVersion,
    resp_limits: RespLimits,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            keepalive: KeepaliveConfig::default(),
            connect_timeout: None,
            protocol: ProtocolVersion::Auto,
            resp_limits: RespLimits::default(),
        }
    }
}

impl ConnectionConfig {
    /// Create a connection configuration with the default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the TCP keepalive configuration.
    #[must_use]
    pub fn with_keepalive(mut self, keepalive: KeepaliveConfig) -> Self {
        self.keepalive = keepalive;
        self
    }

    /// Set or clear the connection-establishment timeout.
    ///
    /// `None` uses the operating system's connection timeout. `Some` applies
    /// to the TCP or Unix-socket connect operation; a TLS handshake is not
    /// included in this timeout.
    #[must_use]
    pub fn with_connect_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the RESP protocol negotiation policy.
    #[must_use]
    pub fn with_protocol(mut self, protocol: ProtocolVersion) -> Self {
        self.protocol = protocol;
        self
    }

    /// Set the resource limits enforced while decoding RESP frames.
    #[must_use]
    pub fn with_resp_limits(mut self, limits: RespLimits) -> Self {
        self.resp_limits = limits;
        self
    }

    /// Return the configured TCP keepalive settings.
    pub fn keepalive(&self) -> &KeepaliveConfig {
        &self.keepalive
    }

    /// Return the connection-establishment timeout.
    pub fn connect_timeout(&self) -> Option<Duration> {
        self.connect_timeout
    }

    /// Return the RESP protocol negotiation policy.
    pub fn protocol(&self) -> ProtocolVersion {
        self.protocol
    }

    /// Return the configured RESP decode limits.
    pub fn resp_limits(&self) -> RespLimits {
        self.resp_limits
    }
}

impl RedisConnection {
    /// Create a connection from a framed stream.
    fn from_framed_inner(framed: Framed<RedisStream, RespCodec>) -> Self {
        Self {
            framed: Some(framed),
            push_tx: None,
            inflight: None,
            resp3: false,
        }
    }

    /// Wrap a transport with the codec configured for this connection.
    fn from_stream_inner(stream: RedisStream, config: &ConnectionConfig) -> Self {
        Self::from_framed_inner(Framed::new(
            stream,
            RespCodec::with_limits(config.resp_limits),
        ))
    }

    /// Open a TCP stream and apply the configured timeout and keepalive.
    async fn open_tcp(addr: &str, config: &ConnectionConfig) -> Result<TcpStream, RedisError> {
        let stream = if let Some(timeout) = config.connect_timeout {
            match tokio::time::timeout(timeout, TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => return Err(RedisError::connection(addr, error)),
                Err(_elapsed) => return Err(RedisError::ConnectTimeout),
            }
        } else {
            TcpStream::connect(addr)
                .await
                .map_err(|error| RedisError::connection(addr, error))?
        };
        let stream = apply_keepalive(stream, &config.keepalive)?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }

    /// Connect to a Redis server over TCP.
    ///
    /// TCP keepalive is enabled with sensible defaults: 60 s idle, 10 s
    /// interval, 3 probes. Use [`connect_with_keepalive`](Self::connect_with_keepalive)
    /// to supply custom keepalive parameters.
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        Self::connect_with_config(addr, &ConnectionConfig::default()).await
    }

    /// Connect to a Redis server over TCP with explicit connection settings.
    ///
    /// The configured RESP decode limits are installed before any server data
    /// is read, including `CLIENT SETINFO` and protocol-negotiation replies.
    pub async fn connect_with_config(
        addr: &str,
        config: &ConnectionConfig,
    ) -> Result<Self, RedisError> {
        let mut conn = Self::connect_raw(addr, config).await?;
        conn.negotiate_protocol(config.protocol).await?;
        Ok(conn)
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
        let config = ConnectionConfig::default().with_keepalive(keepalive.clone());
        Self::connect_with_config(addr, &config).await
    }

    /// TCP connect + CLIENT SETINFO, WITHOUT protocol negotiation. The building
    /// block for the public connectors (which add RESP3 negotiation) and for
    /// `connect_with_protocol` (which negotiates explicitly).
    ///
    /// Instrumented with a `redis.connect` span so every plain-TCP connector
    /// (`connect`, `connect_with_keepalive`, `connect_with_protocol`, and
    /// `connect_resp3`, which all funnel through here) emits exactly one
    /// connection-lifecycle span carrying the target address.
    #[tracing::instrument(
        name = "redis.connect",
        skip_all,
        fields(server.address = %addr, tls = false),
        err
    )]
    async fn connect_raw(addr: &str, config: &ConnectionConfig) -> Result<Self, RedisError> {
        let stream = Self::open_tcp(addr, config).await?;
        let mut conn = Self::from_stream_inner(RedisStream::Tcp(stream), config);
        conn.identify_client().await?;
        Ok(conn)
    }

    /// Connect to a Redis server over TCP with a connect timeout.
    ///
    /// If the TCP handshake is not completed within `timeout`, returns
    /// [`RedisError::ConnectTimeout`] instead of waiting for the OS-default
    /// timeout (which can be several minutes on unreachable hosts).
    ///
    /// TCP keepalive is enabled with sensible defaults after the connection is
    /// established. Use [`connect_with_config`](Self::connect_with_config) to
    /// combine a timeout with custom keepalive or RESP decode limits.
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
        let config = ConnectionConfig::default().with_connect_timeout(Some(timeout));
        Self::connect_with_config(addr, &config).await
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
        Self::connect_tls_with_config(addr, hostname, tls_config, &ConnectionConfig::default())
            .await
    }

    /// Connect over TLS with explicit connection settings.
    ///
    /// The TCP connect timeout, keepalive policy, protocol negotiation, and
    /// RESP decode limits all come from `config`. The connect timeout covers
    /// the TCP handshake only, matching [`connect_tls_with_timeout`](Self::connect_tls_with_timeout).
    #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
    #[cfg_attr(
        docsrs,
        doc(cfg(any(feature = "tls-native-tls", feature = "tls-rustls")))
    )]
    pub async fn connect_tls_with_config(
        addr: &str,
        hostname: &str,
        tls_config: &crate::tls::TlsConfig,
        config: &ConnectionConfig,
    ) -> Result<Self, RedisError> {
        let mut conn = Self::connect_tls_raw(addr, hostname, tls_config, config).await?;
        conn.negotiate_protocol(config.protocol).await?;
        Ok(conn)
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
        let config = ConnectionConfig::default().with_keepalive(keepalive.clone());
        Self::connect_tls_with_config(addr, hostname, tls_config, &config).await
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
        let config = ConnectionConfig::default().with_connect_timeout(Some(timeout));
        Self::connect_tls_with_config(addr, hostname, tls_config, &config).await
    }

    /// Open a TLS transport and install its codec without negotiating RESP.
    #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
    #[tracing::instrument(
        name = "redis.connect",
        skip_all,
        fields(server.address = %addr, server.tls.hostname = %hostname, tls = true),
        err
    )]
    async fn connect_tls_raw(
        addr: &str,
        hostname: &str,
        tls_config: &crate::tls::TlsConfig,
        config: &ConnectionConfig,
    ) -> Result<Self, RedisError> {
        let tcp = Self::open_tcp(addr, config).await?;
        let stream = tls_config.connect(tcp, hostname).await?;
        let mut conn = Self::from_stream_inner(stream, config);
        conn.identify_client().await?;
        Ok(conn)
    }

    /// Connect using a Redis URL.
    ///
    /// Supports `redis://`, `rediss://` (TLS), and `unix://` schemes.
    ///
    /// For `rediss://` URLs, a TLS backend feature must be enabled.
    /// The `tls-rustls` backend is preferred if both are enabled.
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        Self::connect_url_with_config(url, &ConnectionConfig::default()).await
    }

    /// Connect using a Redis URL with explicit connection settings.
    ///
    /// Supports `redis://`, `rediss://` (TLS), and `unix://` schemes. URL
    /// authentication and database selection run before the configured RESP
    /// negotiation. Decode limits are active for every setup response.
    pub async fn connect_url_with_config(
        url: &str,
        config: &ConnectionConfig,
    ) -> Result<Self, RedisError> {
        let parsed = parse_redis_url(url)?;

        let mut conn = if parsed.unix {
            #[cfg(unix)]
            {
                let path = parsed
                    .path
                    .as_deref()
                    .ok_or_else(|| RedisError::InvalidUrl("unix URL missing path".into()))?;
                let stream = if let Some(timeout) = config.connect_timeout {
                    match tokio::time::timeout(timeout, tokio::net::UnixStream::connect(path)).await
                    {
                        Ok(Ok(stream)) => stream,
                        Ok(Err(error)) => return Err(RedisError::connection(path, error)),
                        Err(_elapsed) => return Err(RedisError::ConnectTimeout),
                    }
                } else {
                    tokio::net::UnixStream::connect(path)
                        .await
                        .map_err(|error| RedisError::connection(path, error))?
                };
                Self::from_stream_inner(RedisStream::Unix(stream), config)
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
                Self::connect_tls_raw(&addr, &parsed.host, &tls_config, config).await?
            }
            #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
            {
                let tls_config = crate::tls::TlsConfig::default_native_tls();
                let addr = format!("{}:{}", parsed.host, parsed.port);
                Self::connect_tls_raw(&addr, &parsed.host, &tls_config, config).await?
            }
            #[cfg(not(any(feature = "tls-native-tls", feature = "tls-rustls")))]
            {
                return Err(RedisError::InvalidUrl(
                    "TLS requires the tls-native-tls or tls-rustls feature".into(),
                ));
            }
        } else {
            Self::connect_raw(&format!("{}:{}", parsed.host, parsed.port), config).await?
        };

        conn.post_connect_setup(&parsed).await?;
        conn.negotiate_protocol(config.protocol).await?;
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
    /// ```no_run
    /// use redis_tower_core::RedisConnection;
    /// use redis_tower_core::tls::TlsConfig;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let tls = TlsConfig::default_rustls()
    ///     .with_root_ca_pem(std::fs::read("ca.pem")?)
    ///     .with_client_auth_pem(std::fs::read("client.pem")?, std::fs::read("client.key")?);
    /// let conn = RedisConnection::connect_url_with_tls(
    ///     "rediss://default:secret@redis.internal:6379",
    ///     &tls,
    /// )
    /// .await?;
    /// # let _ = conn;
    /// # Ok(())
    /// # }
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
        Self::connect_url_with_tls_and_config(url, tls_config, &ConnectionConfig::default()).await
    }

    /// Connect from a Redis URL with explicit TLS and connection settings.
    ///
    /// This combines a custom CA or mTLS configuration with keepalive,
    /// connection timeout, protocol, and RESP decode-limit settings. Unix
    /// socket URLs are rejected because this connector always uses TLS.
    #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
    #[cfg_attr(
        docsrs,
        doc(cfg(any(feature = "tls-native-tls", feature = "tls-rustls")))
    )]
    pub async fn connect_url_with_tls_and_config(
        url: &str,
        tls_config: &crate::tls::TlsConfig,
        config: &ConnectionConfig,
    ) -> Result<Self, RedisError> {
        let parsed = parse_redis_url(url)?;
        if parsed.unix {
            return Err(RedisError::InvalidUrl(
                "unix socket URLs cannot use TLS".into(),
            ));
        }
        let addr = format!("{}:{}", parsed.host, parsed.port);
        let mut conn = Self::connect_tls_raw(&addr, &parsed.host, tls_config, config).await?;
        conn.post_connect_setup(&parsed).await?;
        conn.negotiate_protocol(config.protocol).await?;
        Ok(conn)
    }

    /// Connect to a Redis server and negotiate RESP3 protocol.
    ///
    /// Sends `HELLO 3` after connecting. The server will respond with
    /// RESP3 frames for all subsequent commands.
    pub async fn connect_resp3(addr: &str) -> Result<Self, RedisError> {
        Self::connect_with_protocol(addr, ProtocolVersion::Resp3).await
    }

    /// Whether this connection has negotiated RESP3 (via `HELLO 3`).
    ///
    /// Returns `false` for a plain RESP2 connection. Useful for tests and for
    /// middleware that behaves differently under the two protocols -- push-based
    /// features such as client-side caching require RESP3.
    pub fn is_resp3(&self) -> bool {
        self.resp3
    }

    /// Connect and negotiate the protocol explicitly.
    ///
    /// - [`ProtocolVersion::Auto`] tries `HELLO 3` and silently falls back to
    ///   RESP2 on servers that do not support it (Redis < 6.0).
    /// - [`ProtocolVersion::Resp3`] forces RESP3 and errors if it is rejected.
    /// - [`ProtocolVersion::Resp2`] stays on RESP2 without sending `HELLO`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use redis_tower_core::{RedisConnection, ProtocolVersion};
    /// # tokio_test::block_on(async {
    /// let conn = RedisConnection::connect_with_protocol(
    ///     "127.0.0.1:6379",
    ///     ProtocolVersion::Resp3,
    /// ).await?;
    /// assert!(conn.is_resp3());
    /// # Ok::<(), redis_tower_core::RedisError>(())
    /// # });
    /// ```
    pub async fn connect_with_protocol(
        addr: &str,
        version: ProtocolVersion,
    ) -> Result<Self, RedisError> {
        let config = ConnectionConfig::default().with_protocol(version);
        Self::connect_with_config(addr, &config).await
    }

    /// Negotiate `version` on an already-connected connection.
    async fn negotiate_protocol(&mut self, version: ProtocolVersion) -> Result<(), RedisError> {
        match version {
            ProtocolVersion::Resp2 => {
                self.resp3 = false;
                Ok(())
            }
            ProtocolVersion::Resp3 => {
                self.hello(3).await?;
                Ok(())
            }
            ProtocolVersion::Auto => {
                // An older server that does not understand HELLO leaves us on
                // RESP2 (`resp3` stays false) -- exactly the desired fallback.
                // Transport and protocol failures are not a compatibility
                // signal and must still surface, especially configured decode
                // limit violations.
                match self.hello(3).await {
                    Ok(_) | Err(RedisError::Redis(_)) => Ok(()),
                    Err(error) => Err(error),
                }
            }
        }
    }

    /// Send HELLO to negotiate protocol version.
    ///
    /// `HELLO 3` switches to RESP3, `HELLO 2` switches back to RESP2.
    pub async fn hello(&mut self, version: u8) -> Result<Frame, RedisError> {
        let frame = array(vec![bulk("HELLO"), bulk(version.to_string())]);
        let response = {
            let framed = self.framed.as_mut().expect("connection not in flight");
            framed.send(frame).await.map_err(RedisError::from)?;
            read_response_from(framed, &self.push_tx).await?
        };
        if let Frame::Error(ref e) = response {
            return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
        }
        // A successful `HELLO 3` switches the connection to RESP3; `HELLO 2`
        // switches it back. Track it for introspection and middleware.
        self.resp3 = version == 3;
        Ok(response)
    }

    /// Wrap an existing stream in a `RedisConnection`.
    pub fn from_stream(stream: RedisStream) -> Self {
        Self::from_stream_with_config(stream, &ConnectionConfig::default())
    }

    /// Wrap an existing stream using explicit connection settings.
    ///
    /// Only the RESP decode limits apply to an already-open stream. Keepalive,
    /// connect timeout, and protocol negotiation are connection-establishment
    /// settings and are not replayed by this synchronous constructor.
    pub fn from_stream_with_config(stream: RedisStream, config: &ConnectionConfig) -> Self {
        Self::from_stream_inner(stream, config)
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
    /// including reconnections.
    ///
    /// `ConnectionFactory` lives in the `redis-tower` crate, which depends on
    /// this one, so the setup below is shown as the standalone function that
    /// the factory's `connect()` body calls:
    ///
    /// ```no_run
    /// use redis_tower_core::{Frame, RedisConnection, RedisError};
    /// use redis_tower_protocol::helpers::{array, bulk};
    /// use tokio::sync::mpsc::UnboundedReceiver;
    ///
    /// # async fn connect_tracking(
    /// #     addr: &str,
    /// # ) -> Result<(RedisConnection, UnboundedReceiver<Frame>), RedisError> {
    /// let mut conn = RedisConnection::connect_resp3(addr).await?;
    /// // Replay CLIENT TRACKING and re-subscribe on every new connection.
    /// conn.execute_pipeline(vec![array(vec![bulk("CLIENT"), bulk("TRACKING"), bulk("ON")])])
    ///     .await?;
    /// let pushes = conn.subscribe_pushes();
    /// Ok((conn, pushes))
    /// # }
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
    /// Redis command-error replies are ignored because older versions do not
    /// support `CLIENT SETINFO`. Transport and decode errors still propagate:
    /// they mean the connection is unusable or its configured limits were
    /// violated, rather than that the optional command was rejected.
    async fn identify_client(&mut self) -> Result<(), RedisError> {
        let framed = self.framed.as_mut().unwrap();
        // CLIENT SETINFO LIB-NAME redis-tower
        framed
            .send(array(vec![
                bulk("CLIENT"),
                bulk("SETINFO"),
                bulk("LIB-NAME"),
                bulk("redis-tower"),
            ]))
            .await
            .map_err(RedisError::from)?;
        let _response = read_response_from(framed, &self.push_tx).await?;
        // CLIENT SETINFO LIB-VER <version>
        framed
            .send(array(vec![
                bulk("CLIENT"),
                bulk("SETINFO"),
                bulk("LIB-VER"),
                bulk(env!("CARGO_PKG_VERSION")),
            ]))
            .await
            .map_err(RedisError::from)?;
        let _response = read_response_from(framed, &self.push_tx).await?;
        Ok(())
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
    use redis_tower_protocol::ProtocolError;

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

    async fn stream_pair() -> (RedisStream, RedisStream) {
        #[cfg(unix)]
        {
            let (client, server) = tokio::net::UnixStream::pair().unwrap();
            (RedisStream::Unix(client), RedisStream::Unix(server))
        }
        #[cfg(not(unix))]
        {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let client = TcpStream::connect(addr).await.unwrap();
            let (server, _) = listener.accept().await.unwrap();
            (RedisStream::Tcp(client), RedisStream::Tcp(server))
        }
    }

    async fn write_all(stream: &mut RedisStream, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            let written = futures::future::poll_fn(|cx| {
                tokio::io::AsyncWrite::poll_write(Pin::new(&mut *stream), cx, bytes)
            })
            .await
            .unwrap();
            assert!(written > 0, "server socket closed while writing response");
            bytes = &bytes[written..];
        }
    }

    async fn execute_against_response(
        config: &ConnectionConfig,
        response: &'static [u8],
    ) -> Result<(), RedisError> {
        let (client, server) = stream_pair().await;
        let server_task = tokio::spawn(async move {
            let mut framed = Framed::new(server, RespCodec::new());
            framed
                .next()
                .await
                .expect("client closed before sending its command")
                .expect("client sent an invalid command frame");
            let mut server = framed.into_inner();
            write_all(&mut server, response).await;
        });

        let mut conn = RedisConnection::from_stream_with_config(client, config);
        let result = conn.execute(DummyCmd).await;
        server_task.await.unwrap();
        result
    }

    async fn connect_against_hello_response(
        config: &ConnectionConfig,
        response: &'static [u8],
    ) -> Result<RedisConnection, RedisError> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let (server, _) = listener.accept().await.unwrap();
            let mut framed = Framed::new(RedisStream::Tcp(server), RespCodec::new());

            for _ in 0..2 {
                framed
                    .next()
                    .await
                    .expect("client closed during CLIENT SETINFO")
                    .expect("client sent an invalid CLIENT SETINFO frame");
                framed
                    .send(Frame::SimpleString(b"OK"[..].into()))
                    .await
                    .unwrap();
            }

            framed
                .next()
                .await
                .expect("client closed before HELLO")
                .expect("client sent an invalid HELLO frame");
            let mut server = framed.into_inner();
            write_all(&mut server, response).await;
        });

        let result = RedisConnection::connect_with_config(&addr.to_string(), config).await;
        server_task.await.unwrap();
        result
    }

    async fn connect_against_setinfo_response(
        config: &ConnectionConfig,
        response: &'static [u8],
    ) -> Result<RedisConnection, RedisError> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server_task = tokio::spawn(async move {
            let (server, _) = listener.accept().await.unwrap();
            let mut framed = Framed::new(RedisStream::Tcp(server), RespCodec::new());
            framed
                .next()
                .await
                .expect("client closed before CLIENT SETINFO")
                .expect("client sent an invalid CLIENT SETINFO frame");

            let mut server = framed.into_inner();
            write_all(&mut server, response).await;

            // A default connection accepts the first response and sends the
            // LIB-VER command. A tight connection closes as soon as decoding
            // the first response violates its configured limit.
            let mut framed = Framed::new(server, RespCodec::new());
            if let Some(Ok(_command)) = framed.next().await {
                framed
                    .send(Frame::SimpleString(b"OK"[..].into()))
                    .await
                    .unwrap();
            }
        });

        let result = RedisConnection::connect_with_config(&addr.to_string(), config).await;
        server_task.await.unwrap();
        result
    }

    #[test]
    fn connection_config_defaults_and_builders() {
        let defaults = ConnectionConfig::new();
        let default_keepalive = KeepaliveConfig::default();
        assert_eq!(defaults.keepalive().idle, default_keepalive.idle);
        assert_eq!(defaults.keepalive().interval, default_keepalive.interval);
        assert_eq!(defaults.keepalive().probes, default_keepalive.probes);
        assert_eq!(defaults.connect_timeout(), None);
        assert_eq!(defaults.protocol(), ProtocolVersion::Auto);
        assert_eq!(defaults.resp_limits(), RespLimits::default());

        let keepalive = KeepaliveConfig::new()
            .with_idle(Duration::from_secs(20))
            .with_interval(Duration::from_secs(4))
            .with_probes(7);
        let limits = RespLimits {
            max_frame_size: 4096,
            max_depth: 12,
        };
        let config = ConnectionConfig::new()
            .with_keepalive(keepalive)
            .with_connect_timeout(Some(Duration::from_secs(2)))
            .with_protocol(ProtocolVersion::Resp2)
            .with_resp_limits(limits);

        assert_eq!(config.keepalive().idle, Duration::from_secs(20));
        assert_eq!(config.keepalive().interval, Duration::from_secs(4));
        assert_eq!(config.keepalive().probes, 7);
        assert_eq!(config.connect_timeout(), Some(Duration::from_secs(2)));
        assert_eq!(config.protocol(), ProtocolVersion::Resp2);
        assert_eq!(config.resp_limits(), limits);
        assert_eq!(
            config.clone().with_connect_timeout(None).connect_timeout(),
            None
        );
    }

    #[tokio::test]
    async fn from_stream_with_config_installs_decode_limits() {
        let (client, _server) = stream_pair().await;
        let limits = RespLimits {
            max_frame_size: 2048,
            max_depth: 9,
        };
        let config = ConnectionConfig::new().with_resp_limits(limits);
        let conn = RedisConnection::from_stream_with_config(client, &config);

        let framed = conn.into_framed().unwrap();
        assert_eq!(framed.codec().limits(), limits);
    }

    #[tokio::test]
    async fn configured_depth_limit_rejects_response_accepted_by_default() {
        const DEPTH_THREE: &[u8] = b"*1\r\n*1\r\n*1\r\n+OK\r\n";

        execute_against_response(&ConnectionConfig::default(), DEPTH_THREE)
            .await
            .expect("the default nesting limit should accept this response");

        let tight = ConnectionConfig::new().with_resp_limits(RespLimits {
            max_depth: 2,
            ..RespLimits::default()
        });
        let error = execute_against_response(&tight, DEPTH_THREE)
            .await
            .expect_err("the configured nesting limit should reject this response");
        assert!(matches!(
            error,
            RedisError::Protocol(ProtocolError::NestingTooDeep { max: 2 })
        ));
    }

    #[tokio::test]
    async fn connect_with_config_applies_limits_before_hello_response() {
        const DEPTH_THREE: &[u8] = b"*1\r\n*1\r\n*1\r\n+OK\r\n";

        let default_conn =
            connect_against_hello_response(&ConnectionConfig::default(), DEPTH_THREE)
                .await
                .expect("the default nesting limit should accept the HELLO response");
        assert!(default_conn.is_resp3());

        let tight = ConnectionConfig::new().with_resp_limits(RespLimits {
            max_depth: 2,
            ..RespLimits::default()
        });
        let error = match connect_against_hello_response(&tight, DEPTH_THREE).await {
            Err(error) => error,
            Ok(_) => panic!("the configured nesting limit should reject the HELLO response"),
        };
        assert!(matches!(
            error,
            RedisError::Protocol(ProtocolError::NestingTooDeep { max: 2 })
        ));
    }

    #[tokio::test]
    async fn connect_with_config_surfaces_limits_during_setinfo() {
        const DEPTH_THREE: &[u8] = b"*1\r\n*1\r\n*1\r\n+OK\r\n";

        let defaults = ConnectionConfig::new().with_protocol(ProtocolVersion::Resp2);
        connect_against_setinfo_response(&defaults, DEPTH_THREE)
            .await
            .expect("the default nesting limit should accept the SETINFO response");

        let tight = ConnectionConfig::new()
            .with_protocol(ProtocolVersion::Resp2)
            .with_resp_limits(RespLimits {
                max_depth: 2,
                ..RespLimits::default()
            });
        let error = match connect_against_setinfo_response(&tight, DEPTH_THREE).await {
            Err(error) => error,
            Ok(_) => panic!("the configured nesting limit should reject the SETINFO response"),
        };
        assert!(matches!(
            error,
            RedisError::Protocol(ProtocolError::NestingTooDeep { max: 2 })
        ));
    }

    #[test]
    fn into_framed_returns_error_when_none() {
        let conn = RedisConnection {
            framed: None,
            push_tx: None,
            inflight: None,
            resp3: false,
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
            resp3: false,
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
            resp3: false,
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
            resp3: false,
        };

        // poll_ready should detect the cancelled sender and return an error
        // rather than hanging forever.
        let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        match Service::<DummyCmd>::poll_ready(&mut conn, &mut cx) {
            Poll::Ready(Err(RedisError::ConnectionClosed)) => {}
            other => panic!("expected Ready(Err(ConnectionClosed)), got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn connect_failure_carries_target_address() {
        // Port 1 is reserved and effectively never accepts connections, so the
        // connect attempt fails. Whatever the underlying io error, the address
        // it failed against must be attached (#464). `RedisConnection` is not
        // `Debug`, so match rather than use `expect_err`.
        let addr = "127.0.0.1:1";
        match RedisConnection::connect(addr).await {
            Err(err) => {
                assert!(err.is_connection_error(), "got: {err:?}");
                assert_eq!(err.connection_addr(), Some(addr));
                assert!(err.to_string().contains(addr), "missing addr in: {err}");
            }
            Ok(_) => panic!("connecting to a reserved port should fail"),
        }
    }

    #[tokio::test]
    async fn connect_with_timeout_failure_carries_target_address() {
        let addr = "127.0.0.1:1";
        match RedisConnection::connect_with_timeout(addr, Duration::from_secs(5)).await {
            // Either a refused connection (addr attached) or, on a slow host, a
            // ConnectTimeout. The refused path is what we exercise here.
            Err(RedisError::ConnectTimeout) => {}
            Err(err) => assert_eq!(err.connection_addr(), Some(addr), "got: {err:?}"),
            Ok(_) => panic!("connecting to a reserved port should fail"),
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
            resp3: false,
        };
        let _rx = conn.subscribe_pushes();
        assert!(conn.push_tx.is_some());
    }

    // -- redis.connect span --

    use std::sync::{Arc, Mutex};
    use tracing_subscriber::layer::{Context, Layer};
    use tracing_subscriber::prelude::*;

    /// A tracing layer that records each new span as `"<name> field=value ..."`.
    #[derive(Clone, Default)]
    struct SpanCapture {
        spans: Arc<Mutex<Vec<String>>>,
    }

    struct FieldCollector(String);

    impl tracing::field::Visit for FieldCollector {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.0.push_str(&format!(" {}={:?}", field.name(), value));
        }
    }

    impl<S> Layer<S> for SpanCapture
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: Context<'_, S>,
        ) {
            let mut collector = FieldCollector(String::new());
            attrs.record(&mut collector);
            self.spans
                .lock()
                .unwrap()
                .push(format!("{}{}", attrs.metadata().name(), collector.0));
        }
    }

    #[tokio::test]
    async fn connect_emits_redis_connect_span() {
        let capture = SpanCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);

        // Port 1 is closed, so the connect fails fast. The `redis.connect` span
        // is still created around the attempt, which is what we assert on.
        let _ = RedisConnection::connect("127.0.0.1:1").await;

        let spans = capture.spans.lock().unwrap();
        assert!(
            spans.iter().any(|s| {
                s.starts_with("redis.connect")
                    && s.contains("server.address")
                    && s.contains("127.0.0.1:1")
                    && s.contains("tls=false")
            }),
            "expected a redis.connect span with server.address and tls=false, got: {spans:?}"
        );
    }
}
