//! Redis client implementation using Tower services

use crate::codec::RespCodec;
use crate::commands::Command;
use crate::config::ClientConfig;
use crate::connection_pool::ResilientConnection;
use crate::pipeline::{Pipeline, PipelineExecutor, PipelineResults};
use crate::tls::TlsConfig;
use crate::types::RedisError;
use crate::url::RedisUrl;
use futures::{SinkExt, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::codec::Framed;
use tower::Service;

/// Stream type that can be either plain TCP or TLS
pub(crate) enum RedisStream {
    Plain(TcpStream),
    #[cfg(feature = "tls-native-tls")]
    NativeTls(Box<tokio_native_tls::TlsStream<TcpStream>>),
    #[cfg(feature = "tls-rustls")]
    Rustls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for RedisStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            RedisStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls-native-tls")]
            RedisStream::NativeTls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
            #[cfg(feature = "tls-rustls")]
            RedisStream::Rustls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for RedisStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match &mut *self {
            RedisStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls-native-tls")]
            RedisStream::NativeTls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
            #[cfg(feature = "tls-rustls")]
            RedisStream::Rustls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            RedisStream::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls-native-tls")]
            RedisStream::NativeTls(s) => Pin::new(s.as_mut()).poll_flush(cx),
            #[cfg(feature = "tls-rustls")]
            RedisStream::Rustls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match &mut *self {
            RedisStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls-native-tls")]
            RedisStream::NativeTls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            #[cfg(feature = "tls-rustls")]
            RedisStream::Rustls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Redis connection - implements Tower's Service trait
///
/// This is the core connection type that sends commands to Redis and receives responses.
/// It wraps the connection in an Arc<Mutex<>> to allow sharing across async boundaries.
#[derive(Clone)]
pub struct RedisConnection {
    pub(crate) framed: Arc<Mutex<Framed<RedisStream, RespCodec>>>,
}

impl RedisConnection {
    /// Connect to a Redis server without TLS
    ///
    /// # Example
    /// ```no_run
    /// use redis_tower::client::RedisConnection;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let conn = RedisConnection::connect("127.0.0.1:6379").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[tracing::instrument(fields(addr))]
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        tracing::info!("connecting to Redis");
        Self::connect_with_config(addr, TlsConfig::None).await
    }

    /// Connect to a Redis server with TLS configuration
    ///
    /// # Example
    /// ```no_run
    /// use redis_tower::client::RedisConnection;
    /// use redis_tower::tls::TlsConfig;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let tls = TlsConfig::rustls()
    ///     .with_native_roots()
    ///     .build()?;
    /// let conn = RedisConnection::connect_with_config("127.0.0.1:6379", tls).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[tracing::instrument(fields(addr, tls = ?tls))]
    pub async fn connect_with_config(addr: &str, tls: TlsConfig) -> Result<Self, RedisError> {
        tracing::info!("establishing connection");
        let tcp_stream = TcpStream::connect(addr).await?;
        tracing::debug!("TCP connection established");

        let stream = match tls {
            TlsConfig::None => RedisStream::Plain(tcp_stream),

            #[cfg(feature = "tls-native-tls")]
            TlsConfig::NativeTls(config) => {
                tracing::debug!("establishing TLS connection (native-tls)");
                // Extract domain from address for SNI
                let domain = addr.split(':').next().unwrap_or(addr);
                let tls_stream = config.connect(domain, tcp_stream).await?;
                tracing::debug!("TLS handshake complete");
                RedisStream::NativeTls(Box::new(tls_stream))
            }

            #[cfg(feature = "tls-rustls")]
            TlsConfig::Rustls(config) => {
                tracing::debug!("establishing TLS connection (rustls)");
                // Extract domain from address for SNI
                let domain = addr.split(':').next().unwrap_or(addr);
                let tls_stream = config.connect(domain, tcp_stream).await?;
                tracing::debug!("TLS handshake complete");
                RedisStream::Rustls(Box::new(tls_stream))
            }
        };

        let codec = RespCodec::new();
        let framed = Framed::new(stream, codec);

        tracing::info!("connection established successfully");
        Ok(Self {
            framed: Arc::new(Mutex::new(framed)),
        })
    }

    /// Connect to a Redis server using a URL
    ///
    /// Supports `redis://` and `rediss://` schemes. The `rediss://` scheme
    /// automatically enables TLS.
    ///
    /// # Example
    /// ```no_run
    /// use redis_tower::client::RedisConnection;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // Plain connection
    /// let conn = RedisConnection::connect_url("redis://localhost:6379").await?;
    ///
    /// // TLS connection (requires tls feature)
    /// let secure_conn = RedisConnection::connect_url("rediss://localhost:6380").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        let parsed = RedisUrl::parse(url)?;
        Self::connect_with_config(&parsed.addr(), parsed.tls).await
    }

    /// Execute a command
    #[tracing::instrument(skip(self, command))]
    pub async fn execute<Cmd>(&self, command: Cmd) -> Result<Cmd::Response, RedisError>
    where
        Cmd: Command,
    {
        tracing::debug!("executing command");

        let frame = command.to_frame();
        let mut framed = self.framed.lock().await;

        // Send the command frame
        tracing::trace!("sending command frame");
        framed
            .send(frame)
            .await
            .map_err(|e| RedisError::Connection(e.to_string()))?;

        // Receive the response frame
        tracing::trace!("waiting for response");
        let response_frame = framed
            .next()
            .await
            .ok_or_else(|| RedisError::Connection("Connection closed".to_string()))?
            .map_err(|e| RedisError::Connection(e.to_string()))?;

        // Parse the response using the command's type-safe parser
        tracing::trace!("parsing response");
        let result = Cmd::parse_response(response_frame);

        if result.is_ok() {
            tracing::debug!("command executed successfully");
        } else {
            tracing::warn!("command execution failed");
        }

        result
    }
}

/// High-level Redis client
///
/// This wraps RedisConnection and provides a convenient API for sending commands.
#[derive(Clone)]
pub struct RedisClient {
    connection: RedisConnection,
}

/// Redis client with automatic reconnection
///
/// This variant uses ResilientConnection to automatically handle connection
/// failures with configurable retry logic.
#[derive(Clone)]
pub struct ResilientRedisClient {
    connection: ResilientConnection,
}

impl RedisClient {
    /// Connect to a Redis server without TLS
    ///
    /// # Example
    /// ```no_run
    /// use redis_tower::RedisClient;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = RedisClient::connect("127.0.0.1:6379").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        Self::connect_with_config(addr, TlsConfig::None).await
    }

    /// Connect to a Redis server with TLS configuration
    ///
    /// # Example
    /// ```no_run
    /// use redis_tower::RedisClient;
    /// use redis_tower::tls::TlsConfig;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let tls = TlsConfig::rustls()
    ///     .with_native_roots()
    ///     .build()?;
    /// let client = RedisClient::connect_with_config("redis.example.com:6380", tls).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_config(addr: &str, tls: TlsConfig) -> Result<Self, RedisError> {
        let connection = RedisConnection::connect_with_config(addr, tls).await?;
        Ok(Self { connection })
    }

    /// Connect to a Redis server using a URL
    ///
    /// Supports `redis://` and `rediss://` schemes. The `rediss://` scheme
    /// automatically enables TLS.
    ///
    /// # Example
    /// ```no_run
    /// use redis_tower::RedisClient;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // Plain connection
    /// let client = RedisClient::connect_url("redis://localhost:6379").await?;
    ///
    /// // TLS connection (requires tls feature)
    /// let secure_client = RedisClient::connect_url("rediss://redis.example.com:6380").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        let connection = RedisConnection::connect_url(url).await?;
        Ok(Self { connection })
    }

    /// Execute a command
    ///
    /// # Example
    /// ```no_run
    /// use redis_tower::{RedisClient, commands::Get};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut client = RedisClient::connect("127.0.0.1:6379").await?;
    /// let value = client.call(Get::new("mykey")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call<Cmd>(&self, command: Cmd) -> Result<Cmd::Response, RedisError>
    where
        Cmd: Command,
    {
        self.connection.execute(command).await
    }
}

impl ResilientRedisClient {
    /// Connect to a Redis server with full configuration including auto-reconnect
    ///
    /// # Example
    /// ```no_run
    /// use redis_tower::client::ResilientRedisClient;
    /// use redis_tower::config::ClientConfig;
    /// use tower_resilience::reconnect::{ReconnectConfig, ReconnectPolicy};
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = ClientConfig::builder()
    ///     .reconnect(
    ///         ReconnectConfig::builder()
    ///             .policy(ReconnectPolicy::exponential(
    ///                 Duration::from_millis(100),
    ///                 Duration::from_secs(5),
    ///             ))
    ///             .max_attempts(10)
    ///             .build()
    ///     )
    ///     .build();
    ///
    /// let client = ResilientRedisClient::connect_with_full_config(
    ///     "127.0.0.1:6379",
    ///     config
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_with_full_config(
        addr: &str,
        config: ClientConfig,
    ) -> Result<Self, RedisError> {
        let connection = ResilientConnection::new(
            addr.to_string(),
            config.tls,
            config.reconnect,
            config.metrics,
        )
        .await?;

        Ok(Self { connection })
    }

    /// Connect to a Redis server with default reconnection settings
    ///
    /// Uses exponential backoff from 100ms to 5s with unlimited retry attempts.
    ///
    /// # Example
    /// ```no_run
    /// use redis_tower::client::ResilientRedisClient;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = ResilientRedisClient::connect("127.0.0.1:6379").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        Self::connect_with_full_config(addr, ClientConfig::default()).await
    }

    /// Connect using a URL with default reconnection settings
    ///
    /// # Example
    /// ```no_run
    /// use redis_tower::client::ResilientRedisClient;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = ResilientRedisClient::connect_url("redis://localhost:6379").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        let parsed = RedisUrl::parse(url)?;
        let addr = parsed.addr();
        let config = ClientConfig::builder().tls(parsed.tls).build();
        Self::connect_with_full_config(&addr, config).await
    }

    /// Execute a command with automatic reconnection on failure
    ///
    /// # Example
    /// ```no_run
    /// use redis_tower::client::ResilientRedisClient;
    /// use redis_tower::commands::Get;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = ResilientRedisClient::connect("127.0.0.1:6379").await?;
    /// let value: Option<String> = client.call(Get::new("mykey")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn call<Cmd>(&self, command: Cmd) -> Result<Cmd::Response, RedisError>
    where
        Cmd: Command + Clone,
    {
        self.connection.execute(command).await
    }
}

/// Tower Service implementation for RedisConnection
///
/// This allows RedisConnection to be used with Tower middleware like
/// timeouts, retries, circuit breakers, etc.
///
/// # Example
/// ```no_run
/// use redis_tower::client::RedisConnection;
/// use redis_tower::commands::Get;
/// use tower::Service;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// // Use as a Tower service
/// let response = Service::call(&mut conn, Get::new("mykey")).await?;
/// # Ok(())
/// # }
/// ```
impl<Cmd> Service<Cmd> for RedisConnection
where
    Cmd: Command + Send + 'static,
    Cmd::Response: Send + 'static,
{
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Always ready - we use a mutex internally to serialize requests
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, command: Cmd) -> Self::Future {
        let framed = self.framed.clone();

        Box::pin(async move {
            let frame = command.to_frame();
            let mut framed_lock = framed.lock().await;

            // Send the command frame
            framed_lock
                .send(frame)
                .await
                .map_err(|e| RedisError::Connection(e.to_string()))?;

            // Receive the response frame
            let response_frame = framed_lock
                .next()
                .await
                .ok_or_else(|| RedisError::Connection("Connection closed".to_string()))?
                .map_err(|e| RedisError::Connection(e.to_string()))?;

            // Parse the response using the command's type-safe parser
            Cmd::parse_response(response_frame)
        })
    }
}

/// Pipeline execution implementation for RedisConnection
impl PipelineExecutor for RedisConnection {
    async fn execute_pipeline(&self, pipeline: &Pipeline) -> Result<PipelineResults, RedisError> {
        let mut framed = self.framed.lock().await;

        // Send all command frames
        for frame in pipeline.frames() {
            framed
                .send(frame)
                .await
                .map_err(|e| RedisError::Connection(e.to_string()))?;
        }

        // Calculate how many responses we expect
        let expected_responses = if pipeline.is_atomic() {
            // In atomic mode:
            // - MULTI returns +OK
            // - Each command returns +QUEUED
            // - EXEC returns an array with all results
            // We only care about the EXEC response
            pipeline.len() + 2 // +1 for MULTI OK, +1 for EXEC array
        } else {
            // In non-atomic mode, we get one response per command
            pipeline.len()
        };

        // Read all responses
        let mut responses = Vec::with_capacity(expected_responses);
        for _ in 0..expected_responses {
            let response_frame = framed
                .next()
                .await
                .ok_or_else(|| RedisError::Connection("Connection closed".to_string()))?
                .map_err(|e| RedisError::Connection(e.to_string()))?;

            responses.push(response_frame);
        }

        // Handle atomic mode differently
        if pipeline.is_atomic() {
            // Skip MULTI response (should be +OK)
            // Skip QUEUED responses
            // The last response is the EXEC array containing all results
            if let Some(exec_response) = responses.pop() {
                match exec_response {
                    crate::codec::Frame::Array(results) => {
                        return Ok(PipelineResults::new(results));
                    }
                    crate::codec::Frame::Null => {
                        return Err(RedisError::Protocol(
                            "Transaction aborted (EXEC returned null)".to_string(),
                        ));
                    }
                    _ => {
                        return Err(RedisError::Protocol("Expected array from EXEC".to_string()));
                    }
                }
            } else {
                return Err(RedisError::Protocol(
                    "No EXEC response received".to_string(),
                ));
            }
        }

        Ok(PipelineResults::new(responses))
    }
}

/// Pipeline execution implementation for RedisClient
impl PipelineExecutor for RedisClient {
    async fn execute_pipeline(&self, pipeline: &Pipeline) -> Result<PipelineResults, RedisError> {
        self.connection.execute_pipeline(pipeline).await
    }
}
