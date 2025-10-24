//! Redis client implementation using Tower services

use crate::codec::RespCodec;
use crate::commands::Command;
use crate::types::RedisError;
use futures::{SinkExt, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::codec::Framed;
use tower::Service;

/// Redis connection - implements Tower's Service trait
///
/// This is the core connection type that sends commands to Redis and receives responses.
/// It wraps the connection in an Arc<Mutex<>> to allow sharing across async boundaries.
#[derive(Clone)]
pub struct RedisConnection {
    pub(crate) framed: Arc<Mutex<Framed<TcpStream, RespCodec>>>,
}

impl RedisConnection {
    /// Connect to a Redis server
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
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        let stream = TcpStream::connect(addr).await?;
        let codec = RespCodec::new();
        let framed = Framed::new(stream, codec);

        Ok(Self {
            framed: Arc::new(Mutex::new(framed)),
        })
    }

    /// Execute a command
    pub async fn execute<Cmd>(&self, command: Cmd) -> Result<Cmd::Response, RedisError>
    where
        Cmd: Command,
    {
        let frame = command.to_frame();
        let mut framed = self.framed.lock().await;

        // Send the command frame
        framed
            .send(frame)
            .await
            .map_err(|e| RedisError::Connection(e.to_string()))?;

        // Receive the response frame
        let response_frame = framed
            .next()
            .await
            .ok_or_else(|| RedisError::Connection("Connection closed".to_string()))?
            .map_err(|e| RedisError::Connection(e.to_string()))?;

        // Parse the response using the command's type-safe parser
        Cmd::parse_response(response_frame)
    }
}

/// High-level Redis client
///
/// This wraps RedisConnection and provides a convenient API for sending commands.
#[derive(Clone)]
pub struct RedisClient {
    connection: RedisConnection,
}

impl RedisClient {
    /// Connect to a Redis server
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
        let connection = RedisConnection::connect(addr).await?;
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

/// Tower Service implementation for RedisConnection
///
/// This allows RedisConnection to be used with Tower middleware like
/// timeouts, retries, circuit breakers, etc.
///
/// # Example
/// ```no_run
/// use redis_tower::client::RedisConnection;
/// use redis_tower::commands::Get;
/// use tower::{Service, ServiceExt};
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// // Use as a Tower service
/// let response = conn.ready().await?.call(Get::new("mykey")).await?;
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
