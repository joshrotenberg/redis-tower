//! Redis client implementation using Tower services

use crate::codec::RespCodec;
use crate::commands::Command;
use crate::types::RedisError;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::codec::Framed;

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
        framed.send(frame).await.map_err(|e| {
            RedisError::Connection(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                e.to_string(),
            ))
        })?;

        // Receive the response frame
        let response_frame = framed
            .next()
            .await
            .ok_or_else(|| {
                RedisError::Connection(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Connection closed",
                ))
            })?
            .map_err(RedisError::Connection)?;

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
