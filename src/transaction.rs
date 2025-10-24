//! Redis transactions with MULTI/EXEC
//!
//! Redis transactions allow executing a batch of commands atomically.
//! All commands are queued and executed together when EXEC is called.
//!
//! # Example
//! ```no_run
//! use redis_tower::client::RedisConnection;
//! use redis_tower::transaction::Transaction;
//! use redis_tower::commands::{Get, Set, Incr};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisConnection::connect("127.0.0.1:6379").await?;
//!
//! let mut tx = Transaction::new(&client);
//! tx.queue(Set::new("key1", b"value1".to_vec())).await?;
//! tx.queue(Incr::new("counter")).await?;
//! tx.queue(Get::new("key1")).await?;
//!
//! let results = tx.exec().await?;
//! // results is Vec<RedisValue> with response for each command
//! # Ok(())
//! # }
//! ```

use crate::client::RedisConnection;
use crate::codec::Frame;
use crate::commands::Command;
use crate::types::value::FromFrame;
use crate::types::{RedisError, RedisValue};
use bytes::Bytes;
use futures::{SinkExt, StreamExt};

/// A Redis transaction builder
///
/// Transactions queue commands after MULTI and execute them atomically with EXEC.
/// All queued commands execute in order, and either all succeed or all fail together.
pub struct Transaction<'a> {
    client: &'a RedisConnection,
    in_transaction: bool,
    command_count: usize,
}

impl<'a> Transaction<'a> {
    /// Create a new transaction
    ///
    /// This does not send MULTI yet - that happens on the first queue() call.
    pub fn new(client: &'a RedisConnection) -> Self {
        Self {
            client,
            in_transaction: false,
            command_count: 0,
        }
    }

    /// Queue a command in the transaction
    ///
    /// The first call to queue() will automatically send MULTI.
    /// Subsequent commands will be queued and return "QUEUED".
    pub async fn queue<C: Command>(&mut self, command: C) -> Result<(), RedisError> {
        // Send MULTI on first command
        if !self.in_transaction {
            self.send_multi().await?;
            self.in_transaction = true;
        }

        // Send the command
        let frame = command.to_frame();
        let mut framed = self.client.framed.lock().await;
        framed
            .send(frame)
            .await
            .map_err(|e| RedisError::Connection(e.to_string()))?;

        // Receive QUEUED response
        let response = framed
            .next()
            .await
            .ok_or_else(|| {
                RedisError::Protocol("Connection closed during transaction".to_string())
            })?
            .map_err(|e| RedisError::Connection(e.to_string()))?;

        // Verify we got QUEUED
        match response {
            Frame::SimpleString(s) => {
                let status = String::from_utf8_lossy(&s);
                if status != "QUEUED" {
                    return Err(RedisError::Protocol(format!(
                        "Expected QUEUED, got: {}",
                        status
                    )));
                }
            }
            Frame::Error(e) => {
                return Err(RedisError::Redis(String::from_utf8_lossy(&e).to_string()));
            }
            _ => {
                return Err(RedisError::Protocol(
                    "Unexpected response to queued command".to_string(),
                ));
            }
        }

        self.command_count += 1;
        Ok(())
    }

    /// Execute the transaction and return all results
    ///
    /// Returns a Vec<RedisValue> with one entry for each queued command.
    /// If the transaction was aborted (e.g., due to WATCH), returns None.
    pub async fn exec(mut self) -> Result<Option<Vec<RedisValue>>, RedisError> {
        if !self.in_transaction {
            return Err(RedisError::Protocol(
                "No commands queued in transaction".to_string(),
            ));
        }

        // Send EXEC
        let exec_frame = Frame::Array(vec![Frame::BulkString(Some(Bytes::from("EXEC")))]);
        let mut framed = self.client.framed.lock().await;
        framed
            .send(exec_frame)
            .await
            .map_err(|e| RedisError::Connection(e.to_string()))?;

        // Receive response
        let response = framed
            .next()
            .await
            .ok_or_else(|| RedisError::Protocol("Connection closed during EXEC".to_string()))?
            .map_err(|e| RedisError::Connection(e.to_string()))?;

        self.in_transaction = false;

        match response {
            Frame::Array(results) => {
                // Convert each result to RedisValue
                let mut values = Vec::with_capacity(results.len());
                for result in results {
                    values.push(RedisValue::from_frame(result)?);
                }
                Ok(Some(values))
            }
            Frame::Null => {
                // Transaction was aborted (WATCH key was modified)
                Ok(None)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }

    /// Discard the transaction without executing
    ///
    /// This cancels all queued commands.
    pub async fn discard(mut self) -> Result<(), RedisError> {
        if !self.in_transaction {
            return Ok(()); // Nothing to discard
        }

        let discard_frame = Frame::Array(vec![Frame::BulkString(Some(Bytes::from("DISCARD")))]);
        let mut framed = self.client.framed.lock().await;
        framed
            .send(discard_frame)
            .await
            .map_err(|e| RedisError::Connection(e.to_string()))?;

        let response = framed
            .next()
            .await
            .ok_or_else(|| RedisError::Protocol("Connection closed during DISCARD".to_string()))?
            .map_err(|e| RedisError::Connection(e.to_string()))?;

        self.in_transaction = false;

        match response {
            Frame::SimpleString(s) => {
                let status = String::from_utf8_lossy(&s);
                if status == "OK" {
                    Ok(())
                } else {
                    Err(RedisError::Protocol(format!(
                        "Unexpected DISCARD response: {}",
                        status
                    )))
                }
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }

    async fn send_multi(&self) -> Result<(), RedisError> {
        let multi_frame = Frame::Array(vec![Frame::BulkString(Some(Bytes::from("MULTI")))]);
        let mut framed = self.client.framed.lock().await;
        framed
            .send(multi_frame)
            .await
            .map_err(|e| RedisError::Connection(e.to_string()))?;

        let response = framed
            .next()
            .await
            .ok_or_else(|| RedisError::Protocol("Connection closed during MULTI".to_string()))?
            .map_err(|e| RedisError::Connection(e.to_string()))?;

        match response {
            Frame::SimpleString(s) => {
                let status = String::from_utf8_lossy(&s);
                if status == "OK" {
                    Ok(())
                } else {
                    Err(RedisError::Protocol(format!(
                        "Unexpected MULTI response: {}",
                        status
                    )))
                }
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        // If transaction is still active when dropped, we should DISCARD
        // but we can't do async in Drop, so this is best-effort warning
        if self.in_transaction {
            eprintln!(
                "Warning: Transaction dropped without EXEC or DISCARD. \
                 Connection may be in transaction state."
            );
        }
    }
}

/// WATCH command - watch keys for conditional execution
///
/// If any watched key is modified before EXEC, the transaction will abort.
///
/// # Example
/// ```no_run
/// use redis_tower::Watch;
/// use redis_tower::Transaction;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = redis_tower::client::RedisConnection::connect("127.0.0.1:6379").await?;
/// // Watch a key before transaction
/// client.execute(Watch::new("balance")).await?;
///
/// // If balance is modified by another client here, EXEC will return null
///
/// let mut tx = Transaction::new(&client);
/// // ... queue commands
/// let result = tx.exec().await?;
/// if result.is_none() {
///     println!("Transaction aborted - balance was modified");
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Watch {
    pub(crate) keys: Vec<String>,
}

impl Watch {
    /// Create a new WATCH command with the first key
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    /// Add another key to watch
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.keys.push(key.into());
        self
    }

    /// Add multiple keys to watch
    pub fn keys<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.keys.extend(keys.into_iter().map(Into::into));
        self
    }
}

impl Command for Watch {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut parts = vec![Frame::BulkString(Some(Bytes::from("WATCH")))];

        for key in &self.keys {
            parts.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// UNWATCH command - clear all watched keys
///
/// # Example
/// ```no_run
/// use redis_tower::{Watch, Unwatch};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = redis_tower::client::RedisConnection::connect("127.0.0.1:6379").await?;
/// client.execute(Watch::new("key1").key("key2")).await?;
/// // Changed our mind
/// client.execute(Unwatch).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Unwatch;

impl Command for Unwatch {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("UNWATCH")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watch_frame() {
        let cmd = Watch::new("key1").key("key2");
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3); // WATCH, key1, key2
            }
            _ => panic!("Expected array frame"),
        }
    }

    #[test]
    fn test_unwatch_frame() {
        let cmd = Unwatch;
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1); // UNWATCH
            }
            _ => panic!("Expected array frame"),
        }
    }
}
