//! Redis Transaction Commands
//!
//! Redis transactions allow executing a group of commands atomically.
//! Commands are queued with MULTI, executed with EXEC, and can be aborted with DISCARD.
//! WATCH provides optimistic locking by monitoring keys for changes.

use crate::codec::Frame;
use crate::types::value::FromFrame;
use crate::types::{RedisError, RedisValue};
use bytes::Bytes;

use super::Command;

/// MULTI command - Mark the start of a transaction block
///
/// Marks the start of a transaction block. Subsequent commands will be queued and executed
/// atomically when EXEC is called. All commands return "QUEUED" status until EXEC.
///
/// # Request
/// (no parameters)
///
/// # Response
/// Returns `()` - Always returns OK to indicate transaction mode started
///
/// # Redis Version
/// Available since Redis 1.2.0
///
/// # Example
/// ```no_run
/// use redis_tower::commands::transactions::Multi;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// client.call(Multi).await?;
/// // Now in transaction mode - subsequent commands are queued
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Multi;

impl Command for Multi {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("MULTI")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(ref s) if s.as_ref() == b"OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// EXEC command - Execute all commands issued after MULTI
///
/// Executes all previously queued commands in a transaction atomically and restores
/// the connection state to normal. When using WATCH, returns None if any watched key
/// was modified, aborting the transaction.
///
/// # Request
/// (no parameters)
///
/// # Response
/// Returns `Option<Vec<RedisValue>>`:
/// - `Some(results)` - Array of command results, one per queued command
/// - `None` - Transaction aborted (a WATCH key was modified)
///
/// # Redis Version
/// Available since Redis 1.2.0
///
/// # Example
/// ```no_run
/// use redis_tower::commands::transactions::Exec;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let results = client.call(Exec).await?;
/// if let Some(values) = results {
///     println!("Transaction executed: {} commands", values.len());
/// } else {
///     println!("Transaction aborted (WATCH key modified)");
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exec;

impl Command for Exec {
    type Response = Option<Vec<RedisValue>>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("EXEC")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let values: Result<Vec<_>, _> =
                    items.into_iter().map(RedisValue::from_frame).collect();
                Ok(Some(values?))
            }
            Frame::BulkString(None) => Ok(None), // Transaction aborted (WATCH key modified)
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// DISCARD command - Discard all commands issued after MULTI
///
/// Flushes all previously queued commands in a transaction and restores the connection
/// state to normal. If WATCH was used, all watched keys are unwatched.
///
/// # Request
/// (no parameters)
///
/// # Response
/// Returns `()` - Always returns OK
///
/// # Redis Version
/// Available since Redis 2.0.0
///
/// # Example
/// ```no_run
/// use redis_tower::commands::transactions::Discard;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// client.call(Discard).await?;
/// // Transaction discarded, connection back to normal mode
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discard;

impl Command for Discard {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("DISCARD")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(ref s) if s.as_ref() == b"OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// WATCH command - Watch keys for conditional transaction execution
///
/// Marks the given keys to be watched for conditional execution of a transaction. If any
/// watched key is modified before EXEC, the entire transaction will be aborted and EXEC
/// returns None. This provides optimistic locking for implementing check-and-set operations.
///
/// # Request
/// - `keys`: One or more keys to watch for modifications
///
/// # Response
/// Returns `()` - Always returns OK
///
/// # Redis Version
/// Available since Redis 2.2.0
///
/// # Example
/// ```no_run
/// use redis_tower::commands::transactions::Watch;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Watch keys before starting transaction
/// client.call(Watch::new(vec!["balance", "pending"])).await?;
/// // If these keys are modified before EXEC, transaction aborts
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watch {
    keys: Vec<Bytes>,
}

impl Watch {
    /// Create a new WATCH command
    ///
    /// # Arguments
    /// * `keys` - Keys to watch
    pub fn new<K: AsRef<[u8]>>(keys: impl IntoIterator<Item = K>) -> Self {
        Self {
            keys: keys
                .into_iter()
                .map(|k| Bytes::copy_from_slice(k.as_ref()))
                .collect(),
        }
    }
}

impl Command for Watch {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut parts = Vec::with_capacity(1 + self.keys.len());
        parts.push(Frame::BulkString(Some(Bytes::from("WATCH"))));

        for key in &self.keys {
            parts.push(Frame::BulkString(Some(key.clone())));
        }

        Frame::Array(parts)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(ref s) if s.as_ref() == b"OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// UNWATCH command - Forget about all watched keys
///
/// Flushes all the previously watched keys for a transaction. If you call EXEC or DISCARD,
/// there's no need to manually call UNWATCH as they automatically unwatch all keys.
///
/// # Request
/// (no parameters)
///
/// # Response
/// Returns `()` - Always returns OK
///
/// # Redis Version
/// Available since Redis 2.2.0
///
/// # Example
/// ```no_run
/// use redis_tower::commands::transactions::Unwatch;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// client.call(Unwatch).await?;
/// // All watched keys are now unwatched
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unwatch;

impl Command for Unwatch {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("UNWATCH")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(ref s) if s.as_ref() == b"OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multi_to_frame() {
        let cmd = Multi;
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            Frame::Array(vec![Frame::BulkString(Some(Bytes::from("MULTI")))])
        );
    }

    #[test]
    fn test_multi_parse_ok() {
        let frame = Frame::SimpleString(Bytes::from("OK"));
        let result = Multi::parse_response(frame);
        assert!(result.is_ok());
    }

    #[test]
    fn test_exec_to_frame() {
        let cmd = Exec;
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            Frame::Array(vec![Frame::BulkString(Some(Bytes::from("EXEC")))])
        );
    }

    #[test]
    fn test_exec_parse_array() {
        let frame = Frame::Array(vec![
            Frame::SimpleString(Bytes::from("OK")),
            Frame::Integer(42),
        ]);
        let result = Exec::parse_response(frame).unwrap();
        assert!(result.is_some());
        let values = result.unwrap();
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_exec_parse_aborted() {
        let frame = Frame::BulkString(None);
        let result = Exec::parse_response(frame).unwrap();
        assert!(result.is_none()); // Transaction was aborted
    }

    #[test]
    fn test_discard_to_frame() {
        let cmd = Discard;
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            Frame::Array(vec![Frame::BulkString(Some(Bytes::from("DISCARD")))])
        );
    }

    #[test]
    fn test_discard_parse_ok() {
        let frame = Frame::SimpleString(Bytes::from("OK"));
        let result = Discard::parse_response(frame);
        assert!(result.is_ok());
    }

    #[test]
    fn test_watch_single_key() {
        let cmd = Watch::new(vec!["mykey"]);
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::BulkString(Some(Bytes::from("WATCH"))),
                Frame::BulkString(Some(Bytes::from("mykey"))),
            ])
        );
    }

    #[test]
    fn test_watch_multiple_keys() {
        let cmd = Watch::new(vec!["key1", "key2", "key3"]);
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::BulkString(Some(Bytes::from("WATCH"))),
                Frame::BulkString(Some(Bytes::from("key1"))),
                Frame::BulkString(Some(Bytes::from("key2"))),
                Frame::BulkString(Some(Bytes::from("key3"))),
            ])
        );
    }

    #[test]
    fn test_watch_parse_ok() {
        let frame = Frame::SimpleString(Bytes::from("OK"));
        let result = Watch::parse_response(frame);
        assert!(result.is_ok());
    }

    #[test]
    fn test_unwatch_to_frame() {
        let cmd = Unwatch;
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            Frame::Array(vec![Frame::BulkString(Some(Bytes::from("UNWATCH")))])
        );
    }

    #[test]
    fn test_unwatch_parse_ok() {
        let frame = Frame::SimpleString(Bytes::from("OK"));
        let result = Unwatch::parse_response(frame);
        assert!(result.is_ok());
    }

    #[test]
    fn test_all_commands_parse_error() {
        let error_frame = Frame::Error(Bytes::from("ERR something went wrong"));

        assert!(Multi::parse_response(error_frame.clone()).is_err());
        assert!(Exec::parse_response(error_frame.clone()).is_err());
        assert!(Discard::parse_response(error_frame.clone()).is_err());
        assert!(Watch::parse_response(error_frame.clone()).is_err());
        assert!(Unwatch::parse_response(error_frame).is_err());
    }
}
