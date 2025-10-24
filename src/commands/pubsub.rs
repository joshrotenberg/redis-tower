//! Pub/Sub commands for publishing messages.
//!
//! Note: SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE are handled by
//! `PubSubConnection` in the `pubsub` module, as they require a dedicated
//! connection in Pub/Sub mode.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// Publish a message to a channel.
///
/// Returns the number of clients that received the message.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::client::RedisConnection;
/// # use redis_tower::commands::pubsub::Publish;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// // Publish a message
/// let subscribers = client.execute(
///     Publish::new("news", "Breaking: Rust 2.0 released!")
/// ).await?;
///
/// println!("Message delivered to {} subscribers", subscribers);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Publish {
    pub(crate) channel: String,
    pub(crate) message: Bytes,
}

impl Publish {
    /// Create a new PUBLISH command.
    pub fn new(channel: impl Into<String>, message: impl Into<Bytes>) -> Self {
        Self {
            channel: channel.into(),
            message: message.into(),
        }
    }
}

impl Command for Publish {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from_static(b"PUBLISH"))),
            Frame::BulkString(Some(Bytes::from(self.channel.clone()))),
            Frame::BulkString(Some(self.message.clone())),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Get the number of subscribers for channels.
///
/// Returns the number of subscribers (not counting clients subscribed to patterns)
/// for the specified channels.
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::client::RedisConnection;
/// # use redis_tower::commands::pubsub::PubsubNumsub;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let counts = client.execute(
///     PubsubNumsub::new(&["news", "updates"])
/// ).await?;
///
/// for (channel, count) in &counts {
///     println!("{}: {} subscribers", channel, count);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PubsubNumsub {
    pub(crate) channels: Vec<String>,
}

impl PubsubNumsub {
    /// Create a new PUBSUB NUMSUB command.
    pub fn new(channels: &[&str]) -> Self {
        Self {
            channels: channels.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl Command for PubsubNumsub {
    type Response = Vec<(String, i64)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from_static(b"PUBSUB"))),
            Frame::BulkString(Some(Bytes::from_static(b"NUMSUB"))),
        ];

        for channel in &self.channels {
            args.push(Frame::BulkString(Some(Bytes::from(channel.clone()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut results = Vec::new();
                let mut i = 0;

                while i < items.len() {
                    if i + 1 >= items.len() {
                        break;
                    }

                    let channel = match &items[i] {
                        Frame::BulkString(Some(bytes)) => {
                            String::from_utf8_lossy(bytes).to_string()
                        }
                        _ => {
                            return Err(RedisError::Protocol(
                                "Expected bulk string for channel".to_string(),
                            ));
                        }
                    };

                    let count = match &items[i + 1] {
                        Frame::Integer(n) => *n,
                        _ => {
                            return Err(RedisError::Protocol(
                                "Expected integer for count".to_string(),
                            ));
                        }
                    };

                    results.push((channel, count));
                    i += 2;
                }

                Ok(results)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// Get the number of patterns subscribed to.
///
/// Returns the number of pattern subscriptions (those created with PSUBSCRIBE).
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::client::RedisConnection;
/// # use redis_tower::commands::pubsub::PubsubNumpat;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let count = client.execute(PubsubNumpat::new()).await?;
/// println!("Active pattern subscriptions: {}", count);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PubsubNumpat;

impl PubsubNumpat {
    /// Create a new PUBSUB NUMPAT command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for PubsubNumpat {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for PubsubNumpat {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from_static(b"PUBSUB"))),
            Frame::BulkString(Some(Bytes::from_static(b"NUMPAT"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_publish_frame() {
        let cmd = Publish::new("news", "hello");
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 3);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_publish_response() {
        let frame = Frame::Integer(5);
        let result = Publish::parse_response(frame).unwrap();
        assert_eq!(result, 5);
    }

    #[test]
    fn test_pubsub_numsub_frame() {
        let cmd = PubsubNumsub::new(&["news", "updates"]);
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 4); // PUBSUB NUMSUB news updates
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_pubsub_numsub_response() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("news"))),
            Frame::Integer(5),
            Frame::BulkString(Some(Bytes::from("updates"))),
            Frame::Integer(3),
        ]);

        let result = PubsubNumsub::parse_response(frame).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("news".to_string(), 5));
        assert_eq!(result[1], ("updates".to_string(), 3));
    }

    #[test]
    fn test_pubsub_numpat_frame() {
        let cmd = PubsubNumpat::new();
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 2); // PUBSUB NUMPAT
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_pubsub_numpat_response() {
        let frame = Frame::Integer(10);
        let result = PubsubNumpat::parse_response(frame).unwrap();
        assert_eq!(result, 10);
    }
}
