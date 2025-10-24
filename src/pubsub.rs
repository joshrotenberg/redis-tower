//! Redis Pub/Sub support with async streaming.
//!
//! Pub/Sub in Redis is fundamentally different from regular commands:
//! - Once subscribed, the connection enters "Pub/Sub mode" and can only use Pub/Sub commands
//! - Messages arrive asynchronously as a stream
//! - Patterns support wildcard subscriptions
//!
//! # Example
//!
//! ```rust,no_run
//! use redis_tower::{PubSubConnection, PubSubMessage};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut pubsub = PubSubConnection::connect("127.0.0.1:6379").await?;
//!
//! // Subscribe to channels
//! pubsub.subscribe(&["news", "updates"]).await?;
//!
//! // Subscribe with patterns
//! pubsub.psubscribe(&["events:*"]).await?;
//!
//! // Receive messages as a stream
//! while let Some(msg) = pubsub.next_message().await {
//!     match msg? {
//!         PubSubMessage::Message { channel, payload } => {
//!             println!("Message on {}: {:?}", channel, payload);
//!         }
//!         PubSubMessage::PMessage { pattern, channel, payload } => {
//!             println!("Pattern {} matched {}: {:?}", pattern, channel, payload);
//!         }
//!         PubSubMessage::Subscribe { channel, count } => {
//!             println!("Subscribed to {} (total: {})", channel, count);
//!         }
//!         PubSubMessage::Unsubscribe { channel, count } => {
//!             println!("Unsubscribed from {} (total: {})", channel, count);
//!         }
//!         _ => {}
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use crate::codec::{Frame, RespCodec};
use crate::types::RedisError;
use bytes::Bytes;
use futures::SinkExt;
use std::collections::HashSet;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

/// A Redis connection in Pub/Sub mode.
///
/// Once created and subscribed to channels, this connection can only be used for Pub/Sub
/// operations. Regular Redis commands cannot be executed on this connection.
pub struct PubSubConnection {
    framed: Framed<TcpStream, RespCodec>,
    subscribed_channels: HashSet<String>,
    subscribed_patterns: HashSet<String>,
}

/// Messages received from Redis Pub/Sub.
#[derive(Debug, Clone, PartialEq)]
pub enum PubSubMessage {
    /// A message published to a channel.
    Message {
        /// The channel the message was published to.
        channel: String,
        /// The message payload.
        payload: Bytes,
    },
    /// A message published to a channel matching a pattern subscription.
    PMessage {
        /// The pattern that matched.
        pattern: String,
        /// The actual channel the message was published to.
        channel: String,
        /// The message payload.
        payload: Bytes,
    },
    /// Confirmation of a channel subscription.
    Subscribe {
        /// The channel subscribed to.
        channel: String,
        /// Total number of channels currently subscribed to.
        count: i64,
    },
    /// Confirmation of a pattern subscription.
    PSubscribe {
        /// The pattern subscribed to.
        pattern: String,
        /// Total number of patterns currently subscribed to.
        count: i64,
    },
    /// Confirmation of a channel unsubscription.
    Unsubscribe {
        /// The channel unsubscribed from.
        channel: String,
        /// Remaining number of channels subscribed to.
        count: i64,
    },
    /// Confirmation of a pattern unsubscription.
    PUnsubscribe {
        /// The pattern unsubscribed from.
        pattern: String,
        /// Remaining number of patterns subscribed to.
        count: i64,
    },
}

impl PubSubConnection {
    /// Connect to Redis for Pub/Sub operations.
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        let stream = TcpStream::connect(addr)
            .await
            .map_err(RedisError::Connection)?;

        let codec = RespCodec::new();
        let framed = Framed::new(stream, codec);

        Ok(Self {
            framed,
            subscribed_channels: HashSet::new(),
            subscribed_patterns: HashSet::new(),
        })
    }

    /// Subscribe to one or more channels.
    ///
    /// After subscribing, the connection enters Pub/Sub mode and can only execute
    /// Pub/Sub commands (SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE).
    pub async fn subscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        if channels.is_empty() {
            return Ok(());
        }

        let mut args = vec![Frame::BulkString(Some(Bytes::from_static(b"SUBSCRIBE")))];
        for channel in channels {
            args.push(Frame::BulkString(Some(Bytes::from(channel.to_string()))));
            self.subscribed_channels.insert(channel.to_string());
        }

        self.framed
            .send(Frame::Array(args))
            .await
            .map_err(RedisError::Connection)?;

        Ok(())
    }

    /// Unsubscribe from one or more channels.
    ///
    /// If no channels are specified, unsubscribes from all channels.
    pub async fn unsubscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        let mut args = vec![Frame::BulkString(Some(Bytes::from_static(b"UNSUBSCRIBE")))];

        if channels.is_empty() {
            // Unsubscribe from all
            self.subscribed_channels.clear();
        } else {
            for channel in channels {
                args.push(Frame::BulkString(Some(Bytes::from(channel.to_string()))));
                self.subscribed_channels.remove(*channel);
            }
        }

        self.framed
            .send(Frame::Array(args))
            .await
            .map_err(RedisError::Connection)?;

        Ok(())
    }

    /// Subscribe to one or more channel patterns.
    ///
    /// Patterns support glob-style wildcards:
    /// - `*` matches any sequence of characters
    /// - `?` matches a single character
    /// - `[abc]` matches any character in the set
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use redis_tower::pubsub::PubSubConnection;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut pubsub = PubSubConnection::connect("127.0.0.1:6379").await?;
    /// pubsub.psubscribe(&["events:*", "logs:error:*"]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn psubscribe(&mut self, patterns: &[&str]) -> Result<(), RedisError> {
        if patterns.is_empty() {
            return Ok(());
        }

        let mut args = vec![Frame::BulkString(Some(Bytes::from_static(b"PSUBSCRIBE")))];
        for pattern in patterns {
            args.push(Frame::BulkString(Some(Bytes::from(pattern.to_string()))));
            self.subscribed_patterns.insert(pattern.to_string());
        }

        self.framed
            .send(Frame::Array(args))
            .await
            .map_err(RedisError::Connection)?;

        Ok(())
    }

    /// Unsubscribe from one or more channel patterns.
    ///
    /// If no patterns are specified, unsubscribes from all patterns.
    pub async fn punsubscribe(&mut self, patterns: &[&str]) -> Result<(), RedisError> {
        let mut args = vec![Frame::BulkString(Some(Bytes::from_static(b"PUNSUBSCRIBE")))];

        if patterns.is_empty() {
            // Unsubscribe from all patterns
            self.subscribed_patterns.clear();
        } else {
            for pattern in patterns {
                args.push(Frame::BulkString(Some(Bytes::from(pattern.to_string()))));
                self.subscribed_patterns.remove(*pattern);
            }
        }

        self.framed
            .send(Frame::Array(args))
            .await
            .map_err(RedisError::Connection)?;

        Ok(())
    }

    /// Get the next message from subscribed channels/patterns.
    ///
    /// This is an async method that waits for the next message to arrive.
    /// Returns `None` when the connection is closed.
    pub async fn next_message(&mut self) -> Option<Result<PubSubMessage, RedisError>> {
        match self.framed.next().await {
            Some(Ok(frame)) => Some(Self::parse_message(frame)),
            Some(Err(e)) => Some(Err(RedisError::Connection(e))),
            None => None,
        }
    }

    /// Parse a Redis frame into a PubSubMessage.
    fn parse_message(frame: Frame) -> Result<PubSubMessage, RedisError> {
        match frame {
            Frame::Array(items) if items.len() >= 3 => {
                let msg_type = match &items[0] {
                    Frame::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
                    _ => {
                        return Err(RedisError::Protocol(
                            "Expected bulk string for message type".to_string(),
                        ));
                    }
                };

                match msg_type.as_str() {
                    "message" => {
                        // ["message", channel, payload]
                        if items.len() != 3 {
                            return Err(RedisError::Protocol("Invalid message format".to_string()));
                        }

                        let channel = match &items[1] {
                            Frame::BulkString(Some(bytes)) => {
                                String::from_utf8_lossy(bytes).to_string()
                            }
                            _ => {
                                return Err(RedisError::Protocol(
                                    "Expected bulk string for channel".to_string(),
                                ));
                            }
                        };

                        let payload = match &items[2] {
                            Frame::BulkString(Some(bytes)) => bytes.clone(),
                            _ => {
                                return Err(RedisError::Protocol(
                                    "Expected bulk string for payload".to_string(),
                                ));
                            }
                        };

                        Ok(PubSubMessage::Message { channel, payload })
                    }
                    "pmessage" => {
                        // ["pmessage", pattern, channel, payload]
                        if items.len() != 4 {
                            return Err(RedisError::Protocol(
                                "Invalid pmessage format".to_string(),
                            ));
                        }

                        let pattern = match &items[1] {
                            Frame::BulkString(Some(bytes)) => {
                                String::from_utf8_lossy(bytes).to_string()
                            }
                            _ => {
                                return Err(RedisError::Protocol(
                                    "Expected bulk string for pattern".to_string(),
                                ));
                            }
                        };

                        let channel = match &items[2] {
                            Frame::BulkString(Some(bytes)) => {
                                String::from_utf8_lossy(bytes).to_string()
                            }
                            _ => {
                                return Err(RedisError::Protocol(
                                    "Expected bulk string for channel".to_string(),
                                ));
                            }
                        };

                        let payload = match &items[3] {
                            Frame::BulkString(Some(bytes)) => bytes.clone(),
                            _ => {
                                return Err(RedisError::Protocol(
                                    "Expected bulk string for payload".to_string(),
                                ));
                            }
                        };

                        Ok(PubSubMessage::PMessage {
                            pattern,
                            channel,
                            payload,
                        })
                    }
                    "subscribe" => {
                        let channel = match &items[1] {
                            Frame::BulkString(Some(bytes)) => {
                                String::from_utf8_lossy(bytes).to_string()
                            }
                            _ => {
                                return Err(RedisError::Protocol(
                                    "Expected bulk string for channel".to_string(),
                                ));
                            }
                        };

                        let count = match &items[2] {
                            Frame::Integer(n) => *n,
                            _ => {
                                return Err(RedisError::Protocol(
                                    "Expected integer for count".to_string(),
                                ));
                            }
                        };

                        Ok(PubSubMessage::Subscribe { channel, count })
                    }
                    "psubscribe" => {
                        let pattern = match &items[1] {
                            Frame::BulkString(Some(bytes)) => {
                                String::from_utf8_lossy(bytes).to_string()
                            }
                            _ => {
                                return Err(RedisError::Protocol(
                                    "Expected bulk string for pattern".to_string(),
                                ));
                            }
                        };

                        let count = match &items[2] {
                            Frame::Integer(n) => *n,
                            _ => {
                                return Err(RedisError::Protocol(
                                    "Expected integer for count".to_string(),
                                ));
                            }
                        };

                        Ok(PubSubMessage::PSubscribe { pattern, count })
                    }
                    "unsubscribe" => {
                        let channel = match &items[1] {
                            Frame::BulkString(Some(bytes)) => {
                                String::from_utf8_lossy(bytes).to_string()
                            }
                            _ => {
                                return Err(RedisError::Protocol(
                                    "Expected bulk string for channel".to_string(),
                                ));
                            }
                        };

                        let count = match &items[2] {
                            Frame::Integer(n) => *n,
                            _ => {
                                return Err(RedisError::Protocol(
                                    "Expected integer for count".to_string(),
                                ));
                            }
                        };

                        Ok(PubSubMessage::Unsubscribe { channel, count })
                    }
                    "punsubscribe" => {
                        let pattern = match &items[1] {
                            Frame::BulkString(Some(bytes)) => {
                                String::from_utf8_lossy(bytes).to_string()
                            }
                            _ => {
                                return Err(RedisError::Protocol(
                                    "Expected bulk string for pattern".to_string(),
                                ));
                            }
                        };

                        let count = match &items[2] {
                            Frame::Integer(n) => *n,
                            _ => {
                                return Err(RedisError::Protocol(
                                    "Expected integer for count".to_string(),
                                ));
                            }
                        };

                        Ok(PubSubMessage::PUnsubscribe { pattern, count })
                    }
                    _ => Err(RedisError::Protocol(format!(
                        "Unknown message type: {}",
                        msg_type
                    ))),
                }
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol(
                "Expected array for Pub/Sub message".to_string(),
            )),
        }
    }

    /// Get the number of channels currently subscribed to.
    pub fn channel_count(&self) -> usize {
        self.subscribed_channels.len()
    }

    /// Get the number of patterns currently subscribed to.
    pub fn pattern_count(&self) -> usize {
        self.subscribed_patterns.len()
    }

    /// Check if subscribed to any channels or patterns.
    pub fn is_subscribed(&self) -> bool {
        !self.subscribed_channels.is_empty() || !self.subscribed_patterns.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_message() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("message"))),
            Frame::BulkString(Some(Bytes::from("news"))),
            Frame::BulkString(Some(Bytes::from("hello world"))),
        ]);

        let msg = PubSubConnection::parse_message(frame).unwrap();
        assert_eq!(
            msg,
            PubSubMessage::Message {
                channel: "news".to_string(),
                payload: Bytes::from("hello world"),
            }
        );
    }

    #[test]
    fn test_parse_pmessage() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("pmessage"))),
            Frame::BulkString(Some(Bytes::from("events:*"))),
            Frame::BulkString(Some(Bytes::from("events:login"))),
            Frame::BulkString(Some(Bytes::from("user123"))),
        ]);

        let msg = PubSubConnection::parse_message(frame).unwrap();
        assert_eq!(
            msg,
            PubSubMessage::PMessage {
                pattern: "events:*".to_string(),
                channel: "events:login".to_string(),
                payload: Bytes::from("user123"),
            }
        );
    }

    #[test]
    fn test_parse_subscribe() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("subscribe"))),
            Frame::BulkString(Some(Bytes::from("news"))),
            Frame::Integer(1),
        ]);

        let msg = PubSubConnection::parse_message(frame).unwrap();
        assert_eq!(
            msg,
            PubSubMessage::Subscribe {
                channel: "news".to_string(),
                count: 1,
            }
        );
    }

    #[test]
    fn test_parse_psubscribe() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("psubscribe"))),
            Frame::BulkString(Some(Bytes::from("events:*"))),
            Frame::Integer(2),
        ]);

        let msg = PubSubConnection::parse_message(frame).unwrap();
        assert_eq!(
            msg,
            PubSubMessage::PSubscribe {
                pattern: "events:*".to_string(),
                count: 2,
            }
        );
    }

    #[test]
    fn test_parse_unsubscribe() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("unsubscribe"))),
            Frame::BulkString(Some(Bytes::from("news"))),
            Frame::Integer(0),
        ]);

        let msg = PubSubConnection::parse_message(frame).unwrap();
        assert_eq!(
            msg,
            PubSubMessage::Unsubscribe {
                channel: "news".to_string(),
                count: 0,
            }
        );
    }
}
