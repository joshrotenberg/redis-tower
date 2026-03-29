use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::SinkExt;
use redis_tower_core::{Frame, RedisConnection, RedisError};
use redis_tower_protocol::helpers::{array, bulk};
use tokio_stream::{Stream, StreamExt};
use tokio_util::codec::Framed;

use redis_tower_core::RedisStream;
use redis_tower_protocol::RespCodec;

/// A message received on a pub/sub channel.
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    /// The kind of message (channel or pattern).
    pub kind: MessageKind,
    /// The channel name this message was received on.
    pub channel: String,
    /// The pattern that matched (only for pattern subscriptions).
    pub pattern: Option<String>,
    /// The message payload.
    pub payload: Bytes,
}

/// The kind of pub/sub message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageKind {
    /// A message from a direct channel subscription.
    Message,
    /// A message from a pattern subscription.
    PMessage,
    /// A message from a sharded channel subscription.
    SMessage,
}

/// A Redis connection in pub/sub mode.
///
/// Consumes a [`RedisConnection`] and provides an async [`Stream`] of
/// [`PubSubMessage`] values. Once in pub/sub mode, the connection can
/// only subscribe/unsubscribe and receive messages.
///
/// # Example
///
/// ```ignore
/// use redis_tower::{PubSubConnection, RedisConnection};
/// use tokio_stream::StreamExt;
///
/// let conn = RedisConnection::connect("127.0.0.1:6379").await?;
/// let mut pubsub = PubSubConnection::from_connection(conn)?;
/// pubsub.subscribe(&["events"]).await?;
///
/// while let Some(msg) = pubsub.next().await {
///     let msg = msg?;
///     println!("{}: {:?}", msg.channel, msg.payload);
/// }
/// ```
pub struct PubSubConnection {
    framed: Framed<RedisStream, RespCodec>,
}

impl PubSubConnection {
    /// Convert a `RedisConnection` into a pub/sub connection.
    ///
    /// The connection must not be shared (no outstanding clones of the
    /// internal Arc). Use a fresh connection for pub/sub.
    pub fn from_connection(conn: RedisConnection) -> Result<Self, RedisError> {
        let framed = conn.into_framed()?;
        Ok(Self { framed })
    }

    /// Subscribe to one or more channels.
    pub async fn subscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        let mut args = vec![bulk("SUBSCRIBE")];
        for ch in channels {
            args.push(bulk(*ch));
        }
        self.framed
            .send(array(args))
            .await
            .map_err(RedisError::from)?;

        // Read and discard subscribe confirmation messages (one per channel).
        for _ in channels {
            let frame = self
                .framed
                .next()
                .await
                .ok_or(RedisError::ConnectionClosed)?
                .map_err(RedisError::from)?;
            Self::validate_sub_response(&frame, "subscribe")?;
        }

        Ok(())
    }

    /// Subscribe to one or more patterns.
    pub async fn psubscribe(&mut self, patterns: &[&str]) -> Result<(), RedisError> {
        let mut args = vec![bulk("PSUBSCRIBE")];
        for pat in patterns {
            args.push(bulk(*pat));
        }
        self.framed
            .send(array(args))
            .await
            .map_err(RedisError::from)?;

        for _ in patterns {
            let frame = self
                .framed
                .next()
                .await
                .ok_or(RedisError::ConnectionClosed)?
                .map_err(RedisError::from)?;
            Self::validate_sub_response(&frame, "psubscribe")?;
        }

        Ok(())
    }

    /// Unsubscribe from one or more channels.
    ///
    /// If `channels` is empty, unsubscribes from all channels.
    pub async fn unsubscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        let mut args = vec![bulk("UNSUBSCRIBE")];
        for ch in channels {
            args.push(bulk(*ch));
        }
        self.framed
            .send(array(args))
            .await
            .map_err(RedisError::from)?;

        // Read unsubscribe confirmations. If channels is empty, Redis sends
        // one confirmation per previously subscribed channel -- we read until
        // the subscription count reaches 0.
        if channels.is_empty() {
            loop {
                let frame = self
                    .framed
                    .next()
                    .await
                    .ok_or(RedisError::ConnectionClosed)?
                    .map_err(RedisError::from)?;
                if Self::is_unsub_complete(&frame) {
                    break;
                }
            }
        } else {
            for _ in channels {
                let _ = self
                    .framed
                    .next()
                    .await
                    .ok_or(RedisError::ConnectionClosed)?
                    .map_err(RedisError::from)?;
            }
        }

        Ok(())
    }

    /// Subscribe to one or more shard channels.
    pub async fn ssubscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        let mut args = vec![bulk("SSUBSCRIBE")];
        for ch in channels {
            args.push(bulk(*ch));
        }
        self.framed
            .send(array(args))
            .await
            .map_err(RedisError::from)?;

        for _ in channels {
            let frame = self
                .framed
                .next()
                .await
                .ok_or(RedisError::ConnectionClosed)?
                .map_err(RedisError::from)?;
            Self::validate_sub_response(&frame, "ssubscribe")?;
        }

        Ok(())
    }

    /// Unsubscribe from one or more shard channels.
    ///
    /// If `channels` is empty, unsubscribes from all shard channels.
    pub async fn sunsubscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        let mut args = vec![bulk("SUNSUBSCRIBE")];
        for ch in channels {
            args.push(bulk(*ch));
        }
        self.framed
            .send(array(args))
            .await
            .map_err(RedisError::from)?;

        if channels.is_empty() {
            loop {
                let frame = self
                    .framed
                    .next()
                    .await
                    .ok_or(RedisError::ConnectionClosed)?
                    .map_err(RedisError::from)?;
                if Self::is_unsub_complete(&frame) {
                    break;
                }
            }
        } else {
            for _ in channels {
                let _ = self
                    .framed
                    .next()
                    .await
                    .ok_or(RedisError::ConnectionClosed)?
                    .map_err(RedisError::from)?;
            }
        }

        Ok(())
    }

    /// Unsubscribe from one or more patterns.
    pub async fn punsubscribe(&mut self, patterns: &[&str]) -> Result<(), RedisError> {
        let mut args = vec![bulk("PUNSUBSCRIBE")];
        for pat in patterns {
            args.push(bulk(*pat));
        }
        self.framed
            .send(array(args))
            .await
            .map_err(RedisError::from)?;

        if patterns.is_empty() {
            loop {
                let frame = self
                    .framed
                    .next()
                    .await
                    .ok_or(RedisError::ConnectionClosed)?
                    .map_err(RedisError::from)?;
                if Self::is_unsub_complete(&frame) {
                    break;
                }
            }
        } else {
            for _ in patterns {
                let _ = self
                    .framed
                    .next()
                    .await
                    .ok_or(RedisError::ConnectionClosed)?
                    .map_err(RedisError::from)?;
            }
        }

        Ok(())
    }

    /// Validate a subscribe/psubscribe confirmation response.
    fn validate_sub_response(frame: &Frame, expected_kind: &str) -> Result<(), RedisError> {
        // RESP2: ["subscribe", channel, count] as Array
        // RESP3: same but may arrive as Push
        let items = match frame {
            Frame::Array(Some(items)) | Frame::Push(items) => items,
            Frame::Error(e) => {
                return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
            }
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "subscribe confirmation array",
                    actual: format!("{other:?}"),
                });
            }
        };

        if let Some(Frame::BulkString(Some(kind))) = items.first() {
            if kind.as_ref() == expected_kind.as_bytes() {
                return Ok(());
            }
        }

        Err(RedisError::UnexpectedResponse {
            expected: "subscribe confirmation",
            actual: format!("{frame:?}"),
        })
    }

    /// Check if an unsubscribe confirmation indicates zero remaining subscriptions.
    fn is_unsub_complete(frame: &Frame) -> bool {
        let items = match frame {
            Frame::Array(Some(items)) | Frame::Push(items) => items,
            _ => return false,
        };
        // Last element is the subscription count.
        matches!(items.last(), Some(Frame::Integer(0)))
    }

    /// Parse a pub/sub message frame.
    fn parse_message(frame: Frame) -> Result<Option<PubSubMessage>, RedisError> {
        let items = match frame {
            Frame::Array(Some(items)) | Frame::Push(items) => items,
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "pub/sub message array",
                    actual: format!("{other:?}"),
                });
            }
        };

        let kind_bytes = match items.first() {
            Some(Frame::BulkString(Some(b))) => b,
            _ => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "message type",
                    actual: format!("{items:?}"),
                });
            }
        };

        match kind_bytes.as_ref() {
            b"message" if items.len() == 3 => {
                let channel = Self::extract_string(&items[1])?;
                let payload = Self::extract_bytes(&items[2])?;
                Ok(Some(PubSubMessage {
                    kind: MessageKind::Message,
                    channel,
                    pattern: None,
                    payload,
                }))
            }
            b"pmessage" if items.len() == 4 => {
                let pattern = Self::extract_string(&items[1])?;
                let channel = Self::extract_string(&items[2])?;
                let payload = Self::extract_bytes(&items[3])?;
                Ok(Some(PubSubMessage {
                    kind: MessageKind::PMessage,
                    channel,
                    pattern: Some(pattern),
                    payload,
                }))
            }
            b"smessage" if items.len() == 3 => {
                let channel = Self::extract_string(&items[1])?;
                let payload = Self::extract_bytes(&items[2])?;
                Ok(Some(PubSubMessage {
                    kind: MessageKind::SMessage,
                    channel,
                    pattern: None,
                    payload,
                }))
            }
            // Subscribe/unsubscribe confirmations -- skip.
            b"subscribe" | b"unsubscribe" | b"psubscribe" | b"punsubscribe" | b"ssubscribe"
            | b"sunsubscribe" => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "message or pmessage",
                actual: format!("{}", String::from_utf8_lossy(other)),
            }),
        }
    }

    fn extract_string(frame: &Frame) -> Result<String, RedisError> {
        match frame {
            Frame::BulkString(Some(b)) => Ok(String::from_utf8_lossy(b).into_owned()),
            Frame::SimpleString(b) => Ok(String::from_utf8_lossy(b).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn extract_bytes(frame: &Frame) -> Result<Bytes, RedisError> {
        match frame {
            Frame::BulkString(Some(b)) => Ok(b.clone()),
            Frame::SimpleString(b) => Ok(b.clone()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bytes",
                actual: format!("{other:?}"),
            }),
        }
    }
}

impl Stream for PubSubConnection {
    type Item = Result<PubSubMessage, RedisError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let frame = match Pin::new(&mut self.framed).poll_next(cx) {
                Poll::Ready(Some(Ok(frame))) => frame,
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(RedisError::from(e))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            };

            match Self::parse_message(frame) {
                Ok(Some(msg)) => return Poll::Ready(Some(Ok(msg))),
                Ok(None) => continue, // skip confirmations
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
        }
    }
}
