//! Redis Pub/Sub support.
//!
//! Provides [`PubSubConnection`], which consumes a [`RedisConnection`] and
//! exposes an async [`Stream`] of [`PubSubMessage`] values.
//! Supports channel subscriptions, pattern subscriptions, and shard
//! subscriptions (Redis 7+).
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::{PubSubConnection, RedisConnection};
//! use tokio_stream::StreamExt;
//!
//! let conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let mut pubsub = PubSubConnection::from_connection(conn)?;
//! pubsub.subscribe(&["events"]).await?;
//!
//! while let Some(msg) = pubsub.next().await {
//!     let msg = msg?;
//!     println!("{}: {:?}", msg.channel, msg.payload);
//! }
//! ```

use std::collections::{HashSet, VecDeque};
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
    /// Buffer for frames read while searching for specific confirmations.
    /// This prevents confirmations from one subscribe call being silently
    /// consumed by another's confirmation loop.
    buffered_frames: VecDeque<Frame>,
}

impl PubSubConnection {
    /// Convert a `RedisConnection` into a pub/sub connection.
    ///
    /// The connection must not be shared (no outstanding clones of the
    /// internal Arc). Use a fresh connection for pub/sub.
    pub fn from_connection(conn: RedisConnection) -> Result<Self, RedisError> {
        let framed = conn.into_framed()?;
        Ok(Self {
            framed,
            buffered_frames: VecDeque::new(),
        })
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

        self.await_confirmations(channels, "subscribe").await
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

        self.await_confirmations(patterns, "psubscribe").await
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

        self.await_confirmations(channels, "ssubscribe").await
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

    /// Read the next frame, draining the buffer first.
    async fn next_frame(&mut self) -> Result<Frame, RedisError> {
        if let Some(frame) = self.buffered_frames.pop_front() {
            return Ok(frame);
        }
        self.framed
            .next()
            .await
            .ok_or(RedisError::ConnectionClosed)?
            .map_err(RedisError::from)
    }

    /// Wait for subscribe/psubscribe/ssubscribe confirmations, matching each
    /// confirmation's channel name against the expected set.
    ///
    /// Frames that are valid confirmations for the right `kind` but whose
    /// channel name does not match any expected channel are buffered so they
    /// can be consumed by a subsequent confirmation loop or the message stream.
    async fn await_confirmations(
        &mut self,
        names: &[&str],
        expected_kind: &str,
    ) -> Result<(), RedisError> {
        let mut pending: HashSet<&str> = names.iter().copied().collect();

        while !pending.is_empty() {
            let frame = self.next_frame().await?;

            match Self::extract_confirmation_channel(&frame, expected_kind) {
                Some(Ok(channel)) => {
                    if pending.remove(channel.as_str()) {
                        // Matched an expected channel -- continue.
                        continue;
                    }
                    // Confirmation for a channel we did not request in this call.
                    // Buffer it so the caller that IS waiting for it can consume it.
                    self.buffered_frames.push_back(frame);
                }
                Some(Err(e)) => return Err(e),
                None => {
                    // Not a confirmation of the expected kind at all. Buffer it.
                    self.buffered_frames.push_back(frame);
                }
            }
        }

        Ok(())
    }

    /// Try to extract the channel name from a subscribe confirmation frame.
    ///
    /// Returns `Some(Ok(channel))` if the frame is a confirmation of the
    /// expected kind, `Some(Err(_))` if the frame is an error, or `None`
    /// if it is not a confirmation of the expected kind.
    fn extract_confirmation_channel(
        frame: &Frame,
        expected_kind: &str,
    ) -> Option<Result<String, RedisError>> {
        let items = match frame {
            Frame::Array(Some(items)) | Frame::Push(items) => items,
            Frame::Error(e) => {
                return Some(Err(RedisError::Redis(
                    String::from_utf8_lossy(e).into_owned(),
                )));
            }
            _ => return None,
        };

        // items[0] = kind, items[1] = channel, items[2] = subscription count
        if items.len() < 3 {
            return None;
        }

        let kind = match &items[0] {
            Frame::BulkString(Some(b)) => b,
            _ => return None,
        };

        if kind.as_ref() != expected_kind.as_bytes() {
            return None;
        }

        // Extract channel name from items[1].
        match &items[1] {
            Frame::BulkString(Some(b)) => Some(Ok(String::from_utf8_lossy(b).into_owned())),
            Frame::SimpleString(b) => Some(Ok(String::from_utf8_lossy(b).into_owned())),
            _ => None,
        }
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
            // Drain any buffered frames before reading from the transport.
            let frame = if let Some(frame) = self.buffered_frames.pop_front() {
                frame
            } else {
                match Pin::new(&mut self.framed).poll_next(cx) {
                    Poll::Ready(Some(Ok(frame))) => frame,
                    Poll::Ready(Some(Err(e))) => {
                        return Poll::Ready(Some(Err(RedisError::from(e))));
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                }
            };

            match Self::parse_message(frame) {
                Ok(Some(msg)) => return Poll::Ready(Some(Ok(msg))),
                Ok(None) => continue, // skip confirmations
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_protocol::helpers::{array, bulk};

    /// Helper to build a subscribe confirmation frame.
    fn sub_confirmation(kind: &str, channel: &str, count: i64) -> Frame {
        array(vec![bulk(kind), bulk(channel), Frame::Integer(count)])
    }

    #[test]
    fn extract_confirmation_channel_matches_expected_kind() {
        let frame = sub_confirmation("subscribe", "events", 1);
        let result = PubSubConnection::extract_confirmation_channel(&frame, "subscribe");
        assert!(result.is_some());
        assert_eq!(result.unwrap().unwrap(), "events");
    }

    #[test]
    fn extract_confirmation_channel_returns_none_for_wrong_kind() {
        let frame = sub_confirmation("psubscribe", "events.*", 1);
        let result = PubSubConnection::extract_confirmation_channel(&frame, "subscribe");
        assert!(result.is_none());
    }

    #[test]
    fn extract_confirmation_channel_returns_err_for_error_frame() {
        let frame = Frame::Error(b"ERR something"[..].into());
        let result = PubSubConnection::extract_confirmation_channel(&frame, "subscribe");
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn extract_confirmation_channel_returns_none_for_message_frame() {
        let frame = array(vec![bulk("message"), bulk("events"), bulk("hello")]);
        let result = PubSubConnection::extract_confirmation_channel(&frame, "subscribe");
        assert!(result.is_none());
    }

    #[test]
    fn extract_confirmation_channel_returns_none_for_short_array() {
        let frame = array(vec![bulk("subscribe")]);
        let result = PubSubConnection::extract_confirmation_channel(&frame, "subscribe");
        assert!(result.is_none());
    }

    #[test]
    fn parse_message_returns_channel_message() {
        let frame = array(vec![bulk("message"), bulk("events"), bulk("payload")]);
        let msg = PubSubConnection::parse_message(frame).unwrap().unwrap();
        assert_eq!(msg.kind, MessageKind::Message);
        assert_eq!(msg.channel, "events");
        assert_eq!(msg.payload.as_ref(), b"payload");
        assert!(msg.pattern.is_none());
    }

    #[test]
    fn parse_message_returns_pmessage() {
        let frame = array(vec![
            bulk("pmessage"),
            bulk("ev*"),
            bulk("events"),
            bulk("data"),
        ]);
        let msg = PubSubConnection::parse_message(frame).unwrap().unwrap();
        assert_eq!(msg.kind, MessageKind::PMessage);
        assert_eq!(msg.channel, "events");
        assert_eq!(msg.pattern, Some("ev*".to_string()));
    }

    #[test]
    fn parse_message_skips_subscribe_confirmation() {
        let frame = sub_confirmation("subscribe", "ch1", 1);
        assert!(PubSubConnection::parse_message(frame).unwrap().is_none());
    }

    #[test]
    fn is_unsub_complete_detects_zero_count() {
        let frame = array(vec![bulk("unsubscribe"), bulk("ch1"), Frame::Integer(0)]);
        assert!(PubSubConnection::is_unsub_complete(&frame));
    }

    #[test]
    fn is_unsub_complete_returns_false_for_nonzero_count() {
        let frame = array(vec![bulk("unsubscribe"), bulk("ch1"), Frame::Integer(2)]);
        assert!(!PubSubConnection::is_unsub_complete(&frame));
    }
}
