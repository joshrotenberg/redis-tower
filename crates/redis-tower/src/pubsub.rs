//! Redis Pub/Sub support.
//!
//! Provides [`PubSubConnection`], which consumes a [`RedisConnection`] and
//! exposes an async [`Stream`] of [`PubSubMessage`] values.
//! Supports channel subscriptions, pattern subscriptions, and shard
//! subscriptions (Redis 7+).
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
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
//! # Ok(())
//! # }
//! ```

use std::collections::{BTreeSet, HashSet, VecDeque};
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

use crate::reconnect::ConnectionFactory;

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

/// Which family a keyspace notification belongs to.
///
/// Redis publishes every key event twice when `notify-keyspace-events` is
/// configured with both `K` and `E`: once on the keyspace channel and once on
/// the keyevent channel. They carry the same `(key, event)` pair but differ in
/// how it is split between the channel name and the payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationKind {
    /// A `__keyspace@<db>__:<key>` notification: the channel names the key and
    /// the payload is the event name.
    Keyspace,
    /// A `__keyevent@<db>__:<event>` notification: the channel names the event
    /// and the payload is the key.
    Keyevent,
}

/// A parsed Redis keyspace/keyevent notification.
///
/// Redis publishes keyspace notifications on channels of the form
/// `__keyspace@<db>__:<key>` (payload is the event name) and
/// `__keyevent@<db>__:<event>` (payload is the key), gated on the server's
/// `notify-keyspace-events` config. [`KeyspaceEvent`] normalizes both forms
/// into the same `(db, key, event)` triple, recording which channel family it
/// came from in [`kind`](Self::kind).
///
/// Build one from a received [`PubSubMessage`] with
/// [`from_message`](Self::from_message), or stream them directly with
/// [`PubSubConnection::into_keyspace_events`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyspaceEvent {
    /// Which channel family this notification arrived on.
    pub kind: NotificationKind,
    /// The logical database number from the channel (`@<db>`).
    pub db: u32,
    /// The key the event happened to.
    pub key: String,
    /// The event name (for example `set`, `del`, `expired`).
    pub event: String,
}

impl KeyspaceEvent {
    /// Parse a keyspace/keyevent notification channel into its
    /// `(kind, db, tail)` parts, where `tail` is the key (for keyspace) or the
    /// event (for keyevent). Returns `None` if `channel` is not a keyspace or
    /// keyevent channel.
    fn parse_channel(channel: &str) -> Option<(NotificationKind, u32, &str)> {
        for (prefix, kind) in [
            ("__keyspace@", NotificationKind::Keyspace),
            ("__keyevent@", NotificationKind::Keyevent),
        ] {
            if let Some(rest) = channel.strip_prefix(prefix)
                && let Some((db_str, tail)) = rest.split_once("__:")
                && let Ok(db) = db_str.parse::<u32>()
            {
                return Some((kind, db, tail));
            }
        }
        None
    }

    /// Parse a [`KeyspaceEvent`] from a received [`PubSubMessage`].
    ///
    /// Returns `None` when the message's channel is not a keyspace or keyevent
    /// channel, so it can be used to filter a mixed [`PubSubConnection`] stream.
    /// The payload is decoded with [`String::from_utf8_lossy`].
    pub fn from_message(msg: &PubSubMessage) -> Option<KeyspaceEvent> {
        let (kind, db, tail) = Self::parse_channel(&msg.channel)?;
        let payload = String::from_utf8_lossy(&msg.payload).into_owned();
        let (key, event) = match kind {
            NotificationKind::Keyspace => (tail.to_string(), payload),
            NotificationKind::Keyevent => (payload, tail.to_string()),
        };
        Some(KeyspaceEvent {
            kind,
            db,
            key,
            event,
        })
    }
}

/// The subscriptions a [`PubSubConnection`] is tracking, so they can be
/// replayed after a reconnect.
///
/// Redis drops every subscription when the connection is lost, so a pub/sub
/// consumer that reconnects must re-issue them or it silently stops receiving
/// messages. [`PubSubConnection`] records each confirmed subscription here and
/// replays them via [`PubSubConnection::resubscribe`] and
/// [`PubSubConnection::reconnect_with`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Subscriptions {
    /// Channels from `SUBSCRIBE`.
    pub channels: BTreeSet<String>,
    /// Patterns from `PSUBSCRIBE`.
    pub patterns: BTreeSet<String>,
    /// Shard channels from `SSUBSCRIBE`.
    pub shard_channels: BTreeSet<String>,
}

impl Subscriptions {
    /// True when nothing is subscribed.
    pub fn is_empty(&self) -> bool {
        self.channels.is_empty() && self.patterns.is_empty() && self.shard_channels.is_empty()
    }

    /// The command frames that re-establish every tracked subscription: one
    /// each of `SUBSCRIBE` / `PSUBSCRIBE` / `SSUBSCRIBE` for the non-empty
    /// sets, in that order.
    pub fn replay_frames(&self) -> Vec<Frame> {
        let mut frames = Vec::new();
        for (cmd, set) in [
            ("SUBSCRIBE", &self.channels),
            ("PSUBSCRIBE", &self.patterns),
            ("SSUBSCRIBE", &self.shard_channels),
        ] {
            if !set.is_empty() {
                let mut args = vec![bulk(cmd)];
                args.extend(set.iter().map(|s| bulk(s.as_str())));
                frames.push(array(args));
            }
        }
        frames
    }

    /// Add subscription names (channels/patterns/shard channels) to `set`.
    fn add(set: &mut BTreeSet<String>, names: &[&str]) {
        set.extend(names.iter().map(|n| n.to_string()));
    }

    /// Remove subscription names from `set`. An empty `names` clears the whole
    /// set, mirroring Redis `UNSUBSCRIBE`/`PUNSUBSCRIBE`/`SUNSUBSCRIBE` with no
    /// arguments (unsubscribe from everything of that kind).
    fn remove(set: &mut BTreeSet<String>, names: &[&str]) {
        if names.is_empty() {
            set.clear();
        } else {
            for n in names {
                set.remove(*n);
            }
        }
    }
}

/// A Redis connection in pub/sub mode.
///
/// Consumes a [`RedisConnection`] and provides an async [`Stream`] of
/// [`PubSubMessage`] values. Once in pub/sub mode, the connection can
/// only subscribe/unsubscribe and receive messages.
///
/// Active subscriptions are tracked and can be replayed after a connection
/// drop via [`reconnect_with`](Self::reconnect_with), so a blip does not
/// silently end message delivery.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
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
/// # Ok(())
/// # }
/// ```
pub struct PubSubConnection {
    framed: Framed<RedisStream, RespCodec>,
    /// Buffer for frames read while searching for specific confirmations.
    /// This prevents confirmations from one subscribe call being silently
    /// consumed by another's confirmation loop.
    buffered_frames: VecDeque<Frame>,
    /// Active subscriptions, tracked so they can be replayed after a reconnect.
    subs: Subscriptions,
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
            subs: Subscriptions::default(),
        })
    }

    /// Send a subscribe-family command and await its confirmations, without
    /// touching the tracked set.
    async fn send_subscribe(
        &mut self,
        cmd: &str,
        names: &[&str],
        kind: &str,
    ) -> Result<(), RedisError> {
        let mut args = vec![bulk(cmd)];
        for n in names {
            args.push(bulk(*n));
        }
        self.framed
            .send(array(args))
            .await
            .map_err(RedisError::from)?;
        self.await_confirmations(names, kind).await
    }

    /// Subscribe to one or more channels.
    pub async fn subscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        self.send_subscribe("SUBSCRIBE", channels, "subscribe")
            .await?;
        Subscriptions::add(&mut self.subs.channels, channels);
        Ok(())
    }

    /// Subscribe to one or more patterns.
    pub async fn psubscribe(&mut self, patterns: &[&str]) -> Result<(), RedisError> {
        self.send_subscribe("PSUBSCRIBE", patterns, "psubscribe")
            .await?;
        Subscriptions::add(&mut self.subs.patterns, patterns);
        Ok(())
    }

    /// Subscribe to keyspace notifications for `db`, matching keys against
    /// `key_pattern` (a glob, for example `*` or `user:*`).
    ///
    /// This pattern-subscribes to `__keyspace@<db>__:<key_pattern>`; received
    /// messages carry the event name as the payload. The server must have
    /// keyspace notifications enabled (`notify-keyspace-events` must include
    /// `K` plus the relevant class flags). Decode messages with
    /// [`KeyspaceEvent::from_message`], or convert the connection with
    /// [`into_keyspace_events`](Self::into_keyspace_events).
    pub async fn psubscribe_keyspace(
        &mut self,
        db: u32,
        key_pattern: &str,
    ) -> Result<(), RedisError> {
        let pattern = format!("__keyspace@{db}__:{key_pattern}");
        self.psubscribe(&[pattern.as_str()]).await
    }

    /// Subscribe to keyevent notifications for `db`, matching events against
    /// `event_pattern` (a glob, for example `*` or `expired`).
    ///
    /// This pattern-subscribes to `__keyevent@<db>__:<event_pattern>`; received
    /// messages carry the affected key as the payload. The server must have
    /// keyspace notifications enabled (`notify-keyspace-events` must include
    /// `E` plus the relevant class flags). Decode messages with
    /// [`KeyspaceEvent::from_message`], or convert the connection with
    /// [`into_keyspace_events`](Self::into_keyspace_events).
    pub async fn psubscribe_keyevent(
        &mut self,
        db: u32,
        event_pattern: &str,
    ) -> Result<(), RedisError> {
        let pattern = format!("__keyevent@{db}__:{event_pattern}");
        self.psubscribe(&[pattern.as_str()]).await
    }

    /// Consume this connection and yield a [`Stream`] of typed
    /// [`KeyspaceEvent`] values instead of raw [`PubSubMessage`]s.
    ///
    /// Messages whose channel is not a keyspace/keyevent channel are skipped,
    /// so it is safe to use even if other subscriptions are active. Subscribe
    /// to the relevant channels first with
    /// [`psubscribe_keyspace`](Self::psubscribe_keyspace) or
    /// [`psubscribe_keyevent`](Self::psubscribe_keyevent).
    pub fn into_keyspace_events(self) -> KeyspaceEventStream {
        KeyspaceEventStream { inner: self }
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

        Subscriptions::remove(&mut self.subs.channels, channels);
        Ok(())
    }

    /// Subscribe to one or more shard channels.
    pub async fn ssubscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        self.send_subscribe("SSUBSCRIBE", channels, "ssubscribe")
            .await?;
        Subscriptions::add(&mut self.subs.shard_channels, channels);
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

        Subscriptions::remove(&mut self.subs.shard_channels, channels);
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

        Subscriptions::remove(&mut self.subs.patterns, patterns);
        Ok(())
    }

    /// The subscriptions currently tracked on this connection.
    ///
    /// These are replayed by [`resubscribe`](Self::resubscribe) and
    /// [`reconnect_with`](Self::reconnect_with).
    pub fn subscriptions(&self) -> &Subscriptions {
        &self.subs
    }

    /// Re-issue every tracked subscription over the current connection.
    ///
    /// Redis drops all subscriptions on disconnect, so call this after
    /// replacing the underlying connection to restore message delivery. It is
    /// a no-op when nothing is subscribed. The tracked set is unchanged.
    pub async fn resubscribe(&mut self) -> Result<(), RedisError> {
        // Snapshot to release the borrow on `self.subs` before sending.
        let channels: Vec<String> = self.subs.channels.iter().cloned().collect();
        let patterns: Vec<String> = self.subs.patterns.iter().cloned().collect();
        let shard: Vec<String> = self.subs.shard_channels.iter().cloned().collect();

        if !channels.is_empty() {
            let refs: Vec<&str> = channels.iter().map(String::as_str).collect();
            self.send_subscribe("SUBSCRIBE", &refs, "subscribe").await?;
        }
        if !patterns.is_empty() {
            let refs: Vec<&str> = patterns.iter().map(String::as_str).collect();
            self.send_subscribe("PSUBSCRIBE", &refs, "psubscribe")
                .await?;
        }
        if !shard.is_empty() {
            let refs: Vec<&str> = shard.iter().map(String::as_str).collect();
            self.send_subscribe("SSUBSCRIBE", &refs, "ssubscribe")
                .await?;
        }
        Ok(())
    }

    /// Rebuild the underlying connection from `factory` and replay all tracked
    /// subscriptions.
    ///
    /// Use this when the pub/sub stream reports a connection error: instead of
    /// silently going quiet, the connection is re-established and every
    /// subscription is restored, so message delivery resumes.
    pub async fn reconnect_with(
        &mut self,
        factory: &dyn ConnectionFactory,
    ) -> Result<(), RedisError> {
        let conn = factory.connect().await?;
        self.framed = conn.into_framed()?;
        self.buffered_frames.clear();
        self.resubscribe().await
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

/// A [`Stream`] of typed [`KeyspaceEvent`] values, layered over a
/// [`PubSubConnection`].
///
/// Created by [`PubSubConnection::into_keyspace_events`]. Messages received on
/// channels that are not keyspace/keyevent channels are silently skipped;
/// transport errors are surfaced as `Err` items.
pub struct KeyspaceEventStream {
    inner: PubSubConnection,
}

impl KeyspaceEventStream {
    /// Borrow the underlying [`PubSubConnection`], for example to inspect
    /// [`subscriptions`](PubSubConnection::subscriptions).
    pub fn get_ref(&self) -> &PubSubConnection {
        &self.inner
    }

    /// Mutably borrow the underlying [`PubSubConnection`], for example to add
    /// or remove subscriptions while streaming.
    pub fn get_mut(&mut self) -> &mut PubSubConnection {
        &mut self.inner
    }

    /// Recover the underlying [`PubSubConnection`].
    pub fn into_inner(self) -> PubSubConnection {
        self.inner
    }
}

impl Stream for KeyspaceEventStream {
    type Item = Result<KeyspaceEvent, RedisError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match Pin::new(&mut self.inner).poll_next(cx) {
                Poll::Ready(Some(Ok(msg))) => match KeyspaceEvent::from_message(&msg) {
                    Some(event) => return Poll::Ready(Some(Ok(event))),
                    None => continue, // not a keyspace notification -- skip
                },
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
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

    // -- subscription tracking (replayed on reconnect) --

    #[test]
    fn subscriptions_add_accumulates_each_kind() {
        let mut subs = Subscriptions::default();
        assert!(subs.is_empty());
        Subscriptions::add(&mut subs.channels, &["a", "b"]);
        Subscriptions::add(&mut subs.patterns, &["p.*"]);
        Subscriptions::add(&mut subs.shard_channels, &["s"]);
        Subscriptions::add(&mut subs.channels, &["b", "c"]); // dedups b
        assert!(!subs.is_empty());
        assert_eq!(
            subs.channels.iter().cloned().collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        assert_eq!(subs.patterns.len(), 1);
        assert_eq!(subs.shard_channels.len(), 1);
    }

    #[test]
    fn subscriptions_remove_named_leaves_the_rest() {
        let mut subs = Subscriptions::default();
        Subscriptions::add(&mut subs.channels, &["a", "b", "c"]);
        Subscriptions::remove(&mut subs.channels, &["b"]);
        assert_eq!(
            subs.channels.iter().cloned().collect::<Vec<_>>(),
            vec!["a", "c"]
        );
    }

    #[test]
    fn subscriptions_remove_empty_clears_that_kind_only() {
        // UNSUBSCRIBE with no args clears all channels but not patterns/shards.
        let mut subs = Subscriptions::default();
        Subscriptions::add(&mut subs.channels, &["a", "b"]);
        Subscriptions::add(&mut subs.patterns, &["p.*"]);
        Subscriptions::remove(&mut subs.channels, &[]);
        assert!(subs.channels.is_empty());
        assert_eq!(subs.patterns.len(), 1);
        assert!(!subs.is_empty());
    }

    #[test]
    fn replay_frames_emits_one_command_per_nonempty_kind() {
        let mut subs = Subscriptions::default();
        Subscriptions::add(&mut subs.channels, &["c1", "c2"]);
        Subscriptions::add(&mut subs.shard_channels, &["s1"]);
        // No patterns -> no PSUBSCRIBE frame.
        let frames = subs.replay_frames();
        assert_eq!(
            frames,
            vec![
                array(vec![bulk("SUBSCRIBE"), bulk("c1"), bulk("c2")]),
                array(vec![bulk("SSUBSCRIBE"), bulk("s1")]),
            ]
        );
    }

    #[test]
    fn replay_frames_is_empty_when_nothing_subscribed() {
        assert!(Subscriptions::default().replay_frames().is_empty());
    }

    // -- keyspace notifications --

    /// Build a `pmessage` frame the way Redis delivers a keyspace notification.
    fn keyspace_pmessage(pattern: &str, channel: &str, payload: &str) -> Frame {
        array(vec![
            bulk("pmessage"),
            bulk(pattern),
            bulk(channel),
            bulk(payload),
        ])
    }

    #[test]
    fn keyspace_event_parses_keyspace_channel() {
        let msg = PubSubConnection::parse_message(keyspace_pmessage(
            "__keyspace@0__:*",
            "__keyspace@0__:foo",
            "set",
        ))
        .unwrap()
        .unwrap();
        let event = KeyspaceEvent::from_message(&msg).unwrap();
        assert_eq!(event.kind, NotificationKind::Keyspace);
        assert_eq!(event.db, 0);
        assert_eq!(event.key, "foo");
        assert_eq!(event.event, "set");
    }

    #[test]
    fn keyspace_event_parses_keyevent_channel() {
        let msg = PubSubConnection::parse_message(keyspace_pmessage(
            "__keyevent@3__:*",
            "__keyevent@3__:expired",
            "session:42",
        ))
        .unwrap()
        .unwrap();
        let event = KeyspaceEvent::from_message(&msg).unwrap();
        assert_eq!(event.kind, NotificationKind::Keyevent);
        assert_eq!(event.db, 3);
        assert_eq!(event.key, "session:42");
        assert_eq!(event.event, "expired");
    }

    #[test]
    fn keyspace_event_preserves_colons_in_key() {
        // Keys containing the `__:` delimiter's `:` must survive: `split_once`
        // splits on the first `__:` only.
        let msg = PubSubConnection::parse_message(keyspace_pmessage(
            "__keyspace@0__:*",
            "__keyspace@0__:a:b:c",
            "del",
        ))
        .unwrap()
        .unwrap();
        let event = KeyspaceEvent::from_message(&msg).unwrap();
        assert_eq!(event.key, "a:b:c");
        assert_eq!(event.event, "del");
    }

    #[test]
    fn keyspace_event_returns_none_for_ordinary_channel() {
        let msg = PubSubConnection::parse_message(array(vec![
            bulk("message"),
            bulk("events"),
            bulk("payload"),
        ]))
        .unwrap()
        .unwrap();
        assert!(KeyspaceEvent::from_message(&msg).is_none());
    }

    #[test]
    fn keyspace_event_returns_none_for_non_numeric_db() {
        let msg = PubSubConnection::parse_message(keyspace_pmessage(
            "__keyspace@x__:*",
            "__keyspace@x__:foo",
            "set",
        ))
        .unwrap()
        .unwrap();
        assert!(KeyspaceEvent::from_message(&msg).is_none());
    }

    #[test]
    fn keyspace_event_stream_filter_skips_non_keyspace() {
        // KeyspaceEventStream::poll_next maps each PubSubMessage through
        // KeyspaceEvent::from_message and skips the `None`s: a plain message is
        // dropped while a keyspace notification is converted.
        let plain = PubSubConnection::parse_message(array(vec![
            bulk("message"),
            bulk("events"),
            bulk("hi"),
        ]))
        .unwrap()
        .unwrap();
        assert!(KeyspaceEvent::from_message(&plain).is_none());

        let ks = PubSubConnection::parse_message(keyspace_pmessage(
            "__keyspace@0__:*",
            "__keyspace@0__:k",
            "lpush",
        ))
        .unwrap()
        .unwrap();
        let event = KeyspaceEvent::from_message(&ks).unwrap();
        assert_eq!(event.event, "lpush");
        assert_eq!(event.key, "k");
    }
}
