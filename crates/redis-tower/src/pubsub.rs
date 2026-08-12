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

use crate::reconnect::{ConnectionFactory, ReconnectConfig, connect_with_timeout};

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
    /// Any RESP decode limits configured on the connection are retained.
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
        let names = Self::unique_names(names)?;
        let mut args = vec![bulk(cmd)];
        for name in &names {
            args.push(bulk(*name));
        }
        self.framed
            .send(array(args))
            .await
            .map_err(RedisError::from)?;
        self.await_confirmations(&names, kind).await
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
        let tracked: Vec<String> = self.subs.channels.iter().cloned().collect();
        self.send_unsubscribe("UNSUBSCRIBE", channels, "unsubscribe", &tracked)
            .await?;

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
        let tracked: Vec<String> = self.subs.shard_channels.iter().cloned().collect();
        self.send_unsubscribe("SUNSUBSCRIBE", channels, "sunsubscribe", &tracked)
            .await?;

        Subscriptions::remove(&mut self.subs.shard_channels, channels);
        Ok(())
    }

    /// Unsubscribe from one or more patterns.
    pub async fn punsubscribe(&mut self, patterns: &[&str]) -> Result<(), RedisError> {
        let tracked: Vec<String> = self.subs.patterns.iter().cloned().collect();
        self.send_unsubscribe("PUNSUBSCRIBE", patterns, "punsubscribe", &tracked)
            .await?;

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
    /// Connection settings, including RESP decode limits, are determined by
    /// the factory and therefore apply to every replacement connection.
    pub async fn reconnect_with(
        &mut self,
        factory: &dyn ConnectionFactory,
    ) -> Result<(), RedisError> {
        let conn = factory.connect().await?;
        self.install_replacement(conn).await
    }

    /// Rebuild the underlying connection with the retry, backoff, and timeout
    /// policy in `config`, then replay every tracked subscription.
    ///
    /// Each attempt waits for [`ReconnectConfig::base_delay`] (growing up to
    /// its configured maximum) and applies the per-attempt connection timeout.
    /// A successfully-created candidate replays every subscription before it
    /// replaces the current transport; a failed replay therefore cannot expose
    /// a partially subscribed session. If a finite retry budget is exhausted,
    /// the final error is retained as the structured cause of
    /// [`RedisError::ReconnectFailed`].
    ///
    /// As on the other reconnecting surfaces, `connect_timeout` bounds only
    /// [`ConnectionFactory::connect`]. Subscription-confirmation waits have the
    /// same caller-controlled lifetime as [`subscribe`](Self::subscribe) and
    /// [`resubscribe`](Self::resubscribe); wrap this future in an operation
    /// timeout when the complete reconnect-and-replay sequence needs a deadline.
    pub async fn reconnect_with_backoff(
        &mut self,
        factory: &dyn ConnectionFactory,
        config: &ReconnectConfig,
    ) -> Result<(), RedisError> {
        let mut attempt = 0;

        loop {
            tokio::time::sleep(config.delay_for_attempt(attempt)).await;
            let result = match connect_with_timeout(factory, config.connect_timeout).await {
                Ok(conn) => self.install_replacement(conn).await,
                Err(error) => Err(error),
            };

            match result {
                Ok(()) => return Ok(()),
                Err(last_error) => {
                    let attempts = attempt.saturating_add(1);
                    attempt = attempts;
                    if config.attempt_exhausted(attempt) {
                        return Err(RedisError::ReconnectFailed {
                            attempts,
                            last_error: std::sync::Arc::new(last_error),
                        });
                    }
                }
            }
        }
    }

    /// Prepare, fully resubscribe, then atomically install one replacement.
    ///
    /// Keeping the candidate transport local means a failed replay cannot
    /// leave `self` pointing at a partially subscribed session. Messages that
    /// arrive while the replay confirmations are read move across with the
    /// successful candidate's delivery buffer.
    async fn install_replacement(&mut self, conn: RedisConnection) -> Result<(), RedisError> {
        let mut replacement = Self {
            framed: conn.into_framed()?,
            buffered_frames: VecDeque::new(),
            subs: self.subs.clone(),
        };
        replacement.resubscribe().await?;
        self.framed = replacement.framed;
        self.buffered_frames = replacement.buffered_frames;
        Ok(())
    }

    /// Send an unsubscribe-family command and await its confirmations without
    /// discarding messages that arrive between acknowledgements.
    async fn send_unsubscribe(
        &mut self,
        cmd: &str,
        names: &[&str],
        kind: &str,
        tracked: &[String],
    ) -> Result<(), RedisError> {
        let unique_names = if names.is_empty() {
            Vec::new()
        } else {
            Self::unique_names(names)?
        };
        let mut args = vec![bulk(cmd)];
        args.extend(unique_names.iter().map(|name| bulk(*name)));
        self.framed
            .send(array(args))
            .await
            .map_err(RedisError::from)?;

        if !unique_names.is_empty() {
            return self.await_confirmations(&unique_names, kind).await;
        }

        // With no arguments Redis acknowledges every subscription of this
        // family. Match those names instead of waiting for a total count of
        // zero: subscriptions in the other two families contribute to that
        // count and may intentionally remain active.
        if !tracked.is_empty() {
            let names: Vec<&str> = tracked.iter().map(String::as_str).collect();
            return self.await_confirmations(&names, kind).await;
        }

        // Redis still emits one acknowledgement with a null name when there
        // was nothing of this family to unsubscribe from.
        self.await_confirmation(kind).await
    }

    /// Reject an empty subscribe request and retain only the first occurrence
    /// of each name so the number of expected acknowledgements matches the
    /// command sent on the wire.
    fn unique_names<'a>(names: &'a [&'a str]) -> Result<Vec<&'a str>, RedisError> {
        if names.is_empty() {
            return Err(RedisError::Redis(
                "pub/sub subscribe requires at least one name".to_string(),
            ));
        }

        let mut seen = HashSet::with_capacity(names.len());
        Ok(names
            .iter()
            .copied()
            .filter(|name| seen.insert(*name))
            .collect())
    }

    /// Read a newly arrived frame directly from the transport.
    ///
    /// Confirmation searches deliberately bypass `buffered_frames`: those
    /// frames predate the command whose acknowledgement is being awaited and
    /// repeatedly popping and requeueing one would never make wire progress.
    async fn next_wire_frame(&mut self) -> Result<Frame, RedisError> {
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
        let mut deferred = VecDeque::new();

        let result = loop {
            if pending.is_empty() {
                break Ok(());
            }

            let frame = match self.next_wire_frame().await {
                Ok(frame) => frame,
                Err(error) => break Err(error),
            };

            match Self::extract_confirmation_channel(&frame, expected_kind) {
                Some(Ok(channel)) => {
                    if pending.remove(channel.as_str()) {
                        // Matched an expected channel -- continue.
                        continue;
                    }
                    // Confirmation for a channel we did not request in this call.
                    // Buffer it so the caller that IS waiting for it can consume it.
                    deferred.push_back(frame);
                }
                Some(Err(error)) => break Err(error),
                None => {
                    // Not a confirmation of the expected kind at all. Buffer it.
                    deferred.push_back(frame);
                }
            }
        };

        // Existing buffered frames are older than anything read above. Append
        // deferred wire frames to retain delivery order for the message stream.
        self.buffered_frames.extend(deferred);
        result
    }

    /// Wait for one confirmation of `expected_kind`, preserving every frame
    /// unrelated to that acknowledgement.
    async fn await_confirmation(&mut self, expected_kind: &str) -> Result<(), RedisError> {
        let mut deferred = VecDeque::new();

        let result = loop {
            let frame = match self.next_wire_frame().await {
                Ok(frame) => frame,
                Err(error) => break Err(error),
            };

            match Self::is_confirmation(&frame, expected_kind) {
                Some(Ok(())) => break Ok(()),
                Some(Err(error)) => break Err(error),
                None => deferred.push_back(frame),
            }
        };

        self.buffered_frames.extend(deferred);
        result
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
            Frame::BulkString(Some(b)) | Frame::SimpleString(b) => b,
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

    /// Check whether `frame` is a confirmation of `expected_kind`.
    ///
    /// A null name is valid when an unsubscribe command targets an empty
    /// subscription family.
    fn is_confirmation(frame: &Frame, expected_kind: &str) -> Option<Result<(), RedisError>> {
        let items = match frame {
            Frame::Array(Some(items)) | Frame::Push(items) => items,
            Frame::Error(error) => {
                return Some(Err(RedisError::Redis(
                    String::from_utf8_lossy(error).into_owned(),
                )));
            }
            _ => return None,
        };
        if items.len() < 3 || !matches!(items.last(), Some(Frame::Integer(_))) {
            return None;
        }

        let kind = match &items[0] {
            Frame::BulkString(Some(kind)) | Frame::SimpleString(kind) => kind,
            _ => return None,
        };
        if kind.as_ref() != expected_kind.as_bytes() {
            return None;
        }

        match &items[1] {
            Frame::BulkString(_) | Frame::SimpleString(_) => Some(Ok(())),
            _ => None,
        }
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Helper to build a subscribe confirmation frame.
    fn sub_confirmation(kind: &str, channel: &str, count: i64) -> Frame {
        array(vec![bulk(kind), bulk(channel), Frame::Integer(count)])
    }

    fn message(channel: &str, payload: &str) -> Frame {
        array(vec![bulk("message"), bulk(channel), bulk(payload)])
    }

    #[cfg(unix)]
    async fn redis_stream_pair() -> (RedisStream, RedisStream) {
        let (client, server) = tokio::net::UnixStream::pair().unwrap();
        (RedisStream::Unix(client), RedisStream::Unix(server))
    }

    #[cfg(not(unix))]
    async fn redis_stream_pair() -> (RedisStream, RedisStream) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, accepted) =
            tokio::join!(tokio::net::TcpStream::connect(addr), listener.accept());
        let (server, _) = accepted.unwrap();
        (RedisStream::Tcp(client.unwrap()), RedisStream::Tcp(server))
    }

    async fn pubsub_pair() -> (
        PubSubConnection,
        Framed<RedisStream, redis_tower_protocol::RespCodec>,
    ) {
        let (client, server) = redis_stream_pair().await;
        let connection = RedisConnection::from_stream(client);
        let pubsub = PubSubConnection::from_connection(connection).unwrap();
        let server = Framed::new(server, redis_tower_protocol::RespCodec::new());
        (pubsub, server)
    }

    #[test]
    fn extract_confirmation_channel_matches_expected_kind() {
        let frame = sub_confirmation("subscribe", "events", 1);
        let result = PubSubConnection::extract_confirmation_channel(&frame, "subscribe");
        assert!(result.is_some());
        assert_eq!(result.unwrap().unwrap(), "events");
    }

    #[test]
    fn extract_confirmation_channel_accepts_simple_string_kind() {
        let frame = array(vec![
            Frame::SimpleString(Bytes::from_static(b"subscribe")),
            bulk("events"),
            Frame::Integer(1),
        ]);
        assert_eq!(
            PubSubConnection::extract_confirmation_channel(&frame, "subscribe")
                .unwrap()
                .unwrap(),
            "events"
        );
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
    fn is_confirmation_accepts_zero_count() {
        let frame = array(vec![bulk("unsubscribe"), bulk("ch1"), Frame::Integer(0)]);
        assert!(matches!(
            PubSubConnection::is_confirmation(&frame, "unsubscribe"),
            Some(Ok(()))
        ));
    }

    #[test]
    fn is_confirmation_accepts_nonzero_count() {
        let frame = array(vec![bulk("unsubscribe"), bulk("ch1"), Frame::Integer(2)]);
        assert!(matches!(
            PubSubConnection::is_confirmation(&frame, "unsubscribe"),
            Some(Ok(()))
        ));
    }

    #[test]
    fn is_confirmation_accepts_null_unsubscribe_name() {
        let frame = array(vec![
            bulk("unsubscribe"),
            Frame::BulkString(None),
            Frame::Integer(0),
        ]);
        assert!(matches!(
            PubSubConnection::is_confirmation(&frame, "unsubscribe"),
            Some(Ok(()))
        ));
    }

    #[tokio::test]
    async fn subscribe_bypasses_existing_buffer_and_preserves_wire_messages() {
        let (mut pubsub, mut server) = pubsub_pair().await;
        pubsub
            .buffered_frames
            .push_back(message("events", "already-buffered"));

        let server_task = tokio::spawn(async move {
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                array(vec![bulk("SUBSCRIBE"), bulk("new")])
            );
            server.send(message("events", "from-wire")).await.unwrap();
            server
                .send(sub_confirmation("subscribe", "new", 2))
                .await
                .unwrap();
        });

        tokio::time::timeout(Duration::from_secs(1), pubsub.subscribe(&["new"]))
            .await
            .expect("subscribe confirmation search cycled buffered data")
            .unwrap();
        server_task.await.unwrap();

        let first = pubsub.next().await.unwrap().unwrap();
        let second = pubsub.next().await.unwrap().unwrap();
        assert_eq!(first.payload.as_ref(), b"already-buffered");
        assert_eq!(second.payload.as_ref(), b"from-wire");
    }

    #[tokio::test]
    async fn subscribe_rejects_empty_names_before_wire_io() {
        let (mut pubsub, mut server) = pubsub_pair().await;

        let error = pubsub.subscribe(&[]).await.unwrap_err();
        assert!(error.to_string().contains("at least one name"));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), server.next())
                .await
                .is_err(),
            "empty subscribe unexpectedly wrote a command"
        );
    }

    #[tokio::test]
    async fn subscribe_deduplicates_names_before_wire_and_confirmation_wait() {
        let (mut pubsub, mut server) = pubsub_pair().await;
        let server_task = tokio::spawn(async move {
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                array(vec![bulk("SUBSCRIBE"), bulk("a"), bulk("b")])
            );
            server
                .send(sub_confirmation("subscribe", "a", 1))
                .await
                .unwrap();
            server
                .send(sub_confirmation("subscribe", "b", 2))
                .await
                .unwrap();
        });

        tokio::time::timeout(
            Duration::from_secs(1),
            pubsub.subscribe(&["a", "a", "b", "a"]),
        )
        .await
        .expect("duplicate subscribe names left an acknowledgement pending")
        .unwrap();
        server_task.await.unwrap();
        assert_eq!(
            pubsub
                .subscriptions()
                .channels
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[tokio::test]
    async fn unsubscribe_all_matches_family_names_and_preserves_interleaved_message() {
        let (mut pubsub, mut server) = pubsub_pair().await;
        Subscriptions::add(&mut pubsub.subs.channels, &["a", "b"]);
        Subscriptions::add(&mut pubsub.subs.patterns, &["still-active.*"]);

        let server_task = tokio::spawn(async move {
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                array(vec![bulk("UNSUBSCRIBE")])
            );
            // The total never reaches zero because a pattern subscription is
            // intentionally left active.
            server
                .send(sub_confirmation("unsubscribe", "a", 2))
                .await
                .unwrap();
            server.send(message("a", "between-acks")).await.unwrap();
            server
                .send(sub_confirmation("unsubscribe", "b", 1))
                .await
                .unwrap();
        });

        tokio::time::timeout(Duration::from_secs(1), pubsub.unsubscribe(&[]))
            .await
            .expect("unsubscribe waited for an aggregate count of zero")
            .unwrap();
        server_task.await.unwrap();

        assert!(pubsub.subscriptions().channels.is_empty());
        assert_eq!(
            pubsub
                .subscriptions()
                .patterns
                .iter()
                .next()
                .map(String::as_str),
            Some("still-active.*")
        );
        let buffered = pubsub.next().await.unwrap().unwrap();
        assert_eq!(buffered.payload.as_ref(), b"between-acks");
    }

    #[tokio::test]
    async fn unsubscribe_empty_family_accepts_null_ack_and_preserves_message() {
        let (mut pubsub, mut server) = pubsub_pair().await;
        let server_task = tokio::spawn(async move {
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                array(vec![bulk("UNSUBSCRIBE")])
            );
            server
                .send(message("other", "before-null-ack"))
                .await
                .unwrap();
            server
                .send(array(vec![
                    bulk("unsubscribe"),
                    Frame::BulkString(None),
                    Frame::Integer(0),
                ]))
                .await
                .unwrap();
        });

        tokio::time::timeout(Duration::from_secs(1), pubsub.unsubscribe(&[]))
            .await
            .unwrap()
            .unwrap();
        server_task.await.unwrap();

        let buffered = pubsub.next().await.unwrap().unwrap();
        assert_eq!(buffered.payload.as_ref(), b"before-null-ack");
    }

    #[tokio::test]
    async fn unsubscribe_deduplicates_names_before_wire_and_confirmation_wait() {
        let (mut pubsub, mut server) = pubsub_pair().await;
        Subscriptions::add(&mut pubsub.subs.channels, &["a", "b"]);
        let server_task = tokio::spawn(async move {
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                array(vec![bulk("UNSUBSCRIBE"), bulk("a"), bulk("b")])
            );
            server
                .send(sub_confirmation("unsubscribe", "a", 1))
                .await
                .unwrap();
            server
                .send(sub_confirmation("unsubscribe", "b", 0))
                .await
                .unwrap();
        });

        tokio::time::timeout(Duration::from_secs(1), pubsub.unsubscribe(&["a", "a", "b"]))
            .await
            .expect("duplicate unsubscribe names left an acknowledgement pending")
            .unwrap();
        server_task.await.unwrap();
        assert!(pubsub.subscriptions().channels.is_empty());
    }

    #[tokio::test]
    async fn failed_replacement_replay_does_not_install_partial_session() {
        let (mut pubsub, mut original_server) = pubsub_pair().await;
        Subscriptions::add(&mut pubsub.subs.channels, &["a", "b"]);
        pubsub
            .buffered_frames
            .push_back(message("old", "old-buffer"));

        let (candidate, candidate_server) = redis_stream_pair().await;
        let candidate = RedisConnection::from_stream(candidate);
        let mut candidate_server =
            Framed::new(candidate_server, redis_tower_protocol::RespCodec::new());

        let candidate_task = tokio::spawn(async move {
            assert_eq!(
                candidate_server.next().await.unwrap().unwrap(),
                array(vec![bulk("SUBSCRIBE"), bulk("a"), bulk("b")])
            );
            candidate_server
                .send(sub_confirmation("subscribe", "a", 1))
                .await
                .unwrap();
            candidate_server
                .send(message("candidate", "must-not-leak"))
                .await
                .unwrap();
            candidate_server
                .send(Frame::Error(Bytes::from_static(b"ERR replay failed")))
                .await
                .unwrap();
        });

        let error = pubsub.install_replacement(candidate).await.unwrap_err();
        assert!(error.to_string().contains("replay failed"));
        candidate_task.await.unwrap();

        original_server
            .send(message("old", "old-wire"))
            .await
            .unwrap();
        let buffered = pubsub.next().await.unwrap().unwrap();
        let from_original_wire = pubsub.next().await.unwrap().unwrap();
        assert_eq!(buffered.payload.as_ref(), b"old-buffer");
        assert_eq!(from_original_wire.payload.as_ref(), b"old-wire");
        assert_eq!(pubsub.subscriptions().channels.len(), 2);
    }

    #[tokio::test]
    async fn reconnect_with_backoff_returns_final_error_after_zero_delay_budget() {
        let (mut pubsub, _server) = pubsub_pair().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let factory = {
            let calls = Arc::clone(&calls);
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                std::future::ready(Err::<RedisConnection, _>(RedisError::ConnectionClosed))
            }
        };
        let config = ReconnectConfig::default()
            .max_retries(2)
            .base_delay(Duration::ZERO)
            .max_delay(Duration::ZERO)
            .jitter(false);

        let error = tokio::time::timeout(
            Duration::from_secs(1),
            pubsub.reconnect_with_backoff(&factory, &config),
        )
        .await
        .unwrap()
        .unwrap_err();

        assert_eq!(calls.load(Ordering::SeqCst), 3);
        match error {
            RedisError::ReconnectFailed {
                attempts,
                last_error,
            } => {
                assert_eq!(attempts, 3);
                assert!(matches!(last_error.as_ref(), RedisError::ConnectionClosed));
            }
            other => panic!("expected ReconnectFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn reconnect_with_backoff_wraps_per_attempt_timeout() {
        let (mut pubsub, _server) = pubsub_pair().await;
        let factory =
            || async { futures::future::pending::<Result<RedisConnection, RedisError>>().await };
        let config = ReconnectConfig::default()
            .max_retries(0)
            .base_delay(Duration::ZERO)
            .max_delay(Duration::ZERO)
            .jitter(false)
            .connect_timeout(Duration::from_millis(10));

        let error = tokio::time::timeout(
            Duration::from_secs(1),
            pubsub.reconnect_with_backoff(&factory, &config),
        )
        .await
        .unwrap()
        .unwrap_err();

        match error {
            RedisError::ReconnectFailed {
                attempts,
                last_error,
            } => {
                assert_eq!(attempts, 1);
                assert!(matches!(last_error.as_ref(), RedisError::ConnectTimeout));
            }
            other => panic!("expected ReconnectFailed, got {other:?}"),
        }
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
