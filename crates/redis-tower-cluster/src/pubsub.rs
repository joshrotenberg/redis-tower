//! Dedicated Redis Cluster Pub/Sub connections.
//!
//! Regular Pub/Sub has no hash-slot routing semantics, so callers explicitly
//! designate one current cluster node and reconnect to that exact endpoint.
//! Sharded Pub/Sub is slot-scoped: all channels in one connection share a
//! slot, and the connection follows that slot's committed master ownership.

use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::Stream;
use redis_tower::pubsub::{
    NamedPubSubConnection, NamedPubSubEvent, NamedPubSubMessage, NamedSubscriptions, PubSubMessage,
    PubSubName, Subscriptions,
};
use redis_tower_core::{Frame, RedisError};
use redis_tower_protocol::ProtocolError;
use tokio::sync::watch;
use tokio_stream::StreamExt;

use crate::connection::{Redirect, parse_redirect};
use crate::multiplexed::{ClusterPubSubBackend, MultiplexedClusterClient, NodeConnectionFactory};
use crate::slot::slot_for_key;
use crate::topology::NodeAddr;
use crate::topology::changes::TopologyChange;

/// Regular/pattern Cluster Pub/Sub with text channel names.
pub struct ClusterPubSubConnection {
    inner: NamedClusterPubSubConnection<String>,
    subscriptions: Subscriptions,
}
impl ClusterPubSubConnection {
    pub(crate) async fn connect(
        backend: ClusterPubSubBackend,
        node: NodeAddr,
    ) -> Result<Self, RedisError> {
        let inner = NamedClusterPubSubConnection::connect(backend, node).await?;
        let subscriptions = inner.subscriptions().into();
        Ok(Self {
            inner,
            subscriptions,
        })
    }
    /// Node currently hosting the dedicated socket.
    pub fn current_node(&self) -> &NodeAddr {
        self.inner.current_node()
    }
    /// Confirmed subscriptions replayed after a reconnect.
    pub fn subscriptions(&self) -> &Subscriptions {
        &self.subscriptions
    }
    /// See [`NamedClusterPubSubConnection::subscribe`].
    pub async fn subscribe(&mut self, names: &[&str]) -> Result<(), RedisError> {
        let result = self.inner.subscribe(names).await;
        self.subscriptions = self.inner.subscriptions().into();
        result
    }
    /// See [`NamedClusterPubSubConnection::psubscribe`].
    pub async fn psubscribe(&mut self, names: &[&str]) -> Result<(), RedisError> {
        let result = self.inner.psubscribe(names).await;
        self.subscriptions = self.inner.subscriptions().into();
        result
    }
    /// See [`NamedClusterPubSubConnection::unsubscribe`].
    pub async fn unsubscribe(&mut self, names: &[&str]) -> Result<(), RedisError> {
        let result = self.inner.unsubscribe(names).await;
        self.subscriptions = self.inner.subscriptions().into();
        result
    }
    /// See [`NamedClusterPubSubConnection::punsubscribe`].
    pub async fn punsubscribe(&mut self, names: &[&str]) -> Result<(), RedisError> {
        let result = self.inner.punsubscribe(names).await;
        self.subscriptions = self.inner.subscriptions().into();
        result
    }
    /// Receive a message, reconnecting and replaying subscriptions as needed.
    pub async fn next_message(&mut self) -> Result<PubSubMessage, RedisError> {
        self.inner.next_message().await.map(Into::into)
    }
    /// Consume this connection as a reconnect-aware message stream.
    pub fn into_stream(
        self,
    ) -> Pin<Box<dyn Stream<Item = Result<PubSubMessage, RedisError>> + Send>> {
        Box::pin(
            self.inner
                .into_stream()
                .map(|message| message.map(Into::into)),
        )
    }
}

/// Regular/pattern Cluster Pub/Sub preserving exact channel and pattern bytes.
pub type BinaryClusterPubSubConnection = NamedClusterPubSubConnection<Bytes>;
/// Sharded Cluster Pub/Sub with text channel names.
pub struct ShardedClusterPubSubConnection {
    inner: NamedShardedClusterPubSubConnection<String>,
    subscriptions: Subscriptions,
}
impl ShardedClusterPubSubConnection {
    pub(crate) async fn connect(
        backend: ClusterPubSubBackend,
        channels: &[&[u8]],
    ) -> Result<Self, RedisError> {
        let inner = NamedShardedClusterPubSubConnection::connect(backend, channels).await?;
        let subscriptions = inner.subscriptions().into();
        Ok(Self {
            inner,
            subscriptions,
        })
    }
    /// Node currently hosting the dedicated socket.
    pub fn current_node(&self) -> &NodeAddr {
        self.inner.current_node()
    }
    /// Confirmed subscriptions replayed after a reconnect.
    pub fn subscriptions(&self) -> &Subscriptions {
        &self.subscriptions
    }
    /// Hash slot shared by all subscribed channels.
    pub fn slot(&self) -> u16 {
        self.inner.slot()
    }
    /// See [`NamedShardedClusterPubSubConnection::subscribe`].
    pub async fn subscribe(&mut self, names: &[&str]) -> Result<(), RedisError> {
        let result = self.inner.subscribe(names).await;
        self.subscriptions = self.inner.subscriptions().into();
        result
    }
    /// See [`NamedShardedClusterPubSubConnection::unsubscribe`].
    pub async fn unsubscribe(&mut self, names: &[&str]) -> Result<(), RedisError> {
        let result = self.inner.unsubscribe(names).await;
        self.subscriptions = self.inner.subscriptions().into();
        result
    }
    /// See [`NamedShardedClusterPubSubConnection::ssubscribe`].
    pub async fn ssubscribe(&mut self, names: &[&str]) -> Result<(), RedisError> {
        let result = self.inner.ssubscribe(names).await;
        self.subscriptions = self.inner.subscriptions().into();
        result
    }
    /// See [`NamedShardedClusterPubSubConnection::sunsubscribe`].
    pub async fn sunsubscribe(&mut self, names: &[&str]) -> Result<(), RedisError> {
        let result = self.inner.sunsubscribe(names).await;
        self.subscriptions = self.inner.subscriptions().into();
        result
    }
    /// Receive a message, reconnecting and replaying subscriptions as needed.
    pub async fn next_message(&mut self) -> Result<PubSubMessage, RedisError> {
        self.inner.next_message().await.map(Into::into)
    }
    /// Consume this connection as a reconnect-aware message stream.
    pub fn into_stream(
        self,
    ) -> Pin<Box<dyn Stream<Item = Result<PubSubMessage, RedisError>> + Send>> {
        Box::pin(
            self.inner
                .into_stream()
                .map(|message| message.map(Into::into)),
        )
    }
}

/// Sharded Cluster Pub/Sub preserving exact channel bytes across reconnects.
pub type BinaryShardedClusterPubSubConnection = NamedShardedClusterPubSubConnection<Bytes>;

/// A regular or pattern Pub/Sub connection pinned to one exact cluster node.
///
/// The socket is independent of the multiplexed command workers. On a
/// transport failure, [`next_message`](Self::next_message) reconnects to the
/// same designated endpoint and replays every tracked regular/pattern
/// subscription, retrying under
/// [`pubsub_reconnect_config`](crate::MultiplexedClusterClientBuilder::pubsub_reconnect_config)
/// so a node restart is survived. Each failed attempt refreshes the topology;
/// once the node is no longer a cluster member (it died and was failed over,
/// or was removed) the session ends with an error rather than silently
/// moving to another node, whose keyspace notifications would differ. Redis
/// Pub/Sub remains at-most-once: messages published while the socket is
/// disconnected can be lost.
pub struct NamedClusterPubSubConnection<N> {
    connection: NamedPubSubConnection<N>,
    node: NodeAddr,
    factory: NodeConnectionFactory,
    backend: ClusterPubSubBackend,
}

impl<N: PubSubName> NamedClusterPubSubConnection<N> {
    pub(crate) async fn connect(
        backend: ClusterPubSubBackend,
        node: NodeAddr,
    ) -> Result<Self, RedisError> {
        backend.validate_member(&node).await?;
        let connection =
            NamedPubSubConnection::from_connection(backend.connect_node(&node).await?)?;
        let factory = backend.fixed_factory(node.clone());
        Ok(Self {
            connection,
            node,
            factory,
            backend,
        })
    }

    /// The exact node designated for this regular Pub/Sub connection.
    pub fn current_node(&self) -> &NodeAddr {
        &self.node
    }

    /// The confirmed subscriptions replayed after a reconnect.
    pub fn subscriptions(&self) -> &NamedSubscriptions<N> {
        self.connection.subscriptions()
    }

    /// Subscribe to regular channels on the designated node.
    pub async fn subscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        let names: Vec<&[u8]> = channels.iter().map(|name| name.as_bytes()).collect();
        self.subscribe_bytes(&names).await
    }

    /// Byte-oriented `SUBSCRIBE` preserving exact channel or pattern names.
    pub async fn subscribe_bytes(&mut self, channels: &[&[u8]]) -> Result<(), RedisError> {
        self.connection.subscribe_bytes(channels).await
    }

    /// Subscribe to regular channel patterns on the designated node.
    pub async fn psubscribe(&mut self, patterns: &[&str]) -> Result<(), RedisError> {
        let names: Vec<&[u8]> = patterns.iter().map(|name| name.as_bytes()).collect();
        self.psubscribe_bytes(&names).await
    }

    /// Byte-oriented `PSUBSCRIBE` preserving exact channel or pattern names.
    pub async fn psubscribe_bytes(&mut self, patterns: &[&[u8]]) -> Result<(), RedisError> {
        self.connection.psubscribe_bytes(patterns).await
    }

    /// Unsubscribe regular channels, or all regular channels when empty.
    pub async fn unsubscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        let names: Vec<&[u8]> = channels.iter().map(|name| name.as_bytes()).collect();
        self.unsubscribe_bytes(&names).await
    }

    /// Byte-oriented `UNSUBSCRIBE` preserving exact channel or pattern names.
    pub async fn unsubscribe_bytes(&mut self, channels: &[&[u8]]) -> Result<(), RedisError> {
        self.connection.unsubscribe_bytes(channels).await
    }

    /// Unsubscribe patterns, or all patterns when empty.
    pub async fn punsubscribe(&mut self, patterns: &[&str]) -> Result<(), RedisError> {
        let names: Vec<&[u8]> = patterns.iter().map(|name| name.as_bytes()).collect();
        self.punsubscribe_bytes(&names).await
    }

    /// Byte-oriented `PUNSUBSCRIBE` preserving exact channel or pattern names.
    pub async fn punsubscribe_bytes(&mut self, patterns: &[&[u8]]) -> Result<(), RedisError> {
        self.connection.punsubscribe_bytes(patterns).await
    }

    /// Receive the next message, reconnecting and replaying subscriptions when
    /// the pinned node closes the socket.
    pub async fn next_message(&mut self) -> Result<NamedPubSubMessage<N>, RedisError> {
        loop {
            match self.connection.next().await {
                Some(Err(error)) if reconnectable_stream_error(&error) => self.reconnect().await?,
                Some(message) => return message,
                None => self.reconnect().await?,
            }
        }
    }

    /// Reconnect to the designated node, refreshing the topology between
    /// attempts and giving up once the node has left it.
    async fn reconnect(&mut self) -> Result<(), RedisError> {
        let config = self.backend.pubsub_reconnect_config().clone();
        let mut attempt = 0usize;
        loop {
            let one_attempt = single_attempt(&config, attempt);
            match self
                .connection
                .reconnect_with_backoff(&self.factory, &one_attempt)
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let error = single_attempt_error(error);
                    tracing::debug!(
                        node = %self.node,
                        attempt,
                        %error,
                        "cluster Pub/Sub: reconnect attempt failed"
                    );
                    self.backend.await_refresh_since(Instant::now()).await?;
                    if self.backend.validate_member(&self.node).await.is_err() {
                        return Err(RedisError::Redis(format!(
                            "cluster Pub/Sub node {} left the cluster topology: {error}",
                            self.node
                        )));
                    }
                    let attempts = attempt.saturating_add(1);
                    if config
                        .max_retries
                        .is_some_and(|max_retries| attempt >= max_retries)
                    {
                        return Err(RedisError::ReconnectFailed {
                            attempts,
                            last_error: Arc::new(error),
                        });
                    }
                    attempt = attempts;
                }
            }
        }
    }

    /// Consume this connection as a reconnect-aware message stream.
    pub fn into_stream(
        mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<NamedPubSubMessage<N>, RedisError>> + Send>> {
        Box::pin(async_stream::stream! {
            loop {
                let message = self.next_message().await;
                let failed = message.is_err();
                yield message;
                if failed {
                    break;
                }
            }
        })
    }
}

/// A sharded Pub/Sub connection bound to one Redis Cluster hash slot.
///
/// Construction validates that all channels share a slot before any network
/// I/O. The socket connects to that slot's current master and subscribes
/// immediately. [`next_message`](Self::next_message) observes committed
/// topology revisions and reconnects/resubscribes after transport failures and
/// when this slot's owner changes; unrelated topology changes do not repin the
/// socket. When the owner itself drops the subscription -- Redis does this
/// when the slot migrates away or the node is demoted, while the socket stays
/// open -- the session refreshes the topology and resubscribes on the slot's
/// new owner. A failed reconnect refreshes the topology before the next
/// attempt, so a hard failover moves the session to the promoted replica;
/// see
/// [`pubsub_reconnect_config`](crate::MultiplexedClusterClientBuilder::pubsub_reconnect_config)
/// for the retry budget. Delivery is at-most-once across the reconnect gap.
///
/// [`next_message`](Self::next_message) is cancel-safe with respect to this
/// recovery: a pending owner change, server drop, or reconnect interrupted by
/// a dropped future is resumed by the next call.
///
/// Keep the originating [`MultiplexedClusterClient`] alive for the lifetime of
/// this handle. The handle intentionally keeps only a weak topology reference
/// so it cannot prevent cluster shutdown; once the client is gone, topology
/// observation and reconnects return [`RedisError::ConnectionClosed`].
pub struct NamedShardedClusterPubSubConnection<N> {
    connection: NamedPubSubConnection<N>,
    slot: u16,
    node: NodeAddr,
    backend: ClusterPubSubBackend,
    changes: watch::Receiver<Option<std::sync::Arc<TopologyChange>>>,
    // Recovery work recorded before it is awaited, so cancelling
    // `next_message` cannot lose it. `changes.changed()` marks a revision seen
    // as soon as it returns.
    owner_check_pending: bool,
    reconnect_pending: bool,
    /// Channels the server dropped, to resubscribe on the slot's new owner.
    dropped: Vec<N>,
    /// When the oldest entry in `dropped` was observed.
    dropped_at: Option<Instant>,
}

impl<N: PubSubName> NamedShardedClusterPubSubConnection<N> {
    pub(crate) async fn connect(
        backend: ClusterPubSubBackend,
        channels: &[&[u8]],
    ) -> Result<Self, RedisError> {
        let slot = preflight_shard_channels_bytes(channels)?;
        let (snapshot, changes) = backend.topology_state().await?;
        let node = snapshot
            .topology()
            .master_for_slot(slot)
            .cloned()
            .ok_or_else(|| {
                RedisError::Redis(format!(
                    "cluster Pub/Sub has no master for hash slot {slot}"
                ))
            })?;
        let connection =
            NamedPubSubConnection::from_connection(backend.connect_node(&node).await?)?;
        let mut session = Self {
            connection,
            slot,
            node,
            backend,
            changes,
            owner_check_pending: false,
            reconnect_pending: false,
            dropped: Vec::new(),
            dropped_at: None,
        };
        session.subscribe_initial(channels).await?;
        Ok(session)
    }

    /// The hash slot shared by every channel in this session.
    pub fn slot(&self) -> u16 {
        self.slot
    }

    /// The master currently hosting the dedicated sharded Pub/Sub socket.
    pub fn current_node(&self) -> &NodeAddr {
        &self.node
    }

    /// The confirmed sharded subscriptions replayed after owner changes.
    pub fn subscriptions(&self) -> &NamedSubscriptions<N> {
        self.connection.subscriptions()
    }

    /// Add shard channels. Every channel must match this session's slot.
    pub async fn subscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        let names: Vec<&[u8]> = channels.iter().map(|name| name.as_bytes()).collect();
        self.subscribe_bytes(&names).await
    }

    /// Byte-oriented `SUBSCRIBE` preserving exact channel or pattern names.
    pub async fn subscribe_bytes(&mut self, channels: &[&[u8]]) -> Result<(), RedisError> {
        let requested_slot = preflight_shard_channels_bytes(channels)?;
        if requested_slot != self.slot {
            return Err(cross_slot_error());
        }
        self.subscribe_on_current(channels).await
    }

    /// Remove shard channels, or all shard channels when empty.
    pub async fn unsubscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        let names: Vec<&[u8]> = channels.iter().map(|name| name.as_bytes()).collect();
        self.unsubscribe_bytes(&names).await
    }

    /// Byte-oriented `UNSUBSCRIBE` preserving exact channel or pattern names.
    pub async fn unsubscribe_bytes(&mut self, channels: &[&[u8]]) -> Result<(), RedisError> {
        if !channels.is_empty() {
            let requested_slot = preflight_shard_channels_bytes(channels)?;
            if requested_slot != self.slot {
                return Err(cross_slot_error());
            }
        }
        // A channel awaiting resubscription after a server drop is no longer
        // wanted either.
        if channels.is_empty() {
            self.dropped.clear();
        } else {
            self.dropped
                .retain(|dropped| !channels.contains(&dropped.name_bytes()));
        }
        if self.dropped.is_empty() {
            self.dropped_at = None;
        }
        self.connection.sunsubscribe_bytes(channels).await
    }

    /// Alias using Redis's `SSUBSCRIBE` command name.
    pub async fn ssubscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        let names: Vec<&[u8]> = channels.iter().map(|name| name.as_bytes()).collect();
        self.ssubscribe_bytes(&names).await
    }

    /// Byte-oriented `SSUBSCRIBE` preserving exact channel or pattern names.
    pub async fn ssubscribe_bytes(&mut self, channels: &[&[u8]]) -> Result<(), RedisError> {
        self.subscribe_bytes(channels).await
    }

    /// Alias using Redis's `SUNSUBSCRIBE` command name.
    pub async fn sunsubscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        let names: Vec<&[u8]> = channels.iter().map(|name| name.as_bytes()).collect();
        self.sunsubscribe_bytes(&names).await
    }

    /// Byte-oriented `SUNSUBSCRIBE` preserving exact channel or pattern names.
    pub async fn sunsubscribe_bytes(&mut self, channels: &[&[u8]]) -> Result<(), RedisError> {
        self.unsubscribe_bytes(channels).await
    }

    /// Receive the next shard message while following committed ownership
    /// changes for this session's slot.
    pub async fn next_message(&mut self) -> Result<NamedPubSubMessage<N>, RedisError> {
        loop {
            if self.reconnect_pending {
                self.reconnect_current_owner().await?;
                self.reconnect_pending = false;
                continue;
            }
            if !self.dropped.is_empty() {
                self.resubscribe_dropped().await?;
                continue;
            }
            if self.owner_check_pending {
                self.follow_current_owner().await?;
                self.owner_check_pending = false;
                continue;
            }
            tokio::select! {
                biased;
                changed = self.changes.changed() => {
                    match changed {
                        Ok(()) => self.owner_check_pending = true,
                        Err(_) => return Err(RedisError::ConnectionClosed),
                    }
                }
                event = self.connection.next_event() => {
                    match event {
                        Some(Ok(NamedPubSubEvent::Message(message))) => return Ok(message),
                        Some(Ok(NamedPubSubEvent::ShardChannelUnsubscribed(channel))) => {
                            tracing::info!(
                                slot = self.slot,
                                node = %self.node,
                                "cluster Pub/Sub: server dropped a shard subscription; \
                                 following the slot's new owner"
                            );
                            self.dropped_at.get_or_insert_with(Instant::now);
                            self.dropped.push(channel);
                        }
                        Some(Ok(_)) => {}
                        Some(Err(error)) if reconnectable_stream_error(&error) => {
                            self.reconnect_pending = true;
                        }
                        Some(Err(error)) => return Err(error),
                        None => self.reconnect_pending = true,
                    }
                }
            }
        }
    }

    /// Consume this connection as a topology-aware message stream.
    pub fn into_stream(
        mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<NamedPubSubMessage<N>, RedisError>> + Send>> {
        Box::pin(async_stream::stream! {
            loop {
                let message = self.next_message().await;
                let failed = message.is_err();
                yield message;
                if failed {
                    break;
                }
            }
        })
    }

    async fn subscribe_initial(&mut self, channels: &[&[u8]]) -> Result<(), RedisError> {
        let mut redirects = 0;
        loop {
            match self.connection.ssubscribe_bytes(channels).await {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let Some((slot, address)) = moved_error(&error) else {
                        return Err(error);
                    };
                    if slot != self.slot {
                        return Err(RedisError::Redis(format!(
                            "cluster Pub/Sub received MOVED for slot {slot} while subscribing slot {}",
                            self.slot
                        )));
                    }
                    if redirects >= self.backend.max_redirects() {
                        return Err(RedisError::Redis(format!(
                            "too many cluster Pub/Sub redirects ({redirects})"
                        )));
                    }
                    redirects += 1;
                    let target = self.backend.remap_redirect(&address).await?;
                    self.backend.commit_moved(self.slot, target.clone()).await?;
                    self.replace_at(target, None).await?;
                }
            }
        }
    }

    async fn subscribe_on_current(&mut self, channels: &[&[u8]]) -> Result<(), RedisError> {
        match self.connection.ssubscribe_bytes(channels).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let Some((slot, address)) = moved_error(&error) else {
                    return Err(error);
                };
                if slot != self.slot {
                    return Err(RedisError::Redis(format!(
                        "cluster Pub/Sub received MOVED for slot {slot} while subscribing slot {}",
                        self.slot
                    )));
                }
                let target = self.backend.remap_redirect(&address).await?;
                self.backend.commit_moved(self.slot, target.clone()).await?;
                self.replace_at(target, Some(channels)).await
            }
        }
    }

    /// The slot's master in the committed topology.
    async fn committed_owner(&self) -> Result<NodeAddr, RedisError> {
        let snapshot = self.backend.topology_snapshot().await?;
        snapshot
            .topology()
            .master_for_slot(self.slot)
            .cloned()
            .ok_or_else(|| {
                RedisError::Redis(format!(
                    "cluster Pub/Sub has no master for hash slot {}",
                    self.slot
                ))
            })
    }

    async fn follow_current_owner(&mut self) -> Result<(), RedisError> {
        if self.committed_owner().await? == self.node {
            return Ok(());
        }
        self.reconnect_current_owner().await
    }

    /// Move the tracked subscriptions to the slot's committed owner.
    ///
    /// Every failed attempt waits for a topology refresh before the next, so
    /// an unreachable owner (killed, mid-failover) is replaced by the
    /// promoted replica as soon as the cluster reports it. A MOVED reply to
    /// the replayed SSUBSCRIBE is committed directly.
    async fn reconnect_current_owner(&mut self) -> Result<(), RedisError> {
        let config = self.backend.pubsub_reconnect_config().clone();
        let mut attempt = 0usize;

        loop {
            let target = self.committed_owner().await?;
            let factory = self.backend.fixed_factory(target.clone());
            let one_attempt = single_attempt(&config, attempt);

            match self
                .connection
                .reconnect_with_backoff(&factory, &one_attempt)
                .await
            {
                Ok(()) => {
                    self.node = target;
                    return Ok(());
                }
                Err(error) => {
                    if let Some((slot, address)) = nested_moved_error(&error) {
                        if slot != self.slot {
                            return Err(RedisError::Redis(format!(
                                "cluster Pub/Sub received MOVED for slot {slot} while reconnecting slot {}",
                                self.slot
                            )));
                        }
                        let target = self.backend.remap_redirect(&address).await?;
                        self.backend.commit_moved(self.slot, target).await?;
                    } else {
                        tracing::debug!(
                            slot = self.slot,
                            node = %target,
                            attempt,
                            error = %error,
                            "cluster Pub/Sub: reconnect attempt failed; refreshing topology"
                        );
                        self.backend.await_refresh_since(Instant::now()).await?;
                    }

                    let attempts = attempt.saturating_add(1);
                    if config
                        .max_retries
                        .is_some_and(|max_retries| attempt >= max_retries)
                    {
                        return Err(RedisError::ReconnectFailed {
                            attempts,
                            last_error: Arc::new(single_attempt_error(error)),
                        });
                    }
                    attempt = attempts;
                }
            }
        }
    }

    /// Resubscribe channels the server dropped on its own.
    ///
    /// The drop means the slot left this node (migration or demotion) while
    /// the socket stayed up. Wait for a refresh that started after the drop,
    /// move to the committed owner if it changed, and subscribe the dropped
    /// channels there, following MOVED if the refresh has not caught up.
    async fn resubscribe_dropped(&mut self) -> Result<(), RedisError> {
        let observed = *self.dropped_at.get_or_insert_with(Instant::now);
        self.backend.await_refresh_since(observed).await?;
        let mut redirects = 0usize;
        loop {
            if self.reconnect_pending || self.committed_owner().await? != self.node {
                self.reconnect_current_owner().await?;
                self.reconnect_pending = false;
            }
            let dropped = self.dropped.clone();
            let names: Vec<&[u8]> = dropped.iter().map(PubSubName::name_bytes).collect();
            match self.connection.ssubscribe_bytes(&names).await {
                Ok(()) => {
                    self.dropped.clear();
                    self.dropped_at = None;
                    return Ok(());
                }
                Err(error) if reconnectable_stream_error(&error) => {
                    self.reconnect_pending = true;
                }
                Err(error) => {
                    let Some((slot, address)) = moved_error(&error) else {
                        return Err(error);
                    };
                    if slot != self.slot {
                        return Err(RedisError::Redis(format!(
                            "cluster Pub/Sub received MOVED for slot {slot} while resubscribing slot {}",
                            self.slot
                        )));
                    }
                    if redirects >= self.backend.max_redirects() {
                        return Err(error);
                    }
                    redirects += 1;
                    let target = self.backend.remap_redirect(&address).await?;
                    self.backend.commit_moved(self.slot, target).await?;
                }
            }
        }
    }

    /// Prepare a fully subscribed replacement at an authoritative target and
    /// install it only after replay and any new subscriptions are confirmed.
    async fn replace_at(
        &mut self,
        target: NodeAddr,
        additional_channels: Option<&[&[u8]]>,
    ) -> Result<(), RedisError> {
        let mut replacement =
            NamedPubSubConnection::from_connection(self.backend.connect_node(&target).await?)?;
        let tracked: Vec<N> = self
            .connection
            .subscriptions()
            .shard_channels
            .iter()
            .cloned()
            .collect();
        if !tracked.is_empty() {
            let tracked_refs: Vec<&[u8]> = tracked.iter().map(PubSubName::name_bytes).collect();
            replacement.ssubscribe_bytes(&tracked_refs).await?;
        }
        if let Some(channels) = additional_channels {
            replacement.ssubscribe_bytes(channels).await?;
        }
        self.connection = replacement;
        self.node = target;
        Ok(())
    }
}

impl MultiplexedClusterClient {
    /// Open regular/pattern Pub/Sub preserving binary channel and pattern names.
    pub async fn pubsub_on_bytes(
        &self,
        node: NodeAddr,
    ) -> Result<BinaryClusterPubSubConnection, RedisError> {
        BinaryClusterPubSubConnection::connect(self.pubsub_backend().await, node).await
    }

    /// Subscribe same-slot shard channels preserving exact bytes across redirects
    /// and reconnects. Retain this client while using the returned session.
    pub async fn sharded_pubsub_bytes(
        &self,
        channels: &[&[u8]],
    ) -> Result<BinaryShardedClusterPubSubConnection, RedisError> {
        BinaryShardedClusterPubSubConnection::connect(self.pubsub_backend().await, channels).await
    }

    /// Open a regular/pattern Pub/Sub connection pinned to an explicit current
    /// cluster node.
    pub async fn pubsub_on(&self, node: NodeAddr) -> Result<ClusterPubSubConnection, RedisError> {
        ClusterPubSubConnection::connect(self.pubsub_backend().await, node).await
    }

    /// Open and subscribe a sharded Pub/Sub connection for same-slot channels.
    pub async fn sharded_pubsub(
        &self,
        channels: &[&str],
    ) -> Result<ShardedClusterPubSubConnection, RedisError> {
        let names: Vec<&[u8]> = channels.iter().map(|name| name.as_bytes()).collect();
        ShardedClusterPubSubConnection::connect(self.pubsub_backend().await, &names).await
    }
}

#[cfg(test)]
fn preflight_shard_channels(channels: &[&str]) -> Result<u16, RedisError> {
    let names: Vec<&[u8]> = channels.iter().map(|name| name.as_bytes()).collect();
    preflight_shard_channels_bytes(&names)
}

fn preflight_shard_channels_bytes(channels: &[&[u8]]) -> Result<u16, RedisError> {
    let Some(first) = channels.first() else {
        return Err(RedisError::Redis(
            "cluster sharded Pub/Sub requires at least one channel".to_string(),
        ));
    };
    let slot = slot_for_key(first);
    if channels
        .iter()
        .skip(1)
        .any(|channel| slot_for_key(channel) != slot)
    {
        return Err(cross_slot_error());
    }
    Ok(slot)
}

fn cross_slot_error() -> RedisError {
    RedisError::Redis(
        "CROSSSLOT cluster sharded Pub/Sub channels must hash to the same slot".to_string(),
    )
}

fn moved_error(error: &RedisError) -> Option<(u16, String)> {
    let RedisError::Redis(message) = error else {
        return None;
    };
    let frame = Frame::Error(message.as_bytes().to_vec().into());
    match parse_redirect(&frame) {
        Some(Redirect::Moved { slot, addr }) => Some((slot, addr)),
        _ => None,
    }
}

fn reconnectable_stream_error(error: &RedisError) -> bool {
    error.is_connection_error()
        || matches!(error, RedisError::Protocol(ProtocolError::Io(error)) if error.kind() != std::io::ErrorKind::Unsupported)
}

fn reconnect_delay_cap(
    config: &redis_tower::reconnect::ReconnectConfig,
    attempt: usize,
) -> Duration {
    config
        .base_delay
        .saturating_mul(1u32.wrapping_shl(attempt.min(31) as u32))
        .min(config.max_delay)
}

/// `config` narrowed to one connect attempt, delayed as attempt `attempt` of
/// its backoff. The session drives the retry loop itself so it can refresh
/// the topology between attempts.
fn single_attempt(
    config: &redis_tower::reconnect::ReconnectConfig,
    attempt: usize,
) -> redis_tower::reconnect::ReconnectConfig {
    let delay = reconnect_delay_cap(config, attempt);
    let mut one_attempt = config.clone();
    one_attempt.max_retries = Some(0);
    one_attempt.base_delay = delay;
    one_attempt.max_delay = delay;
    one_attempt
}

fn nested_moved_error(error: &RedisError) -> Option<(u16, String)> {
    match error {
        RedisError::ReconnectFailed { last_error, .. } => nested_moved_error(last_error),
        _ => moved_error(error),
    }
}

fn single_attempt_error(error: RedisError) -> RedisError {
    match error {
        RedisError::ReconnectFailed { last_error, .. } => Arc::try_unwrap(last_error)
            .unwrap_or_else(|shared| RedisError::Redis(shared.to_string())),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_protocol_errors_are_not_reconnected() {
        let error = RedisError::Protocol(ProtocolError::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "RESP3 attributed replies are not supported",
        )));
        assert!(!reconnectable_stream_error(&error));
        assert!(reconnectable_stream_error(&RedisError::ConnectionClosed));
    }

    #[test]
    fn sharded_channels_require_a_nonempty_same_slot_set() {
        assert!(preflight_shard_channels(&[]).is_err());
        assert_eq!(
            preflight_shard_channels(&["{orders}:created", "{orders}:paid"]).unwrap(),
            slot_for_key(b"{orders}:created")
        );
        assert!(
            preflight_shard_channels(&["{orders}:created", "{users}:created"])
                .unwrap_err()
                .to_string()
                .contains("CROSSSLOT")
        );
    }

    #[test]
    fn moved_parser_accepts_only_moved_redis_errors() {
        let moved = RedisError::Redis("MOVED 42 127.0.0.1:7001".to_string());
        assert_eq!(
            moved_error(&moved),
            Some((42, "127.0.0.1:7001".to_string()))
        );
        assert!(moved_error(&RedisError::Redis("ASK 42 127.0.0.1:7001".to_string())).is_none());
        assert!(moved_error(&RedisError::ConnectionClosed).is_none());
    }
}
