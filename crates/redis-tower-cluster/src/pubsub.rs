//! Dedicated Redis Cluster Pub/Sub connections.
//!
//! Regular Pub/Sub has no hash-slot routing semantics, so callers explicitly
//! designate one current cluster node and reconnect to that exact endpoint.
//! Sharded Pub/Sub is slot-scoped: all channels in one connection share a
//! slot, and the connection follows that slot's committed master ownership.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use redis_tower::pubsub::Subscriptions;
use redis_tower::{PubSubConnection, PubSubMessage};
use redis_tower_core::{Frame, RedisError};
use redis_tower_protocol::ProtocolError;
use tokio::sync::watch;
use tokio_stream::StreamExt;

use crate::connection::{Redirect, parse_redirect};
use crate::multiplexed::{ClusterPubSubBackend, MultiplexedClusterClient, NodeConnectionFactory};
use crate::slot::slot_for_key;
use crate::topology::NodeAddr;
use crate::topology::changes::TopologyChange;

/// A regular or pattern Pub/Sub connection pinned to one exact cluster node.
///
/// The socket is independent of the multiplexed command workers. On a
/// transport failure, [`next_message`](Self::next_message) reconnects to the
/// same designated endpoint and replays every tracked regular/pattern
/// subscription. Redis Pub/Sub remains at-most-once: messages published while
/// the socket is disconnected can be lost.
pub struct ClusterPubSubConnection {
    connection: PubSubConnection,
    node: NodeAddr,
    factory: NodeConnectionFactory,
    backend: ClusterPubSubBackend,
}

impl ClusterPubSubConnection {
    pub(crate) async fn connect(
        backend: ClusterPubSubBackend,
        node: NodeAddr,
    ) -> Result<Self, RedisError> {
        backend.validate_member(&node).await?;
        let connection = PubSubConnection::from_connection(backend.connect_node(&node).await?)?;
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
    pub fn subscriptions(&self) -> &Subscriptions {
        self.connection.subscriptions()
    }

    /// Subscribe to regular channels on the designated node.
    pub async fn subscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        self.connection.subscribe(channels).await
    }

    /// Subscribe to regular channel patterns on the designated node.
    pub async fn psubscribe(&mut self, patterns: &[&str]) -> Result<(), RedisError> {
        self.connection.psubscribe(patterns).await
    }

    /// Unsubscribe regular channels, or all regular channels when empty.
    pub async fn unsubscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        self.connection.unsubscribe(channels).await
    }

    /// Unsubscribe patterns, or all patterns when empty.
    pub async fn punsubscribe(&mut self, patterns: &[&str]) -> Result<(), RedisError> {
        self.connection.punsubscribe(patterns).await
    }

    /// Receive the next message, reconnecting and replaying subscriptions when
    /// the pinned node closes the socket.
    pub async fn next_message(&mut self) -> Result<PubSubMessage, RedisError> {
        loop {
            match self.connection.next().await {
                Some(Err(error)) if reconnectable_stream_error(&error) => {
                    self.connection
                        .reconnect_with_backoff(&self.factory, self.backend.reconnect_config())
                        .await?;
                }
                Some(message) => return message,
                None => {
                    self.connection
                        .reconnect_with_backoff(&self.factory, self.backend.reconnect_config())
                        .await?;
                }
            }
        }
    }

    /// Consume this connection as a reconnect-aware message stream.
    pub fn into_stream(
        mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<PubSubMessage, RedisError>> + Send>> {
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
/// socket. Delivery is at-most-once across the reconnect gap.
///
/// Keep the originating [`MultiplexedClusterClient`] alive for the lifetime of
/// this handle. The handle intentionally keeps only a weak topology reference
/// so it cannot prevent cluster shutdown; once the client is gone, topology
/// observation and reconnects return [`RedisError::ConnectionClosed`].
pub struct ShardedClusterPubSubConnection {
    connection: PubSubConnection,
    slot: u16,
    node: NodeAddr,
    backend: ClusterPubSubBackend,
    changes: watch::Receiver<Option<std::sync::Arc<TopologyChange>>>,
}

impl ShardedClusterPubSubConnection {
    pub(crate) async fn connect(
        backend: ClusterPubSubBackend,
        channels: &[&str],
    ) -> Result<Self, RedisError> {
        let slot = preflight_shard_channels(channels)?;
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
        let connection = PubSubConnection::from_connection(backend.connect_node(&node).await?)?;
        let mut session = Self {
            connection,
            slot,
            node,
            backend,
            changes,
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
    pub fn subscriptions(&self) -> &Subscriptions {
        self.connection.subscriptions()
    }

    /// Add shard channels. Every channel must match this session's slot.
    pub async fn subscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        let requested_slot = preflight_shard_channels(channels)?;
        if requested_slot != self.slot {
            return Err(cross_slot_error());
        }
        self.subscribe_on_current(channels).await
    }

    /// Remove shard channels, or all shard channels when empty.
    pub async fn unsubscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        if !channels.is_empty() {
            let requested_slot = preflight_shard_channels(channels)?;
            if requested_slot != self.slot {
                return Err(cross_slot_error());
            }
        }
        self.connection.sunsubscribe(channels).await
    }

    /// Alias using Redis's `SSUBSCRIBE` command name.
    pub async fn ssubscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        self.subscribe(channels).await
    }

    /// Alias using Redis's `SUNSUBSCRIBE` command name.
    pub async fn sunsubscribe(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        self.unsubscribe(channels).await
    }

    /// Receive the next shard message while following committed ownership
    /// changes for this session's slot.
    pub async fn next_message(&mut self) -> Result<PubSubMessage, RedisError> {
        loop {
            tokio::select! {
                biased;
                changed = self.changes.changed() => {
                    match changed {
                        Ok(()) => {
                            self.follow_current_owner().await?;
                        }
                        Err(_) => return Err(RedisError::ConnectionClosed),
                    }
                }
                message = self.connection.next() => {
                    match message {
                        Some(Err(error)) if reconnectable_stream_error(&error) => {
                            self.reconnect_current_owner().await?;
                        }
                        Some(message) => return message,
                        None => {
                            self.reconnect_current_owner().await?;
                        }
                    }
                }
            }
        }
    }

    /// Consume this connection as a topology-aware message stream.
    pub fn into_stream(
        mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<PubSubMessage, RedisError>> + Send>> {
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

    async fn subscribe_initial(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        let mut redirects = 0;
        loop {
            match self.connection.ssubscribe(channels).await {
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

    async fn subscribe_on_current(&mut self, channels: &[&str]) -> Result<(), RedisError> {
        match self.connection.ssubscribe(channels).await {
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

    async fn follow_current_owner(&mut self) -> Result<(), RedisError> {
        let snapshot = self.backend.topology_snapshot().await?;
        let next = snapshot
            .topology()
            .master_for_slot(self.slot)
            .cloned()
            .ok_or_else(|| {
                RedisError::Redis(format!(
                    "cluster Pub/Sub has no master for hash slot {}",
                    self.slot
                ))
            })?;
        if next == self.node {
            return Ok(());
        }
        self.reconnect_current_owner().await
    }

    async fn reconnect_current_owner(&mut self) -> Result<(), RedisError> {
        let config = self.backend.reconnect_config().clone();
        let mut attempt = 0usize;

        loop {
            let snapshot = self.backend.topology_snapshot().await?;
            let target = snapshot
                .topology()
                .master_for_slot(self.slot)
                .cloned()
                .ok_or_else(|| {
                    RedisError::Redis(format!(
                        "cluster Pub/Sub has no master for hash slot {}",
                        self.slot
                    ))
                })?;
            let factory = self.backend.fixed_factory(target.clone());
            let mut one_attempt = config.clone();
            let delay = reconnect_delay_cap(&config, attempt);
            one_attempt.max_retries = Some(0);
            one_attempt.base_delay = delay;
            one_attempt.max_delay = delay;

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

    /// Prepare a fully subscribed replacement at an authoritative target and
    /// install it only after replay and any new subscriptions are confirmed.
    async fn replace_at(
        &mut self,
        target: NodeAddr,
        additional_channels: Option<&[&str]>,
    ) -> Result<(), RedisError> {
        let mut replacement =
            PubSubConnection::from_connection(self.backend.connect_node(&target).await?)?;
        let tracked: Vec<String> = self
            .connection
            .subscriptions()
            .shard_channels
            .iter()
            .cloned()
            .collect();
        if !tracked.is_empty() {
            let tracked_refs: Vec<&str> = tracked.iter().map(String::as_str).collect();
            replacement.ssubscribe(&tracked_refs).await?;
        }
        if let Some(channels) = additional_channels {
            replacement.ssubscribe(channels).await?;
        }
        self.connection = replacement;
        self.node = target;
        Ok(())
    }
}

impl MultiplexedClusterClient {
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
        ShardedClusterPubSubConnection::connect(self.pubsub_backend().await, channels).await
    }
}

fn preflight_shard_channels(channels: &[&str]) -> Result<u16, RedisError> {
    let Some(first) = channels.first() else {
        return Err(RedisError::Redis(
            "cluster sharded Pub/Sub requires at least one channel".to_string(),
        ));
    };
    let slot = slot_for_key(first.as_bytes());
    if channels
        .iter()
        .skip(1)
        .any(|channel| slot_for_key(channel.as_bytes()) != slot)
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
    error.is_connection_error() || matches!(error, RedisError::Protocol(ProtocolError::Io(_)))
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
