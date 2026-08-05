//! Batteries-included resilient Redis client.
//!
//! [`ResilientRedisClient`] combines shared access (`Arc<Mutex<>>`) with
//! automatic reconnection on connection loss. It is the recommended
//! client for long-running applications that need to survive transient
//! network failures without manual intervention.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use redis_tower_commands::Ping;
use redis_tower_core::{Command, ConnectionConfig, Frame, RedisConnection, RedisError};
use tokio::sync::{Mutex, Notify, watch};
use tower_service::Service;

use crate::circuit_breaker::{RedisCircuitBreakerClient, RedisCircuitBreakerConfig};
use crate::reconnect::{
    AddrConnectionFactory, ConnectionDisconnectReason, ConnectionEvent, ConnectionEventBus,
    ConnectionFactory, ReconnectConfig, UrlConnectionFactory, publish_disconnect_once,
};
use crate::retry::{RetryClient, RetryPolicy};

/// Configuration for the opt-in queue used while Redis is reconnecting.
///
/// The queue is disabled unless this config is passed to
/// [`ResilientRedisClient::connect_with_offline_queue`] or
/// [`ResilientRedisClient::with_config_and_offline_queue`]. It admits only
/// commands whose [`Command::idempotent`] implementation returns `true`.
/// Queue admission is non-blocking: once `capacity` requests are admitted,
/// another idempotent request fails immediately with [`RedisError::QueueFull`].
/// Admission is serialized under the queue lock, so commands replay in ticket
/// order. Futures first polled concurrently can be admitted in either scheduler
/// order; the queue does not claim an ordering before admission.
///
/// Each command gets at most three wire replays by default. This prevents a
/// server that repeatedly accepts a connection and immediately drops the
/// replay from keeping the head ticket alive forever. Use
/// [`with_max_replay_attempts`](Self::with_max_replay_attempts) to tune that
/// separate replay budget. Reconnect attempts themselves remain governed by
/// [`ReconnectConfig`].
///
/// A capacity of zero is valid and provides fail-fast reconnect behavior while
/// still running reconnect campaigns in the background.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OfflineQueueConfig {
    capacity: usize,
    max_replay_attempts: usize,
}

impl OfflineQueueConfig {
    /// Create an offline queue that holds at most `capacity` commands.
    #[must_use]
    pub const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            max_replay_attempts: 3,
        }
    }

    /// Return the maximum number of commands that may wait for reconnection.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Set the maximum number of replacement-connection wire attempts for one
    /// admitted command.
    ///
    /// This must be non-zero. Once the limit is reached, the command returns
    /// [`RedisError::ReconnectFailed`] with the final wire error. The tainted
    /// connection is still replaced before a later queued command can run.
    #[must_use]
    pub const fn with_max_replay_attempts(mut self, max_replay_attempts: usize) -> Self {
        assert!(
            max_replay_attempts > 0,
            "offline queue replay attempts must be non-zero"
        );
        self.max_replay_attempts = max_replay_attempts;
        self
    }

    /// Return the per-command replacement-connection replay limit.
    #[must_use]
    pub const fn max_replay_attempts(&self) -> usize {
        self.max_replay_attempts
    }
}

impl Default for OfflineQueueConfig {
    fn default() -> Self {
        Self::new(1024)
    }
}

/// A shared, auto-reconnecting Redis client.
///
/// Wraps a [`RedisConnection`] with automatic reconnection on connection
/// loss. Uses `Arc<Mutex<>>` for cross-task sharing.
///
/// # Concurrency
///
/// `ResilientRedisClient` is `Clone + Send + Sync`. All clones share the same
/// `Arc<Mutex<RedisConnection>>`, serializing commands one at a time.
/// Reconnection is triggered only when a command fails with a connection error
/// (`is_connection_error()` returns true); non-connection errors (WRONGTYPE,
/// etc.) are returned to the caller without triggering reconnection. After
/// the first reconnect attempt plus `max_retries` additional attempts are
/// exhausted, the error propagates to the caller; the client is not permanently
/// broken and will attempt reconnection on the next command.
///
/// # Queueing During Reconnection
///
/// The default remains fail-fast: the command that discovers a lost connection
/// receives that error, and commands that reach the broken connection while a
/// reconnect is running also fail. Use
/// [`connect_with_offline_queue`](Self::connect_with_offline_queue) or
/// [`with_config_and_offline_queue`](Self::with_config_and_offline_queue) to
/// opt into a bounded shared queue. In that mode:
///
/// - only commands marked [`Command::idempotent`] wait and replay;
/// - accepted commands replay in queue-ticket order after one shared reconnect
///   campaign (concurrently first-polled futures can be admitted in either
///   scheduler order);
/// - overflow fails immediately with [`RedisError::QueueFull`]; and
/// - non-idempotent commands fail with a connection error instead of risking a
///   duplicate side effect.
///
/// Dropping all client clones cancels an in-progress reconnect, including a
/// factory future with no configured connect timeout. Dropping a queued command
/// future removes its ticket without blocking the commands behind it. Once a
/// command has reached the wire, cancellation conservatively quarantines that
/// socket and starts a replacement campaign before the next ticket can run, so
/// a late response cannot be mistaken for the next command's response.
///
/// Tower users who work directly with [`Frame`] can instead put a bounded
/// `tower::buffer::Buffer` in front of [`ReconnectService`](crate::ReconnectService):
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower::ReconnectService;
/// use redis_tower::reconnect::{AddrConnectionFactory, ReconnectConfig};
/// use tower::buffer::Buffer;
///
/// let reconnecting = ReconnectService::new(
///     AddrConnectionFactory::new("127.0.0.1:6379"),
///     ReconnectConfig::default(),
/// ).await?;
/// let buffered = Buffer::new(reconnecting, 64);
/// # let _ = buffered;
/// # Ok(())
/// # }
/// ```
///
/// A frame buffer cannot inspect [`Command::idempotent`], so callers remain
/// responsible for deciding whether retrying a failed frame is safe.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower::ResilientRedisClient;
/// use redis_tower::commands::Set;
///
/// let client = ResilientRedisClient::connect("127.0.0.1:6379").await?;
///
/// let c = client.clone();
/// tokio::spawn(async move {
///     c.execute(Set::new("key", "value")).await.unwrap();
/// });
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ResilientRedisClient {
    conn: Arc<Mutex<RedisConnection>>,
    factory: Arc<dyn ConnectionFactory>,
    config: ReconnectConfig,
    /// Single-flights reconnects across clones: a connection drop seen by many
    /// clones triggers one reconnect, not one storm per clone.
    gate: Arc<ReconnectGate>,
    /// Present only for clients constructed with explicit offline queueing.
    offline_queue: Option<OfflineQueueHandle>,
    connection_events: Option<ConnectionEventBus>,
    disconnect_reported: Option<Arc<AtomicBool>>,
    /// Counts only client handles. Detached reconnect tasks clone the control,
    /// not this lease, so final-handle shutdown remains observable immediately.
    _lifecycle: Option<ResilientClientLifecycle>,
}

impl ResilientRedisClient {
    /// Connect to Redis with default reconnection settings.
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        Self::with_config(AddrConnectionFactory::new(addr), ReconnectConfig::default()).await
    }

    /// Connect to Redis and publish connection lifecycle events.
    ///
    /// Subscribe to `events` before calling this constructor to observe the
    /// initial [`ConnectionEvent::Connected`] or
    /// [`ConnectionEvent::ConnectFailed`] event.
    pub async fn connect_with_events(
        addr: &str,
        events: ConnectionEventBus,
    ) -> Result<Self, RedisError> {
        Self::with_config_and_events(
            AddrConnectionFactory::new(addr),
            ReconnectConfig::default(),
            events,
        )
        .await
    }

    /// Connect to Redis and enable a bounded queue while reconnecting.
    ///
    /// The queue is shared by every clone. Only idempotent commands are
    /// admitted; see [`OfflineQueueConfig`] for overflow behavior.
    pub async fn connect_with_offline_queue(
        addr: &str,
        queue_config: OfflineQueueConfig,
    ) -> Result<Self, RedisError> {
        Self::with_config_and_offline_queue(
            AddrConnectionFactory::new(addr),
            ReconnectConfig::default(),
            queue_config,
        )
        .await
    }

    /// Connect to Redis with both an offline queue and lifecycle events.
    pub async fn connect_with_offline_queue_and_events(
        addr: &str,
        queue_config: OfflineQueueConfig,
        events: ConnectionEventBus,
    ) -> Result<Self, RedisError> {
        Self::with_config_and_offline_queue_and_events(
            AddrConnectionFactory::new(addr),
            ReconnectConfig::default(),
            queue_config,
            events,
        )
        .await
    }

    /// Connect with settings that are retained across every reconnect.
    pub async fn connect_with_connection_config(
        addr: &str,
        connection_config: ConnectionConfig,
    ) -> Result<Self, RedisError> {
        Self::with_config(
            AddrConnectionFactory::new(addr).with_connection_config(connection_config),
            ReconnectConfig::default(),
        )
        .await
    }

    /// Connect via a Redis URL with default reconnection settings.
    pub async fn connect_url(url: &str) -> Result<Self, RedisError> {
        Self::with_config(UrlConnectionFactory::new(url), ReconnectConfig::default()).await
    }

    /// Connect via a Redis URL and enable a bounded queue while reconnecting.
    pub async fn connect_url_with_offline_queue(
        url: &str,
        queue_config: OfflineQueueConfig,
    ) -> Result<Self, RedisError> {
        Self::with_config_and_offline_queue(
            UrlConnectionFactory::new(url),
            ReconnectConfig::default(),
            queue_config,
        )
        .await
    }

    /// Connect from a Redis URL with settings retained across every reconnect.
    pub async fn connect_url_with_connection_config(
        url: &str,
        connection_config: ConnectionConfig,
    ) -> Result<Self, RedisError> {
        Self::with_config(
            UrlConnectionFactory::new(url).with_connection_config(connection_config),
            ReconnectConfig::default(),
        )
        .await
    }

    /// Connect with a custom factory and reconnection config.
    pub async fn with_config(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
    ) -> Result<Self, RedisError> {
        Self::build(factory, config, None, None).await
    }

    /// Connect with a custom factory and publish connection lifecycle events.
    pub async fn with_config_and_events(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
        events: ConnectionEventBus,
    ) -> Result<Self, RedisError> {
        Self::build(factory, config, None, Some(events)).await
    }

    /// Connect with a custom factory, reconnection policy, and offline queue.
    ///
    /// Unlike [`Self::with_config`], this enables idempotent command replay
    /// after a successful reconnect. The initial connection is still
    /// established before this function returns.
    pub async fn with_config_and_offline_queue(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
        queue_config: OfflineQueueConfig,
    ) -> Result<Self, RedisError> {
        Self::build(factory, config, Some(queue_config), None).await
    }

    /// Connect with custom reconnect, offline queue, and lifecycle settings.
    pub async fn with_config_and_offline_queue_and_events(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
        queue_config: OfflineQueueConfig,
        events: ConnectionEventBus,
    ) -> Result<Self, RedisError> {
        Self::build(factory, config, Some(queue_config), Some(events)).await
    }

    async fn build(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
        queue_config: Option<OfflineQueueConfig>,
        connection_events: Option<ConnectionEventBus>,
    ) -> Result<Self, RedisError> {
        let factory: Arc<dyn ConnectionFactory> = Arc::new(factory);
        let conn = match connect_with_timeout(&*factory, config.connect_timeout).await {
            Ok(conn) => conn,
            Err(error) => {
                if let Some(events) = &connection_events {
                    events.publish_with(|| ConnectionEvent::ConnectFailed {
                        error: Arc::from(error.to_string()),
                    });
                }
                return Err(error);
            }
        };
        if let Some(events) = &connection_events {
            events.publish(ConnectionEvent::Connected);
        }
        let disconnect_reported = connection_events
            .as_ref()
            .map(|_| Arc::new(AtomicBool::new(false)));
        let lifecycle = connection_events
            .as_ref()
            .map(|events| ResilientClientLifecycle::new(events.clone()));
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            factory,
            config,
            gate: Arc::new(ReconnectGate::new()),
            offline_queue: queue_config.map(OfflineQueueHandle::new),
            connection_events,
            disconnect_reported,
            _lifecycle: lifecycle,
        })
    }

    /// Execute a command, reconnecting if the connection is lost.
    ///
    /// # Retry Safety
    ///
    /// Without an offline queue, a connection error starts reconnection but is
    /// returned to the caller. If the caller retries, the original command may
    /// already have executed before the socket dropped.
    ///
    /// With an offline queue, commands marked [`Command::idempotent`] wait for
    /// reconnection and replay automatically. A non-idempotent command still
    /// receives the original connection error and is never replayed. A
    /// deadline carried by [`redis_tower_core::WithDeadline`] bounds queue
    /// waiting, connection acquisition, and every wire attempt under one
    /// absolute budget. Expiry removes the queue ticket; expiry after dispatch
    /// also quarantines the socket before another command can use it.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        if let Some(queue) = &self.offline_queue {
            let deadline = cmd.deadline();
            if deadline.is_some_and(|deadline| deadline <= tokio::time::Instant::now()) {
                return Err(RedisError::CommandTimeout);
            }
            let operation = self.execute_with_offline_queue(cmd, queue);
            return match deadline {
                Some(deadline) => tokio::time::timeout_at(deadline, operation)
                    .await
                    .map_err(|_elapsed| RedisError::CommandTimeout)?,
                None => operation.await,
            };
        }

        let mut conn = self.conn.lock().await;
        let result = conn.execute(cmd).await;

        if let Err(ref e) = result
            && e.is_connection_error()
        {
            self.publish_disconnect(e);
            drop(conn);
            self.reconnect().await;
        }

        result
    }

    /// Return the number of commands currently admitted to the offline queue.
    ///
    /// This is a point-in-time observability value. It returns zero when
    /// offline queueing was not enabled.
    #[must_use]
    pub fn offline_queue_depth(&self) -> usize {
        self.offline_queue
            .as_ref()
            .map_or(0, OfflineQueueHandle::depth)
    }

    /// Return whether this client currently has a reconnect campaign running.
    #[must_use]
    pub fn is_reconnecting(&self) -> bool {
        self.offline_queue
            .as_ref()
            .is_some_and(OfflineQueueHandle::is_reconnecting)
            || self.gate.is_reconnecting()
    }

    async fn execute_with_offline_queue<Cmd: Command>(
        &self,
        mut cmd: Cmd,
        queue: &OfflineQueueHandle,
    ) -> Result<Cmd::Response, RedisError> {
        let idempotent = cmd.idempotent();
        // Freeze the exact wire request before its first send. An idempotent
        // replay must not rerun a command builder that uses interior state.
        let frame = cmd.to_frame();

        match queue.before_execute(idempotent) {
            QueueDecision::Execute => {}
            QueueDecision::Wait { permit, campaign } => {
                if let Some(campaign) = campaign {
                    self.spawn_reconnect_campaign(queue, campaign);
                }
                return self.execute_queued(cmd, frame, queue, permit).await;
            }
            QueueDecision::Reject { error, campaign } => {
                if let Some(campaign) = campaign {
                    self.spawn_reconnect_campaign(queue, campaign);
                }
                return Err(error.into_redis_error());
            }
        }

        let mut conn = self.conn.lock().await;

        // The phase can change while this caller waits for the connection
        // mutex. Recheck before touching a connection another caller has
        // already declared dead.
        match queue.before_execute(idempotent) {
            QueueDecision::Execute => {}
            QueueDecision::Wait { permit, campaign } => {
                drop(conn);
                if let Some(campaign) = campaign {
                    self.spawn_reconnect_campaign(queue, campaign);
                }
                return self.execute_queued(cmd, frame, queue, permit).await;
            }
            QueueDecision::Reject { error, campaign } => {
                drop(conn);
                if let Some(campaign) = campaign {
                    self.spawn_reconnect_campaign(queue, campaign);
                }
                return Err(error.into_redis_error());
            }
        }

        let mut alignment = ProtocolAlignmentGuard::new(self, queue);
        let (returned_cmd, result) = execute_retained(&mut conn, cmd, frame.clone()).await;
        alignment.disarm();
        cmd = returned_cmd;
        let Err(error) = result else {
            return result;
        };
        if !error.is_connection_error() {
            return Err(error);
        }

        self.publish_disconnect(&error);
        let decision = queue.after_connection_error(idempotent);
        drop(conn);
        match decision {
            QueueDecision::Wait { permit, campaign } => {
                if let Some(campaign) = campaign {
                    self.spawn_reconnect_campaign(queue, campaign);
                }
                self.execute_queued(cmd, frame, queue, permit).await
            }
            QueueDecision::Reject {
                error: rejection,
                campaign,
            } => {
                if let Some(campaign) = campaign {
                    self.spawn_reconnect_campaign(queue, campaign);
                }
                // Preserve the error from the command that actually
                // discovered the disconnect. QueueFull is the one useful
                // exception: capacity zero must remain observable.
                if matches!(rejection, QueueRejection::Full) {
                    Err(RedisError::QueueFull)
                } else {
                    Err(error)
                }
            }
            QueueDecision::Execute => unreachable!("a disconnect must leave offline mode"),
        }
    }

    async fn execute_queued<Cmd: Command>(
        &self,
        mut cmd: Cmd,
        frame: Frame,
        queue: &OfflineQueueHandle,
        permit: QueuePermit,
    ) -> Result<Cmd::Response, RedisError> {
        let mut replay_attempts = 0;
        loop {
            permit.wait_for_turn().await?;
            let mut conn = self.conn.lock().await;

            // A raw Service call or the preceding queued request can discover
            // another disconnect between our wake-up and acquiring the
            // connection. Never send until this ticket is still the active
            // head of a draining queue.
            match permit.check_turn() {
                TurnState::Ready => {}
                TurnState::Wait => {
                    drop(conn);
                    continue;
                }
                TurnState::Rejected(error) => return Err(error.into_redis_error()),
            }

            let mut alignment = ProtocolAlignmentGuard::new(self, queue);
            let (returned_cmd, result) = execute_retained(&mut conn, cmd, frame.clone()).await;
            alignment.disarm();
            cmd = returned_cmd;
            let Err(error) = result else {
                return result;
            };
            if !error.is_connection_error() {
                return Err(error);
            }

            self.publish_disconnect(&error);
            let campaign = queue.reconnect_existing(&permit);
            replay_attempts += 1;
            drop(conn);
            if let Some(campaign) = campaign {
                self.spawn_reconnect_campaign(queue, campaign);
            }
            if replay_attempts >= queue.max_replay_attempts() {
                return Err(RedisError::ReconnectFailed {
                    attempts: replay_attempts,
                    last_error: Arc::new(error),
                });
            }
            // Keep the same ticket at the head. The command is idempotent, so
            // it can safely cross another reconnect if the replacement socket
            // dies before or during replay, up to the configured replay limit.
        }
    }

    fn spawn_reconnect_campaign(&self, queue: &OfflineQueueHandle, campaign: u64) {
        let factory = Arc::clone(&self.factory);
        let config = self.config.clone();
        let conn = Arc::downgrade(&self.conn);
        let queue = Arc::downgrade(&queue.inner);
        let lifecycle = self
            ._lifecycle
            .as_ref()
            .map(ResilientClientLifecycle::control);
        let disconnect_reported = self.disconnect_reported.clone();
        let mut closed = queue
            .upgrade()
            .expect("offline queue owner exists while starting reconnect")
            .closed_tx
            .subscribe();

        tokio::spawn(async move {
            let outcome = reconnect_campaign_until_closed(
                &*factory,
                &config,
                &mut closed,
                lifecycle.as_deref(),
            )
            .await;
            let Some(queue) = queue.upgrade() else {
                return;
            };

            match outcome {
                CancellableReconnect::Connected {
                    connection: new_conn,
                    attempts,
                    elapsed,
                } => {
                    let Some(conn) = conn.upgrade() else {
                        return;
                    };
                    let lock = conn.lock();
                    tokio::pin!(lock);
                    let mut conn = tokio::select! {
                        result = closed.changed() => {
                            let _ = result;
                            return;
                        }
                        conn = &mut lock => conn,
                    };
                    if queue.campaign_is_active(campaign) {
                        *conn = new_conn;
                        if let Some(reported) = &disconnect_reported {
                            reported.store(false, Ordering::Release);
                        }
                        if let Some(lifecycle) = &lifecycle {
                            lifecycle.publish(ConnectionEvent::Reconnected { attempts, elapsed });
                        }
                        queue.finish_reconnect(campaign, None);
                    }
                }
                CancellableReconnect::Failed {
                    attempts,
                    last_error,
                } => {
                    queue.finish_reconnect(
                        campaign,
                        Some(ReconnectFailure {
                            attempts,
                            last_error: Arc::new(last_error),
                        }),
                    );
                }
                CancellableReconnect::Closed => {}
            }
        });
    }

    fn publish_disconnect(&self, error: &RedisError) {
        publish_disconnect_once(
            self.connection_events.as_ref(),
            self.disconnect_reported.as_deref(),
            error,
        );
    }

    /// Send a PING to verify the connection is alive.
    ///
    /// Returns `Ok(())` on success. Useful for Kubernetes readiness probes
    /// and `/health` endpoints.
    pub async fn health_check(&self) -> Result<(), RedisError> {
        self.execute(Ping::new()).await?;
        Ok(())
    }

    /// Wrap this client in idempotent-aware automatic retries.
    ///
    /// Returns a [`RetryClient`](crate::RetryClient) whose `execute` reissues
    /// idempotent commands on retryable errors per the
    /// [`RetryPolicy`](crate::RetryPolicy). Reconnection already happens
    /// underneath on connection loss; layering retries on top means an
    /// idempotent command that failed with a connection error is
    /// automatically re-sent after the client reconnects, up to the policy
    /// budget. Non-idempotent writes are never retried.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use redis_tower::{ResilientRedisClient, RetryPolicy};
    /// use redis_tower::commands::Get;
    ///
    /// let client = ResilientRedisClient::connect("127.0.0.1:6379").await?;
    /// let retrying = client.retry(RetryPolicy::default());
    /// let value: Option<bytes::Bytes> = retrying.execute(Get::new("key")).await?;
    /// # let _ = value;
    /// # Ok(())
    /// # }
    /// ```
    pub fn retry(&self, policy: RetryPolicy) -> RetryClient<Self> {
        RetryClient::new(self.clone(), policy)
    }

    /// Protect this reconnecting client with a Redis-aware circuit breaker.
    ///
    /// The breaker sits outside reconnection: connection and timeout failures
    /// affect circuit health, while Redis command errors such as `WRONGTYPE`
    /// do not. The returned client retains typed `execute`, `health_check`, and
    /// idempotent-aware `retry` helpers.
    pub fn with_circuit_breaker(
        self,
        config: RedisCircuitBreakerConfig,
    ) -> RedisCircuitBreakerClient<Self> {
        RedisCircuitBreakerClient::new(self, config)
    }

    /// Attempt to reconnect, single-flighting across clones.
    async fn reconnect(&self) {
        // Snapshot the generation before taking the gate. If another clone
        // reconnects while we wait, the generation advances and we skip --
        // a shared connection drop triggers one reconnect, not one per clone.
        let seen = self.gate.generation();
        let _guard = self.gate.enter().await;
        if self.gate.generation() != seen {
            return;
        }

        let _active = self.gate.mark_reconnecting();
        let lifecycle = self._lifecycle.as_ref().map(|lease| lease.control.as_ref());
        if let Some(success) = reconnect_campaign(&*self.factory, &self.config, lifecycle).await {
            *self.conn.lock().await = success.connection;
            if let Some(reported) = &self.disconnect_reported {
                reported.store(false, Ordering::Release);
            }
            if let Some(lifecycle) = lifecycle {
                lifecycle.publish(ConnectionEvent::Reconnected {
                    attempts: success.attempts,
                    elapsed: success.elapsed,
                });
            }
            self.gate.mark_reconnected();
        }
    }
}

struct ResilientLifecycleControl {
    state: StdMutex<ResilientLifecycleState>,
    events: ConnectionEventBus,
}

struct ResilientLifecycleState {
    handles: usize,
    shutdown_published: bool,
}

impl ResilientLifecycleControl {
    fn new(events: ConnectionEventBus) -> Self {
        Self {
            state: StdMutex::new(ResilientLifecycleState {
                handles: 1,
                shutdown_published: false,
            }),
            events,
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ResilientLifecycleState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn add_handle(&self) {
        let mut state = self.state();
        state.handles = state
            .handles
            .checked_add(1)
            .expect("ResilientRedisClient handle count overflowed");
    }

    fn release_handle(&self) {
        let mut state = self.state();
        state.handles = state
            .handles
            .checked_sub(1)
            .expect("ResilientRedisClient handle count underflowed");
        if state.handles == 0 && !state.shutdown_published {
            state.shutdown_published = true;
            self.events.publish(ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::Shutdown,
            });
        }
    }

    /// Publish while holding the same lock used by final-handle release. An
    /// event that wins this lock is ordered before shutdown; one that loses to
    /// final release is suppressed, making `Shutdown` terminal.
    fn publish_with(&self, make_event: impl FnOnce() -> ConnectionEvent) -> bool {
        let state = self.state();
        if state.handles == 0 {
            return false;
        }
        self.events.publish_with(make_event);
        true
    }

    fn publish(&self, event: ConnectionEvent) -> bool {
        self.publish_with(|| event)
    }
}

struct ResilientClientLifecycle {
    control: Arc<ResilientLifecycleControl>,
}

impl ResilientClientLifecycle {
    fn new(events: ConnectionEventBus) -> Self {
        Self {
            control: Arc::new(ResilientLifecycleControl::new(events)),
        }
    }

    fn control(&self) -> Arc<ResilientLifecycleControl> {
        Arc::clone(&self.control)
    }
}

impl Clone for ResilientClientLifecycle {
    fn clone(&self) -> Self {
        self.control.add_handle();
        Self {
            control: Arc::clone(&self.control),
        }
    }
}

impl Drop for ResilientClientLifecycle {
    fn drop(&mut self) {
        self.control.release_handle();
    }
}

impl Service<Frame> for ResilientRedisClient {
    type Response = Frame;
    type Error = RedisError;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Frame, RedisError>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Frame) -> Self::Future {
        let client = self.clone();
        Box::pin(async move {
            if let Some(queue) = &client.offline_queue {
                match queue.before_execute(false) {
                    QueueDecision::Execute => {}
                    QueueDecision::Reject { error, campaign } => {
                        if let Some(campaign) = campaign {
                            client.spawn_reconnect_campaign(queue, campaign);
                        }
                        return Err(error.into_redis_error());
                    }
                    QueueDecision::Wait { .. } => {
                        unreachable!("raw frames are never admitted to the offline queue")
                    }
                }
            }

            let mut connection = client.conn.lock().await;

            if let Some(queue) = &client.offline_queue {
                match queue.before_execute(false) {
                    QueueDecision::Execute => {}
                    QueueDecision::Reject { error, campaign } => {
                        drop(connection);
                        if let Some(campaign) = campaign {
                            client.spawn_reconnect_campaign(queue, campaign);
                        }
                        return Err(error.into_redis_error());
                    }
                    QueueDecision::Wait { .. } => {
                        unreachable!("raw frames are never admitted to the offline queue")
                    }
                }
            }

            let mut alignment = client
                .offline_queue
                .as_ref()
                .map(|queue| ProtocolAlignmentGuard::new(&client, queue));
            let result =
                connection
                    .execute_pipeline(vec![request])
                    .await
                    .and_then(|mut responses| {
                        responses.pop().ok_or(RedisError::UnexpectedResponse {
                            expected: "one pipeline response",
                            actual: "empty response".to_string(),
                        })
                    });
            if let Some(alignment) = &mut alignment {
                alignment.disarm();
            }

            if matches!(&result, Err(error) if error.is_connection_error()) {
                if let Err(error) = &result {
                    client.publish_disconnect(error);
                }
                drop(connection);
                if let Some(queue) = &client.offline_queue {
                    match queue.after_connection_error(false) {
                        QueueDecision::Reject { campaign, .. } => {
                            if let Some(campaign) = campaign {
                                client.spawn_reconnect_campaign(queue, campaign);
                            }
                        }
                        QueueDecision::Execute | QueueDecision::Wait { .. } => {
                            unreachable!("raw frames are never admitted to the offline queue")
                        }
                    }
                } else {
                    client.reconnect().await;
                }
            }
            result
        })
    }
}

/// A dropped wire future leaves it unknown whether Redis will still send a
/// response. Quarantine that connection synchronously on cancellation so the
/// next queue ticket cannot consume the abandoned response.
struct ProtocolAlignmentGuard<'a> {
    client: &'a ResilientRedisClient,
    queue: &'a OfflineQueueHandle,
    armed: bool,
}

impl<'a> ProtocolAlignmentGuard<'a> {
    fn new(client: &'a ResilientRedisClient, queue: &'a OfflineQueueHandle) -> Self {
        Self {
            client,
            queue,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ProtocolAlignmentGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        self.client
            .publish_disconnect(&RedisError::ConnectionClosed);
        if let Some(campaign) = self.queue.taint_connection() {
            self.client.spawn_reconnect_campaign(self.queue, campaign);
        }
    }
}

/// Execute a typed command while retaining the command value for a possible
/// idempotent replay. This mirrors [`RedisConnection::execute`] while using a
/// one-frame pipeline so serialization does not consume `cmd`.
async fn execute_retained<Cmd: Command>(
    conn: &mut RedisConnection,
    cmd: Cmd,
    frame: Frame,
) -> (Cmd, Result<Cmd::Response, RedisError>) {
    let result = match conn.execute_pipeline(vec![frame]).await {
        Ok(mut responses) => match responses.pop() {
            Some(Frame::Error(error)) => Err(RedisError::Redis(
                String::from_utf8_lossy(&error).into_owned(),
            )),
            Some(response) => cmd.parse_response(response),
            None => Err(RedisError::UnexpectedResponse {
                expected: "one command response",
                actual: "empty response".to_string(),
            }),
        },
        Err(error) => Err(error),
    };
    (cmd, result)
}

/// Queue state shared by all client clones.
struct OfflineQueue {
    capacity: usize,
    max_replay_attempts: usize,
    owners: AtomicUsize,
    state: StdMutex<OfflineQueueState>,
    changed: Notify,
    closed_tx: watch::Sender<bool>,
}

struct OfflineQueueState {
    phase: OfflinePhase,
    tickets: VecDeque<u64>,
    next_ticket: u64,
    next_campaign: u64,
    last_reconnect_failure: Option<ReconnectFailure>,
}

#[derive(Debug, Clone)]
struct ReconnectFailure {
    attempts: usize,
    last_error: Arc<RedisError>,
}

impl ReconnectFailure {
    fn rejection(&self) -> QueueRejection {
        QueueRejection::ReconnectFailed {
            attempts: self.attempts,
            last_error: Arc::clone(&self.last_error),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OfflinePhase {
    Connected,
    Reconnecting { campaign: u64 },
    Draining,
    Failed { attempts: usize },
    Closed,
}

/// An owner-counted handle. Reconnect tasks use `Weak<OfflineQueue>` rather
/// than cloning this handle, so dropping the final client clone closes the
/// queue and cancels a hanging factory future.
struct OfflineQueueHandle {
    inner: Arc<OfflineQueue>,
}

impl OfflineQueueHandle {
    fn new(config: OfflineQueueConfig) -> Self {
        let (closed_tx, _) = watch::channel(false);
        Self {
            inner: Arc::new(OfflineQueue {
                capacity: config.capacity,
                max_replay_attempts: config.max_replay_attempts,
                owners: AtomicUsize::new(1),
                state: StdMutex::new(OfflineQueueState {
                    phase: OfflinePhase::Connected,
                    tickets: VecDeque::new(),
                    next_ticket: 0,
                    next_campaign: 0,
                    last_reconnect_failure: None,
                }),
                changed: Notify::new(),
                closed_tx,
            }),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, OfflineQueueState> {
        self.inner
            .state
            .lock()
            .expect("offline queue state mutex poisoned")
    }

    fn depth(&self) -> usize {
        self.state().tickets.len()
    }

    fn is_reconnecting(&self) -> bool {
        matches!(self.state().phase, OfflinePhase::Reconnecting { .. })
    }

    fn max_replay_attempts(&self) -> usize {
        self.inner.max_replay_attempts
    }

    fn before_execute(&self, idempotent: bool) -> QueueDecision {
        let mut state = self.state();
        match state.phase {
            OfflinePhase::Connected => QueueDecision::Execute,
            OfflinePhase::Reconnecting { .. } | OfflinePhase::Draining => {
                if idempotent {
                    self.enqueue_locked(&mut state, None)
                } else {
                    QueueDecision::reject(QueueRejection::ConnectionClosed, None)
                }
            }
            OfflinePhase::Failed { attempts } if !state.tickets.is_empty() => {
                if idempotent {
                    let rejection = state.last_reconnect_failure.as_ref().map_or_else(
                        || QueueRejection::ReconnectFailed {
                            attempts,
                            last_error: Arc::new(RedisError::ConnectionClosed),
                        },
                        ReconnectFailure::rejection,
                    );
                    QueueDecision::reject(rejection, None)
                } else {
                    QueueDecision::reject(QueueRejection::ConnectionClosed, None)
                }
            }
            OfflinePhase::Failed { .. } => {
                let campaign = Self::begin_campaign_locked(&mut state);
                if idempotent {
                    self.enqueue_locked(&mut state, Some(campaign))
                } else {
                    QueueDecision::reject(QueueRejection::ConnectionClosed, Some(campaign))
                }
            }
            OfflinePhase::Closed => QueueDecision::reject(QueueRejection::ConnectionClosed, None),
        }
    }

    fn after_connection_error(&self, idempotent: bool) -> QueueDecision {
        let mut state = self.state();
        let campaign = match state.phase {
            OfflinePhase::Connected | OfflinePhase::Draining | OfflinePhase::Failed { .. } => {
                Some(Self::begin_campaign_locked(&mut state))
            }
            OfflinePhase::Reconnecting { .. } => None,
            OfflinePhase::Closed => {
                return QueueDecision::reject(QueueRejection::ConnectionClosed, None);
            }
        };

        if idempotent {
            self.enqueue_locked(&mut state, campaign)
        } else {
            QueueDecision::reject(QueueRejection::ConnectionClosed, campaign)
        }
    }

    fn enqueue_locked(
        &self,
        state: &mut OfflineQueueState,
        campaign: Option<u64>,
    ) -> QueueDecision {
        if state.tickets.len() >= self.inner.capacity {
            return QueueDecision::reject(QueueRejection::Full, campaign);
        }
        let ticket = state.next_ticket;
        state.next_ticket = state.next_ticket.wrapping_add(1);
        state.tickets.push_back(ticket);
        QueueDecision::Wait {
            permit: QueuePermit {
                queue: Arc::clone(&self.inner),
                ticket,
            },
            campaign,
        }
    }

    fn begin_campaign_locked(state: &mut OfflineQueueState) -> u64 {
        let campaign = state.next_campaign;
        state.next_campaign = state.next_campaign.wrapping_add(1);
        state.phase = OfflinePhase::Reconnecting { campaign };
        state.last_reconnect_failure = None;
        campaign
    }

    fn reconnect_existing(&self, permit: &QueuePermit) -> Option<u64> {
        let mut state = self.state();
        if matches!(state.phase, OfflinePhase::Closed) {
            return None;
        }
        if matches!(state.phase, OfflinePhase::Reconnecting { .. }) {
            return None;
        }
        debug_assert_eq!(state.tickets.front(), Some(&permit.ticket));
        Some(Self::begin_campaign_locked(&mut state))
    }

    /// Mark a possibly half-consumed connection unusable after the command
    /// future is canceled while an exchange is on the wire.
    fn taint_connection(&self) -> Option<u64> {
        let mut state = self.state();
        match state.phase {
            OfflinePhase::Connected | OfflinePhase::Draining | OfflinePhase::Failed { .. } => {
                Some(Self::begin_campaign_locked(&mut state))
            }
            OfflinePhase::Reconnecting { .. } | OfflinePhase::Closed => None,
        }
    }
}

impl Clone for OfflineQueueHandle {
    fn clone(&self) -> Self {
        self.inner.owners.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl Drop for OfflineQueueHandle {
    fn drop(&mut self) {
        if self.inner.owners.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.close();
        }
    }
}

impl OfflineQueue {
    fn state(&self) -> std::sync::MutexGuard<'_, OfflineQueueState> {
        self.state
            .lock()
            .expect("offline queue state mutex poisoned")
    }

    fn close(&self) {
        self.state().phase = OfflinePhase::Closed;
        self.closed_tx.send_replace(true);
        self.changed.notify_waiters();
    }

    fn campaign_is_active(&self, campaign: u64) -> bool {
        matches!(
            self.state().phase,
            OfflinePhase::Reconnecting { campaign: active } if active == campaign
        )
    }

    fn finish_reconnect(&self, campaign: u64, failure: Option<ReconnectFailure>) {
        let mut state = self.state();
        if !matches!(
            state.phase,
            OfflinePhase::Reconnecting { campaign: active } if active == campaign
        ) {
            return;
        }
        state.phase = match &failure {
            Some(failure) => OfflinePhase::Failed {
                attempts: failure.attempts,
            },
            None if state.tickets.is_empty() => OfflinePhase::Connected,
            None => OfflinePhase::Draining,
        };
        state.last_reconnect_failure = failure;
        drop(state);
        self.changed.notify_waiters();
    }
}

enum QueueDecision {
    Execute,
    Wait {
        permit: QueuePermit,
        campaign: Option<u64>,
    },
    Reject {
        error: QueueRejection,
        campaign: Option<u64>,
    },
}

impl QueueDecision {
    fn reject(error: QueueRejection, campaign: Option<u64>) -> Self {
        Self::Reject { error, campaign }
    }
}

#[derive(Debug, Clone)]
enum QueueRejection {
    ConnectionClosed,
    Full,
    ReconnectFailed {
        attempts: usize,
        last_error: Arc<RedisError>,
    },
}

impl QueueRejection {
    fn into_redis_error(self) -> RedisError {
        match self {
            Self::ConnectionClosed => RedisError::ConnectionClosed,
            Self::Full => RedisError::QueueFull,
            Self::ReconnectFailed {
                attempts,
                last_error,
            } => RedisError::ReconnectFailed {
                attempts,
                last_error,
            },
        }
    }
}

struct QueuePermit {
    queue: Arc<OfflineQueue>,
    ticket: u64,
}

impl QueuePermit {
    async fn wait_for_turn(&self) -> Result<(), RedisError> {
        loop {
            let notified = self.queue.changed.notified();
            match self.check_turn() {
                TurnState::Ready => return Ok(()),
                TurnState::Wait => notified.await,
                TurnState::Rejected(error) => return Err(error.into_redis_error()),
            }
        }
    }

    fn check_turn(&self) -> TurnState {
        let state = self.queue.state();
        match state.phase {
            OfflinePhase::Connected | OfflinePhase::Draining
                if state.tickets.front() == Some(&self.ticket) =>
            {
                TurnState::Ready
            }
            OfflinePhase::Failed { attempts } => {
                let rejection = state.last_reconnect_failure.as_ref().map_or_else(
                    || QueueRejection::ReconnectFailed {
                        attempts,
                        last_error: Arc::new(RedisError::ConnectionClosed),
                    },
                    ReconnectFailure::rejection,
                );
                TurnState::Rejected(rejection)
            }
            OfflinePhase::Closed => TurnState::Rejected(QueueRejection::ConnectionClosed),
            _ => TurnState::Wait,
        }
    }
}

impl Drop for QueuePermit {
    fn drop(&mut self) {
        let mut state = self.queue.state();
        let was_front = state.tickets.front() == Some(&self.ticket);
        if let Some(index) = state
            .tickets
            .iter()
            .position(|ticket| *ticket == self.ticket)
        {
            state.tickets.remove(index);
        }
        if state.tickets.is_empty() && matches!(state.phase, OfflinePhase::Draining) {
            state.phase = OfflinePhase::Connected;
        }
        drop(state);
        if was_front {
            self.queue.changed.notify_waiters();
        }
    }
}

enum TurnState {
    Ready,
    Wait,
    Rejected(QueueRejection),
}

/// Connect via the factory, bounding the attempt by `connect_timeout` if one is
/// configured so a black-holed connect cannot hang the reconnect loop forever.
async fn connect_with_timeout(
    factory: &dyn ConnectionFactory,
    connect_timeout: Option<Duration>,
) -> Result<RedisConnection, RedisError> {
    match connect_timeout {
        Some(t) => tokio::time::timeout(t, factory.connect())
            .await
            .map_err(|_| RedisError::ConnectTimeout)?,
        None => factory.connect().await,
    }
}

/// Run a reconnect campaign, applying the configured exponential delay before
/// every attempt (including `base_delay` before the first). The campaign makes
/// one attempt plus up to `max_retries` additional retries and returns the new
/// connection, or `None` once that budget is exhausted.
async fn reconnect_campaign(
    factory: &dyn ConnectionFactory,
    config: &ReconnectConfig,
    lifecycle: Option<&ResilientLifecycleControl>,
) -> Option<ReconnectSuccess> {
    let max = config.max_retries.unwrap_or(usize::MAX);
    let started = Instant::now();
    for attempt in 0..=max {
        let delay = config.delay_for_attempt(attempt);
        tracing::warn!(attempt, delay = ?delay, "redis: backing off before reconnect");
        if let Some(lifecycle) = lifecycle
            && !lifecycle.publish(ConnectionEvent::ReconnectAttempt {
                attempt: attempt + 1,
                delay,
            })
        {
            return None;
        }
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        match connect_with_timeout(factory, config.connect_timeout).await {
            Ok(conn) => {
                tracing::info!(attempt, "redis: reconnected successfully");
                return Some(ReconnectSuccess {
                    connection: conn,
                    attempts: attempt + 1,
                    elapsed: started.elapsed(),
                });
            }
            Err(e) => {
                tracing::warn!(attempt, error = %e, "redis: reconnect attempt failed");
                if let Some(lifecycle) = lifecycle
                    && !lifecycle.publish_with(|| ConnectionEvent::ReconnectFailed {
                        attempt: attempt + 1,
                        error: Arc::from(e.to_string()),
                    })
                {
                    return None;
                }
            }
        }
    }
    if let Some(lifecycle) = lifecycle {
        lifecycle.publish(ConnectionEvent::ReconnectExhausted {
            attempts: max.saturating_add(1),
        });
    }
    None
}

struct ReconnectSuccess {
    connection: RedisConnection,
    attempts: usize,
    elapsed: Duration,
}

enum CancellableReconnect {
    Connected {
        connection: RedisConnection,
        attempts: usize,
        elapsed: Duration,
    },
    Failed {
        attempts: usize,
        last_error: RedisError,
    },
    Closed,
}

/// Queue-enabled reconnects run independently of the caller that discovered
/// the failure. Watching queue closure makes that detached task cancellation
/// safe in both directions: canceling one command does not cancel everybody's
/// reconnect, while dropping every client clone does cancel a hanging factory.
async fn reconnect_campaign_until_closed(
    factory: &dyn ConnectionFactory,
    config: &ReconnectConfig,
    closed: &mut watch::Receiver<bool>,
    lifecycle: Option<&ResilientLifecycleControl>,
) -> CancellableReconnect {
    let max = config.max_retries.unwrap_or(usize::MAX);
    let started = Instant::now();
    let mut last_error = None;
    for attempt in 0..=max {
        if *closed.borrow() {
            return CancellableReconnect::Closed;
        }

        let delay = config.delay_for_attempt(attempt);
        tracing::warn!(attempt, delay = ?delay, "redis: backing off before reconnect");
        if let Some(lifecycle) = lifecycle
            && !lifecycle.publish(ConnectionEvent::ReconnectAttempt {
                attempt: attempt + 1,
                delay,
            })
        {
            return CancellableReconnect::Closed;
        }
        if !delay.is_zero() {
            tokio::select! {
                // Once shutdown and delay are both ready, shutdown wins.
                biased;
                result = closed.changed() => {
                    let _ = result;
                    return CancellableReconnect::Closed;
                }
                () = tokio::time::sleep(delay) => {}
            }
        }

        let connect = connect_with_timeout(factory, config.connect_timeout);
        tokio::pin!(connect);
        let result = tokio::select! {
            // Do not accept a replacement after terminal close when both
            // branches become ready in the same scheduler turn.
            biased;
            result = closed.changed() => {
                let _ = result;
                return CancellableReconnect::Closed;
            }
            result = &mut connect => result,
        };

        match result {
            Ok(conn) => {
                tracing::info!(attempt, "redis: reconnected successfully");
                return CancellableReconnect::Connected {
                    connection: conn,
                    attempts: attempt + 1,
                    elapsed: started.elapsed(),
                };
            }
            Err(error) => {
                tracing::warn!(attempt, error = %error, "redis: reconnect attempt failed");
                if let Some(lifecycle) = lifecycle
                    && !lifecycle.publish_with(|| ConnectionEvent::ReconnectFailed {
                        attempt: attempt + 1,
                        error: Arc::from(error.to_string()),
                    })
                {
                    return CancellableReconnect::Closed;
                }
                last_error = Some(error);
            }
        }
    }

    let attempts = max.saturating_add(1);
    if let Some(lifecycle) = lifecycle
        && !lifecycle.publish(ConnectionEvent::ReconnectExhausted { attempts })
    {
        return CancellableReconnect::Closed;
    }
    CancellableReconnect::Failed {
        attempts,
        last_error: last_error.unwrap_or(RedisError::ConnectionClosed),
    }
}

/// Single-flight coordinator for reconnects shared across clones.
///
/// A generation counter, bumped once per successful reconnect, lets a clone
/// that was waiting on the gate detect that the connection was already replaced
/// and skip its own attempt.
struct ReconnectGate {
    lock: Mutex<()>,
    generation: AtomicU64,
    reconnecting: std::sync::atomic::AtomicBool,
}

impl ReconnectGate {
    fn new() -> Self {
        Self {
            lock: Mutex::new(()),
            generation: AtomicU64::new(0),
            reconnecting: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// The current reconnect generation.
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Take the gate; only one clone holds it at a time, the rest wait.
    async fn enter(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.lock.lock().await
    }

    /// Advance the generation after a successful reconnect.
    fn mark_reconnected(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    fn is_reconnecting(&self) -> bool {
        self.reconnecting.load(Ordering::Acquire)
    }

    fn mark_reconnecting(&self) -> ActiveReconnect<'_> {
        self.reconnecting.store(true, Ordering::Release);
        ActiveReconnect(&self.reconnecting)
    }
}

struct ActiveReconnect<'a>(&'a std::sync::atomic::AtomicBool);

impl Drop for ActiveReconnect<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::{SinkExt, StreamExt};
    use redis_tower_core::{RedisStream, RespCodec, WithDeadline};
    use redis_tower_protocol::helpers::{array, bulk};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::time::Instant;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{Semaphore, oneshot};
    use tokio_util::codec::Framed;

    type ConnFuture = Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>>;

    /// Factory whose `connect()` always fails immediately, counting calls.
    struct FailingFactory {
        calls: Arc<AtomicUsize>,
    }

    impl ConnectionFactory for FailingFactory {
        fn connect(&self) -> ConnFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Err(RedisError::ConnectionClosed) })
        }
    }

    /// Factory whose `connect()` never resolves, counting calls.
    struct HangingFactory {
        calls: Arc<AtomicUsize>,
    }

    impl ConnectionFactory for HangingFactory {
        fn connect(&self) -> ConnFuture {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::pending())
        }
    }

    /// The reconnect future becomes ready only when the test flips `ready`;
    /// it intentionally stores no waker, so queue closure is the wake that
    /// makes both select branches ready in the same poll.
    struct ReadyOnCloseFactory {
        calls: Arc<AtomicUsize>,
        initial: StdMutex<Option<RedisConnection>>,
        ready: Arc<AtomicBool>,
    }

    impl ConnectionFactory for ReadyOnCloseFactory {
        fn connect(&self) -> ConnFuture {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                let connection = self
                    .initial
                    .lock()
                    .expect("initial connection mutex poisoned")
                    .take();
                return Box::pin(async move { connection.ok_or(RedisError::ConnectionClosed) });
            }

            let ready = Arc::clone(&self.ready);
            Box::pin(std::future::poll_fn(move |_cx| {
                if ready.load(Ordering::Acquire) {
                    std::task::Poll::Ready(Err(RedisError::ConnectionClosed))
                } else {
                    std::task::Poll::Pending
                }
            }))
        }
    }

    struct EchoCommand {
        value: Bytes,
        idempotent: bool,
    }

    impl EchoCommand {
        fn new(value: &'static str, idempotent: bool) -> Self {
            Self {
                value: Bytes::from_static(value.as_bytes()),
                idempotent,
            }
        }
    }

    impl Command for EchoCommand {
        type Response = Bytes;

        fn to_frame(&self) -> Frame {
            array(vec![bulk("ECHO"), bulk(self.value.clone())])
        }

        fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
            match frame {
                Frame::BulkString(Some(value)) => Ok(value),
                other => Err(RedisError::UnexpectedResponse {
                    expected: "bulk string",
                    actual: format!("{other:?}"),
                }),
            }
        }

        fn name(&self) -> &str {
            "ECHO"
        }

        fn idempotent(&self) -> bool {
            self.idempotent
        }
    }

    /// Factory with an immediate initial connection and a replacement held
    /// behind a semaphore so tests can fill and inspect the offline queue.
    struct ControlledFactory {
        calls: Arc<AtomicUsize>,
        initial: StdMutex<Option<RedisConnection>>,
        replacement: Arc<StdMutex<Option<RedisConnection>>>,
        release_reconnect: Arc<Semaphore>,
    }

    impl ConnectionFactory for ControlledFactory {
        fn connect(&self) -> ConnFuture {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                let connection = self
                    .initial
                    .lock()
                    .expect("initial connection mutex poisoned")
                    .take();
                return Box::pin(async move { connection.ok_or(RedisError::ConnectionClosed) });
            }

            let replacement = Arc::clone(&self.replacement);
            let release = Arc::clone(&self.release_reconnect);
            Box::pin(async move {
                let permit = release
                    .acquire_owned()
                    .await
                    .map_err(|_| RedisError::ConnectionClosed)?;
                permit.forget();
                replacement
                    .lock()
                    .expect("replacement connection mutex poisoned")
                    .take()
                    .ok_or(RedisError::ConnectionClosed)
            })
        }
    }

    /// Factory that yields several independently gated replacement sockets.
    /// This lets cancellation tests prove that a late response on one socket
    /// can never be consumed after the client advances to the next ticket.
    struct SequencedFactory {
        calls: Arc<AtomicUsize>,
        initial: StdMutex<Option<RedisConnection>>,
        replacements: Arc<StdMutex<VecDeque<RedisConnection>>>,
        release_reconnect: Arc<Semaphore>,
    }

    impl ConnectionFactory for SequencedFactory {
        fn connect(&self) -> ConnFuture {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                let connection = self
                    .initial
                    .lock()
                    .expect("initial connection mutex poisoned")
                    .take();
                return Box::pin(async move { connection.ok_or(RedisError::ConnectionClosed) });
            }

            let replacements = Arc::clone(&self.replacements);
            let release = Arc::clone(&self.release_reconnect);
            Box::pin(async move {
                let permit = release
                    .acquire_owned()
                    .await
                    .map_err(|_| RedisError::ConnectionClosed)?;
                permit.forget();
                replacements
                    .lock()
                    .expect("replacement connections mutex poisoned")
                    .pop_front()
                    .ok_or(RedisError::ConnectionClosed)
            })
        }
    }

    async fn connection_pair() -> (RedisConnection, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (client, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
        let client = client.unwrap();
        let (server, _) = accepted.unwrap();
        (
            RedisConnection::from_stream(RedisStream::Tcp(client)),
            server,
        )
    }

    fn close_on_first_request(mut server: TcpStream) -> oneshot::Receiver<()> {
        let (seen_tx, seen_rx) = oneshot::channel();
        tokio::spawn(async move {
            let mut bytes = [0; 1024];
            let read = server.read(&mut bytes).await.unwrap();
            assert!(read > 0, "mock Redis did not receive the first command");
            let _ = seen_tx.send(());
            // Dropping the socket without a response creates the connection
            // error that starts the reconnect campaign.
        });
        seen_rx
    }

    fn hold_after_first_request(
        mut server: TcpStream,
    ) -> (oneshot::Receiver<()>, tokio::task::JoinHandle<()>) {
        let (seen_tx, seen_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut bytes = [0; 1024];
            let read = server.read(&mut bytes).await.unwrap();
            assert!(read > 0, "mock Redis did not receive the held command");
            let _ = seen_tx.send(());
            std::future::pending::<()>().await;
        });
        (seen_rx, task)
    }

    fn delayed_response_after_first_request(
        server: TcpStream,
        response: Bytes,
    ) -> (
        oneshot::Receiver<()>,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let (seen_tx, seen_rx) = oneshot::channel();
        let (respond_tx, respond_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut framed = Framed::new(server, RespCodec::new());
            framed
                .next()
                .await
                .expect("mock Redis connection closed before held request")
                .expect("mock Redis received invalid RESP");
            let _ = seen_tx.send(());
            respond_rx.await.expect("late response signal dropped");
            framed
                .send(Frame::BulkString(Some(response)))
                .await
                .expect("late response should reach the quarantined socket");
        });
        (seen_rx, respond_tx, task)
    }

    fn echo_server(server: TcpStream, expected: usize) -> tokio::task::JoinHandle<Vec<Bytes>> {
        tokio::spawn(async move {
            let mut framed = Framed::new(server, RespCodec::new());
            let mut seen = Vec::with_capacity(expected);
            while seen.len() < expected {
                let request = framed
                    .next()
                    .await
                    .expect("mock Redis connection closed early")
                    .expect("mock Redis received invalid RESP");
                let value = match request {
                    Frame::Array(Some(parts)) => match parts.get(1) {
                        Some(Frame::BulkString(Some(value))) => value.clone(),
                        other => panic!("expected ECHO bulk argument, got {other:?}"),
                    },
                    other => panic!("expected command array, got {other:?}"),
                };
                seen.push(value.clone());
                framed.send(Frame::BulkString(Some(value))).await.unwrap();
            }
            seen
        })
    }

    async fn wait_for_depth(client: &ResilientRedisClient, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if client.offline_queue_depth() == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("offline queue never reached depth {expected}"));
    }

    async fn wait_for_calls(calls: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if calls.load(Ordering::SeqCst) == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("factory never reached {expected} calls"));
    }

    fn config(
        max_retries: usize,
        base: Duration,
        connect_timeout: Option<Duration>,
    ) -> ReconnectConfig {
        ReconnectConfig {
            max_retries: Some(max_retries),
            base_delay: base,
            jitter: false,
            connect_timeout,
            ..Default::default()
        }
    }

    // -- defect 1: connect_with_timeout --

    #[tokio::test]
    async fn connect_with_timeout_propagates_factory_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let factory = FailingFactory {
            calls: calls.clone(),
        };
        // RedisConnection is not Debug, so match rather than unwrap_err.
        let result = connect_with_timeout(&factory, None).await;
        assert!(matches!(result, Err(RedisError::ConnectionClosed)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn connect_with_timeout_times_out_a_hanging_connect() {
        let calls = Arc::new(AtomicUsize::new(0));
        let factory = HangingFactory {
            calls: calls.clone(),
        };
        let start = Instant::now();
        let result = connect_with_timeout(&factory, Some(Duration::from_millis(30))).await;
        // Without the timeout this would hang forever.
        assert!(matches!(result, Err(RedisError::ConnectTimeout)));
        assert!(start.elapsed() < Duration::from_secs(2));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    // -- reconnect delay semantics --

    #[tokio::test]
    async fn reconnect_campaign_applies_base_delay_before_first_attempt() {
        let calls = Arc::new(AtomicUsize::new(0));
        let factory = FailingFactory {
            calls: calls.clone(),
        };
        let initial_delay = Duration::from_millis(20);
        let cfg = config(0, initial_delay, None);
        let events = ConnectionEventBus::new(2);
        let mut stream = events.subscribe();
        let lifecycle = ResilientLifecycleControl::new(events.clone());
        let mut campaign = Box::pin(reconnect_campaign(&factory, &cfg, Some(&lifecycle)));

        assert!(futures::poll!(campaign.as_mut()).is_pending());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 1,
                delay: initial_delay,
            }
        );

        let result = tokio::time::timeout(Duration::from_secs(1), campaign)
            .await
            .expect("campaign should run after its initial delay");
        assert!(result.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reconnect_campaign_backs_off_between_retries() {
        let calls = Arc::new(AtomicUsize::new(0));
        let factory = FailingFactory {
            calls: calls.clone(),
        };
        // 3 attempts total (initial + 2 retries) with tiny backoff.
        let cfg = config(2, Duration::from_millis(1), None);
        let result = reconnect_campaign(&factory, &cfg, None).await;
        assert!(result.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn reconnect_campaign_honors_connect_timeout_per_attempt() {
        let calls = Arc::new(AtomicUsize::new(0));
        let factory = HangingFactory {
            calls: calls.clone(),
        };
        let cfg = config(0, Duration::from_millis(1), Some(Duration::from_millis(30)));
        let result = reconnect_campaign(&factory, &cfg, None).await;
        assert!(result.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reconnect_campaign_publishes_failure_and_exhaustion_in_order() {
        let factory = FailingFactory {
            calls: Arc::new(AtomicUsize::new(0)),
        };
        let events = ConnectionEventBus::new(8);
        let mut stream = events.subscribe();
        let lifecycle = ResilientLifecycleControl::new(events.clone());
        let cfg = config(1, Duration::ZERO, None);
        assert!(
            reconnect_campaign(&factory, &cfg, Some(&lifecycle))
                .await
                .is_none()
        );

        for attempt in 1..=2 {
            assert_eq!(
                stream.recv().await.unwrap(),
                ConnectionEvent::ReconnectAttempt {
                    attempt,
                    delay: Duration::ZERO,
                }
            );
            assert!(matches!(
                stream.recv().await.unwrap(),
                ConnectionEvent::ReconnectFailed {
                    attempt: failed,
                    ..
                } if failed == attempt
            ));
        }
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectExhausted { attempts: 2 }
        );
    }

    #[tokio::test]
    async fn event_constructor_publishes_initial_connect_failure() {
        let events = ConnectionEventBus::new(2);
        let mut stream = events.subscribe();
        let result = ResilientRedisClient::with_config_and_events(
            FailingFactory {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            config(0, Duration::ZERO, None),
            events,
        )
        .await;
        assert!(matches!(result, Err(RedisError::ConnectionClosed)));
        assert!(matches!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ConnectFailed { .. }
        ));
    }

    #[tokio::test]
    async fn event_clients_publish_shutdown_once_when_final_clone_drops() {
        use crate::reconnect::ConnectionDisconnectReason;

        for offline_queue in [false, true] {
            let (connection, _server) = connection_pair().await;
            let initial = Arc::new(StdMutex::new(Some(connection)));
            let factory = {
                let initial = Arc::clone(&initial);
                move || {
                    let connection = initial
                        .lock()
                        .expect("initial connection mutex poisoned")
                        .take();
                    async move { connection.ok_or(RedisError::ConnectionClosed) }
                }
            };
            let events = ConnectionEventBus::new(4);
            let mut stream = events.subscribe();
            let client = if offline_queue {
                ResilientRedisClient::with_config_and_offline_queue_and_events(
                    factory,
                    config(0, Duration::ZERO, None),
                    OfflineQueueConfig::new(1),
                    events.clone(),
                )
                .await
                .unwrap()
            } else {
                ResilientRedisClient::with_config_and_events(
                    factory,
                    config(0, Duration::ZERO, None),
                    events.clone(),
                )
                .await
                .unwrap()
            };
            assert_eq!(stream.recv().await.unwrap(), ConnectionEvent::Connected);

            let last = client.clone();
            drop(client);
            assert!(
                tokio::time::timeout(Duration::from_millis(10), stream.recv())
                    .await
                    .is_err(),
                "dropping a non-final clone must not emit shutdown"
            );
            drop(last);
            assert_eq!(
                stream.recv().await.unwrap(),
                ConnectionEvent::Disconnected {
                    reason: ConnectionDisconnectReason::Shutdown,
                }
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(10), stream.recv())
                    .await
                    .is_err(),
                "final clone drop must emit shutdown exactly once"
            );
        }
    }

    #[tokio::test]
    async fn lifecycle_gate_orders_in_progress_publication_before_shutdown() {
        let events = ConnectionEventBus::new(4);
        let mut stream = events.subscribe();
        let lease = ResilientClientLifecycle::new(events);
        let control = lease.control();
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        let publisher = std::thread::spawn(move || {
            control.publish_with(|| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                ConnectionEvent::ReconnectFailed {
                    attempt: 1,
                    error: Arc::from("coordinated failure"),
                }
            })
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("publication never reached the between-check hook");

        let (drop_started_tx, drop_started_rx) = std::sync::mpsc::channel();
        let dropper = std::thread::spawn(move || {
            drop_started_tx.send(()).unwrap();
            drop(lease);
        });
        drop_started_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        release_tx.send(()).unwrap();

        assert!(publisher.join().unwrap());
        dropper.join().unwrap();
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectFailed {
                attempt: 1,
                error: Arc::from("coordinated failure"),
            }
        );
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::Shutdown,
            }
        );
    }

    #[tokio::test]
    async fn final_clone_drop_publishes_shutdown_during_reported_outage() {
        let (initial, initial_server) = connection_pair().await;
        let initial_seen = close_on_first_request(initial_server);
        let (unused_replacement, _replacement_server) = connection_pair().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let factory = ControlledFactory {
            calls: Arc::clone(&calls),
            initial: StdMutex::new(Some(initial)),
            replacement: Arc::new(StdMutex::new(Some(unused_replacement))),
            release_reconnect: release,
        };
        let events = ConnectionEventBus::new(8);
        let mut stream = events.subscribe();
        let client = ResilientRedisClient::with_config_and_offline_queue_and_events(
            factory,
            config(0, Duration::ZERO, None),
            OfflineQueueConfig::new(1),
            events.clone(),
        )
        .await
        .unwrap();
        assert_eq!(stream.recv().await.unwrap(), ConnectionEvent::Connected);

        let command_client = client.clone();
        let command = tokio::spawn(async move {
            command_client
                .execute(EchoCommand::new("outage", true))
                .await
        });
        initial_seen.await.unwrap();
        assert!(matches!(
            stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::ConnectionError { .. }
            }
        ));
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 1,
                delay: Duration::ZERO,
            }
        );
        wait_for_calls(&calls, 2).await;
        assert!(client.is_reconnecting());

        command.abort();
        assert!(command.await.unwrap_err().is_cancelled());
        drop(client);
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::Shutdown,
            },
            "shutdown remains observable after the outage disconnect"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(10), stream.recv())
                .await
                .is_err(),
            "terminal shutdown must be published exactly once"
        );
    }

    #[tokio::test]
    async fn shutdown_is_terminal_when_factory_and_queue_close_are_ready_together() {
        let (initial, initial_server) = connection_pair().await;
        let initial_seen = close_on_first_request(initial_server);
        let calls = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(AtomicBool::new(false));
        let factory = ReadyOnCloseFactory {
            calls: Arc::clone(&calls),
            initial: StdMutex::new(Some(initial)),
            ready: Arc::clone(&ready),
        };
        let events = ConnectionEventBus::new(8);
        let mut stream = events.subscribe();
        let client = ResilientRedisClient::with_config_and_offline_queue_and_events(
            factory,
            config(0, Duration::ZERO, None),
            OfflineQueueConfig::new(1),
            events.clone(),
        )
        .await
        .unwrap();
        assert_eq!(stream.recv().await.unwrap(), ConnectionEvent::Connected);

        let command_client = client.clone();
        let command = tokio::spawn(async move {
            command_client
                .execute(EchoCommand::new("simultaneous-close", true))
                .await
        });
        initial_seen.await.unwrap();
        assert!(matches!(
            stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::ConnectionError { .. }
            }
        ));
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 1,
                delay: Duration::ZERO,
            }
        );
        wait_for_calls(&calls, 2).await;

        // Leave one handle in the queued command, make its factory branch
        // ready without waking it, then cancel that final handle. Queue close
        // wakes a select where both connect and close are ready.
        drop(client);
        ready.store(true, Ordering::Release);
        command.abort();
        assert!(command.await.unwrap_err().is_cancelled());
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::Shutdown,
            }
        );

        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(10), stream.recv())
                .await
                .is_err(),
            "no reconnect event may be published after terminal shutdown"
        );
    }

    // -- defect 3: single-flight gate --

    #[tokio::test]
    async fn gate_skips_when_another_clone_already_reconnected() {
        let gate = ReconnectGate::new();
        // Two clones both observe generation 0 before either reconnects.
        let seen_a = gate.generation();
        let seen_b = gate.generation();

        // Clone A reconnects: it sees no change, proceeds, and marks success.
        {
            let _g = gate.enter().await;
            assert_eq!(gate.generation(), seen_a);
            gate.mark_reconnected();
        }

        // Clone B now takes the gate: the generation advanced, so it skips.
        {
            let _g = gate.enter().await;
            assert_ne!(gate.generation(), seen_b);
        }
    }

    #[tokio::test]
    async fn gate_serializes_concurrent_holders() {
        let gate = Arc::new(ReconnectGate::new());
        let held = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let gate = gate.clone();
            let held = held.clone();
            let max_seen = max_seen.clone();
            handles.push(tokio::spawn(async move {
                let _g = gate.enter().await;
                let now = held.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                held.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // The gate is exclusive: never more than one holder at a time.
        assert_eq!(max_seen.load(Ordering::SeqCst), 1);
    }

    fn wait_decision(decision: QueueDecision) -> (QueuePermit, Option<u64>) {
        match decision {
            QueueDecision::Wait { permit, campaign } => (permit, campaign),
            QueueDecision::Execute => panic!("expected queued decision, got execute"),
            QueueDecision::Reject { error, .. } => {
                panic!("expected queued decision, got {error:?}")
            }
        }
    }

    #[test]
    fn offline_queue_config_is_explicit_and_defaults_to_a_bound() {
        assert_eq!(OfflineQueueConfig::new(7).capacity(), 7);
        assert_eq!(OfflineQueueConfig::new(7).max_replay_attempts(), 3);
        assert_eq!(
            OfflineQueueConfig::new(7)
                .with_max_replay_attempts(5)
                .max_replay_attempts(),
            5
        );
        assert_eq!(OfflineQueueConfig::default().capacity(), 1024);

        let queue = OfflineQueueHandle::new(OfflineQueueConfig::new(0));
        assert!(matches!(
            queue.after_connection_error(true),
            QueueDecision::Reject {
                error: QueueRejection::Full,
                campaign: Some(0),
            }
        ));
        assert!(queue.is_reconnecting());
    }

    #[tokio::test]
    async fn offline_queue_is_bounded_ticket_ordered_and_cancellation_safe() {
        let queue = OfflineQueueHandle::new(OfflineQueueConfig::new(3));
        let (first, campaign) = wait_decision(queue.after_connection_error(true));
        assert_eq!(campaign, Some(0));
        let (second, campaign) = wait_decision(queue.before_execute(true));
        assert_eq!(campaign, None);
        let (third, campaign) = wait_decision(queue.before_execute(true));
        assert_eq!(campaign, None);
        assert_eq!(queue.depth(), 3);

        assert!(matches!(
            queue.before_execute(true),
            QueueDecision::Reject {
                error: QueueRejection::Full,
                campaign: None,
            }
        ));
        assert!(matches!(
            queue.before_execute(false),
            QueueDecision::Reject {
                error: QueueRejection::ConnectionClosed,
                campaign: None,
            }
        ));

        queue.inner.finish_reconnect(0, None);
        first.wait_for_turn().await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), second.wait_for_turn())
                .await
                .is_err(),
            "the second ticket must not overtake the first"
        );

        // Canceling the head removes it synchronously and wakes its successor.
        drop(first);
        second.wait_for_turn().await.unwrap();
        drop(second);
        third.wait_for_turn().await.unwrap();
        drop(third);
        assert_eq!(queue.depth(), 0);
        assert!(matches!(queue.state().phase, OfflinePhase::Connected));
    }

    #[tokio::test]
    async fn failed_campaign_rejects_every_admitted_ticket_deterministically() {
        let queue = OfflineQueueHandle::new(OfflineQueueConfig::new(2));
        let (first, campaign) = wait_decision(queue.after_connection_error(true));
        let campaign = campaign.unwrap();
        let (second, _) = wait_decision(queue.before_execute(true));
        let cause = Arc::new(RedisError::ConnectTimeout);
        queue.inner.finish_reconnect(
            campaign,
            Some(ReconnectFailure {
                attempts: 3,
                last_error: Arc::clone(&cause),
            }),
        );

        let mut waiter_causes = Vec::new();
        for permit in [&first, &second] {
            let error = permit.wait_for_turn().await.unwrap_err();
            let RedisError::ReconnectFailed {
                attempts: 3,
                last_error,
            } = error
            else {
                panic!("expected structured reconnect exhaustion")
            };
            assert!(matches!(&*last_error, RedisError::ConnectTimeout));
            assert!(Arc::ptr_eq(&last_error, &cause));
            waiter_causes.push(last_error);
        }
        assert!(Arc::ptr_eq(&waiter_causes[0], &waiter_causes[1]));

        let QueueDecision::Reject {
            error:
                QueueRejection::ReconnectFailed {
                    attempts: 3,
                    last_error,
                },
            campaign: None,
        } = queue.before_execute(true)
        else {
            panic!("new work should observe the same reconnect exhaustion")
        };
        assert!(matches!(&*last_error, RedisError::ConnectTimeout));
        assert!(Arc::ptr_eq(&last_error, &cause));

        assert!(matches!(
            queue.before_execute(false),
            QueueDecision::Reject {
                error: QueueRejection::ConnectionClosed,
                campaign: None,
            }
        ));

        drop(first);
        drop(second);
        let (replacement, next_campaign) = wait_decision(queue.before_execute(true));
        assert_eq!(next_campaign, Some(1));
        drop(replacement);
    }

    #[tokio::test]
    async fn closing_queue_wakes_waiters_with_connection_closed() {
        let queue = OfflineQueueHandle::new(OfflineQueueConfig::new(1));
        let (permit, _) = wait_decision(queue.after_connection_error(true));
        queue.inner.close();

        assert!(matches!(
            permit.wait_for_turn().await,
            Err(RedisError::ConnectionClosed)
        ));
        assert!(matches!(
            queue.before_execute(true),
            QueueDecision::Reject {
                error: QueueRejection::ConnectionClosed,
                campaign: None,
            }
        ));
    }

    #[tokio::test]
    async fn dropping_last_queue_owner_cancels_hanging_reconnect() {
        let queue = OfflineQueueHandle::new(OfflineQueueConfig::new(1));
        let mut closed = queue.inner.closed_tx.subscribe();
        let calls = Arc::new(AtomicUsize::new(0));
        let factory = HangingFactory {
            calls: Arc::clone(&calls),
        };
        let config = config(0, Duration::ZERO, None);

        let task = tokio::spawn(async move {
            reconnect_campaign_until_closed(&factory, &config, &mut closed, None).await
        });
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        drop(queue);

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .expect("queue closure should cancel a hanging connect")
                .unwrap(),
            CancellableReconnect::Closed
        ));
    }

    #[tokio::test]
    async fn idempotent_commands_replay_in_ticket_order_with_one_campaign() {
        let (initial, initial_server) = connection_pair().await;
        let initial_seen = close_on_first_request(initial_server);
        let (replacement, replacement_server) = connection_pair().await;
        let replacement_task = echo_server(replacement_server, 3);

        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let factory = ControlledFactory {
            calls: Arc::clone(&calls),
            initial: StdMutex::new(Some(initial)),
            replacement: Arc::new(StdMutex::new(Some(replacement))),
            release_reconnect: Arc::clone(&release),
        };
        let client = ResilientRedisClient::with_config_and_offline_queue(
            factory,
            config(0, Duration::ZERO, None),
            OfflineQueueConfig::new(3),
        )
        .await
        .unwrap();

        let first_client = client.clone();
        let first =
            tokio::spawn(
                async move { first_client.execute(EchoCommand::new("first", true)).await },
            );
        initial_seen.await.unwrap();
        wait_for_depth(&client, 1).await;
        assert!(client.is_reconnecting());

        let second_client = client.clone();
        let second = tokio::spawn(async move {
            second_client
                .execute(EchoCommand::new("second", true))
                .await
        });
        wait_for_depth(&client, 2).await;
        let third_client = client.clone();
        let third =
            tokio::spawn(
                async move { third_client.execute(EchoCommand::new("third", true)).await },
            );
        wait_for_depth(&client, 3).await;

        assert_eq!(calls.load(Ordering::SeqCst), 2, "one reconnect campaign");
        release.add_permits(1);

        assert_eq!(first.await.unwrap().unwrap(), Bytes::from_static(b"first"));
        assert_eq!(
            second.await.unwrap().unwrap(),
            Bytes::from_static(b"second")
        );
        assert_eq!(third.await.unwrap().unwrap(), Bytes::from_static(b"third"));
        assert_eq!(
            replacement_task.await.unwrap(),
            vec![
                Bytes::from_static(b"first"),
                Bytes::from_static(b"second"),
                Bytes::from_static(b"third"),
            ]
        );
        assert_eq!(client.offline_queue_depth(), 0);
        assert!(!client.is_reconnecting());
    }

    #[tokio::test]
    async fn offline_queue_campaign_publishes_ordered_lifecycle_events() {
        use crate::reconnect::ConnectionDisconnectReason;

        let (initial, initial_server) = connection_pair().await;
        let initial_seen = close_on_first_request(initial_server);
        let (replacement, replacement_server) = connection_pair().await;
        let replacement_task = echo_server(replacement_server, 1);
        let release = Arc::new(Semaphore::new(0));
        let factory = ControlledFactory {
            calls: Arc::new(AtomicUsize::new(0)),
            initial: StdMutex::new(Some(initial)),
            replacement: Arc::new(StdMutex::new(Some(replacement))),
            release_reconnect: Arc::clone(&release),
        };
        let events = ConnectionEventBus::new(8);
        let mut event_stream = events.subscribe();
        let client = ResilientRedisClient::with_config_and_offline_queue_and_events(
            factory,
            config(0, Duration::ZERO, None),
            OfflineQueueConfig::new(1),
            events,
        )
        .await
        .unwrap();
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Connected
        );

        let command_client = client.clone();
        let command = tokio::spawn(async move {
            command_client
                .execute(EchoCommand::new("eventful", true))
                .await
        });
        initial_seen.await.unwrap();
        assert!(matches!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: ConnectionDisconnectReason::ConnectionError { .. }
            }
        ));
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 1,
                delay: Duration::ZERO,
            }
        );

        release.add_permits(1);
        assert_eq!(
            command.await.unwrap().unwrap(),
            Bytes::from_static(b"eventful")
        );
        assert!(matches!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Reconnected { attempts: 1, .. }
        ));
        assert_eq!(
            replacement_task.await.unwrap(),
            vec![Bytes::from_static(b"eventful")]
        );
    }

    #[tokio::test]
    async fn default_client_reconnects_but_does_not_replay_idempotent_command() {
        let (initial, initial_server) = connection_pair().await;
        let initial_seen = close_on_first_request(initial_server);
        let (replacement, mut replacement_server) = connection_pair().await;

        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let factory = ControlledFactory {
            calls: Arc::clone(&calls),
            initial: StdMutex::new(Some(initial)),
            replacement: Arc::new(StdMutex::new(Some(replacement))),
            release_reconnect: Arc::clone(&release),
        };
        let client = ResilientRedisClient::with_config(factory, config(0, Duration::ZERO, None))
            .await
            .unwrap();

        let command_client = client.clone();
        let command = tokio::spawn(async move {
            command_client
                .execute(EchoCommand::new("not-replayed", true))
                .await
        });
        initial_seen.await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while calls.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("default reconnect campaign did not start");
        assert!(client.is_reconnecting());
        release.add_permits(1);

        assert!(command.await.unwrap().is_err());
        let mut bytes = [0; 128];
        assert!(
            tokio::time::timeout(
                Duration::from_millis(25),
                replacement_server.read(&mut bytes)
            )
            .await
            .is_err(),
            "default construction must not replay the failed command"
        );
        assert_eq!(client.offline_queue_depth(), 0);
        assert!(!client.is_reconnecting());
    }

    #[tokio::test]
    async fn non_idempotent_commands_fail_instead_of_waiting_or_replaying() {
        let (initial, initial_server) = connection_pair().await;
        let initial_seen = close_on_first_request(initial_server);
        let (replacement, replacement_server) = connection_pair().await;
        let replacement_task = echo_server(replacement_server, 1);

        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let factory = ControlledFactory {
            calls: Arc::clone(&calls),
            initial: StdMutex::new(Some(initial)),
            replacement: Arc::new(StdMutex::new(Some(replacement))),
            release_reconnect: Arc::clone(&release),
        };
        let client = ResilientRedisClient::with_config_and_offline_queue(
            factory,
            config(0, Duration::ZERO, None),
            OfflineQueueConfig::new(2),
        )
        .await
        .unwrap();

        let discovering_client = client.clone();
        let discovering = tokio::spawn(async move {
            discovering_client
                .execute(EchoCommand::new("unsafe-first", false))
                .await
        });
        initial_seen.await.unwrap();
        assert!(discovering.await.unwrap().is_err());

        tokio::time::timeout(Duration::from_secs(1), async {
            while !client.is_reconnecting() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reconnect campaign did not start");

        let error = client
            .execute(EchoCommand::new("unsafe-while-offline", false))
            .await
            .unwrap_err();
        assert!(matches!(error, RedisError::ConnectionClosed));
        let mut raw_client = client.clone();
        let raw_error = raw_client
            .call(array(vec![bulk("ECHO"), bulk("raw-while-offline")]))
            .await
            .unwrap_err();
        assert!(matches!(raw_error, RedisError::ConnectionClosed));
        assert_eq!(client.offline_queue_depth(), 0);

        let safe_client = client.clone();
        let safe =
            tokio::spawn(async move { safe_client.execute(EchoCommand::new("safe", true)).await });
        wait_for_depth(&client, 1).await;
        release.add_permits(1);
        assert_eq!(safe.await.unwrap().unwrap(), Bytes::from_static(b"safe"));
        assert_eq!(
            replacement_task.await.unwrap(),
            vec![Bytes::from_static(b"safe")],
            "neither non-idempotent command may be replayed"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn already_expired_offline_command_never_reaches_wire() {
        let (connection, mut server) = connection_pair().await;
        let initial = Arc::new(StdMutex::new(Some(connection)));
        let factory = {
            let initial = Arc::clone(&initial);
            move || {
                let connection = initial
                    .lock()
                    .expect("initial connection mutex poisoned")
                    .take();
                async move { connection.ok_or(RedisError::ConnectionClosed) }
            }
        };
        let client = ResilientRedisClient::with_config_and_offline_queue(
            factory,
            config(0, Duration::ZERO, None),
            OfflineQueueConfig::new(1),
        )
        .await
        .unwrap();

        let result = client
            .execute(WithDeadline::new(
                EchoCommand::new("expired", true),
                tokio::time::Instant::now() - Duration::from_millis(1),
            ))
            .await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(client.offline_queue_depth(), 0);
        assert!(!client.is_reconnecting());
        let mut bytes = [0; 128];
        assert!(
            tokio::time::timeout(Duration::from_millis(25), server.read(&mut bytes))
                .await
                .is_err(),
            "an already-expired command reached the socket"
        );
    }

    #[tokio::test]
    async fn queued_deadline_expiry_releases_ticket_without_replay() {
        let (initial, initial_server) = connection_pair().await;
        let initial_seen = close_on_first_request(initial_server);
        let (replacement, replacement_server) = connection_pair().await;
        let replacement_task = echo_server(replacement_server, 1);

        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let factory = ControlledFactory {
            calls: Arc::clone(&calls),
            initial: StdMutex::new(Some(initial)),
            replacement: Arc::new(StdMutex::new(Some(replacement))),
            release_reconnect: Arc::clone(&release),
        };
        let client = ResilientRedisClient::with_config_and_offline_queue(
            factory,
            config(0, Duration::ZERO, None),
            OfflineQueueConfig::new(2),
        )
        .await
        .unwrap();

        let expiring_client = client.clone();
        let expiring = tokio::spawn(async move {
            expiring_client
                .execute(WithDeadline::after(
                    EchoCommand::new("expired", true),
                    Duration::from_millis(250),
                ))
                .await
        });
        initial_seen.await.unwrap();
        wait_for_depth(&client, 1).await;

        assert!(matches!(
            expiring.await.unwrap(),
            Err(RedisError::CommandTimeout)
        ));
        wait_for_depth(&client, 0).await;

        // Finish the shared reconnect after the expired ticket has left the
        // queue. Only the later command may reach the replacement socket.
        wait_for_calls(&calls, 2).await;
        release.add_permits(1);
        assert_eq!(
            client
                .execute(EchoCommand::new("later", true))
                .await
                .unwrap(),
            Bytes::from_static(b"later")
        );
        assert_eq!(
            replacement_task.await.unwrap(),
            vec![Bytes::from_static(b"later")]
        );
        assert_eq!(client.offline_queue_depth(), 0);
    }

    #[tokio::test]
    async fn in_wire_deadline_timeout_quarantines_socket_before_successor() {
        let (initial, initial_server) = connection_pair().await;
        let (initial_seen, held_server) = hold_after_first_request(initial_server);
        let (replacement, replacement_server) = connection_pair().await;
        let replacement_task = echo_server(replacement_server, 1);

        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let factory = ControlledFactory {
            calls: Arc::clone(&calls),
            initial: StdMutex::new(Some(initial)),
            replacement: Arc::new(StdMutex::new(Some(replacement))),
            release_reconnect: Arc::clone(&release),
        };
        let client = ResilientRedisClient::with_config_and_offline_queue(
            factory,
            config(0, Duration::ZERO, None),
            OfflineQueueConfig::new(2),
        )
        .await
        .unwrap();

        let timed_client = client.clone();
        let timed = tokio::spawn(async move {
            timed_client
                .execute(WithDeadline::after(
                    EchoCommand::new("timed-out", true),
                    Duration::from_millis(250),
                ))
                .await
        });
        initial_seen.await.unwrap();
        assert!(matches!(
            timed.await.unwrap(),
            Err(RedisError::CommandTimeout)
        ));

        // ProtocolAlignmentGuard starts replacement synchronously when the
        // deadline drops an exchange that was already on the wire.
        wait_for_calls(&calls, 2).await;
        assert!(client.is_reconnecting());
        let later_client = client.clone();
        let later =
            tokio::spawn(
                async move { later_client.execute(EchoCommand::new("later", true)).await },
            );
        wait_for_depth(&client, 1).await;
        release.add_permits(1);

        assert_eq!(later.await.unwrap().unwrap(), Bytes::from_static(b"later"));
        assert_eq!(
            replacement_task.await.unwrap(),
            vec![Bytes::from_static(b"later")]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(client.offline_queue_depth(), 0);
        assert!(!client.is_reconnecting());
        held_server.abort();
    }

    #[tokio::test]
    async fn canceling_head_command_does_not_cancel_shared_reconnect() {
        let (initial, initial_server) = connection_pair().await;
        let initial_seen = close_on_first_request(initial_server);
        let (replacement, replacement_server) = connection_pair().await;
        let replacement_task = echo_server(replacement_server, 1);

        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let factory = ControlledFactory {
            calls: Arc::clone(&calls),
            initial: StdMutex::new(Some(initial)),
            replacement: Arc::new(StdMutex::new(Some(replacement))),
            release_reconnect: Arc::clone(&release),
        };
        let client = ResilientRedisClient::with_config_and_offline_queue(
            factory,
            config(0, Duration::ZERO, None),
            OfflineQueueConfig::new(2),
        )
        .await
        .unwrap();

        let canceled_client = client.clone();
        let canceled = tokio::spawn(async move {
            canceled_client
                .execute(EchoCommand::new("canceled", true))
                .await
        });
        initial_seen.await.unwrap();
        wait_for_depth(&client, 1).await;

        let surviving_client = client.clone();
        let surviving = tokio::spawn(async move {
            surviving_client
                .execute(EchoCommand::new("survives", true))
                .await
        });
        wait_for_depth(&client, 2).await;
        assert!(matches!(
            client.execute(EchoCommand::new("overflow", true)).await,
            Err(RedisError::QueueFull)
        ));
        canceled.abort();
        assert!(canceled.await.unwrap_err().is_cancelled());
        wait_for_depth(&client, 1).await;

        release.add_permits(1);
        assert_eq!(
            surviving.await.unwrap().unwrap(),
            Bytes::from_static(b"survives")
        );
        assert_eq!(
            replacement_task.await.unwrap(),
            vec![Bytes::from_static(b"survives")]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn canceling_initial_wire_attempt_replaces_socket_before_later_command() {
        let (initial, initial_server) = connection_pair().await;
        let (initial_seen, held_server) = hold_after_first_request(initial_server);
        let (replacement, replacement_server) = connection_pair().await;
        let replacement_task = echo_server(replacement_server, 1);

        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let factory = ControlledFactory {
            calls: Arc::clone(&calls),
            initial: StdMutex::new(Some(initial)),
            replacement: Arc::new(StdMutex::new(Some(replacement))),
            release_reconnect: Arc::clone(&release),
        };
        let client = ResilientRedisClient::with_config_and_offline_queue(
            factory,
            config(0, Duration::ZERO, None),
            OfflineQueueConfig::new(2),
        )
        .await
        .unwrap();

        let canceled_client = client.clone();
        let canceled = tokio::spawn(async move {
            canceled_client
                .execute(EchoCommand::new("abandoned", true))
                .await
        });
        initial_seen.await.unwrap();
        canceled.abort();
        assert!(canceled.await.unwrap_err().is_cancelled());

        // Dropping the in-wire future synchronously starts one replacement
        // campaign. A later command waits rather than reading the abandoned
        // ECHO response from the original socket.
        wait_for_calls(&calls, 2).await;
        assert!(client.is_reconnecting());
        let later_client = client.clone();
        let later =
            tokio::spawn(
                async move { later_client.execute(EchoCommand::new("later", true)).await },
            );
        wait_for_depth(&client, 1).await;
        release.add_permits(1);

        assert_eq!(later.await.unwrap().unwrap(), Bytes::from_static(b"later"));
        assert_eq!(
            replacement_task.await.unwrap(),
            vec![Bytes::from_static(b"later")]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2, "one reconnect campaign");
        assert_eq!(client.offline_queue_depth(), 0);
        assert!(!client.is_reconnecting());
        held_server.abort();
    }

    #[tokio::test]
    async fn canceling_raw_wire_attempt_replaces_socket_before_waiting_successor() {
        let (initial, initial_server) = connection_pair().await;
        let (initial_seen, send_late_response, late_response_task) =
            delayed_response_after_first_request(
                initial_server,
                Bytes::from_static(b"abandoned-raw"),
            );
        let (replacement, replacement_server) = connection_pair().await;
        let replacement_task = echo_server(replacement_server, 1);

        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let factory = ControlledFactory {
            calls: Arc::clone(&calls),
            initial: StdMutex::new(Some(initial)),
            replacement: Arc::new(StdMutex::new(Some(replacement))),
            release_reconnect: Arc::clone(&release),
        };
        let client = ResilientRedisClient::with_config_and_offline_queue(
            factory,
            config(0, Duration::ZERO, None),
            OfflineQueueConfig::new(2),
        )
        .await
        .unwrap();

        let mut raw_client = client.clone();
        let canceled = tokio::spawn(async move {
            raw_client
                .call(array(vec![bulk("ECHO"), bulk("abandoned-raw")]))
                .await
        });
        initial_seen.await.unwrap();

        // Put the successor behind the raw call's connection mutex before
        // aborting it. Guard drop must mark the connection offline before that
        // mutex is released, forcing the successor into the queue.
        let later_client = client.clone();
        let later = tokio::spawn(async move {
            later_client
                .execute(EchoCommand::new("after-raw", true))
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !later.is_finished(),
            "successor should wait behind raw request"
        );

        canceled.abort();
        assert!(canceled.await.unwrap_err().is_cancelled());
        wait_for_calls(&calls, 2).await;
        wait_for_depth(&client, 1).await;

        // Deliver the abandoned raw response while the replacement factory is
        // still gated. It remains buffered on the quarantined socket and must
        // never satisfy the already-waiting successor.
        send_late_response.send(()).unwrap();
        late_response_task.await.unwrap();
        assert!(!later.is_finished());
        release.add_permits(1);

        assert_eq!(
            later.await.unwrap().unwrap(),
            Bytes::from_static(b"after-raw")
        );
        assert_eq!(
            replacement_task.await.unwrap(),
            vec![Bytes::from_static(b"after-raw")]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2, "one reconnect campaign");
        assert_eq!(client.offline_queue_depth(), 0);
        assert!(!client.is_reconnecting());
    }

    #[tokio::test]
    async fn canceling_queued_wire_replay_replaces_socket_before_next_ticket() {
        let (initial, initial_server) = connection_pair().await;
        let initial_seen = close_on_first_request(initial_server);
        let (first_replacement, first_replacement_server) = connection_pair().await;
        let (replay_seen, held_replay_server) = hold_after_first_request(first_replacement_server);
        let (fresh_replacement, fresh_replacement_server) = connection_pair().await;
        let fresh_server_task = echo_server(fresh_replacement_server, 1);

        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let factory = SequencedFactory {
            calls: Arc::clone(&calls),
            initial: StdMutex::new(Some(initial)),
            replacements: Arc::new(StdMutex::new(VecDeque::from([
                first_replacement,
                fresh_replacement,
            ]))),
            release_reconnect: Arc::clone(&release),
        };
        let client = ResilientRedisClient::with_config_and_offline_queue(
            factory,
            config(0, Duration::ZERO, None),
            OfflineQueueConfig::new(2),
        )
        .await
        .unwrap();

        let canceled_client = client.clone();
        let canceled = tokio::spawn(async move {
            canceled_client
                .execute(EchoCommand::new("head", true))
                .await
        });
        initial_seen.await.unwrap();
        wait_for_depth(&client, 1).await;
        wait_for_calls(&calls, 2).await;
        release.add_permits(1);
        replay_seen.await.unwrap();

        canceled.abort();
        assert!(canceled.await.unwrap_err().is_cancelled());
        wait_for_calls(&calls, 3).await;
        assert!(client.is_reconnecting());

        let later_client = client.clone();
        let later = tokio::spawn(async move {
            later_client
                .execute(EchoCommand::new("next-ticket", true))
                .await
        });
        wait_for_depth(&client, 1).await;
        release.add_permits(1);

        assert_eq!(
            later.await.unwrap().unwrap(),
            Bytes::from_static(b"next-ticket")
        );
        assert_eq!(
            fresh_server_task.await.unwrap(),
            vec![Bytes::from_static(b"next-ticket")]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3, "two reconnect campaigns");
        assert_eq!(client.offline_queue_depth(), 0);
        assert!(!client.is_reconnecting());
        held_replay_server.abort();
    }

    #[tokio::test]
    async fn replay_attempt_limit_bounds_successful_connect_disconnect_churn() {
        let (initial, initial_server) = connection_pair().await;
        let initial_seen = close_on_first_request(initial_server);
        let (replacement_one, server_one) = connection_pair().await;
        let replay_one_seen = close_on_first_request(server_one);
        let (replacement_two, server_two) = connection_pair().await;
        let replay_two_seen = close_on_first_request(server_two);
        let (repair, repair_server) = connection_pair().await;
        let repair_task = echo_server(repair_server, 1);

        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let factory = SequencedFactory {
            calls: Arc::clone(&calls),
            initial: StdMutex::new(Some(initial)),
            replacements: Arc::new(StdMutex::new(VecDeque::from([
                replacement_one,
                replacement_two,
                repair,
            ]))),
            release_reconnect: Arc::clone(&release),
        };
        let client = ResilientRedisClient::with_config_and_offline_queue(
            factory,
            config(0, Duration::ZERO, None),
            OfflineQueueConfig::new(2).with_max_replay_attempts(2),
        )
        .await
        .unwrap();

        let head_client = client.clone();
        let head = tokio::spawn(async move {
            head_client
                .execute(EchoCommand::new("bounded-head", true))
                .await
        });
        initial_seen.await.unwrap();
        wait_for_calls(&calls, 2).await;
        release.add_permits(1);
        replay_one_seen.await.unwrap();
        wait_for_calls(&calls, 3).await;
        release.add_permits(1);
        replay_two_seen.await.unwrap();

        let error = head.await.unwrap().unwrap_err();
        assert!(matches!(
            error,
            RedisError::ReconnectFailed { attempts: 2, .. }
        ));
        wait_for_calls(&calls, 4).await;
        assert_eq!(client.offline_queue_depth(), 0);
        assert!(client.is_reconnecting());

        let later_client = client.clone();
        let later = tokio::spawn(async move {
            later_client
                .execute(EchoCommand::new("after-limit", true))
                .await
        });
        wait_for_depth(&client, 1).await;
        release.add_permits(1);
        assert_eq!(
            later.await.unwrap().unwrap(),
            Bytes::from_static(b"after-limit")
        );
        assert_eq!(
            repair_task.await.unwrap(),
            vec![Bytes::from_static(b"after-limit")]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(client.offline_queue_depth(), 0);
        assert!(!client.is_reconnecting());
    }
}
