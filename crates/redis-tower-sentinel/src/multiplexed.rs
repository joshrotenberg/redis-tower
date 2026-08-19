//! Multiplexed sentinel-managed Redis client.
//!
//! [`MultiplexedSentinelClient`] batches concurrent requests from multiple
//! tasks into Redis pipelines automatically. It uses a single TCP connection
//! to the sentinel-discovered master, with a background worker shared across
//! all clones.
//!
//! # When to use
//!
//! - Many tasks issuing independent commands concurrently against a
//!   sentinel-managed Redis deployment
//! - Read-heavy workloads where connection pool overhead is undesirable
//! - High-concurrency scenarios where [`crate::SentinelClient`]'s mutex
//!   becomes a bottleneck
//!
//! For transactions (MULTI/EXEC) or commands requiring exclusive connection
//! access, use [`crate::SentinelConnection`] directly.
//!
//! # Example
//!
//! ```no_run
//! use redis_tower_sentinel::MultiplexedSentinelClient;
//! use redis_tower_commands::{Get, Set};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = MultiplexedSentinelClient::connect(
//!     &["127.0.0.1:26379"],
//!     "mymaster",
//! ).await?;
//!
//! // Clone and share across tasks -- all share the same connection.
//! let c = client.clone();
//! tokio::spawn(async move {
//!     c.execute(Set::new("key", "value")).await.unwrap();
//! });
//!
//! let val: Option<bytes::Bytes> = client.execute(Get::new("key")).await?;
//! # let _ = val;
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use redis_tower::auto_pipeline::{
    AutoPipelineConfig, AutoPipelineReconnectConfig, AutoPipelineService,
};
use redis_tower::command_adapter::CommandAdapter;
use redis_tower::credentials::{
    CredentialProvider, CredentialReauthenticationHandle, Credentials, StreamingCredentialProvider,
    spawn_credential_reauthentication as spawn_reauthentication_task,
};
use redis_tower::{
    ConnectionEvent, ConnectionEventBus, NodeAddr, ReadPreference, ReadRoutingStrategy,
    RoundRobinRouting, is_readonly_command,
};
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::RespLimits;
use tower_service::Service;

use crate::discovery::{self, SentinelConfig};

/// Builder for [`MultiplexedSentinelClient`].
///
/// Obtain one via [`MultiplexedSentinelClient::builder`].
///
/// # Example
///
/// ```no_run
/// use redis_tower_sentinel::MultiplexedSentinelClient;
/// use redis_tower::credentials::StaticCredentials;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = MultiplexedSentinelClient::builder(
///     &["127.0.0.1:26379"],
///     "mymaster",
/// )
/// .sentinel_credentials(StaticCredentials::password("sentinel_pass"))
/// .node_credentials(StaticCredentials::password("redis_pass"))
/// .connect_with_reconnect()
/// .await?;
/// # let _ = client;
/// # Ok(())
/// # }
/// ```
pub struct MultiplexedSentinelClientBuilder {
    sentinel_addrs: Vec<String>,
    master_name: String,
    config: SentinelConfig,
    connection_events: Option<ConnectionEventBus>,
    read_preference: ReadPreference,
    read_routing: Option<Arc<dyn ReadRoutingStrategy>>,
}

impl MultiplexedSentinelClientBuilder {
    /// Authenticate sentinel connections with the given credential provider.
    ///
    /// Called once per sentinel query. Supports dynamic credentials (token
    /// rotation) via a custom [`CredentialProvider`] implementation.
    pub fn sentinel_credentials(mut self, provider: impl CredentialProvider) -> Self {
        self.config.sentinel_credentials = Some(Arc::new(provider));
        self
    }

    /// Authenticate master (node) connections with the given credential provider.
    ///
    /// Sentinels and the data node commonly use different passwords in production.
    /// This credential is also used on every reconnect, so failover is
    /// re-authenticated automatically.
    pub fn node_credentials(mut self, provider: impl CredentialProvider) -> Self {
        self.config.node_credentials = Some(Arc::new(provider));
        self
    }

    /// Set RESP decode limits for every sentinel and Redis data-node connection.
    ///
    /// The limits are retained by the reconnect factory, so discovery and data
    /// connections opened after failover enforce the same bounds. Defaults to
    /// [`RespLimits::default`].
    pub fn resp_limits(mut self, limits: RespLimits) -> Self {
        self.config.resp_limits = limits;
        self
    }

    /// Publish connection and verified master-failover lifecycle events.
    ///
    /// Subscribe to the bus before connecting to observe the initial
    /// [`ConnectionEvent::Connected`] or [`ConnectionEvent::ConnectFailed`]
    /// event. Initial discovery, data-node connection, and ROLE verification
    /// failures are all reported as `ConnectFailed`. During reconnect,
    /// Sentinel's ROLE-verified master endpoint string is compared with the
    /// previously connected endpoint. An exact address-string change emits
    /// [`ConnectionEvent::Failover`] before [`ConnectionEvent::Reconnected`].
    /// This is an endpoint transition, not durable Redis node identity:
    /// textual aliases for the same server can look different, while replacing
    /// a node behind the same endpoint does not emit `Failover`.
    pub fn connection_events(mut self, events: ConnectionEventBus) -> Self {
        self.connection_events = Some(events);
        self
    }

    /// Set the read preference for routing read-only commands.
    ///
    /// Defaults to [`ReadPreference::Master`]. When set to
    /// [`ReadPreference::Replica`] or [`ReadPreference::PreferReplica`],
    /// `connect`/`connect_with_reconnect` additionally discover the
    /// monitored replicas via sentinel and open one shared, automatically
    /// pipelined connection to each reachable replica. Read-only commands (as
    /// classified by [`redis_tower::is_readonly_command`]) select among those
    /// connections with [`read_routing`](Self::read_routing) on every call.
    ///
    /// The two non-default variants differ on what happens when no replica
    /// is connected (none discovered, or every discovered replica refused the
    /// connection): [`ReadPreference::Replica`] returns an error for the
    /// read rather than silently serving it from the master, while
    /// [`ReadPreference::PreferReplica`] falls back to the master. Writes
    /// are unaffected either way and always go to the master.
    ///
    /// Replica connections do not currently participate in
    /// `connect_with_reconnect`'s Sentinel-failover reconnect loop: it is
    /// discovered and connected once at construction. If one drops, reads
    /// routed to it return an error rather than falling back to the master or
    /// reconnecting; reconstructing the client refreshes the replica set.
    pub fn read_preference(mut self, pref: ReadPreference) -> Self {
        self.read_preference = pref;
        self
    }

    /// Set a custom read routing strategy for replica selection.
    ///
    /// See [`read_preference`](Self::read_preference). If not set, defaults
    /// to [`RoundRobinRouting`].
    pub fn read_routing(mut self, strategy: impl ReadRoutingStrategy) -> Self {
        self.read_routing = Some(Arc::new(strategy));
        self
    }

    /// Set the TLS configuration for sentinel connections.
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn sentinel_tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        self.config.sentinel_tls = Some(Arc::new(tls));
        self
    }

    /// Set the TLS configuration for node (master) connections.
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn node_tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        self.config.node_tls = Some(Arc::new(tls));
        self
    }

    /// Set the same TLS configuration for both sentinel and node connections.
    ///
    /// Convenience method when both hops share the same TLS settings.
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        let shared = Arc::new(tls);
        self.config.sentinel_tls = Some(shared.clone());
        self.config.node_tls = Some(shared);
        self
    }

    /// Connect to the sentinel-discovered master (no automatic reconnection).
    ///
    /// For production use with failover support, prefer
    /// [`connect_with_reconnect`](Self::connect_with_reconnect).
    pub async fn connect(
        self,
    ) -> Result<MultiplexedSentinelClient<AutoPipelineService>, RedisError> {
        let initial = async {
            let master_addr = discovery::discover_master_with_config(
                &self.sentinel_addrs,
                &self.master_name,
                &self.config,
            )
            .await?;
            discovery::connect_hop(
                &master_addr,
                self.config.node_credentials.as_ref(),
                self.config.resp_limits,
                #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
                self.config.node_tls.as_ref(),
            )
            .await
        }
        .await;
        let conn = match initial {
            Ok(conn) => conn,
            Err(error) => {
                if let Some(events) = &self.connection_events {
                    events.publish_with(|| ConnectionEvent::ConnectFailed {
                        error: Arc::from(error.to_string()),
                    });
                }
                return Err(error);
            }
        };
        let read_preference = self.read_preference;
        let read_routing = self
            .read_routing
            .clone()
            .unwrap_or_else(|| Arc::new(RoundRobinRouting::new()));
        let (replicas, replica_addrs) = if read_preference == ReadPreference::Master {
            (HashMap::new(), Vec::new())
        } else {
            let (mut connections, replica_addrs) =
                discovery::connect_replicas(&self.sentinel_addrs, &self.master_name, &self.config)
                    .await;
            let replicas = replica_addrs
                .iter()
                .filter_map(|addr| {
                    let key = addr.addr_string();
                    connections.remove(&key).map(|conn| {
                        (
                            key,
                            CommandAdapter::new(AutoPipelineService::new(
                                conn,
                                AutoPipelineConfig::default(),
                            )),
                        )
                    })
                })
                .collect();
            (replicas, replica_addrs)
        };
        let pipeline = match self.connection_events {
            Some(events) => {
                AutoPipelineService::new_with_events(conn, AutoPipelineConfig::default(), events)
            }
            None => AutoPipelineService::new(conn, AutoPipelineConfig::default()),
        };
        Ok(MultiplexedSentinelClient {
            inner: CommandAdapter::new(pipeline),
            replicas,
            replica_addrs,
            read_preference,
            read_routing,
        })
    }

    /// Connect with automatic reconnection via sentinel discovery.
    ///
    /// On connection failure (or READONLY from a demoted master), the factory
    /// re-queries sentinel to find the current master. The reconnected master
    /// connection respects the configured node credentials, TLS, and RESP
    /// decode limits.
    pub async fn connect_with_reconnect(
        self,
    ) -> Result<MultiplexedSentinelClient<AutoPipelineService>, RedisError> {
        let addrs = self.sentinel_addrs;
        let name = self.master_name;
        let config = self.config;
        let events = self.connection_events;
        let master_tracker = events
            .as_ref()
            .map(|events| VerifiedMasterTracker::new(events.clone()));

        let read_preference = self.read_preference;
        let read_routing = self
            .read_routing
            .clone()
            .unwrap_or_else(|| Arc::new(RoundRobinRouting::new()));
        // Discover and connect before `addrs`/`name`/`config` are moved into
        // the reconnect factory below. See `read_preference`'s doc comment:
        // replica connections do not participate in that factory's loop.
        let (replicas, replica_addrs) = if read_preference == ReadPreference::Master {
            (HashMap::new(), Vec::new())
        } else {
            let (mut connections, replica_addrs) =
                discovery::connect_replicas(&addrs, &name, &config).await;
            let replicas = replica_addrs
                .iter()
                .filter_map(|addr| {
                    let key = addr.addr_string();
                    connections.remove(&key).map(|conn| {
                        (
                            key,
                            CommandAdapter::new(AutoPipelineService::new(
                                conn,
                                AutoPipelineConfig::default(),
                            )),
                        )
                    })
                })
                .collect();
            (replicas, replica_addrs)
        };

        let factory = move || {
            let addrs = addrs.clone();
            let name = name.clone();
            let config = config.clone();
            let master_tracker = master_tracker.clone();
            async move {
                // Verify ROLE so a reconnect lands on a real master, not a
                // demoted replica that sentinel still reports during a failover.
                let (conn, addr) =
                    discovery::connect_verified_master_with_config(&addrs, &name, &config).await?;
                if let Some(tracker) = master_tracker {
                    tracker.observe(&addr);
                }
                Ok(conn)
            }
        };
        // Enable READONLY-triggered reconnect: if the master is demoted to a
        // replica with TCP intact, writes return READONLY (not a connection
        // error), and the worker must rebuild via the factory to find the new
        // master instead of wedging on the demoted node.
        let pipeline_config = AutoPipelineConfig {
            reconnect_on_readonly: true,
            ..AutoPipelineConfig::default()
        };
        let reconnect = AutoPipelineReconnectConfig::default();
        let svc = match events {
            Some(events) => {
                AutoPipelineService::with_factory_and_events(
                    factory,
                    pipeline_config,
                    reconnect,
                    events,
                )
                .await?
            }
            None => AutoPipelineService::with_factory(factory, pipeline_config, reconnect).await?,
        };
        Ok(MultiplexedSentinelClient {
            inner: CommandAdapter::new(svc),
            replicas,
            replica_addrs,
            read_preference,
            read_routing,
        })
    }
}

/// Tracks the ROLE-verified Sentinel primary across connection factory calls.
///
/// The first successful address establishes a baseline. Only a later exact
/// address-string change is a failover; a transport reconnect to the same
/// address is not. This tracks verified endpoints rather than durable node
/// identity, so aliases for one server can compare different and a replacement
/// behind one endpoint can compare equal. The mutex is held only for the
/// in-memory comparison and never across discovery, connection I/O, or event
/// publication.
#[derive(Clone)]
struct VerifiedMasterTracker {
    events: ConnectionEventBus,
    current: Arc<Mutex<Option<Arc<str>>>>,
}

impl VerifiedMasterTracker {
    fn new(events: ConnectionEventBus) -> Self {
        Self {
            events,
            current: Arc::new(Mutex::new(None)),
        }
    }

    /// Record a verified master address, returning whether it changed from an
    /// established baseline.
    fn observe(&self, addr: &str) -> bool {
        let current: Arc<str> = Arc::from(addr);
        let previous = {
            let mut known = self
                .current
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if known.as_deref() == Some(current.as_ref()) {
                return false;
            }
            known.replace(Arc::clone(&current))
        };

        let Some(previous) = previous else {
            return false;
        };
        self.events.publish(ConnectionEvent::Failover {
            previous: Some(previous),
            current: Some(current),
        });
        true
    }
}

/// A multiplexed sentinel-managed Redis client for high-concurrency workloads.
///
/// Wraps [`AutoPipelineService`] + [`CommandAdapter`] with sentinel discovery
/// for automatic master resolution. Clone-friendly: all clones share the same
/// background worker and TCP connection.
///
/// Concurrent requests from multiple tasks are batched into Redis pipelines
/// automatically. Single requests flush immediately with no batching delay.
///
/// # Auth and TLS
///
/// For auth or TLS, use [`MultiplexedSentinelClient::builder`] to configure
/// sentinel credentials, node credentials, and TLS independently:
///
/// ```no_run
/// use redis_tower_sentinel::MultiplexedSentinelClient;
/// use redis_tower::credentials::StaticCredentials;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
///     .sentinel_credentials(StaticCredentials::password("sentinel_pass"))
///     .node_credentials(StaticCredentials::password("redis_pass"))
///     .connect_with_reconnect()
///     .await?;
/// # let _ = client;
/// # Ok(())
/// # }
/// ```
///
/// # Middleware
///
/// The type parameter `S` is the inner Frame-level [`Service`] and defaults to
/// [`AutoPipelineService`]. Use [`from_layered`](Self::from_layered) to wrap the
/// sentinel-managed client in a Tower middleware stack (circuit breaker,
/// timeout, retry).
///
/// # Read Preference
///
/// [`ReadPreference`] (set via the builder's `read_preference`) routes
/// read-only commands to shared background connections to sentinel-monitored
/// replicas instead of the master. See
/// [`MultiplexedSentinelClientBuilder::read_preference`] for how replica
/// selection and fallback work, and how this differs from
/// [`SentinelConnection`](crate::SentinelConnection)'s per-read strategy.
#[derive(Clone)]
pub struct MultiplexedSentinelClient<S = AutoPipelineService> {
    inner: CommandAdapter<S>,
    /// Shared background connections to reachable replicas, keyed by address.
    replicas: HashMap<String, CommandAdapter<AutoPipelineService>>,
    /// Stable selection input matching the keys in `replicas`.
    replica_addrs: Vec<NodeAddr>,
    read_preference: ReadPreference,
    read_routing: Arc<dyn ReadRoutingStrategy>,
}

impl MultiplexedSentinelClient<AutoPipelineService> {
    /// Re-authenticate the current master worker and all replica workers.
    ///
    /// Updates are serialized through each worker, preserving RESP framing and
    /// atomic pipeline boundaries. All workers are attempted before the first
    /// error is returned.
    pub async fn reauthenticate_all(&self, credentials: &Credentials) -> Result<(), RedisError> {
        let mut first_error =
            execute_with_deadline(self.inner.clone(), credentials.auth_command(), None)
                .await
                .err();

        for service in self.replicas.values() {
            if let Err(error) =
                execute_with_deadline(service.clone(), credentials.auth_command(), None).await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Apply every credential emitted by `provider` to current data workers.
    ///
    /// Short-lived Sentinel discovery sockets fetch credentials during each
    /// query and are not retained. Dropping the returned handle stops future
    /// master/replica updates.
    pub fn spawn_credential_reauthentication(
        &self,
        provider: Arc<dyn StreamingCredentialProvider>,
    ) -> CredentialReauthenticationHandle {
        let client = self.clone();
        spawn_reauthentication_task(provider, move |credentials| {
            let client = client.clone();
            async move { client.reauthenticate_all(&credentials).await }
        })
    }

    /// Create a builder for configuring the client.
    ///
    /// Use the builder to set sentinel credentials, node credentials, TLS, and
    /// RESP decode limits for each hop. Connection settings configured through
    /// the builder are retained across reconnects and failover.
    pub fn builder(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> MultiplexedSentinelClientBuilder {
        MultiplexedSentinelClientBuilder {
            sentinel_addrs: sentinel_addrs
                .iter()
                .map(|a| a.as_ref().to_string())
                .collect(),
            master_name: master_name.to_string(),
            config: SentinelConfig::default(),
            connection_events: None,
            read_preference: ReadPreference::Master,
            read_routing: None,
        }
    }

    /// Connect to the sentinel-discovered master.
    ///
    /// Does not reconnect automatically on connection failure. For
    /// production use with failover support, use [`Self::connect_with_reconnect`].
    ///
    /// Uses plain TCP without auth, and always routes every command to the
    /// master (`ReadPreference::Master`). For auth, TLS, or replica read
    /// routing, use [`Self::builder`].
    pub async fn connect(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> Result<Self, RedisError> {
        let addrs: Vec<String> = sentinel_addrs
            .iter()
            .map(|a| a.as_ref().to_string())
            .collect();
        let config = SentinelConfig::default();
        let master_addr =
            discovery::discover_master_with_config(&addrs, master_name, &config).await?;
        let conn = discovery::connect_hop(
            &master_addr,
            None,
            config.resp_limits,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            None,
        )
        .await?;
        Ok(Self {
            inner: CommandAdapter::new(AutoPipelineService::new(
                conn,
                AutoPipelineConfig::default(),
            )),
            replicas: HashMap::new(),
            replica_addrs: Vec::new(),
            read_preference: ReadPreference::Master,
            read_routing: Arc::new(RoundRobinRouting::new()),
        })
    }

    /// Connect with automatic reconnection via sentinel discovery.
    ///
    /// On connection failure, the factory re-queries sentinel to find
    /// the current master (which may have changed due to failover).
    ///
    /// Uses plain TCP without auth, and always routes every command to the
    /// master (`ReadPreference::Master`). For auth, TLS, or replica read
    /// routing, use [`Self::builder`].
    pub async fn connect_with_reconnect(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> Result<Self, RedisError> {
        let addrs: Vec<String> = sentinel_addrs
            .iter()
            .map(|a| a.as_ref().to_string())
            .collect();
        let name = master_name.to_string();
        let factory = move || {
            let addrs = addrs.clone();
            let name = name.clone();
            async move {
                // Verify ROLE so a reconnect lands on a real master, not a
                // demoted replica that sentinel still reports during a failover.
                let (conn, _addr) = discovery::connect_verified_master(&addrs, &name).await?;
                Ok(conn)
            }
        };
        // Enable READONLY-triggered reconnect: if the master is demoted to a
        // replica with TCP intact, writes return READONLY (not a connection
        // error), and the worker must rebuild via the factory to find the new
        // master instead of wedging on the demoted node.
        let config = AutoPipelineConfig {
            reconnect_on_readonly: true,
            ..AutoPipelineConfig::default()
        };
        let svc = AutoPipelineService::with_factory(
            factory,
            config,
            AutoPipelineReconnectConfig::default(),
        )
        .await?;
        Ok(Self {
            inner: CommandAdapter::new(svc),
            replicas: HashMap::new(),
            replica_addrs: Vec::new(),
            read_preference: ReadPreference::Master,
            read_routing: Arc::new(RoundRobinRouting::new()),
        })
    }

    /// Connect with automatic Sentinel rediscovery and lifecycle events.
    ///
    /// In addition to connection/reconnect events, this publishes
    /// [`ConnectionEvent::Failover`] when a reconnect successfully reaches a
    /// different ROLE-verified master endpoint string. Endpoint aliases are
    /// compared textually; this does not establish durable node identity.
    /// Subscribe before calling this method to observe the initial connection
    /// event.
    pub async fn connect_with_reconnect_and_events(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
        events: ConnectionEventBus,
    ) -> Result<Self, RedisError> {
        Self::builder(sentinel_addrs, master_name)
            .connection_events(events)
            .connect_with_reconnect()
            .await
    }

    /// Gracefully shut down the multiplexed sentinel client.
    ///
    /// Signals the background worker to stop accepting new requests, then
    /// waits for all in-flight requests to complete and joins the background
    /// task. If other clones of this client are still alive, this returns
    /// immediately -- the worker continues running until the last clone shuts
    /// down or is dropped.
    ///
    /// For clean application shutdown, prefer calling `shutdown()` over
    /// simply dropping the client.
    pub async fn shutdown(self) {
        let Self {
            inner, replicas, ..
        } = self;
        for replica in replicas.into_values() {
            replica.into_inner().shutdown().await;
        }
        inner.into_inner().shutdown().await;
    }
}

impl<S> MultiplexedSentinelClient<S>
where
    S: Service<Frame, Response = Frame, Error = RedisError> + Clone,
    S::Future: Send + 'static,
{
    /// Build a sentinel client from a layered Frame-level [`Service`].
    ///
    /// The middleware injection point: wrap [`AutoPipelineService`] (or any
    /// `Service<Frame, Response = Frame, Error = RedisError>`) in a Tower stack
    /// and hand it here. Every [`execute`](Self::execute) then flows through the
    /// middleware. The caller is responsible for sentinel discovery when
    /// building the inner service; for the built-in discovery use
    /// [`connect`](Self::connect) or
    /// [`connect_with_reconnect`](Self::connect_with_reconnect).
    ///
    /// A client built this way always routes through `service`
    /// (`ReadPreference::Master`) -- there are no replica connections to route
    /// reads to, since the caller supplied the complete stack.
    pub fn from_layered(service: S) -> Self {
        Self {
            inner: CommandAdapter::new(service),
            replicas: HashMap::new(),
            replica_addrs: Vec::new(),
            read_preference: ReadPreference::Master,
            read_routing: Arc::new(RoundRobinRouting::new()),
        }
    }

    /// Execute a command against the sentinel-managed master, or, with
    /// [`ReadPreference::Replica`]/[`ReadPreference::PreferReplica`] and a
    /// connected replica, against that replica for commands
    /// [`redis_tower::is_readonly_command`] accepts.
    ///
    /// `ReadPreference::Replica` returns an error for a read-only command
    /// when no replica is connected, rather than silently serving it from
    /// the master; `ReadPreference::PreferReplica` falls back to the master
    /// in that case. Writes, and every command under the default
    /// `ReadPreference::Master`, are unaffected and always go to the master.
    ///
    /// If other tasks are calling execute concurrently, their commands
    /// will be batched into a single Redis pipeline for efficiency.
    /// A deadline carried by [`redis_tower_core::WithDeadline`] bounds both
    /// waiting for inner readiness and the dispatched call.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let deadline = cmd.deadline();
        let wants_replica =
            self.read_preference != ReadPreference::Master && is_readonly_command(&cmd.to_frame());
        if wants_replica {
            if let Some(selected) = self.read_routing.select_replica(0, &self.replica_addrs) {
                let addr = selected.addr_string();
                let svc = self
                    .replicas
                    .get(&addr)
                    .expect("replica_addrs only contains keys present in replicas")
                    .clone();
                return execute_with_deadline(svc, cmd, deadline).await;
            }
            if self.read_preference == ReadPreference::Replica {
                return Err(RedisError::Redis(
                    "sentinel: ReadPreference::Replica requires a connected replica, but none is \
                     available"
                        .to_string(),
                ));
            }
        }

        execute_with_deadline(self.inner.clone(), cmd, deadline).await
    }
}

async fn execute_with_deadline<S, Cmd>(
    mut svc: CommandAdapter<S>,
    cmd: Cmd,
    deadline: Option<tokio::time::Instant>,
) -> Result<Cmd::Response, RedisError>
where
    S: Service<Frame, Response = Frame, Error = RedisError> + Clone,
    S::Future: Send + 'static,
    Cmd: Command,
{
    let operation = async move {
        std::future::poll_fn(|cx| <CommandAdapter<S> as Service<Cmd>>::poll_ready(&mut svc, cx))
            .await?;
        Service::call(&mut svc, cmd).await
    };

    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, operation)
            .await
            .map_err(|_elapsed| RedisError::CommandTimeout)?,
        None => operation.await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use redis_tower::credentials::StaticCredentials;
    use redis_tower_commands::{Get, Set};
    use redis_tower_core::WithDeadline;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    #[test]
    fn builder_defaults_to_standard_resp_limits() {
        let builder = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster");
        assert_eq!(builder.config.resp_limits, RespLimits::default());
    }

    #[test]
    fn builder_retains_custom_resp_limits_for_reconnects() {
        let limits = RespLimits {
            max_frame_size: 4096,
            max_depth: 16,
        };
        let builder = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
            .resp_limits(limits);
        assert_eq!(builder.config.resp_limits, limits);
    }

    #[test]
    fn builder_retains_connection_event_bus() {
        let builder = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
            .connection_events(ConnectionEventBus::default());
        assert!(builder.connection_events.is_some());
    }

    #[test]
    fn builder_defaults_read_preference_to_master() {
        let builder = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster");
        assert_eq!(builder.read_preference, ReadPreference::Master);
        assert!(builder.read_routing.is_none());
    }

    #[test]
    fn builder_sets_read_preference_and_routing() {
        struct AlwaysFirst;
        impl ReadRoutingStrategy for AlwaysFirst {
            fn select_replica<'a>(
                &self,
                _slot: u16,
                replicas: &'a [redis_tower::NodeAddr],
            ) -> Option<&'a redis_tower::NodeAddr> {
                replicas.first()
            }
        }

        let builder = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
            .read_preference(ReadPreference::PreferReplica)
            .read_routing(AlwaysFirst);
        assert_eq!(builder.read_preference, ReadPreference::PreferReplica);
        assert!(builder.read_routing.is_some());
    }

    #[tokio::test]
    async fn builder_connect_publishes_discovery_failure() {
        let events = ConnectionEventBus::new(4);
        let mut stream = events.subscribe();

        let result = MultiplexedSentinelClient::builder(&["127.0.0.1:0"], "missing")
            .connection_events(events)
            .connect()
            .await;
        assert!(result.is_err());
        assert!(matches!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ConnectFailed { .. }
        ));
    }

    #[tokio::test]
    async fn builder_reconnecting_connect_publishes_verified_factory_failure() {
        let events = ConnectionEventBus::new(4);
        let mut stream = events.subscribe();
        let addrs: [&str; 0] = [];

        let result = MultiplexedSentinelClient::builder(&addrs, "missing")
            .connection_events(events)
            .connect_with_reconnect()
            .await;
        assert!(result.is_err());
        assert!(matches!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ConnectFailed { .. }
        ));
    }

    #[tokio::test]
    async fn verified_master_change_emits_failover_between_attempt_and_reconnected() {
        let events = ConnectionEventBus::new(8);
        let mut stream = events.subscribe();
        let tracker = VerifiedMasterTracker::new(events.clone());

        // The first verified address establishes the baseline, and a normal
        // reconnect to that same address is not a failover.
        assert!(!tracker.observe("redis-a:6379"));
        assert!(!tracker.observe("redis-a:6379"));

        // AutoPipeline publishes the attempt before invoking the factory. The
        // factory's verified address tracker publishes Failover, and the worker
        // publishes Reconnected only after the factory returns.
        events.publish(ConnectionEvent::ReconnectAttempt {
            attempt: 1,
            delay: Duration::from_millis(10),
        });
        assert!(tracker.observe("redis-b:6379"));
        events.publish(ConnectionEvent::Reconnected {
            attempts: 1,
            elapsed: Duration::from_millis(12),
        });

        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 1,
                delay: Duration::from_millis(10),
            }
        );
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::Failover {
                previous: Some(Arc::from("redis-a:6379")),
                current: Some(Arc::from("redis-b:6379")),
            }
        );
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::Reconnected {
                attempts: 1,
                elapsed: Duration::from_millis(12),
            }
        );
    }

    #[derive(Clone)]
    struct MockFrameService {
        reply: Frame,
    }

    impl Service<Frame> for MockFrameService {
        type Response = Frame;
        type Error = RedisError;
        type Future = std::future::Ready<Result<Frame, RedisError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), RedisError>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Frame) -> Self::Future {
            std::future::ready(Ok(self.reply.clone()))
        }
    }

    #[derive(Clone)]
    struct NeverReadyFrameService {
        calls: Arc<AtomicUsize>,
    }

    impl Service<Frame> for NeverReadyFrameService {
        type Response = Frame;
        type Error = RedisError;
        type Future = std::future::Ready<Result<Frame, RedisError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), RedisError>> {
            Poll::Pending
        }

        fn call(&mut self, _req: Frame) -> Self::Future {
            self.calls.fetch_add(1, Ordering::Relaxed);
            std::future::ready(Ok(Frame::Null))
        }
    }

    #[tokio::test]
    async fn from_layered_routes_execute_through_injected_service() {
        let inner = MockFrameService {
            reply: Frame::BulkString(Some(Bytes::from("layered"))),
        };
        let client = MultiplexedSentinelClient::from_layered(inner);

        let client2 = client.clone();
        let val: Option<Bytes> = client2.execute(Get::new("k")).await.unwrap();
        assert_eq!(val, Some(Bytes::from("layered")));
    }

    #[tokio::test]
    async fn replica_preference_without_a_connected_replica_errors_reads_but_not_writes() {
        // White-box: construct directly (bypassing discovery) to exercise the
        // ReadPreference::Replica no-usable-replica path without a live
        // sentinel. `redis_tower_commands::Set` and `Get` isolate this from
        // is_readonly_command's own coverage, tested separately.
        let client = MultiplexedSentinelClient {
            inner: CommandAdapter::new(MockFrameService {
                reply: Frame::SimpleString(Bytes::from("OK")),
            }),
            replicas: HashMap::new(),
            replica_addrs: Vec::new(),
            read_preference: ReadPreference::Replica,
            read_routing: Arc::new(RoundRobinRouting::new()),
        };

        let err = client.execute(Get::new("k")).await.unwrap_err();
        assert!(matches!(err, RedisError::Redis(_)));

        // Writes never consult the replica, so they still reach `inner`.
        client.execute(Set::new("k", "v")).await.unwrap();
    }

    #[tokio::test]
    async fn prefer_replica_without_a_connected_replica_falls_back_to_master() {
        let client = MultiplexedSentinelClient {
            inner: CommandAdapter::new(MockFrameService {
                reply: Frame::BulkString(Some(Bytes::from("from-master"))),
            }),
            replicas: HashMap::new(),
            replica_addrs: Vec::new(),
            read_preference: ReadPreference::PreferReplica,
            read_routing: Arc::new(RoundRobinRouting::new()),
        };

        let val: Option<Bytes> = client.execute(Get::new("k")).await.unwrap();
        assert_eq!(val, Some(Bytes::from("from-master")));
    }

    #[tokio::test]
    async fn typed_deadline_bounds_layered_readiness() {
        let calls = Arc::new(AtomicUsize::new(0));
        let client = MultiplexedSentinelClient::from_layered(NeverReadyFrameService {
            calls: Arc::clone(&calls),
        });

        let result = client
            .execute(WithDeadline::after(
                Get::new("k"),
                Duration::from_millis(20),
            ))
            .await;

        assert!(matches!(result, Err(RedisError::CommandTimeout)));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn builder_sets_sentinel_credentials() {
        let builder = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
            .sentinel_credentials(StaticCredentials::password("sp"));
        assert!(builder.config.sentinel_credentials.is_some());
        assert!(builder.config.node_credentials.is_none());
    }

    #[test]
    fn builder_sets_node_credentials() {
        let builder = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
            .node_credentials(StaticCredentials::new("user", "np"));
        assert!(builder.config.node_credentials.is_some());
        assert!(builder.config.sentinel_credentials.is_none());
    }

    #[test]
    fn builder_sets_independent_credentials() {
        let builder = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster")
            .sentinel_credentials(StaticCredentials::password("sp"))
            .node_credentials(StaticCredentials::password("np"));
        assert!(builder.config.sentinel_credentials.is_some());
        assert!(builder.config.node_credentials.is_some());
    }

    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    #[test]
    fn builder_tls_sets_both_hops() {
        #[cfg(feature = "tls-rustls")]
        let tls = redis_tower_core::tls::TlsConfig::default_rustls();
        #[cfg(all(not(feature = "tls-rustls"), feature = "tls-native-tls"))]
        let tls = redis_tower_core::tls::TlsConfig::default_native_tls();

        let builder = MultiplexedSentinelClient::builder(&["127.0.0.1:26379"], "mymaster").tls(tls);
        assert!(builder.config.sentinel_tls.is_some());
        assert!(builder.config.node_tls.is_some());
    }
}
