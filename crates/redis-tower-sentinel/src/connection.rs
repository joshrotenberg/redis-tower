//! Sentinel-managed Redis connection with automatic failover.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use redis_tower::credentials::{CredentialProvider, Credentials};
use redis_tower::{
    NodeAddr, ReadPreference, ReadRoutingStrategy, RoundRobinRouting, is_readonly_command,
};
use redis_tower_core::{Command, RedisConnection, RedisError};
use redis_tower_protocol::RespLimits;

use crate::discovery::{self, SentinelConfig};

/// Builder for [`SentinelConnection`].
///
/// Obtain one via [`SentinelConnection::builder`].
///
/// # Example
///
/// ```no_run
/// use redis_tower_sentinel::SentinelConnection;
/// use redis_tower::credentials::StaticCredentials;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let conn = SentinelConnection::builder(
///     &["127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"],
///     "mymaster",
/// )
/// .sentinel_credentials(StaticCredentials::password("sentinel_pass"))
/// .node_credentials(StaticCredentials::password("redis_pass"))
/// .connect()
/// .await?;
/// # let _ = conn;
/// # Ok(())
/// # }
/// ```
pub struct SentinelConnectionBuilder {
    sentinel_addrs: Vec<String>,
    master_name: String,
    pub(crate) config: SentinelConfig,
    read_preference: ReadPreference,
    read_routing: Option<Arc<dyn ReadRoutingStrategy>>,
}

impl SentinelConnectionBuilder {
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
    /// Used when connecting to the discovered Redis master. Sentinels and the
    /// data node commonly use different passwords in production.
    pub fn node_credentials(mut self, provider: impl CredentialProvider) -> Self {
        self.config.node_credentials = Some(Arc::new(provider));
        self
    }

    /// Set RESP decode limits for every sentinel and Redis data-node connection.
    ///
    /// The limits apply to discovery, the initial master connection, replica
    /// discovery, and every connection opened while rediscovering after a
    /// failover. Defaults to [`RespLimits::default`].
    pub fn resp_limits(mut self, limits: RespLimits) -> Self {
        self.config.resp_limits = limits;
        self
    }

    /// Set the TLS configuration for sentinel connections.
    ///
    /// When set, all connections to sentinel nodes use TLS. The hostname for
    /// SNI verification is derived from each sentinel's address.
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn sentinel_tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        self.config.sentinel_tls = Some(Arc::new(tls));
        self
    }

    /// Set the TLS configuration for node (master) connections.
    ///
    /// When set, connections to the discovered Redis master use TLS.
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn node_tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        self.config.node_tls = Some(Arc::new(tls));
        self
    }

    /// Set the same TLS configuration for both sentinel and node connections.
    ///
    /// Convenience method when both hops share the same TLS settings. Equivalent
    /// to calling `.sentinel_tls(tls.clone())` and `.node_tls(tls)` (the config
    /// is cloned internally and stored in an `Arc` for each hop).
    ///
    /// Requires the `tls-rustls` or `tls-native-tls` feature.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn tls(mut self, tls: redis_tower_core::tls::TlsConfig) -> Self {
        let shared = Arc::new(tls);
        self.config.sentinel_tls = Some(shared.clone());
        self.config.node_tls = Some(shared);
        self
    }

    /// Set the read preference for routing read-only commands.
    ///
    /// Defaults to [`ReadPreference::Master`]. When set to
    /// [`ReadPreference::Replica`] or [`ReadPreference::PreferReplica`], the
    /// connection additionally discovers the monitored replicas via
    /// sentinel and connects to each one so read-only commands (as
    /// classified by [`redis_tower::is_readonly_command`]) can be routed to
    /// them.
    ///
    /// The two non-default variants differ on what happens when no replica
    /// is connected (none discovered, or all unreachable):
    /// [`ReadPreference::Replica`] returns an error for the read rather than
    /// silently serving it from the master, while
    /// [`ReadPreference::PreferReplica`] falls back to the master. Writes are
    /// unaffected either way and always go to the master.
    pub fn read_preference(mut self, pref: ReadPreference) -> Self {
        self.read_preference = pref;
        self
    }

    /// Set a custom read routing strategy for replica selection.
    ///
    /// Used only when [`read_preference`](Self::read_preference) is
    /// [`ReadPreference::Replica`] or [`ReadPreference::PreferReplica`]. If
    /// not set, defaults to [`RoundRobinRouting`]. The `slot` argument the
    /// strategy receives is always `0` -- sentinel monitors one shard, not a
    /// cluster's 16384 hash slots.
    pub fn read_routing(mut self, strategy: impl ReadRoutingStrategy) -> Self {
        self.read_routing = Some(Arc::new(strategy));
        self
    }

    /// Connect to the Redis master discovered via sentinel.
    pub async fn connect(self) -> Result<SentinelConnection, RedisError> {
        SentinelConnection::connect_with_config(
            self.sentinel_addrs,
            self.master_name,
            self.config,
            self.read_preference,
            self.read_routing,
        )
        .await
    }
}

/// A Redis connection managed by Sentinel.
///
/// Discovers the current master via Sentinel and connects to it.
/// When a command fails with a connection error, the connection
/// immediately rediscovers the master (which may have changed due to
/// failover). The caller should retry the command.
///
/// # Concurrency
///
/// `SentinelConnection` requires exclusive (`&mut self`) access for all
/// operations. It is NOT `Clone`. Share it via
/// [`SentinelClient`](crate::client::SentinelClient)
/// (`Arc<Mutex<SentinelConnection>>`) or use it directly in a single task.
///
/// # Example
///
/// ```no_run
/// use redis_tower_sentinel::SentinelConnection;
/// use redis_tower_commands::Set;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut conn = SentinelConnection::connect(
///     &["127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"],
///     "mymaster",
/// ).await?;
///
/// conn.execute(Set::new("key", "value")).await?;
/// # Ok(())
/// # }
/// ```
pub struct SentinelConnection {
    /// Current connection to the master.
    conn: RedisConnection,
    /// Sentinel addresses for rediscovery.
    sentinel_addrs: Vec<String>,
    /// Monitored master name.
    master_name: String,
    /// Address of the master this connection is currently bound to.
    ///
    /// Tracked so that rediscovery can log the old -> new master transition
    /// after a failover.
    current_addr: String,
    /// Whether the connection needs rediscovery.
    needs_rediscovery: bool,
    /// Sentinel and node configuration (credentials, TLS, RESP limits).
    config: SentinelConfig,
    /// Read routing preference.
    read_preference: ReadPreference,
    /// Strategy for selecting which replica to read from.
    read_routing: Arc<dyn ReadRoutingStrategy>,
    /// Connections to currently known replicas, keyed by `"host:port"`.
    ///
    /// Populated at connect time (and refreshed on rediscovery) when
    /// `read_preference != Master`. Empty whenever no replica is monitored,
    /// reachable, or configured; the preference determines whether reads then
    /// fall back to the master or return an error.
    replicas: HashMap<String, RedisConnection>,
    /// Addresses of `replicas`, kept alongside it so [`ReadRoutingStrategy`]
    /// can select over a `&[NodeAddr]` without rebuilding the list per read.
    replica_addrs: Vec<NodeAddr>,
    /// Address of the replica that served the most recent read-only
    /// command, or `None` if the most recent command went to the master
    /// (including every command when `read_preference` is `Master`).
    last_replica_read: Option<String>,
}

impl SentinelConnection {
    /// Create a builder for configuring the connection.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use redis_tower_sentinel::SentinelConnection;
    /// use redis_tower::credentials::StaticCredentials;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let conn = SentinelConnection::builder(
    ///     &["127.0.0.1:26379"],
    ///     "mymaster",
    /// )
    /// .node_credentials(StaticCredentials::password("secret"))
    /// .connect()
    /// .await?;
    /// # let _ = conn;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> SentinelConnectionBuilder {
        SentinelConnectionBuilder {
            sentinel_addrs: sentinel_addrs
                .iter()
                .map(|a| a.as_ref().to_string())
                .collect(),
            master_name: master_name.to_string(),
            config: SentinelConfig::default(),
            read_preference: ReadPreference::Master,
            read_routing: None,
        }
    }

    /// Connect to the Redis master discovered via Sentinel.
    ///
    /// Uses plain TCP connections to both sentinel and master without
    /// authentication. For auth or TLS, use [`Self::builder`].
    pub async fn connect(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> Result<Self, RedisError> {
        let addrs: Vec<String> = sentinel_addrs
            .iter()
            .map(|a| a.as_ref().to_string())
            .collect();
        Self::connect_with_config(
            addrs,
            master_name.to_string(),
            SentinelConfig::default(),
            ReadPreference::Master,
            None,
        )
        .await
    }

    /// Internal: connect using explicit config.
    async fn connect_with_config(
        addrs: Vec<String>,
        master_name: String,
        config: SentinelConfig,
        read_preference: ReadPreference,
        read_routing: Option<Arc<dyn ReadRoutingStrategy>>,
    ) -> Result<Self, RedisError> {
        let master_addr =
            discovery::discover_master_with_config(&addrs, &master_name, &config).await?;
        let conn = discovery::connect_hop(
            &master_addr,
            config.node_credentials.as_ref(),
            config.resp_limits,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            config.node_tls.as_ref(),
        )
        .await?;

        let (replicas, replica_addrs) = if read_preference == ReadPreference::Master {
            (HashMap::new(), Vec::new())
        } else {
            discovery::connect_replicas(&addrs, &master_name, &config).await
        };

        Ok(Self {
            conn,
            sentinel_addrs: addrs,
            master_name,
            current_addr: master_addr,
            needs_rediscovery: false,
            config,
            read_preference,
            read_routing: read_routing.unwrap_or_else(|| Arc::new(RoundRobinRouting::new())),
            replicas,
            replica_addrs,
            last_replica_read: None,
        })
    }

    /// Execute a command, routing read-only commands to a replica when
    /// [`read_preference`](Self::read_preference) allows it.
    ///
    /// If the connection was marked as needing rediscovery (after a
    /// previous connection error), rediscovers the master first.
    ///
    /// Writes and (with `ReadPreference::Master`, the default) reads always
    /// go to the master. With `Replica` or `PreferReplica`, a command
    /// [`redis_tower::is_readonly_command`] accepts is routed to a replica
    /// selected by the configured [`ReadRoutingStrategy`] -- if a replica is
    /// connected. With no connected replica (none discovered, or all
    /// unreachable at connect/rediscover time), `Replica` returns an error
    /// for the read rather than silently serving it from the master;
    /// `PreferReplica` falls back to the master, exactly like
    /// `ReadPreference::Master`. A failed replica read (the connection is
    /// dead, as opposed to none being connected) is returned to the caller
    /// as-is either way -- there is no same-command retry against the
    /// master, since `cmd` has already been consumed. Call
    /// [`Self::rediscover`] to refresh the replica set.
    pub async fn execute<Cmd: Command>(&mut self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        if self.needs_rediscovery {
            self.rediscover().await?;
        }

        if self.read_preference != ReadPreference::Master && is_readonly_command(&cmd.to_frame()) {
            match self.pick_replica_addr() {
                Some(addr) => {
                    self.last_replica_read = None;
                    let conn = self
                        .replicas
                        .get_mut(&addr)
                        .expect("pick_replica_addr only returns addresses present in `replicas`");
                    let result = conn.execute(cmd).await;
                    if result.is_ok() {
                        self.last_replica_read = Some(addr);
                    }
                    return result;
                }
                None if self.read_preference == ReadPreference::Replica => {
                    self.last_replica_read = None;
                    return Err(RedisError::Redis(format!(
                        "sentinel: ReadPreference::Replica requires a connected replica for '{}', \
                         but none is available",
                        self.master_name
                    )));
                }
                None => {
                    // PreferReplica: fall through to the master below.
                }
            }
        }
        self.last_replica_read = None;

        let result = self.conn.execute(cmd).await;
        if let Err(ref e) = result
            && (e.is_connection_error() || e.is_readonly())
        {
            // Two failover modes trigger rediscovery:
            //   - connection error: the master became unreachable.
            //   - READONLY: the master was demoted to a replica (REPLICAOF)
            //     with TCP intact, so writes now fail with READONLY. Without
            //     this the client wedges on the demoted node forever.
            tracing::warn!(
                error = %e,
                master_name = %self.master_name,
                "sentinel: master unreachable or demoted, rediscovering"
            );
            // Eagerly rediscover the new master so the next execute() call
            // connects immediately. The current command cannot be retried here
            // because it has been consumed; the caller should retry if
            // appropriate. If rediscovery fails, fall back to the deferred path
            // so the next call tries again.
            self.needs_rediscovery = self.rediscover().await.is_err();
        }
        result
    }

    /// Re-authenticate the current master and every connected replica.
    ///
    /// A failed master update marks the connection for ROLE-verified
    /// rediscovery on the next command. Failed replicas are removed so stale
    /// credentials cannot remain selectable.
    pub async fn reauthenticate_all(
        &mut self,
        credentials: &Credentials,
    ) -> Result<(), RedisError> {
        let mut first_error = None;
        if let Err(error) = self.conn.execute(credentials.auth_command()).await {
            self.needs_rediscovery = true;
            first_error = Some(error);
        }

        let addresses: Vec<String> = self.replicas.keys().cloned().collect();
        for address in addresses {
            let result = self
                .replicas
                .get_mut(&address)
                .expect("address came from the replica map")
                .execute(credentials.auth_command())
                .await;
            if let Err(error) = result {
                self.replicas.remove(&address);
                self.replica_addrs
                    .retain(|candidate| candidate.addr_string() != address);
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }

        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Select a replica to read from, using the configured routing strategy.
    ///
    /// `slot` is always `0`: sentinel monitors one shard, not a cluster's
    /// hash-sloted keyspace, so every [`ReadRoutingStrategy`] call shares it.
    fn pick_replica_addr(&self) -> Option<String> {
        let selected = self.read_routing.select_replica(0, &self.replica_addrs)?;
        let addr = selected.addr_string();
        self.replicas.contains_key(&addr).then_some(addr)
    }

    /// Force rediscovery of the master and reconnect.
    ///
    /// Sentinel's view of the master can lag a failover, so each candidate is
    /// verified with `ROLE` before it is trusted: if the node still reports
    /// `slave` (the failover has not fully propagated, or we got the demoted
    /// old master back), the attempt is retried with exponential backoff until
    /// a real master is found or the attempt budget is exhausted.
    ///
    /// The reconnected master connection respects the node credentials and TLS
    /// settings configured via [`SentinelConnection::builder`].
    ///
    /// When [`read_preference`](Self::read_preference) is not `Master`, this
    /// also refreshes the replica set from sentinel -- the failover that
    /// prompted rediscovery may have changed who the replicas are. The
    /// refresh is best-effort: a failure is logged and produces an empty set,
    /// so stale connections are never retained across a master failover.
    pub async fn rediscover(&mut self) -> Result<(), RedisError> {
        match discovery::connect_verified_master_with_config(
            &self.sentinel_addrs,
            &self.master_name,
            &self.config,
        )
        .await
        {
            Ok((conn, master_addr)) => {
                tracing::info!(
                    old_addr = %self.current_addr,
                    new_addr = %master_addr,
                    master_name = %self.master_name,
                    "sentinel: master rediscovered"
                );
                self.conn = conn;
                self.current_addr = master_addr;
                self.needs_rediscovery = false;
                if self.read_preference != ReadPreference::Master {
                    let (replicas, replica_addrs) = discovery::connect_replicas(
                        &self.sentinel_addrs,
                        &self.master_name,
                        &self.config,
                    )
                    .await;
                    self.replicas = replicas;
                    self.replica_addrs = replica_addrs;
                    self.last_replica_read = None;
                }
                Ok(())
            }
            Err(e) => {
                self.needs_rediscovery = true;
                Err(e)
            }
        }
    }

    /// Get the sentinel addresses.
    pub fn sentinel_addrs(&self) -> &[String] {
        &self.sentinel_addrs
    }

    /// Get the monitored master name.
    pub fn master_name(&self) -> &str {
        &self.master_name
    }

    /// Discover current replica addresses from sentinel.
    pub async fn discover_replicas(&self) -> Result<Vec<String>, RedisError> {
        discovery::discover_replicas_with_config(
            &self.sentinel_addrs,
            &self.master_name,
            &self.config,
        )
        .await
    }

    /// Get the configured read preference.
    pub fn read_preference(&self) -> ReadPreference {
        self.read_preference
    }

    /// Addresses of the replicas currently connected for reads.
    ///
    /// Empty when `read_preference()` is [`ReadPreference::Master`], or when
    /// no monitored replica was reachable at the last connect/rediscover.
    pub fn connected_replicas(&self) -> &[NodeAddr] {
        &self.replica_addrs
    }

    /// Address of the replica that served the most recent read-only command,
    /// or `None` if the most recent command went to the master.
    pub fn last_replica_read(&self) -> Option<&str> {
        self.last_replica_read.as_deref()
    }
}

impl redis_tower::RedisExecutor for SentinelConnection {
    fn execute<Cmd: redis_tower_core::Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl std::future::Future<Output = Result<Cmd::Response, redis_tower_core::RedisError>> + Send
    {
        SentinelConnection::execute(self, cmd)
    }
}

impl<Cmd: Command + 'static> tower_service::Service<Cmd> for SentinelConnection {
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Cmd::Response, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <RedisConnection as tower_service::Service<Cmd>>::poll_ready(&mut self.conn, cx)
    }

    fn call(&mut self, cmd: Cmd) -> Self::Future {
        <RedisConnection as tower_service::Service<Cmd>>::call(&mut self.conn, cmd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use futures::{SinkExt, StreamExt};
    use redis_tower::credentials::{Credentials, StaticCredentials};
    #[cfg(unix)]
    use redis_tower_core::{Frame, RedisStream};
    #[cfg(unix)]
    use redis_tower_protocol::RespCodec;
    #[cfg(unix)]
    use tokio_util::codec::Framed;

    #[cfg(unix)]
    fn command_name(frame: &Frame) -> &[u8] {
        let Frame::Array(Some(parts)) = frame else {
            panic!("expected command array, got {frame:?}");
        };
        let Some(Frame::BulkString(Some(command))) = parts.first() else {
            panic!("expected bulk-string command name, got {frame:?}");
        };
        command.as_ref()
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn pushed_credentials_reach_master_and_discard_rejected_replicas() {
        let (master_client, master_server) = tokio::net::UnixStream::pair().unwrap();
        let (replica_client, replica_server) = tokio::net::UnixStream::pair().unwrap();
        let replica_address = "127.0.0.1:6380".to_string();
        let mut connection = SentinelConnection {
            conn: RedisConnection::from_stream(RedisStream::Unix(master_client)),
            sentinel_addrs: vec!["127.0.0.1:26379".to_string()],
            master_name: "mymaster".to_string(),
            current_addr: "127.0.0.1:6379".to_string(),
            needs_rediscovery: false,
            config: SentinelConfig::default(),
            read_preference: ReadPreference::PreferReplica,
            read_routing: Arc::new(RoundRobinRouting::new()),
            replicas: HashMap::from([(
                replica_address.clone(),
                RedisConnection::from_stream(RedisStream::Unix(replica_client)),
            )]),
            replica_addrs: vec![NodeAddr {
                host: "127.0.0.1".to_string(),
                port: 6380,
            }],
            last_replica_read: None,
        };

        let master = tokio::spawn(async move {
            let mut framed = Framed::new(RedisStream::Unix(master_server), RespCodec::new());
            let auth = framed.next().await.unwrap().unwrap();
            assert_eq!(command_name(&auth), b"AUTH");
            framed
                .send(Frame::SimpleString(b"OK"[..].into()))
                .await
                .unwrap();
        });
        let replica = tokio::spawn(async move {
            let mut framed = Framed::new(RedisStream::Unix(replica_server), RespCodec::new());
            let auth = framed.next().await.unwrap().unwrap();
            assert_eq!(command_name(&auth), b"AUTH");
            framed
                .send(Frame::Error(b"WRONGPASS rejected"[..].into()))
                .await
                .unwrap();
        });

        let error = connection
            .reauthenticate_all(&Credentials::new("alice", "fresh"))
            .await
            .unwrap_err();
        assert!(matches!(error, RedisError::Redis(message) if message.contains("WRONGPASS")));
        master.await.unwrap();
        replica.await.unwrap();
        assert!(!connection.needs_rediscovery);
        assert!(!connection.replicas.contains_key(&replica_address));
        assert!(connection.replica_addrs.is_empty());
    }

    #[test]
    fn builder_defaults_to_standard_resp_limits() {
        let builder = SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster");
        assert_eq!(builder.config.resp_limits, RespLimits::default());
    }

    #[test]
    fn builder_defaults_read_preference_to_master() {
        let builder = SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster");
        assert_eq!(builder.read_preference, ReadPreference::Master);
        assert!(builder.read_routing.is_none());
    }

    #[test]
    fn builder_sets_read_preference() {
        let builder = SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster")
            .read_preference(ReadPreference::PreferReplica);
        assert_eq!(builder.read_preference, ReadPreference::PreferReplica);
    }

    #[test]
    fn builder_accepts_custom_read_routing() {
        struct AlwaysFirst;
        impl ReadRoutingStrategy for AlwaysFirst {
            fn select_replica<'a>(
                &self,
                _slot: u16,
                replicas: &'a [NodeAddr],
            ) -> Option<&'a NodeAddr> {
                replicas.first()
            }
        }

        let builder = SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster")
            .read_preference(ReadPreference::Replica)
            .read_routing(AlwaysFirst);
        assert!(builder.read_routing.is_some());
        assert_eq!(builder.read_preference, ReadPreference::Replica);
    }

    #[test]
    fn builder_sets_resp_limits_for_all_hops() {
        let limits = RespLimits {
            max_frame_size: 1024,
            max_depth: 8,
        };
        let builder =
            SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster").resp_limits(limits);
        assert_eq!(builder.config.resp_limits, limits);
    }

    #[test]
    fn builder_sets_sentinel_credentials() {
        let builder = SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster")
            .sentinel_credentials(StaticCredentials::password("sentinel_pass"));
        assert!(builder.config.sentinel_credentials.is_some());
        assert!(builder.config.node_credentials.is_none());
    }

    #[test]
    fn builder_sets_node_credentials() {
        let builder = SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster")
            .node_credentials(StaticCredentials::new("alice", "redis_pass"));
        assert!(builder.config.node_credentials.is_some());
        assert!(builder.config.sentinel_credentials.is_none());
    }

    #[test]
    fn builder_sets_independent_credentials() {
        let builder = SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster")
            .sentinel_credentials(StaticCredentials::password("s"))
            .node_credentials(StaticCredentials::password("n"));
        assert!(builder.config.sentinel_credentials.is_some());
        assert!(builder.config.node_credentials.is_some());
    }

    #[test]
    fn builder_stores_sentinel_addrs_and_name() {
        let builder =
            SentinelConnection::builder(&["127.0.0.1:26379", "127.0.0.1:26380"], "mymaster");
        assert_eq!(builder.sentinel_addrs.len(), 2);
        assert_eq!(builder.master_name, "mymaster");
    }

    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    #[test]
    fn builder_tls_sets_both_hops() {
        #[cfg(feature = "tls-rustls")]
        let tls = redis_tower_core::tls::TlsConfig::default_rustls();
        #[cfg(all(not(feature = "tls-rustls"), feature = "tls-native-tls"))]
        let tls = redis_tower_core::tls::TlsConfig::default_native_tls();

        let builder = SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster").tls(tls);
        assert!(builder.config.sentinel_tls.is_some());
        assert!(builder.config.node_tls.is_some());
    }

    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    #[test]
    fn builder_sentinel_tls_only() {
        #[cfg(feature = "tls-rustls")]
        let tls = redis_tower_core::tls::TlsConfig::default_rustls();
        #[cfg(all(not(feature = "tls-rustls"), feature = "tls-native-tls"))]
        let tls = redis_tower_core::tls::TlsConfig::default_native_tls();

        let builder =
            SentinelConnection::builder(&["127.0.0.1:26379"], "mymaster").sentinel_tls(tls);
        assert!(builder.config.sentinel_tls.is_some());
        assert!(builder.config.node_tls.is_none());
    }
}
