//! Managed Redis Cluster fixture for live tests and benchmarks.
//!
//! [`ClusterFixture`] always starts three masters and one replica per master.
//! It adds bounded readiness, topology inspection, deterministic hash-slot
//! keys, resharding, and failover helpers on top of `redis-server-wrapper`.

use std::collections::HashSet;
use std::fs;
use std::future::Future;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use redis_server_wrapper::chaos;
use redis_server_wrapper::{RedisCluster, RedisClusterHandle, RedisServerHandle};
use tokio::time::{Instant, sleep, timeout};

/// Number of masters in every fixture.
pub const MASTER_COUNT: usize = 3;

/// Number of replicas assigned to every master in a fixture.
pub const REPLICAS_PER_MASTER: usize = 1;

/// Total number of Redis processes in every fixture.
pub const NODE_COUNT: usize = MASTER_COUNT * (1 + REPLICAS_PER_MASTER);

/// Number of hash slots in Redis Cluster.
pub const SLOT_COUNT: u16 = 16_384;

const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_READINESS_TIMEOUT: Duration = Duration::from_secs(20);
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_CLUSTER_NODE_TIMEOUT_MS: u64 = 1_000;
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const CLUSTER_BUS_PORT_OFFSET: u16 = 10_000;
const AUTO_PORT_START: u16 = 18_000;
const AUTO_PORT_SPAN: u16 = 24_000;
const STARTUP_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const STARTUP_CLEANUP_QUIET_PERIOD: Duration = Duration::from_millis(500);
const STARTUP_CLEANUP_POLL_INTERVAL: Duration = Duration::from_millis(25);

static CLUSTER_STARTUP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static RESERVED_PORTS: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();

/// Errors produced by the managed cluster fixture.
#[derive(Debug, thiserror::Error)]
pub enum ClusterFixtureError {
    /// `redis-server-wrapper` could not start or operate the cluster.
    #[error(transparent)]
    Wrapper(#[from] redis_server_wrapper::Error),

    /// The requested base port cannot fit all client and cluster-bus ports.
    #[error("invalid Redis Cluster base port {0}")]
    InvalidBasePort(u16),

    /// One or more ports in the requested fixture range are already occupied.
    #[error("Redis Cluster port range beginning at {0} is unavailable")]
    PortRangeUnavailable(u16),

    /// The fixture could not inspect or remove its startup artifacts safely.
    #[error("Redis Cluster startup artifact error: {0}")]
    StartupArtifact(#[from] std::io::Error),

    /// Redis returned a malformed or incomplete cluster topology.
    #[error("invalid Redis Cluster topology: {0}")]
    InvalidTopology(String),

    /// `redis-cli` returned an error reply despite exiting successfully.
    #[error("Redis command failed while {operation}: {response}")]
    RedisCommand {
        /// Description of the state transition being attempted.
        operation: String,
        /// Error reply returned by Redis.
        response: String,
    },

    /// A bounded fixture operation did not complete in time.
    #[error("timed out after {timeout:?} while {operation}")]
    Timeout {
        /// Description of the operation that timed out.
        operation: String,
        /// Configured bound for the operation.
        timeout: Duration,
    },
}

/// Result alias for cluster fixture operations.
pub type Result<T> = std::result::Result<T, ClusterFixtureError>;

/// Inclusive range of hash slots owned by a master.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlotRange {
    /// First slot in the range.
    pub start: u16,
    /// Last slot in the range.
    pub end: u16,
}

impl SlotRange {
    /// Return whether this range contains `slot`.
    pub fn contains(&self, slot: u16) -> bool {
        self.start <= slot && slot <= self.end
    }
}

/// Current role reported for a cluster node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterNodeRole {
    /// A node that currently owns hash slots.
    Master,
    /// A node replicating the master with the given node ID.
    Replica {
        /// Current master's Redis Cluster node ID.
        master_id: String,
    },
}

/// One node in a dynamic cluster topology snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterNode {
    /// Stable fixture index, matching [`ClusterFixture::node`].
    pub index: usize,
    /// Redis Cluster node ID.
    pub id: String,
    /// Reachable `host:port` address.
    pub addr: String,
    /// Client port.
    pub port: u16,
    /// Current master or replica role.
    pub role: ClusterNodeRole,
    /// Slot ranges currently owned by this node. Empty for replicas.
    pub slots: Vec<SlotRange>,
    /// Whether the cluster gossip link is currently connected.
    pub connected: bool,
    /// Raw comma-separated flags reported by `CLUSTER NODES`.
    pub flags: Vec<String>,
}

impl ClusterNode {
    /// Return whether this node is currently a master.
    pub fn is_master(&self) -> bool {
        matches!(self.role, ClusterNodeRole::Master)
    }

    /// Return whether this node is currently a replica.
    pub fn is_replica(&self) -> bool {
        matches!(self.role, ClusterNodeRole::Replica { .. })
    }

    fn has_failure_flag(&self) -> bool {
        self.flags
            .iter()
            .any(|flag| matches!(flag.as_str(), "fail" | "fail?" | "handshake" | "noaddr"))
    }
}

/// Point-in-time view of all known fixture nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterTopology {
    /// Nodes sorted by stable fixture index.
    pub nodes: Vec<ClusterNode>,
}

impl ClusterTopology {
    /// Nodes sorted by stable fixture index.
    pub fn nodes(&self) -> &[ClusterNode] {
        &self.nodes
    }

    /// Find a node by its Redis Cluster node ID.
    pub fn node_by_id(&self, id: &str) -> Option<&ClusterNode> {
        self.nodes.iter().find(|node| node.id == id)
    }

    /// Find a node by its stable fixture index.
    pub fn node(&self, index: usize) -> Option<&ClusterNode> {
        self.nodes.iter().find(|node| node.index == index)
    }

    /// Current master nodes, ordered by stable fixture index.
    pub fn masters(&self) -> impl Iterator<Item = &ClusterNode> {
        self.nodes.iter().filter(|node| node.is_master())
    }

    /// Current replica nodes, ordered by stable fixture index.
    pub fn replicas(&self) -> impl Iterator<Item = &ClusterNode> {
        self.nodes.iter().filter(|node| node.is_replica())
    }

    /// Find the current master that owns `slot`.
    pub fn owner_of_slot(&self, slot: u16) -> Option<&ClusterNode> {
        self.masters()
            .find(|node| node.slots.iter().any(|range| range.contains(slot)))
    }

    /// Find replicas currently attached to `master_id`.
    pub fn replicas_of(&self, master_id: &str) -> Vec<&ClusterNode> {
        self.replicas()
            .filter(|node| {
                matches!(
                    &node.role,
                    ClusterNodeRole::Replica { master_id: id } if id == master_id
                )
            })
            .collect()
    }

    /// Return true when the expected six healthy nodes cover all hash slots.
    pub fn is_ready(&self) -> bool {
        if self.nodes.len() != NODE_COUNT
            || self.masters().count() != MASTER_COUNT
            || self.replicas().count() != MASTER_COUNT * REPLICAS_PER_MASTER
            || self
                .nodes
                .iter()
                .any(|node| !node.connected || node.has_failure_flag())
        {
            return false;
        }

        let master_ids: HashSet<&str> = self.masters().map(|node| node.id.as_str()).collect();
        if master_ids
            .iter()
            .any(|master_id| self.replicas_of(master_id).len() != REPLICAS_PER_MASTER)
        {
            return false;
        }

        let mut owners = vec![0_u8; SLOT_COUNT as usize];
        for node in self.masters() {
            for range in &node.slots {
                if range.end >= SLOT_COUNT || range.start > range.end {
                    return false;
                }
                for slot in range.start..=range.end {
                    owners[slot as usize] = owners[slot as usize].saturating_add(1);
                }
            }
        }
        owners.into_iter().all(|count| count == 1)
    }
}

/// Builder for a safe three-master, three-replica cluster fixture.
#[derive(Clone, Debug)]
pub struct ClusterFixtureBuilder {
    base_port: Option<u16>,
    startup_timeout: Duration,
    readiness_timeout: Duration,
    operation_timeout: Duration,
    cluster_node_timeout_ms: u64,
}

impl Default for ClusterFixtureBuilder {
    fn default() -> Self {
        Self {
            base_port: None,
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            readiness_timeout: DEFAULT_READINESS_TIMEOUT,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            cluster_node_timeout_ms: DEFAULT_CLUSTER_NODE_TIMEOUT_MS,
        }
    }
}

impl ClusterFixtureBuilder {
    /// Use an explicit run of six client ports beginning at `base_port`.
    ///
    /// The matching six cluster-bus ports (`base_port + 10000`) must also be
    /// available. By default the fixture searches for a free range. Port
    /// ranges are reserved across fixtures in this process for their entire
    /// lifetime. TCP probing is best-effort across separate processes, so
    /// callers coordinating multiple test processes should still assign
    /// disjoint explicit ranges when practical.
    pub fn base_port(mut self, base_port: u16) -> Self {
        self.base_port = Some(base_port);
        self
    }

    /// Bound process startup and `redis-cli --cluster create`.
    pub fn startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }

    /// Bound initial topology and replication convergence.
    pub fn readiness_timeout(mut self, timeout: Duration) -> Self {
        self.readiness_timeout = timeout;
        self
    }

    /// Bound each `redis-cli` command issued by fixture helpers.
    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }

    /// Set Redis's cluster failure-detection timeout in milliseconds.
    pub fn cluster_node_timeout(mut self, timeout_ms: u64) -> Self {
        self.cluster_node_timeout_ms = timeout_ms;
        self
    }

    /// Start the six-node cluster and wait for full slot and replica readiness.
    pub async fn start(self) -> Result<ClusterFixture> {
        let mut port_lease = match self.base_port {
            Some(base_port) => reserve_port_range(base_port)?,
            None => find_and_reserve_port_range()?,
        };
        let base_port = port_lease.base_port();

        let handle = {
            // redis-server-wrapper first issues SHUTDOWN, sleeps, and only then
            // binds its ports. Serialize that vulnerable interval so another
            // fixture in this process cannot probe or start through the gap.
            let _startup_guard = CLUSTER_STARTUP_LOCK.lock().await;
            let cleanup = StartupArtifactGuard::new(base_port)?;

            // The wrapper must bind these ports itself. The process-local lease
            // remains active; only the OS listeners are released.
            port_lease.release_listeners();
            let start = RedisCluster::builder()
                .masters(MASTER_COUNT as u16)
                .replicas_per_master(REPLICAS_PER_MASTER as u16)
                .base_port(base_port)
                .cluster_node_timeout(self.cluster_node_timeout_ms)
                .repl_diskless_sync(true)
                .repl_diskless_sync_delay(0)
                .save(false)
                .appendonly(false)
                .start();
            await_cluster_start(cleanup, self.startup_timeout, start).await?
        };

        let fixture = ClusterFixture {
            handle: Some(handle),
            operation_timeout: self.operation_timeout,
            _port_lease: port_lease,
        };
        fixture.wait_for_ready(self.readiness_timeout).await?;
        Ok(fixture)
    }
}

/// Managed three-master, three-replica Redis Cluster.
///
/// Dropping the fixture resumes any frozen processes, delegates process
/// shutdown to `redis-server-wrapper`, then removes the fixture's validated
/// temporary working directory.
pub struct ClusterFixture {
    // `Option` lets Drop stop every process before removing the wrapper's
    // working directory. The value is Some for the fixture's entire usable
    // lifetime and is taken only during Drop.
    handle: Option<RedisClusterHandle>,
    operation_timeout: Duration,
    // Retained until after the cluster handle shuts down, preventing another
    // fixture in this process from targeting the same ports during its
    // wrapper-level SHUTDOWN prelude.
    _port_lease: PortRangeLease,
}

/// A slot migration held in Redis's `MIGRATING`/`IMPORTING` state.
///
/// This fixture-owned guard mirrors the useful surface of
/// `redis_server_wrapper::chaos::ReshardGuard`, while discovering the current
/// live masters when the handoff completes. That distinction matters after a
/// replica promotion because the wrapper's startup-time master slice is then
/// stale.
pub struct FixtureReshardGuard<'a> {
    fixture: &'a ClusterFixture,
    slot: u16,
    source_index: usize,
    target_index: usize,
    target_id: String,
    resolved: bool,
}

impl FixtureReshardGuard<'_> {
    /// Migrate every key currently in the held slot without changing ownership.
    ///
    /// The whole sweep, including every `GETKEYSINSLOT` and `MIGRATE`, is
    /// bounded by the fixture's operation timeout.
    pub async fn migrate_keys(&self) -> Result<usize> {
        let budget = self.fixture.operation_timeout;
        self.migrate_keys_before(Instant::now() + budget, budget)
            .await
    }

    /// Complete the handoff and announce it to every current live master.
    pub async fn complete(mut self) -> Result<usize> {
        let budget = self.fixture.operation_timeout;
        let deadline = Instant::now() + budget;
        let moved = self.migrate_keys_before(deadline, budget).await?;
        let topology = self.fixture.topology_before(deadline, budget).await?;
        let target = topology.node_by_id(&self.target_id).ok_or_else(|| {
            ClusterFixtureError::InvalidTopology(format!(
                "reshard target {} disappeared from the topology",
                self.target_id
            ))
        })?;
        if !target.is_master() || !target.connected || target.has_failure_flag() {
            return Err(ClusterFixtureError::InvalidTopology(format!(
                "reshard target {} is not a live master",
                self.target_id
            )));
        }

        let live_master_indices: Vec<usize> = topology
            .masters()
            .filter(|node| node.connected && !node.has_failure_flag())
            .map(|node| node.index)
            .collect();
        if live_master_indices.len() != MASTER_COUNT {
            return Err(ClusterFixtureError::InvalidTopology(format!(
                "expected {MASTER_COUNT} live masters while completing slot {}, found {}",
                self.slot,
                live_master_indices.len()
            )));
        }

        let slot = self.slot.to_string();
        for index in live_master_indices {
            let output = self
                .fixture
                .run_node_before(
                    index,
                    &["CLUSTER", "SETSLOT", &slot, "NODE", &self.target_id],
                    deadline,
                    budget,
                    format!("announcing slot {} owner to master {index}", self.slot),
                )
                .await?;
            ensure_redis_success(
                &output,
                format!("announcing slot {} owner to master {index}", self.slot),
            )?;
        }
        self.resolved = true;
        Ok(moved)
    }

    /// Abort the held window and reset both endpoints to `STABLE`.
    ///
    /// Keys already moved remain on the target, matching Redis's native
    /// reshard semantics.
    pub async fn abort(mut self) -> Result<()> {
        let budget = self.fixture.operation_timeout;
        let deadline = Instant::now() + budget;
        let slot = self.slot.to_string();
        for index in [self.source_index, self.target_index] {
            let output = self
                .fixture
                .run_node_before(
                    index,
                    &["CLUSTER", "SETSLOT", &slot, "STABLE"],
                    deadline,
                    budget,
                    format!("aborting migration of slot {} on node {index}", self.slot),
                )
                .await?;
            ensure_redis_success(
                &output,
                format!("aborting migration of slot {} on node {index}", self.slot),
            )?;
        }
        self.resolved = true;
        Ok(())
    }

    async fn migrate_keys_before(&self, deadline: Instant, budget: Duration) -> Result<usize> {
        let slot = self.slot.to_string();
        let batch_size = "100";
        let target = self.fixture.node(self.target_index);
        let target_host = target.host().to_owned();
        let target_port = target.port().to_string();
        let mut moved = 0;

        loop {
            let output = self
                .fixture
                .run_node_before(
                    self.source_index,
                    &["CLUSTER", "GETKEYSINSLOT", &slot, batch_size],
                    deadline,
                    budget,
                    format!("listing keys in migrating slot {}", self.slot),
                )
                .await?;
            ensure_redis_success(&output, format!("listing keys in slot {}", self.slot))?;
            let keys: Vec<&str> = output
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .collect();
            if keys.is_empty() {
                return Ok(moved);
            }

            for key in keys {
                let output = self
                    .fixture
                    .run_node_before(
                        self.source_index,
                        &["MIGRATE", &target_host, &target_port, key, "0", "5000"],
                        deadline,
                        budget,
                        format!("migrating a key in slot {}", self.slot),
                    )
                    .await?;
                ensure_redis_success(&output, format!("migrating a key in slot {}", self.slot))?;
                moved += 1;
            }
        }
    }
}

impl Drop for FixtureReshardGuard<'_> {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        let slot = self.slot.to_string();
        for index in [self.source_index, self.target_index] {
            self.fixture
                .node(index)
                .cli()
                .fire_and_forget(&["CLUSTER", "SETSLOT", &slot, "STABLE"]);
        }
    }
}

impl ClusterFixture {
    /// Create a fixture builder with safe defaults and automatic port selection.
    pub fn builder() -> ClusterFixtureBuilder {
        ClusterFixtureBuilder::default()
    }

    /// Start a fixture with default settings.
    pub async fn start() -> Result<Self> {
        Self::builder().start().await
    }

    /// Address of the seed node as `host:port`.
    pub fn seed_addr(&self) -> String {
        self.handle().addr()
    }

    /// URL of the seed node.
    pub fn seed_url(&self) -> String {
        format!("redis://{}/", self.seed_addr())
    }

    /// Addresses of all six nodes in stable fixture-index order.
    pub fn node_addrs(&self) -> Vec<String> {
        self.handle().node_addrs()
    }

    /// Borrow the underlying `redis-server-wrapper` cluster handle.
    pub fn handle(&self) -> &RedisClusterHandle {
        self.handle
            .as_ref()
            .expect("cluster handle remains present until fixture drop")
    }

    /// Borrow one server handle by stable fixture index.
    ///
    /// # Panics
    ///
    /// Panics if `index >= 6`.
    pub fn node(&self, index: usize) -> &RedisServerHandle {
        self.handle().node(index)
    }

    /// Run a command against a node with the configured operation timeout.
    pub async fn run_node(&self, index: usize, args: &[&str]) -> Result<String> {
        let budget = self.operation_timeout;
        self.run_node_before(
            index,
            args,
            Instant::now() + budget,
            budget,
            format!("running redis-cli against node {index}"),
        )
        .await
    }

    async fn run_node_before(
        &self,
        index: usize,
        args: &[&str],
        deadline: Instant,
        budget: Duration,
        operation: String,
    ) -> Result<String> {
        let remaining = remaining_before(deadline).ok_or_else(|| ClusterFixtureError::Timeout {
            operation: operation.clone(),
            timeout: budget,
        })?;
        let command_timeout = self.operation_timeout.min(remaining);
        self.run_node_for(index, args, command_timeout, budget, operation)
            .await
    }

    async fn run_node_for(
        &self,
        index: usize,
        args: &[&str],
        command_timeout: Duration,
        budget: Duration,
        operation: String,
    ) -> Result<String> {
        timeout(command_timeout, self.node(index).run(args))
            .await
            .map_err(|_| ClusterFixtureError::Timeout {
                operation,
                timeout: budget,
            })?
            .map_err(Into::into)
    }

    /// Read and parse the current topology from the first reachable node.
    pub async fn topology(&self) -> Result<ClusterTopology> {
        let budget = self.operation_timeout;
        self.topology_before(Instant::now() + budget, budget).await
    }

    async fn topology_before(
        &self,
        deadline: Instant,
        budget: Duration,
    ) -> Result<ClusterTopology> {
        let mut last_error = None;
        for index in 0..NODE_COUNT {
            let remaining =
                remaining_before(deadline).ok_or_else(|| ClusterFixtureError::Timeout {
                    operation: "reading Redis Cluster topology".into(),
                    timeout: budget,
                })?;
            let nodes_left = (NODE_COUNT - index) as u32;
            let probe_timeout = self.operation_timeout.min(remaining / nodes_left);
            match self
                .run_node_for(
                    index,
                    &["CLUSTER", "NODES"],
                    probe_timeout,
                    budget,
                    format!("reading topology from node {index}"),
                )
                .await
            {
                Ok(output) => match parse_topology(&output, self.handle()) {
                    Ok(topology) => return Ok(topology),
                    Err(error) => last_error = Some(error),
                },
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            ClusterFixtureError::InvalidTopology("fixture has no reachable nodes".into())
        }))
    }

    /// Wait until six connected nodes, three replica links, and all slots are ready.
    pub async fn wait_for_ready(&self, wait: Duration) -> Result<ClusterTopology> {
        let deadline = Instant::now() + wait;
        loop {
            if remaining_before(deadline).is_none() {
                return Err(ClusterFixtureError::Timeout {
                    operation: "waiting for six-node cluster readiness".into(),
                    timeout: wait,
                });
            }
            if let Ok(topology) = self.topology_before(deadline, wait).await
                && topology.is_ready()
                && self
                    .replication_links_ready_before(&topology, deadline, wait)
                    .await
            {
                return Ok(topology);
            }
            sleep_before(deadline).await;
        }
    }

    /// Begin a held slot migration to the master at `target_index`.
    ///
    /// The returned guard exposes `migrate_keys`, `complete`, and `abort`, and
    /// makes a best-effort reset to `STABLE` if dropped unresolved.
    pub async fn begin_reshard(
        &self,
        slot: u16,
        target_index: usize,
    ) -> Result<FixtureReshardGuard<'_>> {
        validate_slot(slot)?;
        let budget = self.operation_timeout;
        let deadline = Instant::now() + budget;
        let topology = self.topology_before(deadline, budget).await?;
        let source = topology.owner_of_slot(slot).ok_or_else(|| {
            ClusterFixtureError::InvalidTopology(format!("slot {slot} has no owner"))
        })?;
        let target = topology.node(target_index).ok_or_else(|| {
            ClusterFixtureError::InvalidTopology(format!("node index {target_index} is absent"))
        })?;
        if !target.is_master() {
            return Err(ClusterFixtureError::InvalidTopology(format!(
                "node index {target_index} is not a master"
            )));
        }
        if source.index == target.index {
            return Err(ClusterFixtureError::InvalidTopology(format!(
                "node index {target_index} already owns slot {slot}"
            )));
        }
        let source_index = source.index;
        let source_id = source.id.clone();
        let target_id = target.id.clone();
        let slot_string = slot.to_string();
        let importing = self
            .run_node_before(
                target_index,
                &["CLUSTER", "SETSLOT", &slot_string, "IMPORTING", &source_id],
                deadline,
                budget,
                format!("marking slot {slot} importing on node {target_index}"),
            )
            .await?;
        ensure_redis_success(
            &importing,
            format!("marking slot {slot} importing on node {target_index}"),
        )?;
        let migrating = self
            .run_node_before(
                source_index,
                &["CLUSTER", "SETSLOT", &slot_string, "MIGRATING", &target_id],
                deadline,
                budget,
                format!("marking slot {slot} migrating on node {source_index}"),
            )
            .await;
        let migrating = match migrating {
            Ok(output) => output,
            Err(error) => {
                self.node(target_index).cli().fire_and_forget(&[
                    "CLUSTER",
                    "SETSLOT",
                    &slot_string,
                    "STABLE",
                ]);
                return Err(error);
            }
        };
        if let Err(error) = ensure_redis_success(
            &migrating,
            format!("marking slot {slot} migrating on node {source_index}"),
        ) {
            self.node(target_index).cli().fire_and_forget(&[
                "CLUSTER",
                "SETSLOT",
                &slot_string,
                "STABLE",
            ]);
            return Err(error);
        }

        Ok(FixtureReshardGuard {
            fixture: self,
            slot,
            source_index,
            target_index,
            target_id,
            resolved: false,
        })
    }

    /// Fully migrate `slot` to the master at `target_index`.
    pub async fn reshard_slot(&self, slot: u16, target_index: usize) -> Result<usize> {
        let guard = self.begin_reshard(slot, target_index).await?;
        guard.complete().await
    }

    /// Kill the current owner of `slot`, returning its pre-kill snapshot.
    pub async fn kill_slot_owner(&self, slot: u16) -> Result<ClusterNode> {
        validate_slot(slot)?;
        let topology = self.topology().await?;
        let owner = topology.owner_of_slot(slot).cloned().ok_or_else(|| {
            ClusterFixtureError::InvalidTopology(format!("slot {slot} has no owner"))
        })?;
        chaos::kill_node(self.node(owner.index));
        Ok(owner)
    }

    /// Request manual promotion of the replica at `replica_index`.
    pub async fn promote_replica(&self, replica_index: usize) -> Result<String> {
        let topology = self.topology().await?;
        let replica = topology.node(replica_index).ok_or_else(|| {
            ClusterFixtureError::InvalidTopology(format!("node index {replica_index} is absent"))
        })?;
        if !replica.is_replica() {
            return Err(ClusterFixtureError::InvalidTopology(format!(
                "node index {replica_index} is not a replica"
            )));
        }
        timeout(
            self.operation_timeout,
            chaos::trigger_failover(self.node(replica_index)),
        )
        .await
        .map_err(|_| ClusterFixtureError::Timeout {
            operation: format!("promoting replica {replica_index}"),
            timeout: self.operation_timeout,
        })?
        .map_err(Into::into)
    }

    /// Wait until `expected_node_id` owns `slot`.
    pub async fn wait_for_slot_owner(
        &self,
        slot: u16,
        expected_node_id: &str,
        wait: Duration,
    ) -> Result<ClusterNode> {
        validate_slot(slot)?;
        let deadline = Instant::now() + wait;
        loop {
            if remaining_before(deadline).is_none() {
                return Err(ClusterFixtureError::Timeout {
                    operation: format!("waiting for node {expected_node_id} to own slot {slot}"),
                    timeout: wait,
                });
            }
            if let Ok(topology) = self.topology_before(deadline, wait).await
                && let Some(owner) = topology.owner_of_slot(slot)
                && owner.id == expected_node_id
            {
                return Ok(owner.clone());
            }
            sleep_before(deadline).await;
        }
    }

    /// Wait until a node other than `previous_node_id` owns `slot`.
    pub async fn wait_for_slot_owner_change(
        &self,
        slot: u16,
        previous_node_id: &str,
        wait: Duration,
    ) -> Result<ClusterNode> {
        validate_slot(slot)?;
        let deadline = Instant::now() + wait;
        loop {
            if remaining_before(deadline).is_none() {
                return Err(ClusterFixtureError::Timeout {
                    operation: format!(
                        "waiting for slot {slot} owner to change from {previous_node_id}"
                    ),
                    timeout: wait,
                });
            }
            if let Ok(topology) = self.topology_before(deadline, wait).await
                && let Some(owner) = topology.owner_of_slot(slot)
                && owner.id != previous_node_id
            {
                return Ok(owner.clone());
            }
            sleep_before(deadline).await;
        }
    }

    /// Wait until `node_id` is reported as a master and return its snapshot.
    pub async fn wait_for_promotion(&self, node_id: &str, wait: Duration) -> Result<ClusterNode> {
        let deadline = Instant::now() + wait;
        loop {
            if remaining_before(deadline).is_none() {
                return Err(ClusterFixtureError::Timeout {
                    operation: format!("waiting for node {node_id} promotion"),
                    timeout: wait,
                });
            }
            if let Ok(topology) = self.topology_before(deadline, wait).await
                && let Some(node) = topology.node_by_id(node_id)
                && node.is_master()
            {
                return Ok(node.clone());
            }
            sleep_before(deadline).await;
        }
    }

    async fn replication_links_ready_before(
        &self,
        topology: &ClusterTopology,
        deadline: Instant,
        budget: Duration,
    ) -> bool {
        let replicas: Vec<&ClusterNode> = topology.replicas().collect();
        for (position, replica) in replicas.iter().enumerate() {
            let Some(remaining) = remaining_before(deadline) else {
                return false;
            };
            let probes_left = (replicas.len() - position) as u32;
            let probe_timeout = self.operation_timeout.min(remaining / probes_left);
            let Ok(info) = self
                .run_node_for(
                    replica.index,
                    &["INFO", "replication"],
                    probe_timeout,
                    budget,
                    format!("checking replication on node {}", replica.index),
                )
                .await
            else {
                return false;
            };
            if !info.lines().any(|line| line == "master_link_status:up") {
                return false;
            }
        }
        true
    }
}

impl Drop for ClusterFixture {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        // A failed assertion can leave a node SIGSTOP'd. Resume first so the
        // wrapper's graceful shutdown does not stall and leak test processes.
        chaos::recover(&handle);
        let cluster_base = handle.cluster_base().to_path_buf();
        // RedisClusterHandle drops its node handles synchronously. Only after
        // every process has stopped is it safe to remove their working tree.
        drop(handle);
        let _ = remove_cluster_base(&cluster_base);
    }
}

/// Compute the Redis Cluster hash slot for `key`.
pub fn hash_slot(key: &[u8]) -> u16 {
    let hash_input = hash_tag(key).unwrap_or(key);
    crc16_xmodem(hash_input) % SLOT_COUNT
}

/// Generate a deterministic key that hashes to exactly `slot`.
///
/// # Panics
///
/// Panics if `slot >= 16384`.
pub fn key_for_slot(slot: u16) -> String {
    assert!(slot < SLOT_COUNT, "Redis Cluster slot must be below 16384");
    for nonce in 0_u32..=u32::MAX {
        let key = format!("{{redis-tower-{slot}-{nonce}}}");
        if hash_slot(key.as_bytes()) == slot {
            return key;
        }
    }
    unreachable!("a 16-bit CRC search must find every Redis hash slot")
}

/// Generate a deterministic key for the midpoint of `range`.
pub fn key_in_slot_range(range: SlotRange) -> String {
    assert!(range.start <= range.end && range.end < SLOT_COUNT);
    key_for_slot(range.start + (range.end - range.start) / 2)
}

fn ensure_redis_success(output: &str, operation: String) -> Result<()> {
    let response = output.trim();
    if response.starts_with("ERR ")
        || response.starts_with("-ERR ")
        || response.starts_with("(error)")
    {
        Err(ClusterFixtureError::RedisCommand {
            operation,
            response: response.to_owned(),
        })
    } else {
        Ok(())
    }
}

fn remaining_before(deadline: Instant) -> Option<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    (!remaining.is_zero()).then_some(remaining)
}

async fn sleep_before(deadline: Instant) {
    if let Some(remaining) = remaining_before(deadline) {
        sleep(POLL_INTERVAL.min(remaining)).await;
    }
}

struct PortRangeLease {
    base_port: u16,
    ports: Vec<u16>,
    listeners: Vec<TcpListener>,
}

impl PortRangeLease {
    fn base_port(&self) -> u16 {
        self.base_port
    }

    fn release_listeners(&mut self) {
        self.listeners.clear();
    }
}

impl Drop for PortRangeLease {
    fn drop(&mut self) {
        let mut reserved = RESERVED_PORTS
            .get_or_init(|| Mutex::new(HashSet::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for port in &self.ports {
            reserved.remove(port);
        }
    }
}

struct StartupArtifactGuard {
    base_port: u16,
    existing_bases: HashSet<PathBuf>,
    armed: bool,
}

impl StartupArtifactGuard {
    fn new(base_port: u16) -> std::io::Result<Self> {
        Ok(Self {
            base_port,
            existing_bases: current_process_cluster_bases()?,
            armed: true,
        })
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    fn cleanup(&mut self) -> std::io::Result<()> {
        if !self.armed {
            return Ok(());
        }

        for path in current_process_cluster_bases()?.difference(&self.existing_bases) {
            if startup_base_belongs_to_range(path, self.base_port)? {
                cleanup_startup_base(path, self.base_port)?;
            }
        }
        self.armed = false;
        Ok(())
    }
}

impl Drop for StartupArtifactGuard {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

async fn await_cluster_start<F>(
    mut cleanup: StartupArtifactGuard,
    startup_timeout: Duration,
    start: F,
) -> Result<RedisClusterHandle>
where
    F: Future<Output = redis_server_wrapper::Result<RedisClusterHandle>>,
{
    match timeout(startup_timeout, start).await {
        Ok(Ok(handle)) => {
            cleanup.disarm();
            Ok(handle)
        }
        Ok(Err(error)) => {
            cleanup.cleanup()?;
            Err(error.into())
        }
        Err(_) => {
            cleanup.cleanup()?;
            Err(ClusterFixtureError::Timeout {
                operation: "starting Redis Cluster processes".into(),
                timeout: startup_timeout,
            })
        }
    }
}

fn current_process_cluster_bases() -> std::io::Result<HashSet<PathBuf>> {
    let temp_dir = std::env::temp_dir();
    let prefix = format!("redis-cluster-wrapper-{}-", std::process::id());
    let mut bases = HashSet::new();
    for entry in fs::read_dir(&temp_dir)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if !file_type.is_dir() || file_type.is_symlink() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(unique) = name.strip_prefix(&prefix) else {
            continue;
        };
        if !unique.is_empty() && unique.bytes().all(|byte| byte.is_ascii_digit()) {
            bases.insert(entry.path());
        }
    }
    Ok(bases)
}

fn startup_base_belongs_to_range(path: &Path, base_port: u16) -> std::io::Result<bool> {
    let Ok(ports) = client_ports(base_port) else {
        return Ok(false);
    };
    let allowed_names: HashSet<String> = ports
        .into_iter()
        .map(|port| format!("node-{port}"))
        .collect();
    let mut saw_node = false;
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Ok(false);
        };
        if !file_type.is_dir() || file_type.is_symlink() || !allowed_names.contains(name) {
            return Ok(false);
        }
        saw_node = true;
    }
    Ok(saw_node)
}

fn cleanup_startup_base(path: &Path, base_port: u16) -> std::io::Result<()> {
    let deadline = std::time::Instant::now() + STARTUP_CLEANUP_TIMEOUT;
    let mut quiet_since = None;
    loop {
        if !path.exists() {
            return Ok(());
        }
        if !startup_base_belongs_to_range(path, base_port)? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "startup directory ownership changed while cleaning {}",
                    path.display()
                ),
            ));
        }

        if stop_startup_nodes(path, base_port) {
            quiet_since = None;
        }
        if startup_ports_are_free(base_port) {
            let quiet_since = quiet_since.get_or_insert_with(std::time::Instant::now);
            if quiet_since.elapsed() >= STARTUP_CLEANUP_QUIET_PERIOD {
                // Revalidate immediately before deletion. A path that was
                // replaced or gained an unexpected child is preserved.
                if !startup_base_belongs_to_range(path, base_port)? {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "startup directory ownership changed while cleaning {}",
                            path.display()
                        ),
                    ));
                }
                return remove_cluster_base(path);
            }
        } else {
            quiet_since = None;
        }

        if std::time::Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!(
                    "startup processes or ports did not quiesce for {}",
                    path.display()
                ),
            ));
        }
        std::thread::sleep(STARTUP_CLEANUP_POLL_INTERVAL);
    }
}

fn stop_startup_nodes(path: &Path, base_port: u16) -> bool {
    let Ok(ports) = client_ports(base_port) else {
        return false;
    };
    let mut stopped_process = false;
    for port in ports {
        let node_name = format!("node-{port}");
        let node_dir = path.join(&node_name);
        for pidfile in [
            node_dir.join("redis.pid"),
            node_dir.join(&node_name).join("redis.pid"),
        ] {
            if let Some(pid) = redis_server_wrapper::process::read_pidfile(&pidfile)
                && redis_server_wrapper::process::pid_alive(pid)
            {
                redis_server_wrapper::process::force_kill(pid);
                stopped_process = true;
            }
        }
    }
    stopped_process
}

fn startup_ports_are_free(base_port: u16) -> bool {
    let Ok(ports) = fixture_ports(base_port) else {
        return false;
    };
    let mut listeners = Vec::with_capacity(ports.len());
    for port in ports {
        let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) else {
            return false;
        };
        listeners.push(listener);
    }
    true
}

fn remove_cluster_base(path: &Path) -> std::io::Result<()> {
    let temp_dir = std::env::temp_dir();
    let safe_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("redis-cluster-wrapper-"));
    if path.parent() != Some(temp_dir.as_path()) || !safe_name {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "refusing to remove non-fixture cluster directory {}",
                path.display()
            ),
        ));
    }
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn parse_topology(output: &str, handle: &RedisClusterHandle) -> Result<ClusterTopology> {
    let mut nodes = Vec::new();
    for line in output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 8 {
            return Err(ClusterFixtureError::InvalidTopology(format!(
                "malformed CLUSTER NODES line: {line}"
            )));
        }
        let port = parse_node_port(parts[1]).ok_or_else(|| {
            ClusterFixtureError::InvalidTopology(format!(
                "could not parse node address {}",
                parts[1]
            ))
        })?;
        let index = handle
            .nodes()
            .iter()
            .position(|node| node.port() == port)
            .ok_or_else(|| {
                ClusterFixtureError::InvalidTopology(format!(
                    "topology contains unknown node port {port}"
                ))
            })?;
        let flags: Vec<String> = parts[2].split(',').map(String::from).collect();
        let role = if flags.iter().any(|flag| flag == "master") {
            ClusterNodeRole::Master
        } else if flags
            .iter()
            .any(|flag| matches!(flag.as_str(), "slave" | "replica"))
        {
            if parts[3] == "-" {
                return Err(ClusterFixtureError::InvalidTopology(format!(
                    "replica {} has no master ID",
                    parts[0]
                )));
            }
            ClusterNodeRole::Replica {
                master_id: parts[3].to_owned(),
            }
        } else {
            return Err(ClusterFixtureError::InvalidTopology(format!(
                "node {} has neither master nor replica role",
                parts[0]
            )));
        };
        let slots = if matches!(role, ClusterNodeRole::Master) {
            parts[8..]
                .iter()
                .filter_map(|value| parse_slot_range(value))
                .collect()
        } else {
            Vec::new()
        };
        nodes.push(ClusterNode {
            index,
            id: parts[0].to_owned(),
            addr: handle.node(index).addr(),
            port,
            role,
            slots,
            connected: parts[7] == "connected",
            flags,
        });
    }
    nodes.sort_by_key(|node| node.index);
    Ok(ClusterTopology { nodes })
}

fn parse_node_port(addr: &str) -> Option<u16> {
    let endpoint = addr.split(',').next()?.split('@').next()?;
    endpoint.rsplit_once(':')?.1.parse().ok()
}

fn parse_slot_range(value: &str) -> Option<SlotRange> {
    if value.starts_with('[') {
        return None;
    }
    if let Some((start, end)) = value.split_once('-') {
        Some(SlotRange {
            start: start.parse().ok()?,
            end: end.parse().ok()?,
        })
    } else {
        let slot = value.parse().ok()?;
        Some(SlotRange {
            start: slot,
            end: slot,
        })
    }
}

fn validate_slot(slot: u16) -> Result<()> {
    if slot < SLOT_COUNT {
        Ok(())
    } else {
        Err(ClusterFixtureError::InvalidTopology(format!(
            "slot {slot} is outside 0..16383"
        )))
    }
}

fn client_ports(base_port: u16) -> Result<Vec<u16>> {
    let last_client_port = base_port
        .checked_add(NODE_COUNT as u16 - 1)
        .ok_or(ClusterFixtureError::InvalidBasePort(base_port))?;
    let last_bus_port = last_client_port
        .checked_add(CLUSTER_BUS_PORT_OFFSET)
        .ok_or(ClusterFixtureError::InvalidBasePort(base_port))?;
    if base_port == 0 || last_bus_port == 0 {
        return Err(ClusterFixtureError::InvalidBasePort(base_port));
    }

    Ok((base_port..=last_client_port).collect())
}

fn fixture_ports(base_port: u16) -> Result<Vec<u16>> {
    let client_ports = client_ports(base_port)?;
    let mut ports = Vec::with_capacity(NODE_COUNT * 2);
    ports.extend(client_ports.iter().copied());
    ports.extend(
        client_ports
            .into_iter()
            .map(|port| port + CLUSTER_BUS_PORT_OFFSET),
    );
    Ok(ports)
}

fn reserve_port_range(base_port: u16) -> Result<PortRangeLease> {
    let ports = fixture_ports(base_port)?;
    {
        let mut reserved = RESERVED_PORTS
            .get_or_init(|| Mutex::new(HashSet::new()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if ports.iter().any(|port| reserved.contains(port)) {
            return Err(ClusterFixtureError::PortRangeUnavailable(base_port));
        }
        reserved.extend(ports.iter().copied());
    }

    let mut listeners = Vec::with_capacity(NODE_COUNT * 2);
    for port in &ports {
        match TcpListener::bind(("127.0.0.1", *port)) {
            Ok(listener) => listeners.push(listener),
            Err(_) => {
                let lease = PortRangeLease {
                    base_port,
                    ports,
                    listeners,
                };
                drop(lease);
                return Err(ClusterFixtureError::PortRangeUnavailable(base_port));
            }
        }
    }
    Ok(PortRangeLease {
        base_port,
        ports,
        listeners,
    })
}

fn find_and_reserve_port_range() -> Result<PortRangeLease> {
    let process_offset = (std::process::id() % AUTO_PORT_SPAN as u32) as u16;
    for attempt in 0..AUTO_PORT_SPAN {
        let offset = (process_offset + attempt.wrapping_mul(17)) % AUTO_PORT_SPAN;
        let candidate = AUTO_PORT_START + offset;
        if let Ok(lease) = reserve_port_range(candidate) {
            return Ok(lease);
        }
    }
    Err(ClusterFixtureError::PortRangeUnavailable(AUTO_PORT_START))
}

fn hash_tag(key: &[u8]) -> Option<&[u8]> {
    let open = key.iter().position(|byte| *byte == b'{')?;
    let rest = &key[open + 1..];
    let close = rest.iter().position(|byte| *byte == b'}')?;
    (close > 0).then_some(&rest[..close])
}

fn crc16_xmodem(bytes: &[u8]) -> u16 {
    let mut crc = 0_u16;
    for byte in bytes {
        crc ^= u16::from(*byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn synthetic_cluster_base() -> PathBuf {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "redis-cluster-wrapper-{}-{time}{sequence}",
            std::process::id()
        ))
    }

    fn create_synthetic_node(base: &Path, port: u16) {
        fs::create_dir_all(base.join(format!("node-{port}"))).unwrap();
    }

    #[test]
    fn redis_crc_and_hash_tags_match_known_slots() {
        assert_eq!(crc16_xmodem(b"123456789"), 0x31c3);
        assert_eq!(hash_slot(b"foo"), 12_182);
        assert_eq!(hash_slot(b"{user1000}.following"), 3_443);
        assert_eq!(hash_slot(b"{user1000}.followers"), 3_443);
        assert_eq!(
            hash_slot(b"{}empty-tag"),
            crc16_xmodem(b"{}empty-tag") % SLOT_COUNT
        );
    }

    #[test]
    fn generated_keys_hit_requested_slots() {
        for slot in [0, 1, 42, 5_460, 5_461, 10_922, 10_923, 16_383] {
            assert_eq!(hash_slot(key_for_slot(slot).as_bytes()), slot);
        }
    }

    #[test]
    fn slot_range_parser_ignores_migration_markers() {
        assert_eq!(
            parse_slot_range("0-5460"),
            Some(SlotRange {
                start: 0,
                end: 5_460
            })
        );
        assert_eq!(
            parse_slot_range("42"),
            Some(SlotRange { start: 42, end: 42 })
        );
        assert_eq!(parse_slot_range("[42->-node-id]"), None);
    }

    #[test]
    fn node_port_parser_accepts_cluster_metadata() {
        assert_eq!(parse_node_port("127.0.0.1:7000@17000,redis-1"), Some(7_000));
        assert_eq!(parse_node_port("[::1]:7001@17001"), Some(7_001));
    }

    #[test]
    fn builder_uses_failover_friendly_defaults() {
        let builder = ClusterFixtureBuilder::default();
        assert_eq!(builder.cluster_node_timeout_ms, 1_000);
        assert_eq!(builder.operation_timeout, Duration::from_secs(3));
        assert!(builder.base_port.is_none());
    }

    #[test]
    fn process_port_leases_reject_overlap_until_drop() {
        // Keep this range outside automatic allocation so parallel tests cannot
        // claim it between the drop and re-acquisition assertions below.
        let first = (53_000..=55_000)
            .step_by(17)
            .find_map(|base_port| reserve_port_range(base_port).ok())
            .expect("an explicit high-port fixture range should be available");
        let base_port = first.base_port();
        assert!(matches!(
            reserve_port_range(base_port),
            Err(ClusterFixtureError::PortRangeUnavailable(port)) if port == base_port
        ));
        assert!(matches!(
            reserve_port_range(base_port + 1),
            Err(ClusterFixtureError::PortRangeUnavailable(port)) if port == base_port + 1
        ));

        drop(first);
        let replacement = reserve_port_range(base_port).unwrap();
        assert_eq!(replacement.base_port(), base_port);
    }

    #[test]
    fn concurrent_automatic_port_leases_are_disjoint() {
        const WORKERS: usize = 8;
        let barrier = std::sync::Barrier::new(WORKERS);
        let leases = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..WORKERS)
                .map(|_| {
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        find_and_reserve_port_range().unwrap()
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });

        let mut observed = HashSet::new();
        for lease in &leases {
            for port in &lease.ports {
                assert!(observed.insert(*port), "port {port} was leased twice");
            }
        }
    }

    #[tokio::test]
    async fn failed_start_removes_only_its_new_artifact_tree() {
        let mut lease = find_and_reserve_port_range().unwrap();
        let base_port = lease.base_port();
        lease.release_listeners();
        let preexisting = synthetic_cluster_base();
        create_synthetic_node(&preexisting, base_port);
        let cleanup = StartupArtifactGuard::new(base_port).unwrap();
        let owned = synthetic_cluster_base();
        let unrelated = synthetic_cluster_base();
        let owned_for_start = owned.clone();
        let unrelated_for_start = unrelated.clone();

        let result = await_cluster_start(cleanup, Duration::from_secs(1), async move {
            create_synthetic_node(&owned_for_start, base_port);
            create_synthetic_node(&unrelated_for_start, base_port + NODE_COUNT as u16);
            Err(redis_server_wrapper::Error::ServerStart { port: base_port })
        })
        .await;

        assert!(matches!(result, Err(ClusterFixtureError::Wrapper(_))));
        assert!(!owned.exists());
        assert!(preexisting.exists());
        assert!(unrelated.exists());
        remove_cluster_base(&preexisting).unwrap();
        remove_cluster_base(&unrelated).unwrap();
    }

    #[tokio::test]
    async fn cancelled_start_removes_its_artifact_tree() {
        let mut lease = find_and_reserve_port_range().unwrap();
        let base_port = lease.base_port();
        lease.release_listeners();
        let cleanup = StartupArtifactGuard::new(base_port).unwrap();
        let owned = synthetic_cluster_base();
        let owned_for_start = owned.clone();
        let (created_tx, created_rx) = tokio::sync::oneshot::channel();
        let start = async move {
            create_synthetic_node(&owned_for_start, base_port);
            let _ = created_tx.send(());
            std::future::pending::<redis_server_wrapper::Result<RedisClusterHandle>>().await
        };
        let mut startup = Box::pin(await_cluster_start(cleanup, Duration::from_secs(60), start));

        tokio::select! {
            result = &mut startup => panic!("startup unexpectedly completed: {}", result.is_ok()),
            result = created_rx => result.unwrap(),
        }
        assert!(owned.exists());
        drop(startup);
        assert!(!owned.exists());
    }

    #[tokio::test]
    async fn timed_out_start_removes_its_artifact_tree() {
        let mut lease = find_and_reserve_port_range().unwrap();
        let base_port = lease.base_port();
        lease.release_listeners();
        let cleanup = StartupArtifactGuard::new(base_port).unwrap();
        let owned = synthetic_cluster_base();
        let owned_for_start = owned.clone();
        let start = async move {
            create_synthetic_node(&owned_for_start, base_port);
            std::future::pending::<redis_server_wrapper::Result<RedisClusterHandle>>().await
        };

        let result = await_cluster_start(cleanup, Duration::from_millis(10), start).await;
        assert!(matches!(
            result,
            Err(ClusterFixtureError::Timeout { timeout, .. })
                if timeout == Duration::from_millis(10)
        ));
        assert!(!owned.exists());
    }

    #[cfg(unix)]
    #[test]
    fn startup_cleanup_reaps_a_late_pidfile_before_removing_the_tree() {
        let mut lease = find_and_reserve_port_range().unwrap();
        let base_port = lease.base_port();
        lease.release_listeners();
        let mut cleanup = StartupArtifactGuard::new(base_port).unwrap();
        let owned = synthetic_cluster_base();
        let node_name = format!("node-{base_port}");
        let process_dir = owned.join(&node_name).join(&node_name);
        fs::create_dir_all(&process_dir).unwrap();

        let child = std::process::Command::new("sleep")
            .arg("60")
            .spawn()
            .unwrap();
        let pid = child.id();
        let waiter = std::thread::spawn(move || child.wait_with_output().unwrap().status);
        let pidfile = process_dir.join("redis.pid");
        let writer = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            fs::write(pidfile, pid.to_string()).unwrap();
        });

        cleanup.cleanup().unwrap();
        writer.join().unwrap();
        let status = waiter.join().unwrap();
        assert!(!status.success(), "synthetic process was not reaped");
        assert!(!owned.exists());
    }

    #[test]
    fn cleanup_only_removes_prefixed_immediate_temp_children() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let fixture_dir = std::env::temp_dir().join(format!(
            "redis-cluster-wrapper-cleanup-test-{}-{unique}",
            std::process::id()
        ));
        let nested = fixture_dir.join("node-1");
        fs::create_dir_all(&nested).unwrap();
        fs::write(nested.join("nodes.conf"), "fixture").unwrap();
        remove_cluster_base(&fixture_dir).unwrap();
        assert!(!fixture_dir.exists());

        let wrong_prefix = std::env::temp_dir().join(format!(
            "redis-cluster-unsafe-cleanup-test-{}-{unique}",
            std::process::id()
        ));
        assert_eq!(
            remove_cluster_base(&wrong_prefix).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
        let nested_fixture = std::env::temp_dir()
            .join("nested")
            .join(format!("redis-cluster-wrapper-{unique}"));
        assert_eq!(
            remove_cluster_base(&nested_fixture).unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    #[tokio::test]
    #[ignore = "requires redis-server and redis-cli"]
    async fn live_startup_timeout_reaps_partial_nodes_and_artifacts() {
        let base_port = 17_850;
        let existing_bases = current_process_cluster_bases().unwrap();
        let result = ClusterFixture::builder()
            .base_port(base_port)
            // The wrapper has a fixed 500 ms shutdown prelude. This expires
            // while its first few daemonized nodes may still be starting.
            .startup_timeout(Duration::from_millis(650))
            .start()
            .await;

        assert!(matches!(
            result,
            Err(ClusterFixtureError::Timeout { timeout, .. })
                if timeout == Duration::from_millis(650)
        ));
        assert_eq!(current_process_cluster_bases().unwrap(), existing_bases);
        let lease = reserve_port_range(base_port).unwrap();
        assert_eq!(lease.base_port(), base_port);
    }

    #[tokio::test]
    #[ignore = "requires redis-server and redis-cli"]
    async fn live_fixture_reshards_after_promotion_and_cleans_directory() {
        let fixture = ClusterFixture::start().await.unwrap();
        let cluster_base = fixture.handle().cluster_base().to_path_buf();
        assert!(cluster_base.exists());
        let topology = fixture.topology().await.unwrap();
        assert!(topology.is_ready());

        let slot = 42;
        let old_owner = topology.owner_of_slot(slot).unwrap().clone();
        let initial_target = topology
            .masters()
            .find(|node| node.id != old_owner.id)
            .unwrap()
            .clone();
        let promoted = topology.replicas_of(&initial_target.id)[0].clone();
        fixture.promote_replica(promoted.index).await.unwrap();
        fixture
            .wait_for_promotion(&promoted.id, Duration::from_secs(20))
            .await
            .unwrap();
        fixture
            .wait_for_ready(Duration::from_secs(20))
            .await
            .unwrap();

        // The target was a replica at startup. Completing this migration must
        // discover it as a current master rather than using the wrapper's stale
        // startup-time master slice.
        fixture.reshard_slot(slot, promoted.index).await.unwrap();
        fixture
            .wait_for_slot_owner(slot, &promoted.id, Duration::from_secs(10))
            .await
            .unwrap();

        let killed = fixture.kill_slot_owner(slot).await.unwrap();
        let replacement = fixture
            .wait_for_slot_owner_change(slot, &killed.id, Duration::from_secs(30))
            .await
            .unwrap();
        assert_ne!(replacement.id, killed.id);

        drop(fixture);
        assert!(
            !cluster_base.exists(),
            "fixture Drop left {} behind",
            cluster_base.display()
        );
    }

    #[tokio::test]
    #[ignore = "requires redis-server and redis-cli"]
    async fn live_wait_deadline_is_hard_with_frozen_nodes_and_cleanup_runs() {
        let fixture = ClusterFixture::builder()
            .operation_timeout(Duration::from_secs(2))
            .start()
            .await
            .unwrap();
        let cluster_base = fixture.handle().cluster_base().to_path_buf();
        for node in fixture.handle().nodes() {
            chaos::freeze_node(node);
        }

        let wait = Duration::from_millis(200);
        let started = Instant::now();
        let error = fixture
            .wait_for_promotion("node-that-does-not-exist", wait)
            .await
            .unwrap_err();
        assert!(matches!(error, ClusterFixtureError::Timeout { .. }));
        assert!(
            started.elapsed() < Duration::from_millis(750),
            "declared {wait:?} wait took {:?}",
            started.elapsed()
        );

        // Drop must recover the SIGSTOP'd processes, wait for their shutdown,
        // and only then remove the working directory.
        drop(fixture);
        assert!(
            !cluster_base.exists(),
            "fixture Drop left {} behind",
            cluster_base.display()
        );
    }
}
