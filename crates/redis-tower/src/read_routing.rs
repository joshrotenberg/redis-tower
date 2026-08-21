//! Shared read-preference and replica-routing primitives.
//!
//! [`ReadPreference`] and [`ReadRoutingStrategy`] are used by both
//! `redis-tower-cluster` (routing per hash slot) and `redis-tower-sentinel`
//! (routing across a monitored master's replica set). They live here, in a
//! crate both siblings already depend on, so neither pulls in the other --
//! `redis-tower-cluster` re-exports these types under their original paths
//! for source compatibility.
//!
//! [`NodeAddr`] is likewise shared: cluster topology and sentinel replica
//! discovery both resolve to a `host:port` pair, and [`ReadRoutingStrategy`]
//! selects among a slice of them regardless of which crate is asking.
//!
//! [`is_readonly_command`] classifies a serialized command frame as
//! read-only or not, the other input routing needs beyond an address: given
//! a [`ReadPreference`] other than [`ReadPreference::Master`], only commands
//! this function accepts are safe to send to a replica.
//!
//! Read preferences have the same availability semantics in every client that
//! supports replica routing. [`ReadPreference::Replica`] is strict: an eligible
//! read fails when no usable replica is available, and is never silently sent
//! to the master. [`ReadPreference::PreferReplica`] makes the same selection
//! attempt but falls back to the master. Writes are unaffected and always use
//! the master. The strict guarantee covers the full command attempt: retries
//! and cluster redirects must not replay an eligible `Replica` read against a
//! master.

use redis_tower_core::Frame;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;

/// Address of a Redis node, identified by host and port.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeAddr {
    /// Hostname or IP address.
    pub host: String,
    /// TCP port.
    pub port: u16,
}

impl NodeAddr {
    /// Create a node address from a host and port.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// Format as `"host:port"`.
    pub fn addr_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    /// Parse a `"host:port"` string into a [`NodeAddr`].
    ///
    /// Splits on the last `:` and preserves the host portion verbatim, so both
    /// hostnames and bracketed IPv6 addresses round-trip. Returns `None` if the
    /// host is empty, there is no `:`, or the port is not a valid `u16`.
    pub fn parse(addr: &str) -> Option<Self> {
        let (host, port) = addr.rsplit_once(':')?;
        if host.is_empty() {
            return None;
        }
        let port = port.parse().ok()?;
        Some(Self {
            host: host.to_string(),
            port,
        })
    }
}

impl std::fmt::Display for NodeAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// Read routing preference for read-only commands.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ReadPreference {
    /// Always read from the master (default).
    #[default]
    Master,
    /// Route read-only commands strictly to a replica.
    ///
    /// If no usable replica is available, return an error rather than falling
    /// back to the master.
    Replica,
    /// Prefer a replica, but fall back to the master when none is usable.
    PreferReplica,
}

/// Strategy for selecting which replica to read from.
///
/// Implement this trait to provide custom replica selection logic.
/// Built-in implementations include [`RoundRobinRouting`], [`RandomRouting`],
/// [`FirstReplicaRouting`], and [`AdaptiveReplicaRouting`].
///
/// `slot` is a Redis Cluster hash slot when the caller is cluster-aware, or
/// `0` when the caller has no slot concept (Sentinel monitors one shard, not
/// 16384 of them). Implementations that only care about the replica list --
/// every built-in strategy -- can ignore it.
pub trait ReadRoutingStrategy: Send + Sync + 'static {
    /// Select a replica address for the given slot.
    ///
    /// `replicas` is the list of available replica addresses for the slot.
    /// Return the selected address, or `None` when no replica should be used.
    /// The caller's [`ReadPreference`] determines whether that becomes an
    /// error or a fallback to the master.
    fn select_replica<'a>(&self, slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr>;

    /// Observe the completed attempt for a selected replica.
    ///
    /// Stateless strategies can keep the default no-op implementation.
    /// Adaptive strategies use successful response latency and transport-level
    /// failures to update future selection decisions. Redis command errors are
    /// successful node responses and should be reported as
    /// [`ReplicaRoutingOutcome::Success`].
    fn record_outcome(
        &self,
        _replica: &NodeAddr,
        _latency: Duration,
        _outcome: ReplicaRoutingOutcome,
    ) {
    }
}

/// Health outcome for one attempt against a selected replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRoutingOutcome {
    /// The replica produced a complete Redis response.
    Success,
    /// The attempt failed at the transport, protocol, or timeout boundary.
    Failure,
}

/// Invalid [`AdaptiveReplicaRouting`] configuration.
#[derive(Debug, Error, PartialEq)]
pub enum AdaptiveReplicaRoutingConfigError {
    /// EWMA alpha must be finite and in the interval `(0, 1]`.
    #[error("EWMA alpha must be finite and in the interval (0, 1]")]
    InvalidEwmaAlpha,
    /// At least one consecutive failure must be required for ejection.
    #[error("replica failure threshold must be greater than zero")]
    ZeroFailureThreshold,
    /// An ejection must last long enough to exclude the replica from a
    /// subsequent selection.
    #[error("replica ejection duration must be greater than zero")]
    ZeroEjectionDuration,
    /// Availability-zone names are explicit non-empty identifiers.
    #[error("availability-zone names must not be empty")]
    EmptyAvailabilityZone,
}

/// Builder for [`AdaptiveReplicaRouting`].
///
/// Defaults use an EWMA alpha of `0.2`, eject after three consecutive
/// failures for 30 seconds, and retain at least one candidate per replica set.
#[derive(Debug, Clone)]
pub struct AdaptiveReplicaRoutingBuilder {
    local_zone: Option<String>,
    replica_zones: HashMap<NodeAddr, String>,
    ewma_alpha: f64,
    failure_threshold: u32,
    ejection_duration: Duration,
    minimum_healthy_replicas: usize,
}

impl Default for AdaptiveReplicaRoutingBuilder {
    fn default() -> Self {
        Self {
            local_zone: None,
            replica_zones: HashMap::new(),
            ewma_alpha: 0.2,
            failure_threshold: 3,
            ejection_duration: Duration::from_secs(30),
            minimum_healthy_replicas: 1,
        }
    }
}

impl AdaptiveReplicaRoutingBuilder {
    /// Create a builder with production-oriented defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Prefer healthy replicas in this availability zone.
    ///
    /// When no eligible candidate is mapped to this zone, selection falls
    /// back to all eligible replicas.
    pub fn local_zone(mut self, zone: impl Into<String>) -> Self {
        self.local_zone = Some(zone.into());
        self
    }

    /// Associate a replica address with an availability zone.
    ///
    /// The address must match the final address exposed to the routing
    /// strategy after any cluster address remapping.
    pub fn replica_zone(mut self, replica: NodeAddr, zone: impl Into<String>) -> Self {
        self.replica_zones.insert(replica, zone.into());
        self
    }

    /// Set the EWMA smoothing factor in the interval `(0, 1]`.
    pub fn ewma_alpha(mut self, alpha: f64) -> Self {
        self.ewma_alpha = alpha;
        self
    }

    /// Set the consecutive transport-failure threshold for ejection.
    pub fn failure_threshold(mut self, failures: u32) -> Self {
        self.failure_threshold = failures;
        self
    }

    /// Set how long an unhealthy replica remains ejected before a recovery
    /// probe becomes eligible.
    pub fn ejection_duration(mut self, duration: Duration) -> Self {
        self.ejection_duration = duration;
        self
    }

    /// Set the minimum number of candidates retained from each supplied
    /// replica set, even when more replicas are currently ejected.
    ///
    /// Set this to zero to permit every replica to be ejected. A strict
    /// [`ReadPreference::Replica`] read then fails, while
    /// [`ReadPreference::PreferReplica`] can fall back to the master.
    pub fn minimum_healthy_replicas(mut self, minimum: usize) -> Self {
        self.minimum_healthy_replicas = minimum;
        self
    }

    /// Validate the configuration and construct the strategy.
    pub fn build(self) -> Result<AdaptiveReplicaRouting, AdaptiveReplicaRoutingConfigError> {
        if !self.ewma_alpha.is_finite() || !(0.0 < self.ewma_alpha && self.ewma_alpha <= 1.0) {
            return Err(AdaptiveReplicaRoutingConfigError::InvalidEwmaAlpha);
        }
        if self.failure_threshold == 0 {
            return Err(AdaptiveReplicaRoutingConfigError::ZeroFailureThreshold);
        }
        if self.ejection_duration.is_zero() {
            return Err(AdaptiveReplicaRoutingConfigError::ZeroEjectionDuration);
        }
        if self.local_zone.as_deref().is_some_and(str::is_empty)
            || self.replica_zones.values().any(String::is_empty)
        {
            return Err(AdaptiveReplicaRoutingConfigError::EmptyAvailabilityZone);
        }

        Ok(AdaptiveReplicaRouting {
            local_zone: self.local_zone,
            replica_zones: self.replica_zones,
            ewma_alpha: self.ewma_alpha,
            failure_threshold: self.failure_threshold,
            ejection_duration: self.ejection_duration,
            minimum_healthy_replicas: self.minimum_healthy_replicas,
            state: Mutex::new(AdaptiveRoutingState::default()),
        })
    }
}

/// Replica selector composing AZ affinity, EWMA latency, and health ejection.
///
/// Selection first excludes replicas whose consecutive transport failures
/// triggered a live ejection. It restores the soonest-recovering ejected
/// candidates only when needed to preserve `minimum_healthy_replicas`. Within
/// that eligible set it prefers the configured local availability zone when
/// possible. New and recovered replicas are sampled round-robin; once latency
/// samples exist, inverse-EWMA weighted selection biases traffic toward faster
/// replicas without permanently starving slower ones.
///
/// No background task is created. Recovery is evaluated lazily during
/// selection, and callers provide observations through
/// [`ReadRoutingStrategy::record_outcome`].
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use redis_tower::{AdaptiveReplicaRouting, NodeAddr};
///
/// let routing = AdaptiveReplicaRouting::builder()
///     .local_zone("us-east-1a")
///     .replica_zone(NodeAddr::new("replica-a.internal", 6379), "us-east-1a")
///     .replica_zone(NodeAddr::new("replica-b.internal", 6379), "us-east-1b")
///     .failure_threshold(2)
///     .ejection_duration(Duration::from_secs(15))
///     .build()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug)]
pub struct AdaptiveReplicaRouting {
    local_zone: Option<String>,
    replica_zones: HashMap<NodeAddr, String>,
    ewma_alpha: f64,
    failure_threshold: u32,
    ejection_duration: Duration,
    minimum_healthy_replicas: usize,
    state: Mutex<AdaptiveRoutingState>,
}

#[derive(Debug, Default)]
struct AdaptiveRoutingState {
    replicas: HashMap<NodeAddr, AdaptiveReplicaState>,
    sequence: u64,
}

#[derive(Debug, Default)]
struct AdaptiveReplicaState {
    ewma_latency_seconds: Option<f64>,
    consecutive_failures: u32,
    ejected_until: Option<Instant>,
}

impl AdaptiveReplicaRouting {
    /// Begin configuring an adaptive routing strategy.
    pub fn builder() -> AdaptiveReplicaRoutingBuilder {
        AdaptiveReplicaRoutingBuilder::new()
    }

    fn select_replica_at<'a>(
        &self,
        replicas: &'a [NodeAddr],
        now: Instant,
    ) -> Option<&'a NodeAddr> {
        if replicas.is_empty() {
            return None;
        }

        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut eligible = Vec::with_capacity(replicas.len());
        let mut ejected = Vec::new();

        for (index, replica) in replicas.iter().enumerate() {
            let replica_state = state.replicas.entry(replica.clone()).or_default();
            match replica_state.ejected_until {
                Some(until) if until > now => ejected.push((index, until)),
                Some(_) => {
                    // Force one fresh sample after timed recovery rather than
                    // immediately trusting stale latency from before ejection.
                    replica_state.ejected_until = None;
                    replica_state.consecutive_failures = 0;
                    replica_state.ewma_latency_seconds = None;
                    eligible.push(index);
                }
                None => eligible.push(index),
            }
        }

        let floor = self.minimum_healthy_replicas.min(replicas.len());
        if eligible.len() < floor {
            ejected.sort_by_key(|(index, until)| (*until, *index));
            eligible.extend(
                ejected
                    .into_iter()
                    .take(floor - eligible.len())
                    .map(|(index, _until)| index),
            );
        }

        if let Some(local_zone) = self.local_zone.as_deref() {
            let local: Vec<usize> = eligible
                .iter()
                .copied()
                .filter(|index| {
                    self.replica_zones
                        .get(&replicas[*index])
                        .is_some_and(|zone| zone == local_zone)
                })
                .collect();
            if !local.is_empty() {
                eligible = local;
            }
        }

        let unknown: Vec<usize> = eligible
            .iter()
            .copied()
            .filter(|index| {
                state
                    .replicas
                    .get(&replicas[*index])
                    .and_then(|replica| replica.ewma_latency_seconds)
                    .is_none()
            })
            .collect();
        if !unknown.is_empty() {
            let selected = unknown[state.sequence as usize % unknown.len()];
            state.sequence = state.sequence.wrapping_add(1);
            return Some(&replicas[selected]);
        }

        let total_weight: f64 = eligible
            .iter()
            .map(|index| {
                let latency = state.replicas[&replicas[*index]]
                    .ewma_latency_seconds
                    .expect("eligible latency was checked above");
                1.0 / latency.max(1e-9)
            })
            .sum();
        let sample = splitmix64(state.sequence) as f64 / u64::MAX as f64;
        state.sequence = state.sequence.wrapping_add(1);
        let mut ticket = sample * total_weight;

        for index in eligible.iter().copied() {
            let latency = state.replicas[&replicas[index]]
                .ewma_latency_seconds
                .expect("eligible latency was checked above");
            let weight = 1.0 / latency.max(1e-9);
            if ticket < weight {
                return Some(&replicas[index]);
            }
            ticket -= weight;
        }

        eligible.last().map(|index| &replicas[*index])
    }

    fn record_outcome_at(
        &self,
        replica: &NodeAddr,
        latency: Duration,
        outcome: ReplicaRoutingOutcome,
        now: Instant,
    ) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let replica_state = state.replicas.entry(replica.clone()).or_default();
        match outcome {
            ReplicaRoutingOutcome::Success => {
                let sample = latency.as_secs_f64();
                replica_state.ewma_latency_seconds = Some(
                    replica_state
                        .ewma_latency_seconds
                        .map_or(sample, |current| {
                            self.ewma_alpha * sample + (1.0 - self.ewma_alpha) * current
                        }),
                );
                replica_state.consecutive_failures = 0;
                replica_state.ejected_until = None;
            }
            ReplicaRoutingOutcome::Failure => {
                replica_state.consecutive_failures =
                    replica_state.consecutive_failures.saturating_add(1);
                if replica_state.consecutive_failures >= self.failure_threshold {
                    replica_state.ejected_until = now.checked_add(self.ejection_duration);
                }
            }
        }
    }
}

impl ReadRoutingStrategy for AdaptiveReplicaRouting {
    fn select_replica<'a>(&self, _slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr> {
        self.select_replica_at(replicas, Instant::now())
    }

    fn record_outcome(
        &self,
        replica: &NodeAddr,
        latency: Duration,
        outcome: ReplicaRoutingOutcome,
    ) {
        self.record_outcome_at(replica, latency, outcome, Instant::now());
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Round-robin across replicas (default).
///
/// Distributes reads evenly across all available replicas for a slot
/// by cycling through them in order.
pub struct RoundRobinRouting {
    counter: AtomicUsize,
}

impl RoundRobinRouting {
    /// Create a new round-robin routing strategy.
    pub fn new() -> Self {
        Self {
            counter: AtomicUsize::new(0),
        }
    }
}

impl Default for RoundRobinRouting {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadRoutingStrategy for RoundRobinRouting {
    fn select_replica<'a>(&self, _slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr> {
        if replicas.is_empty() {
            return None;
        }
        let idx = self.counter.fetch_add(1, Ordering::Relaxed) % replicas.len();
        Some(&replicas[idx])
    }
}

/// Pseudo-random replica selection.
///
/// Uses an atomic counter with a time-based seed to approximate random
/// distribution without requiring an external RNG dependency.
pub struct RandomRouting {
    counter: AtomicUsize,
}

impl RandomRouting {
    /// Create a new random routing strategy.
    pub fn new() -> Self {
        // Seed from the current time for a pseudo-random starting point.
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as usize)
            .unwrap_or(0);
        Self {
            counter: AtomicUsize::new(seed),
        }
    }
}

impl Default for RandomRouting {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadRoutingStrategy for RandomRouting {
    fn select_replica<'a>(&self, _slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr> {
        if replicas.is_empty() {
            return None;
        }
        // Mix the counter value to spread selections across replicas.
        let val = self.counter.fetch_add(7919, Ordering::Relaxed);
        let idx = val % replicas.len();
        Some(&replicas[idx])
    }
}

/// Always pick the first replica.
///
/// Useful for testing or when replicas are ordered by preference
/// (e.g., closest datacenter first).
pub struct FirstReplicaRouting;

impl ReadRoutingStrategy for FirstReplicaRouting {
    fn select_replica<'a>(&self, _slot: u16, replicas: &'a [NodeAddr]) -> Option<&'a NodeAddr> {
        replicas.first()
    }
}

/// Returns true if the command is read-only, and so safe to route to a
/// replica under [`ReadPreference::Replica`] or [`ReadPreference::PreferReplica`].
///
/// Routing happens on the serialized frame -- callers batch frames, not
/// typed commands, ahead of this check -- so this matches the command name
/// rather than a `Command` trait flag. The name is uppercased into a stack
/// buffer to avoid a heap allocation on every replica-routed command.
///
/// Coverage follows the Redis command `readonly` flag across the core types and
/// the common Redis Stack reads. Commands that can mutate -- even conditionally,
/// like `GETEX` (may change a TTL), `GEORADIUS`/`SORT` (have a `STORE` option),
/// or `XREADGROUP` (advances a consumer group) -- are treated as writes and
/// routed to the master; their dedicated `_RO` variants are read-only.
pub fn is_readonly_command(frame: &Frame) -> bool {
    let items = match frame {
        Frame::Array(Some(items)) if !items.is_empty() => items,
        _ => return false,
    };

    let cmd_name = match &items[0] {
        Frame::BulkString(Some(b)) => b.as_ref(),
        _ => return false,
    };

    // Uppercase into a stack buffer. No read-only command name is longer than
    // this, so anything that overflows it cannot be read-only.
    let mut buf = [0u8; 24];
    if cmd_name.len() > buf.len() {
        return false;
    }
    for (i, b) in cmd_name.iter().enumerate() {
        buf[i] = b.to_ascii_uppercase();
    }

    matches!(
        &buf[..cmd_name.len()],
        // strings / bitmaps
        b"GET" | b"GETRANGE" | b"SUBSTR" | b"MGET" | b"STRLEN" | b"LCS" | b"DIGEST"
        | b"GETBIT" | b"BITCOUNT" | b"BITPOS" | b"BITFIELD_RO"
        // generic keyspace
        | b"EXISTS" | b"TYPE" | b"TTL" | b"PTTL" | b"EXPIRETIME" | b"PEXPIRETIME"
        | b"DUMP" | b"OBJECT" | b"MEMORY" | b"SORT_RO"
        // hashes
        | b"HGET" | b"HGETALL" | b"HKEYS" | b"HVALS" | b"HLEN" | b"HEXISTS"
        | b"HMGET" | b"HSTRLEN" | b"HRANDFIELD" | b"HSCAN"
        // lists
        | b"LRANGE" | b"LLEN" | b"LINDEX" | b"LPOS"
        // sets
        | b"SMEMBERS" | b"SISMEMBER" | b"SMISMEMBER" | b"SCARD" | b"SINTER"
        | b"SINTERCARD" | b"SUNION" | b"SDIFF" | b"SRANDMEMBER" | b"SSCAN"
        // sorted sets
        | b"ZRANGE" | b"ZRANGEBYSCORE" | b"ZRANGEBYLEX" | b"ZREVRANGE"
        | b"ZREVRANGEBYSCORE" | b"ZREVRANGEBYLEX" | b"ZSCORE" | b"ZMSCORE"
        | b"ZCARD" | b"ZRANK" | b"ZREVRANK" | b"ZCOUNT" | b"ZLEXCOUNT"
        | b"ZRANDMEMBER" | b"ZSCAN" | b"ZDIFF" | b"ZINTER" | b"ZUNION"
        | b"ZINTERCARD"
        // streams (XREADGROUP mutates a consumer group -- excluded)
        | b"XLEN" | b"XRANGE" | b"XREVRANGE" | b"XREAD" | b"XINFO" | b"XPENDING"
        // arrays (Redis 8.8; mutation commands are excluded)
        | b"ARCOUNT" | b"ARGET" | b"ARGETRANGE" | b"ARGREP" | b"ARINFO"
        | b"ARLASTITEMS" | b"ARLEN" | b"ARMGET" | b"ARNEXT" | b"AROP" | b"ARSCAN"
        // geo (read-only; STORE-capable GEORADIUS routes to master)
        | b"GEOPOS" | b"GEODIST" | b"GEOHASH" | b"GEOSEARCH"
        | b"GEORADIUS_RO" | b"GEORADIUSBYMEMBER_RO"
        // hyperloglog (PFADD/PFMERGE mutate -- excluded)
        | b"PFCOUNT"
        // scripting (read-only variants only)
        | b"EVAL_RO" | b"EVALSHA_RO" | b"FCALL_RO"
        // server
        | b"DBSIZE" | b"PING" | b"ECHO" | b"INFO"
        // Redis Stack: JSON
        | b"JSON.GET" | b"JSON.MGET" | b"JSON.TYPE" | b"JSON.STRLEN"
        | b"JSON.ARRLEN" | b"JSON.ARRINDEX" | b"JSON.OBJLEN" | b"JSON.OBJKEYS"
        | b"JSON.RESP"
        // Redis Stack: Search
        | b"FT.SEARCH" | b"FT.AGGREGATE" | b"FT.INFO" | b"FT.PROFILE"
        | b"FT.EXPLAIN" | b"FT.EXPLAINCLI" | b"FT.HYBRID" | b"FT.TAGVALS"
        // Redis Stack: TimeSeries
        | b"TS.GET" | b"TS.MGET" | b"TS.RANGE" | b"TS.REVRANGE" | b"TS.MRANGE"
        | b"TS.MREVRANGE" | b"TS.INFO" | b"TS.QUERYINDEX"
        // Redis Stack: probabilistic
        | b"BF.EXISTS" | b"BF.MEXISTS" | b"BF.INFO" | b"BF.CARD"
        | b"CF.EXISTS" | b"CF.COUNT" | b"CF.INFO"
        | b"CMS.QUERY" | b"CMS.INFO"
        | b"TOPK.QUERY" | b"TOPK.COUNT" | b"TOPK.LIST" | b"TOPK.INFO"
        | b"TDIGEST.MIN" | b"TDIGEST.MAX" | b"TDIGEST.QUANTILE"
        | b"TDIGEST.CDF" | b"TDIGEST.RANK" | b"TDIGEST.INFO"
        // Redis Stack: vector sets
        | b"VSIM" | b"VCARD" | b"VDIM" | b"VEMB" | b"VGETATTR" | b"VLINKS"
        | b"VINFO" | b"VISMEMBER" | b"VRANGE"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_addr_display() {
        let addr = NodeAddr {
            host: "127.0.0.1".to_string(),
            port: 7000,
        };
        assert_eq!(addr.to_string(), "127.0.0.1:7000");
        assert_eq!(addr.addr_string(), "127.0.0.1:7000");
    }

    #[test]
    fn node_addr_equality() {
        let a = NodeAddr {
            host: "127.0.0.1".to_string(),
            port: 7000,
        };
        let b = NodeAddr {
            host: "127.0.0.1".to_string(),
            port: 7000,
        };
        let c = NodeAddr {
            host: "127.0.0.1".to_string(),
            port: 7001,
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn node_addr_parse_roundtrips() {
        let addr = NodeAddr::parse("127.0.0.1:6380").unwrap();
        assert_eq!(addr.host, "127.0.0.1");
        assert_eq!(addr.port, 6380);

        let ipv6 = NodeAddr::parse("[::1]:6380").unwrap();
        assert_eq!(ipv6.addr_string(), "[::1]:6380");
    }

    #[test]
    fn node_addr_parse_rejects_missing_port() {
        assert!(NodeAddr::parse("127.0.0.1").is_none());
        assert!(NodeAddr::parse(":6379").is_none());
    }

    #[test]
    fn node_addr_parse_rejects_non_numeric_port() {
        assert!(NodeAddr::parse("127.0.0.1:notaport").is_none());
    }

    #[test]
    fn read_preference_default_is_master() {
        assert_eq!(ReadPreference::default(), ReadPreference::Master);
    }

    #[test]
    fn read_preference_variants() {
        assert_ne!(ReadPreference::Master, ReadPreference::Replica);
        assert_ne!(ReadPreference::Replica, ReadPreference::PreferReplica);
        assert_ne!(ReadPreference::Master, ReadPreference::PreferReplica);
    }

    // -- ReadRoutingStrategy tests --

    fn make_replicas() -> Vec<NodeAddr> {
        vec![
            NodeAddr {
                host: "10.0.0.1".to_string(),
                port: 7001,
            },
            NodeAddr {
                host: "10.0.0.2".to_string(),
                port: 7002,
            },
            NodeAddr {
                host: "10.0.0.3".to_string(),
                port: 7003,
            },
        ]
    }

    #[test]
    fn round_robin_distributes_across_replicas() {
        let strategy = RoundRobinRouting::new();
        let replicas = make_replicas();

        let first = strategy.select_replica(0, &replicas).unwrap();
        let second = strategy.select_replica(0, &replicas).unwrap();
        let third = strategy.select_replica(0, &replicas).unwrap();
        let fourth = strategy.select_replica(0, &replicas).unwrap();

        assert_eq!(first.port, 7001);
        assert_eq!(second.port, 7002);
        assert_eq!(third.port, 7003);
        // Wraps around.
        assert_eq!(fourth.port, 7001);
    }

    #[test]
    fn round_robin_returns_none_for_empty_replicas() {
        let strategy = RoundRobinRouting::new();
        assert!(strategy.select_replica(0, &[]).is_none());
    }

    #[test]
    fn random_routing_returns_valid_replica() {
        let strategy = RandomRouting::new();
        let replicas = make_replicas();

        // Call many times and verify all results are valid replicas.
        for _ in 0..100 {
            let selected = strategy.select_replica(0, &replicas).unwrap();
            assert!(
                replicas.contains(selected),
                "selected replica not in list: {selected:?}"
            );
        }
    }

    #[test]
    fn random_routing_returns_none_for_empty_replicas() {
        let strategy = RandomRouting::new();
        assert!(strategy.select_replica(0, &[]).is_none());
    }

    #[test]
    fn adaptive_builder_rejects_unsafe_configuration() {
        assert_eq!(
            AdaptiveReplicaRouting::builder()
                .ewma_alpha(0.0)
                .build()
                .unwrap_err(),
            AdaptiveReplicaRoutingConfigError::InvalidEwmaAlpha
        );
        assert_eq!(
            AdaptiveReplicaRouting::builder()
                .failure_threshold(0)
                .build()
                .unwrap_err(),
            AdaptiveReplicaRoutingConfigError::ZeroFailureThreshold
        );
        assert_eq!(
            AdaptiveReplicaRouting::builder()
                .ejection_duration(Duration::ZERO)
                .build()
                .unwrap_err(),
            AdaptiveReplicaRoutingConfigError::ZeroEjectionDuration
        );
        assert_eq!(
            AdaptiveReplicaRouting::builder()
                .local_zone("")
                .build()
                .unwrap_err(),
            AdaptiveReplicaRoutingConfigError::EmptyAvailabilityZone
        );
    }

    #[test]
    fn adaptive_routing_prefers_local_zone_and_falls_back_when_it_is_ejected() {
        let replicas = make_replicas();
        let local = replicas[0].clone();
        let remote = replicas[1].clone();
        let strategy = AdaptiveReplicaRouting::builder()
            .local_zone("az-a")
            .replica_zone(local.clone(), "az-a")
            .replica_zone(remote.clone(), "az-b")
            .failure_threshold(1)
            .ejection_duration(Duration::from_secs(30))
            .build()
            .unwrap();
        let now = Instant::now();

        assert_eq!(
            strategy.select_replica_at(&replicas[..2], now),
            Some(&local)
        );
        strategy.record_outcome_at(
            &local,
            Duration::from_millis(5),
            ReplicaRoutingOutcome::Failure,
            now,
        );

        assert_eq!(
            strategy.select_replica_at(&replicas[..2], now + Duration::from_secs(1)),
            Some(&remote)
        );
    }

    #[test]
    fn adaptive_routing_biases_selection_by_inverse_ewma_latency() {
        let replicas = make_replicas();
        let fast = replicas[0].clone();
        let slow = replicas[1].clone();
        let strategy = AdaptiveReplicaRouting::builder()
            .ewma_alpha(1.0)
            .build()
            .unwrap();
        let now = Instant::now();
        strategy.record_outcome_at(
            &fast,
            Duration::from_millis(1),
            ReplicaRoutingOutcome::Success,
            now,
        );
        strategy.record_outcome_at(
            &slow,
            Duration::from_millis(100),
            ReplicaRoutingOutcome::Success,
            now,
        );

        let mut fast_count = 0;
        let mut slow_count = 0;
        for _ in 0..4096 {
            match strategy.select_replica_at(&replicas[..2], now) {
                Some(selected) if selected == &fast => fast_count += 1,
                Some(selected) if selected == &slow => slow_count += 1,
                selected => panic!("unexpected selection: {selected:?}"),
            }
        }

        assert!(
            fast_count > slow_count * 20,
            "fast={fast_count}, slow={slow_count}"
        );
        assert!(
            slow_count > 0,
            "weighted routing permanently starved the slower replica"
        );
    }

    #[test]
    fn adaptive_routing_requires_consecutive_failures_and_resets_on_success() {
        let replica = make_replicas().remove(0);
        let strategy = AdaptiveReplicaRouting::builder()
            .failure_threshold(2)
            .build()
            .unwrap();
        let now = Instant::now();

        strategy.record_outcome_at(
            &replica,
            Duration::from_millis(1),
            ReplicaRoutingOutcome::Failure,
            now,
        );
        strategy.record_outcome_at(
            &replica,
            Duration::from_millis(1),
            ReplicaRoutingOutcome::Success,
            now,
        );
        strategy.record_outcome_at(
            &replica,
            Duration::from_millis(1),
            ReplicaRoutingOutcome::Failure,
            now,
        );

        let state = strategy.state.lock().unwrap();
        let replica_state = &state.replicas[&replica];
        assert_eq!(replica_state.consecutive_failures, 1);
        assert!(replica_state.ejected_until.is_none());
    }

    #[test]
    fn adaptive_routing_recovers_after_the_ejection_window() {
        let replicas = make_replicas();
        let local = replicas[0].clone();
        let remote = replicas[1].clone();
        let ejection = Duration::from_secs(10);
        let strategy = AdaptiveReplicaRouting::builder()
            .local_zone("az-a")
            .replica_zone(local.clone(), "az-a")
            .replica_zone(remote, "az-b")
            .failure_threshold(1)
            .ejection_duration(ejection)
            .build()
            .unwrap();
        let now = Instant::now();
        strategy.record_outcome_at(
            &local,
            Duration::from_millis(5),
            ReplicaRoutingOutcome::Failure,
            now,
        );

        assert_ne!(
            strategy.select_replica_at(&replicas[..2], now + ejection - Duration::from_nanos(1)),
            Some(&local)
        );
        assert_eq!(
            strategy.select_replica_at(&replicas[..2], now + ejection),
            Some(&local)
        );
    }

    #[test]
    fn adaptive_routing_floor_never_ejects_every_candidate() {
        let replicas = make_replicas();
        let strategy = AdaptiveReplicaRouting::builder()
            .failure_threshold(1)
            .minimum_healthy_replicas(1)
            .build()
            .unwrap();
        let now = Instant::now();
        for replica in &replicas {
            strategy.record_outcome_at(
                replica,
                Duration::from_millis(5),
                ReplicaRoutingOutcome::Failure,
                now,
            );
        }

        assert!(strategy.select_replica_at(&replicas, now).is_some());
    }

    #[test]
    fn adaptive_routing_zero_floor_can_eject_every_candidate() {
        let replicas = make_replicas();
        let strategy = AdaptiveReplicaRouting::builder()
            .failure_threshold(1)
            .minimum_healthy_replicas(0)
            .build()
            .unwrap();
        let now = Instant::now();
        for replica in &replicas {
            strategy.record_outcome_at(
                replica,
                Duration::from_millis(5),
                ReplicaRoutingOutcome::Failure,
                now,
            );
        }

        assert!(strategy.select_replica_at(&replicas, now).is_none());
    }

    #[test]
    fn first_replica_always_returns_first() {
        let strategy = FirstReplicaRouting;
        let replicas = make_replicas();

        for _ in 0..10 {
            let selected = strategy.select_replica(0, &replicas).unwrap();
            assert_eq!(selected.port, 7001);
            assert_eq!(selected.host, "10.0.0.1");
        }
    }

    #[test]
    fn first_replica_returns_none_for_empty_replicas() {
        let strategy = FirstReplicaRouting;
        assert!(strategy.select_replica(0, &[]).is_none());
    }

    #[test]
    fn is_readonly_command_accepts_reads_and_rejects_writes() {
        use redis_tower_protocol::helpers::{array, bulk};

        assert!(is_readonly_command(&array(vec![bulk("GET"), bulk("key")])));
        assert!(is_readonly_command(&array(vec![
            bulk("HGETALL"),
            bulk("h")
        ])));
        assert!(!is_readonly_command(&array(vec![
            bulk("SET"),
            bulk("key"),
            bulk("val")
        ])));
        assert!(!is_readonly_command(&array(vec![bulk("GETEX"), bulk("k")])));
        assert!(!is_readonly_command(&Frame::Array(None)));
    }

    #[test]
    fn is_readonly_command_is_case_insensitive() {
        use redis_tower_protocol::helpers::{array, bulk};
        assert!(is_readonly_command(&array(vec![bulk("get"), bulk("k")])));
    }
}
