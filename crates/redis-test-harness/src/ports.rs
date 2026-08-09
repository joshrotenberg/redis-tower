//! Canonical fixed-port registry for this workspace's live-server test and
//! benchmark fixtures.
//!
//! Every fixture that binds a *fixed* (non-ephemeral, non-OS-assigned) port
//! defines it here and imports the constant, rather than hardcoding the
//! number again at the call site. [`FIXTURE_PORT_BLOCKS`] is the full list of
//! blocks those constants cover, and `fixture_port_blocks_do_not_overlap`
//! below is the one place that checks them pairwise.
//!
//! This is a single source of truth, not a second hand-maintained copy: a
//! test file that reuses one of these constants gets the real port, so
//! moving a fixture (or landing a new one) here is what makes a collision
//! with another fixture fail the overlap test, instead of two independent
//! literals silently drifting apart (#655).
//!
//! Dynamic/OS-assigned ports (`.port(0)` + `local_addr()`, `TcpListener`
//! probes) are excluded: they cannot collide by construction, so they add
//! nothing to a static registry.

use crate::cluster::{CLUSTER_BUS_PORT_OFFSET, NODE_COUNT};

/// A single contiguous, inclusive port range owned by one fixture.
#[derive(Debug, Clone, Copy)]
pub struct PortBlock {
    /// Fixture and file that owns this range, for assertion messages.
    pub name: &'static str,
    pub start: u16,
    pub end: u16,
}

#[cfg(test)]
impl PortBlock {
    const fn overlaps(&self, other: &PortBlock) -> bool {
        self.start <= other.end && other.start <= self.end
    }
}

// -- redis-tower standalone tests (crates/redis-tower/tests/) --

/// `common/mod.rs`: shared no-auth server used by most standalone tests.
pub const STANDALONE_COMMON_PORT: u16 = 6399;
/// `test_infrastructure.rs`: dedicated TLS-only server.
pub const STANDALONE_TLS_PORT: u16 = 6387;
/// `test_object.rs`: dedicated `allkeys-lfu` server for `OBJECT FREQ`.
pub const STANDALONE_LFU_PORT: u16 = 6388;
/// `test_auth.rs`: dedicated `requirepass`-protected server.
pub const STANDALONE_AUTH_PORT: u16 = 6398;
/// `integration.rs`: `multiplexed_client_reconnects_after_server_restart`
/// stops and restarts a server on this port within one test.
pub const STANDALONE_RECONNECT_PORT: u16 = 6401;

// -- redis-tower-client (crates/redis-tower-client/tests/) --

/// `standalone.rs`: throwaway server for the `UniversalClient` standalone path.
pub const CLIENT_STANDALONE_PORT: u16 = 6402;

// -- redis-tower-sync (crates/redis-tower-sync/tests/) --

/// `sync_integration.rs`: one dedicated server per test, ports
/// `SYNC_PORT_BASE..SYNC_PORT_BASE + SYNC_PORT_COUNT`.
pub const SYNC_PORT_BASE: u16 = 6403;
pub const SYNC_PORT_COUNT: u16 = 6;

// -- redis-tower-sentinel (crates/redis-tower-sentinel/tests/) --

/// `sentinel_integration.rs` (healthy suite, shared via `OnceCell` for the
/// whole binary): master, then `SENTINEL_HEALTHY_REPLICAS` replicas, then
/// `SENTINEL_HEALTHY_SENTINELS` sentinels.
pub const SENTINEL_HEALTHY_MASTER_PORT: u16 = 6390;
pub const SENTINEL_HEALTHY_REPLICAS: u16 = 2;
pub const SENTINEL_HEALTHY_SENTINEL_BASE_PORT: u16 = 26389;
pub const SENTINEL_HEALTHY_SENTINELS: u16 = 3;

/// `sentinel_failover.rs` (destructive suite, its own port block so it never
/// shares a topology with the healthy suite).
pub const SENTINEL_FAILOVER_MASTER_PORT: u16 = 6393;
pub const SENTINEL_FAILOVER_REPLICAS: u16 = 2;
pub const SENTINEL_FAILOVER_SENTINEL_BASE_PORT: u16 = 26392;
pub const SENTINEL_FAILOVER_SENTINELS: u16 = 3;

// -- redis-tower-cluster (crates/redis-tower-cluster/tests/cluster_integration.rs) --
//
// Each base below starts either a bare 3-master/0-replica `RedisCluster`, or a
// full `ClusterFixture` (`NODE_COUNT` = 3 masters + 3 replicas). Blocks are
// registered at the conservative `NODE_COUNT` width regardless, since bases
// are spaced 100 apart -- comfortably wider than either shape -- and a fixture
// that later grows replicas should not have to touch this file.

/// `ensure_cluster()`: primary read-path suite.
pub const CLUSTER_PLAIN_BASE_PORT: u16 = 17_200;
/// `mux_cluster_credentials_authenticate_on_connect`.
pub const CLUSTER_AUTH_MUX_BASE_PORT: u16 = 17_300;
/// `cluster_tls_fixture()`: TLS-enabled cluster.
pub const CLUSTER_TLS_BASE_PORT: u16 = 17_400;
/// `mux_cluster_replaces_killed_master_after_replica_promotion`.
pub const CLUSTER_FAILOVER_BASE_PORT: u16 = 17_500;
/// `cluster_connection_credentials_and_connect_url`.
pub const CLUSTER_AUTH_CONN_BASE_PORT: u16 = 17_600;
/// `mux_cluster_handles_ask_then_moved_during_live_reshard`.
pub const CLUSTER_RESHARD_BASE_PORT: u16 = 17_700;

// -- cluster-bench (crates/cluster-bench/src/main.rs) --
//
// Manual (not CI-automated) throughput and churn benchmarks against their own
// throwaway clusters. `CLUSTER_BENCH_CHURN_BASE_PORT` used to default to
// 17_500, colliding with `CLUSTER_FAILOVER_BASE_PORT` above; moved to keep
// the 100-wide spacing the rest of this block uses (#655).

/// `BENCH_BASE_PORT` env var default: throughput scenario's 3-master cluster.
pub const CLUSTER_BENCH_BASE_PORT: u16 = 17_000;
/// `BENCH_CHURN_BASE_PORT` env var default: reshard/failover churn fixture.
pub const CLUSTER_BENCH_CHURN_BASE_PORT: u16 = 17_800;

const CLUSTER_BASE_PORTS: [(&str, u16); 8] = [
    ("cluster: plain read-path suite", CLUSTER_PLAIN_BASE_PORT),
    ("cluster: mux auth suite", CLUSTER_AUTH_MUX_BASE_PORT),
    ("cluster: TLS suite", CLUSTER_TLS_BASE_PORT),
    ("cluster: failover suite", CLUSTER_FAILOVER_BASE_PORT),
    (
        "cluster: connection auth suite",
        CLUSTER_AUTH_CONN_BASE_PORT,
    ),
    ("cluster: reshard suite", CLUSTER_RESHARD_BASE_PORT),
    (
        "cluster-bench (manual; BENCH_BASE_PORT default)",
        CLUSTER_BENCH_BASE_PORT,
    ),
    (
        "cluster-bench (manual; BENCH_CHURN_BASE_PORT default)",
        CLUSTER_BENCH_CHURN_BASE_PORT,
    ),
];

// -- Benchmarks --

/// `standalone-bench`: `BENCH_PORT` env var default.
pub const STANDALONE_BENCH_DEFAULT_PORT: u16 = 6480;
/// `redis-tower/benches/commands.rs`: criterion bench server.
pub const CRITERION_BENCH_PORT: u16 = 6482;
/// `sentinel-bench`: `BENCH_MASTER_PORT` / `BENCH_REPLICA_BASE` env var defaults.
pub const SENTINEL_BENCH_DEFAULT_MASTER_PORT: u16 = 6490;
pub const SENTINEL_BENCH_DEFAULT_REPLICAS: u16 = 2;
/// `sentinel-bench`: `BENCH_SENTINEL_BASE` env var default.
pub const SENTINEL_BENCH_DEFAULT_SENTINEL_BASE_PORT: u16 = 26490;
pub const SENTINEL_BENCH_DEFAULT_SENTINELS: u16 = 3;

/// Every fixed-port block any fixture in this workspace starts. Consulted by
/// `fixture_port_blocks_do_not_overlap` -- add a fixture's ports here (and
/// have the fixture import the constant) rather than hand-copying the number.
pub fn fixture_port_blocks() -> Vec<PortBlock> {
    let mut blocks = vec![
        PortBlock {
            name: "standalone: common/mod.rs",
            start: STANDALONE_COMMON_PORT,
            end: STANDALONE_COMMON_PORT,
        },
        PortBlock {
            name: "standalone: test_infrastructure.rs TLS",
            start: STANDALONE_TLS_PORT,
            end: STANDALONE_TLS_PORT,
        },
        PortBlock {
            name: "standalone: test_object.rs LFU",
            start: STANDALONE_LFU_PORT,
            end: STANDALONE_LFU_PORT,
        },
        PortBlock {
            name: "standalone: test_auth.rs",
            start: STANDALONE_AUTH_PORT,
            end: STANDALONE_AUTH_PORT,
        },
        PortBlock {
            name: "standalone: integration.rs reconnect",
            start: STANDALONE_RECONNECT_PORT,
            end: STANDALONE_RECONNECT_PORT,
        },
        PortBlock {
            name: "redis-tower-client: standalone.rs",
            start: CLIENT_STANDALONE_PORT,
            end: CLIENT_STANDALONE_PORT,
        },
        PortBlock {
            name: "redis-tower-sync: sync_integration.rs",
            start: SYNC_PORT_BASE,
            end: SYNC_PORT_BASE + SYNC_PORT_COUNT - 1,
        },
        PortBlock {
            name: "sentinel: healthy suite redis processes",
            start: SENTINEL_HEALTHY_MASTER_PORT,
            end: SENTINEL_HEALTHY_MASTER_PORT + SENTINEL_HEALTHY_REPLICAS,
        },
        PortBlock {
            name: "sentinel: healthy suite sentinel processes",
            start: SENTINEL_HEALTHY_SENTINEL_BASE_PORT,
            end: SENTINEL_HEALTHY_SENTINEL_BASE_PORT + SENTINEL_HEALTHY_SENTINELS - 1,
        },
        PortBlock {
            name: "sentinel: failover suite redis processes",
            start: SENTINEL_FAILOVER_MASTER_PORT,
            end: SENTINEL_FAILOVER_MASTER_PORT + SENTINEL_FAILOVER_REPLICAS,
        },
        PortBlock {
            name: "sentinel: failover suite sentinel processes",
            start: SENTINEL_FAILOVER_SENTINEL_BASE_PORT,
            end: SENTINEL_FAILOVER_SENTINEL_BASE_PORT + SENTINEL_FAILOVER_SENTINELS - 1,
        },
        PortBlock {
            name: "standalone-bench (manual; BENCH_PORT default)",
            start: STANDALONE_BENCH_DEFAULT_PORT,
            end: STANDALONE_BENCH_DEFAULT_PORT,
        },
        PortBlock {
            name: "redis-tower criterion bench: commands.rs",
            start: CRITERION_BENCH_PORT,
            end: CRITERION_BENCH_PORT,
        },
        PortBlock {
            name: "sentinel-bench (manual; default redis processes)",
            start: SENTINEL_BENCH_DEFAULT_MASTER_PORT,
            end: SENTINEL_BENCH_DEFAULT_MASTER_PORT + SENTINEL_BENCH_DEFAULT_REPLICAS,
        },
        PortBlock {
            name: "sentinel-bench (manual; default sentinel processes)",
            start: SENTINEL_BENCH_DEFAULT_SENTINEL_BASE_PORT,
            end: SENTINEL_BENCH_DEFAULT_SENTINEL_BASE_PORT + SENTINEL_BENCH_DEFAULT_SENTINELS - 1,
        },
    ];

    for (name, base) in CLUSTER_BASE_PORTS {
        let client_end = base + NODE_COUNT as u16 - 1;
        blocks.push(PortBlock {
            name,
            start: base,
            end: client_end,
        });
        let bus_base = base + CLUSTER_BUS_PORT_OFFSET;
        blocks.push(PortBlock {
            name,
            start: bus_base,
            end: bus_base + NODE_COUNT as u16 - 1,
        });
    }

    blocks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_port_blocks_do_not_overlap() {
        let blocks = fixture_port_blocks();
        for (i, a) in blocks.iter().enumerate() {
            for b in &blocks[i + 1..] {
                assert!(
                    !a.overlaps(b),
                    "port block \"{}\" ({}..={}) overlaps \"{}\" ({}..={})",
                    a.name,
                    a.start,
                    a.end,
                    b.name,
                    b.start,
                    b.end
                );
            }
        }
    }
}
