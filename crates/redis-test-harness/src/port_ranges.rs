//! Registry of the fixed TCP port blocks live-server test fixtures reserve
//! across the workspace.
//!
//! Every fixture that starts a real `redis-server` (or `redis-sentinel`)
//! process on a hardcoded port belongs here, even if it does not import the
//! constants directly, so `fixture_port_ranges_do_not_overlap` can catch a
//! new fixture landing on top of an existing one before it ever produces the
//! kind of cross-suite ordering flake described in #655 (the sync fixture
//! silently reused the Sentinel fixtures' 6390-6395 block, so writes could
//! land on a Sentinel-managed read-only replica depending on run order).
//!
//! [`SYNC_PORT_BASE`](crate::port_ranges::SYNC_PORT_BASE),
//! [`SENTINEL_HEALTHY_MASTER_PORT`](crate::port_ranges::SENTINEL_HEALTHY_MASTER_PORT),
//! and
//! [`SENTINEL_FAILOVER_MASTER_PORT`](crate::port_ranges::SENTINEL_FAILOVER_MASTER_PORT)
//! are the source of truth: the fixtures
//! that collided in #655 import them directly instead of re-declaring the
//! literal, so they cannot drift back apart. The rest of the table documents
//! blocks that stayed hardcoded in their own test/bench file; keep it in sync
//! by hand when adding a new one.

/// A named, half-open `[start, start + len)` block of ports one fixture owns.
#[derive(Debug, Clone, Copy)]
pub struct FixturePortRange {
    /// Stable name of the fixture that owns the range.
    pub name: &'static str,
    /// First port in the reserved range.
    pub start: u16,
    /// Number of consecutive reserved ports.
    pub len: u16,
}

#[cfg(test)]
impl FixturePortRange {
    const fn end(&self) -> u16 {
        self.start + self.len
    }

    fn overlaps(&self, other: &FixturePortRange) -> bool {
        self.start < other.end() && other.start < self.end()
    }
}

/// Base port for the `redis-tower-sync` live fixture (`sync_integration.rs`).
/// Six consecutive ports, one per test (`PORT_BASE..PORT_BASE + 6`).
pub const SYNC_PORT_BASE: u16 = 6410;
/// Number of consecutive ports reserved by the sync fixture.
pub const SYNC_PORT_COUNT: u16 = 6;

/// Healthy Sentinel topology (`sentinel_integration.rs`): one master, two
/// replicas, three sentinels.
pub const SENTINEL_HEALTHY_MASTER_PORT: u16 = 6390;
/// First replica port in the healthy Sentinel fixture.
pub const SENTINEL_HEALTHY_REPLICA_BASE_PORT: u16 = 6391;
/// First Sentinel port in the healthy Sentinel fixture.
pub const SENTINEL_HEALTHY_SENTINEL_BASE_PORT: u16 = 26389;

/// Destructive Sentinel failover topology (`sentinel_failover.rs`): its own
/// master/replica/sentinel block so the destructive phases never degrade the
/// healthy suite above (see #509).
pub const SENTINEL_FAILOVER_MASTER_PORT: u16 = 6393;
/// First replica port in the destructive failover fixture.
pub const SENTINEL_FAILOVER_REPLICA_BASE_PORT: u16 = 6394;
/// First Sentinel port in the destructive failover fixture.
pub const SENTINEL_FAILOVER_SENTINEL_BASE_PORT: u16 = 26392;

/// Every fixed port block a live-server test or bench fixture reserves.
///
/// `len` is intentionally generous where the exact node count is an
/// implementation detail (e.g. cluster fixtures) so this stays a conservative
/// overlap check rather than a precise accounting of every port in use.
pub const FIXTURE_PORT_RANGES: &[FixturePortRange] = &[
    FixturePortRange {
        name: "redis-tower/tests/test_object.rs::cover_object_freq",
        start: 6388,
        len: 1,
    },
    FixturePortRange {
        name: "redis-tower-sentinel/tests/sentinel_integration.rs (master + replicas)",
        start: SENTINEL_HEALTHY_MASTER_PORT,
        len: 3,
    },
    FixturePortRange {
        name: "redis-tower-sentinel/tests/sentinel_integration.rs (sentinels)",
        start: SENTINEL_HEALTHY_SENTINEL_BASE_PORT,
        len: 3,
    },
    FixturePortRange {
        name: "redis-tower-sentinel/tests/sentinel_failover.rs (master + replicas)",
        start: SENTINEL_FAILOVER_MASTER_PORT,
        len: 3,
    },
    FixturePortRange {
        name: "redis-tower-sentinel/tests/sentinel_failover.rs (sentinels)",
        start: SENTINEL_FAILOVER_SENTINEL_BASE_PORT,
        len: 3,
    },
    FixturePortRange {
        name: "redis-tower-sync/tests/sync_integration.rs",
        start: SYNC_PORT_BASE,
        len: SYNC_PORT_COUNT,
    },
    FixturePortRange {
        name: "redis-tower/tests/test_infrastructure.rs (TLS_PORT)",
        start: 6387,
        len: 1,
    },
    FixturePortRange {
        name: "redis-tower/tests/test_auth.rs",
        start: 6398,
        len: 1,
    },
    FixturePortRange {
        name: "redis-tower/tests/{common/mod.rs,integration.rs}::shared standalone server",
        start: 6399,
        len: 1,
    },
    FixturePortRange {
        name: "redis-tower/tests/integration.rs::mux reconnect fixture",
        start: 6401,
        len: 1,
    },
    FixturePortRange {
        name: "redis-tower-client/tests/standalone.rs",
        start: 6402,
        len: 1,
    },
    FixturePortRange {
        name: "standalone-bench (BENCH_PORT default)",
        start: 6480,
        len: 1,
    },
    FixturePortRange {
        name: "redis-tower/benches/commands.rs (BENCH_PORT)",
        start: 6482,
        len: 1,
    },
    FixturePortRange {
        name: "sentinel-bench (BENCH_MASTER_PORT/BENCH_REPLICA_BASE defaults)",
        start: 6490,
        len: 10,
    },
    FixturePortRange {
        name: "sentinel-bench (BENCH_SENTINEL_BASE default)",
        start: 26490,
        len: 3,
    },
    FixturePortRange {
        name: "cluster-bench (BENCH_BASE_PORT default)",
        start: 17000,
        len: 10,
    },
    FixturePortRange {
        name: "cluster-bench (BENCH_CHURN_BASE_PORT default)",
        start: 17800,
        len: 10,
    },
    FixturePortRange {
        name: "redis-tower-cluster/tests/cluster_integration.rs (plain)",
        start: 17200,
        len: 10,
    },
    FixturePortRange {
        name: "redis-tower-cluster/tests/cluster_integration.rs (auth)",
        start: 17300,
        len: 10,
    },
    FixturePortRange {
        name: "redis-tower-cluster/tests/cluster_integration.rs (TLS)",
        start: 17400,
        len: 10,
    },
    FixturePortRange {
        name: "redis-tower-cluster/tests/cluster_integration.rs (killed-master promotion)",
        start: 17500,
        len: 10,
    },
    FixturePortRange {
        name: "redis-tower-cluster/tests/cluster_integration.rs (auth-password topology)",
        start: 17600,
        len: 10,
    },
    FixturePortRange {
        name: "redis-tower-cluster/tests/cluster_integration.rs (live reshard)",
        start: 17700,
        len: 10,
    },
];

/// Fails clearly, naming both fixtures and the exact overlapping ports, if
/// any two entries in [`FIXTURE_PORT_RANGES`] claim the same port. This is
/// the regression guard for #655: it runs as a plain unit test (no
/// `redis-server` needed) so it executes in the fast `cargo test --lib` tier
/// on every PR, not just when the live suites happen to run back to back.
#[test]
fn fixture_port_ranges_do_not_overlap() {
    let mut collisions = Vec::new();
    for (i, a) in FIXTURE_PORT_RANGES.iter().enumerate() {
        for b in &FIXTURE_PORT_RANGES[i + 1..] {
            if a.overlaps(b) {
                let overlap_start = a.start.max(b.start);
                let overlap_end = a.end().min(b.end());
                collisions.push(format!(
                    "{} [{}, {}) overlaps {} [{}, {}) on ports [{}, {})",
                    a.name,
                    a.start,
                    a.end(),
                    b.name,
                    b.start,
                    b.end(),
                    overlap_start,
                    overlap_end,
                ));
            }
        }
    }
    assert!(
        collisions.is_empty(),
        "fixture port ranges collide -- give one fixture a new block:\n{}",
        collisions.join("\n")
    );
}
