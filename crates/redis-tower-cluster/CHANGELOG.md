# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- provider-backed setup now force-refreshes once after an authentication
  rejection, and direct/shared multiplexed Cluster clients can apply streamed
  credentials to every established node connection (#475)
- dedicated reconnecting Cluster Pub/Sub connections: regular subscriptions
  pinned to an explicit node, and same-slot sharded subscriptions that follow
  committed ownership changes; `SPUBLISH` remains on the normal slot-routed
  command path (#441)
- node-grouped `ClusterPipeline` execution with per-command redirect handling,
  explicit cross-slot `MGET`/`MSET`/`DEL` helpers, and slot-pinned
  WATCH/MULTI/EXEC support on `ClusterConnection` and `ClusterClient`; keyed
  transactions fail closed on unknown ownership, never replay redirects, and
  reject closure-based transaction helpers before WATCH
- reusable 3-master/3-replica live-cluster fixtures, deterministic ASK/MOVED
  reshard coverage, explicit replica-promotion validation, and cluster churn
  benchmark reporting
- end-to-end typed command deadlines across routing, node lookup, redirects,
  retries, and pinned-node execution for direct and multiplexed clients
- multiplexed cluster redirect and topology-refresh tracing, plus opt-in bounded per-node latency labels
- configurable RESP decode limits across cluster discovery, node connections, topology refreshes, and reconnects
- full `ConnectionConfig` and explicit RESP2/RESP3 selection on the ordinary
  cluster builders, retained across discovery, redirects, topology refreshes,
  and reconnects; the cached builder forces RESP3
- revisioned master/slot topology changes and slot-scoped cache epochs for
  topology-aware client-side caching
- cloneable `CachedMultiplexedClusterClient` with one shared slot-aware cache,
  Broadcast/ServerDefault/OptIn tracking modes, atomic OptIn and ASK dispatch
  (ASK safely bypasses cache fill), and
  one RESP3 invalidation receiver per current master;
  receiver/data loss and topology coverage reconfiguration fail closed with a
  full clear, while slot epochs immediately reject stale in-flight fills after
  ownership changes. Replica read preferences are rejected until equivalent
  invalidation coverage exists

### Fixed

- require a finite client-side cache TTL for Cluster before connecting, so an
  unobserved empty-slot ownership change cannot leave an old-owner cached miss
  stale indefinitely; standalone cached clients still permit disabling TTL
- authenticate protected cluster nodes before final RESP negotiation so Auto
  and forced RESP3 do not silently remain on RESP2 after a pre-auth `NOAUTH`
- count `max_redirects` as actual redirect/transient follow-ups after the
  initial attempt, including chained ASK/MOVED responses, without target setup
  or backoff after the budget is exhausted
- quarantine a direct node connection after canceled or incomplete command I/O,
  and send `ASKING` with its redirected command as one atomic exchange
- route both MIGRATE forms by their source key and keep keyless server/operations families on the default node
- route Redis 8.8 Array reads to replicas while keeping all mutating `AR*` commands on masters
- route MSETEX by its numkeys-prefixed key list, classify HOTKEYS as keyless, and allow DIGEST replica reads
- route FT.HYBRID, FT.EXPLAIN, FT.EXPLAINCLI, FT.PROFILE, FT.TAGVALS, VISMEMBER, and VRANGE through read-only cluster paths

## [0.1.0](https://github.com/joshrotenberg/redis-tower/releases/tag/redis-tower-cluster-v0.1.0) - 2026-06-05

### Added

- RedisExecutor impls for ClusterConnection and SentinelConnection, ConnectionPool tests ([#259](https://github.com/joshrotenberg/redis-tower/pull/259)) ([#286](https://github.com/joshrotenberg/redis-tower/pull/286))
- TLS for ClusterConnection + automated TLS cluster harness ([#244](https://github.com/joshrotenberg/redis-tower/pull/244))
- TLS support for MultiplexedClusterClient ([#236](https://github.com/joshrotenberg/redis-tower/pull/236))
- MultiplexedClusterClient for redis-tower-cluster ([#235](https://github.com/joshrotenberg/redis-tower/pull/235))
- CLIENT SETINFO, credential rotation, cluster routing strategies ([#233](https://github.com/joshrotenberg/redis-tower/pull/233))
- final audit round -- cluster NAT, TLS flex, pool health, 87 new tests, CI ([#226](https://github.com/joshrotenberg/redis-tower/pull/226))
- implement Service<Cmd> for ClusterConnection and SentinelConnection
- shared command test macro for standalone/cluster matrix
- add redis-test-harness crate and run cluster integration tests
- add read preference, ClusterClient, and readonly routing
- add MOVED/ASK redirect handling to cluster connection
- add redis-tower-cluster crate with slot routing and topology
- [**breaking**] v2 rewrite -- workspace scaffold with core features
- add comprehensive TLS support for Redis connections

### Fixed

- structured reconnect/redirect/failover logs, non-idempotent retry guard, middleware unit tests (closes #303, #306, #340) ([#369](https://github.com/joshrotenberg/redis-tower/pull/369))
- cluster MOVED/ASK topology refresh, topology_mut, CROSSSLOT and redirect tests (closes #317, #327, #333) ([#367](https://github.com/joshrotenberg/redis-tower/pull/367))
- [**breaking**] honor Tower backpressure contract in poll_ready
- rewrite test harness to sync process management

### Other

- thread-safety docs, missing examples, #[deny(missing_docs)] (closes #337, #341, #343) ([#370](https://github.com/joshrotenberg/redis-tower/pull/370))
- replace test harness server lifecycle with redis-server-wrapper ([#248](https://github.com/joshrotenberg/redis-tower/pull/248))
- bump MSRV to 1.88, apply let-chain suggestions ([#247](https://github.com/joshrotenberg/redis-tower/pull/247))
- surface MultiplexedClusterClient, add benchmarks, fix flaky tests ([#243](https://github.com/joshrotenberg/redis-tower/pull/243))
- clean up README, move examples, add licenses, remove stale files ([#204](https://github.com/joshrotenberg/redis-tower/pull/204))
- add tower-resilience integration guide and example
- comprehensive documentation rewrite
- add crate metadata, module docs, and 10 runnable examples
- format
- add README with full API overview
- comprehensive cluster unit test coverage
- polish phase for v0.1.0 release
- initialize redis-tower experimental project skeleton
