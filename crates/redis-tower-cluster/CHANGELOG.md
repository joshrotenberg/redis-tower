# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- connection-setup/client-name builder policies replayed on discovery, masters,
  replicas, redirected/topology-created nodes, dedicated sockets, and replacements
- `MultiplexedClusterClientBuilder::pubsub_reconnect_config`, the retry policy
  for re-establishing dedicated Pub/Sub sessions. It defaults to the node
  reconnect backoff and connect timeout with no retry limit

### Changed

- run node-scoped keyless commands on every node of their scope with combined
  replies in both clients, instead of on the single keyless default master
  where they returned a normal-looking but partial reply (redis-rs #2474):
  `DBSIZE` is summed across masters; `FLUSHALL`, `FLUSHDB`, `SAVE`, `BGSAVE`,
  and `FUNCTION LOAD`/`FLUSH`/`DELETE`/`RESTORE` run on every master; and
  `SCRIPT LOAD`/`FLUSH`/`EXISTS` and `CONFIG SET`/`RESETSTAT`/`REWRITE` also
  run on connected replicas (`SCRIPT EXISTS` is ANDed element-wise). A master
  that answers `READONLY` is replaced by the promoted one after a refresh, and
  a node answering `TRYAGAIN`, `CLUSTERDOWN`, or `LOADING` is retried as
  ordinary routing does, without re-sending to nodes that answered. A
  fan-out is not atomic: the first error is returned after every node was
  attempted. Other keyless commands, including node-local diagnostics, still
  use the default node, and explicit pipelines do not fan out

### Fixed

- resubscribe a sharded Pub/Sub session on the slot's new owner when the old
  owner drops the subscription itself (slot migration, graceful failover). The
  socket stays open in that case, so the session previously went silent
  forever unless other traffic happened to refresh the topology
- refresh the topology between Pub/Sub reconnect attempts and retry under
  `pubsub_reconnect_config` instead of the node workers' ~1.5 s budget: a
  sharded session kept dialing a killed owner from its cached topology and gave
  up long before any failover could finish. A regular session pinned to a node
  survives the node's restart and ends with an error once the node has left the
  topology instead of retrying forever
- make sharded Pub/Sub recovery cancel-safe: a topology change observed by a
  `next_message` call that was then cancelled (for example by a caller's
  timeout) was marked seen and lost, leaving the session on the old owner
- ask nodes the topology lists without a connection (replicas under
  `ReadPreference::Master`) during a topology refresh in both clients; in a
  one-shard cluster whose master died, no other node was left to ask
- check the `ASKING` reply in `ClusterConnection` before trusting the
  migrated command's reply (redis-rs #2305); a rejected `ASKING` was ignored
- return the server's own last reply (`MOVED ...`, `ASK ...`, `TRYAGAIN ...`,
  `CLUSTERDOWN ...`) when the redirect budget is exhausted, in commands,
  pipelines, and sharded Pub/Sub subscribes, instead of replacing it with
  "too many redirects (N)"; under `max_redirects(0)` that was every redirect,
  and callers could not tell a moved slot from a down cluster
- schedule a topology refresh when a pipeline (including `mget_split`,
  `mset_split`, and `del_split`) fails during planning because a slot has no
  known owner, and connect an owner that has no worker before planning; the
  failure happened before any I/O, so nothing healed it
- reject `SELECT` of a database other than 0, and a `/db` URL path other than
  `/0` in `connect_url`, in both clients: Redis Cluster has only database 0.
  The URL path was silently ignored, and a server that accepts other databases
  in cluster mode would have switched one node's shared connection
- connect both clients best-effort: a master that is unreachable at connect
  time (still listed by `CLUSTER SLOTS` mid-failover, or restarting) no
  longer fails `connect()` for the whole cluster. Its slots fail with a
  connection error until it answers and are then connected lazily; only a
  cluster with no reachable master fails to connect. Replicas are likewise
  best-effort. `MultiplexedClusterClient` also connects a slot owner that has
  no worker (skipped by a refresh, or dropped after a failed
  re-authentication) when a command routes to it, instead of bouncing off the
  default node with a `MOVED` that `max_redirects(0)` turned into a failure
- resolve an empty or NULL endpoint in `CLUSTER SLOTS` to the node that was
  asked, as Redis documents: a single-node cluster reports `""` for itself and
  `cluster-preferred-endpoint-type unknown-endpoint` reports NULL, so neither
  client could connect to such a cluster. Host-less redirects
  (`MOVED 3999 :6381`) likewise resolve to the replying node's host, including
  pipelined and Pub/Sub redirects. An endpoint of `?` is rejected as unknown,
  and the public `topology::discover_topology`, which does not know the
  queried address, now rejects empty/NULL endpoints instead of producing
  unusable `:port` addresses
- stop `MultiplexedClusterClient::reauthenticate_all` from holding the routing
  write lock while it sent `AUTH` to every node in turn: one slow node added
  its latency to every command on every node, and a hung one stalled routing
  indefinitely (redis-rs #2092). Nodes are now authenticated concurrently
  from a snapshot, each bounded by 5 s; a worker that rejects the credential
  or times out is dropped from routing only if a concurrent refresh has not
  already replaced it
- treat a `READONLY` reply from a master as stale topology in both clients:
  wait for a topology refresh that started after the reply, then re-send the
  command to the slot's (or keyless default's) current master. A write sent
  to a master demoted with its sockets intact previously failed forever with
  no refresh scheduled (redis-rs #2085/#2115). Redis rejects the command
  before running it, so re-sending is safe for non-idempotent writes; the
  multiplexed client bypasses the background refresh rate limit for this, and
  the server's `READONLY` text is returned once the redirect budget is spent
- reuse a `ClusterConnection` node socket only for the role it was opened
  for: after a failover the demoted master's socket (opened without
  `READONLY`) is replaced for replica reads, which previously bounced with
  `MOVED` forever under `ReadPreference::Replica`
- make `ClusterConnection::refresh_topology` ask each connected node and then
  the original seed until one reports slot owners, skip (and later connect
  lazily) nodes it cannot reach instead of failing, close sockets for departed
  nodes, and move the keyless default node to a current master it is
  connected to
- rebuild dead replica workers in `MultiplexedClusterClient`: a replica read
  whose slot has an advertised replica with no worker, or one that exhausted
  its reconnect budget, now schedules a (rate-limited) topology refresh. Reads
  route around such workers, so nothing scheduled the refresh that rebuilds
  them; strict replica reads failed forever and `PreferReplica` reads stayed
  on the master after a replica restart (redis-rs #2120)
- reconnect `ClusterConnection` replica sockets lazily: a replica whose
  socket was dropped after one I/O error or a deadline was filtered out of
  replica selection forever. It is now reconnected the next time routing
  picks it; a replica whose reconnect fails is skipped for a backoff that
  starts at 250 ms and doubles up to 5 s, and the command is re-routed to
  another replica, the master (`PreferReplica`), or the strict-replica error.
  One command tries each replica at most once, so connects slower than the
  backoff cannot make re-routing alternate between dead replicas
- route `ClusterConnection` keyed commands to the slot owner even when its
  socket was dropped, reconnecting lazily; they were sent to the default node
  and bounced with `MOVED`, which failed outright under `max_redirects(0)`
- move `MultiplexedClusterClient`'s keyless-command default node to a surviving
  master when a topology refresh prunes it; previously a failover or scale-in
  of the first-listed master made PING, INFO, DBSIZE, and other keyless
  commands fail with `ConnectionClosed` until the client was rebuilt
- reject a topology refresh in which a node reports no slot owners (for
  example after restarting without `nodes.conf`) instead of committing it and
  pruning every node service, and keep the caller's seed as a last-resort
  discovery source so a client whose services are all gone can still recover
- reject out-of-range slots and ports in `CLUSTER SLOTS` and `MOVED`/`ASK`
  replies instead of truncating them with `as u16`; a negative or ≥16384 slot
  panicked while indexing the slot table, and port 70000 silently became 4464
- route `XINFO` and `XGROUP` subcommands by their stream key; they were hashed
  by the subcommand name, costing a MOVED and a topology refresh per call and
  failing outright on connections built with `max_redirects(0)`
- recognize `HMSET`, `SDIFFCARD`, `SUNIONCARD`, `LMOVEM`, `BLMOVEM`, and the
  `XREAD`/`XREADGROUP` `MAXCOUNT`, `MAXSIZE`, and `CLAIM` options in
  `extract_keys`, so `common_slot` and pipeline routing no longer reject or
  misroute them

## [0.1.4](https://github.com/joshrotenberg/redis-tower/compare/redis-tower-cluster-v0.1.3...redis-tower-cluster-v0.1.4) - 2026-10-01

### Added

- binary-safe routing inputs from `CommandArg` across typed command families
  ([#721](https://github.com/joshrotenberg/redis-tower/pull/721),
  [#726](https://github.com/joshrotenberg/redis-tower/pull/726))

### Fixed

- normalize IPv6 TLS server names through the shared core transport path
  ([#718](https://github.com/joshrotenberg/redis-tower/pull/718))

### Changed

- make Cluster TLS conformance use a real TLS-only three-node cluster with
  trusted test certificates and fail-closed CI activation for both TLS backends
- share refreshing credential-provider state across node sessions to avoid
  concurrent refresh stampedes
  ([#732](https://github.com/joshrotenberg/redis-tower/pull/732))

## [0.1.3](https://github.com/joshrotenberg/redis-tower/compare/redis-tower-cluster-v0.1.2...redis-tower-cluster-v0.1.3) - 2026-09-24

### Fixed

- *(cluster)* support TLS connections over IPv6 ([#697](https://github.com/joshrotenberg/redis-tower/pull/697))

## [0.1.2](https://github.com/joshrotenberg/redis-tower/compare/redis-tower-cluster-v0.1.1...redis-tower-cluster-v0.1.2) - 2026-09-24

### Added

- support dedicated Cluster connections and binary Pub/Sub ([#693](https://github.com/joshrotenberg/redis-tower/pull/693))

## [0.1.1](https://github.com/joshrotenberg/redis-tower/compare/redis-tower-cluster-v0.1.0...redis-tower-cluster-v0.1.1) - 2026-08-26

### Added

- adaptive replica-read routing across direct and multiplexed clients, with
  explicit AZ mappings, EWMA latency feedback, health-aware ejection, timed
  recovery, and a max-ejection floor; writes and master reads are unchanged
  (#485)
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

- preserve percent-decoded credentials and TLS settings from cluster URLs
  across discovery, node setup, redirects, and reconnects
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
