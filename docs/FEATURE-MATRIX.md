# Feature matrix

How redis-tower compares to other Redis clients. Two tables: the Rust async
clients it is most often weighed against (redis-rs, fred), and the
cross-language clients that set the broader expectation for what a Redis client
should do (Lettuce, go-redis, StackExchange.Redis, ioredis).

## How to read this page

- Every **redis-tower** cell links to the code or test that backs the claim, so
  you can verify it rather than take it on faith. A claim with no verification
  path is not on this page.
- The competitor columns describe each library's **documented** capabilities.
  They are summaries of those projects' own docs, not measurements taken in this
  repository, and they are not linked. Check the upstream project before relying
  on a competitor cell.
- "Standalone-only" and "in progress" cells say so on purpose.
  This page is meant to be honest about where redis-tower is today, not where it
  is headed.
- This page is the canonical client comparison. The obsolete empty
  `comparisons/` directory husks were deliberately removed and remain removed;
  comparison claims belong here, next to a verification link, rather than in
  duplicated per-client pages that can drift independently.

Legend: yes / no / partial, with a short qualifier where it matters.

## redis-tower vs redis-rs and fred

| Capability | redis-tower | redis-rs | fred |
|---|---|---|---|
| Typed command builders (compile-time response types) | yes -- [`redis-tower-commands`](https://github.com/joshrotenberg/redis-tower/tree/main/crates/redis-tower-commands/src) | partial (stringly-typed `Cmd` + `AsyncCommands`) | partial (typed args, generic responses) |
| Tower `Service` / `Layer` composition | yes -- [`command_adapter.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/command_adapter.rs) | no | no |
| Auto-pipelining of concurrent commands | yes -- [`auto_pipeline.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/auto_pipeline.rs) | yes (multiplexed connection) | yes |
| Connection pool | yes (fixed/dynamic/lazy, active probing, explicit idle reaping) -- [`pool.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/pool.rs) | yes (`bb8`/`deadpool` via features) | yes |
| Cluster (MOVED/ASK, topology refresh) | yes -- [`redis-tower-cluster`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-cluster/src/connection.rs) | yes | yes |
| Sentinel discovery + failover | yes -- [`redis-tower-sentinel`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-sentinel/src/connection.rs) | yes | yes |
| Client-side caching (RESP3 tracking) | standalone and master-routed cluster: cloneable cached clients, tracking modes, reconnect-safe invalidation, bounds, and metrics -- [`caching.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/caching.rs), [`cluster caching.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-cluster/src/caching.rs), [live cluster coverage](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-cluster/tests/cluster_integration.rs) | yes (`cache-aio`) | yes |
| Circuit breaker | yes -- [`circuit_breaker.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/circuit_breaker.rs) | no | partial (via reconnect policy) |
| Per-command timeout | yes -- [`command_timeout.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/command_timeout.rs) | partial (connection-level) | yes |
| Reconnect with backoff + jitter | yes -- [`reconnect.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/reconnect.rs) | partial | yes |
| Tracing / observability | tracing with OTel DB semconv; metrics-facade recorder, pool/queue exporters, and cluster redirect/topology/opt-in bounded node metrics -- [`tracing_layer.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/tracing_layer.rs), [`metrics_layer.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/metrics_layer.rs) | no | partial (tracing feature) |
| RESP3 protocol | standalone, cluster, and Sentinel, including AUTH-before-HELLO setup -- [`codec.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-protocol/src/codec.rs), [`cluster connection.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-cluster/src/connection.rs), [`sentinel discovery.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-sentinel/src/discovery.rs) | yes | yes |
| TLS (rustls + native-tls, mTLS) | yes -- [`tls.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-core/src/tls.rs) | yes | yes |
| Pub/sub | standalone plus fixed-node and slot-following Cluster subscriptions -- [`pubsub.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/pubsub.rs), [`cluster pubsub.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-cluster/src/pubsub.rs) | yes | yes |
| Stream consumer groups (high-level) | yes -- [`consumer.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/consumer.rs) | partial (raw commands) | partial |
| Pipeline + transactions (MULTI/EXEC/WATCH) | yes -- [`pipeline.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/pipeline.rs), [`transaction.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/transaction.rs) | yes | yes |
| Lua scripting (EVALSHA-first) | yes -- [`script.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/script.rs) | yes | yes |
| Distributed locks and shared rate limits | fenced lock and Redis-time GCRA -- [`redis-tower-primitives`](https://github.com/joshrotenberg/redis-tower/tree/main/crates/redis-tower-primitives/src) | no (external crates available) | no (external crates available) |
| Redis Stack modules (JSON/Search/TS/Bloom/Vector) | yes -- [`redis-tower-modules`](https://github.com/joshrotenberg/redis-tower/tree/main/crates/redis-tower-modules/src) | partial (JSON via separate crate) | partial (RedisJSON/RediSearch) |
| Blocking (sync) client | yes -- [`redis-tower-sync`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-sync/src/lib.rs) | yes (sync connection) | no |
| One client over standalone/cluster/sentinel | yes -- [`redis-tower-client`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-client/src/lib.rs) | no | no |
| Pluggable credential provider (token rotation) | reconnect and push rotation across standalone, pool, cluster, and Sentinel; AWS IAM and Entra providers -- [`credentials.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/credentials.rs), [`cloud auth guide`](https://github.com/joshrotenberg/redis-tower/blob/main/docs/CLOUD-AUTH.md) | partial | partial |

## redis-tower vs Lettuce, go-redis, StackExchange.Redis, ioredis

These are the most-used clients in the JVM, Go, .NET, and Node ecosystems. The
comparison is about capability parity across languages, not API shape.

| Capability | redis-tower (Rust) | Lettuce (Java) | go-redis (Go) | StackExchange.Redis (.NET) | ioredis (Node) |
|---|---|---|---|---|---|
| Typed command surface | yes -- [`redis-tower-commands`](https://github.com/joshrotenberg/redis-tower/tree/main/crates/redis-tower-commands/src) | yes | yes | yes | partial |
| Composable middleware layer | yes (Tower) -- [`command_adapter.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/command_adapter.rs) | no | partial (hooks) | no | partial (Promise wrappers) |
| Auto-pipelining | yes -- [`auto_pipeline.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/auto_pipeline.rs) | yes | partial | yes (multiplexed) | yes |
| Connection pool | yes (fixed/dynamic/lazy, active probing, explicit idle reaping) -- [`pool.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/pool.rs) | yes | yes | yes (multiplexed) | yes |
| Cluster (MOVED/ASK) | yes -- [`redis-tower-cluster`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-cluster/src/connection.rs) | yes | yes | yes | yes |
| Sentinel | yes -- [`redis-tower-sentinel`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-sentinel/src/connection.rs) | yes | yes | yes | yes |
| Client-side caching | standalone and master-routed cluster yes -- [`caching.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/caching.rs), [`cluster caching.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-cluster/src/caching.rs) | yes | partial | no | no |
| Circuit breaker | yes -- [`circuit_breaker.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/circuit_breaker.rs) | no (external) | no (external) | no (external) | no (external) |
| Per-command timeout | yes -- [`command_timeout.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/command_timeout.rs) | yes | yes (context) | yes | yes |
| Tracing / observability | tracing with OTel DB semconv; metrics-facade recorder, pool/queue exporters, and cluster redirect/topology/opt-in bounded node metrics -- [`tracing_layer.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/tracing_layer.rs), [`metrics_layer.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/metrics_layer.rs) | partial (Micrometer) | partial (hooks) | partial (events/profiling) | partial (events) |
| RESP3 protocol | standalone, cluster, and Sentinel, including authenticated setup -- [`codec.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-protocol/src/codec.rs), [`cluster connection.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-cluster/src/connection.rs), [`sentinel discovery.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-sentinel/src/discovery.rs) | yes | yes | partial | yes |
| TLS (incl. mTLS) | yes -- [`tls.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-core/src/tls.rs) | yes | yes | yes | yes |
| Pub/sub | standalone plus fixed-node and slot-following Cluster subscriptions -- [`pubsub.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/pubsub.rs), [`cluster pubsub.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower-cluster/src/pubsub.rs) | yes | yes | yes | yes |
| Stream consumer groups (high-level) | yes -- [`consumer.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/consumer.rs) | yes | partial | partial | partial |
| Transactions (MULTI/EXEC/WATCH) | yes -- [`transaction.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/transaction.rs) | yes | yes | yes | yes |
| Lua scripting (EVALSHA-first) | yes -- [`script.rs`](https://github.com/joshrotenberg/redis-tower/blob/main/crates/redis-tower/src/script.rs) | yes | yes | yes | yes |
| Redis Stack modules | yes -- [`redis-tower-modules`](https://github.com/joshrotenberg/redis-tower/tree/main/crates/redis-tower-modules/src) | partial | partial | partial | partial |
| Pluggable credential provider | reconnect and push rotation across standalone, pool, cluster, and Sentinel; AWS IAM and Entra providers -- [`cloud auth guide`](https://github.com/joshrotenberg/redis-tower/blob/main/docs/CLOUD-AUTH.md) | yes | partial | partial | partial |

## Two corrections this page enforces

These are stated explicitly so the page does not drift back into overclaiming:

1. **RESP3 is available on standalone, cluster, and Sentinel clients.** Cluster
   and Sentinel builders preserve an explicit protocol policy across
   authenticated discovery, node creation, redirects, topology refreshes,
   failover, and reconnects, with authentication before HELLO negotiation.
2. **Observability includes tracing and metrics.** redis-tower ships a
   `TracingLayer` with OpenTelemetry database semantic conventions and a
   `MetricsRecorder` integration backed by the `metrics` facade. The built-in
   recorder covers commands, pipelines, pools, cluster redirects, and topology
   refreshes; pool/queue snapshot exporters and Prometheus/OpenTelemetry
   examples show complete backend wiring. Per-node cluster labels remain
   opt-in and bounded to 64 concrete addresses per client plus `_OTHER`, with
   overflow folded into `_OTHER`.
3. **Client-side caching covers standalone and master-routed Cluster use.** The
   cluster client keeps one cache above routing, forces RESP3, and installs a
   receiver plus `CLIENT TRACKING ... REDIRECT` on every current master. Cache
   use fails closed with a full clear across receiver/data loss and topology
   coverage reconfiguration. Slot epochs also reject stale in-flight fills
   immediately when ownership moves. Replica read preferences remain
   unsupported until equivalent invalidation coverage can be guaranteed.

## Verifying a cell yourself

Each redis-tower link points at the module or test that implements the
capability. To exercise them:

```bash
# Unit + doc tests across all features
cargo test --lib --all-features

# Standalone integration suites (starts its own redis-server)
cargo test --test '*' --all-features

# Cluster and sentinel suites (single-threaded, gated behind --ignored)
cargo test -p redis-tower-cluster  --test cluster_integration  -- --ignored
cargo test -p redis-tower-sentinel --test sentinel_integration -- --ignored
```

See
[CONTRIBUTING.md](https://github.com/joshrotenberg/redis-tower/blob/main/CONTRIBUTING.md)
for the full check list.
