# Client comparison

This page compares documented client contracts, not feature counts. A checkmark
can hide the questions that matter in production: whether a connection is
exclusive or multiplexed, which commands are replayed after reconnect, whether
a cache is managed for you, and whether a "typed" API fixes the response type
or lets the caller choose one.

The comparison was checked on **2026-09-25** against the versions and revisions
below. Links point to upstream documentation or pinned source. Treat a claim as
unknown when the cited material does not establish it; do not infer absence
from an empty cell.

| Client | Baseline used here |
|---|---|
| redis-tower | [`186719e`](https://github.com/joshrotenberg/redis-tower/commit/186719e97c3b742ef713d4210d26538aebfe4f68); published workspace versions range from 0.1.1 to 0.1.3 |
| redis-rs | [`1.7.0` (`2a29a8f`)](https://github.com/redis-rs/redis-rs/tree/2a29a8fdec37ba5858f7681639b21617e7f824e1) |
| Fred | [`10.1.0` (`29d4790`)](https://github.com/aembke/fred.rs/tree/29d4790e8522a3b0c67531081fc0488585dbc665) |
| Lettuce | [`4461844`](https://github.com/redis/lettuce/tree/44618449ca11ad4c3414819e72280022a5c52163) plus the rolling Lettuce wiki, checked on the date above |
| go-redis | [`2fc3ccd`](https://github.com/redis/go-redis/tree/2fc3ccd4e373e2b38b47a7f0bb55c72a3a64d9f6) |
| StackExchange.Redis | [`0a92ae4`](https://github.com/StackExchange/StackExchange.Redis/tree/0a92ae43b0f8e80467920115a54f9761cc04ee4c) |
| ioredis | [`b597303`](https://github.com/redis/ioredis/tree/b59730310716d7e4b3330ee42c140f7acd409444) |
| node-redis | [`d3eac3d`](https://github.com/redis/node-redis/tree/d3eac3d5834cfe32970fb6e7383f044e49ee26ff) |
| redis-py | [`ba6976b`](https://github.com/redis/redis-py/tree/ba6976bc2b8d5daed034be982c30194770ed1c15) |

## Rust clients

redis-tower, redis-rs, and Fred overlap heavily, but expose different contracts.

| Contract | redis-tower | redis-rs 1.7 | Fred 10.1 |
|---|---|---|---|
| Typed command API | Command values fix their response types; [`Get`](../crates/redis-tower-commands/src/strings.rs) returns `Option<Bytes>`. Raw commands can opt into a decoder. | [`AsyncTypedCommands`](https://docs.rs/redis/1.7.0/redis/trait.AsyncTypedCommands.html) provides typed convenience methods; `Cmd::query_async::<T>` retains caller-selected conversion. | Command traits expose typed arguments and caller-selected response types; see the [interfaces module](https://docs.rs/fred/10.1.0/fred/interfaces/index.html). |
| Composition point | Frame services implement Tower `Service`; middleware includes [timeouts](../crates/redis-tower/src/command_timeout.rs), [circuit breaking](../crates/redis-tower/src/circuit_breaker.rs), [tracing](../crates/redis-tower/src/tracing_layer.rs), and [metrics](../crates/redis-tower/src/metrics_layer.rs). | No general Tower-style command middleware contract is documented. | [`Config`](https://docs.rs/fred/10.1.0/fred/types/config/struct.Config.html) and client interfaces expose client-specific configuration and hooks; no Tower `Layer` contract is documented. |
| Shared connection | [`MultiplexedClient`](../crates/redis-tower/src/multiplexed.rs) is cloneable and auto-pipelines concurrent commands over one connection. | [`MultiplexedConnection`](https://docs.rs/redis/1.7.0/redis/aio/struct.MultiplexedConnection.html) is cloneable and multiplexed. | [`Client`](https://docs.rs/fred/10.1.0/fred/clients/struct.Client.html) is a cloneable handle over the driver's connections; configuration selects central, cluster, Sentinel, or Unix transport. |
| Pool | [`ConnectionPool`](../crates/redis-tower/src/pool.rs) owns independent connections with fixed, dynamic, or lazy population and explicit health/lifecycle controls. | Optional `r2d2` and `bb8` integrations are listed in the [crate features](https://docs.rs/redis/1.7.0/redis/#features). A multiplexed connection is not itself a checkout pool. | [`Pool`](https://docs.rs/fred/10.1.0/fred/clients/struct.Pool.html) distributes commands across clients; stateful interfaces are intentionally omitted. |
| Topology | Standalone, Cluster, and Sentinel are separate clients; [`UniversalClient`](../crates/redis-tower-client/src/lib.rs) provides one enum-backed entry point. | Standalone, async Cluster, and Sentinel are documented crate features. | [`ServerConfig`](https://docs.rs/fred/10.1.0/fred/types/config/enum.ServerConfig.html) covers centralized, Cluster, Sentinel, and Unix deployments. |
| Reconnect and replay | [`ResilientRedisClient`](../crates/redis-tower/src/resilient.rs) single-flights reconnects with bounded backoff. Ordinary in-flight commands are not silently replayed; stateful APIs document their own restoration rules. | [`ConnectionManager`](https://docs.rs/redis/1.7.0/redis/aio/struct.ConnectionManager.html) reconnects in the background; the command that observes the dropped connection errors and later commands wait for the replacement connection. | [`ReconnectPolicy`](https://docs.rs/fred/10.1.0/fred/types/config/enum.ReconnectPolicy.html) supports constant, linear, and exponential delay with jitter. Replay guarantees vary by interface and should be checked at the call site. |
| Client-side caching | [Standalone caching](CLIENT-SIDE-CACHING.md) and master-routed Cluster caching manage tracking, invalidations, bounds, metrics, and reconnect fail-closed behavior. | The experimental [`cache-aio`](https://docs.rs/redis/1.7.0/redis/caching/index.html) feature integrates caching with multiplexed, connection-manager, and async Cluster connections. | [`TrackingInterface`](https://docs.rs/fred/10.1.0/fred/interfaces/trait.TrackingInterface.html) exposes RESP3 tracking and invalidation commands; it is not documented as a managed local cache. |
| Redis Stack and newer families | [`redis-tower-modules`](../crates/redis-tower-modules/src/lib.rs) covers JSON, Search, TimeSeries, Bloom/Cuckoo, and Vector Sets. | The [1.7 feature list](https://docs.rs/redis/1.7.0/redis/#features) includes JSON, Bloom, and Vector Sets; Search is marked unfinished. | The [interfaces module](https://docs.rs/fred/10.1.0/fred/interfaces/index.html) documents JSON and Search interfaces. Verify other families individually. |
| Credentials | [`CredentialProvider`](../crates/redis-tower/src/credentials.rs) supports reconnect/push rotation; sibling crates provide AWS IAM and Microsoft Entra providers. | The [feature list](https://docs.rs/redis/1.7.0/redis/#features) includes token-based authentication and Microsoft Entra support. | [`Config`](https://docs.rs/fred/10.1.0/fred/types/config/struct.Config.html) supports a credential provider. |
| Observability | Stable tracing fields, metrics-facade recording, pool/queue snapshots, and bounded opt-in per-node Cluster labels are documented in [Production tuning](PRODUCTION-TUNING.md). | No client-wide tracing/metrics contract is documented by the 1.7 crate front page; application wrappers remain possible. | The crate exposes partial tracing configuration and a [`MetricsInterface`](https://docs.rs/fred/10.1.0/fred/interfaces/trait.MetricsInterface.html). |
| RESP behavior | RESP2/RESP3 negotiation, push demultiplexing, and explicit rejection of unsupported attributes/streamed aggregates are documented by [`redis-tower-protocol`](../crates/redis-tower-protocol/src/lib.rs). | RESP2 and RESP3 are supported; URL protocol selection is documented by [`ConnectionInfo`](https://docs.rs/redis/1.7.0/redis/struct.ConnectionInfo.html). | RESP2 and RESP3 are supported; tracking APIs require RESP3. |

The important typed-API distinction is not "typed versus untyped." All three
clients provide typed conveniences. redis-tower command values own a specific
decoder, while redis-rs and Fred also make caller-selected response conversion
a common path.

## Lessons from widely used clients

These clients are useful design references, not direct feature-score opponents.

| Client | Documented contract worth carrying into a review |
|---|---|
| Lettuce | Its [execution reliability](https://github.com/redis/lettuce/wiki/Command-Execution-Reliability) documentation treats reconnect replay, duplicates, ordering, and transaction behavior as separate guarantees. A generic "reconnects automatically" claim is insufficient. |
| go-redis | The pinned [README](https://github.com/redis/go-redis/blob/2fc3ccd4e373e2b38b47a7f0bb55c72a3a64d9f6/README.md) documents automatic pooling, experimental streaming credentials, RESP3 client caching limits, and OpenTelemetry instrumentation. Its experimental auto-pipeline warns that retrying a whole batch can duplicate non-idempotent work. |
| StackExchange.Redis | [`ConnectionMultiplexer`](https://github.com/StackExchange/StackExchange.Redis/blob/0a92ae43b0f8e80467920115a54f9761cc04ee4c/docs/Basics.md) is designed to be shared and reused; cheap database handles and multiplexing are deliberately different from checking out a socket per request. |
| ioredis | The pinned [README](https://github.com/redis/ioredis/blob/b59730310716d7e4b3330ee42c140f7acd409444/README.md) documents standalone, Sentinel, and Cluster connections, offline queues, retry behavior, and auto-pipelining. Queueing before connect and replaying after a disconnect are different decisions. |
| node-redis | Its pinned [FAQ](https://github.com/redis/node-redis/blob/d3eac3d5834cfe32970fb6e7383f044e49ee26ff/docs/FAQ.md) says already-sent commands reject when the socket closes because Redis may have executed them; unsent commands can remain queued for reconnect. That boundary is a useful retry model. |
| redis-py | The [unified response proposal](https://github.com/redis/redis-py/blob/ba6976bc2b8d5daed034be982c30194770ed1c15/docs/unified_responses.md) separates wire-protocol shape from the public response contract. Differential tests must normalize client-facing values, not merely compare raw RESP frames. |

## What this page does not claim

- It does not rank clients. API fit, deployed topology, language ecosystem, and
  operational familiarity usually matter more than a total capability count.
- It does not publish performance conclusions. The repository's
  [benchmark publication protocol](../scripts/benchmarks/README.md) defines
  workloads, client versions and adapters, topology, connection count,
  timeouts, metadata, and reproducibility requirements. A result is evidence
  only for the measured environment and scenario.
- It does not turn undocumented behavior into "no." Unknown behavior stays
  unknown until a stable upstream source or a reproducible experiment supports
  a narrower statement.

When updating this page, pin the source revision, record the check date, link
the precise upstream contract, and describe reconnect, retry, replay, pooling,
caching, and response typing independently.
