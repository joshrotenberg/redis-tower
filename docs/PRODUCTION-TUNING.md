# Production tuning

redis-tower's defaults favor predictable behavior and broad compatibility.
Production tuning is the process of putting explicit bounds around your own
latency, concurrency, payload, and failure budgets. There is no universally
correct pool size or timeout: measure a representative workload, change one
limit at a time, and keep enough observability to tell saturation from Redis
latency or network failure.

This page describes the current public configuration surface. The values in
examples are starting points to test, not recommendations to copy unchanged.

## Start with the right client

Client choice has a larger effect than most numeric knobs:

| Workload | Start with | Why |
|---|---|---|
| Many tasks issuing short, independent commands | `MultiplexedClient` | One auto-pipelined connection; cheap clones; real queue backpressure |
| Long-running service that must reconnect | Factory-backed `MultiplexedClient` | Retains high concurrency while rebuilding the worker's connection |
| Repeated standalone reads with tolerable bounded staleness | `CachedMultiplexedClient` | Cloneable auto-pipelined misses plus local hits and RESP3 invalidation tracking |
| Modest traffic where the simplest reconnecting handle matters most | `ResilientRedisClient` | Built-in reconnect and single-flight recovery, but commands serialize through one mutex |
| Blocking commands or expensive reply parsing | `ConnectionPool<RedisConnection>` | Multiple independent connections isolate head-of-line blocking |
| Stateful sequence, pub/sub, or MONITOR | Dedicated `RedisConnection`-based API | Exclusive protocol/session ownership |
| High-concurrency Redis Cluster | `MultiplexedClusterClient` | Per-node auto-pipeline workers, redirects, and topology refresh |
| Repeated master-routed Cluster reads with bounded staleness | `CachedMultiplexedClusterClient` | One slot-aware shared cache with complete per-master RESP3 invalidation coverage |
| Sentinel-managed primary with failover | `MultiplexedSentinelClient::connect_with_reconnect` | Re-discovers and verifies the primary after failure or READONLY |

Never send `BLPOP`, `BRPOP`, blocking `XREAD` / `XREADGROUP`, or another
blocking command through a shared `MultiplexedClient`. One blocked Redis
connection stalls its pipeline worker and every clone. Isolate blocking traffic
on dedicated or pooled connections, and ensure the pool has enough independent
slots for the maximum intended number of simultaneous blockers.

Likewise, keep `PubSubConnection` and `MonitorStream` dedicated. Redis changes
the connection's mode for pub/sub and MONITOR, and MONITOR itself has a large
server-side throughput cost.

## Plan for Smart Client Handoff maintenance

Planned-maintenance handoff is opt-in for a factory-backed
`MultiplexedClient`. Construct it with
`MultiplexedClient::from_factory_with_maintenance` and retain the returned
`MaintenanceListenerHandle`. The initial connection and every replacement must
be RESP3 and must accept
`CLIENT MAINT_NOTIFICATIONS ON moving-endpoint-type none`; construction or
reconnection fails if registration does not succeed.

For each valid `MOVING` notification, redis-tower waits until half of the
server-supplied TTL and then creates a replacement through the original
factory. That factory must reproduce all required authentication, TLS, URL,
and other per-connection session setup. The `none` endpoint mode means the
server does not select a destination: service discovery or endpoint changes
must happen inside the factory. Set its connect timeout and reconnect budget so
it can establish a usable connection within the remaining half of the TTL.

Once `MOVING` has been accepted, the worker lets any active batch make one
completion attempt and does not replay it for the handoff. Newly queued work is
held until replacement succeeds; if the reconnect budget is exhausted, that
work fails instead of being sent on an unverified connection. Account for this
pause when choosing queue capacity and end-to-end deadlines.

`MIGRATING` is observational only and does not reconnect. Use
`from_factory_with_maintenance_and_events` and its lifecycle event bus if the
application needs the notification metadata. This support does not relax
command timeouts and does not cover pools, Cluster, blocking connections, or
Pub/Sub.

Dropping the maintenance handle disables future handling without shutting down
the client and cancels a half-TTL wait that has not committed. Prefer
`MaintenanceListenerHandle::shutdown().await` during orderly shutdown: it
waits for worker acknowledgement and, if replacement has already begun, for a
connected or terminal result. Shut down the client separately.

## Establish a baseline before tuning

Capture at least these workload characteristics under realistic concurrency:

- operations per second and the mix of command names;
- p50, p95, p99, and maximum command latency;
- number of concurrent application requests;
- request and response byte distributions, including worst-case collections;
- fraction of commands that block or perform expensive server work;
- auto-pipeline batch size and queue depth;
- client-cache hit/miss, invalidation, eviction, size, and tracking-health data;
- pool acquisition wait and in-flight commands, when using a pool;
- reconnect state, offline queue depth, lifecycle-event lag, timeout, cluster
  redirect, and topology refresh rates;
- Redis CPU, memory, evictions, connected clients, and slow-log entries.

Tune against the tail, not only average latency. A setting that improves mean
throughput while growing the queue can make p99 latency and overload recovery
worse.

## Tune automatic pipelining

`MultiplexedClient` wraps `AutoPipelineService`. Its defaults are:

| Setting | Default | Effect |
|---|---:|---|
| `max_batch_size` | 100 frames | Upper bound on one flush |
| `batch_window` | zero | Flush immediately after draining work already available |
| `queue_capacity` | 1024 requests | Bound on pending worker requests |
| `shed_load_on_full` | `false` | Apply backpressure instead of returning `QueueFull` |
| `response_timeout` | `None` | No deadline for a whole batch response |

Configure the worker before wrapping the connection:

```rust,ignore
use std::time::Duration;
use redis_tower::{AutoPipelineConfig, MultiplexedClient, RedisConnection};

let connection = RedisConnection::connect("127.0.0.1:6379").await?;
let client = MultiplexedClient::from_connection_with_config(
    connection,
    AutoPipelineConfig {
        max_batch_size: 128,
        batch_window: Duration::from_micros(250),
        queue_capacity: 2048,
        shed_load_on_full: false,
        response_timeout: Some(Duration::from_millis(500)),
        ..AutoPipelineConfig::default()
    },
);
```

### Batch size and window

- Increase `max_batch_size` when many commands arrive concurrently and Redis
  or network round trips dominate. Watch response size and p99 latency: larger
  batches occupy the connection longer and one slow response delays everything
  behind it.
- Keep `batch_window` at zero for latency-sensitive or lightly concurrent
  traffic. A short non-zero window can improve batching for bursty writes, but
  it deliberately adds that wait to the first request in a batch.
- Use the `redis_tower.pipeline.batch_size` histogram to verify that a change
  actually creates batches larger than one. Raising a limit that workloads
  never reach only adds theoretical capacity.

### Queue capacity and overload policy

The queue is a concurrency bound, not a throughput control. A larger queue can
absorb short bursts, but it also permits more memory use and queueing latency.
Start from a measured burst budget and alert well before sustained depth reaches
capacity.

With `shed_load_on_full: false`, Tower readiness waits for capacity and paces
producers. This is the preferred default when upstream callers honor
backpressure. Set it to `true` only when the application has an explicit
load-shedding response for `RedisError::QueueFull` and would rather reject than
wait.

`MultiplexedClient::queue_depth()` gives an instantaneous snapshot. With the
`metrics` feature, `spawn_queue_depth_exporter` publishes
`redis_tower.pipeline.queue_depth`; give each pipeline a stable low-cardinality
name.

### Response deadline

`response_timeout` covers the complete round trip for a flushed batch. When it
expires, the in-flight batch fails and a factory-backed worker discards and
rebuilds its connection. Set the deadline above the slowest command permitted
on that worker. A deliberately long blocking command and a 200 ms response
deadline are incompatible; separate those workloads instead of continually
raising the shared deadline.

## Use pools for isolation, not by default

A pool adds independent connections and server client state. More connections
are useful only when one connection is the bottleneck or commands must be
isolated; they also cost sockets, Redis memory, TLS handshakes, authentication,
health checks, and more opportunities for uneven load.

```rust,ignore
use std::time::Duration;
use redis_tower::{ConnectionConfig, ConnectionPool, RedisConnection};
use redis_tower::pool::{DispatchStrategy, PoolConfig};
use redis_tower::reconnect::AddrConnectionFactory;

let factory = AddrConnectionFactory::new("127.0.0.1:6379")
    .with_connection_config(
        ConnectionConfig::new()
            .with_connect_timeout(Some(Duration::from_secs(3))),
    );

let pool = ConnectionPool::<RedisConnection>::connect_with_factory(
    PoolConfig::default()
        .name("blocking-jobs")
        .bounds(2, 8)
        .dispatch(DispatchStrategy::LeastConnections)
        .health_check_interval(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(60))
        .acquisition_timeout(Duration::from_millis(250)),
    factory,
).await?;

// Background work is never implicit. Retain each handle for its lifetime.
let reaper = pool.spawn_idle_reaper(Duration::from_secs(15));
let prober = pool.spawn_health_prober(Default::default());
# let _ = (reaper, prober);
```

Pool guidance:

- `RoundRobin` is the cheapest and works well when command latency is uniform.
- `LeastConnections` is a better starting point for mixed short and long work;
  it avoids deliberately sending new work to the busiest slot.
- Keep the default bounded acquisition timeout or choose a service-specific
  bound. Disabling it can turn saturation into an invisible hang.
- Configure a factory when health checks should replace a dead slot. A closure-
  built pool without a retained factory can detect failure but cannot restore
  capacity automatically.
- Dynamic sizing is opt-in. `bounds(min, max)` starts with `min` connections
  and creates more only when every live slot is contended. An `idle_timeout`
  is inert until `spawn_idle_reaper` is called; dropping its handle stops
  shrinking immediately.
- Active health probing is also explicit. The default PING probe, ROLE probe,
  replication-lag-via-INFO probe, and custom `HealthProbe` implementations
  update metrics and `PoolStats`; they do not silently become a second circuit
  breaker or change dispatch policy. Run the byte-lag probe against primaries:
  it compares primary-side offsets for directly connected replicas and marks
  missing/offline replicas unhealthy. Replica-local INFO cannot establish the
  upstream primary's current byte offset and is rejected conservatively.
- Size from observed concurrency and Redis capacity. For blocking commands, the
  minimum useful size is usually the maximum number of blockers you intend to
  allow, plus deliberate headroom for health/control traffic. Enforce a higher-
  level concurrency limit rather than letting callers grow without bound.
- Use a stable, unique `PoolConfig::name`; it becomes the pool metrics label.

Inspect `pool.stats()` for current/min/max size, idle and in-flight counts,
active-probe health, replication lag, and cumulative idle reaping. Acquisition
wait increasing while Redis command latency stays flat indicates pool
contention. Both increasing usually means Redis or the network is the
bottleneck, and adding connections may make it worse.

## Set timeouts at distinct failure boundaries

One timeout cannot represent all phases of a Redis operation:

| Boundary | API | What it limits |
|---|---|---|
| TCP/Unix connect | `ConnectionConfig::with_connect_timeout` | Initial transport establishment |
| Reconnect attempt | `ReconnectConfig::connect_timeout` | One factory call during recovery |
| Auto-pipeline response | `AutoPipelineConfig::response_timeout` | A whole flushed batch's response |
| End-to-end typed call | `WithDeadline<Cmd>` | One absolute budget carried through middleware, queueing, routing, retries, pool waiting, and I/O |
| Tower call | `CommandTimeoutLayer` | A static upper bound; `.with_request_deadlines()` lets an earlier `WithDeadline` shorten it |
| Pool acquisition | `PoolConfig::acquisition_timeout` | Waiting to reserve a pool slot |

Choose each from a remaining end-to-end latency budget. For example, pool wait
plus command execution plus application processing must fit inside the request
deadline; making all three independently equal to that deadline silently
multiplies the worst case.

Carry one caller budget with `WithDeadline` and put `CommandTimeoutLayer`
**outside** `ExecutorService`. This ordering lets the timeout layer inspect the
typed command before `ExecutorService` adapts it, while the pool sees the same
absolute deadline during acquisition:

```rust,ignore
use std::time::Duration;
use redis_tower::{
    CommandTimeoutLayer, ConnectionPool, ExecutorService, RedisConnection,
    WithDeadline,
};
use redis_tower::commands::Get;
use tower_layer::Layer;
use tower_service::Service;

let pool = ConnectionPool::connect(4, || {
    RedisConnection::connect("127.0.0.1:6379")
}).await?;

let mut service = CommandTimeoutLayer::new(Duration::from_secs(1))
    .with_request_deadlines()
    .layer(ExecutorService::new(pool));

let command = WithDeadline::after(Get::new("key"), Duration::from_millis(250));
let value = service.call(command).await?;
```

The earliest limit wins rather than starting a fresh budget at each stage:

- nested `WithDeadline` envelopes retain the earliest absolute instant, and
  cloning or retrying them does not reset it;
- `CommandTimeoutLayer::with_request_deadlines` uses the earlier of that
  instant and its static duration, returning `RedisError::CommandTimeout`;
- a pool uses the earlier of the command deadline and its static acquisition
  timeout. The command deadline returns `CommandTimeout`; the pool's own limit
  returns `PoolAcquisitionTimeout`.

The plain `CommandTimeoutLayer` remains generic over arbitrary Tower request
types and applies only its configured duration. Opting in to request deadline
extraction adds the `RequestDeadline` bound; all typed commands implement it,
and raw RESP frames report no typed deadline.

`CommandAdapter` is the typed-to-frame boundary. Frame middleware inside the
adapter cannot inspect command metadata, although the adapter enforces the
absolute command deadline around its inner frame call. Likewise,
`MultiplexedClient::execute` applies it across readiness and dispatch. Put
metadata-aware middleware outside the adapter (normally outside
`ExecutorService`) when the middleware itself needs to observe the deadline.

An already-expired command is rejected before the inner service is called.
This prevents newly enqueueing a side effect after its caller budget is gone.
Cluster clients retain the same deadline through node lookup, routing,
redirects, and pinned-node execution; multiplexed Sentinel dispatch and the
resilient client's offline queue retain it through their own waits as well.
If a deadline interrupts an exchange after bytes reach the wire, redis-tower
quarantines that connection instead of allowing its late response to poison a
later request.
The caller-level timeout still is not a substitute for worker-level
`response_timeout`, which detects a connection whose batch never returns. Use
both when a multiplexed production client needs bounded caller latency and
bounded connection recovery.

Do not retry every timeout automatically. Redis may have executed a write before
the response was lost. The typed retry wrapper checks command idempotency and
retryable error classification before replaying:

```rust,ignore
use std::time::Duration;
use redis_tower::RetryPolicy;

let client = client.retry(
    RetryPolicy::default()
        .max_retries(2)
        .base_delay(Duration::from_millis(25))
        .max_delay(Duration::from_millis(250)),
);
```

Keep jitter enabled in production so a Redis restart does not synchronize all
application instances into a reconnect or retry storm. `RetryService` also
checks the command's absolute deadline while waiting for readiness, executing
each attempt, and sleeping between attempts. A retry layer cannot reset or
sleep past the caller's `WithDeadline`, regardless of its position relative to
the timeout layer. `RetryClient::execute` also bounds its initial readiness
wait. With the lower-level Tower `Service` API, `poll_ready` necessarily occurs
before the request is supplied, so request metadata starts governing the retry
service once `call` begins; place a separate outer bound around a potentially
blocking caller-side readiness wait.

## Make reconnection replay connection state

Use a connection factory for long-running multiplexed clients. The factory is
responsible for recreating all required session state on every connection:

- URL username/password and database selection;
- TLS roots and client certificates;
- RESP mode and decode limits;
- rotating credentials supplied by `CredentialConnectionFactory`, or other
  initialization performed by a custom factory.

```rust,ignore
use std::time::Duration;
use redis_tower::{
    AutoPipelineConfig, ConnectionConfig, KeepaliveConfig, MultiplexedClient,
    RespLimits,
};
use redis_tower::auto_pipeline::AutoPipelineReconnectConfig;
use redis_tower::reconnect::{ReconnectConfig, UrlConnectionFactory};

let connection_config = ConnectionConfig::new()
    .with_connect_timeout(Some(Duration::from_secs(3)))
    .with_keepalive(
        KeepaliveConfig::new()
            .with_idle(Duration::from_secs(30))
            .with_interval(Duration::from_secs(10))
            .with_probes(3),
    )
    .with_resp_limits(RespLimits {
        max_frame_size: 16 * 1024 * 1024,
        max_depth: 64,
    });

let factory = UrlConnectionFactory::new(
    "redis://default:secret@127.0.0.1:6379/0",
).with_connection_config(connection_config);

let reconnect = ReconnectConfig::default()
    .base_delay(Duration::from_millis(100))
    .max_delay(Duration::from_secs(5))
    .connect_timeout(Duration::from_secs(3));

let client = MultiplexedClient::from_factory(
    factory,
    AutoPipelineConfig {
        response_timeout: Some(Duration::from_secs(1)),
        ..AutoPipelineConfig::default()
    },
    AutoPipelineReconnectConfig::new(reconnect),
).await?;
```

Use `CredentialConnectionFactory` when authentication comes from a dynamic
provider rather than a static URL:

```rust,ignore
use redis_tower::{
    AutoPipelineConfig, ConnectionConfig, CredentialConnectionFactory,
    MultiplexedClient, ProtocolVersion, StaticCredentials,
};
use redis_tower::auto_pipeline::AutoPipelineReconnectConfig;

// Replace StaticCredentials with an application or cloud CredentialProvider.
let factory = CredentialConnectionFactory::new(
    "127.0.0.1:6379",
    StaticCredentials::password("token"),
)
.with_connection_config(
    ConnectionConfig::new().with_protocol(ProtocolVersion::Resp3),
);

let client = MultiplexedClient::from_factory(
    factory,
    AutoPipelineConfig::default(),
    AutoPipelineReconnectConfig::default(),
).await?;
```

The credential factory opens each transport in RESP2, fetches credentials,
runs `AUTH`, and then negotiates the protocol requested by
`ConnectionConfig`. The complete sequence runs on initial connection and every
reconnect, so each fresh socket consults the provider. The same factory also
implements `PoolFactory`, allowing lazy or replacement pool slots to use the
same setup.

An authentication rejection during connection establishment is handled
narrowly: `NOAUTH` or `WRONGPASS` calls the provider's `force_refresh()` and
retries `AUTH` once. Providers shared by multiple clients or pool slots must
synchronize concurrent refreshes. If an already-established connection
returns `NOAUTH` or `WRONGPASS` for a user command, redis-tower returns that
error without reauthenticating or replaying the command. This avoids silently
duplicating work and keeps retry policy with the caller.

`max_retries` counts retries after the first reconnect attempt: zero still
allows one attempt, and `n` allows at most `n + 1` attempts. The initial
connection is outside that retry count. `base_delay` is applied before the
first reconnect attempt, and `connect_timeout` bounds every factory call,
including initial construction and each reconnect.

TCP keepalive detects dead peers below the Redis protocol, but its effective
timing is platform-dependent and usually much slower than an application
deadline. It complements response and reconnect timeouts; it does not replace
them.

## Decide whether to queue while offline

`ResilientRedisClient` fails later work while reconnecting by default. For a
workload that benefits from waiting through a short outage, enable its bounded
offline queue explicitly:

```rust,ignore
use redis_tower::{OfflineQueueConfig, ResilientRedisClient};

let client = ResilientRedisClient::connect_url_with_offline_queue(
    "redis://default:secret@127.0.0.1:6379/0",
    OfflineQueueConfig::new(256).with_max_replay_attempts(3),
).await?;
```

The queue is shared across clones and replays admitted work in queue-ticket
order after one reconnect campaign. Admission is serialized, but concurrently
first-polled futures can receive tickets in either scheduler order. Admission
is deliberately narrow:

- only typed commands marked idempotent can wait and replay;
- non-idempotent commands and raw frame-service calls fail offline;
- overflow returns `RedisError::QueueFull` immediately;
- reconnect exhaustion returns `RedisError::ReconnectFailed` to queued work;
- canceling a queued future releases its slot; and
- canceling after a request reaches the wire quarantines that socket and
  reconnects before the next ticket runs, preventing a late response from
  being consumed by the wrong caller;
- each command has a finite replacement-wire replay budget (three by default),
  so repeated successful connects followed by immediate disconnects cannot
  keep the head ticket alive forever; and
- capacity zero preserves fail-fast behavior while reconnect continues in the
  background.

A `WithDeadline` budget includes admission, queue wait, reconnect, replay, and
wire I/O. Expiry removes the ticket, and an expiry after dispatch quarantines
the socket before the next ticket can run.

Size capacity from the number of operations the service can safely absorb over
the outage window, not from normal throughput. Alert on
`offline_queue_depth()` before it reaches capacity and use `is_reconnecting()`
as a diagnostic or readiness input. Queueing increases outage latency and
memory and can create a recovery burst, so fail-fast remains the safer choice
for many request/response services. The replay-attempt budget is distinct from
`ReconnectConfig::max_retries`, which limits retries after the first connection
attempt within one campaign. Configure a finite reconnect budget and
`connect_timeout` as well if each queued operation needs a bounded recovery
path through an unreachable endpoint.

For a frame-level Tower stack, `tower::buffer::Buffer` can sit in front of
`ReconnectService`. It cannot inspect `Command::idempotent()` after a command
has become a raw frame, so it does not provide the typed queue's replay-safety
guarantee. Do not treat a generic frame buffer as authorization to replay
mutations.

## Bound protocol memory

`RespLimits` constrains the maximum buffered frame size and nesting depth. The
defaults preserve normal Redis compatibility. Tighten them for shared,
untrusted, or memory-constrained environments, but keep `max_frame_size` above
the largest legitimate response (large values, collection reads, module
results, and administrative output are common surprises).

Install limits through `ConnectionConfig` before connecting. For reconnecting
clients put that config on the factory, so recovery cannot silently return to
unbounded/default decoding. Cluster and Sentinel builders expose
`.resp_limits(...)` and retain the limits across topology changes and failover.

Limit large collection work at the command level as well: prefer bounded scans
and pagination over commands that materialize an entire unbounded keyspace or
collection in one response.

## Retry and circuit-breaker policy

Retries consume capacity precisely when a dependency is unhealthy. Keep retry
budgets small, exponential, and jittered, and reserve them for commands whose
typed implementation reports that replay is safe. A non-idempotent command is
not retried by `RetryClient`, even when its error is otherwise retryable.

The Redis-aware circuit breaker counts transport failures and command deadlines
that expire after dispatch, not readiness-only waits or ordinary Redis command
errors such as `WRONGTYPE`. Start with the default and tune only from
incident/load-test evidence:

```rust,ignore
use redis_tower::RedisCircuitBreakerConfig;

let client = client.with_circuit_breaker(RedisCircuitBreakerConfig {
    failure_threshold: 5,
    recovery_probe_interval: std::time::Duration::from_secs(5),
});
let health = client.circuit_breaker_handle();
```

Export `health.state()`, `health.health_status()`, or its metrics rather than
using a successful process check as Redis readiness. Keep liveness independent
from Redis so an outage does not restart every application instance in a loop;
use Redis health for readiness and traffic admission.

## Cluster tuning

`MultiplexedClusterClient` creates an auto-pipeline worker per node. Its builder
accepts the same `AutoPipelineConfig`, a per-node reconnect config, a redirect
budget, read preference, credentials, TLS, and RESP limits.

- Leave `ReadPreference::Master` in place unless the application accepts
  replica staleness. `Replica` and `PreferReplica` change consistency, not only
  performance.
- `max_redirects` bounds both latency and repeated work during resharding.
  Raising it can hide unstable topology while increasing tail latency; monitor
  `MOVED`, `ASK`, and topology refresh metrics first.
- Use `host_override` or `address_map` when CLUSTER SLOTS advertises internal
  addresses that clients cannot reach. Validate every failover and resharding
  path, not only initial seed discovery.
- Per-node metric labels are disabled by default. Enable them only when the
  diagnostic value is worth the additional series; redis-tower bounds concrete
  node-address labels and folds overflow into `_OTHER`.
- Call `shutdown()` after stopping producers so all per-node workers can drain.

## Sentinel tuning

Use `MultiplexedSentinelClient::builder(...).connect_with_reconnect()` for
production failover. Configure Sentinel-hop and Redis-node credentials/TLS
separately when the deployment does; the client re-queries Sentinel, verifies
`ROLE`, and reconnects after a transport failure or a `READONLY` response from
a demoted primary.

Test the complete failure path under load:

1. stop or demote the current primary;
2. verify in-flight errors are visible and bounded;
3. verify Sentinel reaches agreement and the client discovers the new primary;
4. verify credentials, TLS, and RESP limits still apply;
5. verify writes resume without a retry storm or silent duplicate mutation.

Sentinel discovery availability depends on the set of addresses you provide.
Use multiple independently placed Sentinel endpoints and monitor discovery
failures separately from data-node failures.

To observe verified primary changes, pass a `ConnectionEventBus` through
`MultiplexedSentinelClient::builder(...).connection_events(events)` before
`connect_with_reconnect()`, or use
`MultiplexedSentinelClient::connect_with_reconnect_and_events(...)`. The first
ROLE-verified primary establishes the baseline; only a later verified address
change publishes `ConnectionEvent::Failover`. The comparison uses the exact
endpoint strings Sentinel returns after ROLE verification; it is not durable
node identity. DNS/textual aliases for one server can therefore look like a
change, while replacing a node behind the same endpoint does not emit a
failover event.

## Observability that drives tuning

Enable the `metrics` feature and install one application-level `metrics`
exporter. The built-in recorder can report:

- `db.client.operation.duration` and `redis_tower.commands` for command
  latency/count/outcome;
- `redis_tower.pipeline.batch_size` and
  `redis_tower.pipeline.queue_depth` for worker efficiency and saturation;
- `redis_tower.cache.events` for bounded hit, miss, invalidation, and eviction
  counts;
- `db.client.connection.wait_time`,
  `db.client.connection.pending_requests`, connection count/max, and pool
  lifecycle counters, including active-probe outcomes, replication lag, and
  idle reaping. Treat replication lag as current only while its paired
  `*_observed` freshness gauge is `1`;
- `redis_tower.cluster.redirects`, topology refresh count, and topology refresh
  duration.

Use stable pool and pipeline names. Do not put keys, user IDs, request IDs, or
unbounded host strings into metric labels. Pair client metrics with Redis
`INFO`, latency monitoring, slow log, CPU, memory, eviction, and network data;
client-side latency alone cannot distinguish its cause.

For cached clients, alert on unhealthy caching before optimizing hit rate.
While the invalidation receiver is being replaced, redis-tower clears and
disables the local cache, so a falling hit rate paired with healthy Redis
latency is an expected safety response rather than silent staleness. A lost
fixed data worker on standalone `CachedMultiplexedClient` also clears the cache
but requires constructing a new cached client. The Cluster cached client
instead rebuilds complete data-worker and receiver coverage across every
master before reopening its global gate, and requires a finite `client_ttl` as
a backstop for ownership changes it does not observe. `is_caching_healthy()`
distinguishes these states. See the
[client-side caching guide](CLIENT-SIDE-CACHING.md) for tracking modes and
failure semantics.

Tracing can be sampled more aggressively than metrics. Set a slow-command
threshold on `TracingLayer` to preserve useful tail diagnostics without making
every successful GET equally prominent.

### Observe connection lifecycle without gating it

Attach a bounded `ConnectionEventBus` when constructing a reconnecting client.
Subscribe first if the initial connection result matters because the stream
does not replay old events:

```rust,ignore
use redis_tower::{
    AutoPipelineConfig, ConnectionEventBus, ConnectionEventRecvError,
    MultiplexedClient,
};
use redis_tower::auto_pipeline::AutoPipelineReconnectConfig;
use redis_tower::reconnect::{ReconnectConfig, UrlConnectionFactory};

let events = ConnectionEventBus::new(256);
let mut stream = events.subscribe();

let client = MultiplexedClient::from_factory_with_events(
    UrlConnectionFactory::new("redis://127.0.0.1:6379"),
    AutoPipelineConfig::default(),
    AutoPipelineReconnectConfig::new(ReconnectConfig::default()),
    events,
).await?;

tokio::spawn(async move {
    loop {
        match stream.recv().await {
            Ok(event) => record_connection_event(event),
            Err(ConnectionEventRecvError::Lagged { skipped }) => {
                record_event_lag(skipped);
            }
            Err(ConnectionEventRecvError::Closed) => break,
            Err(_) => break,
        }
    }
});
```

`ConnectionEventBus::default()` retains 64 events per subscriber; choose a
larger explicit capacity only from observed event bursts and consumer latency.
Publishing is synchronous and non-waiting, and every subscriber has an
independent cursor. A slow consumer receives an explicit `Lagged { skipped }`
error but never backpressures reconnect or failover. Treat the stream as an
observability feed rather than a durable audit log, and ensure consumer work
cannot grow an unbounded secondary queue.

Events distinguish initial connect failure, disconnect reason, each scheduled
and failed reconnect attempt, successful reconnection, exhausted retry budget,
intentional worker shutdown, and topology-confirmed endpoint failover.
`Disconnected { reason: Shutdown }` is a producer's terminal transition and is
emitted separately after an earlier outage disconnect; when clients share one
bus, other producers may continue publishing afterward.
Human-readable connect/disconnect error strings can contain endpoint names,
server responses, and other deployment details; redact them before exporting
to systems with a broader audience. Aggregate events into stable counters and
durations, and never put their free-form error text or addresses into
unbounded metric labels. Sentinel publishes a single verified primary endpoint
change automatically. Redis Cluster has multiple slot-scoped primaries, so its
topology manager should call `ConnectionEventBus::publish` only when the
application can state an honest, specific failover transition; do not infer one
global failover from ordinary slot churn.

## Graceful shutdown

Shutdown ordering prevents accepted work from disappearing in background
queues:

1. stop accepting new application requests and stop background producers;
2. await tasks that own client clones;
3. stop queue/pool metrics exporters that retain client clones;
4. call `ConnectionPool::close()` to reject new pool work and drain accepted
   operations;
5. consume the last multiplexed client with `shutdown().await` (cluster and
   Sentinel clients also expose `shutdown`);
6. flush tracing/metrics providers and exit.

Calling `shutdown()` while another multiplexed clone remains alive returns
without stopping the shared worker. Make ownership explicit in the application's
shutdown coordinator, and test SIGTERM behavior while commands are queued and
in flight.

## A repeatable tuning loop

1. Define an end-to-end latency SLO and an overload response.
2. Load test the correct client shape with representative command and payload
   distributions.
3. Set connect, acquisition, response, and caller deadlines from that budget;
   use `WithDeadline` when one absolute caller budget must cross pool waiting.
4. Choose backpressure, explicit `QueueFull` shedding, and fail-fast versus
   bounded idempotent offline queueing; test saturation and recovery.
5. Adjust one of batch size, batch window, queue capacity, pool size, or
   dispatch strategy at a time.
6. Compare throughput and latency distributions together with queue/pool/Redis
   utilization.
7. Inject connection loss, a black-holed endpoint, Redis restart, cluster
   resharding, and Sentinel failover as applicable.
8. Verify idempotent retry behavior and ensure mutations are never duplicated.
9. Exercise graceful shutdown with live work.
10. Record the chosen values, workload assumptions, and alert thresholds next
    to application configuration so future changes have a baseline.
