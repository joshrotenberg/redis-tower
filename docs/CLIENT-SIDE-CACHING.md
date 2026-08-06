# Client-side caching

redis-tower combines Redis server-assisted tracking with bounded local response
caches for standalone Redis and master-routed Redis Cluster deployments. Use
`CachedMultiplexedClient` for standalone work or
`redis_tower_cluster::CachedMultiplexedClusterClient` for Cluster. Both are
cheap to clone and share one coherent cache across their clones.

```rust,ignore
use std::time::Duration;
use redis_tower::{
    CacheTrackingMode, CachedClientConfig, CachedMultiplexedClient,
    commands::{Get, Set},
};

let config = CachedClientConfig::new()
    .max_entries(50_000)
    .client_ttl(Some(Duration::from_secs(15)))
    .tracking_mode(CacheTrackingMode::broadcast_with_prefixes(["user:"]));

let client =
    CachedMultiplexedClient::connect_with_config("127.0.0.1:6379", config).await?;
client.execute(Set::new("user:42", "Ada")).await?;

let first = client.execute(Get::new("user:42")).await?; // Redis miss
let second = client.clone().execute(Get::new("user:42")).await?; // local hit
assert_eq!(first, second);

let statistics = client.cache_statistics().await;
println!("hits={}, misses={}", statistics.hits, statistics.misses);
# client.shutdown().await;
# Ok::<(), redis_tower::RedisError>(())
```

## Standalone connection setup

The address constructor is the shortest path for plain standalone Redis.
`connect_url` applies URL authentication, database selection, TLS, or a Unix
socket to both the data connection and invalidation receiver. The
`*_with_connection_config` variants also propagate keepalive, connect timeout,
and RESP decode limits while forcing the RESP3 protocol required by tracking.

For custom TLS roots, mTLS, rotating credentials, or application-specific
setup, pass a `ConnectionFactory` to `from_factory`. To reuse an existing data
connection, use `from_connection_with_factory`; its receiver factory must
connect to the same standalone Redis server and reproduce the required
authentication/session setup. The data worker is intentionally fixed rather
than silently reconnecting without replaying tracking state; construct a new
cached client after a data-connection failure.

This two-connection design requires Redis Open Source-compatible `REDIRECT`
support. Redis Software and Redis Cloud currently do not support two-connection
tracking, so a `rediss://` URL alone does not make this client compatible with
those services.

## Tracking modes

`CachedClientConfig` supports three Redis tracking modes:

- `CacheTrackingMode::Broadcast` receives invalidations for every key, or only
  for configured binary-safe prefixes. It uses no per-client key table on the
  Redis server and is the default.
- `CacheTrackingMode::ServerDefault` asks Redis to remember the keys this data
  connection reads. It reduces invalidation traffic when the working set is
  narrow, at the cost of server-side tracking memory.
- `CacheTrackingMode::OptIn` tracks only cache misses selected by the client.
  redis-tower submits `CLIENT CACHING YES` and the read as one atomic worker
  request, so another clone cannot interleave a command between them.

For standalone `CachedMultiplexedClient`, all modes use two RESP3 connections.
A dedicated receiver owns invalidation pushes; the data connection redirects
tracking messages to its client ID and uses `NOLOOP`. Writes through the
standalone cached client therefore invalidate locally both before and after
dispatch. The second invalidation also rejects an old read that raced the
write.

The standalone cached client owns that connection-local state. Caller-issued `CLIENT
TRACKING`, `CLIENT CACHING`, `CLIENT REPLY`, `HELLO`, `RESET`, and `QUIT`
commands are rejected, including inside explicit pipelines and transactions.
Use a dedicated uncached connection for session administration. Read-only
diagnostics such as `CLIENT TRACKINGINFO`, `CLIENT GETREDIR`, and `CLIENT ID`
remain available.

The local response cache currently supports `GET`, `HGET`, `HGETALL`,
`LRANGE`, `SMEMBERS`, `ZRANGE`, and `TYPE`. Other commands still use the same
client and auto-pipeline worker, but bypass the local response cache. With
prefix-filtered broadcast tracking, cacheable reads outside the configured
prefixes also bypass both the cache and its hit/miss statistics; Redis would
not send invalidations for those keys, so caching them would be unsafe.

## Staleness and failure behavior

The local cache has two independent safety bounds:

- Redis invalidation pushes remove all cached command variants that depend on
  a changed key.
- `client_ttl` caps how long any entry can be served even when no invalidation
  arrives. The safe default is 30 seconds; pass `None` only when the deployment
  can tolerate relying exclusively on tracking.

Each cache miss snapshots a per-key invalidation epoch and a global generation.
If an invalidation, explicit clear, or local write happens while Redis is
answering, the late response is returned to its caller but is not inserted.
Epoch bookkeeping is bounded alongside the cache.

If the standalone invalidation receiver disconnects, redis-tower immediately
clears and disables the cache. Reads pass through to Redis while a supervisor
reconnects, obtains a new receiver ID, and installs the new redirect on the
data worker. Caching resumes only after that setup succeeds.

The data connection has stricter semantics because its tracking state and
in-flight command ordering cannot be reconstructed transparently. A clean
idle socket loss is detected by the auto-pipeline worker without waiting for a
cache miss or write; redis-tower marks caching unhealthy and clears the cache
before it releases any affected callers. The fixed worker then remains closed,
so construct a new cached client. Silent network black holes remain bounded by
the configured TCP keepalive policy or the next command deadline rather than
by an application heartbeat.

For the standalone client, `is_caching_healthy()` includes both receiver
tracking and data-worker health and is suitable for readiness or diagnostics.
It never treats an open cache as healthy after the fixed data worker has
stopped.

## Redis Cluster

`CachedMultiplexedClusterClient` keeps one cache above cluster routing and one
dedicated RESP3 invalidation receiver for every current master. Its builder
accepts the same `CachedClientConfig` and forces RESP3 across seed discovery,
data nodes, redirects, topology refreshes, receivers, and reconnects.

```rust,ignore
use redis_tower::{CacheTrackingMode, CachedClientConfig, commands::Get};
use redis_tower_cluster::CachedMultiplexedClusterClient;

let config = CachedClientConfig::new()
    .tracking_mode(CacheTrackingMode::OptIn);
let client = CachedMultiplexedClusterClient::builder("127.0.0.1:7000")
    .cache_config(config)
    .connect()
    .await?;

let value = client.execute(Get::new("user:42")).await?;
# let _ = value;
# client.shutdown().await;
# Ok::<(), redis_tower::RedisError>(())
```

Cache use opens only after every current master has a healthy data worker and
`CLIENT TRACKING ... REDIRECT` points at a live receiver. Data or receiver loss,
an ambiguous timeout, and any receiver/topology coverage rebuild close the gate
and clear the cache before it can reopen. MOVED and ASK close the gate before
the router awaits a new connection; slot ownership epochs reject responses
that began under an older owner. Opt-in dispatch remains one reserved worker
submission: `[CLIENT CACHING YES, command]`. Redis's one-shot `ASKING` and
`CLIENT CACHING YES` flags consume one another, so ASK closes the gate and
retries as `[ASKING, command]`; that migrated response is not cached.

The initial Cluster implementation intentionally supports
`ReadPreference::Master` only. Replica and prefer-replica policies are rejected
until equivalent invalidation coverage can be proven for replica reads.

## Capacity and observability

`max_entries` bounds local response entries; `0` makes the cache unbounded and
is not recommended for production. Eviction is intentionally simple and
bounded rather than an application-level replacement policy.

Every cached client exposes aggregate `hits`, `misses`, `invalidations`, and
`evictions`. Supplying a `MetricsRecorder` in `CachedClientConfig` emits the
same bounded events; `MetricsFacadeRecorder` publishes them as
`redis_tower.cache.events` with a four-value `event` label. Keys and command
arguments are never metric labels.

## Tower and exclusive-client forms

`CacheLayer` / `CacheService` remain available for custom Tower stacks. Prefer
constructors that attach and own an invalidation stream. A cache service built
without invalidations is only safe when the caller explicitly clears it after
every possible external write; its constructor documents that hazard loudly.
Recorder-enabled stream constructors are available when custom Tower stacks
need the same cache-event metrics as `CachedClientConfig`.

Tower calls `poll_ready` before supplying a request, so an inner service may
already hold bounded capacity when `CacheService` discovers a local hit. Cache
backends must implement `ReleaseReadiness` to return that capacity without an
inner `call`; redis-tower implements the contract for `AutoPipelineService`,
`FrameService`, and `ReconnectService`. Put `CacheLayer` directly above one of
those backends and place tracing, metrics, timeouts, and other middleware
outside the cache. Custom middleware placed inside the cache must explicitly
propagate `ReleaseReadiness` to its inner service. There is no blanket no-op
implementation because that could silently leak permits and stall a bounded
service.

`CachedClient` is the lower-throughput serialized compatibility form. It uses
the same cache service and connection actor with a one-request batch limit, so
it shares the clone-safe tracking, failure, race-protection, and shutdown
semantics without auto-batching concurrent misses. Existing
`CachedClient::connect` callers keep the safe broadcast defaults. Both
standalone cached client types expose `shutdown()` so the final clone can
explicitly join their worker and invalidation lifecycles. The cached Cluster
client provides the same final-clone shutdown behavior for all per-master
workers and receivers.
