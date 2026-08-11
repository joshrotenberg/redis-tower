# Serverless and scale-to-zero deployments

Serverless processes are often created speculatively, frozen between requests,
or started while Redis is unavailable. Opening Redis during global
initialization adds cold-start latency and can prevent an otherwise healthy
process from starting. `MultiplexedClient` therefore has lazy constructors that
create its lightweight worker immediately but defer DNS and socket work until
the first command. Construct the client after entering a Tokio runtime (for
example inside an async `main` before starting the handler loop); the
synchronous constructor panics when no runtime is entered.

## Lazy multiplexed client

For a Redis URL, including credentials, TLS, database selection, or a Unix
socket, construct the client synchronously:

```rust,ignore
use redis_tower::MultiplexedClient;
use redis_tower::commands::Ping;

# async fn invoke() -> Result<(), redis_tower::RedisError> {
let client = MultiplexedClient::connect_url_lazy(
    "redis://default:secret@cache.internal:6379/0",
);

// The first network connection is made here, not above.
client.execute(Ping::new()).await?;
# Ok(())
# }
```

Use `MultiplexedClient::connect_lazy("host:port")` for an unauthenticated TCP
endpoint. For custom connection setup or reconnect policy, pass a
`ConnectionFactory` to `MultiplexedClient::from_lazy_factory`:

```rust,ignore
use std::time::Duration;
use redis_tower::{AutoPipelineConfig, MultiplexedClient};
use redis_tower::auto_pipeline::AutoPipelineReconnectConfig;
use redis_tower::reconnect::{ReconnectConfig, UrlConnectionFactory};

let factory = UrlConnectionFactory::new("redis://cache.internal:6379/0");
let client = MultiplexedClient::from_lazy_factory(
    factory,
    AutoPipelineConfig::default(),
    AutoPipelineReconnectConfig::new(
        ReconnectConfig::default()
            .connect_timeout(Duration::from_secs(2))
            .base_delay(Duration::from_millis(50)),
    ),
);
# let _ = client;
```

Lazy construction cannot report a connection error because it performs no
connection attempt. The command that triggers a failed first attempt receives
`RedisError::ConnectionClosed`; a later command tries the factory again. After
the first successful connection, ordinary factory-backed reconnect behavior
applies when that connection is lost.

## Lazy, scale-to-zero pool

When commands need independent connections, a factory-backed pool can also
start empty. It grows on command demand and acquisition contention, up to the
configured maximum:

```rust,ignore
use std::time::Duration;
use redis_tower::{ConnectionPool, PoolConfig};
use redis_tower::reconnect::UrlConnectionFactory;

let pool = ConnectionPool::connect_lazy(
    PoolConfig::default()
        .max_size(8)
        .idle_timeout(Duration::from_secs(60)),
    UrlConnectionFactory::new("redis://cache.internal:6379/0"),
);
assert_eq!(pool.size(), 0); // no DNS or socket work yet

// Reaping is explicit. Retain this handle for as long as the task should run;
// dropping it stops the task and lets the pool remain at its current size.
let reaper = pool.spawn_idle_reaper(Duration::from_secs(15));
# let _ = reaper;
```

The lazy pool's minimum is zero. An idle reaper may therefore return it to
zero after the configured timeout. Pool construction never starts the reaper
implicitly. Use `connect_with_factory` plus `PoolConfig::bounds(min, max)`
when a warm minimum is more important than scale-to-zero behavior.

## Health and lifecycle semantics

A lazy client's connection-health snapshot starts unhealthy. This means
"not connected yet," not that construction failed. It becomes healthy just
before the first `ConnectionEvent::Connected` event. With
`from_lazy_factory_with_events`:

- construction emits no lifecycle event;
- a failed deferred attempt emits `ConnectionEvent::ConnectFailed`;
- the first successful deferred attempt emits `ConnectionEvent::Connected`;
- failures after that point use the normal disconnect and reconnect events.

`is_connection_healthy()` reads that local snapshot without opening a socket;
`subscribe_connection_health()` observes later changes. In contrast,
`health_check()` sends `PING` and therefore triggers the deferred connection.

Do not use a Redis `PING` as the platform's process-liveness check: it forces a
cold instance to connect and defeats lazy initialization. Keep liveness local.
Use a Redis-backed readiness or dependency check only when the platform should
route traffic based on current Redis availability.

## Invocation and shutdown pattern

Create one client per warm process and clone it into invocation handlers.
Clones share one worker and connection, preserving auto-pipelining across
concurrent invocations. Avoid constructing a client for every request.

Bound command duration using command deadlines or an outer timeout appropriate
to the function's remaining execution budget. Configure
`ReconnectConfig::connect_timeout`; otherwise an unreachable endpoint may wait
for the operating system's TCP timeout.

When the runtime offers a reliable shutdown hook, call `shutdown()` after all
client clones are gone so accepted work drains and the worker is joined. Many
serverless platforms freeze or terminate without such a hook, so correctness
must not depend on shutdown running.

## Practical checklist

- Store credentials in the platform's secret store and prefer a `redis://` or
  `rediss://` URL so AUTH and SELECT are replayed after reconnect.
- Keep the client outside the per-invocation handler and clone it cheaply.
- Set a finite connection timeout below the invocation deadline.
- Expect the first command on a cold process to include connection latency.
- Keep process liveness local; use Redis-backed readiness deliberately.
- Size concurrency so one auto-pipelined connection is appropriate, or use a
  bounded pool when commands need exclusive connection state.
- Never send blocking commands through `MultiplexedClient`; use a dedicated
  connection or pool checkout instead.

See [Production tuning](PRODUCTION-TUNING.md) for queue sizing, response
timeouts, reconnect policy, and graceful shutdown details.
