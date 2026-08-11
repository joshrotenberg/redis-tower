# Pool health probing design

Status: accepted for issues #483 and #512.

## Context

`ConnectionPool` already supports a lazy PING before using a connection that
has been idle longer than `PoolConfig::health_check_interval`. That check is
traffic-driven: it cannot report or repair a dead connection while the
application is idle.

An earlier experiment at commits `4025d1f` and `869556d` integrated
`tower-resilience-healthcheck`. The recovered design and review material show
that package was built as a generic, health-aware multi-resource selector. It
owned resource status, selection policy, thresholds, callbacks, and a
background task. The pool subsequently developed different invariants:

- each connection has a mutex and an in-flight reservation;
- a retained `PoolFactory` can replace or add slots;
- dispatch, acquisition deadlines, draining, and metrics are pool-native;
- repository policy requires background work to be explicitly spawned and
  stopped by dropping an owned handle.

Putting the generic wrapper around the pool would duplicate selection state
and would not let a probe safely coordinate with slot replacement or idle
reaping. Putting each pool slot inside the wrapper would make two components
compete to own dispatch and lifecycle state.

## Decision

Active pool probing is implemented inside `redis-tower` rather than by adding
`tower-resilience-healthcheck` as a dependency.

The public surface has two layers:

1. `HealthProbe<S>` is a small async trait for one connection. Built-in PING,
   ROLE, and INFO-replication-lag probes cover the Redis-specific checks. The
   byte-lag probe targets a primary: it compares the primary offset with every
   directly connected replica offset reported by primary-side INFO. It treats
   no connected replicas, non-online replicas, and replica-local INFO as
   unhealthy rather than inventing a zero-byte lag.
2. `ConnectionPool::spawn_health_prober` and
   `ConnectionPool::spawn_health_prober_with` are the only APIs that create a
   prober task. Both return `HealthProberHandle`; dropping the handle cancels
   the task, while `shutdown` cancels and joins it.

Creating a pool never starts a task. The existing lazy health check remains
available for request-path validation and replacement. The active prober is
observational: it updates per-slot state, `PoolStats`, and `MetricsRecorder`,
but it does not silently change dispatch policy. Applications that need to
stop sending work after an unhealthy result should compose the pool with the
existing circuit breaker.

Dynamic pool shrinking follows the same lifecycle rule. Configuring an idle
timeout does not start work; `spawn_idle_reaper` returns an owned
`IdleReaperHandle`. Reaping only removes idle slots above the configured
minimum. Scale-up happens synchronously on acquisition contention, so it does
not require a background task.

## Consequences

- Probe and reaper tasks share the pool's slot synchronization and cannot
  invalidate an in-flight connection.
- Existing fixed-size configurations remain fixed by default (`min == max ==
  size`). Dynamic sizing is opt-in through explicit bounds and requires a
  retained `PoolFactory`.
- A lazy factory-backed pool can start at zero connections and creates its
  first slot on the first command.
- The library owns a small amount of Redis-specific parsing for ROLE and INFO,
  but avoids a second resource-selection model and an additional dependency.
- Health state is intentionally telemetry, not a second circuit breaker.
