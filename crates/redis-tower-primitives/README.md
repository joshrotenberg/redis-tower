# redis-tower-primitives

Auditable distributed coordination primitives for `redis-tower`:

- `DistributedLock` uses `SET NX PX`, compare-and-delete release,
  compare-and-extend renewal, and a monotonic `INCR` fencing token.
- `GcraRateLimiter` uses Redis `TIME` and one sliding sorted-set key for
  shared-quota admission without client-clock skew.
- `LeaderElection` returns an owned renewal handle plus observable elected,
  renewal-failed, and demoted events.
- `ExpirableSemaphore` restores capacity when crashed holders' permit TTLs
  expire.
- `CountDownLatch` provides atomic countdown and an explicit polling wait with
  caller-selected timing.

Every Lua program is a documented public constant and executes through
`redis_tower::Script`, which tries EVALSHA before its NOSCRIPT fallback.

```rust,no_run
use std::time::Duration;

use redis_tower::MultiplexedClient;
use redis_tower_primitives::{DistributedLock, GcraRateLimiter};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut client = MultiplexedClient::connect("127.0.0.1:6379").await?;

let lock = DistributedLock::new(
    "{invoice:42}:lock",
    "{invoice:42}:fence",
    Duration::from_secs(10),
)?;
if let Some(lease) = lock.acquire(&mut client).await? {
    // The protected system must reject fencing tokens older than the greatest
    // token it has already accepted.
    update_invoice(lease.fencing_token()).await?;
    lease.release(&mut client).await?;
}

let limiter = GcraRateLimiter::new(
    "{tenant:7}:api-rate",
    100,
    Duration::from_secs(1),
)?;
let decision = limiter.check(&mut client).await?;
if !decision.is_allowed() {
    tokio::time::sleep(decision.retry_after()).await;
}
# Ok(())
# }
# async fn update_invoice(_fencing_token: u64) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
```

## Owned lifecycles

No task starts during lock construction or acquisition. Renewal starts only
when `LockLease::spawn_renewal` consumes a lease. The returned owned handle
stops renewal on drop; `shutdown().await` stops it cleanly and returns the lease
for explicit release.

Leader election likewise starts no task until `campaign()` succeeds. Its
`Leadership` handle owns renewal. `abdicate().await` performs and confirms
compare-and-delete cleanup; drop requests the same cleanup asynchronously and
the required TTL remains the final bound if cleanup cannot run. Leadership
events can be split from the handle for independent observation.

## Cluster keys

Leader election, semaphores, latches, and the rate limiter each touch one key
and are cluster-safe as-is. Lock acquisition touches two keys, so the lock and
fencing keys must share a Redis Cluster hash tag, such as
`{invoice:42}:lock` and `{invoice:42}:fence`.

## Failure model

An expiring Redis lock is not consensus. A process pause or network partition
can outlive its TTL, allowing a stale owner and a replacement owner to execute
concurrently. Pass the fencing token to the protected resource and make that
resource reject stale values. Redis failover can roll back unreplicated lock or
counter writes when persistence settings allow it.

Rate-limit Redis errors are returned to the caller; the crate never silently
chooses fail-open or fail-closed. A lost response is indeterminate because the
script may already have consumed quota. Pair the shared limiter with a local
[`tower-resilience-ratelimiter`](https://docs.rs/tower-resilience-ratelimiter)
`RateLimiterLayer` in backpressure mode when a process also needs admission
pressure before Redis.

Leadership and semaphore leases have the same pause/partition limitation as a
lock: expiry can admit a replacement while stale work resumes. Latch expiry is
reported separately from count-zero release. All connection failures remain
indeterminate, so callers must choose retry and stale-work policies explicitly.
