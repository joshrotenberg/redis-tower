# Distributed primitives

The `redis-tower-primitives` crate builds locks and shared rate limits on the
generic `RedisExecutor` interface. It works with standalone, multiplexed,
pooled, resilient, cluster, Sentinel, and universal clients that implement that
interface. Every script is a public, line-documented constant and runs through
the EVALSHA-first `Script` helper.

## Distributed lock

`DistributedLock::new(lock_key, fencing_key, ttl)` requires its TTL. Acquisition
atomically checks the lock, increments the fencing counter, and performs `SET
NX PX`. A lease can release or extend only while its random owner token still
matches.

```rust,ignore
use std::time::Duration;
use redis_tower::MultiplexedClient;
use redis_tower_primitives::DistributedLock;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut client = MultiplexedClient::connect("127.0.0.1:6379").await?;
let lock = DistributedLock::new(
    "{job:17}:lock",
    "{job:17}:fence",
    Duration::from_secs(15),
)?;

if let Some(lease) = lock.acquire(&mut client).await? {
    let fencing_token = lease.fencing_token();
    // The protected service must reject tokens lower than the greatest token
    // it has already accepted.
    write_with_fence(fencing_token).await?;
    lease.release(&mut client).await?;
}
# Ok(())
# }
# async fn write_with_fence(_token: u64) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
```

Lock acquisition touches two keys. In Redis Cluster they must share a hash tag,
as `{job:17}:lock` and `{job:17}:fence` do.

Renewal is opt-in. `LockLease::spawn_renewal` consumes the lease and starts a
task only at that explicit call. Its owned handle cancels and aborts renewal on
drop. `shutdown().await` stops cleanly and returns the lease for release.

### Lock failure mode

The TTL bounds ownership; it does not prove that a paused or partitioned owner
has stopped. Such an owner can resume after a higher-token replacement begins
work. The protected resource must enforce fencing tokens. A lost acquisition
response is indeterminate, and Redis failover can roll back an unreplicated
fencing increment if deployment durability permits it.

## GCRA rate limiter

`GcraRateLimiter::new(key, quota, window)` requires both quota and window. Its
single Lua call reads Redis `TIME`, prunes obsolete virtual arrivals from one
sorted set, and atomically admits or denies a request. A quota of 100 over one
second sustains 100 requests per second and permits an initial burst of 100.

```rust,ignore
use std::time::Duration;
use redis_tower::MultiplexedClient;
use redis_tower_primitives::GcraRateLimiter;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut client = MultiplexedClient::connect("127.0.0.1:6379").await?;
let limiter = GcraRateLimiter::new("{tenant:7}:rate", 100, Duration::from_secs(1))?;
let decision = limiter.check(&mut client).await?;
if !decision.is_allowed() {
    tokio::time::sleep(decision.retry_after()).await;
}
# Ok(())
# }
```

The script touches one key and is cluster-safe. A shared Redis limiter enforces
cross-process quota, while
[`tower-resilience-ratelimiter`](https://docs.rs/tower-resilience-ratelimiter)
can put its `RateLimiterLayer` in backpressure mode for process-local admission
pressure; production services commonly use both.

### Rate-limit failure mode

Redis failures are surfaced without a fail-open/fail-closed policy. Applications
must choose that policy at their boundary. A connection loss is indeterminate:
the request may have consumed a cell before the response disappeared, so an
automatic retry can double-consume quota. Failover can lose recent admissions
when Redis durability permits data loss.
