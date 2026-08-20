# Distributed primitives

The `redis-tower-primitives` crate builds locks, leader election, expirable
semaphores, countdown latches, and shared rate limits on the generic
`RedisExecutor` interface. It works with standalone, multiplexed, pooled,
resilient, cluster, Sentinel, and universal clients that implement that
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

## Leader election

`LeaderElection::new(key, ttl, renewal_interval)` requires both timing choices.
No task starts during construction. A successful `campaign(executor)` call
atomically claims the single election key and returns a `Campaign` containing
the owned `Leadership` renewal handle and a separate `LeadershipEvents`
receiver.

```rust,ignore
use std::time::Duration;
use redis_tower::MultiplexedClient;
use redis_tower_primitives::{LeaderElection, LeadershipEvent};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let candidate = MultiplexedClient::connect("127.0.0.1:6379").await?;
let election = LeaderElection::new(
    "{scheduler}:leader",
    Duration::from_secs(15),
    Duration::from_secs(5),
)?;

if let Some(campaign) = election.campaign(candidate).await? {
    let (leadership, mut events) = campaign.into_parts();
    assert_eq!(events.recv().await, Some(LeadershipEvent::Elected));
    run_one_scheduler_term().await?;
    let _outcome = leadership.abdicate().await?;
    assert_eq!(events.recv().await, Some(LeadershipEvent::Demoted));
}
# Ok(())
# }
# async fn run_one_scheduler_term() -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
```

`LeadershipEvent::RenewalFailed` means the process must stop leader-only work
immediately. `LeadershipEvent::Demoted` reports expiration, replacement, or
completed abdication. `abdicate().await` confirms cleanup. Dropping leadership
requests the same compare-and-delete operation in the detached owned task, and
the TTL bounds the key if that best-effort cleanup cannot run.

### Leader-election failure mode

A TTL lease cannot prove that a paused or partitioned process has stopped.
Redis failover can also lose an unreplicated campaign or renewal. Applications
must make leader work idempotent or protect downstream writes with another
stale-owner mechanism. A lost campaign response is indeterminate, so the caller
must not infer election without a successful return and `Elected` event.

## Expirable semaphore

`ExpirableSemaphore::new(key, permit_limit, ttl)` stores random permit tokens in
one sorted set. Acquisition uses Redis `TIME`, removes expired holders, and
checks capacity atomically. A `SemaphorePermit` supports token-checked `renew`
and `release`; drop leaves recovery to the required TTL.

```rust,ignore
use std::time::Duration;
use redis_tower::MultiplexedClient;
use redis_tower_primitives::ExpirableSemaphore;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut client = MultiplexedClient::connect("127.0.0.1:6379").await?;
let semaphore = ExpirableSemaphore::new(
    "{worker-pool}:permits",
    16,
    Duration::from_secs(30),
)?;

if let Some(permit) = semaphore.try_acquire(&mut client).await? {
    run_bounded_work().await?;
    permit.release(&mut client).await?;
}
# Ok(())
# }
# async fn run_bounded_work() -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
```

All users of a semaphore key must agree on its limit and TTL. There is no
automatic permit-renewal task; the holder chooses whether and when to call
`renew`.

### Semaphore failure mode

Expiry recovers capacity after a crash, but a paused holder can resume after a
replacement acquires the freed permit. The protected operation must tolerate
stale overlap. Lost acquire, renew, and release responses are indeterminate and
must not be blindly retried when duplicate bounded work would be unsafe.

## Countdown latch

`CountDownLatch::new(key, initial_count, ttl)` requires a positive count and an
absolute TTL. `initialize` never resets an existing latch. `count_down` uses
`DECR` without allowing the count below zero. `wait` requires a poll interval
and timeout, starts no task, and reports count-zero release, expiry, and timeout
as distinct outcomes. Its timeout bounds polling sleeps but does not cancel an
in-flight Redis request.

```rust,ignore
use std::time::Duration;
use redis_tower::MultiplexedClient;
use redis_tower_primitives::{CountDownLatch, LatchWaitOutcome};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut client = MultiplexedClient::connect("127.0.0.1:6379").await?;
let latch = CountDownLatch::new("{deploy:42}:ready", 3, Duration::from_secs(60))?;
latch.initialize(&mut client).await?;

match latch
    .wait(
        &mut client,
        Duration::from_millis(100),
        Duration::from_secs(30),
    )
    .await?
{
    LatchWaitOutcome::Released => start_traffic().await?,
    LatchWaitOutcome::Expired | LatchWaitOutcome::TimedOut { .. } => abort_deploy().await?,
}
# Ok(())
# }
# async fn start_traffic() -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
# async fn abort_deploy() -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
```

### Latch failure mode

The TTL is an abandonment bound, not a successful release signal. Waiters
receive `Expired` for a missing key and must decide how to recover. A lost
countdown response is indeterminate because the decrement may already have
committed; retrying can release the latch earlier than intended.

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
