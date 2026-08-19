//! Live Redis coverage for the distributed primitives.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use redis_server_wrapper::RedisServer;
use redis_tower::commands::Del;
use redis_tower::{MultiplexedClient, RedisConnection};
use redis_tower_primitives::{DistributedLock, GcraRateLimiter, RenewalOutcome};
use tokio::sync::OnceCell;

static REDIS: OnceCell<redis_server_wrapper::RedisServerHandle> = OnceCell::const_new();
static REDIS_ADDR: OnceCell<String> = OnceCell::const_new();
static NEXT_KEY: AtomicU64 = AtomicU64::new(1);

async fn redis_addr() -> &'static str {
    REDIS_ADDR
        .get_or_init(|| async {
            if let Ok(url) = std::env::var("REDIS_URL") {
                return url
                    .strip_prefix("redis://")
                    .unwrap_or(&url)
                    .trim_end_matches('/')
                    .to_string();
            }

            let handle = RedisServer::new()
                .port(6408)
                .start()
                .await
                .expect("failed to start Redis server");
            let addr = handle.addr();
            REDIS.set(handle).ok();
            addr
        })
        .await
}

async fn connection() -> RedisConnection {
    RedisConnection::connect(redis_addr().await)
        .await
        .expect("failed to connect to Redis")
}

fn lock(ttl: Duration) -> DistributedLock {
    let id = NEXT_KEY.fetch_add(1, Ordering::Relaxed);
    DistributedLock::new(
        format!("{{primitives:{id}}}:lock"),
        format!("{{primitives:{id}}}:fence"),
        ttl,
    )
    .unwrap()
}

fn limiter(quota: u32, window: Duration) -> GcraRateLimiter {
    let id = NEXT_KEY.fetch_add(1, Ordering::Relaxed);
    GcraRateLimiter::new(format!("primitives:{id}:rate"), quota, window).unwrap()
}

#[tokio::test]
async fn lock_excludes_contenders_and_fencing_increases() {
    let lock = lock(Duration::from_secs(1));
    let mut first_connection = connection().await;
    let mut second_connection = connection().await;

    let first = lock
        .acquire(&mut first_connection)
        .await
        .unwrap()
        .expect("first owner should acquire");
    assert!(
        lock.acquire(&mut second_connection)
            .await
            .unwrap()
            .is_none()
    );
    assert!(first.release(&mut first_connection).await.unwrap());

    let second = lock
        .acquire(&mut second_connection)
        .await
        .unwrap()
        .expect("second owner should acquire after release");
    assert!(second.fencing_token() > first.fencing_token());
    assert!(second.release(&mut second_connection).await.unwrap());
}

#[tokio::test]
async fn stale_owner_cannot_release_or_extend_replacement() {
    let lock = lock(Duration::from_millis(60));
    let mut first_connection = connection().await;
    let mut second_connection = connection().await;

    let stale = lock
        .acquire(&mut first_connection)
        .await
        .unwrap()
        .expect("first owner should acquire");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let replacement = lock
        .acquire(&mut second_connection)
        .await
        .unwrap()
        .expect("replacement should acquire after expiration");

    assert!(replacement.fencing_token() > stale.fencing_token());
    assert!(!stale.extend(&mut first_connection).await.unwrap());
    assert!(!stale.release(&mut first_connection).await.unwrap());
    assert!(replacement.extend(&mut second_connection).await.unwrap());
    assert!(lock.acquire(&mut first_connection).await.unwrap().is_none());
    assert!(replacement.release(&mut second_connection).await.unwrap());
}

#[tokio::test]
async fn owned_renewal_keeps_lease_alive_and_returns_it_on_shutdown() {
    let lock = lock(Duration::from_millis(80));
    let mut owner_connection = connection().await;
    let mut contender_connection = connection().await;
    let renewal_client = MultiplexedClient::connect(redis_addr().await)
        .await
        .unwrap();

    let lease = lock
        .acquire(&mut owner_connection)
        .await
        .unwrap()
        .expect("owner should acquire");
    let fencing_token = lease.fencing_token();
    let renewal = lease
        .spawn_renewal(renewal_client, Duration::from_millis(20))
        .unwrap();

    tokio::time::sleep(Duration::from_millis(180)).await;
    assert_eq!(renewal.fencing_token(), fencing_token);
    assert!(!renewal.is_finished());
    assert!(
        lock.acquire(&mut contender_connection)
            .await
            .unwrap()
            .is_none()
    );

    let (lease, outcome) = renewal.shutdown().await.unwrap();
    assert!(matches!(outcome, RenewalOutcome::Stopped));
    assert!(lease.release(&mut owner_connection).await.unwrap());
}

#[tokio::test]
async fn dropping_renewal_stops_extending_the_lock() {
    let lock = lock(Duration::from_millis(70));
    let mut owner_connection = connection().await;
    let mut contender_connection = connection().await;
    let renewal_client = MultiplexedClient::connect(redis_addr().await)
        .await
        .unwrap();

    let lease = lock
        .acquire(&mut owner_connection)
        .await
        .unwrap()
        .expect("owner should acquire");
    let renewal = lease
        .spawn_renewal(renewal_client, Duration::from_millis(15))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(35)).await;
    drop(renewal);

    tokio::time::sleep(Duration::from_millis(110)).await;
    let replacement = lock
        .acquire(&mut contender_connection)
        .await
        .unwrap()
        .expect("lock should expire after renewal handle is dropped");
    assert!(
        replacement
            .release(&mut contender_connection)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn renewal_reports_ownership_loss() {
    let lock = lock(Duration::from_millis(100));
    let mut owner_connection = connection().await;
    let mut mutator_connection = connection().await;
    let renewal_client = MultiplexedClient::connect(redis_addr().await)
        .await
        .unwrap();

    let lease = lock
        .acquire(&mut owner_connection)
        .await
        .unwrap()
        .expect("owner should acquire");
    let renewal = lease
        .spawn_renewal(renewal_client, Duration::from_millis(20))
        .unwrap();
    mutator_connection
        .execute(Del::new(lock.lock_key()))
        .await
        .unwrap();

    let (stale, outcome) = renewal.wait().await.unwrap();
    assert!(matches!(outcome, RenewalOutcome::OwnershipLost));
    assert!(!stale.release(&mut owner_connection).await.unwrap());
}

#[tokio::test]
async fn gcra_enforces_shared_quota_and_reports_retry() {
    let limiter = limiter(3, Duration::from_secs(3));
    let mut first_connection = connection().await;
    let mut second_connection = connection().await;

    let first = limiter.check(&mut first_connection).await.unwrap();
    let second = limiter.check(&mut second_connection).await.unwrap();
    let third = limiter.check(&mut first_connection).await.unwrap();
    let denied = limiter.check(&mut second_connection).await.unwrap();

    assert!(first.is_allowed());
    assert!(second.is_allowed());
    assert!(third.is_allowed());
    assert_eq!(first.remaining(), 2);
    assert_eq!(third.remaining(), 0);
    assert!(!denied.is_allowed());
    assert_eq!(denied.remaining(), 0);
    assert!(denied.retry_after() > Duration::ZERO);
    assert!(denied.retry_after() <= Duration::from_secs(1));
    assert!(denied.reset_after() > denied.retry_after());
}

#[tokio::test]
async fn gcra_recovers_capacity_after_server_computed_delay() {
    let limiter = limiter(1, Duration::from_millis(80));
    let mut connection = connection().await;

    assert!(limiter.check(&mut connection).await.unwrap().is_allowed());
    let denied = limiter.check(&mut connection).await.unwrap();
    assert!(!denied.is_allowed());

    tokio::time::sleep(denied.retry_after() + Duration::from_millis(20)).await;
    assert!(limiter.check(&mut connection).await.unwrap().is_allowed());
}
