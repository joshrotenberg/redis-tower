//! Live Redis coverage for the distributed primitives.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use redis_server_wrapper::RedisServer;
use redis_tower::commands::Del;
use redis_tower::{MultiplexedClient, RedisConnection};
use redis_tower_primitives::{
    CountDownLatch, DelayedQueue, DistributedLock, ExpirableSemaphore, GcraRateLimiter,
    IdGenerator, LatchCountDown, LatchWaitOutcome, LeaderElection, LeadershipEvent,
    LeadershipOutcome, RenewalOutcome,
};
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

fn election(ttl: Duration, renewal_interval: Duration) -> LeaderElection {
    let id = NEXT_KEY.fetch_add(1, Ordering::Relaxed);
    LeaderElection::new(format!("primitives:{id}:leader"), ttl, renewal_interval).unwrap()
}

fn semaphore(limit: u32, ttl: Duration) -> ExpirableSemaphore {
    let id = NEXT_KEY.fetch_add(1, Ordering::Relaxed);
    ExpirableSemaphore::new(format!("primitives:{id}:semaphore"), limit, ttl).unwrap()
}

fn latch(count: u64, ttl: Duration) -> CountDownLatch {
    let id = NEXT_KEY.fetch_add(1, Ordering::Relaxed);
    CountDownLatch::new(format!("primitives:{id}:latch"), count, ttl).unwrap()
}

fn delayed_queue(retention: Duration) -> DelayedQueue {
    let id = NEXT_KEY.fetch_add(1, Ordering::Relaxed);
    DelayedQueue::new(format!("primitives:{id}:delayed"), retention).unwrap()
}

fn id_generator(block_size: u64) -> IdGenerator {
    let id = NEXT_KEY.fetch_add(1, Ordering::Relaxed);
    IdGenerator::new(format!("primitives:{id}:ids"), block_size).unwrap()
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

#[tokio::test]
async fn leader_election_is_exclusive_and_abdication_is_observable() {
    let election = election(Duration::from_millis(120), Duration::from_millis(25));
    let owner = MultiplexedClient::connect(redis_addr().await)
        .await
        .unwrap();
    let contender = MultiplexedClient::connect(redis_addr().await)
        .await
        .unwrap();

    let campaign = election
        .campaign(owner)
        .await
        .unwrap()
        .expect("first candidate should be elected");
    let (leadership, mut events) = campaign.into_parts();
    assert_eq!(events.recv().await, Some(LeadershipEvent::Elected));
    assert!(election.campaign(contender).await.unwrap().is_none());

    assert!(matches!(
        leadership.abdicate().await.unwrap(),
        LeadershipOutcome::Abdicated
    ));
    assert_eq!(events.recv().await, Some(LeadershipEvent::Demoted));

    let replacement_client = MultiplexedClient::connect(redis_addr().await)
        .await
        .unwrap();
    let replacement = election
        .campaign(replacement_client)
        .await
        .unwrap()
        .expect("abdication should make the election immediately available");
    let (replacement, _) = replacement.into_parts();
    assert!(matches!(
        replacement.abdicate().await.unwrap(),
        LeadershipOutcome::Abdicated
    ));
}

#[tokio::test]
async fn dropping_leadership_requests_abdication() {
    let election = election(Duration::from_millis(150), Duration::from_millis(30));
    let owner = MultiplexedClient::connect(redis_addr().await)
        .await
        .unwrap();
    let campaign = election.campaign(owner).await.unwrap().unwrap();
    let (leadership, mut events) = campaign.into_parts();
    assert_eq!(events.recv().await, Some(LeadershipEvent::Elected));

    drop(leadership);
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(200), events.recv())
            .await
            .expect("drop abdication should finish before the TTL"),
        Some(LeadershipEvent::Demoted)
    );

    let replacement_client = MultiplexedClient::connect(redis_addr().await)
        .await
        .unwrap();
    let replacement = election
        .campaign(replacement_client)
        .await
        .unwrap()
        .expect("drop abdication should release the election key");
    let (replacement, _) = replacement.into_parts();
    let _ = replacement.abdicate().await.unwrap();
}

#[tokio::test]
async fn semaphore_enforces_capacity_and_release_recovers_it() {
    let semaphore = semaphore(2, Duration::from_millis(200));
    let mut first_connection = connection().await;
    let mut second_connection = connection().await;

    let first = semaphore
        .try_acquire(&mut first_connection)
        .await
        .unwrap()
        .expect("first permit should be available");
    let second = semaphore
        .try_acquire(&mut second_connection)
        .await
        .unwrap()
        .expect("second permit should be available");
    assert_eq!(first.remaining_at_acquire(), 1);
    assert_eq!(second.remaining_at_acquire(), 0);
    assert!(
        semaphore
            .try_acquire(&mut first_connection)
            .await
            .unwrap()
            .is_none()
    );

    assert!(first.release(&mut first_connection).await.unwrap());
    let replacement = semaphore
        .try_acquire(&mut first_connection)
        .await
        .unwrap()
        .expect("released capacity should be immediately reusable");
    assert!(second.release(&mut second_connection).await.unwrap());
    assert!(replacement.release(&mut first_connection).await.unwrap());
}

#[tokio::test]
async fn semaphore_expiry_recovers_from_crashed_holder() {
    let semaphore = semaphore(1, Duration::from_millis(60));
    let mut stale_connection = connection().await;
    let mut replacement_connection = connection().await;
    let stale = semaphore
        .try_acquire(&mut stale_connection)
        .await
        .unwrap()
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    let replacement = semaphore
        .try_acquire(&mut replacement_connection)
        .await
        .unwrap()
        .expect("expired holder should no longer consume capacity");
    assert!(!stale.renew(&mut stale_connection).await.unwrap());
    assert!(!stale.release(&mut stale_connection).await.unwrap());
    assert!(
        replacement
            .renew(&mut replacement_connection)
            .await
            .unwrap()
    );
    assert!(
        replacement
            .release(&mut replacement_connection)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn latch_initializes_once_and_waits_for_release() {
    let latch = latch(2, Duration::from_secs(1));
    let mut waiter_connection = connection().await;
    let mut countdown_connection = connection().await;
    assert!(latch.initialize(&mut waiter_connection).await.unwrap());
    assert!(!latch.initialize(&mut waiter_connection).await.unwrap());
    assert_eq!(
        latch.count_down(&mut countdown_connection).await.unwrap(),
        LatchCountDown::Waiting { remaining: 1 }
    );

    let countdown_latch = latch.clone();
    let countdown = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        countdown_latch
            .count_down(&mut countdown_connection)
            .await
            .unwrap()
    });
    assert_eq!(
        latch
            .wait(
                &mut waiter_connection,
                Duration::from_millis(5),
                Duration::from_millis(300),
            )
            .await
            .unwrap(),
        LatchWaitOutcome::Released
    );
    assert_eq!(countdown.await.unwrap(), LatchCountDown::Released);
    assert_eq!(
        latch.current(&mut waiter_connection).await.unwrap(),
        Some(0)
    );
}

#[tokio::test]
async fn latch_distinguishes_timeout_from_expiry() {
    let timed = latch(1, Duration::from_secs(1));
    let mut timed_connection = connection().await;
    assert!(timed.initialize(&mut timed_connection).await.unwrap());
    assert_eq!(
        timed
            .wait(
                &mut timed_connection,
                Duration::from_millis(5),
                Duration::from_millis(30),
            )
            .await
            .unwrap(),
        LatchWaitOutcome::TimedOut { remaining: 1 }
    );

    let expiring = latch(1, Duration::from_millis(40));
    let mut expiring_connection = connection().await;
    assert!(expiring.initialize(&mut expiring_connection).await.unwrap());
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert_eq!(
        expiring
            .wait(
                &mut expiring_connection,
                Duration::from_millis(5),
                Duration::from_millis(100),
            )
            .await
            .unwrap(),
        LatchWaitOutcome::Expired
    );
}

#[tokio::test]
async fn delayed_queue_preserves_binary_duplicates_and_deadlines() {
    let queue = delayed_queue(Duration::from_secs(1));
    let mut connection = connection().await;
    let duplicate = [0, 255, 42];

    let first_deadline = queue
        .enqueue(&mut connection, duplicate, Duration::ZERO)
        .await
        .unwrap();
    let second_deadline = queue
        .enqueue(&mut connection, duplicate, Duration::ZERO)
        .await
        .unwrap();
    let later_deadline = queue
        .enqueue(&mut connection, b"later", Duration::from_millis(80))
        .await
        .unwrap();
    assert!(second_deadline >= first_deadline);
    assert!(later_deadline > second_deadline);

    let first = queue.claim_due(&mut connection, 1).await.unwrap();
    let second = queue.claim_due(&mut connection, 1).await.unwrap();
    assert_eq!(first.expired(), 0);
    assert_eq!(first.payloads(), &[duplicate.to_vec()]);
    assert_eq!(second.payloads(), &[duplicate.to_vec()]);
    assert!(
        queue
            .claim_due(&mut connection, 10)
            .await
            .unwrap()
            .payloads()
            .is_empty()
    );

    tokio::time::sleep(Duration::from_millis(110)).await;
    assert_eq!(
        queue
            .claim_due(&mut connection, 10)
            .await
            .unwrap()
            .into_payloads(),
        vec![b"later".to_vec()]
    );
}

#[tokio::test]
async fn delayed_queue_reports_retention_pruning() {
    let queue = delayed_queue(Duration::from_millis(50));
    let mut connection = connection().await;
    queue
        .enqueue(&mut connection, b"stale", Duration::ZERO)
        .await
        .unwrap();
    queue
        .enqueue(&mut connection, b"future", Duration::from_millis(200))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(80)).await;
    let claim = queue.claim_due(&mut connection, 10).await.unwrap();
    assert_eq!(claim.expired(), 1);
    assert!(claim.payloads().is_empty());

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        queue
            .claim_due(&mut connection, 10)
            .await
            .unwrap()
            .into_payloads(),
        vec![b"future".to_vec()]
    );
}

#[tokio::test]
async fn id_generator_allocates_disjoint_local_blocks() {
    let generator = id_generator(3);
    let mut first_connection = connection().await;
    let mut second_connection = connection().await;
    first_connection
        .execute(Del::new(generator.key()))
        .await
        .unwrap();

    let first = generator.allocate(&mut first_connection).await.unwrap();
    assert_eq!(first.first_id(), 1);
    assert_eq!(first.last_id(), 3);
    assert_eq!(first.collect::<Vec<_>>(), vec![1, 2, 3]);

    let second = generator.allocate(&mut second_connection).await.unwrap();
    assert_eq!(second.first_id(), 4);
    assert_eq!(second.last_id(), 6);
    assert_eq!(second.collect::<Vec<_>>(), vec![4, 5, 6]);
}
