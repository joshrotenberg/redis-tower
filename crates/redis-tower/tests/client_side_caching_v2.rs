use std::net::TcpListener;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use redis_server_wrapper::{RedisServer, RedisServerHandle};
use redis_tower::commands::{
    ClientCaching, ClientKill, ClientList, ClientTracking, Get, Reset, Set,
};
use redis_tower::{
    CacheTrackingMode, CachedClient, CachedClientConfig, CachedMultiplexedClient, Pipeline,
    RedisConnection,
};
use tokio::sync::OnceCell;
use tokio::time::{sleep, timeout};

const EVENTUALLY_TIMEOUT: Duration = Duration::from_secs(3);
const POLL_INTERVAL: Duration = Duration::from_millis(5);

static REDIS: OnceCell<(RedisServerHandle, String)> = OnceCell::const_new();
static NEXT_KEY: AtomicU64 = AtomicU64::new(0);

fn reserve_redis_port() -> u16 {
    // redis-server-wrapper uses port 0 to disable the plaintext listener, so
    // reserve an ephemeral host port and hand that port to the test server.
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .expect("reserve an ephemeral port for the caching test server");
    listener
        .local_addr()
        .expect("read the reserved caching test address")
        .port()
}

async fn start_redis() -> (RedisServerHandle, String) {
    let server = RedisServer::new()
        .port(reserve_redis_port())
        .save(false)
        .start()
        .await
        .expect("start the caching test server");
    let addr = server.addr();
    (server, addr)
}

async fn redis_addr() -> &'static str {
    &REDIS.get_or_init(start_redis).await.1
}

fn unique_key(scope: &str) -> String {
    let id = NEXT_KEY.fetch_add(1, Ordering::Relaxed);
    format!("redis-tower:csc-v2:{scope}:{}:{id}", std::process::id())
}

fn tracked_config(mode: CacheTrackingMode) -> CachedClientConfig {
    CachedClientConfig::new()
        .max_entries(128)
        .client_ttl(None)
        .tracking_mode(mode)
}

async fn connect_cached(addr: &str, mode: CacheTrackingMode) -> CachedMultiplexedClient {
    CachedMultiplexedClient::connect_with_config(addr, tracked_config(mode))
        .await
        .expect("connect cached multiplexed client")
}

async fn wait_for_value(client: &CachedMultiplexedClient, key: &str, expected: &[u8]) {
    timeout(EVENTUALLY_TIMEOUT, async {
        loop {
            let value: Option<Bytes> = client
                .execute(Get::new(key))
                .await
                .expect("read while waiting for an invalidation");
            if value.as_deref() == Some(expected) {
                return;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {key} to be invalidated"));
}

async fn shutdown(client: CachedMultiplexedClient) {
    timeout(EVENTUALLY_TIMEOUT, client.shutdown())
        .await
        .expect("cached multiplexed client did not shut down gracefully");
}

async fn shutdown_serialized(client: CachedClient) {
    timeout(EVENTUALLY_TIMEOUT, client.shutdown())
        .await
        .expect("serialized cached client did not shut down gracefully");
}

fn client_list_number(line: &str, field_name: &str) -> Option<i64> {
    line.split_ascii_whitespace().find_map(|field| {
        let (name, value) = field.split_once('=')?;
        (name == field_name).then(|| value.parse().ok()).flatten()
    })
}

async fn only_tracking_redirect(admin: &mut RedisConnection) -> (i64, i64) {
    let clients = admin
        .execute(ClientList::new())
        .await
        .expect("inspect Redis clients");
    let clients = String::from_utf8_lossy(&clients);
    let redirects: Vec<_> = clients
        .lines()
        .filter_map(|line| {
            let data_id = client_list_number(line, "id")?;
            let receiver_id = client_list_number(line, "redir")?;
            (receiver_id > 0).then_some((data_id, receiver_id))
        })
        .collect();

    assert_eq!(
        redirects.len(),
        1,
        "isolated Redis server should have exactly one tracking redirect; CLIENT LIST:\n{clients}"
    );
    let redirect = redirects[0];
    assert!(
        clients
            .lines()
            .any(|line| { client_list_number(line, "id") == Some(redirect.1) }),
        "tracking redirect target {} was absent from CLIENT LIST:\n{clients}",
        redirect.1
    );
    redirect
}

async fn wait_for_tracking_health(client: &CachedMultiplexedClient, expected: bool) {
    timeout(EVENTUALLY_TIMEOUT, async {
        loop {
            if client.is_caching_healthy().await == expected {
                return;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for caching health to become {expected}"));
}

async fn wait_for_serialized_tracking_health(client: &CachedClient, expected: bool) {
    timeout(EVENTUALLY_TIMEOUT, async {
        loop {
            if client.is_caching_healthy().await == expected {
                return;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("timed out waiting for serialized caching health to become {expected}")
    });
}

async fn exercise_tracking_mode(mode: CacheTrackingMode, scope: &str) {
    let addr = redis_addr().await;
    let key = unique_key(scope);
    let mut writer = RedisConnection::connect(addr)
        .await
        .expect("connect external writer");
    writer
        .execute(Set::new(&key, "before"))
        .await
        .expect("seed tracked key");

    let client = connect_cached(addr, mode).await;
    assert!(client.is_caching_healthy().await);

    let first: Option<Bytes> = client.execute(Get::new(&key)).await.expect("initial GET");
    assert_eq!(first.as_deref(), Some(b"before".as_slice()));
    let after_miss = client.cache_statistics().await;

    let repeated: Option<Bytes> = client.execute(Get::new(&key)).await.expect("cached GET");
    assert_eq!(repeated.as_deref(), Some(b"before".as_slice()));
    let after_hit = client.cache_statistics().await;
    assert_eq!(
        after_hit.misses, after_miss.misses,
        "repeated GET unexpectedly reached Redis"
    );
    assert!(
        after_hit.hits > after_miss.hits,
        "repeated GET was not cached"
    );

    writer
        .execute(Set::new(&key, "after"))
        .await
        .expect("mutate tracked key externally");
    wait_for_value(&client, &key, b"after").await;

    let after_invalidation = client.cache_statistics().await;
    assert!(
        after_invalidation.invalidations > after_hit.invalidations,
        "external mutation did not produce an invalidation"
    );

    let before_refill_hit = client.cache_statistics().await;
    let repeated: Option<Bytes> = client
        .execute(Get::new(&key))
        .await
        .expect("GET after cache refill");
    assert_eq!(repeated.as_deref(), Some(b"after".as_slice()));
    let after_refill_hit = client.cache_statistics().await;
    assert!(after_refill_hit.hits > before_refill_hit.hits);

    shutdown(client).await;
}

#[tokio::test]
async fn clones_share_cache_hits_and_shutdown_gracefully() {
    let addr = redis_addr().await;
    let key = unique_key("clones");
    let mut writer = RedisConnection::connect(addr)
        .await
        .expect("connect external writer");
    writer
        .execute(Set::new(&key, "shared"))
        .await
        .expect("seed clone test key");

    let client = connect_cached(addr, CacheTrackingMode::broadcast()).await;
    let warm: Option<Bytes> = client.execute(Get::new(&key)).await.expect("warm cache");
    assert_eq!(warm.as_deref(), Some(b"shared".as_slice()));
    let before = client.cache_statistics().await;

    let mut tasks = Vec::new();
    for _ in 0..16 {
        let client = client.clone();
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            let value: Option<Bytes> = client.execute(Get::new(key)).await.expect("cloned GET");
            assert_eq!(value.as_deref(), Some(b"shared".as_slice()));
        }));
    }
    for task in tasks {
        task.await.expect("cloned cache task panicked");
    }

    let after = client.cache_statistics().await;
    assert!(
        after.hits >= before.hits + 16,
        "all clone requests should hit the shared cache"
    );
    assert_eq!(client.cache_size().await, 1);

    shutdown(client).await;
}

#[tokio::test]
async fn local_writes_are_immediately_read_your_own_writes_safe() {
    let addr = redis_addr().await;
    let key = unique_key("read-your-own-writes");
    let client = connect_cached(addr, CacheTrackingMode::broadcast()).await;

    client
        .execute(Set::new(&key, "before"))
        .await
        .expect("write initial value");
    let initial: Option<Bytes> = client.execute(Get::new(&key)).await.expect("cache value");
    assert_eq!(initial.as_deref(), Some(b"before".as_slice()));
    let cached: Option<Bytes> = client.execute(Get::new(&key)).await.expect("hit cache");
    assert_eq!(cached.as_deref(), Some(b"before".as_slice()));
    let before_write = client.cache_statistics().await;

    client
        .execute(Set::new(&key, "after"))
        .await
        .expect("replace value through cached client");
    assert_eq!(client.cache_size().await, 0);

    let updated: Option<Bytes> = client
        .execute(Get::new(&key))
        .await
        .expect("read value immediately after local write");
    assert_eq!(updated.as_deref(), Some(b"after".as_slice()));
    let after_write = client.cache_statistics().await;
    assert!(after_write.misses > before_write.misses);
    assert!(after_write.invalidations > before_write.invalidations);

    shutdown(client).await;
}

#[tokio::test]
async fn prefix_broadcast_tracking_observes_external_invalidation() {
    let addr = redis_addr().await;
    let prefix = format!("{}:", unique_key("broadcast-prefix"));
    let key = format!("{prefix}tracked");
    let outside_key = unique_key("outside-broadcast-prefix");
    let mut writer = RedisConnection::connect(addr)
        .await
        .expect("connect external writer");
    writer
        .execute(Set::new(&key, "before"))
        .await
        .expect("seed broadcast key");
    writer
        .execute(Set::new(&outside_key, "outside-before"))
        .await
        .expect("seed key outside the broadcast prefix");

    let mode = CacheTrackingMode::broadcast_with_prefixes([Bytes::from(prefix)]);
    let client = connect_cached(addr, mode).await;
    let first: Option<Bytes> = client.execute(Get::new(&key)).await.expect("initial GET");
    assert_eq!(first.as_deref(), Some(b"before".as_slice()));
    let cached: Option<Bytes> = client.execute(Get::new(&key)).await.expect("cached GET");
    assert_eq!(cached.as_deref(), Some(b"before".as_slice()));

    let before_outside_reads = client.cache_statistics().await;
    let cache_size_before_outside_reads = client.cache_size().await;
    for _ in 0..2 {
        let outside: Option<Bytes> = client
            .execute(Get::new(&outside_key))
            .await
            .expect("read key outside the broadcast prefix");
        assert_eq!(outside.as_deref(), Some(b"outside-before".as_slice()));
    }
    assert_eq!(
        client.cache_statistics().await,
        before_outside_reads,
        "outside-prefix reads must bypass local cache accounting"
    );
    assert_eq!(
        client.cache_size().await,
        cache_size_before_outside_reads,
        "outside-prefix reads must not populate the local cache"
    );

    writer
        .execute(Set::new(&outside_key, "outside-after"))
        .await
        .expect("mutate key outside the broadcast prefix");
    let outside_after_write: Option<Bytes> = client
        .execute(Get::new(&outside_key))
        .await
        .expect("read outside-prefix key immediately after external mutation");
    assert_eq!(
        outside_after_write.as_deref(),
        Some(b"outside-after".as_slice()),
        "outside-prefix read was served from a stale local entry"
    );
    assert_eq!(client.cache_statistics().await, before_outside_reads);
    assert_eq!(client.cache_size().await, cache_size_before_outside_reads);

    writer
        .execute(Set::new(&key, "after"))
        .await
        .expect("mutate broadcast key externally");
    wait_for_value(&client, &key, b"after").await;

    shutdown(client).await;
}

#[tokio::test]
async fn default_tracking_caches_reads_and_observes_external_invalidation() {
    exercise_tracking_mode(CacheTrackingMode::ServerDefault, "default").await;
}

#[tokio::test]
async fn opt_in_tracking_caches_reads_and_observes_external_invalidation() {
    exercise_tracking_mode(CacheTrackingMode::OptIn, "opt-in").await;
}

#[tokio::test]
async fn managed_connection_state_commands_are_rejected_without_losing_tracking() {
    let addr = redis_addr().await;
    let key = unique_key("managed-state");
    let mut writer = RedisConnection::connect(addr)
        .await
        .expect("connect external writer");
    writer
        .execute(Set::new(&key, "before"))
        .await
        .expect("seed managed-state key");

    let client = connect_cached(addr, CacheTrackingMode::broadcast()).await;
    let warm: Option<Bytes> = client.execute(Get::new(&key)).await.expect("warm cache");
    assert_eq!(warm.as_deref(), Some(b"before".as_slice()));

    for error in [
        client.execute(ClientTracking::off()).await.unwrap_err(),
        client.execute(ClientCaching::new(true)).await.unwrap_err(),
        client.execute(Reset::new()).await.unwrap_err(),
    ] {
        assert!(error.to_string().contains("managed internally"));
    }

    let mut pipeline_client = client.clone();
    let pipeline_error = match Pipeline::new()
        .push(ClientTracking::off())
        .execute(&mut pipeline_client)
        .await
    {
        Ok(_) => panic!("managed state command unexpectedly reached a pipeline"),
        Err(error) => error,
    };
    assert!(pipeline_error.to_string().contains("managed internally"));

    writer
        .execute(Set::new(&key, "after"))
        .await
        .expect("mutate after rejected state commands");
    wait_for_value(&client, &key, b"after").await;
    assert!(client.is_caching_healthy().await);

    shutdown(client).await;
}

#[tokio::test]
async fn tracking_receiver_loss_fails_closed_and_reinstalls_redirect() {
    // Use a dedicated server so CLIENT KILL cannot target another concurrently
    // running cache test's invalidation receiver.
    let (_server, addr) = start_redis().await;
    let key = unique_key("tracking-recovery");
    let mut writer = RedisConnection::connect(&addr)
        .await
        .expect("connect external writer");
    let mut admin = RedisConnection::connect(&addr)
        .await
        .expect("connect administrative client");
    writer
        .execute(Set::new(&key, "before"))
        .await
        .expect("seed recovery key");

    let client = connect_cached(
        &addr,
        CacheTrackingMode::broadcast_with_prefixes([Bytes::from(key.clone())]),
    )
    .await;
    let warm: Option<Bytes> = client.execute(Get::new(&key)).await.expect("warm cache");
    assert_eq!(warm.as_deref(), Some(b"before".as_slice()));
    let cached: Option<Bytes> = client.execute(Get::new(&key)).await.expect("hit cache");
    assert_eq!(cached.as_deref(), Some(b"before".as_slice()));
    assert_eq!(client.cache_size().await, 1);

    let (data_id, receiver_id) = only_tracking_redirect(&mut admin).await;
    let killed = admin
        .execute(ClientKill::new().id(receiver_id))
        .await
        .expect("kill invalidation receiver");
    assert_eq!(killed, 1, "expected to kill only the invalidation receiver");

    wait_for_tracking_health(&client, false).await;
    assert_eq!(
        client.cache_size().await,
        0,
        "receiver loss must clear cached responses before recovery"
    );

    writer
        .execute(Set::new(&key, "during-outage"))
        .await
        .expect("mutate while invalidation tracking is unavailable");
    let during_outage: Option<Bytes> = client
        .execute(Get::new(&key))
        .await
        .expect("pass read through while tracking is unavailable");
    assert_eq!(during_outage.as_deref(), Some(b"during-outage".as_slice()));

    wait_for_tracking_health(&client, true).await;
    let (recovered_data_id, recovered_receiver_id) = only_tracking_redirect(&mut admin).await;
    assert_eq!(
        recovered_data_id, data_id,
        "tracking recovery unexpectedly replaced the data connection"
    );
    assert_ne!(
        recovered_receiver_id, receiver_id,
        "tracking recovery did not install a replacement invalidation receiver"
    );

    let refill: Option<Bytes> = client
        .execute(Get::new(&key))
        .await
        .expect("refill cache after tracking recovery");
    assert_eq!(refill.as_deref(), Some(b"during-outage".as_slice()));
    let before_recovered_hit = client.cache_statistics().await;
    let repeated: Option<Bytes> = client
        .execute(Get::new(&key))
        .await
        .expect("hit cache after tracking recovery");
    assert_eq!(repeated, refill);
    assert!(
        client.cache_statistics().await.hits > before_recovered_hit.hits,
        "recovered tracking path did not resume local cache hits"
    );

    writer
        .execute(Set::new(&key, "after-recovery"))
        .await
        .expect("mutate after tracking recovery");
    wait_for_value(&client, &key, b"after-recovery").await;

    shutdown(client).await;
}

#[tokio::test]
async fn multiplexed_data_connection_loss_clears_cache_without_a_followup_command() {
    // Use a dedicated server so the exact data connection can be killed
    // without disturbing another cache test.
    let (_server, addr) = start_redis().await;
    let key = unique_key("multiplexed-data-loss");
    let mut writer = RedisConnection::connect(&addr)
        .await
        .expect("connect external writer");
    let mut admin = RedisConnection::connect(&addr)
        .await
        .expect("connect administrative client");
    writer
        .execute(Set::new(&key, "before"))
        .await
        .expect("seed data-loss key");

    let client = connect_cached(&addr, CacheTrackingMode::broadcast()).await;
    let warm: Option<Bytes> = client.execute(Get::new(&key)).await.expect("warm cache");
    assert_eq!(warm.as_deref(), Some(b"before".as_slice()));
    let hit: Option<Bytes> = client.execute(Get::new(&key)).await.expect("hit cache");
    assert_eq!(hit, warm);
    let before_loss = client.cache_statistics().await;
    assert_eq!(client.cache_size().await, 1);

    let (data_id, _receiver_id) = only_tracking_redirect(&mut admin).await;
    let killed = admin
        .execute(ClientKill::new().id(data_id))
        .await
        .expect("kill cached data connection");
    assert_eq!(killed, 1, "expected to kill only the data connection");

    // These are local observations. No Redis-bound cached-client command is
    // needed to discover the idle socket loss and fail the cache closed.
    wait_for_tracking_health(&client, false).await;
    assert_eq!(
        client.cache_size().await,
        0,
        "data connection loss must clear cached responses while idle"
    );

    writer
        .execute(Set::new(&key, "after"))
        .await
        .expect("mutate after the cached data connection was lost");
    let error = client
        .execute(Get::new(&key))
        .await
        .expect_err("a fixed dead data worker must not serve or reconnect silently");
    assert!(error.is_connection_error());
    assert_eq!(
        client.cache_statistics().await.hits,
        before_loss.hits,
        "the post-loss read must not be counted as a cache hit"
    );

    shutdown(client).await;
}

#[tokio::test]
async fn serialized_data_connection_loss_clears_cache_without_a_followup_command() {
    // The compatibility client must share the same fail-closed actor lifecycle
    // as CachedMultiplexedClient rather than relying on a later request to
    // notice a dead socket.
    let (_server, addr) = start_redis().await;
    let key = unique_key("serialized-data-loss");
    let mut writer = RedisConnection::connect(&addr)
        .await
        .expect("connect external writer");
    let mut admin = RedisConnection::connect(&addr)
        .await
        .expect("connect administrative client");
    writer
        .execute(Set::new(&key, "before"))
        .await
        .expect("seed serialized data-loss key");

    let client =
        CachedClient::connect_with_config(&addr, tracked_config(CacheTrackingMode::broadcast()))
            .await
            .expect("connect serialized cached client");
    let warm: Option<Bytes> = client.execute(Get::new(&key)).await.expect("warm cache");
    assert_eq!(warm.as_deref(), Some(b"before".as_slice()));
    let hit: Option<Bytes> = client.execute(Get::new(&key)).await.expect("hit cache");
    assert_eq!(hit, warm);
    let before_loss = client.cache_statistics().await;
    assert_eq!(client.cache_size().await, 1);

    let (data_id, _receiver_id) = only_tracking_redirect(&mut admin).await;
    let killed = admin
        .execute(ClientKill::new().id(data_id))
        .await
        .expect("kill serialized cached data connection");
    assert_eq!(killed, 1, "expected to kill only the data connection");

    wait_for_serialized_tracking_health(&client, false).await;
    assert_eq!(
        client.cache_size().await,
        0,
        "serialized data connection loss must clear cached responses while idle"
    );

    writer
        .execute(Set::new(&key, "after"))
        .await
        .expect("mutate after the serialized data connection was lost");
    let error = client
        .execute(Get::new(&key))
        .await
        .expect_err("a fixed dead serialized worker must not serve stale data");
    assert!(error.is_connection_error());
    assert_eq!(
        client.cache_statistics().await.hits,
        before_loss.hits,
        "the serialized post-loss read must not be counted as a cache hit"
    );

    shutdown_serialized(client).await;
}

#[tokio::test]
async fn cache_capacity_and_client_ttl_bound_local_entries() {
    let addr = redis_addr().await;
    let capacity_prefix = format!("{}:", unique_key("capacity"));
    let keys = [
        format!("{capacity_prefix}a"),
        format!("{capacity_prefix}b"),
        format!("{capacity_prefix}c"),
    ];
    let mut writer = RedisConnection::connect(addr)
        .await
        .expect("connect external writer");
    for (index, key) in keys.iter().enumerate() {
        writer
            .execute(Set::new(key, index.to_string()))
            .await
            .expect("seed capacity key");
    }

    let bounded = CachedMultiplexedClient::connect_with_config(
        addr,
        CachedClientConfig::new()
            .max_entries(2)
            .client_ttl(None)
            .tracking_mode(CacheTrackingMode::broadcast_with_prefixes([Bytes::from(
                capacity_prefix,
            )])),
    )
    .await
    .expect("connect capacity-bounded client");
    for key in &keys {
        let _: Option<Bytes> = bounded.execute(Get::new(key)).await.expect("cache key");
    }
    assert_eq!(bounded.cache_size().await, 2);
    let bounded_stats = bounded.cache_statistics().await;
    assert!(bounded_stats.misses >= 3);
    assert!(bounded_stats.evictions >= 1);
    shutdown(bounded).await;

    let ttl_key = unique_key("ttl");
    writer
        .execute(Set::new(&ttl_key, "fresh"))
        .await
        .expect("seed TTL key");
    let ttl_client = CachedMultiplexedClient::connect_with_config(
        addr,
        CachedClientConfig::new()
            .max_entries(4)
            .client_ttl(Some(Duration::from_millis(25)))
            .tracking_mode(CacheTrackingMode::broadcast_with_prefixes([Bytes::from(
                ttl_key.clone(),
            )])),
    )
    .await
    .expect("connect TTL-bounded client");
    let _: Option<Bytes> = ttl_client
        .execute(Get::new(&ttl_key))
        .await
        .expect("warm TTL cache");
    let before_expiry = ttl_client.cache_statistics().await;

    let after_expiry = timeout(EVENTUALLY_TIMEOUT, async {
        loop {
            let value: Option<Bytes> = ttl_client
                .execute(Get::new(&ttl_key))
                .await
                .expect("poll TTL cache");
            assert_eq!(value.as_deref(), Some(b"fresh".as_slice()));
            let stats = ttl_client.cache_statistics().await;
            if stats.misses > before_expiry.misses {
                return stats;
            }
            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .expect("client TTL did not force a bounded refresh");
    assert!(after_expiry.evictions > before_expiry.evictions);

    shutdown(ttl_client).await;
}
