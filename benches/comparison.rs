//! Benchmark redis-tower vs fred
//!
//! Compares performance between:
//! - redis-tower: Our Tower-based, strongly-typed client
//! - fred: High-performance async Redis client
//!
//! Run with: cargo bench

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use fred::interfaces::ClientLike;
use tokio::runtime::Runtime;

// Test data
const KEY: &str = "benchmark_key";
const VALUE: &[u8] = b"benchmark_value_with_some_reasonable_length_to_simulate_real_usage";
const PIPELINE_SIZE: usize = 100;

// =============================================================================
// redis-tower benchmarks
// =============================================================================

async fn redis_tower_setup() -> redis_tower::RedisClient {
    redis_tower::RedisClient::connect("127.0.0.1:6379")
        .await
        .expect("Failed to connect redis-tower")
}

async fn redis_tower_get(client: &redis_tower::RedisClient) {
    use redis_tower::commands::Get;
    let _: Option<bytes::Bytes> = client.call(Get::new(KEY)).await.unwrap();
}

async fn redis_tower_set(client: &redis_tower::RedisClient) {
    use redis_tower::commands::Set;
    client.call(Set::new(KEY, VALUE)).await.unwrap();
}

async fn redis_tower_pipeline(client: &redis_tower::RedisClient) {
    use redis_tower::Pipeline;
    use redis_tower::commands::{Get, Set};

    let mut pipeline = Pipeline::with_capacity(PIPELINE_SIZE);
    for i in 0..PIPELINE_SIZE / 2 {
        pipeline.add(Set::new(format!("key_{}", i), VALUE));
    }
    for i in 0..PIPELINE_SIZE / 2 {
        pipeline.add(Get::new(format!("key_{}", i)));
    }

    let _results = pipeline.execute(client).await.unwrap();
}

// =============================================================================
// fred benchmarks
// =============================================================================

async fn fred_setup() -> fred::clients::RedisClient {
    let client = fred::clients::RedisClient::default();
    client.connect();
    client
        .wait_for_connect()
        .await
        .expect("Failed to connect fred");
    client
}

async fn fred_get(client: &fred::clients::RedisClient) {
    use fred::interfaces::KeysInterface;
    let _: Option<Vec<u8>> = client.get(KEY).await.unwrap();
}

async fn fred_set(client: &fred::clients::RedisClient) {
    use fred::interfaces::KeysInterface;
    let _: () = client.set(KEY, VALUE, None, None, false).await.unwrap();
}

async fn fred_pipeline(_client: &fred::clients::RedisClient) {
    // TODO: Figure out fred's pipeline API - it's more complex than expected
    // For now, just skip this benchmark
}

// =============================================================================
// Benchmark groups
// =============================================================================

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");
    let rt = Runtime::new().unwrap();

    // Setup clients
    let redis_tower_client = rt.block_on(redis_tower_setup());
    let fred_client = rt.block_on(fred_setup());

    // Set initial value
    rt.block_on(redis_tower_set(&redis_tower_client));

    group.bench_function("redis-tower", |b| {
        b.to_async(&rt)
            .iter(|| redis_tower_get(&redis_tower_client));
    });

    group.bench_function("fred", |b| {
        b.to_async(&rt).iter(|| fred_get(&fred_client));
    });

    group.finish();
}

fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("set");
    let rt = Runtime::new().unwrap();

    let redis_tower_client = rt.block_on(redis_tower_setup());
    let fred_client = rt.block_on(fred_setup());

    group.bench_function("redis-tower", |b| {
        b.to_async(&rt)
            .iter(|| redis_tower_set(&redis_tower_client));
    });

    group.bench_function("fred", |b| {
        b.to_async(&rt).iter(|| fred_set(&fred_client));
    });

    group.finish();
}

fn bench_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline");
    let rt = Runtime::new().unwrap();

    let redis_tower_client = rt.block_on(redis_tower_setup());
    let fred_client = rt.block_on(fred_setup());

    group.bench_with_input(
        BenchmarkId::new("redis-tower", PIPELINE_SIZE),
        &PIPELINE_SIZE,
        |b, _| {
            b.to_async(&rt)
                .iter(|| redis_tower_pipeline(&redis_tower_client));
        },
    );

    group.bench_with_input(
        BenchmarkId::new("fred", PIPELINE_SIZE),
        &PIPELINE_SIZE,
        |b, _| {
            b.to_async(&rt).iter(|| fred_pipeline(&fred_client));
        },
    );

    group.finish();
}

async fn redis_tower_mixed(client: &redis_tower::RedisClient) {
    for i in 0..10 {
        if i < 7 {
            redis_tower_get(client).await;
        } else {
            redis_tower_set(client).await;
        }
    }
}

async fn fred_mixed(client: &fred::clients::RedisClient) {
    for i in 0..10 {
        if i < 7 {
            fred_get(client).await;
        } else {
            fred_set(client).await;
        }
    }
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    let rt = Runtime::new().unwrap();

    let redis_tower_client = rt.block_on(redis_tower_setup());
    let fred_client = rt.block_on(fred_setup());

    // Mixed workload: 70% reads, 30% writes
    group.bench_function("redis-tower", |b| {
        b.to_async(&rt)
            .iter(|| redis_tower_mixed(&redis_tower_client));
    });

    group.bench_function("fred", |b| {
        b.to_async(&rt).iter(|| fred_mixed(&fred_client));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_get,
    bench_set,
    bench_pipeline,
    bench_mixed_workload
);
criterion_main!(benches);
