use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use redis_tower::commands::{FlushDb, Get, Set};
use redis_tower::{
    AutoPipelineConfig, AutoPipelineService, CommandAdapter, RedisClient, RedisConnection,
    RedisValueExt,
};
use tokio::sync::RwLock;
use tower::Service;

const NUM_KEYS: usize = 100;
const NUM_REQUESTS: usize = 10_000;
const NUM_TASKS: usize = 10;
const SEED: u64 = 42;

/// Helper: poll_ready then call.
async fn call_ready<S, Req>(svc: &mut S, req: Req) -> Result<S::Response, S::Error>
where
    S: Service<Req>,
{
    std::future::poll_fn(|cx| svc.poll_ready(cx)).await?;
    svc.call(req).await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use RedisClient for setup (shared, simple).
    let client = RedisClient::connect("127.0.0.1:6379").await?;
    client.execute(FlushDb::new()).await?;

    for i in 0..NUM_KEYS {
        client
            .execute(Set::new(format!("item:{i}"), format!("value-{i}")))
            .await?;
    }

    // Use AutoPipelineService for the benchmark -- batches concurrent
    // requests into pipelines automatically, similar to redis-rs's
    // multiplexed connection.
    let conn = RedisConnection::connect("127.0.0.1:6379").await?;
    let svc = CommandAdapter::new(AutoPipelineService::new(
        conn,
        AutoPipelineConfig::default(),
    ));

    let cache: Arc<RwLock<HashMap<String, String>>> = Arc::new(RwLock::new(HashMap::new()));
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let start = Instant::now();

    let requests_per_task = NUM_REQUESTS / NUM_TASKS;
    let mut rng = StdRng::seed_from_u64(SEED);
    let task_keys: Vec<Vec<String>> = (0..NUM_TASKS)
        .map(|_| {
            (0..requests_per_task)
                .map(|_| format!("item:{}", rng.random_range(0..NUM_KEYS)))
                .collect()
        })
        .collect();

    let mut handles = Vec::new();
    for keys in task_keys {
        let mut svc = svc.clone();
        let cache = cache.clone();
        let hits = hits.clone();
        handles.push(tokio::spawn(async move {
            for key in keys {
                if cache.read().await.contains_key(&key) {
                    hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
                let raw: Option<bytes::Bytes> =
                    call_ready(&mut svc, Get::new(&key)).await.unwrap();
                let value: String = raw.parse_into().unwrap();
                cache.write().await.insert(key, value);
            }
        }));
    }

    for h in handles {
        h.await?;
    }

    let elapsed = start.elapsed();
    let total_hits = hits.load(std::sync::atomic::Ordering::Relaxed);
    let hit_rate = total_hits as f64 / NUM_REQUESTS as f64 * 100.0;
    let rps = NUM_REQUESTS as f64 / elapsed.as_secs_f64();

    println!("redis-tower (auto-pipeline) results:");
    println!("  Total time:   {elapsed:.2?}");
    println!("  Hit rate:     {hit_rate:.1}% ({total_hits}/{NUM_REQUESTS})");
    println!("  Requests/sec: {rps:.0}");
    Ok(())
}
