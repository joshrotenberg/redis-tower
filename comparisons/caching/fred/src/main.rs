use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use fred::prelude::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::RwLock;

const NUM_KEYS: usize = 100;
const NUM_REQUESTS: usize = 10_000;
const NUM_TASKS: usize = 10;
const SEED: u64 = 42;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Builder::default_centralized().build()?;
    client.init().await?;

    client.flushall::<()>(false).await?;

    // Populate Redis with 100 keys.
    for i in 0..NUM_KEYS {
        client
            .set::<(), _, _>(format!("item:{i}"), format!("value-{i}"), None, None, false)
            .await?;
    }

    let cache: Arc<RwLock<HashMap<String, String>>> = Arc::new(RwLock::new(HashMap::new()));
    let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let start = Instant::now();

    // Pre-generate random keys per task with a fixed seed.
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
        let client = client.clone();
        let cache = cache.clone();
        let hits = hits.clone();
        handles.push(tokio::spawn(async move {
            for key in keys {
                // Check local cache first.
                if cache.read().await.contains_key(&key) {
                    hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
                // Cache miss -- fetch from Redis with typed get.
                let value: String = client.get(&key).await.unwrap();
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

    println!("fred results:");
    println!("  Total time:   {elapsed:.2?}");
    println!("  Hit rate:     {hit_rate:.1}% ({total_hits}/{NUM_REQUESTS})");
    println!("  Requests/sec: {rps:.0}");

    client.quit().await?;
    Ok(())
}
