//! Job queue comparison -- fred implementation.
//!
//! Uses fred's built-in stream methods (xadd, xgroup_create,
//! xreadgroup, xack) for consumer group processing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use fred::prelude::*;
use fred::types::streams::XReadResponse;

const NUM_JOBS: usize = 100;
const NUM_WORKERS: usize = 4;
const STREAM: &str = "jobs";
const GROUP: &str = "workers";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Builder::default_centralized().build()?;
    client.init().await?;

    // Clean up from any previous run.
    let _: Result<i64, _> = client.xgroup_destroy(STREAM, GROUP).await;
    let _: Result<i64, _> = client.del(STREAM).await;

    // Produce 100 jobs.
    for i in 0..NUM_JOBS {
        let fields: Vec<(&str, String)> = vec![("job_id", i.to_string()), ("payload", format!("task-{i}"))];
        let _: String = client.xadd(STREAM, false, None, "*", fields).await?;
    }

    // Create consumer group.
    let _: () = client.xgroup_create(STREAM, GROUP, "$", true).await?;

    // Reset the group to read from 0 so consumers see existing entries.
    let _: () = client.xgroup_setid(STREAM, GROUP, "0").await?;

    let start = Instant::now();
    let total = Arc::new(AtomicUsize::new(0));
    let worker_counts: Arc<[AtomicUsize; NUM_WORKERS]> =
        Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));

    let mut handles = Vec::new();
    for id in 0..NUM_WORKERS {
        let client = client.clone();
        let total = Arc::clone(&total);
        let counts = Arc::clone(&worker_counts);

        handles.push(tokio::spawn(async move {
            let consumer_name = format!("worker-{id}");
            loop {
                let result: XReadResponse<String, String, String, String> = client
                    .xreadgroup_map(GROUP, &consumer_name, Some(10), Some(500), false, STREAM, ">")
                    .await
                    .unwrap();

                for (_stream_key, entries) in &result {
                    for (entry_id, _fields) in entries {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                        let _: i64 = client.xack(STREAM, GROUP, entry_id).await.unwrap();
                        counts[id].fetch_add(1, Ordering::Relaxed);
                        if total.fetch_add(1, Ordering::Relaxed) + 1 >= NUM_JOBS {
                            return;
                        }
                    }
                }
            }
        }));
    }

    for h in handles {
        h.await?;
    }

    let elapsed = start.elapsed();
    let processed = total.load(Ordering::Relaxed);
    println!("fred: {processed} jobs in {elapsed:.2?}");
    println!("  throughput: {:.0} jobs/sec", processed as f64 / elapsed.as_secs_f64());
    for (i, c) in worker_counts.iter().enumerate() {
        println!("  worker-{i}: {} jobs", c.load(Ordering::Relaxed));
    }

    Ok(())
}
