//! Job queue comparison -- redis-rs implementation.
//!
//! Uses raw xadd, xgroup_create, xreadgroup, and xack commands
//! via the Commands trait, showing the boilerplate required for
//! stream-based consumer group processing.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use redis::AsyncCommands;
use redis::streams::{StreamReadOptions, StreamReadReply};

const NUM_JOBS: usize = 100;
const NUM_WORKERS: usize = 4;
const STREAM: &str = "jobs";
const GROUP: &str = "workers";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // Clean up from any previous run.
    let _: Result<(), _> = redis::cmd("XGROUP")
        .arg("DESTROY")
        .arg(STREAM)
        .arg(GROUP)
        .query_async(&mut conn)
        .await;
    let _: Result<(), _> = conn.del(STREAM).await;

    // Produce 100 jobs.
    for i in 0..NUM_JOBS {
        let _: String = conn
            .xadd(STREAM, "*", &[("job_id", &i.to_string()), ("payload", &format!("task-{i}"))])
            .await?;
    }

    // Create consumer group.
    let _: () = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(STREAM)
        .arg(GROUP)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(&mut conn)
        .await?;

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
            let mut conn = client.get_multiplexed_async_connection().await.unwrap();
            let consumer_name = format!("worker-{id}");
            let opts = StreamReadOptions::default()
                .group(GROUP, &consumer_name)
                .count(10)
                .block(500);

            loop {
                let reply: StreamReadReply =
                    conn.xread_options(&[STREAM], &[">"], &opts).await.unwrap();

                for key in &reply.keys {
                    for entry in &key.ids {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                        let _: i64 = conn.xack(STREAM, GROUP, &[&entry.id]).await.unwrap();
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
    println!("redis-rs: {processed} jobs in {elapsed:.2?}");
    println!("  throughput: {:.0} jobs/sec", processed as f64 / elapsed.as_secs_f64());
    for (i, c) in worker_counts.iter().enumerate() {
        println!("  worker-{i}: {} jobs", c.load(Ordering::Relaxed));
    }

    Ok(())
}
