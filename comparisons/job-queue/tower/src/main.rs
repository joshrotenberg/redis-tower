//! Job queue comparison -- redis-tower implementation.
//!
//! Uses StreamConsumer for the worker side and XAdd for the producer,
//! demonstrating how redis-tower wraps stream consumption into a
//! clean async Stream interface.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use redis_tower::consumer::{ConsumerConfig, StreamConsumer};
use redis_tower_commands::{Del, XAdd, XGroupDestroy};
use redis_tower_core::RedisConnection;
use tokio_stream::StreamExt;

const NUM_JOBS: usize = 100;
const NUM_WORKERS: usize = 4;
const STREAM: &str = "jobs";
const GROUP: &str = "workers";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Clean up from any previous run.
    let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
    let _ = conn.execute(XGroupDestroy::new(STREAM, GROUP)).await;
    let _ = conn.execute(Del::new(STREAM)).await;

    // Produce 100 jobs.
    for i in 0..NUM_JOBS {
        conn.execute(
            XAdd::new(STREAM)
                .field("job_id", i.to_string())
                .field("payload", format!("task-{i}")),
        )
        .await?;
    }

    let start = Instant::now();
    let total = Arc::new(AtomicUsize::new(0));
    let worker_counts: Arc<[AtomicUsize; NUM_WORKERS]> =
        Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));

    let mut handles = Vec::new();
    for id in 0..NUM_WORKERS {
        let total = Arc::clone(&total);
        let counts = Arc::clone(&worker_counts);

        handles.push(tokio::spawn(async move {
            let conn = RedisConnection::connect("127.0.0.1:6379").await.unwrap();
            let consumer = StreamConsumer::new(GROUP, format!("worker-{id}"), [STREAM]).config(
                ConsumerConfig {
                    batch_size: 10,
                    block_ms: Some(100),
                    auto_ack: true,
                    ..Default::default()
                },
            );

            let mut stream = std::pin::pin!(consumer.into_stream(conn));
            while let Some(msg) = stream.next().await {
                let _msg = msg.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                counts[id].fetch_add(1, Ordering::Relaxed);
                if total.fetch_add(1, Ordering::Relaxed) + 1 >= NUM_JOBS {
                    break;
                }
            }
        }));
    }

    // Workers block on XREADGROUP; give them a timeout to notice all jobs are done.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for h in handles {
            let _ = h.await;
        }
    })
    .await;

    let elapsed = start.elapsed();
    let processed = total.load(Ordering::Relaxed);
    println!("redis-tower: {processed} jobs in {elapsed:.2?}");
    println!("  throughput: {:.0} jobs/sec", processed as f64 / elapsed.as_secs_f64());
    for (i, c) in worker_counts.iter().enumerate() {
        println!("  worker-{i}: {} jobs", c.load(Ordering::Relaxed));
    }

    Ok(())
}
