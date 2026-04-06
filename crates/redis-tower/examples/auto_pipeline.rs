//! Automatic pipelining: concurrent tasks are batched transparently.
//!
//! AutoPipelineService collects requests from multiple tasks and sends
//! them as a single Redis pipeline for better throughput.

use redis_tower::commands::*;
use redis_tower::{AutoPipelineConfig, AutoPipelineService, CommandAdapter, RedisConnection};
use tower_service::Service;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = RedisConnection::connect("127.0.0.1:6379").await?;

    // Wrap the connection in the auto-pipelining service.
    let config = AutoPipelineConfig {
        max_batch_size: 50,
        ..Default::default()
    };
    let auto = AutoPipelineService::new(conn, config);

    // Spawn concurrent tasks -- their commands are batched automatically.
    let mut handles = Vec::new();
    for i in 0..10 {
        let mut svc = CommandAdapter::new(auto.clone());
        handles.push(tokio::spawn(async move {
            let key = format!("auto:{i}");
            let _: Option<bytes::Bytes> = svc.call(Set::new(&key, "batched")).await.unwrap();
            let val: Option<bytes::Bytes> = svc.call(Get::new(&key)).await.unwrap();
            println!("Task {i}: {val:?}");
            let _: i64 = svc.call(Del::new(&key)).await.unwrap();
        }));
    }

    for h in handles {
        h.await?;
    }

    Ok(())
}
