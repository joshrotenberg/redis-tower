//! Share a RedisClient across multiple Tokio tasks.
//!
//! RedisClient wraps the connection in Arc<Mutex<>> so cloning is cheap
//! and each task can execute commands independently.

use redis_tower::commands::*;
use redis_tower::{RedisClient, RedisValueExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = RedisClient::connect("127.0.0.1:6379").await?;

    // Spawn several tasks that share the same client.
    let mut handles = Vec::new();
    for i in 0..5 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("shared:{i}");
            let val = format!("task-{i}");
            c.execute(Set::new(&key, &val)).await.unwrap();
            let got: String = c.execute(Get::new(&key)).await.unwrap().parse_into().unwrap();
            println!("Task {i}: {got}");
            c.execute(Del::new(&key)).await.unwrap();
        }));
    }

    // Wait for all tasks to finish.
    for h in handles {
        h.await?;
    }

    Ok(())
}
