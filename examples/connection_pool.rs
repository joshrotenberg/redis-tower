//! # Connection Pool Example
//!
//! Demonstrates creating a `ConnectionPool<RedisConnection>` and executing
//! concurrent commands from multiple tasks. The pool manages N independent
//! connections and dispatches commands across them.
//!
//! **Prerequisites:** A Redis server running on `127.0.0.1:6379`.

use redis_tower::commands::*;
use redis_tower::pool::{ConnectionPool, DispatchStrategy, PoolConfig};
use redis_tower::{RedisConnection, RedisValueExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a pool of 4 connections using round-robin dispatch.
    let pool = ConnectionPool::connect_with_config(
        PoolConfig::default()
            .size(4)
            .dispatch(DispatchStrategy::RoundRobin),
        || async { RedisConnection::connect("127.0.0.1:6379").await },
    )
    .await?;

    println!("Pool created with {} connections.", pool.size());

    // Spawn several tasks that all share the same pool.
    // Each task clone is cheap -- they all point to the same Arc<PoolInner>.
    let mut handles = Vec::new();
    for i in 0..8 {
        let p = pool.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("pool:key:{i}");
            let val = format!("value-{i}");
            p.execute(Set::new(&key, &val)).await.unwrap();
            let got: String = p
                .execute(Get::new(&key))
                .await
                .unwrap()
                .parse_into()
                .unwrap();
            println!("Task {i}: {got}");
            p.execute(Del::new(&key)).await.unwrap();
        }));
    }

    for h in handles {
        h.await?;
    }

    println!("Connection pool example complete.");
    Ok(())
}
