//! Atomic pipeline example using MULTI/EXEC transactions
//!
//! Demonstrates using pipelines in atomic mode where all commands
//! execute as a single transaction (all or nothing).
//!
//! Run with:
//! ```bash
//! cargo run --example pipeline_atomic
//! ```

use redis_tower::commands::{Decr, Get, Incr, Set};
use redis_tower::{Pipeline, RedisClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("Redis Atomic Pipeline Example");
    println!("==============================\n");

    // Connect to Redis
    let client = RedisClient::connect("127.0.0.1:6379").await?;

    println!("Example 1: Atomic Pipeline (MULTI/EXEC)");
    println!("----------------------------------------");

    // Set up initial values
    client.call(Set::new("balance", "100")).await?;
    client.call(Set::new("transactions", "0")).await?;

    let mut pipeline = Pipeline::new();
    pipeline
        .atomic() // Enable atomic mode (wraps in MULTI/EXEC)
        .add(Get::new("balance"))
        .add(Decr::new("balance")) // Deduct 1
        .add(Incr::new("transactions")); // Track transaction count

    let mut results = pipeline.execute(&client).await?;

    let balance: Option<bytes::Bytes> = results.next_result::<Get>()?;
    let new_balance: i64 = results.next_result::<Decr>()?;
    let tx_count: i64 = results.next_result::<Incr>()?;

    println!(
        "Original balance: {}",
        String::from_utf8_lossy(&balance.unwrap())
    );
    println!("New balance: {}", new_balance);
    println!("Transaction count: {}", tx_count);
    println!();

    println!("Example 2: Multiple Atomic Operations");
    println!("--------------------------------------");

    // Perform 5 transactions atomically
    for i in 1..=5 {
        let mut pipeline = Pipeline::new();
        pipeline
            .atomic()
            .add(Decr::new("balance"))
            .add(Incr::new("transactions"));

        let mut results = pipeline.execute(&client).await?;
        let balance: i64 = results.next_result::<Decr>()?;
        let tx: i64 = results.next_result::<Incr>()?;

        println!("Transaction {}: balance={}, total_txs={}", i, balance, tx);
    }
    println!();

    println!("Example 3: Verifying Final State");
    println!("---------------------------------");

    let mut pipeline = Pipeline::new();
    pipeline
        .add(Get::new("balance"))
        .add(Get::new("transactions"));

    let mut results = pipeline.execute(&client).await?;

    let final_balance: Option<bytes::Bytes> = results.next_result::<Get>()?;
    let final_txs: Option<bytes::Bytes> = results.next_result::<Get>()?;

    println!(
        "Final balance: {}",
        String::from_utf8_lossy(&final_balance.unwrap())
    );
    println!(
        "Total transactions: {}",
        String::from_utf8_lossy(&final_txs.unwrap())
    );
    println!();

    println!("Atomic Pipeline Benefits:");
    println!("- All commands execute atomically (all or nothing)");
    println!("- No other commands can interleave");
    println!("- Perfect for maintaining consistency");
    println!("- Automatic rollback on errors");

    Ok(())
}
