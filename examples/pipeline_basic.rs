//! Basic pipeline example demonstrating type-safe command batching
//!
//! Pipelines allow sending multiple commands in a single network roundtrip,
//! dramatically improving throughput when executing many commands.
//!
//! Run with:
//! ```bash
//! cargo run --example pipeline_basic
//! ```

use redis_tower::commands::{Del, Get, Incr, Set};
use redis_tower::{Pipeline, RedisClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("Redis Pipeline Basic Example");
    println!("============================\n");

    // Connect to Redis
    let client = RedisClient::connect("127.0.0.1:6379").await?;

    // Clean up any existing keys
    client.call(Del::new(vec!["counter".to_string()])).await?;

    println!("Example 1: Basic Pipeline");
    println!("--------------------------");

    let mut pipeline = Pipeline::new();
    pipeline
        .add(Set::new("name", "Alice"))
        .add(Set::new("age", "30"))
        .add(Set::new("city", "NYC"))
        .add(Get::new("name"))
        .add(Get::new("age"))
        .add(Get::new("city"));

    let mut results = pipeline.execute(&client).await?;

    // Extract results in order
    println!("Set name: {:?}", results.next_result::<Set>()?);
    println!("Set age: {:?}", results.next_result::<Set>()?);
    println!("Set city: {:?}", results.next_result::<Set>()?);

    let name: Option<bytes::Bytes> = results.next_result::<Get>()?;
    let age: Option<bytes::Bytes> = results.next_result::<Get>()?;
    let city: Option<bytes::Bytes> = results.next_result::<Get>()?;

    println!("Got name: {}", String::from_utf8_lossy(&name.unwrap()));
    println!("Got age: {}", String::from_utf8_lossy(&age.unwrap()));
    println!("Got city: {}", String::from_utf8_lossy(&city.unwrap()));
    println!();

    println!("Example 2: Pipeline with Multiple Operations");
    println!("---------------------------------------------");

    let mut pipeline = Pipeline::new();
    pipeline
        .add(Set::new("counter", "0"))
        .add(Incr::new("counter"))
        .add(Incr::new("counter"))
        .add(Incr::new("counter"))
        .add(Get::new("counter"));

    let mut results = pipeline.execute(&client).await?;

    println!("Set counter: {:?}", results.next_result::<Set>()?);
    println!("Incr 1: {}", results.next_result::<Incr>()?);
    println!("Incr 2: {}", results.next_result::<Incr>()?);
    println!("Incr 3: {}", results.next_result::<Incr>()?);

    let counter: Option<bytes::Bytes> = results.next_result::<Get>()?;
    println!(
        "Final counter: {}",
        String::from_utf8_lossy(&counter.unwrap())
    );
    println!();

    println!("Example 3: Reusing Pipeline");
    println!("----------------------------");

    let mut pipeline = Pipeline::with_capacity(5);

    // First batch
    pipeline
        .add(Set::new("batch1_key1", "value1"))
        .add(Set::new("batch1_key2", "value2"));

    let results = pipeline.execute(&client).await?;
    println!("Batch 1 complete: {} results", results.len());

    // Clear and reuse
    pipeline.clear();

    // Second batch
    pipeline
        .add(Get::new("batch1_key1"))
        .add(Get::new("batch1_key2"));

    let mut results = pipeline.execute(&client).await?;
    println!("Batch 2 complete: {} results", results.len());

    let val1: Option<bytes::Bytes> = results.next_result::<Get>()?;
    let val2: Option<bytes::Bytes> = results.next_result::<Get>()?;
    println!(
        "Retrieved: {}, {}",
        String::from_utf8_lossy(&val1.unwrap()),
        String::from_utf8_lossy(&val2.unwrap())
    );
    println!();

    println!("Benefits of Pipelining:");
    println!("- Reduced network roundtrips (1 instead of N)");
    println!("- Higher throughput for bulk operations");
    println!("- Lower latency for batch workloads");
    println!("- Type safety preserved for all commands");

    Ok(())
}
