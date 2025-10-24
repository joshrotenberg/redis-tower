//! Cluster pipeline example
//!
//! Demonstrates using pipelines with Redis Cluster.
//! IMPORTANT: All commands in a cluster pipeline MUST target the same hash slot!
//!
//! Run with:
//! ```bash
//! cargo run --example pipeline_cluster
//! ```

use redis_tower::Pipeline;
use redis_tower::cluster::ClusterClient;
use redis_tower::commands::{Get, Incr, Set};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("Redis Cluster Pipeline Example");
    println!("================================\n");

    // Connect to cluster
    let client = ClusterClient::new(vec!["127.0.0.1:7000".to_string()]).await?;

    println!("Example 1: Single-Slot Pipeline");
    println!("--------------------------------");
    println!("All commands target keys with the same hash slot");
    println!();

    // Use hash tags to ensure all keys go to the same slot
    // Keys with {user:123} all hash to the same slot
    let mut pipeline = Pipeline::new();
    pipeline
        .add(Set::new("{user:123}:name", "Alice"))
        .add(Set::new("{user:123}:age", "30"))
        .add(Set::new("{user:123}:city", "NYC"))
        .add(Get::new("{user:123}:name"))
        .add(Get::new("{user:123}:age"))
        .add(Get::new("{user:123}:city"));

    let mut results = pipeline.execute(&client).await?;

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

    println!("Example 2: Counter Operations (Same Slot)");
    println!("------------------------------------------");

    let mut pipeline = Pipeline::new();
    pipeline
        .add(Set::new("{counter:main}:visits", "0"))
        .add(Incr::new("{counter:main}:visits"))
        .add(Incr::new("{counter:main}:visits"))
        .add(Incr::new("{counter:main}:visits"))
        .add(Get::new("{counter:main}:visits"));

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

    println!("Important Notes for Cluster Pipelines:");
    println!("---------------------------------------");
    println!("1. All commands MUST target the same hash slot");
    println!("2. Use hash tags {{key}} to control slot assignment");
    println!("3. Example: {{user:123}}:name and {{user:123}}:email hash to same slot");
    println!("4. Violating single-slot rule will result in CROSSSLOT error");
    println!();

    println!("Hash Tag Examples:");
    println!("- {{user:123}}:profile → All 'user:123' keys same slot");
    println!("- {{order:456}}:items → All 'order:456' keys same slot");
    println!("- {{session:abc}}:data → All 'session:abc' keys same slot");

    Ok(())
}
