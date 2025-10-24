//! Tower Service example demonstrating the Tower Service trait
//!
//! This example shows how RedisConnection implements the Tower Service trait,
//! enabling use with Tower's ecosystem.
//!
//! Prerequisites:
//! - Redis server running on localhost:6379
//!
//! Run with: cargo run --example tower_service

use redis_tower::client::RedisConnection;
use redis_tower::commands::{Del, Get, Incr, Set};
use tower::Service;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Tower Service Integration Example\n");

    demo_service_trait().await?;
    demo_generic_service().await?;

    println!("\nTower Service integration complete!");
    println!("\nKey benefits:");
    println!("  ✓ RedisConnection implements tower::Service<Cmd>");
    println!("  ✓ Enables middleware composition");
    println!("  ✓ Compatible with Tower ecosystem");
    println!("  ✓ Type-safe command/response pattern");

    Ok(())
}

async fn demo_service_trait() -> anyhow::Result<()> {
    println!("=== Demo 1: Tower Service Trait ===\n");

    let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;

    // RedisConnection implements Service<Cmd> for any Command
    println!("Setting key using Tower Service trait...");
    let set_cmd = Set::new("tower_demo", "Hello from Tower!");
    let _: () = Service::call(&mut conn, set_cmd).await?;

    println!("Getting key using Tower Service trait...");
    let get_cmd = Get::new("tower_demo");
    let value: Option<bytes::Bytes> = Service::call(&mut conn, get_cmd).await?;
    println!(
        "Value: {:?}",
        value.map(|b| String::from_utf8_lossy(&b).to_string())
    );

    // Clean up
    conn.execute(Del::new(vec!["tower_demo".to_string()]))
        .await?;
    println!();

    Ok(())
}

async fn demo_generic_service() -> anyhow::Result<()> {
    println!("=== Demo 2: Generic Service Function ===\n");

    let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;

    // You can write generic functions that work with any Tower Service
    async fn increment_with_service<S>(service: &mut S, key: &str) -> Result<i64, S::Error>
    where
        S: Service<Incr, Response = i64>,
    {
        let cmd = Incr::new(key);
        service.call(cmd).await
    }

    println!("Using generic service function to increment counter...");
    for i in 1..=5 {
        let count = increment_with_service(&mut conn, "tower_counter").await?;
        println!("  Iteration {}: counter = {}", i, count);
    }

    // Get final value
    let final_count: Option<bytes::Bytes> = conn.execute(Get::new("tower_counter")).await?;

    if let Some(count_bytes) = final_count {
        println!(
            "\nFinal counter value: {}",
            String::from_utf8_lossy(&count_bytes)
        );
    }

    // Clean up
    conn.execute(Del::new(vec!["tower_counter".to_string()]))
        .await?;

    Ok(())
}
