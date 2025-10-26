//! Comparison of different Redis client types
//!
//! This example demonstrates the differences between:
//! - RedisConnection (low-level, single connection)
//! - RedisClient (high-level wrapper, single connection)
//! - ResilientRedisClient (auto-reconnecting, single connection)
//! - ConnectionPool (multiple connections with pooling)
//!
//! Run with: cargo run --example client_types_comparison

use redis_tower::client::{RedisClient, RedisConnection, ResilientRedisClient};
use redis_tower::commands::{Get, Incr, Set};
use redis_tower::config::ClientConfig;
use redis_tower::pool::{ConnectionPool, PoolConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Redis Client Types Comparison ===\n");

    // 1. RedisConnection - Low-level, single connection
    println!("1. RedisConnection - Low-level API");
    println!("   Use when: You need direct control, transactions, or pipelines");
    {
        let conn = RedisConnection::connect("127.0.0.1:6379").await?;

        // Must use .execute() with RedisConnection
        conn.execute(Set::new("demo:conn", "connection_value"))
            .await?;
        let value: Option<bytes::Bytes> = conn.execute(Get::new("demo:conn")).await?;
        println!(
            "   Value: {:?}",
            value.map(|b| String::from_utf8_lossy(&b).to_string())
        );

        // Good for transactions (needs &RedisConnection)
        use redis_tower::Transaction;
        let mut tx = Transaction::new(&conn);
        tx.queue(Set::new("demo:tx", "tx_value")).await?;
        tx.queue(Get::new("demo:tx")).await?;
        let results = tx.exec().await?;
        println!("   Transaction results: {:?}", results);
    }
    println!();

    // 2. RedisClient - High-level wrapper around single connection
    println!("2. RedisClient - High-level API (single connection)");
    println!("   Use when: You want a simpler API, single connection is enough");
    {
        let client = RedisClient::connect("127.0.0.1:6379").await?;

        // Nicer .call() API instead of .execute()
        client.call(Set::new("demo:client", "client_value")).await?;
        let value: Option<bytes::Bytes> = client.call(Get::new("demo:client")).await?;
        println!(
            "   Value: {:?}",
            value.map(|b| String::from_utf8_lossy(&b).to_string())
        );

        // Can clone cheaply (Arc internally)
        let client2 = client.clone();
        let count: i64 = client2.call(Incr::new("demo:counter")).await?;
        println!("   Counter: {}", count);
    }
    println!();

    // 3. ResilientRedisClient - Auto-reconnecting single connection
    println!("3. ResilientRedisClient - Auto-reconnecting (single connection)");
    println!("   Use when: You need automatic reconnection for unreliable networks");
    {
        // Configure reconnection behavior
        let config = ClientConfig::builder()
            .address("127.0.0.1:6379")
            .reconnect_exponential(
                std::time::Duration::from_millis(100), // min delay
                std::time::Duration::from_secs(5),     // max delay
            )
            .max_reconnect_attempts(10) // or unlimited with .unlimited_reconnect_attempts()
            .build();

        let client = ResilientRedisClient::connect_with_full_config(config).await?;

        // Same .call() API, but reconnects automatically on failure
        client
            .call(Set::new("demo:resilient", "resilient_value"))
            .await?;
        let value: Option<bytes::Bytes> = client.call(Get::new("demo:resilient")).await?;
        println!(
            "   Value: {:?}",
            value.map(|b| String::from_utf8_lossy(&b).to_string())
        );

        // If connection drops, it will automatically reconnect with exponential backoff
        println!("   (Will auto-reconnect on failure with exponential backoff)");
    }
    println!();

    // 4. ConnectionPool - Multiple connections with pooling
    println!("4. ConnectionPool - Multiple connections with round-robin");
    println!("   Use when: You have high concurrency needs (many concurrent requests)");
    {
        let pool_config = PoolConfig::builder()
            .max_size(10) // Up to 10 connections
            .min_idle(2) // Keep 2 idle connections ready
            .connection_timeout(std::time::Duration::from_secs(5))
            .build();

        let pool = ConnectionPool::new("127.0.0.1:6379", pool_config).await?;

        // Get a connection from the pool
        let conn = pool.get().await?;

        // Use .execute() on pooled connection
        conn.execute(Set::new("demo:pool", "pool_value")).await?;
        let value: Option<bytes::Bytes> = conn.execute(Get::new("demo:pool")).await?;
        println!(
            "   Value: {:?}",
            value.map(|b| String::from_utf8_lossy(&b).to_string())
        );

        // Connection automatically returned to pool when dropped
        drop(conn);

        // Simulate concurrent requests
        let mut handles = vec![];
        for i in 0..5 {
            let pool = pool.clone();
            let handle = tokio::spawn(async move {
                let conn = pool.get().await.unwrap();
                let count: i64 = conn.execute(Incr::new("demo:pool_counter")).await.unwrap();
                println!("   Task {} incremented counter to: {}", i, count);
            });
            handles.push(handle);
        }

        // Wait for all tasks
        for handle in handles {
            handle.await?;
        }

        // Check pool stats
        let stats = pool.stats();
        println!(
            "   Pool stats: {} active, {} idle, {} total created",
            stats
                .active_connections
                .load(std::sync::atomic::Ordering::Relaxed),
            stats
                .idle_connections
                .load(std::sync::atomic::Ordering::Relaxed),
            stats
                .total_created
                .load(std::sync::atomic::Ordering::Relaxed)
        );
    }
    println!();

    // Decision Guide
    println!("=== When to Use Each ===\n");

    println!("RedisConnection:");
    println!("  ✓ Transactions (MULTI/EXEC)");
    println!("  ✓ Pipelines");
    println!("  ✓ Low-level control");
    println!("  ✓ Testing/debugging");
    println!("  ✗ No auto-reconnect");
    println!("  ✗ No pooling\n");

    println!("RedisClient:");
    println!("  ✓ Simple applications");
    println!("  ✓ Low concurrency (<10 requests/sec)");
    println!("  ✓ Cleaner API (.call vs .execute)");
    println!("  ✓ Can clone cheaply");
    println!("  ✗ No auto-reconnect");
    println!("  ✗ No pooling (single connection)\n");

    println!("ResilientRedisClient:");
    println!("  ✓ Unreliable networks");
    println!("  ✓ Long-running applications");
    println!("  ✓ Auto-reconnect with exponential backoff");
    println!("  ✓ Configurable retry logic");
    println!("  ✓ Health checking");
    println!("  ✗ No pooling (single connection)\n");

    println!("ConnectionPool:");
    println!("  ✓ High concurrency (100s-1000s requests/sec)");
    println!("  ✓ Web servers");
    println!("  ✓ Multiple concurrent tasks");
    println!("  ✓ Connection reuse");
    println!("  ✓ Round-robin load distribution");
    println!("  ⚠ Use with RedisConnection (not RedisClient)\n");

    println!("=== Common Patterns ===\n");

    println!("Single-threaded CLI tool:");
    println!("  → RedisClient (simple, clean API)\n");

    println!("Background worker processing queue:");
    println!("  → ResilientRedisClient (auto-reconnect for long-running process)\n");

    println!("Web server (Axum/Actix):");
    println!("  → ConnectionPool (handle concurrent requests efficiently)\n");

    println!("Microservice with Tower middleware:");
    println!("  → ResilientRedisClient + Tower layers (retry, circuit breaker, timeout)\n");

    println!("Transaction-heavy application:");
    println!("  → ConnectionPool + RedisConnection (get from pool, run transaction)\n");

    Ok(())
}
