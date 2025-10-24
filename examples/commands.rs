//! Comprehensive commands example
//!
//! This example demonstrates redis-tower's strongly typed commands across
//! different Redis data structures:
//! - Strings (GET, SET, INCR, DECR, MGET)
//! - Hashes (HGET, HSET, HGETALL, HDEL)
//! - Lists (LPUSH, RPUSH, LPOP, RPOP, LRANGE)
//!
//! Each command has compile-time type safety for both parameters and responses.
//!
//! Prerequisites:
//! - Redis server running on localhost:6379
//!
//! Run with: cargo run --example commands

use redis_tower::RedisClient;
use redis_tower::commands::{hashes, lists, strings};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!("Redis-Tower: Type-Safe Redis Commands\n");
    println!("Connecting to Redis...\n");

    let client = RedisClient::connect("localhost:6379").await?;

    // ========== String Commands ==========
    println!("=== String Commands ===\n");

    // Basic GET/SET
    println!("1. SET/GET:");
    client.call(strings::Set::new("user:name", "Alice")).await?;
    let name: Option<bytes::Bytes> = client.call(strings::Get::new("user:name")).await?;
    println!("   Name: {}", String::from_utf8_lossy(&name.unwrap()));

    // Atomic counters
    println!("\n2. INCR/DECR (atomic counters):");
    let _count1: i64 = client.call(strings::Incr::new("visitor_count")).await?;
    let _count2: i64 = client.call(strings::Incr::new("visitor_count")).await?;
    let count3: i64 = client.call(strings::Incr::new("visitor_count")).await?;
    println!("   After 3 increments: {}", count3);

    let count4: i64 = client.call(strings::Decr::new("visitor_count")).await?;
    println!("   After 1 decrement: {}", count4);

    // Multiple get
    println!("\n3. MGET (get multiple keys at once):");
    client.call(strings::Set::new("key1", "value1")).await?;
    client.call(strings::Set::new("key2", "value2")).await?;
    client.call(strings::Set::new("key3", "value3")).await?;

    let values: Vec<Option<bytes::Bytes>> = client
        .call(strings::MGet::new(vec![
            "key1".to_string(),
            "key2".to_string(),
            "key3".to_string(),
            "nonexistent".to_string(),
        ]))
        .await?;

    println!("   Retrieved {} values:", values.len());
    for (i, value) in values.iter().enumerate() {
        match value {
            Some(v) => println!("     [{}]: {}", i, String::from_utf8_lossy(v)),
            None => println!("     [{}]: (null)", i),
        }
    }

    // ========== Hash Commands ==========
    println!("\n=== Hash Commands ===\n");

    // HSET/HGET
    println!("1. HSET/HGET (store structured data):");
    let _: i64 = client
        .call(hashes::HSet::new("user:1000", "name", "Bob"))
        .await?;
    let _: i64 = client
        .call(hashes::HSet::new("user:1000", "email", "bob@example.com"))
        .await?;
    let _: i64 = client
        .call(hashes::HSet::new("user:1000", "age", "30"))
        .await?;

    let name: Option<bytes::Bytes> = client.call(hashes::HGet::new("user:1000", "name")).await?;
    println!(
        "   User 1000 name: {}",
        String::from_utf8_lossy(&name.unwrap())
    );

    // HGETALL
    println!("\n2. HGETALL (get all fields):");
    let user_data: std::collections::HashMap<String, bytes::Bytes> =
        client.call(hashes::HGetAll::new("user:1000")).await?;

    println!("   User 1000 data:");
    for (field, value) in &user_data {
        println!("     {}: {}", field, String::from_utf8_lossy(value));
    }

    // HDEL
    println!("\n3. HDEL (delete fields):");
    let deleted: i64 = client
        .call(hashes::HDel::new("user:1000", vec!["age".to_string()]))
        .await?;
    println!("   Deleted {} field(s)", deleted);

    // ========== List Commands ==========
    println!("\n=== List Commands ===\n");

    // LPUSH/RPUSH
    println!("1. LPUSH/RPUSH (push to list):");
    let _len1: i64 = client.call(lists::LPush::single("tasks", "task1")).await?;
    let _len2: i64 = client.call(lists::LPush::single("tasks", "task2")).await?;
    let len3: i64 = client.call(lists::RPush::single("tasks", "task3")).await?;
    println!("   List length after pushes: {}", len3);

    // LRANGE
    println!("\n2. LRANGE (get range from list):");
    let all_tasks: Vec<bytes::Bytes> = client.call(lists::LRange::all("tasks")).await?;
    println!("   All tasks:");
    for (i, task) in all_tasks.iter().enumerate() {
        println!("     [{}]: {}", i, String::from_utf8_lossy(task));
    }

    let range: Vec<bytes::Bytes> = client.call(lists::LRange::new("tasks", 0, 1)).await?;
    println!("   First 2 tasks:");
    for (i, task) in range.iter().enumerate() {
        println!("     [{}]: {}", i, String::from_utf8_lossy(task));
    }

    // LPOP/RPOP
    println!("\n3. LPOP/RPOP (pop from list):");
    let popped_left: Option<bytes::Bytes> = client.call(lists::LPop::new("tasks")).await?;
    println!(
        "   Popped from left: {}",
        String::from_utf8_lossy(&popped_left.unwrap())
    );

    let popped_right: Option<bytes::Bytes> = client.call(lists::RPop::new("tasks")).await?;
    println!(
        "   Popped from right: {}",
        String::from_utf8_lossy(&popped_right.unwrap())
    );

    let remaining: Vec<bytes::Bytes> = client.call(lists::LRange::all("tasks")).await?;
    println!("   Remaining tasks: {}", remaining.len());

    // ========== Cleanup ==========
    println!("\n=== Cleanup ===\n");
    let deleted: i64 = client
        .call(strings::Del::new(vec![
            "user:name".to_string(),
            "visitor_count".to_string(),
            "key1".to_string(),
            "key2".to_string(),
            "key3".to_string(),
            "user:1000".to_string(),
            "tasks".to_string(),
        ]))
        .await?;
    println!("Deleted {} keys\n", deleted);

    // ========== Summary ==========
    println!("=== Type Safety Highlights ===\n");
    println!("String commands:");
    println!("  GET:  Option<Bytes>      - nullable result");
    println!("  INCR: i64                - atomic counter returns new value");
    println!("  MGET: Vec<Option<Bytes>> - array of nullable results");
    println!();
    println!("Hash commands:");
    println!("  HGET:    Option<Bytes>            - single field lookup");
    println!("  HSET:    i64                      - returns 1 if new, 0 if update");
    println!("  HGETALL: HashMap<String, Bytes>   - structured data");
    println!();
    println!("List commands:");
    println!("  LPUSH:  i64           - returns new list length");
    println!("  LPOP:   Option<Bytes> - nullable (empty list)");
    println!("  LRANGE: Vec<Bytes>    - always returns array (empty if no match)");
    println!();
    println!("All type information is enforced at compile time!");

    Ok(())
}
