//! Example demonstrating essential Redis commands (Phase 1)
//!
//! This showcases:
//! - PING: Connection testing
//! - ECHO: Message echoing
//! - EXISTS: Key existence checks
//! - TTL/EXPIRE: Key expiration management
//! - MSET: Multi-key setting

use redis_tower::client::RedisConnection;
use redis_tower::commands::{Del, Echo, Exists, Expire, Get, Mset, Ping, Set, Ttl};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let client = RedisConnection::connect("127.0.0.1:6379").await?;
    println!("Connected to Redis");

    println!("\n=== Example 1: PING - Connection Testing ===");

    // Simple ping
    let response: String = client.execute(Ping::new()).await?;
    println!("PING -> {}", response);
    assert_eq!(response, "PONG");

    // Ping with custom message
    let response: String = client.execute(Ping::with_message("Hello Redis!")).await?;
    println!("PING 'Hello Redis!' -> {}", response);
    assert_eq!(response, "Hello Redis!");

    println!("\n=== Example 2: ECHO - Message Echoing ===");

    let message = "The quick brown fox jumps over the lazy dog";
    let response: String = client.execute(Echo::new(message)).await?;
    println!("ECHO '{}' -> {}", message, response);
    assert_eq!(response, message);

    println!("\n=== Example 3: EXISTS - Key Existence Checks ===");

    // Clean up first
    let _ = client
        .execute(Del::new(vec![
            "exists_test1".to_string(),
            "exists_test2".to_string(),
            "exists_test3".to_string(),
        ]))
        .await;

    // Check non-existent key
    let count: i64 = client.execute(Exists::new("exists_test1")).await?;
    println!("EXISTS exists_test1 (before SET) -> {}", count);
    assert_eq!(count, 0);

    // Set some keys
    client
        .execute(Set::new("exists_test1", b"value1".to_vec()))
        .await?;
    client
        .execute(Set::new("exists_test2", b"value2".to_vec()))
        .await?;

    // Check single key
    let count: i64 = client.execute(Exists::new("exists_test1")).await?;
    println!("EXISTS exists_test1 (after SET) -> {}", count);
    assert_eq!(count, 1);

    // Check multiple keys (mix of existing and non-existing)
    let count: i64 = client
        .execute(Exists::multiple(vec![
            "exists_test1",
            "exists_test2",
            "exists_test3",
        ]))
        .await?;
    println!(
        "EXISTS exists_test1 exists_test2 exists_test3 -> {} (2 exist, 1 doesn't)",
        count
    );
    assert_eq!(count, 2);

    println!("\n=== Example 4: MSET - Multi-Key Setting ===");

    // Set multiple keys at once
    let response: String = client
        .execute(
            Mset::new()
                .pair("user:1:name", b"Alice".to_vec())
                .pair("user:1:email", b"alice@example.com".to_vec())
                .pair("user:1:age", b"30".to_vec()),
        )
        .await?;
    println!("MSET user:1:* -> {}", response);
    assert_eq!(response, "OK");

    // Verify all keys were set
    let name = client.execute(Get::new("user:1:name")).await?;
    let email = client.execute(Get::new("user:1:email")).await?;
    let age = client.execute(Get::new("user:1:age")).await?;

    println!(
        "  user:1:name = {}",
        String::from_utf8_lossy(&name.unwrap())
    );
    println!(
        "  user:1:email = {}",
        String::from_utf8_lossy(&email.unwrap())
    );
    println!("  user:1:age = {}", String::from_utf8_lossy(&age.unwrap()));

    // Alternative: using pairs() method
    let kvs = vec![
        ("product:1:name", "Laptop"),
        ("product:1:price", "999.99"),
        ("product:1:stock", "42"),
    ];

    let response: String = client
        .execute(Mset::new().pairs(kvs.into_iter().map(|(k, v)| (k, v.as_bytes()))))
        .await?;
    println!("MSET product:1:* -> {}", response);

    println!("\n=== Example 5: EXPIRE and TTL - Key Expiration ===");

    // Set a key
    client
        .execute(Set::new("session:abc123", b"user_data".to_vec()))
        .await?;

    // Check TTL before setting expiration (-1 means no expiration)
    let ttl: i64 = client.execute(Ttl::new("session:abc123")).await?;
    println!("TTL session:abc123 (no expiration set) -> {}", ttl);
    assert_eq!(ttl, -1);

    // Set expiration to 60 seconds
    let success: bool = client.execute(Expire::new("session:abc123", 60)).await?;
    println!("EXPIRE session:abc123 60 -> {}", success);
    assert!(success);

    // Check TTL after setting expiration
    let ttl: i64 = client.execute(Ttl::new("session:abc123")).await?;
    println!("TTL session:abc123 (after EXPIRE) -> {} seconds", ttl);
    assert!(ttl > 0 && ttl <= 60);

    // Try to expire non-existent key
    let success: bool = client.execute(Expire::new("nonexistent", 60)).await?;
    println!("EXPIRE nonexistent 60 -> {} (key doesn't exist)", success);
    assert!(!success);

    // Check TTL for non-existent key (-2 means key doesn't exist)
    let ttl: i64 = client.execute(Ttl::new("nonexistent")).await?;
    println!("TTL nonexistent -> {} (key doesn't exist)", ttl);
    assert_eq!(ttl, -2);

    println!("\n=== Example 6: Practical Use Case - Session Management ===");

    // Simulate user login
    let session_id = "session:user:12345";
    let session_data = b"user_id=12345,role=admin,login_time=2025-10-23T10:00:00Z".to_vec();

    // Create session with 3600 second (1 hour) expiration
    client.execute(Set::new(session_id, session_data)).await?;
    client.execute(Expire::new(session_id, 3600)).await?;
    println!("Created session: {}", session_id);

    // Check if session exists
    let exists: i64 = client.execute(Exists::new(session_id)).await?;
    println!("Session exists: {}", exists == 1);

    // Check remaining time
    let ttl: i64 = client.execute(Ttl::new(session_id)).await?;
    println!("Session expires in: {} seconds", ttl);

    // Extend session (refresh expiration)
    client.execute(Expire::new(session_id, 7200)).await?;
    let new_ttl: i64 = client.execute(Ttl::new(session_id)).await?;
    println!("Session extended, now expires in: {} seconds", new_ttl);

    println!("\n=== Example 7: Practical Use Case - Cache Management ===");

    // Set multiple cache entries
    client
        .execute(
            Mset::new()
                .pair("cache:user:1", b"cached_user_data_1".to_vec())
                .pair("cache:user:2", b"cached_user_data_2".to_vec())
                .pair("cache:user:3", b"cached_user_data_3".to_vec()),
        )
        .await?;

    // Set TTL for cache entries (5 minutes)
    for i in 1..=3 {
        let key = format!("cache:user:{}", i);
        client.execute(Expire::new(&key, 300)).await?;
    }
    println!("Created 3 cache entries with 5-minute expiration");

    // Check which cache entries exist
    let count: i64 = client
        .execute(Exists::multiple(vec![
            "cache:user:1",
            "cache:user:2",
            "cache:user:3",
            "cache:user:4",
        ]))
        .await?;
    println!("Cache entries exist: {} out of 4", count);

    // Verify connection is still alive
    let pong: String = client.execute(Ping::new()).await?;
    println!("\nConnection check: {}", pong);

    println!("\n=== All essential commands demonstrated! ===");
    println!("\nNew commands added:");
    println!("  ✅ PING - Test connection with optional message");
    println!("  ✅ ECHO - Echo messages back");
    println!("  ✅ EXISTS - Check key existence (single or multiple)");
    println!("  ✅ TTL - Get time-to-live in seconds");
    println!("  ✅ EXPIRE - Set key expiration");
    println!("  ✅ MSET - Set multiple keys atomically");

    Ok(())
}
