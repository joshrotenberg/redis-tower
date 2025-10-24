//! Complex Commands Example - Level 3+ Difficulty
//!
//! This example demonstrates redis-tower's handling of complex Redis commands:
//! - SCAN: Cursor-based iteration with custom response types
//! - HSCAN: Hash field iteration
//! - RESP3 types: Map, Set, Double support
//!
//! These commands showcase:
//! 1. Custom response types (ScanResult with cursor + data)
//! 2. Stateful iteration patterns
//! 3. Builder pattern for optional arguments
//! 4. RESP3 protocol features
//!
//! Prerequisites:
//! - Redis server running on localhost:6379
//!
//! Run with: cargo run --example complex_commands

use redis_tower::RedisClient;
use redis_tower::commands::{hashes, scan, strings};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    println!("Redis-Tower: Complex Commands (Level 3)\n");
    println!("Demonstrating cursor-based iteration and RESP3 features\n");

    let client = RedisClient::connect("localhost:6379").await?;

    // ========== Setup: Create test data ==========
    println!("=== Setup: Creating test data ===\n");

    // Create 100 keys for SCAN testing
    println!("Creating 100 test keys...");
    for i in 0..100 {
        client
            .call(strings::Set::new(
                format!("testkey:{}", i),
                format!("value{}", i),
            ))
            .await?;
    }
    println!("Created 100 keys: testkey:0 through testkey:99\n");

    // Create a hash with many fields for HSCAN testing
    println!("Creating hash with 50 fields...");
    for i in 0..50 {
        let _: i64 = client
            .call(hashes::HSet::new(
                "testhash",
                format!("field{}", i),
                format!("value{}", i),
            ))
            .await?;
    }
    println!("Created hash 'testhash' with 50 fields\n");

    // ========== SCAN Command ==========
    println!("=== SCAN: Cursor-based Key Iteration ===\n");

    println!("Pattern: SCAN demonstrates Level 3 complexity:");
    println!("  - Returns tuple (cursor, results)");
    println!("  - Requires iteration state management");
    println!("  - Builder pattern for optional MATCH/COUNT\n");

    // Basic SCAN - iterate all keys
    println!("1. Basic SCAN (no pattern):");
    let mut cursor = 0u64;
    let mut total_keys = 0;
    let mut iterations = 0;

    loop {
        let result = client.call(scan::Scan::new(cursor)).await?;

        iterations += 1;
        total_keys += result.keys.len();

        println!(
            "   Iteration {}: cursor={}, found {} keys",
            iterations,
            result.cursor,
            result.keys.len()
        );

        cursor = result.cursor;
        if cursor == 0 {
            break;
        }
    }
    println!(
        "   Total: {} keys in {} iterations\n",
        total_keys, iterations
    );

    // SCAN with pattern matching
    println!("2. SCAN with MATCH pattern:");
    cursor = 0;
    let mut matched_keys = Vec::new();
    iterations = 0;

    loop {
        let result = client
            .call(scan::Scan::new(cursor).pattern("testkey:1*"))
            .await?;

        iterations += 1;
        for key in &result.keys {
            let key_str = String::from_utf8_lossy(key);
            matched_keys.push(key_str.to_string());
        }

        cursor = result.cursor;
        if cursor == 0 {
            break;
        }
    }

    println!("   Found {} keys matching 'testkey:1*'", matched_keys.len());
    println!(
        "   Sample: {:?}",
        &matched_keys[..matched_keys.len().min(5)]
    );
    println!("   Iterations: {}\n", iterations);

    // SCAN with COUNT hint
    println!("3. SCAN with COUNT hint (request more per iteration):");
    cursor = 0;
    iterations = 0;
    let mut keys_per_iter = Vec::new();

    loop {
        let result = client.call(scan::Scan::new(cursor).count(20)).await?;

        iterations += 1;
        keys_per_iter.push(result.keys.len());

        cursor = result.cursor;
        if cursor == 0 {
            break;
        }
    }

    println!(
        "   Total iterations: {} (typically fewer with COUNT)",
        iterations
    );
    println!("   Keys per iteration: {:?}\n", keys_per_iter);

    // ========== HSCAN Command ==========
    println!("=== HSCAN: Hash Field Iteration ===\n");

    println!("Pattern: HSCAN returns field-value pairs");
    println!("  - Similar to SCAN but for hash fields");
    println!("  - Returns Vec<(Bytes, Bytes)> instead of Vec<Bytes>\n");

    cursor = 0;
    let mut total_fields = 0;
    iterations = 0;
    let mut sample_fields = Vec::new();

    loop {
        let result = client.call(scan::HScan::new("testhash", cursor)).await?;

        iterations += 1;
        total_fields += result.fields.len();

        // Collect first few fields as samples
        if sample_fields.len() < 5 {
            for (field, value) in &result.fields {
                if sample_fields.len() < 5 {
                    sample_fields.push((
                        String::from_utf8_lossy(field).to_string(),
                        String::from_utf8_lossy(value).to_string(),
                    ));
                }
            }
        }

        println!(
            "   Iteration {}: cursor={}, found {} fields",
            iterations,
            result.cursor,
            result.fields.len()
        );

        cursor = result.cursor;
        if cursor == 0 {
            break;
        }
    }

    println!(
        "   Total: {} fields in {} iterations",
        total_fields, iterations
    );
    println!("   Sample fields:");
    for (field, value) in &sample_fields {
        println!("     {}: {}", field, value);
    }
    println!();

    // ========== Cleanup ==========
    println!("=== Cleanup ===\n");

    // Delete all test keys
    let mut keys_to_delete = Vec::new();
    for i in 0..100 {
        keys_to_delete.push(format!("testkey:{}", i));
    }
    keys_to_delete.push("testhash".to_string());

    let deleted: i64 = client.call(strings::Del::new(keys_to_delete)).await?;
    println!("Deleted {} keys\n", deleted);

    // ========== Summary ==========
    println!("=== Level 3 Command Patterns Demonstrated ===\n");

    println!("Custom Response Types:");
    println!("  ✓ ScanResult {{ cursor: u64, keys: Vec<Bytes> }}");
    println!("  ✓ HScanResult {{ cursor: u64, fields: Vec<(Bytes, Bytes)> }}\n");

    println!("Builder Pattern:");
    println!("  ✓ Scan::new(cursor).pattern(\"*\").count(10)");
    println!("  ✓ HScan::new(key, cursor).pattern(\"field*\")\n");

    println!("Iteration Patterns:");
    println!("  ✓ Cursor-based loops (while cursor != 0)");
    println!("  ✓ State management across multiple calls");
    println!("  ✓ Pattern matching and count hints\n");

    println!("These patterns enable:");
    println!("  • Scanning millions of keys without blocking");
    println!("  • Type-safe iteration with compile-time guarantees");
    println!("  • Memory-efficient processing of large datasets");

    Ok(())
}
