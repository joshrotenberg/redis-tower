//! Example demonstrating streaming SCAN APIs
//!
//! This example shows how to use the streaming wrappers for SCAN, HSCAN, SSCAN, and ZSCAN
//! to iterate over keys, hash fields, set members, and sorted set members.
//!
//! Run with: cargo run --example streaming_scan

use redis_tower::RedisClient;
use redis_tower::commands::{Del, HSet, Sadd, Set, Zadd};
use redis_tower::streaming::{HScanStream, SScanStream, ScanStream, ZScanStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Redis Streaming SCAN API Demo ===\n");

    let client = RedisClient::connect("localhost:6379").await?;

    // Clean up any existing test data
    let _ = client
        .call(Del::new(vec![
            "scan_test:1".to_string(),
            "scan_test:2".to_string(),
            "scan_test:3".to_string(),
            "scan_test:other".to_string(),
            "myhash".to_string(),
            "myset".to_string(),
            "myzset".to_string(),
        ]))
        .await;

    // === SCAN: Iterate over all keys ===
    println!("1. SCAN - Iterating over keys with pattern matching\n");

    // Add some test keys
    for i in 1..=3 {
        client
            .call(Set::new(format!("scan_test:{}", i), format!("value{}", i)))
            .await?;
    }
    client
        .call(Set::new("scan_test:other", "other_value"))
        .await?;

    // Stream all keys matching pattern
    let mut scan_stream = ScanStream::new(client.clone())
        .pattern("scan_test:*")
        .count(10);

    println!("Keys matching 'scan_test:*':");
    while let Some(keys) = scan_stream.next().await? {
        for key in keys {
            println!("  - {}", String::from_utf8_lossy(&key));
        }
    }
    println!();

    // === HSCAN: Iterate over hash fields ===
    println!("2. HSCAN - Iterating over hash fields\n");

    // Populate a hash
    client.call(HSet::new("myhash", "field1", "value1")).await?;
    client.call(HSet::new("myhash", "field2", "value2")).await?;
    client.call(HSet::new("myhash", "field3", "value3")).await?;
    client
        .call(HSet::new("myhash", "other_field", "other_value"))
        .await?;

    // Stream hash fields with pattern
    let mut hscan_stream = HScanStream::new(client.clone(), "myhash")
        .pattern("field*")
        .count(10);

    println!("Hash fields matching 'field*' in 'myhash':");
    while let Some(fields) = hscan_stream.next().await? {
        for (field, value) in fields {
            println!(
                "  {} = {}",
                String::from_utf8_lossy(&field),
                String::from_utf8_lossy(&value)
            );
        }
    }
    println!();

    // === SSCAN: Iterate over set members ===
    println!("3. SSCAN - Iterating over set members\n");

    // Populate a set
    for i in 1..=5 {
        client
            .call(Sadd::new("myset", format!("member{}", i).into_bytes()))
            .await?;
    }

    // Stream all set members
    let mut sscan_stream = SScanStream::new(client.clone(), "myset").count(2);

    println!("Set members in 'myset' (batches of 2):");
    let mut batch_num = 1;
    while let Some(members) = sscan_stream.next().await? {
        println!("  Batch {}:", batch_num);
        for member in members {
            println!("    - {}", String::from_utf8_lossy(&member));
        }
        batch_num += 1;
    }
    println!();

    // === ZSCAN: Iterate over sorted set members and scores ===
    println!("4. ZSCAN - Iterating over sorted set members with scores\n");

    // Populate a sorted set
    let mut zadd = Zadd::new("myzset");
    zadd.add(100.0, b"player1".to_vec());
    zadd.add(200.0, b"player2".to_vec());
    zadd.add(300.0, b"player3".to_vec());
    zadd.add(150.0, b"player4".to_vec());
    client.call(zadd).await?;

    // Stream sorted set members with scores
    let mut zscan_stream = ZScanStream::new(client.clone(), "myzset").count(2);

    println!("Sorted set members in 'myzset' (batches of 2):");
    let mut batch_num = 1;
    while let Some(members) = zscan_stream.next().await? {
        println!("  Batch {}:", batch_num);
        for (member, score) in members {
            println!("    {} => {}", String::from_utf8_lossy(&member), score);
        }
        batch_num += 1;
    }
    println!();

    // === Demonstrate stream reset ===
    println!("5. Stream Reset - Iterating again from the beginning\n");

    // Reset and iterate again
    sscan_stream.reset();
    println!("Set members after reset:");
    while let Some(members) = sscan_stream.next().await? {
        for member in members {
            println!("  - {}", String::from_utf8_lossy(&member));
        }
    }
    println!();

    // === Large dataset demonstration ===
    println!("6. Large Dataset - Streaming 1000 keys efficiently\n");

    // Clean up first
    let _ = client.call(Del::new(vec!["largeset".to_string()])).await;

    // Add 1000 members to a set
    println!("Adding 1000 members to set...");
    for i in 0..1000 {
        client
            .call(Sadd::new(
                "largeset",
                format!("member{:04}", i).into_bytes(),
            ))
            .await?;
    }

    // Stream them all
    let mut count = 0;
    let mut sscan_large = SScanStream::new(client.clone(), "largeset").count(100);

    println!("Streaming 1000 members (count=100):");
    while let Some(members) = sscan_large.next().await? {
        count += members.len();
        println!(
            "  Received batch of {} members (total so far: {})",
            members.len(),
            count
        );
    }
    println!("Total members streamed: {}\n", count);

    // Clean up
    let _ = client
        .call(Del::new(vec![
            "scan_test:1".to_string(),
            "scan_test:2".to_string(),
            "scan_test:3".to_string(),
            "scan_test:other".to_string(),
            "myhash".to_string(),
            "myset".to_string(),
            "myzset".to_string(),
            "largeset".to_string(),
        ]))
        .await;

    println!("=== Demo Complete ===");
    Ok(())
}
