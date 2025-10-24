//! Example demonstrating Redis Set commands (Phase 2)
//!
//! This showcases:
//! - SADD/SREM: Adding and removing members
//! - SMEMBERS: Getting all members
//! - SISMEMBER: Checking membership
//! - SCARD: Getting set size
//! - SINTER/SUNION/SDIFF: Set operations
//! - SSCAN: Iterating large sets

use redis_tower::client::RedisConnection;
use redis_tower::commands::{
    Del, Sadd, Scard, Sdiff, Sinter, Sismember, Smembers, Srem, Sscan, Sunion,
};
use std::collections::HashSet;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let client = RedisConnection::connect("127.0.0.1:6379").await?;
    println!("Connected to Redis");

    // Clean up from previous runs
    let _ = client
        .execute(Del::new(vec![
            "fruits".to_string(),
            "vegetables".to_string(),
            "colors".to_string(),
            "primary_colors".to_string(),
            "warm_colors".to_string(),
            "cool_colors".to_string(),
            "large_set".to_string(),
        ]))
        .await;

    println!("\n=== Example 1: SADD - Adding Members ===");

    // Add single member
    let added: i64 = client
        .execute(Sadd::new("fruits", b"apple".to_vec()))
        .await?;
    println!("SADD fruits apple -> {} member added", added);
    assert_eq!(added, 1);

    // Add multiple members at once
    let added: i64 = client
        .execute(
            Sadd::new("fruits", b"banana".to_vec())
                .member(b"orange".to_vec())
                .member(b"grape".to_vec()),
        )
        .await?;
    println!("SADD fruits banana orange grape -> {} members added", added);
    assert_eq!(added, 3);

    // Try adding duplicate (returns 0)
    let added: i64 = client
        .execute(Sadd::new("fruits", b"apple".to_vec()))
        .await?;
    println!(
        "SADD fruits apple (duplicate) -> {} (already exists)",
        added
    );
    assert_eq!(added, 0);

    println!("\n=== Example 2: SMEMBERS - Getting All Members ===");

    let members = client.execute(Smembers::new("fruits")).await?;
    println!("SMEMBERS fruits -> {} members:", members.len());
    for member in &members {
        println!("  - {}", String::from_utf8_lossy(member));
    }
    assert_eq!(members.len(), 4);

    println!("\n=== Example 3: SISMEMBER - Checking Membership ===");

    let exists: bool = client
        .execute(Sismember::new("fruits", b"apple".to_vec()))
        .await?;
    println!("SISMEMBER fruits apple -> {}", exists);
    assert!(exists);

    let exists: bool = client
        .execute(Sismember::new("fruits", b"carrot".to_vec()))
        .await?;
    println!("SISMEMBER fruits carrot -> {}", exists);
    assert!(!exists);

    println!("\n=== Example 4: SCARD - Getting Set Size ===");

    let size: i64 = client.execute(Scard::new("fruits")).await?;
    println!("SCARD fruits -> {} members", size);
    assert_eq!(size, 4);

    println!("\n=== Example 5: SREM - Removing Members ===");

    let removed: i64 = client
        .execute(Srem::new("fruits", b"grape".to_vec()))
        .await?;
    println!("SREM fruits grape -> {} member removed", removed);
    assert_eq!(removed, 1);

    // Remove multiple members
    let removed: i64 = client
        .execute(Srem::new("fruits", b"apple".to_vec()).member(b"banana".to_vec()))
        .await?;
    println!("SREM fruits apple banana -> {} members removed", removed);
    assert_eq!(removed, 2);

    let size: i64 = client.execute(Scard::new("fruits")).await?;
    println!("SCARD fruits (after removal) -> {} members", size);
    assert_eq!(size, 1); // Only "orange" remains

    println!("\n=== Example 6: Set Operations Setup ===");

    // Create three color sets
    client
        .execute(
            Sadd::new("primary_colors", b"red".to_vec())
                .member(b"blue".to_vec())
                .member(b"yellow".to_vec()),
        )
        .await?;
    println!("Created set: primary_colors (red, blue, yellow)");

    client
        .execute(
            Sadd::new("warm_colors", b"red".to_vec())
                .member(b"orange".to_vec())
                .member(b"yellow".to_vec()),
        )
        .await?;
    println!("Created set: warm_colors (red, orange, yellow)");

    client
        .execute(
            Sadd::new("cool_colors", b"blue".to_vec())
                .member(b"green".to_vec())
                .member(b"purple".to_vec()),
        )
        .await?;
    println!("Created set: cool_colors (blue, green, purple)");

    println!("\n=== Example 7: SINTER - Set Intersection ===");

    let intersection = client
        .execute(Sinter::new("primary_colors").key("warm_colors"))
        .await?;
    println!(
        "SINTER primary_colors warm_colors -> {} members:",
        intersection.len()
    );
    for member in &intersection {
        println!("  - {}", String::from_utf8_lossy(member));
    }
    // Should be: red, yellow (common to both)
    assert_eq!(intersection.len(), 2);

    let intersection = client
        .execute(
            Sinter::new("primary_colors")
                .key("warm_colors")
                .key("cool_colors"),
        )
        .await?;
    println!(
        "SINTER primary_colors warm_colors cool_colors -> {} members (none in all three)",
        intersection.len()
    );
    assert_eq!(intersection.len(), 0);

    println!("\n=== Example 8: SUNION - Set Union ===");

    let union = client
        .execute(Sunion::new("primary_colors").key("cool_colors"))
        .await?;
    println!(
        "SUNION primary_colors cool_colors -> {} members:",
        union.len()
    );
    for member in &union {
        println!("  - {}", String::from_utf8_lossy(member));
    }
    // Should be: red, blue, yellow, green, purple (all unique colors)
    assert_eq!(union.len(), 5);

    println!("\n=== Example 9: SDIFF - Set Difference ===");

    let diff = client
        .execute(Sdiff::new("primary_colors").key("warm_colors"))
        .await?;
    println!(
        "SDIFF primary_colors warm_colors -> {} members (in primary but not warm):",
        diff.len()
    );
    for member in &diff {
        println!("  - {}", String::from_utf8_lossy(member));
    }
    // Should be: blue (in primary but not in warm)
    assert_eq!(diff.len(), 1);
    assert_eq!(String::from_utf8_lossy(&diff[0]), "blue");

    let diff = client
        .execute(Sdiff::new("warm_colors").key("primary_colors"))
        .await?;
    println!(
        "SDIFF warm_colors primary_colors -> {} members (in warm but not primary):",
        diff.len()
    );
    for member in &diff {
        println!("  - {}", String::from_utf8_lossy(member));
    }
    // Should be: orange (in warm but not in primary)
    assert_eq!(diff.len(), 1);
    assert_eq!(String::from_utf8_lossy(&diff[0]), "orange");

    println!("\n=== Example 10: SSCAN - Iterating Large Sets ===");

    // Create a large set
    let mut sadd_cmd = Sadd::new("large_set", b"item:0".to_vec());
    for i in 1..100 {
        sadd_cmd = sadd_cmd.member(format!("item:{}", i).into_bytes());
    }
    let added: i64 = client.execute(sadd_cmd).await?;
    println!("Created large_set with {} members", added);

    // Scan through the set
    let mut cursor = 0u64;
    let mut total_scanned = 0;
    let mut iterations = 0;

    loop {
        let result = client
            .execute(Sscan::new("large_set", cursor).count(10))
            .await?;

        iterations += 1;
        total_scanned += result.members.len();
        println!(
            "SSCAN iteration {}: cursor {} -> {} -> {} members",
            iterations,
            cursor,
            result.cursor,
            result.members.len()
        );

        cursor = result.cursor;
        if cursor == 0 {
            break;
        }
    }

    println!(
        "SSCAN complete: scanned {} members in {} iterations",
        total_scanned, iterations
    );

    println!("\n=== Example 11: SSCAN with MATCH Pattern ===");

    // Add some members with different patterns
    client
        .execute(
            Sadd::new("colors", b"red:light".to_vec())
                .member(b"red:dark".to_vec())
                .member(b"blue:light".to_vec())
                .member(b"blue:dark".to_vec())
                .member(b"green".to_vec())
                .member(b"yellow".to_vec()),
        )
        .await?;

    // Scan for only "red:*" members
    let mut cursor = 0u64;
    let mut red_members = Vec::new();

    loop {
        let result = client
            .execute(Sscan::new("colors", cursor).pattern("red:*").count(100))
            .await?;

        red_members.extend(result.members);
        cursor = result.cursor;
        if cursor == 0 {
            break;
        }
    }

    println!("SSCAN colors MATCH red:* -> {} members:", red_members.len());
    for member in &red_members {
        println!("  - {}", String::from_utf8_lossy(member));
    }
    assert_eq!(red_members.len(), 2);

    println!("\n=== Example 12: Practical Use Case - Tag System ===");

    // Tag articles with categories
    client
        .execute(
            Sadd::new("article:1:tags", b"rust".to_vec())
                .member(b"programming".to_vec())
                .member(b"systems".to_vec()),
        )
        .await?;

    client
        .execute(
            Sadd::new("article:2:tags", b"rust".to_vec())
                .member(b"async".to_vec())
                .member(b"programming".to_vec()),
        )
        .await?;

    client
        .execute(
            Sadd::new("article:3:tags", b"python".to_vec())
                .member(b"programming".to_vec())
                .member(b"web".to_vec()),
        )
        .await?;

    println!("Created article tag sets");

    // Find common tags between article 1 and 2
    let common_tags = client
        .execute(Sinter::new("article:1:tags").key("article:2:tags"))
        .await?;
    println!(
        "Common tags between article 1 and 2 ({}): {:?}",
        common_tags.len(),
        common_tags
            .iter()
            .map(|t| String::from_utf8_lossy(t))
            .collect::<Vec<_>>()
    );

    // Find all unique tags across all articles
    let all_tags = client
        .execute(
            Sunion::new("article:1:tags")
                .key("article:2:tags")
                .key("article:3:tags"),
        )
        .await?;
    println!(
        "All unique tags across articles ({}): {:?}",
        all_tags.len(),
        all_tags
            .iter()
            .map(|t| String::from_utf8_lossy(t))
            .collect::<HashSet<_>>()
    );

    // Tags unique to article 1 (not in 2 or 3)
    let unique_tags = client
        .execute(
            Sdiff::new("article:1:tags")
                .key("article:2:tags")
                .key("article:3:tags"),
        )
        .await?;
    println!(
        "Tags unique to article 1 ({}): {:?}",
        unique_tags.len(),
        unique_tags
            .iter()
            .map(|t| String::from_utf8_lossy(t))
            .collect::<Vec<_>>()
    );

    println!("\n=== All set operations demonstrated! ===");
    println!("\nCommands implemented:");
    println!("  ✅ SADD - Add members to set");
    println!("  ✅ SREM - Remove members from set");
    println!("  ✅ SMEMBERS - Get all members");
    println!("  ✅ SISMEMBER - Check membership");
    println!("  ✅ SCARD - Get set size");
    println!("  ✅ SINTER - Set intersection");
    println!("  ✅ SUNION - Set union");
    println!("  ✅ SDIFF - Set difference");
    println!("  ✅ SSCAN - Iterate members with pattern matching");

    Ok(())
}
