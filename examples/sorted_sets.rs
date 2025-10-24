//! Examples demonstrating sorted set operations with redis-tower.
//!
//! Run with: cargo run --example sorted_sets

use redis_tower::client::RedisConnection;
use redis_tower::commands::sorted_sets::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = RedisConnection::connect("127.0.0.1:6379").await?;

    println!("=== Sorted Sets Examples ===\n");

    // Example 1: Basic leaderboard
    example_leaderboard(&client).await?;

    // Example 2: Top N queries
    example_top_n(&client).await?;

    // Example 3: Score increments (voting/likes)
    example_voting(&client).await?;

    // Example 4: Ranking system
    example_ranking(&client).await?;

    // Example 5: Time-series with scores as timestamps
    example_timeseries(&client).await?;

    // Example 6: Priority queue
    example_priority_queue(&client).await?;

    // Example 7: Range queries with scores
    example_range_queries(&client).await?;

    // Example 8: Conditional updates (NX, XX, GT, LT)
    example_conditional_updates(&client).await?;

    // Example 9: Scanning large sorted sets
    example_scanning(&client).await?;

    // Example 10: Leaderboard with ties
    example_ties(&client).await?;

    Ok(())
}

async fn example_leaderboard(client: &RedisConnection) -> Result<(), Box<dyn std::error::Error>> {
    println!("1. Basic Leaderboard");

    // Add players to leaderboard
    let added = client
        .execute(
            Zadd::new("game:leaderboard")
                .member(1500.0, "alice")
                .member(2300.0, "bob")
                .member(1800.0, "charlie")
                .member(2100.0, "diana"),
        )
        .await?;
    println!("   Added {} players to leaderboard", added);

    // Get top 3 players
    let top3 = client
        .execute(Zrevrange::new("game:leaderboard", 0, 2).withscores())
        .await?;

    println!("   Top 3 players:");
    for (i, (player, score)) in top3.members.iter().enumerate() {
        println!(
            "   {}. {} - {} points",
            i + 1,
            String::from_utf8_lossy(player),
            score
        );
    }

    // Get alice's rank (0-based from lowest score)
    if let Some(rank) = client
        .execute(Zrank::new("game:leaderboard", "alice"))
        .await?
    {
        let reverse_rank = client
            .execute(Zrevrank::new("game:leaderboard", "alice"))
            .await?
            .unwrap();
        println!(
            "   Alice's rank: {} (or {} from top)",
            rank,
            reverse_rank + 1
        );
    }

    println!();
    Ok(())
}

async fn example_top_n(client: &RedisConnection) -> Result<(), Box<dyn std::error::Error>> {
    println!("2. Top N Queries (Most Viewed Articles)");

    // Track article views
    client
        .execute(
            Zadd::new("articles:views")
                .member(1250.0, "how-to-rust")
                .member(3420.0, "async-await-guide")
                .member(890.0, "intro-to-tower")
                .member(5670.0, "redis-patterns")
                .member(2100.0, "error-handling"),
        )
        .await?;

    // Get top 3 most viewed
    let top_articles = client
        .execute(Zrevrange::new("articles:views", 0, 2).withscores())
        .await?;

    println!("   Top 3 most viewed articles:");
    for (article, views) in &top_articles.members {
        println!(
            "   - {} ({} views)",
            String::from_utf8_lossy(article),
            views
        );
    }

    // Get total number of articles
    let total = client.execute(Zcard::new("articles:views")).await?;
    println!("   Total articles: {}", total);

    println!();
    Ok(())
}

async fn example_voting(client: &RedisConnection) -> Result<(), Box<dyn std::error::Error>> {
    println!("3. Voting System (Upvotes)");

    // Initialize posts with initial vote counts
    client
        .execute(
            Zadd::new("post:votes")
                .member(0.0, "post:123")
                .member(0.0, "post:456")
                .member(0.0, "post:789"),
        )
        .await?;

    // Users upvote posts
    let new_score = client
        .execute(Zincrby::new("post:votes", 1.0, "post:123"))
        .await?;
    println!("   post:123 upvoted, new score: {}", new_score);

    let new_score = client
        .execute(Zincrby::new("post:votes", 1.0, "post:123"))
        .await?;
    println!("   post:123 upvoted again, new score: {}", new_score);

    let new_score = client
        .execute(Zincrby::new("post:votes", 1.0, "post:456"))
        .await?;
    println!("   post:456 upvoted, new score: {}", new_score);

    // Downvote (negative increment)
    let new_score = client
        .execute(Zincrby::new("post:votes", -1.0, "post:789"))
        .await?;
    println!("   post:789 downvoted, new score: {}", new_score);

    // Get current scores
    if let Some(score) = client
        .execute(Zscore::new("post:votes", "post:123"))
        .await?
    {
        println!("   post:123 current score: {}", score);
    }

    println!();
    Ok(())
}

async fn example_ranking(client: &RedisConnection) -> Result<(), Box<dyn std::error::Error>> {
    println!("4. Ranking System (Student Grades)");

    client
        .execute(
            Zadd::new("class:scores")
                .member(95.5, "student:alice")
                .member(87.3, "student:bob")
                .member(92.1, "student:charlie")
                .member(88.9, "student:diana")
                .member(95.5, "student:eve"), // Tie with alice
        )
        .await?;

    // Get diana's class rank
    if let Some(rank_from_bottom) = client
        .execute(Zrank::new("class:scores", "student:diana"))
        .await?
    {
        let rank_from_top = client
            .execute(Zrevrank::new("class:scores", "student:diana"))
            .await?
            .unwrap();
        println!(
            "   Diana's rank: {} from bottom, {} from top",
            rank_from_bottom + 1,
            rank_from_top + 1
        );

        if let Some(score) = client
            .execute(Zscore::new("class:scores", "student:diana"))
            .await?
        {
            println!("   Diana's score: {}", score);
        }
    }

    println!();
    Ok(())
}

async fn example_timeseries(client: &RedisConnection) -> Result<(), Box<dyn std::error::Error>> {
    println!("5. Time-Series Events (Scores as Timestamps)");

    // Use Unix timestamps as scores
    let now = 1700000000.0;
    client
        .execute(
            Zadd::new("events:log")
                .member(now - 3600.0, "user_login:alice")
                .member(now - 1800.0, "purchase:order123")
                .member(now - 900.0, "user_logout:alice")
                .member(now, "system_health_check"),
        )
        .await?;

    // Get all events in chronological order
    let events = client
        .execute(Zrange::new("events:log", 0, -1).withscores())
        .await?;

    println!("   Events in chronological order:");
    for (event, timestamp) in &events.members {
        println!("   [{}] {}", timestamp, String::from_utf8_lossy(event));
    }

    // Get most recent event
    let recent = client
        .execute(Zrevrange::new("events:log", 0, 0).withscores())
        .await?;
    if let Some((event, timestamp)) = recent.members.first() {
        println!(
            "   Most recent: {} at {}",
            String::from_utf8_lossy(event),
            timestamp
        );
    }

    println!();
    Ok(())
}

async fn example_priority_queue(
    client: &RedisConnection,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("6. Priority Queue (Task Scheduler)");

    // Add tasks with priority scores (lower = higher priority)
    client
        .execute(
            Zadd::new("tasks:queue")
                .member(1.0, "critical_security_patch")
                .member(3.0, "update_documentation")
                .member(2.0, "fix_performance_issue")
                .member(5.0, "refactor_old_code"),
        )
        .await?;

    // Get highest priority task (lowest score)
    let highest_priority = client
        .execute(Zrange::new("tasks:queue", 0, 0).withscores())
        .await?;

    if let Some((task, priority)) = highest_priority.members.first() {
        println!(
            "   Next task: {} (priority: {})",
            String::from_utf8_lossy(task),
            priority
        );

        // Process and remove task
        let removed = client
            .execute(Zrem::new("tasks:queue").member(task.clone()))
            .await?;
        println!("   Removed {} task from queue", removed);
    }

    // Show remaining tasks
    let remaining = client.execute(Zcard::new("tasks:queue")).await?;
    println!("   Remaining tasks: {}", remaining);

    println!();
    Ok(())
}

async fn example_range_queries(client: &RedisConnection) -> Result<(), Box<dyn std::error::Error>> {
    println!("7. Range Queries (Products by Price)");

    client
        .execute(
            Zadd::new("products:by_price")
                .member(9.99, "widget_small")
                .member(19.99, "widget_medium")
                .member(49.99, "widget_large")
                .member(99.99, "widget_xl")
                .member(14.99, "gadget_basic")
                .member(29.99, "gadget_pro"),
        )
        .await?;

    // Get all products in price order
    let all_products = client
        .execute(Zrange::new("products:by_price", 0, -1).withscores())
        .await?;

    println!("   All products by price:");
    for (product, price) in &all_products.members {
        println!("   ${:.2} - {}", price, String::from_utf8_lossy(product));
    }

    // Get cheapest 3 products
    let cheapest = client
        .execute(Zrange::new("products:by_price", 0, 2).withscores())
        .await?;

    println!("\n   Cheapest 3 products:");
    for (product, price) in &cheapest.members {
        println!("   ${:.2} - {}", price, String::from_utf8_lossy(product));
    }

    println!();
    Ok(())
}

async fn example_conditional_updates(
    client: &RedisConnection,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("8. Conditional Updates (NX, XX, GT, LT)");

    // Initialize with a base score
    client
        .execute(Zadd::new("player:stats").member(100.0, "health"))
        .await?;
    println!("   Initial health: 100");

    // Only update if NEW (NX) - will fail
    let added = client
        .execute(Zadd::new("player:stats").member(150.0, "health").nx())
        .await?;
    println!("   Tried to add with NX: {} changed", added);

    // Only update if EXISTS (XX) - will succeed
    let added = client
        .execute(Zadd::new("player:stats").member(120.0, "health").xx())
        .await?;
    println!("   Updated with XX: {} changed", added);

    if let Some(health) = client
        .execute(Zscore::new("player:stats", "health"))
        .await?
    {
        println!("   Current health: {}", health);
    }

    // Only update if GREATER (GT) - will succeed
    let added = client
        .execute(Zadd::new("player:stats").member(150.0, "health").gt())
        .await?;
    println!("   Updated with GT to 150: {} changed", added);

    // GT with lower value - will fail
    let added = client
        .execute(Zadd::new("player:stats").member(130.0, "health").gt())
        .await?;
    println!("   Tried GT with 130: {} changed", added);

    if let Some(health) = client
        .execute(Zscore::new("player:stats", "health"))
        .await?
    {
        println!("   Final health: {}", health);
    }

    println!();
    Ok(())
}

async fn example_scanning(client: &RedisConnection) -> Result<(), Box<dyn std::error::Error>> {
    println!("9. Scanning Large Sorted Sets");

    // Add many items
    let mut zadd = Zadd::new("large:set");
    for i in 0..50 {
        zadd = zadd.member(i as f64, format!("item:{}", i));
    }
    client.execute(zadd).await?;

    println!("   Added 50 items to sorted set");

    // Scan with pattern
    let mut cursor = 0u64;
    let mut total_found = 0;

    loop {
        let result = client
            .execute(Zscan::new("large:set", cursor).pattern("item:1*").count(10))
            .await?;

        total_found += result.members.len();
        cursor = result.cursor;

        if cursor == 0 {
            break;
        }
    }

    println!("   Found {} items matching 'item:1*'", total_found);

    // Scan and collect all
    cursor = 0;
    let mut all_items = Vec::new();

    loop {
        let result = client
            .execute(Zscan::new("large:set", cursor).count(20))
            .await?;

        all_items.extend(result.members);
        cursor = result.cursor;

        if cursor == 0 {
            break;
        }
    }

    println!("   Scanned all {} items", all_items.len());

    println!();
    Ok(())
}

async fn example_ties(client: &RedisConnection) -> Result<(), Box<dyn std::error::Error>> {
    println!("10. Handling Ties in Leaderboard");

    // Multiple players with same score
    client
        .execute(
            Zadd::new("tournament:scores")
                .member(100.0, "player_a")
                .member(100.0, "player_b")
                .member(95.0, "player_c")
                .member(100.0, "player_d")
                .member(105.0, "player_e"),
        )
        .await?;

    println!("   Tournament scores:");
    let results = client
        .execute(Zrevrange::new("tournament:scores", 0, -1).withscores())
        .await?;

    for (i, (player, score)) in results.members.iter().enumerate() {
        let rank = client
            .execute(Zrevrank::new("tournament:scores", player.clone()))
            .await?
            .unwrap();
        println!(
            "   Rank {}: {} - {} points (actual rank: {})",
            i + 1,
            String::from_utf8_lossy(player),
            score,
            rank
        );
    }

    println!("\n   Note: Players with same score maintain lexicographical order");

    println!();
    Ok(())
}
