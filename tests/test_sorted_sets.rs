//! Integration tests for Sorted Set commands.
//!
//! These tests require a running Redis instance on localhost:6379.
//! Run with: cargo test --test test_sorted_sets

mod common;

use common::{connect, test_key};
use redis_tower::commands::{Del, Zadd, Zcard, Zincrby, Zrange, Zrank, Zrem, Zrevrank, Zscore};

#[tokio::test]
async fn test_zadd_basic() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("zadd");

    // Add single member
    let added = client
        .execute(Zadd::new(&key).member(1.0, "one"))
        .await
        .unwrap();
    assert_eq!(added, 1);

    // Add multiple members
    let added = client
        .execute(Zadd::new(&key).member(2.0, "two").member(3.0, "three"))
        .await
        .unwrap();
    assert_eq!(added, 2);

    // Get cardinality
    let count = client.execute(Zcard::new(&key)).await.unwrap();
    assert_eq!(count, 3);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_zadd_update() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("zadd_update");

    // Add member
    client
        .execute(Zadd::new(&key).member(1.0, "member"))
        .await
        .unwrap();

    // Update score
    let added = client
        .execute(Zadd::new(&key).member(5.0, "member"))
        .await
        .unwrap();
    assert_eq!(added, 0); // No new members added, just updated

    // Verify score
    let score = client.execute(Zscore::new(&key, "member")).await.unwrap();
    assert_eq!(score, Some(5.0));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_zrem() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("zrem");

    // Add members
    client
        .execute(
            Zadd::new(&key)
                .member(1.0, "one")
                .member(2.0, "two")
                .member(3.0, "three"),
        )
        .await
        .unwrap();

    // Remove one member
    let removed = client.execute(Zrem::new(&key).member("two")).await.unwrap();
    assert_eq!(removed, 1);

    // Remove multiple members (one exists, one doesn't)
    let removed = client
        .execute(Zrem::new(&key).member("one").member("nonexistent"))
        .await
        .unwrap();
    assert_eq!(removed, 1);

    // Verify only one member left
    let count = client.execute(Zcard::new(&key)).await.unwrap();
    assert_eq!(count, 1);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_zscore() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("zscore");

    // Add members with scores
    client
        .execute(
            Zadd::new(&key)
                .member(1.5, "one")
                .member(2.7, "two")
                .member(3.9, "three"),
        )
        .await
        .unwrap();

    // Get scores
    let score = client.execute(Zscore::new(&key, "one")).await.unwrap();
    assert_eq!(score, Some(1.5));

    let score = client.execute(Zscore::new(&key, "two")).await.unwrap();
    assert_eq!(score, Some(2.7));

    // Nonexistent member
    let score = client
        .execute(Zscore::new(&key, "nonexistent"))
        .await
        .unwrap();
    assert_eq!(score, None);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_zrange() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("zrange");

    // Add members
    client
        .execute(
            Zadd::new(&key)
                .member(1.0, "one")
                .member(2.0, "two")
                .member(3.0, "three")
                .member(4.0, "four")
                .member(5.0, "five"),
        )
        .await
        .unwrap();

    // Get all members with scores
    let result = client
        .execute(Zrange::new(&key, 0, -1).withscores())
        .await
        .unwrap();

    assert_eq!(result.members.len(), 5);
    assert_eq!(result.members[0].1, 1.0);
    assert_eq!(result.members[4].1, 5.0);

    // Get subset
    let result = client
        .execute(Zrange::new(&key, 1, 3).withscores())
        .await
        .unwrap();

    assert_eq!(result.members.len(), 3);
    assert_eq!(String::from_utf8_lossy(&result.members[0].0), "two");
    assert_eq!(result.members[0].1, 2.0);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_zrank() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("zrank");

    // Add members
    client
        .execute(
            Zadd::new(&key)
                .member(10.0, "a")
                .member(20.0, "b")
                .member(30.0, "c")
                .member(40.0, "d"),
        )
        .await
        .unwrap();

    // Get rank (0-indexed from lowest score)
    let rank = client.execute(Zrank::new(&key, "a")).await.unwrap();
    assert_eq!(rank, Some(0)); // Lowest score

    let rank = client.execute(Zrank::new(&key, "d")).await.unwrap();
    assert_eq!(rank, Some(3)); // Highest score

    // Nonexistent member
    let rank = client
        .execute(Zrank::new(&key, "nonexistent"))
        .await
        .unwrap();
    assert_eq!(rank, None);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_zrevrank() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("zrevrank");

    // Add members
    client
        .execute(
            Zadd::new(&key)
                .member(10.0, "a")
                .member(20.0, "b")
                .member(30.0, "c")
                .member(40.0, "d"),
        )
        .await
        .unwrap();

    // Get reverse rank (0-indexed from highest score)
    let rank = client.execute(Zrevrank::new(&key, "d")).await.unwrap();
    assert_eq!(rank, Some(0)); // Highest score = rank 0

    let rank = client.execute(Zrevrank::new(&key, "a")).await.unwrap();
    assert_eq!(rank, Some(3)); // Lowest score = rank 3

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_zincrby() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("zincrby");

    // Add member
    client
        .execute(Zadd::new(&key).member(10.0, "score"))
        .await
        .unwrap();

    // Increment
    let new_score = client
        .execute(Zincrby::new(&key, 5.0, "score"))
        .await
        .unwrap();
    assert_eq!(new_score, 15.0);

    // Increment again
    let new_score = client
        .execute(Zincrby::new(&key, 2.5, "score"))
        .await
        .unwrap();
    assert_eq!(new_score, 17.5);

    // Decrement (negative increment)
    let new_score = client
        .execute(Zincrby::new(&key, -7.5, "score"))
        .await
        .unwrap();
    assert_eq!(new_score, 10.0);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_leaderboard_pattern() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("leaderboard");

    // Add players with scores
    client
        .execute(
            Zadd::new(&key)
                .member(1500.0, "alice")
                .member(2300.0, "bob")
                .member(1800.0, "charlie")
                .member(2100.0, "diana"),
        )
        .await
        .unwrap();

    // Get top 3 players (highest scores)
    let _top3 = client
        .execute(Zrange::new(&key, 0, 2).withscores())
        .await
        .unwrap();

    // ZRANGE returns lowest to highest, so we need the last 3
    // Actually, let me get all and check the last 3
    let all = client
        .execute(Zrange::new(&key, 0, -1).withscores())
        .await
        .unwrap();

    assert_eq!(all.members.len(), 4);
    // Verify they're sorted by score (ascending)
    assert!(all.members[0].1 < all.members[1].1);
    assert!(all.members[1].1 < all.members[2].1);
    assert!(all.members[2].1 < all.members[3].1);

    // Get alice's rank
    let rank = client.execute(Zrank::new(&key, "alice")).await.unwrap();
    assert_eq!(rank, Some(0)); // Lowest score

    // Get bob's reverse rank (from top)
    let rev_rank = client.execute(Zrevrank::new(&key, "bob")).await.unwrap();
    assert_eq!(rev_rank, Some(0)); // Highest score

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_score_updates() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("score_updates");

    // Add member
    client
        .execute(Zadd::new(&key).member(0.0, "counter"))
        .await
        .unwrap();

    // Increment multiple times
    for _ in 0..5 {
        client
            .execute(Zincrby::new(&key, 1.0, "counter"))
            .await
            .unwrap();
    }

    // Verify final score
    let score = client.execute(Zscore::new(&key, "counter")).await.unwrap();
    assert_eq!(score, Some(5.0));

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_empty_sorted_set() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("empty_zset");

    // ZCARD on non-existent key
    let count = client.execute(Zcard::new(&key)).await.unwrap();
    assert_eq!(count, 0);

    // ZSCORE on non-existent key
    let score = client.execute(Zscore::new(&key, "member")).await.unwrap();
    assert_eq!(score, None);

    // ZRANK on non-existent key
    let rank = client.execute(Zrank::new(&key, "member")).await.unwrap();
    assert_eq!(rank, None);

    // ZRANGE on non-existent key
    let result = client
        .execute(Zrange::new(&key, 0, -1).withscores())
        .await
        .unwrap();
    assert_eq!(result.members.len(), 0);
}
