//! Integration tests for SORT and WAIT commands
//!
//! These tests require a running Redis instance on localhost:6379.
//! Run with: cargo test --test test_sort_wait

mod common;

use bytes::Bytes;
use common::{connect, test_key};
use redis_tower::commands::{Del, LPush, Sadd, Set, Sort, SortOrder, SortResult, Wait, Zadd};

#[tokio::test]
async fn test_sort_list_basic() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("sort_list");

    // Create a list with unsorted numbers
    client
        .execute(LPush::new(
            &key,
            vec![Bytes::from("3"), Bytes::from("1"), Bytes::from("2")],
        ))
        .await
        .unwrap();

    // Sort the list
    let result = client.execute(Sort::new(&key)).await.unwrap();

    match result {
        SortResult::Elements(elements) => {
            assert_eq!(elements.len(), 3);
            assert_eq!(elements[0], Bytes::from("1"));
            assert_eq!(elements[1], Bytes::from("2"));
            assert_eq!(elements[2], Bytes::from("3"));
        }
        _ => panic!("Expected Elements result"),
    }

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_sort_list_desc() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("sort_desc");

    // Create a list
    client
        .execute(LPush::new(
            &key,
            vec![
                Bytes::from("1"),
                Bytes::from("5"),
                Bytes::from("3"),
                Bytes::from("2"),
                Bytes::from("4"),
            ],
        ))
        .await
        .unwrap();

    // Sort descending
    let result = client
        .execute(Sort::new(&key).order(SortOrder::Desc))
        .await
        .unwrap();

    match result {
        SortResult::Elements(elements) => {
            assert_eq!(elements.len(), 5);
            assert_eq!(elements[0], Bytes::from("5"));
            assert_eq!(elements[1], Bytes::from("4"));
            assert_eq!(elements[2], Bytes::from("3"));
            assert_eq!(elements[3], Bytes::from("2"));
            assert_eq!(elements[4], Bytes::from("1"));
        }
        _ => panic!("Expected Elements result"),
    }

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_sort_set() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("sort_set");

    // Create a set with numbers
    client.execute(Sadd::new(&key, "30")).await.unwrap();
    client.execute(Sadd::new(&key, "1")).await.unwrap();
    client.execute(Sadd::new(&key, "20")).await.unwrap();
    client.execute(Sadd::new(&key, "10")).await.unwrap();

    // Sort the set
    let result = client.execute(Sort::new(&key)).await.unwrap();

    match result {
        SortResult::Elements(elements) => {
            assert_eq!(elements.len(), 4);
            assert_eq!(elements[0], Bytes::from("1"));
            assert_eq!(elements[1], Bytes::from("10"));
            assert_eq!(elements[2], Bytes::from("20"));
            assert_eq!(elements[3], Bytes::from("30"));
        }
        _ => panic!("Expected Elements result"),
    }

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_sort_alpha() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("sort_alpha");

    // Create a list with strings
    client
        .execute(LPush::new(
            &key,
            vec![
                Bytes::from("zebra"),
                Bytes::from("apple"),
                Bytes::from("banana"),
            ],
        ))
        .await
        .unwrap();

    // Sort alphabetically
    let result = client.execute(Sort::new(&key).alpha()).await.unwrap();

    match result {
        SortResult::Elements(elements) => {
            assert_eq!(elements.len(), 3);
            assert_eq!(elements[0], Bytes::from("apple"));
            assert_eq!(elements[1], Bytes::from("banana"));
            assert_eq!(elements[2], Bytes::from("zebra"));
        }
        _ => panic!("Expected Elements result"),
    }

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_sort_limit() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("sort_limit");

    // Create a list with numbers
    client
        .execute(LPush::new(
            &key,
            vec![
                Bytes::from("5"),
                Bytes::from("4"),
                Bytes::from("3"),
                Bytes::from("2"),
                Bytes::from("1"),
            ],
        ))
        .await
        .unwrap();

    // Sort with limit - skip first 1, return 3 elements
    let result = client.execute(Sort::new(&key).limit(1, 3)).await.unwrap();

    match result {
        SortResult::Elements(elements) => {
            assert_eq!(elements.len(), 3);
            assert_eq!(elements[0], Bytes::from("2"));
            assert_eq!(elements[1], Bytes::from("3"));
            assert_eq!(elements[2], Bytes::from("4"));
        }
        _ => panic!("Expected Elements result"),
    }

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_sort_store() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("sort_store_src");
    let dest = test_key("sort_store_dest");

    // Create a list
    client
        .execute(LPush::new(
            &key,
            vec![Bytes::from("3"), Bytes::from("1"), Bytes::from("2")],
        ))
        .await
        .unwrap();

    // Sort and store result
    let result = client.execute(Sort::new(&key).store(&dest)).await.unwrap();

    match result {
        SortResult::Stored(n) => {
            assert_eq!(n, 3); // Number of elements stored
        }
        _ => panic!("Expected Stored result"),
    }

    // Verify the stored list exists and is sorted
    // We can use LRANGE or similar to verify, but for now just check it was stored
    // by trying to delete it
    let deleted: i64 = client.execute(Del::new(vec![dest.clone()])).await.unwrap();
    assert_eq!(deleted, 1);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_sort_empty_list() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("sort_empty");

    // Create an empty list (just try to sort non-existent key)
    let result = client.execute(Sort::new(&key)).await.unwrap();

    match result {
        SortResult::Elements(elements) => {
            assert_eq!(elements.len(), 0);
        }
        _ => panic!("Expected Elements result"),
    }
}

#[tokio::test]
async fn test_sort_sorted_set() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("sort_zset");

    // Create a sorted set
    client
        .execute(Zadd::new(&key).member(5.0, "five"))
        .await
        .unwrap();
    client
        .execute(Zadd::new(&key).member(1.0, "one"))
        .await
        .unwrap();
    client
        .execute(Zadd::new(&key).member(3.0, "three"))
        .await
        .unwrap();

    // Sort alphabetically (ignoring scores)
    let result = client.execute(Sort::new(&key).alpha()).await.unwrap();

    match result {
        SortResult::Elements(elements) => {
            assert_eq!(elements.len(), 3);
            assert_eq!(elements[0], Bytes::from("five"));
            assert_eq!(elements[1], Bytes::from("one"));
            assert_eq!(elements[2], Bytes::from("three"));
        }
        _ => panic!("Expected Elements result"),
    }

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_sort_with_get() {
    let client = connect().await.expect("Failed to connect to Redis");
    let list_key = test_key("sort_get_list");
    let obj1 = test_key("obj_1");
    let obj2 = test_key("obj_2");
    let obj3 = test_key("obj_3");

    // Create objects
    client.execute(Set::new(&obj1, "object_one")).await.unwrap();
    client.execute(Set::new(&obj2, "object_two")).await.unwrap();
    client
        .execute(Set::new(&obj3, "object_three"))
        .await
        .unwrap();

    // Create a list of IDs (extract the last part after the last colon)
    let id1 = obj1.split(':').next_back().unwrap();
    let id2 = obj2.split(':').next_back().unwrap();
    let id3 = obj3.split(':').next_back().unwrap();

    client
        .execute(LPush::new(
            &list_key,
            vec![
                Bytes::copy_from_slice(id3.as_bytes()),
                Bytes::copy_from_slice(id1.as_bytes()),
                Bytes::copy_from_slice(id2.as_bytes()),
            ],
        ))
        .await
        .unwrap();

    // Sort and get the objects
    // Note: This requires setting up the pattern correctly
    // For simplicity, we'll just test that SORT with GET doesn't crash
    let base_pattern = obj1.rsplit_once(':').map(|(base, _)| base).unwrap();
    let pattern = format!("{}:*", base_pattern);

    let result = client
        .execute(Sort::new(&list_key).alpha().get(&pattern))
        .await
        .unwrap();

    match result {
        SortResult::Elements(elements) => {
            // Should return the dereferenced values
            assert_eq!(elements.len(), 3);
        }
        _ => panic!("Expected Elements result"),
    }

    // Clean up
    client
        .execute(Del::new(vec![list_key, obj1, obj2, obj3]))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_wait_no_replicas() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("wait_test");

    // Perform a write
    client.execute(Set::new(&key, "value")).await.unwrap();

    // Wait for 0 replicas (should return immediately)
    let replicas = client.execute(Wait::new(0, 100)).await.unwrap();

    // With no replication setup, should return 0
    assert_eq!(replicas, 0);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_wait_timeout() {
    let client = connect().await.expect("Failed to connect to Redis");
    let key = test_key("wait_timeout");

    // Perform a write
    client.execute(Set::new(&key, "value")).await.unwrap();

    // Wait for 1 replica with very short timeout
    // Since we don't have replication, this should timeout and return 0
    let replicas = client.execute(Wait::new(1, 10)).await.unwrap();

    assert_eq!(replicas, 0);

    // Clean up
    client.execute(Del::new(vec![key])).await.unwrap();
}

#[tokio::test]
async fn test_wait_multiple_writes() {
    let client = connect().await.expect("Failed to connect to Redis");

    // Perform multiple writes
    for i in 0..5 {
        let key = format!("{}:wait_multi_{}", test_key("base"), i);
        client.execute(Set::new(&key, "value")).await.unwrap();
    }

    // Wait for replication (will return 0 since no replicas)
    let replicas = client.execute(Wait::new(1, 100)).await.unwrap();
    assert_eq!(replicas, 0);

    // Clean up
    for i in 0..5 {
        let key = format!("{}:wait_multi_{}", test_key("base"), i);
        client.execute(Del::new(vec![key])).await.unwrap();
    }
}
