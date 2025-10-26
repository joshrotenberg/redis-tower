//! Integration tests for Redis Bloom Filter module
//!
//! **Requires**: Redis Stack or RedisBloom module loaded
//!
//! These tests use the `redis/redis-stack-server` Docker image via testcontainers
//! which includes the RedisBloom module.
//!
//! Tests cover:
//! - BF.RESERVE - Create filter with custom parameters
//! - BF.ADD/BF.MADD - Add single/multiple items
//! - BF.EXISTS/BF.MEXISTS - Check single/multiple items
//! - BF.INFO - Get filter information
//! - BF.INSERT - Add with auto-creation
//! - BF.CARD - Get cardinality estimate
//!
//! Run with: cargo test --test integration_bloom --features bloom

use redis_tower::client::RedisClient;
use redis_tower::modules::bloom::*;
use testcontainers::GenericImage;
use testcontainers::core::WaitFor;
use testcontainers::runners::AsyncRunner;

/// Helper to create a Redis Stack client for bloom filter testing
async fn setup_redis_stack() -> RedisClient {
    let image = GenericImage::new("redis/redis-stack-server", "latest")
        .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"));

    let container = image
        .start()
        .await
        .expect("Failed to start Redis Stack container");

    let host = container.get_host().await.expect("Failed to get host");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get port");

    let client = RedisClient::connect(&format!("{}:{}", host, port))
        .await
        .expect("Failed to connect to Redis Stack");

    // Keep container alive by leaking it
    std::mem::forget(container);

    client
}

#[tokio::test]
#[cfg(feature = "bloom")]
async fn test_bf_reserve_and_add() {
    let client = setup_redis_stack().await;
    let filter = "test:bf:reserve";

    // Create filter with 0.01 error rate and 1000 capacity
    let _: () = client
        .call(BfReserve::new(filter, 0.01, 1000))
        .await
        .unwrap();

    // Add an item
    let added: bool = client.call(BfAdd::new(filter, "item1")).await.unwrap();
    assert!(added); // First add should return true

    // Add same item again
    let added: bool = client.call(BfAdd::new(filter, "item1")).await.unwrap();
    assert!(!added); // Duplicate should return false
}

#[tokio::test]
#[cfg(feature = "bloom")]
async fn test_bf_add_auto_create() {
    let client = setup_redis_stack().await;
    let filter = "test:bf:autocreate";

    // BF.ADD auto-creates filter with default parameters
    let added: bool = client.call(BfAdd::new(filter, "auto_item")).await.unwrap();
    assert!(added);

    // Verify it exists
    let exists: bool = client
        .call(BfExists::new(filter, "auto_item"))
        .await
        .unwrap();
    assert!(exists);
}

#[tokio::test]
#[cfg(feature = "bloom")]
async fn test_bf_exists() {
    let client = setup_redis_stack().await;
    let filter = "test:bf:exists";

    // Add items
    let _: bool = client.call(BfAdd::new(filter, "apple")).await.unwrap();
    let _: bool = client.call(BfAdd::new(filter, "banana")).await.unwrap();

    // Check existing item
    let exists: bool = client.call(BfExists::new(filter, "apple")).await.unwrap();
    assert!(exists);

    // Check non-existing item
    let exists: bool = client.call(BfExists::new(filter, "cherry")).await.unwrap();
    assert!(!exists);
}

#[tokio::test]
#[cfg(feature = "bloom")]
async fn test_bf_madd() {
    let client = setup_redis_stack().await;
    let filter = "test:bf:madd";

    // Add multiple items using from_items
    let results: Vec<bool> = client
        .call(BfMadd::from_items(filter, vec!["item1", "item2", "item3"]))
        .await
        .unwrap();

    assert_eq!(results.len(), 3);
    assert!(results[0]); // All new items
    assert!(results[1]);
    assert!(results[2]);

    // Add again (should show duplicates)
    let results: Vec<bool> = client
        .call(BfMadd::from_items(filter, vec!["item1", "item4"]))
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert!(!results[0]); // item1 is duplicate
    assert!(results[1]); // item4 is new
}

#[tokio::test]
#[cfg(feature = "bloom")]
async fn test_bf_mexists() {
    let client = setup_redis_stack().await;
    let filter = "test:bf:mexists";

    // Add some items
    let _: Vec<bool> = client
        .call(BfMadd::from_items(filter, vec!["a", "b", "c"]))
        .await
        .unwrap();

    // Check multiple items
    let results: Vec<bool> = client
        .call(BfMexists::from_items(filter, vec!["a", "x", "c"]))
        .await
        .unwrap();

    assert_eq!(results.len(), 3);
    assert!(results[0]); // "a" exists
    assert!(!results[1]); // "x" doesn't exist
    assert!(results[2]); // "c" exists
}

#[tokio::test]
#[cfg(feature = "bloom")]
async fn test_bf_info() {
    let client = setup_redis_stack().await;
    let filter = "test:bf:info";

    // Create with known parameters
    let _: () = client
        .call(BfReserve::new(filter, 0.01, 1000))
        .await
        .unwrap();

    // Get filter info
    let info: BfInfoResult = client.call(BfInfo::new(filter)).await.unwrap();

    assert_eq!(info.capacity, 1000);
    assert_eq!(info.num_items_inserted, 0); // No items added yet
}

#[tokio::test]
#[cfg(feature = "bloom")]
async fn test_bf_insert_with_options() {
    let client = setup_redis_stack().await;
    let filter = "test:bf:insert";

    // Insert with auto-creation options
    let results: Vec<bool> = client
        .call(
            BfInsert::new(filter, vec!["insert1".into(), "insert2".into()])
                .capacity(500)
                .error(0.001),
        )
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert!(results[0]);
    assert!(results[1]);

    // Verify items exist
    let exists: Vec<bool> = client
        .call(BfMexists::from_items(filter, vec!["insert1", "insert2"]))
        .await
        .unwrap();

    assert!(exists[0]);
    assert!(exists[1]);
}

#[tokio::test]
#[cfg(feature = "bloom")]
async fn test_bf_insert_nocreate() {
    let client = setup_redis_stack().await;
    let filter = "test:bf:nocreate";

    // Try to insert without creating (should fail)
    let result: Result<Vec<bool>, _> = client
        .call(BfInsert::new(filter, vec!["item".into()]).nocreate())
        .await;

    assert!(result.is_err()); // Should error because filter doesn't exist
}

#[tokio::test]
#[cfg(feature = "bloom")]
async fn test_bf_card() {
    let client = setup_redis_stack().await;
    let filter = "test:bf:card";

    // Add items
    let _: Vec<bool> = client
        .call(BfMadd::from_items(filter, vec!["a", "b", "c", "d", "e"]))
        .await
        .unwrap();

    // Get cardinality estimate
    let card: i64 = client.call(BfCard::new(filter)).await.unwrap();

    // Cardinality should be approximately 5 (bloom filters are probabilistic)
    assert!(card >= 4 && card <= 6);
}

#[tokio::test]
#[cfg(feature = "bloom")]
async fn test_bf_reserve_with_expansion() {
    let client = setup_redis_stack().await;
    let filter = "test:bf:expansion";

    // Create with expansion factor
    let _: () = client
        .call(BfReserve::new(filter, 0.01, 100).expansion(4))
        .await
        .unwrap();

    // Get info to verify expansion
    let info: BfInfoResult = client.call(BfInfo::new(filter)).await.unwrap();

    assert_eq!(info.expansion_rate, 4);
}

#[tokio::test]
#[cfg(feature = "bloom")]
async fn test_bf_reserve_nonscaling() {
    let client = setup_redis_stack().await;
    let filter = "test:bf:nonscaling";

    // Create non-scaling filter
    let _: () = client
        .call(BfReserve::new(filter, 0.01, 10).nonscaling())
        .await
        .unwrap();

    // Add items up to capacity
    for i in 0..10 {
        let _: bool = client
            .call(BfAdd::new(filter, format!("item{}", i)))
            .await
            .unwrap();
    }

    // Adding beyond capacity should work but may increase error rate
    let added: bool = client.call(BfAdd::new(filter, "overflow")).await.unwrap();
    assert!(added);
}

#[tokio::test]
#[cfg(feature = "bloom")]
async fn test_bf_false_positive_rate() {
    let client = setup_redis_stack().await;
    let filter = "test:bf:error_rate";

    // Create with low error rate
    let _: () = client
        .call(BfReserve::new(filter, 0.001, 1000))
        .await
        .unwrap();

    // Add 100 items
    for i in 0..100 {
        let _: bool = client
            .call(BfAdd::new(filter, format!("exists_{}", i)))
            .await
            .unwrap();
    }

    // Check 100 items that don't exist
    let mut false_positives = 0;
    for i in 0..100 {
        let exists: bool = client
            .call(BfExists::new(filter, format!("notexists_{}", i)))
            .await
            .unwrap();
        if exists {
            false_positives += 1;
        }
    }

    // With 0.001 error rate, we expect ~0 false positives out of 100 checks
    // Allow up to 2 due to randomness
    assert!(false_positives <= 2);
}

#[tokio::test]
#[cfg(feature = "bloom")]
async fn test_bf_binary_data() {
    let client = setup_redis_stack().await;
    let filter = "test:bf:binary";

    // Add binary data
    let binary_item = vec![0u8, 1, 2, 255, 254, 253];
    let added: bool = client
        .call(BfAdd::new(filter, binary_item.clone()))
        .await
        .unwrap();
    assert!(added);

    // Check binary data
    let exists: bool = client
        .call(BfExists::new(filter, binary_item))
        .await
        .unwrap();
    assert!(exists);
}
