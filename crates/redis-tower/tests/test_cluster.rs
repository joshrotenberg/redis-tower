mod common;

use common::conn;
use redis_tower::commands::*;

// These tests require a Redis instance with cluster-enabled yes.
// Run with: cargo test --test test_cluster -- --ignored

#[tokio::test]
#[ignore = "requires cluster-enabled Redis"]
async fn cluster_info() {
    let c = conn().await;
    let info = c.execute(ClusterInfo::new()).await.unwrap();
    assert!(info.contains("cluster_enabled"));
}

#[tokio::test]
#[ignore = "requires cluster-enabled Redis"]
async fn cluster_myid() {
    let c = conn().await;
    let id = c.execute(ClusterMyId::new()).await.unwrap();
    // Node ID is a 40-character hex string.
    assert_eq!(id.len(), 40);
    assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
}

#[tokio::test]
#[ignore = "requires cluster-enabled Redis"]
async fn cluster_keyslot() {
    let c = conn().await;
    let slot = c.execute(ClusterKeySlot::new("foo")).await.unwrap();
    assert!((0..=16383).contains(&slot));

    // Same key always returns same slot.
    let slot2 = c.execute(ClusterKeySlot::new("foo")).await.unwrap();
    assert_eq!(slot, slot2);

    // Known value: CRC16("foo") mod 16384 = 12182.
    assert_eq!(slot, 12182);
}
