//! Integration tests for the probabilistic dump/restore commands (#477).
//!
//! These require the Bloom module, which is bundled in Redis 8.x but absent
//! from plain Redis 7.x. Each test probes with a reserve and returns early
//! (skips) when the module is unavailable, so the suite stays green on the
//! Redis 7.4 CI matrix and runs for real on 8.x.

mod common;

use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn bf_card_counts_items() {
    let mut c = conn().await;
    let key = "cover2:bloom:card";
    let _ = c.execute(Del::new(key)).await;

    // Probe: skip if the Bloom module is unavailable (plain Redis < 8).
    if c.execute(BfReserve::new(key, 0.01, 1000)).await.is_err() {
        return;
    }
    c.execute(BfAdd::new(key, "a")).await.unwrap();
    c.execute(BfAdd::new(key, "b")).await.unwrap();

    let card = c.execute(BfCard::new(key)).await.unwrap();
    assert_eq!(card, 2, "BF.CARD should report the two added items");
}

#[tokio::test]
async fn bf_scandump_loadchunk_roundtrip() {
    let mut c = conn().await;
    let src = "cover2:bloom:dump:src";
    let dst = "cover2:bloom:dump:dst";
    let _ = c.execute(Del::new(src)).await;
    let _ = c.execute(Del::new(dst)).await;

    if c.execute(BfReserve::new(src, 0.01, 1000)).await.is_err() {
        return; // Bloom module unavailable
    }
    for i in 0..50 {
        c.execute(BfAdd::new(src, format!("item-{i}")))
            .await
            .unwrap();
    }

    // Dump the source filter chunk by chunk and load each into the destination.
    let mut iter: u64 = 0;
    loop {
        let (next, chunk) = c.execute(BfScanDump::new(src, iter)).await.unwrap();
        if next == 0 {
            break;
        }
        c.execute(BfLoadChunk::new(dst, next, chunk)).await.unwrap();
        iter = next;
    }

    // The restored filter reports the same cardinality and members.
    assert_eq!(
        c.execute(BfCard::new(dst)).await.unwrap(),
        c.execute(BfCard::new(src)).await.unwrap(),
        "restored filter cardinality should match the source"
    );
    for i in 0..50 {
        assert!(
            c.execute(BfExists::new(dst, format!("item-{i}")))
                .await
                .unwrap(),
            "restored filter should contain item-{i}"
        );
    }
}

#[tokio::test]
async fn cf_scandump_loadchunk_roundtrip() {
    let mut c = conn().await;
    let src = "cover2:cuckoo:dump:src";
    let dst = "cover2:cuckoo:dump:dst";
    let _ = c.execute(Del::new(src)).await;
    let _ = c.execute(Del::new(dst)).await;

    if c.execute(CfReserve::new(src, 1000)).await.is_err() {
        return; // Cuckoo/Bloom module unavailable
    }
    for i in 0..50 {
        c.execute(CfAdd::new(src, format!("item-{i}")))
            .await
            .unwrap();
    }

    let mut iter: u64 = 0;
    loop {
        let (next, chunk) = c.execute(CfScanDump::new(src, iter)).await.unwrap();
        if next == 0 {
            break;
        }
        c.execute(CfLoadChunk::new(dst, next, chunk)).await.unwrap();
        iter = next;
    }

    for i in 0..50 {
        assert!(
            c.execute(CfExists::new(dst, format!("item-{i}")))
                .await
                .unwrap(),
            "restored cuckoo filter should contain item-{i}"
        );
    }
}
