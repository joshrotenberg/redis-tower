mod common;

use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn setbit_getbit() {
    let c = conn().await;
    let key = "cover2:bitmap:setbit_getbit";

    c.execute(Del::new(key)).await.unwrap();

    let old = c.execute(SetBit::new(key, 7, 1)).await.unwrap();
    assert_eq!(old, 0, "SETBIT should return the old bit value (0)");

    let bit = c.execute(GetBit::new(key, 7)).await.unwrap();
    assert_eq!(bit, 1);
}

#[tokio::test]
async fn bitcount() {
    let c = conn().await;
    let key = "cover2:bitmap:bitcount";

    c.execute(Del::new(key)).await.unwrap();
    // Set all 8 bits via SETBIT.
    for i in 0..8 {
        c.execute(SetBit::new(key, i, 1)).await.unwrap();
    }

    let count = c.execute(BitCount::new(key)).await.unwrap();
    assert_eq!(count, 8);
}

#[tokio::test]
async fn bitpos() {
    let c = conn().await;
    let key = "cover2:bitmap:bitpos";

    c.execute(Del::new(key)).await.unwrap();
    // First byte all zeros, second byte: set bits 8..16 via SETBIT.
    for i in 8..16 {
        c.execute(SetBit::new(key, i, 1)).await.unwrap();
    }

    let pos = c.execute(BitPos::new(key, 1)).await.unwrap();
    assert_eq!(pos, 8, "first set bit should be at position 8");
}

#[tokio::test]
async fn bitop() {
    let c = conn().await;
    let key1 = "cover2:bitmap:bitop:1";
    let key2 = "cover2:bitmap:bitop:2";
    let dest = "cover2:bitmap:bitop:dest";

    c.execute(Del::new(key1)).await.unwrap();
    c.execute(Del::new(key2)).await.unwrap();
    c.execute(Del::new(dest)).await.unwrap();

    // key1: all 8 bits set (0xFF)
    for i in 0..8 {
        c.execute(SetBit::new(key1, i, 1)).await.unwrap();
    }
    // key2: lower 4 bits set (0x0F = bits 4,5,6,7 in Redis bit ordering)
    for i in 4..8 {
        c.execute(SetBit::new(key2, i, 1)).await.unwrap();
    }

    let len = c
        .execute(BitOp::new(BitOperation::And, dest, [key1, key2]))
        .await
        .unwrap();
    assert_eq!(len, 1, "BITOP AND should return the length of the result");

    // AND of 0xFF and 0x0F should be 0x0F -- 4 bits set.
    let count = c.execute(BitCount::new(dest)).await.unwrap();
    assert_eq!(count, 4);
}
