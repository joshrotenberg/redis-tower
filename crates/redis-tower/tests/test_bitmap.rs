mod common;

use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn setbit_getbit() {
    let mut c = conn().await;
    let key = "cover2:bitmap:setbit_getbit";

    c.execute(Del::new(key)).await.unwrap();

    let old = c.execute(SetBit::new(key, 7, 1)).await.unwrap();
    assert_eq!(old, 0, "SETBIT should return the old bit value (0)");

    let bit = c.execute(GetBit::new(key, 7)).await.unwrap();
    assert_eq!(bit, 1);
}

#[tokio::test]
async fn bitcount() {
    let mut c = conn().await;
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
    let mut c = conn().await;
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
    let mut c = conn().await;
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

#[tokio::test]
async fn bitfield_set_get_incr() {
    let mut c = conn().await;
    let key = "cover2:bitmap:bitfield_set_get_incr";
    c.execute(Del::new(key)).await.unwrap();

    // SET returns the previous value (0 for a fresh field).
    let set = c
        .execute(Bitfield::new(key).set("u8", "0", 200))
        .await
        .unwrap();
    assert_eq!(set, vec![Some(0)]);

    // GET reads back the value just written.
    let got = c.execute(Bitfield::new(key).get("u8", "0")).await.unwrap();
    assert_eq!(got, vec![Some(200)]);

    // INCRBY returns the new value.
    let incr = c
        .execute(Bitfield::new(key).incr_by("u8", "0", 10))
        .await
        .unwrap();
    assert_eq!(incr, vec![Some(210)]);
}

#[tokio::test]
async fn bitfield_overflow_sat_caps() {
    let mut c = conn().await;
    let key = "cover2:bitmap:bitfield_overflow_sat";
    c.execute(Del::new(key)).await.unwrap();

    // u8 max is 255; SAT overflow should cap at 255 rather than wrapping.
    let result = c
        .execute(
            Bitfield::new(key)
                .set("u8", "0", 250)
                .overflow(BitfieldOverflow::Sat)
                .incr_by("u8", "0", 100),
        )
        .await
        .unwrap();
    assert_eq!(result, vec![Some(0), Some(255)]);
}

#[tokio::test]
async fn bitfield_overflow_fail_returns_nil() {
    let mut c = conn().await;
    let key = "cover2:bitmap:bitfield_overflow_fail";
    c.execute(Del::new(key)).await.unwrap();

    // FAIL overflow should leave the value unchanged and report nil.
    let result = c
        .execute(
            Bitfield::new(key)
                .set("u8", "0", 250)
                .overflow(BitfieldOverflow::Fail)
                .incr_by("u8", "0", 100),
        )
        .await
        .unwrap();
    assert_eq!(result, vec![Some(0), None]);
}

#[tokio::test]
async fn bitfield_ro_get() {
    let mut c = conn().await;
    let key = "cover2:bitmap:bitfield_ro_get";
    c.execute(Del::new(key)).await.unwrap();

    c.execute(Bitfield::new(key).set("u8", "0", 42))
        .await
        .unwrap();

    let got = c
        .execute(BitfieldRo::new(key).get("u8", "0"))
        .await
        .unwrap();
    assert_eq!(got, vec![Some(42)]);
}
