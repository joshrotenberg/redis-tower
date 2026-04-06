mod common;

use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn hsetnx() {
    let mut c = conn().await;
    let key = "cover2:hash:hsetnx";

    c.execute(Del::new(key)).await.unwrap();

    let first = c.execute(HSetNx::new(key, "field1", "val1")).await.unwrap();
    assert!(first, "HSETNX should return true for a new field");

    let second = c.execute(HSetNx::new(key, "field1", "val2")).await.unwrap();
    assert!(!second, "HSETNX should return false for an existing field");
}

#[tokio::test]
async fn hincrbyfloat() {
    let mut c = conn().await;
    let key = "cover2:hash:hincrbyfloat";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(HSet::new(key, "field", "10.5")).await.unwrap();

    let result = c
        .execute(HIncrByFloat::new(key, "field", 0.5))
        .await
        .unwrap();
    assert!((result - 11.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn hrandfield() {
    let mut c = conn().await;
    let key = "cover2:hash:hrandfield";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(HSet::new(key, "f1", "v1").field("f2", "v2"))
        .await
        .unwrap();

    let result = c.execute(HRandField::new(key).count(2)).await.unwrap();
    assert!(!result.is_empty());
}
