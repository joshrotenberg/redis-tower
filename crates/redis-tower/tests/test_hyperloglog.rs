mod common;

use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn pfadd_pfcount() {
    let c = conn().await;
    let key = "cover2:hll:pfadd_pfcount";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(PfAdd::elements(key, ["a", "b", "c", "d"]))
        .await
        .unwrap();

    let count = c.execute(PfCount::new(key)).await.unwrap();
    assert!(
        (3..=5).contains(&count),
        "PFCOUNT should be approximately 4, got {count}"
    );
}

#[tokio::test]
async fn pfmerge() {
    let c = conn().await;
    let key1 = "cover2:hll:pfmerge:1";
    let key2 = "cover2:hll:pfmerge:2";
    let dest = "cover2:hll:pfmerge:dest";

    c.execute(Del::new(key1)).await.unwrap();
    c.execute(Del::new(key2)).await.unwrap();
    c.execute(Del::new(dest)).await.unwrap();

    c.execute(PfAdd::elements(key1, ["a", "b", "c"]))
        .await
        .unwrap();
    c.execute(PfAdd::elements(key2, ["c", "d", "e"]))
        .await
        .unwrap();

    c.execute(PfMerge::new(dest, [key1, key2])).await.unwrap();

    let merged_count = c.execute(PfCount::new(dest)).await.unwrap();
    let count1 = c.execute(PfCount::new(key1)).await.unwrap();
    let count2 = c.execute(PfCount::new(key2)).await.unwrap();
    assert!(
        merged_count >= count1 && merged_count >= count2,
        "merged count ({merged_count}) should be >= individual counts ({count1}, {count2})"
    );
}

#[tokio::test]
async fn pfadd_returns_bool() {
    let c = conn().await;
    let key = "cover2:hll:pfadd_bool";

    c.execute(Del::new(key)).await.unwrap();

    let first = c.execute(PfAdd::new(key, "new_element")).await.unwrap();
    assert!(first, "PFADD should return true when cardinality changes");

    let second = c.execute(PfAdd::new(key, "new_element")).await.unwrap();
    assert!(
        !second,
        "PFADD should return false when cardinality does not change"
    );
}
