mod common;

use bytes::Bytes;
use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn geoadd_geopos() {
    let c = conn().await;
    let key = "cover2:geo:geopos";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        GeoAdd::new(key)
            .member(-122.4194, 37.7749, "San Francisco")
            .member(-73.9857, 40.7484, "New York"),
    )
    .await
    .unwrap();

    let positions = c
        .execute(GeoPos::members(key, ["San Francisco", "New York"]))
        .await
        .unwrap();
    assert_eq!(positions.len(), 2);

    let sf = positions[0].unwrap();
    assert!((sf.0 - (-122.4194)).abs() < 0.01);
    assert!((sf.1 - 37.7749).abs() < 0.01);

    let ny = positions[1].unwrap();
    assert!((ny.0 - (-73.9857)).abs() < 0.01);
    assert!((ny.1 - 40.7484).abs() < 0.01);
}

#[tokio::test]
async fn geodist() {
    let c = conn().await;
    let key = "cover2:geo:geodist";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        GeoAdd::new(key)
            .member(-122.4194, 37.7749, "San Francisco")
            .member(-73.9857, 40.7484, "New York"),
    )
    .await
    .unwrap();

    let dist = c
        .execute(GeoDist::new(key, "San Francisco", "New York").unit(GeoUnit::Kilometers))
        .await
        .unwrap();
    assert!(dist.unwrap() > 0.0);
}

#[tokio::test]
async fn geohash() {
    let c = conn().await;
    let key = "cover2:geo:geohash";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        GeoAdd::new(key)
            .member(-122.4194, 37.7749, "San Francisco")
            .member(-73.9857, 40.7484, "New York"),
    )
    .await
    .unwrap();

    let hashes = c
        .execute(GeoHash::members(key, ["San Francisco", "New York"]))
        .await
        .unwrap();
    assert_eq!(hashes.len(), 2);
    assert!(!hashes[0].as_ref().unwrap().is_empty());
    assert!(!hashes[1].as_ref().unwrap().is_empty());
}

#[tokio::test]
async fn geosearch() {
    let c = conn().await;
    let key = "cover2:geo:geosearch";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(
        GeoAdd::new(key)
            .member(13.361389, 38.115556, "Palermo")
            .member(15.087269, 37.502669, "Catania")
            .member(2.349014, 48.864716, "Paris"),
    )
    .await
    .unwrap();

    // Search within 200 km of Palermo -- should find Palermo and Catania.
    let members = c
        .execute(
            GeoSearch::from_member(key, "Palermo")
                .by_radius(200.0, GeoUnit::Kilometers)
                .asc(),
        )
        .await
        .unwrap();
    assert!(members.len() >= 2);
    assert!(members.contains(&Bytes::from("Palermo")));
    assert!(members.contains(&Bytes::from("Catania")));
}
