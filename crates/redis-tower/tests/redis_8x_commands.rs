use bytes::Bytes;
use redis_tower::RedisConnection;
use redis_tower::commands::*;

fn configured_version() -> Option<(u32, u32, u32)> {
    let Ok(version) = std::env::var("REDIS_STAGE1_VERSION") else {
        eprintln!("REDIS_STAGE1_VERSION is unset; skipping Redis 8.x command tests");
        return None;
    };

    let mut components = version.split('.');
    let major = components
        .next()
        .and_then(|value| value.parse().ok())
        .expect("REDIS_STAGE1_VERSION major must be an integer");
    let minor = components
        .next()
        .and_then(|value| value.parse().ok())
        .expect("REDIS_STAGE1_VERSION minor must be an integer");
    let patch = components
        .next()
        .map(|value| {
            value
                .parse()
                .expect("REDIS_STAGE1_VERSION patch must be an integer")
        })
        .unwrap_or(0);
    assert!(
        components.next().is_none(),
        "REDIS_STAGE1_VERSION must use major.minor or major.minor.patch format"
    );
    assert_eq!(major, 8, "Stage 1 tests require a Redis 8.x server");
    Some((major, minor, patch))
}

async fn connection() -> RedisConnection {
    let url = std::env::var("REDIS_URL")
        .expect("REDIS_URL must be set when REDIS_STAGE1_VERSION is configured");
    RedisConnection::connect_url(&url)
        .await
        .expect("failed to connect to the configured Redis 8.x server")
}

fn key(name: &str) -> String {
    format!("redis_tower:stage1:{}:{name}", std::process::id())
}

#[tokio::test]
async fn redis_8_4_string_commands() {
    let Some(version) = configured_version() else {
        return;
    };
    if version < (8, 4, 0) {
        return;
    }

    let mut conn = connection().await;
    let first = key("strings:first");
    let second = key("strings:second");
    conn.execute(Del::keys([&first, &second])).await.unwrap();

    let set = conn
        .execute(MSetEx::new([(&first, "alpha"), (&second, "beta")]).ex(60))
        .await
        .unwrap();
    assert!(set);
    assert_eq!(
        conn.execute(Get::new(&first)).await.unwrap(),
        Some(Bytes::from_static(b"alpha"))
    );
    assert_eq!(
        conn.execute(Get::new(&second)).await.unwrap(),
        Some(Bytes::from_static(b"beta"))
    );

    let digest = conn
        .execute(Digest::new(&first))
        .await
        .unwrap()
        .expect("DIGEST should return a digest for a string key");
    assert!(!digest.is_empty());
    let digest = String::from_utf8(digest.to_vec()).expect("DIGEST should be hexadecimal UTF-8");
    assert!(
        conn.execute(DelEx::new(&first).if_digest_eq(digest))
            .await
            .unwrap()
    );
    assert!(
        conn.execute(DelEx::new(&second).if_eq("beta"))
            .await
            .unwrap()
    );
    assert_eq!(conn.execute(Digest::new(&first)).await.unwrap(), None);
}

#[tokio::test]
async fn redis_8_6_stream_and_hotkeys_commands() {
    let Some(version) = configured_version() else {
        return;
    };
    if version < (8, 6, 0) {
        return;
    }

    let mut conn = connection().await;
    let stream = key("xcfgset");
    conn.execute(Del::new(&stream)).await.unwrap();
    conn.execute(XAdd::new(&stream).id("1-0").field("field", "value"))
        .await
        .unwrap();
    conn.execute(XCfgSet::new(&stream, XCfgSetOption::IdmpDuration(300)).idmp_maxsize(10))
        .await
        .unwrap();

    if version >= (8, 6, 1) {
        let help = conn.execute(HotkeysHelp::new()).await.unwrap();
        assert!(!help.is_empty());
    }

    // HOTKEYS state is global to the server. Clear any data left by an
    // interrupted prior run before exercising the full lifecycle.
    let _ = conn.execute(HotkeysStop::new()).await;
    let _ = conn.execute(HotkeysReset::new()).await;

    conn.execute(
        HotkeysStart::new(HotkeysMetrics::Net)
            .count(5)
            .sample(1)
            .duration(60),
    )
    .await
    .unwrap();

    let hotkey = key("hotkeys");
    conn.execute(Set::new(&hotkey, "tracked")).await.unwrap();
    for _ in 0..3 {
        assert_eq!(
            conn.execute(Get::new(&hotkey)).await.unwrap(),
            Some(Bytes::from_static(b"tracked"))
        );
    }

    let stats = conn
        .execute(HotkeysGet::new())
        .await
        .unwrap()
        .expect("HOTKEYS GET should return an active session");
    assert!(!stats.is_empty());
    assert!(stats[0].tracking_active);
    assert!(stats.iter().any(|stats| {
        stats.by_net_bytes.as_ref().is_some_and(|measurements| {
            measurements.iter().any(|measurement| {
                measurement.key.as_ref() == hotkey.as_bytes() && measurement.bytes > 0
            })
        })
    }));

    assert!(conn.execute(HotkeysStop::new()).await.unwrap());
    conn.execute(HotkeysReset::new()).await.unwrap();
    conn.execute(Del::keys([stream, hotkey])).await.unwrap();
}

#[tokio::test]
async fn redis_8_8_increx_and_stream_delivery_commands() {
    let Some(version) = configured_version() else {
        return;
    };
    if version < (8, 8, 0) {
        return;
    }

    let mut conn = connection().await;
    let counter = key("increx");
    let float_counter = key("increx-float");
    conn.execute(Del::keys([&counter, &float_counter]))
        .await
        .unwrap();
    assert_eq!(
        conn.execute(IncrEx::new(&counter).by_int(3).upper_bound(10_i64).ex(60))
            .await
            .unwrap(),
        IncrExResult::Integer {
            value: 3,
            actual_increment: 3,
        }
    );
    assert_eq!(
        conn.execute(IncrEx::new(&float_counter).by_float(1.5))
            .await
            .unwrap(),
        IncrExResult::Float {
            value: 1.5,
            actual_increment: 1.5,
        }
    );

    let idmp_stream = key("idmp-record");
    conn.execute(Del::new(&idmp_stream)).await.unwrap();
    conn.execute(XAdd::new(&idmp_stream).id("1-0").field("field", "value"))
        .await
        .unwrap();
    conn.execute(XCfgSet::new(&idmp_stream, XCfgSetOption::IdmpDuration(300)).idmp_maxsize(10))
        .await
        .unwrap();
    conn.execute(XIdmpRecord::new(
        &idmp_stream,
        "producer-1",
        "idempotency-1",
        "1-0",
    ))
    .await
    .unwrap();

    let nack_stream = key("xnack");
    let group = "stage1-group";
    conn.execute(Del::new(&nack_stream)).await.unwrap();
    let entry_id = conn
        .execute(XAdd::new(&nack_stream).id("1-0").field("field", "value"))
        .await
        .unwrap();
    conn.execute(XGroupCreate::new(&nack_stream, group, "0"))
        .await
        .unwrap();
    let delivered = conn
        .execute(XReadGroup::new(group, "stage1-consumer", &nack_stream).count(1))
        .await
        .unwrap();
    assert_eq!(delivered[0].1[0].id, entry_id);
    assert_eq!(
        conn.execute(
            XNack::new(&nack_stream, group, XNackMode::Silent, [&entry_id],).retrycount(2),
        )
        .await
        .unwrap(),
        1
    );

    conn.execute(Del::keys([
        counter,
        float_counter,
        idmp_stream,
        nack_stream,
    ]))
    .await
    .unwrap();
}
