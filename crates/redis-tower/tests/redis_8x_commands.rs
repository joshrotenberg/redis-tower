use bytes::Bytes;
use redis_tower::commands::*;
use redis_tower::{Frame, RedisConnection};
use std::time::Duration;

fn configured_version() -> Option<(u32, u32, u32)> {
    let Ok(version) = std::env::var("REDIS_8X_VERSION") else {
        eprintln!("REDIS_8X_VERSION is unset; skipping Redis 8.x command tests");
        return None;
    };

    let mut components = version.split('.');
    let major = components
        .next()
        .and_then(|value| value.parse().ok())
        .expect("REDIS_8X_VERSION major must be an integer");
    let minor = components
        .next()
        .and_then(|value| value.parse().ok())
        .expect("REDIS_8X_VERSION minor must be an integer");
    let patch = components
        .next()
        .map(|value| {
            value
                .parse()
                .expect("REDIS_8X_VERSION patch must be an integer")
        })
        .unwrap_or(0);
    assert!(
        components.next().is_none(),
        "REDIS_8X_VERSION must use major.minor or major.minor.patch format"
    );
    assert_eq!(major, 8, "Redis 8.x command tests require a Redis 8 server");
    Some((major, minor, patch))
}

async fn connection() -> RedisConnection {
    let url = std::env::var("REDIS_URL")
        .expect("REDIS_URL must be set when REDIS_8X_VERSION is configured");
    RedisConnection::connect_url(&url)
        .await
        .expect("failed to connect to the configured Redis 8.x server")
}

fn key(name: &str) -> String {
    format!("redis_tower:redis8:{}:{name}", std::process::id())
}

fn vector_blob(values: &[f32]) -> Bytes {
    let mut bytes = Vec::with_capacity(std::mem::size_of_val(values));
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    Bytes::from(bytes)
}

#[tokio::test]
async fn redis_8_0_vector_membership_and_search_diagnostics() {
    let Some(version) = configured_version() else {
        return;
    };
    if version < (8, 0, 0) {
        return;
    }

    let mut conn = connection().await;
    let vector_set = key("vector-membership");
    conn.execute(Del::new(&vector_set)).await.unwrap();
    assert!(
        conn.execute(VAdd::new(&vector_set, vec![1.0, 0.0], "alpha"))
            .await
            .unwrap()
    );
    assert!(
        conn.execute(VIsMember::new(&vector_set, "alpha"))
            .await
            .unwrap()
    );
    assert!(
        !conn
            .execute(VIsMember::new(&vector_set, "missing"))
            .await
            .unwrap()
    );

    let index = key("search-diagnostics-index");
    let prefix = format!("{}:", key("search-diagnostics-doc"));
    let document = format!("{prefix}1");
    conn.execute(
        FtCreate::new(&index)
            .on_hash()
            .prefix(&prefix)
            .field("title", FieldType::Text)
            .field("category", FieldType::Tag),
    )
    .await
    .unwrap();
    conn.execute(HSet::new(&document, "title", "hello redis").field("category", "database"))
        .await
        .unwrap();

    let explain = conn
        .execute(FtExplain::new(&index, "@title:hello").dialect(2))
        .await
        .unwrap();
    assert!(!explain.is_empty());
    let explain_cli = conn
        .execute(FtExplainCli::new(&index, "@title:hello").dialect(2))
        .await
        .unwrap();
    assert!(!explain_cli.is_empty());
    conn.execute(FtProfile::search(&index, "@title:hello").limited())
        .await
        .unwrap();

    let mut tag_values = Vec::new();
    for _ in 0..50 {
        tag_values = conn
            .execute(FtTagVals::new(&index, "category"))
            .await
            .unwrap();
        if tag_values.iter().any(|value| value == "database") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(tag_values, vec!["database".to_string()]);

    conn.execute(FtDropIndex::new(index).dd()).await.unwrap();
    conn.execute(Del::new(vector_set)).await.unwrap();
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
async fn redis_8_4_vector_range_and_hybrid_search() {
    let Some(version) = configured_version() else {
        return;
    };
    if version < (8, 4, 0) {
        return;
    }

    let mut conn = connection().await;
    let vector_set = key("vector-range");
    conn.execute(Del::new(&vector_set)).await.unwrap();
    for (element, vector) in [
        ("gamma", vec![0.5, 0.5]),
        ("alpha", vec![1.0, 0.0]),
        ("beta", vec![0.0, 1.0]),
    ] {
        assert!(
            conn.execute(VAdd::new(&vector_set, vector, element))
                .await
                .unwrap()
        );
    }
    assert_eq!(
        conn.execute(VRange::new(&vector_set, "-", "+").count(-1))
            .await
            .unwrap(),
        vec![
            Bytes::from_static(b"alpha"),
            Bytes::from_static(b"beta"),
            Bytes::from_static(b"gamma"),
        ]
    );
    assert_eq!(
        conn.execute(VRange::new(&vector_set, "[beta", "+").count(1))
            .await
            .unwrap(),
        vec![Bytes::from_static(b"beta")]
    );

    if version >= (8, 4, 4) {
        let index = key("hybrid-index");
        let prefix = format!("{}:", key("hybrid-document"));
        let document = format!("{prefix}1");
        let document_vector = vector_blob(&[1.0, 0.0]);
        let query_vector = vector_blob(&[1.0, 0.0]);

        conn.execute(
            RawCommand::new("FT.CREATE")
                .arg(index.as_str())
                .arg("ON")
                .arg("HASH")
                .arg("PREFIX")
                .arg("1")
                .arg(prefix.as_str())
                .arg("SCHEMA")
                .arg("title")
                .arg("TEXT")
                .arg("embedding")
                .arg("VECTOR")
                .arg("FLAT")
                .arg("6")
                .arg("TYPE")
                .arg("FLOAT32")
                .arg("DIM")
                .arg("2")
                .arg("DISTANCE_METRIC")
                .arg("COSINE"),
        )
        .await
        .unwrap();
        conn.execute(
            RawCommand::new("HSET")
                .arg(document.as_str())
                .arg("title")
                .arg("laptop computer")
                .arg("embedding")
                .arg(document_vector),
        )
        .await
        .unwrap();

        let basic_hybrid = FtHybrid::new(
            &index,
            "laptop",
            "embedding",
            "query_vector",
            query_vector.clone(),
        )
        .knn(FtHybridKnn::new(2))
        .vector_score_as("vector_score")
        .limit(0, 2);
        let response = conn.execute(basic_hybrid).await.unwrap();
        assert!(matches!(response, Frame::Map(_) | Frame::Array(Some(_))));

        let range_response = conn
            .execute(
                FtHybrid::new(
                    &index,
                    "laptop",
                    "embedding",
                    "query_vector",
                    query_vector.clone(),
                )
                .range(FtHybridRange::new(1.0))
                .limit(0, 2),
            )
            .await
            .unwrap();
        assert!(matches!(
            range_response,
            Frame::Map(_) | Frame::Array(Some(_))
        ));

        if version >= (8, 8, 0) {
            let profiled = FtProfile::hybrid(
                FtHybrid::new(&index, "laptop", "embedding", "query_vector", query_vector)
                    .knn(FtHybridKnn::new(2))
                    .search_score_as("text_score")
                    .vector_score_as("vector_score")
                    .rrf(
                        FtHybridRrf::new()
                            .window(20)
                            .yield_score_as("combined_score"),
                    )
                    .sortby("@combined_score", SortOrder::Desc)
                    .load_field_as("@title", "title")
                    .limit(0, 2),
            )
            .limited();
            let response = conn.execute(profiled).await.unwrap();
            assert!(matches!(response, Frame::Map(_) | Frame::Array(Some(_))));
        }

        conn.execute(FtDropIndex::new(index).dd()).await.unwrap();
    }

    conn.execute(Del::new(vector_set)).await.unwrap();
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
    let group = "redis8-group";
    conn.execute(Del::new(&nack_stream)).await.unwrap();
    let entry_id = conn
        .execute(XAdd::new(&nack_stream).id("1-0").field("field", "value"))
        .await
        .unwrap();
    conn.execute(XGroupCreate::new(&nack_stream, group, "0"))
        .await
        .unwrap();
    let delivered = conn
        .execute(XReadGroup::new(group, "redis8-consumer", &nack_stream).count(1))
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

#[tokio::test]
async fn redis_8_8_array_commands() {
    let Some(version) = configured_version() else {
        return;
    };
    if version < (8, 8, 0) {
        return;
    }

    let mut conn = connection().await;
    let array_key = key("array");
    let ring_key = key("array-ring");
    let array_arg = Bytes::from(array_key.clone());
    let ring_arg = Bytes::from(ring_key.clone());
    conn.execute(Del::keys([array_key.as_str(), ring_key.as_str()]))
        .await
        .unwrap();

    assert_eq!(
        conn.execute(
            ArSet::new(array_arg.clone(), 0, "zero")
                .value("one")
                .value("two"),
        )
        .await
        .unwrap(),
        3
    );
    assert_eq!(
        conn.execute(ArGet::new(array_arg.clone(), 1))
            .await
            .unwrap(),
        Some(Bytes::from_static(b"one"))
    );
    assert_eq!(
        conn.execute(ArGetRange::new(array_arg.clone(), 0, 4))
            .await
            .unwrap(),
        vec![
            Some(Bytes::from_static(b"zero")),
            Some(Bytes::from_static(b"one")),
            Some(Bytes::from_static(b"two")),
            None,
            None,
        ]
    );

    assert_eq!(
        conn.execute(ArMSet::new(array_arg.clone(), 4, "four").pair(6, "six"))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        conn.execute(ArMGet::new(array_arg.clone(), 0).index(3).index(4).index(6),)
            .await
            .unwrap(),
        vec![
            Some(Bytes::from_static(b"zero")),
            None,
            Some(Bytes::from_static(b"four")),
            Some(Bytes::from_static(b"six")),
        ]
    );
    assert_eq!(
        conn.execute(ArLen::new(array_arg.clone())).await.unwrap(),
        7
    );
    assert_eq!(
        conn.execute(ArCount::new(array_arg.clone())).await.unwrap(),
        5
    );
    assert_eq!(
        conn.execute(ArNext::new(array_arg.clone())).await.unwrap(),
        Some(0)
    );

    assert!(
        conn.execute(ArSeek::new(array_arg.clone(), 10))
            .await
            .unwrap()
    );
    assert_eq!(
        conn.execute(ArNext::new(array_arg.clone())).await.unwrap(),
        Some(10)
    );
    assert_eq!(
        conn.execute(ArInsert::new(array_arg.clone(), "ten").value("eleven"))
            .await
            .unwrap(),
        11
    );
    assert_eq!(
        conn.execute(ArNext::new(array_arg.clone())).await.unwrap(),
        Some(12)
    );
    assert_eq!(
        conn.execute(ArLastItems::new(array_arg.clone(), 3))
            .await
            .unwrap(),
        vec![
            None,
            Some(Bytes::from_static(b"ten")),
            Some(Bytes::from_static(b"eleven")),
        ]
    );

    let scanned = conn
        .execute(ArScan::new(array_arg.clone(), 0, 11).limit(20))
        .await
        .unwrap();
    assert_eq!(
        scanned,
        vec![
            ArrayEntry {
                index: 0,
                value: Bytes::from_static(b"zero"),
            },
            ArrayEntry {
                index: 1,
                value: Bytes::from_static(b"one"),
            },
            ArrayEntry {
                index: 2,
                value: Bytes::from_static(b"two"),
            },
            ArrayEntry {
                index: 4,
                value: Bytes::from_static(b"four"),
            },
            ArrayEntry {
                index: 6,
                value: Bytes::from_static(b"six"),
            },
            ArrayEntry {
                index: 10,
                value: Bytes::from_static(b"ten"),
            },
            ArrayEntry {
                index: 11,
                value: Bytes::from_static(b"eleven"),
            },
        ]
    );
    assert_eq!(
        conn.execute(
            ArGrep::new(
                array_arg.clone(),
                ArGrepBound::Index(0),
                ArGrepBound::Index(11),
                ArGrepPredicate::Match(Bytes::from_static(b"O")),
            )
            .nocase()
            .with_values()
            .limit(10),
        )
        .await
        .unwrap(),
        ArGrepResult::Entries(vec![
            ArrayEntry {
                index: 0,
                value: Bytes::from_static(b"zero"),
            },
            ArrayEntry {
                index: 1,
                value: Bytes::from_static(b"one"),
            },
            ArrayEntry {
                index: 2,
                value: Bytes::from_static(b"two"),
            },
            ArrayEntry {
                index: 4,
                value: Bytes::from_static(b"four"),
            },
        ])
    );
    assert_eq!(
        conn.execute(ArOp::new(array_arg.clone(), 0, 11, ArOpOperation::Used,))
            .await
            .unwrap(),
        ArOpResult::Integer(Some(7))
    );
    let info = conn
        .execute(ArInfo::new(array_arg.clone()).full())
        .await
        .unwrap();
    assert_eq!(info.count, 7);
    assert_eq!(info.len, 12);
    assert_eq!(info.next_insert_index, 12);
    let full = info.full.expect("ARINFO FULL should include slice details");
    assert_eq!(full.dense_slices + full.sparse_slices, info.slices);

    assert_eq!(
        conn.execute(ArDel::new(array_arg.clone(), 1).index(4))
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        conn.execute(ArDelRange::new(array_arg, 5, 10))
            .await
            .unwrap(),
        2
    );

    assert_eq!(
        conn.execute(
            ArRing::new(ring_arg.clone(), 3, "a")
                .value("b")
                .value("c")
                .value("d"),
        )
        .await
        .unwrap(),
        0
    );
    assert_eq!(conn.execute(ArLen::new(ring_arg.clone())).await.unwrap(), 3);
    assert_eq!(
        conn.execute(ArCount::new(ring_arg.clone())).await.unwrap(),
        3
    );
    assert_eq!(
        conn.execute(ArLastItems::new(ring_arg, 3).rev())
            .await
            .unwrap(),
        vec![
            Some(Bytes::from_static(b"d")),
            Some(Bytes::from_static(b"c")),
            Some(Bytes::from_static(b"b")),
        ]
    );

    conn.execute(Del::keys([array_key, ring_key]))
        .await
        .unwrap();
}
