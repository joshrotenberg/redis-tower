//! RedisJSON reply comparison with the pinned redis-rs test oracle.
//!
//! This remains on the existing module-enabled nightly gate: ordinary Redis
//! builds cannot execute JSON commands, so pretending this belongs in the
//! standalone per-PR matrix would turn a real assertion into a skip.

#![cfg(feature = "json")]

use redis_tower::Frame;
use redis_tower::commands::RawCommand;
use redis_tower_core::{ProtocolVersion, RedisConnection};

fn url(protocol: &str) -> String {
    let base = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6399".to_owned());
    let separator = if base.contains('?') { '&' } else { '?' };
    format!("{base}{separator}protocol={protocol}")
}

fn tower_bytes(frame: Frame) -> Vec<u8> {
    match frame {
        Frame::BulkString(Some(value)) | Frame::SimpleString(value) => value.to_vec(),
        other => panic!("expected string reply, got {other:?}"),
    }
}

fn tower_strings(frame: Frame) -> Vec<Vec<u8>> {
    match frame {
        Frame::Array(Some(values)) | Frame::Set(values) => {
            values.into_iter().map(tower_bytes).collect()
        }
        other => panic!("expected collection reply, got {other:?}"),
    }
}

fn redis_strings(value: redis::Value) -> Vec<Vec<u8>> {
    match value {
        redis::Value::Array(values) | redis::Value::Set(values) => values
            .into_iter()
            .map(|value| match value {
                redis::Value::BulkString(value) => value,
                redis::Value::SimpleString(value) => value.into_bytes(),
                other => panic!("expected string member, got {other:?}"),
            })
            .collect(),
        other => panic!("expected collection reply, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires a module-enabled Redis server with RedisJSON"]
async fn differential_json_raw_replies_match_redis_rs() {
    for (protocol, tower_protocol) in [
        ("resp2", ProtocolVersion::Resp2),
        ("resp3", ProtocolVersion::Resp3),
    ] {
        let base =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6399".to_owned());
        let mut tower = RedisConnection::connect_url_with_config(
            &base,
            &redis_tower_core::ConnectionConfig::new().with_protocol(tower_protocol),
        )
        .await
        .expect("redis-tower module connection");
        let client = redis::Client::open(url(protocol)).expect("valid redis-rs URL");
        let mut redis_rs = client
            .get_multiplexed_async_connection()
            .await
            .expect("redis-rs module connection");

        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let tower_key = format!("redis_tower:diff:json:{protocol}:tower:{suffix}");
        let redis_key = format!("redis_tower:diff:json:{protocol}:redis-rs:{suffix}");
        let document = br#"{"name":"Ada","tags":["systems","redis"],"raw":"\u0000"}"#;

        tower
            .execute(
                RawCommand::new("JSON.SET")
                    .arg(&tower_key)
                    .arg("$")
                    .arg(document),
            )
            .await
            .expect("tower JSON.SET");
        redis::cmd("JSON.SET")
            .arg(&redis_key)
            .arg("$")
            .arg(document)
            .query_async::<redis::Value>(&mut redis_rs)
            .await
            .expect("redis-rs JSON.SET");

        let tower_get = tower
            .execute(RawCommand::new("JSON.GET").arg(&tower_key).arg("$"))
            .await
            .map(tower_bytes)
            .expect("tower JSON.GET");
        let redis_get = redis::cmd("JSON.GET")
            .arg(&redis_key)
            .arg("$")
            .query_async::<Vec<u8>>(&mut redis_rs)
            .await
            .expect("redis-rs JSON.GET");
        assert_eq!(tower_get, redis_get, "JSON.GET diverged under {protocol}");

        let mut tower_keys = tower
            .execute(RawCommand::new("JSON.OBJKEYS").arg(&tower_key).arg("."))
            .await
            .map(tower_strings)
            .expect("tower JSON.OBJKEYS");
        let mut redis_keys = redis::cmd("JSON.OBJKEYS")
            .arg(&redis_key)
            .arg(".")
            .query_async::<redis::Value>(&mut redis_rs)
            .await
            .map(redis_strings)
            .expect("redis-rs JSON.OBJKEYS");
        tower_keys.sort();
        redis_keys.sort();
        assert_eq!(
            tower_keys, redis_keys,
            "JSON.OBJKEYS diverged under {protocol}"
        );

        tower
            .execute(RawCommand::new("DEL").arg(&tower_key))
            .await
            .expect("tower cleanup");
        redis::cmd("DEL")
            .arg(&redis_key)
            .query_async::<redis::Value>(&mut redis_rs)
            .await
            .expect("redis-rs cleanup");
    }
}
