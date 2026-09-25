//! Compile-only coverage for the published redis-rs migration examples.

#![forbid(unsafe_code)]

use bytes::Bytes;
use redis::AsyncCommands;
use redis_tower::commands::{Get, RawCommand, Set};
use redis_tower::{
    ConnectionConfig, MultiplexedClient, ProtocolVersion, PubSubConnection, RedisConnection,
};
use redis_tower_cluster::MultiplexedClusterClient;
use redis_tower_sentinel::MultiplexedSentinelClient;

/// The redis-rs 1.7 typed convenience-method path used by the guide.
pub async fn redis_rs_text_round_trip(url: &str) -> redis::RedisResult<Option<String>> {
    let client = redis::Client::open(url)?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    let _: () = connection.set("migration:redis-rs", "value").await?;
    connection.get("migration:redis-rs").await
}

/// The redis-tower 0.1.3 typed UTF-8 path used by the guide.
pub async fn redis_tower_text_round_trip(
    url: &str,
) -> Result<Option<Bytes>, redis_tower::RedisError> {
    let client = MultiplexedClient::connect_url(url).await?;
    client
        .execute(Set::new("migration:redis-tower", "value"))
        .await?;
    client.execute(Get::new("migration:redis-tower")).await
}

/// The binary-safe raw-command escape hatch in the published API.
pub async fn redis_tower_binary_round_trip(
    url: &str,
) -> Result<Option<Bytes>, redis_tower::RedisError> {
    let client = MultiplexedClient::connect_url(url).await?;
    let key = b"migration:redis-tower:\xff".as_slice();
    let payload = vec![0x00, 0xfe, 0xff];

    client
        .execute(RawCommand::new("SET").arg(key).arg(payload))
        .await?;
    client
        .execute(RawCommand::new("GET").arg(key).query::<Option<Bytes>>())
        .await
}

/// The explicit RESP2 connection configuration used by the guide.
pub async fn redis_tower_resp2(url: &str) -> Result<MultiplexedClient, redis_tower::RedisError> {
    let config = ConnectionConfig::new().with_protocol(ProtocolVersion::Resp2);
    MultiplexedClient::connect_url_with_connection_config(url, &config).await
}

/// A dedicated pub/sub session created from an exclusive connection.
pub async fn redis_tower_pubsub(url: &str) -> Result<PubSubConnection, redis_tower::RedisError> {
    let connection = RedisConnection::connect_url(url).await?;
    PubSubConnection::from_connection(connection)
}

/// The released Cluster URL constructor used by the guide.
pub async fn redis_tower_cluster(
    url: &str,
) -> Result<MultiplexedClusterClient, redis_tower::RedisError> {
    MultiplexedClusterClient::connect_url(url).await
}

/// The released Sentinel constructor used by the guide.
pub async fn redis_tower_sentinel(
    sentinels: &[&str],
    master_name: &str,
) -> Result<MultiplexedSentinelClient, redis_tower::RedisError> {
    MultiplexedSentinelClient::connect(sentinels, master_name).await
}
