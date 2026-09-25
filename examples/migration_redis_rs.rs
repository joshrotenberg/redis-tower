//! Compiled side-by-side migration example for redis-rs 1.7 and redis-tower.
//!
//! `cargo check -p redis-tower-examples --example migration_redis_rs` validates
//! both APIs without a Redis server. Set `REDIS_URL` to run the round trips.

use bytes::Bytes;
use redis::AsyncCommands;
use redis_tower::MultiplexedClient;
use redis_tower::commands::{Get, Set};

async fn redis_rs_round_trip(url: &str) -> redis::RedisResult<Option<Vec<u8>>> {
    let client = redis::Client::open(url)?;
    let mut connection = client.get_multiplexed_async_connection().await?;
    let key = b"migration:redis-rs:\xff".as_slice();
    let payload = vec![0x00, 0xfe, 0xff];

    let _: () = connection.set(key, payload).await?;
    connection.get(key).await
}

async fn redis_tower_round_trip(url: &str) -> Result<Option<Bytes>, redis_tower::RedisError> {
    let client = MultiplexedClient::connect_url(url).await?;
    let key = b"migration:redis-tower:\xff".as_slice();
    let payload = vec![0x00, 0xfe, 0xff];

    client.execute(Set::new(key, payload)).await?;
    client.execute(Get::new(key)).await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(url) = std::env::var_os("REDIS_URL") else {
        println!("set REDIS_URL to run both migration round trips");
        return Ok(());
    };
    let url = url.into_string().map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "REDIS_URL is not UTF-8")
    })?;

    let redis_rs = redis_rs_round_trip(&url).await?;
    let redis_tower = redis_tower_round_trip(&url).await?;
    assert_eq!(redis_rs.as_deref(), redis_tower.as_deref());
    println!("both clients preserved the same binary value");
    Ok(())
}
