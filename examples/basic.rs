//! Basic redis-tower usage: connect, set a key, get it back, then clean up.

use redis_tower::commands::*;
use redis_tower::{RedisConnection, RedisValueExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to a local Redis server.
    let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;

    // SET a key.
    conn.execute(Set::new("example:key", "hello")).await?;

    // GET the key and convert from Option<Bytes> to String.
    let val: String = conn.execute(Get::new("example:key")).await?.parse_into()?;
    println!("Got: {val}");

    // Clean up.
    conn.execute(Del::new("example:key")).await?;

    Ok(())
}
