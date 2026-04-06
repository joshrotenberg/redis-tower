//! Batch multiple commands into a single network roundtrip with Pipeline.

use bytes::Bytes;
use redis_tower::commands::*;
use redis_tower::{Pipeline, RedisConnection};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;

    // Queue three commands and execute them in one roundtrip.
    let results = Pipeline::new()
        .push(Set::new("pipe:a", "1"))
        .push(Set::new("pipe:b", "2"))
        .push(Get::new("pipe:a"))
        .push(Get::new("pipe:b"))
        .execute(&mut conn)
        .await?;

    // Extract typed results by index.
    let a: &Option<Bytes> = results.get(2)?;
    let b: &Option<Bytes> = results.get(3)?;
    println!("a = {a:?}, b = {b:?}");

    // Clean up.
    conn.execute(Del::new("pipe:a")).await?;
    conn.execute(Del::new("pipe:b")).await?;

    Ok(())
}
