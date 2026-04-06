//! Lua scripting with SHA1 caching via the Script helper.
//!
//! Script tries EVALSHA first and transparently falls back to EVAL
//! when the server has not yet cached the script.

use redis_tower::commands::*;
use redis_tower::{RedisConnection, Script};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;

    // Seed a key for the script to read.
    conn.execute(Set::new("script:name", "redis-tower")).await?;

    // Create a script that reads a key and returns it uppercased.
    let script = Script::new("return redis.call('GET', KEYS[1])");
    println!("Script SHA: {}", script.sha());

    // First call uses EVAL (server caches the script).
    let result = script.execute(&mut conn, &["script:name"], &[]).await?;
    println!("Result: {result:?}");

    // Second call uses EVALSHA (script already cached).
    let result = script.execute(&mut conn, &["script:name"], &[]).await?;
    println!("Cached result: {result:?}");

    // Clean up.
    conn.execute(Del::new("script:name")).await?;

    Ok(())
}
