//! # JSON API Example
//!
//! Demonstrates using the `Json<>` wrapper to store and retrieve a
//! serde-serializable struct via RedisJSON.
//!
//! **Prerequisites:**
//! - A Redis server with the RedisJSON module loaded, running on `127.0.0.1:6379`.
//! - The `serde` feature enabled for `redis-tower` (see `examples/Cargo.toml`).
//!
//! **Feature flags required:** `serde` (enables `Json`, `Search`, and serde
//! integration for the JSON and Search APIs).

use redis_tower::commands::*;
use redis_tower::{Json, RedisConnection};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct User {
    name: String,
    age: u32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
    let mut json = Json::new(&mut conn);

    let user = User {
        name: "Alice".into(),
        age: 30,
    };

    // Store the struct as a JSON document at the root path "$".
    // The value is serialized to JSON automatically via serde_json.
    json.set("json:user:1", "$", &user).await?;

    // Retrieve and deserialize it back.
    let retrieved: User = json.get("json:user:1", "$").await?;
    println!("Retrieved: {:?}", retrieved);
    assert_eq!(user, retrieved);

    // Clean up.
    let mut conn2 = RedisConnection::connect("127.0.0.1:6379").await?;
    conn2.execute(Del::new("json:user:1")).await?;

    println!("JSON API example complete.");
    Ok(())
}
