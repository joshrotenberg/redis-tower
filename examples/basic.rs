//! Basic usage example

use redis_tower::RedisClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Connect to Redis
    let _client = RedisClient::connect("localhost:6379").await?;

    // TODO: Demonstrate strongly typed commands
    // let value: Option<String> = client.call(Get::new("my_key")).await?;
    // println!("Value: {:?}", value);

    println!("redis-tower basic example");
    println!("TODO: Implement RedisClient service");

    Ok(())
}
