use redis_tower::client::RedisClient;

/// Setup a standalone Redis client
///
/// This expects a Redis instance to be running on localhost:6379.
/// You can start one with: redis-server
///
/// For tests, the cluster script also starts standalone nodes we can use.
pub async fn setup_redis() -> RedisClient {
    RedisClient::connect("localhost:6379")
        .await
        .expect("Failed to connect to Redis on localhost:6379. Is Redis running?")
}
