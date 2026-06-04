use redis_server_wrapper::RedisServer;
use redis_tower::RedisConnection;
use tokio::sync::OnceCell;

static REDIS: OnceCell<redis_server_wrapper::RedisServerHandle> = OnceCell::const_new();
static REDIS_ADDR: OnceCell<String> = OnceCell::const_new();

pub async fn redis_addr() -> &'static str {
    REDIS_ADDR
        .get_or_init(|| async {
            if let Ok(url) = std::env::var("REDIS_URL") {
                return url
                    .strip_prefix("redis://")
                    .unwrap_or(&url)
                    .trim_end_matches('/')
                    .to_string();
            }

            let handle = RedisServer::new()
                .port(6399)
                .start()
                .await
                .expect("failed to start Redis server");
            let addr = handle.addr();
            REDIS.set(handle).ok();
            addr
        })
        .await
}

#[allow(dead_code)]
pub async fn conn() -> RedisConnection {
    let addr = redis_addr().await;
    RedisConnection::connect(addr)
        .await
        .expect("failed to connect to Redis")
}
