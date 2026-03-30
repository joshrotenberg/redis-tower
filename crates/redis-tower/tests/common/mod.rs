use std::sync::OnceLock;

use redis_test_harness::standalone::{RedisStandalone, StandaloneConfig};
use redis_tower::RedisConnection;

static REDIS: OnceLock<RedisStandalone> = OnceLock::new();

pub fn ensure_redis() -> &'static RedisStandalone {
    REDIS.get_or_init(|| {
        if let Ok(url) = std::env::var("REDIS_URL") {
            let addr = url
                .strip_prefix("redis://")
                .unwrap_or(&url)
                .trim_end_matches('/')
                .to_string();
            if let Some((host, port_str)) = addr.rsplit_once(':') {
                if let Ok(port) = port_str.parse::<u16>() {
                    return RedisStandalone::new(StandaloneConfig {
                        port,
                        bind: host.to_string(),
                        ..Default::default()
                    });
                }
            }
        }

        let mut standalone = RedisStandalone::with_defaults();
        standalone.start().expect("failed to start Redis server");
        standalone
    })
}

pub fn redis_addr() -> String {
    ensure_redis().addr()
}

pub async fn conn() -> RedisConnection {
    let addr = redis_addr();
    RedisConnection::connect(&addr)
        .await
        .expect("failed to connect to Redis")
}
