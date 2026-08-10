use async_trait::async_trait;
use redis::AsyncCommands;
use resource_bench::{FIXTURE_KEY, ProbeConnection, REDIS_RS_FEATURES, run_client};

struct RedisRsConnection(redis::aio::MultiplexedConnection);

#[async_trait]
impl ProbeConnection for RedisRsConnection {
    async fn connect(url: &str) -> Result<Self, String> {
        let client = redis::Client::open(url).map_err(|error| error.to_string())?;
        client
            .get_multiplexed_async_connection()
            .await
            .map(Self)
            .map_err(|error| error.to_string())
    }

    async fn set_fixture(&mut self, value: &str) -> Result<(), String> {
        self.0
            .set::<_, _, ()>(FIXTURE_KEY, value)
            .await
            .map_err(|error| error.to_string())
    }

    async fn get_fixture(&mut self, expected: &[u8]) -> Result<(), String> {
        let value: Option<Vec<u8>> = self
            .0
            .get(FIXTURE_KEY)
            .await
            .map_err(|error| error.to_string())?;
        match value {
            Some(value) if value == expected => Ok(()),
            Some(value) => Err(format!(
                "payload length mismatch: expected {}, got {}",
                expected.len(),
                value.len()
            )),
            None => Err("fixture key is missing".to_owned()),
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(error) = run_client::<RedisRsConnection>("redis-rs", REDIS_RS_FEATURES).await {
        eprintln!("resource probe failed: {error}");
        std::process::exit(1);
    }
}
