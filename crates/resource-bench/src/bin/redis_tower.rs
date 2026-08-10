use async_trait::async_trait;
use redis_tower::RedisConnection;
use redis_tower::commands::{Get, Set};
use resource_bench::{FIXTURE_KEY, ProbeConnection, REDIS_TOWER_FEATURES, run_client};

struct TowerConnection(RedisConnection);

#[async_trait]
impl ProbeConnection for TowerConnection {
    async fn connect(url: &str) -> Result<Self, String> {
        RedisConnection::connect_url(url)
            .await
            .map(Self)
            .map_err(|error| error.to_string())
    }

    async fn set_fixture(&mut self, value: &str) -> Result<(), String> {
        self.0
            .execute(Set::new(FIXTURE_KEY, value))
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn get_fixture(&mut self, expected: &[u8]) -> Result<(), String> {
        match self.0.execute(Get::new(FIXTURE_KEY)).await {
            Ok(Some(value)) if value.as_ref() == expected => Ok(()),
            Ok(Some(value)) => Err(format!(
                "payload length mismatch: expected {}, got {}",
                expected.len(),
                value.len()
            )),
            Ok(None) => Err("fixture key is missing".to_owned()),
            Err(error) => Err(error.to_string()),
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(error) = run_client::<TowerConnection>("redis-tower", REDIS_TOWER_FEATURES).await {
        eprintln!("resource probe failed: {error}");
        std::process::exit(1);
    }
}
