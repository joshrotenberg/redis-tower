use async_trait::async_trait;
use fred::prelude::*;
use resource_bench::{FIXTURE_KEY, FRED_FEATURES, ProbeConnection, run_client};

struct FredConnection(Client);

#[async_trait]
impl ProbeConnection for FredConnection {
    async fn connect(url: &str) -> Result<Self, String> {
        let config = Config::from_url(url).map_err(|error| error.to_string())?;
        let client = Builder::from_config(config)
            .build()
            .map_err(|error| error.to_string())?;
        client.init().await.map_err(|error| error.to_string())?;
        Ok(Self(client))
    }

    async fn set_fixture(&mut self, value: &str) -> Result<(), String> {
        self.0
            .set::<(), _, _>(FIXTURE_KEY, value, None, None, false)
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
    if let Err(error) = run_client::<FredConnection>("fred", FRED_FEATURES).await {
        eprintln!("resource probe failed: {error}");
        std::process::exit(1);
    }
}
