//! Minimal proof that the Docker fixture can drive the real redis-tower client.
//!
//! The broader server-version matrix reuses `redis-tower`'s existing
//! standalone integration suite from the nightly workflow. This test exists to
//! keep the Docker lifecycle helper itself exercised.

mod support;

use redis_tower::{RedisConnection, commands::Ping};
use std::error::Error;
use support::RedisFixture;

#[tokio::test]
#[ignore = "requires Docker; exercised by the nightly compatibility workflow"]
async fn docker_fixture_runs_the_real_client() -> Result<(), Box<dyn Error>> {
    let image = std::env::var("REDIS_TEST_IMAGE").unwrap_or_else(|_| "redis:8.8-alpine".to_owned());
    let fixture = RedisFixture::start(&image).await?;
    let mut connection = RedisConnection::connect(fixture.address()).await?;

    let pong: String = connection.execute(Ping::new()).await?;
    assert_eq!(pong, "PONG");

    Ok(())
}
