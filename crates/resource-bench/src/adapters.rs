//! Feature-selected client adapters used by isolated binaries and live tests.

#[cfg(feature = "client-redis-tower")]
mod tower {
    use crate::{FIXTURE_KEY, ProbeConnection, validate_payload};
    use async_trait::async_trait;
    use redis_tower::commands::{Get, Set};
    use redis_tower::{MultiplexedClient, RedisConnection};
    /// Direct tower baseline.
    pub struct TowerConnection(RedisConnection);
    /// Production multiplexed tower path, independently opened per worker.
    pub struct TowerMuxConnection(MultiplexedClient);
    macro_rules! adapter {
        ($type:ident, $connection:ident) => {
            #[async_trait]
            impl ProbeConnection for $type {
                async fn connect(url: &str) -> Result<Self, String> {
                    $connection::connect_url(url)
                        .await
                        .map(Self)
                        .map_err(|e| e.to_string())
                }
                async fn set_fixture(&mut self, value: &str) -> Result<(), String> {
                    self.0
                        .execute(Set::new(FIXTURE_KEY, value))
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                }
                async fn get_fixture(&mut self, expected: &[u8]) -> Result<(), String> {
                    let value = self
                        .0
                        .execute(Get::new(FIXTURE_KEY))
                        .await
                        .map_err(|e| e.to_string())?;
                    validate_payload(value.as_deref(), expected)
                }
            }
        };
    }
    adapter!(TowerConnection, RedisConnection);
    adapter!(TowerMuxConnection, MultiplexedClient);
}
#[cfg(feature = "client-redis-tower")]
pub use tower::{TowerConnection, TowerMuxConnection};

#[cfg(feature = "client-redis-rs")]
mod redis_rs {
    use crate::{FIXTURE_KEY, ProbeConnection, ProbeProfile, validate_payload};
    use async_trait::async_trait;
    use redis::{AsyncCommands, IntoConnectionInfo};
    use std::time::Duration;
    /// redis-rs multiplexed adapter with default or matched sockets.
    pub struct RedisRsConnection(redis::aio::MultiplexedConnection);
    #[async_trait]
    impl ProbeConnection for RedisRsConnection {
        async fn connect(url: &str) -> Result<Self, String> {
            Self::connect_with_profile(url, ProbeProfile::Baseline).await
        }
        async fn connect_with_profile(url: &str, profile: ProbeProfile) -> Result<Self, String> {
            profile.validate_url(url)?;
            let mut info = url.into_connection_info().map_err(|e| e.to_string())?;
            if profile == ProbeProfile::MatchedMuxResp2 {
                let keepalive = socket2::TcpKeepalive::new()
                    .with_time(Duration::from_secs(60))
                    .with_interval(Duration::from_secs(10));
                #[cfg(not(windows))]
                let keepalive = keepalive.with_retries(3);
                info = info.set_tcp_settings(
                    redis::io::tcp::TcpSettings::default()
                        .set_nodelay(true)
                        .set_keepalive(keepalive),
                );
            }
            let client = redis::Client::open(info).map_err(|e| e.to_string())?;
            client
                .get_multiplexed_async_connection()
                .await
                .map(Self)
                .map_err(|e| e.to_string())
        }
        async fn set_fixture(&mut self, value: &str) -> Result<(), String> {
            self.0
                .set::<_, _, ()>(FIXTURE_KEY, value)
                .await
                .map_err(|e| e.to_string())
        }
        async fn get_fixture(&mut self, expected: &[u8]) -> Result<(), String> {
            let value: Option<Vec<u8>> =
                self.0.get(FIXTURE_KEY).await.map_err(|e| e.to_string())?;
            validate_payload(value.as_deref(), expected)
        }
    }
}
#[cfg(feature = "client-redis-rs")]
pub use redis_rs::RedisRsConnection;

#[cfg(feature = "client-fred")]
mod fred_adapter {
    use crate::{FIXTURE_KEY, ProbeConnection, validate_payload};
    use async_trait::async_trait;
    use fred::prelude::*;
    /// Fred baseline, with uninspected default sockets (never labeled matched).
    pub struct FredConnection(Client);
    #[async_trait]
    impl ProbeConnection for FredConnection {
        async fn connect(url: &str) -> Result<Self, String> {
            let config = Config::from_url(url).map_err(|e| e.to_string())?;
            let client = Builder::from_config(config)
                .build()
                .map_err(|e| e.to_string())?;
            client.init().await.map_err(|e| e.to_string())?;
            Ok(Self(client))
        }
        async fn set_fixture(&mut self, value: &str) -> Result<(), String> {
            self.0
                .set::<(), _, _>(FIXTURE_KEY, value, None, None, false)
                .await
                .map_err(|e| e.to_string())
        }
        async fn get_fixture(&mut self, expected: &[u8]) -> Result<(), String> {
            let value: Option<Vec<u8>> =
                self.0.get(FIXTURE_KEY).await.map_err(|e| e.to_string())?;
            validate_payload(value.as_deref(), expected)
        }
    }
}
#[cfg(feature = "client-fred")]
pub use fred_adapter::FredConnection;
