//! Versioned, configured churn policy; not measured socket attestation.
//!
//! The opt-in profile aligns a small set of named socket/deadline values.
//! Retry, routing, queue and timeout boundaries still differ between clients.

use std::time::Duration;

use redis_tower::{AutoPipelineConfig, ConnectionConfig, KeepaliveConfig};
use serde::Serialize;

use crate::clients::ClientKind;

const CONNECT: Duration = Duration::from_secs(1);
const RESPONSE: Duration = Duration::from_secs(2);
const IDLE: Duration = Duration::from_secs(60);
const INTERVAL: Duration = Duration::from_secs(10);
const PROBES: u32 = 3;

/// Fixed values avoid silently accepting misspelled numeric overrides.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub enum ChurnProfile {
    #[default]
    #[serde(rename = "client-defaults")]
    ClientDefaults,
    #[serde(rename = "socket-deadlines-v1")]
    SocketDeadlinesV1,
}

impl ChurnProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "client-defaults" => Ok(Self::ClientDefaults),
            "socket-deadlines-v1" => Ok(Self::SocketDeadlinesV1),
            _ => Err("BENCH_CHURN_PROFILE must be client-defaults or socket-deadlines-v1".into()),
        }
    }

    pub fn tower_connection(self) -> ConnectionConfig {
        let config = ConnectionConfig::default();
        match self {
            Self::ClientDefaults => config,
            Self::SocketDeadlinesV1 => config
                .with_keepalive(
                    KeepaliveConfig::new()
                        .with_idle(IDLE)
                        .with_interval(INTERVAL)
                        .with_probes(PROBES),
                )
                .with_connect_timeout(Some(CONNECT)),
        }
    }

    pub fn tower_pipeline(self) -> AutoPipelineConfig {
        AutoPipelineConfig {
            response_timeout: (self == Self::SocketDeadlinesV1).then_some(RESPONSE),
            ..AutoPipelineConfig::default()
        }
    }

    pub fn redis_rs_tcp(self) -> redis::io::tcp::TcpSettings {
        let settings = redis::io::tcp::TcpSettings::default();
        if self == Self::ClientDefaults {
            return settings;
        }
        let keepalive = redis::io::tcp::socket2::TcpKeepalive::new()
            .with_time(IDLE)
            .with_interval(INTERVAL);
        #[cfg(not(windows))]
        let keepalive = keepalive.with_retries(PROBES);
        settings.set_nodelay(true).set_keepalive(keepalive)
    }

    pub fn redis_rs_builder(self, urls: &[String]) -> redis::cluster::ClusterClientBuilder {
        let builder = redis::cluster::ClusterClient::builder(urls.to_vec());
        match self {
            Self::ClientDefaults => builder,
            Self::SocketDeadlinesV1 => builder
                .tcp_settings(self.redis_rs_tcp())
                .connection_timeout(CONNECT)
                .response_timeout(RESPONSE)
                .overall_response_timeout(Some(RESPONSE)),
        }
    }

    pub fn policy(self, kind: ClientKind) -> ChurnPolicy {
        let tower = kind == ClientKind::RedisTowerMux;
        let explicit = self == Self::SocketDeadlinesV1;
        ChurnPolicy {
            profile: self,
            provenance: "configured; defaults source-derived; not observed socket state",
            tcp_nodelay: tower || explicit,
            keepalive_idle_ms: (tower || explicit).then_some(IDLE.as_millis() as u64),
            keepalive_interval_ms: (tower || explicit).then_some(INTERVAL.as_millis() as u64),
            keepalive_probes: (tower || explicit).then_some(PROBES),
            keepalive_probes_supported: !cfg!(windows),
            connect_timeout_ms: (!tower || explicit).then_some(CONNECT.as_millis() as u64),
            connect_boundary: if tower {
                "TCP connect only; excludes negotiation and TLS"
            } else {
                "multiplexed socket/handshake creation; later cluster READONLY/PING use response timeout"
            },
            response_timeout_ms: explicit.then_some(RESPONSE.as_millis() as u64),
            response_boundary: if tower {
                "auto-pipeline batch write/reply wait; excludes queueing/reconnect/redirect loop"
            } else {
                "node request; when enabled, overall request bounded across retries/reconnections/redirects"
            },
            overall_response_timeout_ms: (!tower && explicit)
                .then_some(RESPONSE.as_millis() as u64),
            fully_matched_failure_policy: false,
            max_redirects: tower.then_some(5),
            reconnect_retries: tower.then_some(3),
            cluster_retries: (!tower).then_some(16),
            retry_policy: if tower {
                "unchanged: node reconnect jittered 100ms base/5000ms max; no application retry"
            } else {
                "unchanged redis-rs 1.7.0: cluster jitter min1280ms/max655360ms/base2/factor10; no application retry"
            },
            max_batch_size: tower.then_some(100),
            batch_window_ms: tower.then_some(0),
            queue_capacity: tower.then_some(1024),
            internal_policy: if tower {
                "auto-pipeline bounded queue; primary reads; connect policy retained for discovery/replacement/topology sockets"
            } else {
                "internal driver queue/batching unavailable; primary reads; ClusterParams retained for replacement/topology sockets"
            },
        }
    }
}

/// `None` is disabled/unavailable as explained by the boundary/policy fields,
/// never a measured zero. This is configuration, not failure-policy parity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ChurnPolicy {
    pub profile: ChurnProfile,
    pub provenance: &'static str,
    pub tcp_nodelay: bool,
    pub keepalive_idle_ms: Option<u64>,
    pub keepalive_interval_ms: Option<u64>,
    pub keepalive_probes: Option<u32>,
    pub keepalive_probes_supported: bool,
    pub connect_timeout_ms: Option<u64>,
    pub connect_boundary: &'static str,
    pub response_timeout_ms: Option<u64>,
    pub response_boundary: &'static str,
    pub overall_response_timeout_ms: Option<u64>,
    pub fully_matched_failure_policy: bool,
    pub max_redirects: Option<u64>,
    pub reconnect_retries: Option<u64>,
    pub cluster_retries: Option<u64>,
    pub retry_policy: &'static str,
    pub max_batch_size: Option<usize>,
    pub batch_window_ms: Option<u64>,
    pub queue_capacity: Option<usize>,
    pub internal_policy: &'static str,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_fixed_selector() {
        for valid in ["client-defaults", "socket-deadlines-v1"] {
            ChurnProfile::parse(valid).unwrap();
        }
        for invalid in [
            "",
            "SOCKET-DEADLINES-V1",
            "socket-deadlines-v1 ",
            "socket-deadlines-v2",
            "socket-deadlines-v1:2000",
        ] {
            assert_eq!(
                ChurnProfile::parse(invalid).unwrap_err(),
                "BENCH_CHURN_PROFILE must be client-defaults or socket-deadlines-v1"
            );
        }
    }

    #[test]
    fn actual_configuration_matches_disclosure_and_preserves_defaults() {
        for profile in [
            ChurnProfile::ClientDefaults,
            ChurnProfile::SocketDeadlinesV1,
        ] {
            let policy = profile.policy(ClientKind::RedisTowerMux);
            let connection = profile.tower_connection();
            let pipeline = profile.tower_pipeline();
            assert_eq!(
                connection.connect_timeout().map(|d| d.as_millis() as u64),
                policy.connect_timeout_ms
            );
            assert_eq!(
                connection.keepalive().idle.as_millis() as u64,
                policy.keepalive_idle_ms.unwrap()
            );
            assert_eq!(
                connection.keepalive().interval.as_millis() as u64,
                policy.keepalive_interval_ms.unwrap()
            );
            assert_eq!(
                connection.keepalive().probes,
                policy.keepalive_probes.unwrap()
            );
            assert_eq!(
                pipeline.response_timeout.map(|d| d.as_millis() as u64),
                policy.response_timeout_ms
            );
            assert_eq!(pipeline.max_batch_size, policy.max_batch_size.unwrap());
            assert_eq!(pipeline.queue_capacity, policy.queue_capacity.unwrap());
            let policy = profile.policy(ClientKind::RedisRsAsync);
            let tcp = profile.redis_rs_tcp();
            assert_eq!(tcp.nodelay(), policy.tcp_nodelay);
            assert_eq!(
                tcp.keepalive().is_some(),
                policy.keepalive_idle_ms.is_some()
            );
            if profile == ChurnProfile::SocketDeadlinesV1 {
                // TcpKeepalive has no value getters/PartialEq. Compare its
                // structured Debug representation to independently fixed
                // expected configuration, not claimed kernel observation.
                let expected = redis::io::tcp::socket2::TcpKeepalive::new()
                    .with_time(Duration::from_secs(60))
                    .with_interval(Duration::from_secs(10));
                #[cfg(not(windows))]
                let expected = expected.with_retries(3);
                assert_eq!(
                    format!("{:?}", tcp.keepalive().unwrap()),
                    format!("{expected:?}")
                );
            }
            assert!(!policy.fully_matched_failure_policy);
            assert!(policy.queue_capacity.is_none());
        }
        assert!(CONNECT < RESPONSE);
    }
}
