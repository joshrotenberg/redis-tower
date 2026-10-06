//! Declared client/runtime/socket policy for resource comparisons.

use serde::Serialize;

/// Comparison profile selected before the runtime or any socket is created.
///
/// ```
/// use resource_bench::ProbeProfile;
/// let profile = ProbeProfile::parse("matched-mux-resp2-v1")?;
/// profile.validate_url("redis://localhost/?protocol=resp2")?;
/// # Ok::<(), String>(())
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub enum ProbeProfile {
    /// Preserve the subject's existing default connection policy.
    #[default]
    #[serde(rename = "baseline")]
    Baseline,
    /// Compare multiplexed clients with explicit RESP2 and tower TCP defaults.
    #[serde(rename = "matched-mux-resp2-v1")]
    MatchedMuxResp2,
}

impl ProbeProfile {
    /// Parse a supported profile name; errors never echo input or URL secrets.
    pub fn parse(name: &str) -> Result<Self, String> {
        match name {
            "baseline" => Ok(Self::Baseline),
            "matched-mux-resp2-v1" => Ok(Self::MatchedMuxResp2),
            _ => Err("unsupported RESOURCE_PROFILE".to_owned()),
        }
    }

    /// Matched profiles accept only TCP URLs with exactly one explicit RESP2
    /// selector. Reject TLS/Unix and conflicting or duplicate selectors rather
    /// than silently applying a profile to a different transport or protocol.
    pub fn validate_url(self, raw: &str) -> Result<(), String> {
        if self == Self::Baseline {
            return Ok(());
        }
        let url = url::Url::parse(raw).map_err(|_| "invalid matched-profile URL".to_owned())?;
        let protocols: Vec<_> = url.query_pairs().collect();
        if url.scheme() != "redis"
            || url.host_str().is_none()
            || protocols.len() != 1
            || protocols[0].0 != "protocol"
            || protocols[0].1 != "resp2"
            || url.fragment().is_some()
        {
            return Err(
                "matched profile requires a TCP redis URL with exactly one protocol=resp2 selector"
                    .to_owned(),
            );
        }
        Ok(())
    }
}

/// Explicit TCP keepalive policy; probes are unavailable on Windows.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct KeepalivePolicy {
    /// Idle seconds before probing.
    pub idle_secs: u64,
    /// Interval between probes.
    pub interval_secs: u64,
    /// Number of failed probes before disconnect, where supported.
    pub probes: Option<u32>,
}

/// Recorded subject identity and measurement policy, not an OS-level socket
/// attestation. A matched profile is limited to one request per independent
/// multiplexed socket; it is not a shared-single-socket concurrency benchmark.
#[derive(Clone, Debug, Serialize)]
pub struct ProbePolicy {
    /// Selected transport, without retaining endpoint information.
    pub transport: &'static str,
    /// Selected connection implementation.
    pub client_path: &'static str,
    /// Explicit protocol selector, or uninspected baseline client/URL defaults.
    pub protocol_selection: &'static str,
    /// Socket policy identity; baseline Fred settings are deliberately unknown.
    pub socket_policy: &'static str,
    /// TCP_NODELAY setting, or unknown for the Fred baseline.
    pub tcp_nodelay: Option<bool>,
    /// Keepalive parameters, absent for disabled/uninspected baseline settings.
    pub keepalive: Option<KeepalivePolicy>,
    /// Independent physical connection population.
    pub physical_connections: usize,
    /// Maximum outstanding GETs per physical socket.
    pub inflight_per_socket: usize,
    /// Explicit Tokio runtime worker count.
    pub runtime_workers: usize,
    /// Completion-gated arrival and accounting model.
    pub measurement_model: &'static str,
}

impl ProbePolicy {
    /// Bind a supported subject to its actual adapter and selected policy.
    pub fn for_client(
        client: &str,
        profile: ProbeProfile,
        connections: usize,
        workers: usize,
        raw_url: &str,
    ) -> Result<Self, String> {
        if connections == 0 || workers == 0 || workers > 256 {
            return Err("invalid connection/runtime population".to_owned());
        }
        profile.validate_url(raw_url)?;
        let url = url::Url::parse(raw_url).map_err(|_| "invalid resource URL".to_owned())?;
        let transport = match url.scheme() {
            "redis" | "valkey" => "tcp",
            "rediss" | "valkeys" => "tls",
            "unix" | "redis+unix" | "valkey+unix" => "unix",
            _ => return Err("unsupported resource transport".to_owned()),
        };
        let (path, nodelay, keepalive, baseline_policy) = match client {
            "redis-tower" => ("redis-tower-direct", Some(true), true, "tower-default"),
            "redis-tower-mux" => ("redis-tower-multiplexed", Some(true), true, "tower-default"),
            "redis-rs" => (
                "redis-rs-multiplexed",
                Some(false),
                false,
                "redis-rs-default",
            ),
            "fred" => ("fred", None, false, "fred-default-uninspected"),
            _ => return Err("unknown resource subject".to_owned()),
        };
        let matched = profile == ProbeProfile::MatchedMuxResp2;
        if matched && !matches!(client, "redis-tower-mux" | "redis-rs") {
            return Err(
                "matched profile requires a multiplexed tower or redis-rs subject".to_owned(),
            );
        }
        Ok(Self {
            transport,
            client_path: path,
            protocol_selection: if matched {
                "resp2"
            } else {
                "client-default-or-url"
            },
            socket_policy: if transport == "unix" {
                "not-applicable-unix"
            } else if matched {
                "matched-v1"
            } else {
                baseline_policy
            },
            tcp_nodelay: if transport == "unix" {
                None
            } else if matched {
                Some(true)
            } else {
                nodelay
            },
            keepalive: (transport != "unix" && (matched || keepalive)).then_some(KeepalivePolicy {
                idle_secs: 60,
                interval_secs: 10,
                probes: if cfg!(windows) { None } else { Some(3) },
            }),
            physical_connections: connections,
            inflight_per_socket: 1,
            runtime_workers: workers,
            measurement_model: "completion-gated-staggered-get",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matched_url_rejects_protocol_and_transport_drift_without_echoing_secrets() {
        for raw in [
            "redis://u:secret@localhost/",
            "redis://localhost/?protocol=resp3",
            "redis://localhost/?protocol=resp2&protocol=resp2",
            "redis://localhost/?protocol=resp2&db=1",
            "rediss://localhost/?protocol=resp2",
            "unix:///tmp/private?protocol=resp2",
            "not-a-url-secret",
        ] {
            let error = ProbeProfile::MatchedMuxResp2.validate_url(raw).unwrap_err();
            assert!(!error.contains("secret"));
        }
        ProbeProfile::MatchedMuxResp2
            .validate_url("redis://localhost/?protocol=resp2")
            .unwrap();
        assert!(ProbeProfile::parse("unknown").is_err());
    }

    #[test]
    fn direct_and_fred_cannot_claim_matched_mux_policy() {
        for client in ["redis-tower", "fred", "unknown"] {
            assert!(
                ProbePolicy::for_client(
                    client,
                    ProbeProfile::MatchedMuxResp2,
                    4,
                    2,
                    "redis://localhost/?protocol=resp2"
                )
                .is_err()
            );
        }
        for client in ["redis-tower-mux", "redis-rs"] {
            let policy = ProbePolicy::for_client(
                client,
                ProbeProfile::MatchedMuxResp2,
                4,
                2,
                "redis://localhost/?protocol=resp2",
            )
            .unwrap();
            assert_eq!(policy.tcp_nodelay, Some(true));
            assert_eq!(policy.keepalive.unwrap().idle_secs, 60);
            assert_eq!(policy.protocol_selection, "resp2");
        }
    }

    #[test]
    fn unix_baseline_does_not_claim_tcp_socket_options() {
        let policy = ProbePolicy::for_client(
            "redis-tower",
            ProbeProfile::Baseline,
            4,
            2,
            "unix:///tmp/resource-private.sock",
        )
        .unwrap();
        assert_eq!(policy.transport, "unix");
        assert_eq!(policy.socket_policy, "not-applicable-unix");
        assert!(policy.tcp_nodelay.is_none());
        assert!(policy.keepalive.is_none());
    }

    #[test]
    fn tower_baseline_preserves_valkey_transport_aliases() {
        for (url, transport) in [
            ("valkey://localhost/", "tcp"),
            ("valkeys://localhost/", "tls"),
            ("valkey+unix:///tmp/private.sock", "unix"),
        ] {
            let policy =
                ProbePolicy::for_client("redis-tower", ProbeProfile::Baseline, 4, 2, url).unwrap();
            assert_eq!(policy.transport, transport);
        }
    }
}
