//! Sentinel discovery for the current master and its replicas.
//!
//! The free functions in this module are useful when an application needs to
//! inspect Sentinel directly. Most applications should use
//! [`crate::SentinelConnection`], [`crate::SentinelClient`], or
//! [`crate::MultiplexedSentinelClient`], which combine discovery with command
//! execution and failover recovery.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower_sentinel::discovery::discover_master;
//!
//! let sentinels = vec![
//!     "127.0.0.1:26379".to_owned(),
//!     "127.0.0.1:26380".to_owned(),
//! ];
//! let master = discover_master(&sentinels, "mymaster").await?;
//! println!("current master: {master}");
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use redis_tower::NodeAddr;
use redis_tower::credentials::{CredentialProvider, authenticate_with_refresh};
use redis_tower_core::{ConnectionConfig, Frame, ProtocolVersion, RedisConnection, RedisError};
use redis_tower_protocol::RespLimits;
use redis_tower_protocol::helpers::{array, bulk};

/// Configuration for sentinel and node connections.
///
/// Holds independent credentials and (when a TLS feature is enabled) TLS
/// configs for the two hops a sentinel client makes, plus RESP decode limits
/// shared by every connection:
///
/// - **Sentinel hop** -- connects to the sentinel nodes for discovery.
/// - **Node hop** -- connects to the discovered master.
///
/// Sentinels and the master commonly use different passwords in production, so
/// credentials and TLS are configured independently. RESP limits apply to both
/// hops, including connections created during failover. Use
/// [`SentinelConnectionBuilder`], [`SentinelClientBuilder`], or
/// [`MultiplexedSentinelClientBuilder`] instead of constructing this directly.
///
/// [`SentinelConnectionBuilder`]: crate::connection::SentinelConnectionBuilder
/// [`SentinelClientBuilder`]: crate::client::SentinelClientBuilder
/// [`MultiplexedSentinelClientBuilder`]: crate::multiplexed::MultiplexedSentinelClientBuilder
#[derive(Clone, Default)]
pub struct SentinelConfig {
    /// Credentials for authenticating to sentinel nodes.
    pub(crate) sentinel_credentials: Option<Arc<dyn CredentialProvider>>,
    /// Credentials for authenticating to the Redis data node (master).
    pub(crate) node_credentials: Option<Arc<dyn CredentialProvider>>,
    /// RESP decode limits applied to every sentinel and data-node connection.
    pub(crate) resp_limits: RespLimits,
    /// TLS configuration for sentinel connections.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub(crate) sentinel_tls: Option<Arc<redis_tower_core::tls::TlsConfig>>,
    /// TLS configuration for node (master) connections.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub(crate) node_tls: Option<Arc<redis_tower_core::tls::TlsConfig>>,
}

/// Open a connection to `addr`, optionally using TLS and/or authenticating.
///
/// When a TLS feature is enabled and `tls` is `Some`, the connection is
/// upgraded via `RedisConnection::connect_tls_with_config`. Otherwise a plain
/// TCP connection is made. The configured RESP limits apply before any
/// connection setup response is decoded. The transport starts in RESP2;
/// optional `AUTH` completes before automatic RESP3 negotiation so a protected
/// server cannot make `HELLO` silently fall back to RESP2.
pub(crate) async fn connect_hop(
    addr: &str,
    credentials: Option<&Arc<dyn CredentialProvider>>,
    resp_limits: RespLimits,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))] tls: Option<
        &Arc<redis_tower_core::tls::TlsConfig>,
    >,
) -> Result<RedisConnection, RedisError> {
    let connection_config = ConnectionConfig::new()
        .with_resp_limits(resp_limits)
        .with_protocol(ProtocolVersion::Resp2);
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    let conn = match tls {
        Some(tls_cfg) => {
            let hostname = addr
                .rsplit_once(':')
                .map(|(h, _)| h)
                .unwrap_or(addr)
                .to_string();
            RedisConnection::connect_tls_with_config(addr, &hostname, tls_cfg, &connection_config)
                .await?
        }
        None => RedisConnection::connect_with_config(addr, &connection_config).await?,
    };
    #[cfg(not(any(feature = "tls-rustls", feature = "tls-native-tls")))]
    let conn = RedisConnection::connect_with_config(addr, &connection_config).await?;

    finish_hop_setup(conn, credentials).await
}

async fn finish_hop_setup(
    mut conn: RedisConnection,
    credentials: Option<&Arc<dyn CredentialProvider>>,
) -> Result<RedisConnection, RedisError> {
    if let Some(provider) = credentials {
        authenticate_with_refresh(&mut conn, provider.as_ref()).await?;
    }
    conn.negotiate_protocol(ProtocolVersion::Auto).await?;
    Ok(conn)
}

/// Discover the current master address by querying sentinel nodes.
///
/// Tries each sentinel in order until one responds. Returns the
/// master's `"host:port"` address.
///
/// Uses a default per-sentinel timeout of 1 second so that an
/// unreachable sentinel fails fast rather than blocking on the OS TCP
/// connect timeout. See [`discover_master_with_timeout`] to customize.
///
/// Sentinel connections are made without credentials or TLS. For auth/TLS,
/// use [`SentinelConnection::builder`](crate::connection::SentinelConnection::builder).
pub async fn discover_master(
    sentinel_addrs: &[String],
    master_name: &str,
) -> Result<String, RedisError> {
    discover_master_with_config(sentinel_addrs, master_name, &SentinelConfig::default()).await
}

/// Discover the current master address, with a per-sentinel timeout.
///
/// Like [`discover_master`], but each sentinel query is bounded by
/// `timeout`. A sentinel that does not respond within the timeout is
/// skipped and the next sentinel is tried. This prevents an unreachable
/// sentinel from blocking discovery on the OS TCP connect timeout.
///
/// Sentinel connections are made without credentials or TLS. For auth/TLS,
/// use [`SentinelConnection::builder`](crate::connection::SentinelConnection::builder).
pub async fn discover_master_with_timeout(
    sentinel_addrs: &[String],
    master_name: &str,
    timeout: Duration,
) -> Result<String, RedisError> {
    discover_master_with_config_timeout(
        sentinel_addrs,
        master_name,
        &SentinelConfig::default(),
        timeout,
    )
    .await
}

/// Discover the current master address using the given sentinel config.
///
/// Uses the config's sentinel credentials and TLS for sentinel connections.
/// Uses a default per-sentinel timeout of 1 second.
pub(crate) async fn discover_master_with_config(
    sentinel_addrs: &[String],
    master_name: &str,
    config: &SentinelConfig,
) -> Result<String, RedisError> {
    discover_master_with_config_timeout(
        sentinel_addrs,
        master_name,
        config,
        Duration::from_millis(1000),
    )
    .await
}

/// Discover the current master address using the given sentinel config and timeout.
pub(crate) async fn discover_master_with_config_timeout(
    sentinel_addrs: &[String],
    master_name: &str,
    config: &SentinelConfig,
    timeout: Duration,
) -> Result<String, RedisError> {
    for addr in sentinel_addrs {
        match tokio::time::timeout(timeout, query_master_addr(addr, master_name, config)).await {
            Ok(Ok(master_addr)) => return Ok(master_addr),
            Ok(Err(e)) => {
                tracing::warn!(
                    sentinel_addr = %addr,
                    master_name = %master_name,
                    error = %e,
                    "sentinel: failed to query sentinel"
                );
                continue;
            }
            Err(_timeout) => {
                tracing::warn!(
                    sentinel_addr = %addr,
                    master_name = %master_name,
                    "sentinel: timed out querying sentinel"
                );
                continue;
            }
        }
    }
    Err(RedisError::Redis(format!(
        "no sentinel responded for master '{master_name}'"
    )))
}

/// Discover replica addresses from a sentinel.
///
/// Sentinel connections are made without credentials or TLS. For auth/TLS,
/// use [`SentinelConnection::builder`](crate::connection::SentinelConnection::builder).
pub async fn discover_replicas(
    sentinel_addrs: &[String],
    master_name: &str,
) -> Result<Vec<String>, RedisError> {
    discover_replicas_with_config(sentinel_addrs, master_name, &SentinelConfig::default()).await
}

/// Discover replica addresses using the given sentinel config.
pub(crate) async fn discover_replicas_with_config(
    sentinel_addrs: &[String],
    master_name: &str,
    config: &SentinelConfig,
) -> Result<Vec<String>, RedisError> {
    for addr in sentinel_addrs {
        match query_replicas(addr, master_name, config).await {
            Ok(replicas) => return Ok(replicas),
            Err(_) => continue,
        }
    }
    Err(RedisError::Redis(format!(
        "no sentinel responded for replicas of '{master_name}'"
    )))
}

/// Discover replicas via sentinel and connect to each one.
///
/// Best-effort on both axes: a sentinel discovery failure is treated the
/// same as an empty replica list, and a replica that refuses the connection
/// is logged and skipped rather than failing the whole call. Callers apply
/// their configured read preference when this returns an empty result.
///
/// Uses `config.node_credentials` and `config.node_tls`, matching the master
/// (node) hop, since sentinel-monitored replicas commonly share the master's
/// data-plane credentials.
pub(crate) async fn connect_replicas(
    sentinel_addrs: &[String],
    master_name: &str,
    config: &SentinelConfig,
) -> (HashMap<String, RedisConnection>, Vec<NodeAddr>) {
    let addrs = match discover_replicas_with_config(sentinel_addrs, master_name, config).await {
        Ok(addrs) => addrs,
        Err(error) => {
            tracing::warn!(
                master_name,
                error = %error,
                "sentinel: replica discovery failed, no replica is available"
            );
            return (HashMap::new(), Vec::new());
        }
    };

    let mut connections = HashMap::new();
    let mut resolved = Vec::new();
    for addr in addrs {
        let Some(node) = NodeAddr::parse(&addr) else {
            tracing::warn!(addr, "sentinel: replica address is not host:port, skipping");
            continue;
        };
        match connect_hop(
            &addr,
            config.node_credentials.as_ref(),
            config.resp_limits,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            config.node_tls.as_ref(),
        )
        .await
        {
            Ok(conn) => {
                if connections.insert(node.addr_string(), conn).is_none() {
                    resolved.push(node);
                }
            }
            Err(error) => {
                tracing::warn!(
                    addr,
                    error = %error,
                    "sentinel: failed to connect to replica, skipping"
                );
            }
        }
    }
    (connections, resolved)
}

/// Query a single sentinel for the master address.
///
/// Sends `SENTINEL GET-MASTER-ADDR-BY-NAME <name>` and parses the
/// response (a two-element array: \[host, port\]).
async fn query_master_addr(
    sentinel_addr: &str,
    master_name: &str,
    config: &SentinelConfig,
) -> Result<String, RedisError> {
    let mut conn = connect_hop(
        sentinel_addr,
        config.sentinel_credentials.as_ref(),
        config.resp_limits,
        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
        config.sentinel_tls.as_ref(),
    )
    .await?;
    let frame = array(vec![
        bulk("SENTINEL"),
        bulk("GET-MASTER-ADDR-BY-NAME"),
        bulk(master_name),
    ]);
    let responses = conn.execute_pipeline(vec![frame]).await?;
    let response = responses
        .into_iter()
        .next()
        .ok_or(RedisError::ConnectionClosed)?;

    parse_addr_response(&response)
}

/// Query a sentinel for replica addresses.
///
/// Sends `SENTINEL REPLICAS <name>` (Redis 7+) and parses the response.
async fn query_replicas(
    sentinel_addr: &str,
    master_name: &str,
    config: &SentinelConfig,
) -> Result<Vec<String>, RedisError> {
    let mut conn = connect_hop(
        sentinel_addr,
        config.sentinel_credentials.as_ref(),
        config.resp_limits,
        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
        config.sentinel_tls.as_ref(),
    )
    .await?;

    // Try SENTINEL REPLICAS first (Redis 7+), fall back to SENTINEL SLAVES.
    let frame = array(vec![bulk("SENTINEL"), bulk("REPLICAS"), bulk(master_name)]);
    let responses = conn.execute_pipeline(vec![frame]).await?;
    let response = responses
        .into_iter()
        .next()
        .ok_or(RedisError::ConnectionClosed)?;

    // If REPLICAS fails (older Redis), try SLAVES.
    if let Frame::Error(_) = &response {
        let mut conn2 = connect_hop(
            sentinel_addr,
            config.sentinel_credentials.as_ref(),
            config.resp_limits,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            config.sentinel_tls.as_ref(),
        )
        .await?;
        let frame = array(vec![bulk("SENTINEL"), bulk("SLAVES"), bulk(master_name)]);
        let responses = conn2.execute_pipeline(vec![frame]).await?;
        let response = responses
            .into_iter()
            .next()
            .ok_or(RedisError::ConnectionClosed)?;
        return parse_replicas_response(&response);
    }

    parse_replicas_response(&response)
}

/// Parse the response from SENTINEL GET-MASTER-ADDR-BY-NAME.
///
/// Returns `"host:port"`.
fn parse_addr_response(frame: &Frame) -> Result<String, RedisError> {
    match frame {
        Frame::Array(Some(items)) if items.len() == 2 => {
            let host = extract_bulk_string(&items[0])?;
            let port = extract_bulk_string(&items[1])?;
            Ok(format!("{host}:{port}"))
        }
        Frame::Null | Frame::Array(None) => Err(RedisError::Redis(
            "master not found by sentinel".to_string(),
        )),
        other => Err(RedisError::UnexpectedResponse {
            expected: "two-element array [host, port]",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse the response from SENTINEL REPLICAS/SLAVES.
///
/// Returns a list of `"host:port"` addresses.
fn parse_replicas_response(frame: &Frame) -> Result<Vec<String>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) => items,
        Frame::Array(None) => return Ok(Vec::new()),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of replica info",
                actual: format!("{other:?}"),
            });
        }
    };

    let mut addrs = Vec::new();
    for item in items {
        // Each replica is a flat array of alternating key-value pairs.
        if let Ok(map) = parse_flat_map(item)
            && let (Some(ip), Some(port)) = (map.get("ip"), map.get("port"))
        {
            addrs.push(format!("{ip}:{port}"));
        }
    }
    Ok(addrs)
}

/// Parse a key-value reply into a map, accepting both the RESP2 flat
/// key-value array and the RESP3 map shape.
fn parse_flat_map(frame: &Frame) -> Result<std::collections::HashMap<String, String>, RedisError> {
    let mut map = std::collections::HashMap::new();
    match frame {
        Frame::Array(Some(items)) => {
            let mut i = 0;
            while i + 1 < items.len() {
                let key = extract_bulk_string(&items[i])?;
                let value = extract_bulk_string(&items[i + 1])?;
                map.insert(key, value);
                i += 2;
            }
        }
        // RESP3 returns the per-replica info as a map rather than a flat array.
        Frame::Map(pairs) => {
            for (k, v) in pairs {
                map.insert(extract_bulk_string(k)?, extract_bulk_string(v)?);
            }
        }
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "flat key-value array or map",
                actual: format!("{other:?}"),
            });
        }
    }
    Ok(map)
}

fn extract_bulk_string(frame: &Frame) -> Result<String, RedisError> {
    match frame {
        Frame::BulkString(Some(b)) => Ok(String::from_utf8_lossy(b).into_owned()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "bulk string",
            actual: format!("{other:?}"),
        }),
    }
}

/// True if a `ROLE` reply indicates the connected node is a master.
///
/// `ROLE` returns an array whose first element is the role name -- `master`,
/// `slave`, or `sentinel`. Sentinel's view of which node is the master can lag
/// a real failover, so after (re)connecting to the address it reports, callers
/// confirm the node actually reports `master` before trusting it for writes --
/// otherwise they rebind to the demoted replica and keep getting READONLY.
pub(crate) fn role_reports_master(frame: &Frame) -> bool {
    let Frame::Array(Some(items)) = frame else {
        return false;
    };
    match items.first() {
        Some(Frame::BulkString(Some(b))) => b.eq_ignore_ascii_case(b"master"),
        Some(Frame::SimpleString(b)) => b.eq_ignore_ascii_case(b"master"),
        _ => false,
    }
}

/// Issue `ROLE` on `conn` and report whether the node is currently a master.
pub(crate) async fn connection_is_master(conn: &mut RedisConnection) -> Result<bool, RedisError> {
    let responses = conn
        .execute_pipeline(vec![array(vec![bulk("ROLE")])])
        .await?;
    let frame = responses
        .into_iter()
        .next()
        .ok_or(RedisError::ConnectionClosed)?;
    Ok(role_reports_master(&frame))
}

/// Discover the master via sentinel, connect to it, and verify it actually
/// reports the master role -- retrying with exponential backoff while
/// sentinel's view lags a failover (or returns the just-demoted old master).
///
/// Returns the verified connection and the master's `"host:port"` address.
/// Sentinel connections use default config (no auth, no TLS). For auth/TLS,
/// use [`connect_verified_master_with_config`].
pub(crate) async fn connect_verified_master(
    sentinel_addrs: &[String],
    master_name: &str,
) -> Result<(RedisConnection, String), RedisError> {
    connect_verified_master_with_config(sentinel_addrs, master_name, &SentinelConfig::default())
        .await
}

/// Discover the master via sentinel and verify its role, using the given config.
///
/// The sentinel hop uses `config.sentinel_credentials` and `config.sentinel_tls`.
/// The node (master) hop uses `config.node_credentials` and `config.node_tls`.
/// Both hops use `config.resp_limits`.
/// Returns the verified connection and the master's `"host:port"` address.
pub(crate) async fn connect_verified_master_with_config(
    sentinel_addrs: &[String],
    master_name: &str,
    config: &SentinelConfig,
) -> Result<(RedisConnection, String), RedisError> {
    const MAX_ATTEMPTS: u32 = 5;
    const BASE_BACKOFF: Duration = Duration::from_millis(100);

    let mut last_err: Option<RedisError> = None;
    for attempt in 0..MAX_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(BASE_BACKOFF * 2u32.pow(attempt - 1)).await;
        }

        let master_addr =
            match discover_master_with_config(sentinel_addrs, master_name, config).await {
                Ok(addr) => addr,
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            };
        let mut conn = match connect_hop(
            &master_addr,
            config.node_credentials.as_ref(),
            config.resp_limits,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            config.node_tls.as_ref(),
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                last_err = Some(e);
                continue;
            }
        };
        match connection_is_master(&mut conn).await {
            Ok(true) => return Ok((conn, master_addr)),
            Ok(false) => {
                tracing::warn!(
                    addr = %master_addr,
                    master_name,
                    attempt,
                    "sentinel: discovered node is not yet a master, retrying"
                );
                last_err = Some(RedisError::Redis(format!(
                    "sentinel returned {master_addr} but it does not report the master role"
                )));
            }
            Err(e) => last_err = Some(e),
        }
    }

    Err(last_err
        .unwrap_or_else(|| RedisError::Redis("sentinel master discovery exhausted".to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    #[cfg(unix)]
    async fn protected_hop_authenticates_before_resp3_negotiation() {
        use futures::{SinkExt, StreamExt};
        use redis_tower::credentials::StaticCredentials;
        use redis_tower_core::RedisStream;
        use redis_tower_protocol::RespCodec;
        use tokio_util::codec::Framed;

        let (client, server) = tokio::net::UnixStream::pair().unwrap();
        let provider: Arc<dyn CredentialProvider> = Arc::new(StaticCredentials::password("secret"));
        let server_task = tokio::spawn(async move {
            let mut framed = Framed::new(RedisStream::Unix(server), RespCodec::new());
            let auth = framed.next().await.unwrap().unwrap();
            assert_eq!(auth.as_array().unwrap()[0].as_str(), Some("AUTH"));
            framed
                .send(Frame::SimpleString(Bytes::from_static(b"OK")))
                .await
                .unwrap();

            let hello = framed.next().await.unwrap().unwrap();
            assert_eq!(hello.as_array().unwrap()[0].as_str(), Some("HELLO"));
            framed
                .send(Frame::Map(vec![(
                    Frame::BulkString(Some(Bytes::from_static(b"proto"))),
                    Frame::Integer(3),
                )]))
                .await
                .unwrap();
        });

        let connection = RedisConnection::from_stream(RedisStream::Unix(client));
        let connection = finish_hop_setup(connection, Some(&provider)).await.unwrap();
        assert!(connection.is_resp3());
        server_task.await.unwrap();
    }

    #[test]
    fn role_reports_master_detects_master() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("master"))),
            Frame::Integer(12345),
        ]));
        assert!(role_reports_master(&frame));
    }

    #[test]
    fn role_reports_master_rejects_replica() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("slave"))),
            Frame::BulkString(Some(Bytes::from("127.0.0.1"))),
        ]));
        assert!(!role_reports_master(&frame));
    }

    #[test]
    fn role_reports_master_is_case_insensitive_and_handles_garbage() {
        assert!(role_reports_master(&Frame::Array(Some(vec![
            Frame::SimpleString(Bytes::from("MASTER"))
        ]))));
        assert!(!role_reports_master(&Frame::Null));
        assert!(!role_reports_master(&Frame::Array(Some(vec![]))));
        assert!(!role_reports_master(&Frame::Array(None)));
    }

    #[test]
    fn parse_master_addr() {
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("127.0.0.1"))),
            Frame::BulkString(Some(Bytes::from("6380"))),
        ]));
        assert_eq!(parse_addr_response(&frame).unwrap(), "127.0.0.1:6380");
    }

    #[test]
    fn parse_master_addr_null() {
        let frame = Frame::Null;
        assert!(parse_addr_response(&frame).is_err());
    }

    #[test]
    fn parse_replicas_empty() {
        let frame = Frame::Array(Some(vec![]));
        let addrs = parse_replicas_response(&frame).unwrap();
        assert!(addrs.is_empty());
    }

    #[test]
    fn parse_replicas_with_entries() {
        let replica = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("ip"))),
            Frame::BulkString(Some(Bytes::from("127.0.0.1"))),
            Frame::BulkString(Some(Bytes::from("port"))),
            Frame::BulkString(Some(Bytes::from("6381"))),
            Frame::BulkString(Some(Bytes::from("flags"))),
            Frame::BulkString(Some(Bytes::from("slave"))),
        ]));
        let frame = Frame::Array(Some(vec![replica]));
        let addrs = parse_replicas_response(&frame).unwrap();
        assert_eq!(addrs, vec!["127.0.0.1:6381"]);
    }

    #[test]
    fn parse_replicas_with_resp3_map_entries() {
        // RESP3 returns each replica's info as a map rather than a flat array.
        let replica = Frame::Map(vec![
            (
                Frame::BulkString(Some(Bytes::from("ip"))),
                Frame::BulkString(Some(Bytes::from("127.0.0.1"))),
            ),
            (
                Frame::BulkString(Some(Bytes::from("port"))),
                Frame::BulkString(Some(Bytes::from("6381"))),
            ),
            (
                Frame::BulkString(Some(Bytes::from("flags"))),
                Frame::BulkString(Some(Bytes::from("slave"))),
            ),
        ]);
        let frame = Frame::Array(Some(vec![replica]));
        let addrs = parse_replicas_response(&frame).unwrap();
        assert_eq!(addrs, vec!["127.0.0.1:6381"]);
    }

    // -- SentinelConfig unit tests --

    #[test]
    fn sentinel_config_default_preserves_connection_defaults() {
        let config = SentinelConfig::default();
        assert!(config.sentinel_credentials.is_none());
        assert!(config.node_credentials.is_none());
        assert_eq!(config.resp_limits, RespLimits::default());
        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
        {
            assert!(config.sentinel_tls.is_none());
            assert!(config.node_tls.is_none());
        }
    }

    #[test]
    fn sentinel_config_clone_is_independent() {
        use redis_tower::credentials::StaticCredentials;
        let config = SentinelConfig {
            sentinel_credentials: Some(Arc::new(StaticCredentials::password("s"))),
            ..SentinelConfig::default()
        };
        let cloned = config.clone();
        assert!(cloned.sentinel_credentials.is_some());
    }
}
