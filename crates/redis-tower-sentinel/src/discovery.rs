//! Sentinel discovery: find the current master and replicas.

use std::sync::Arc;
use std::time::Duration;

use redis_tower::credentials::{CredentialProvider, Credentials};
use redis_tower_commands::Auth;
use redis_tower_core::{Command, Frame, RedisConnection, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// Configuration for sentinel and node connections.
///
/// Holds independent credentials and (when a TLS feature is enabled) TLS
/// configs for the two hops a sentinel client makes:
///
/// - **Sentinel hop** -- connects to the sentinel nodes for discovery.
/// - **Node hop** -- connects to the discovered master.
///
/// Sentinels and the master commonly use different passwords in production, so
/// both hops are configured independently. Use [`SentinelConnectionBuilder`],
/// [`SentinelClientBuilder`], or [`MultiplexedSentinelClientBuilder`] instead
/// of constructing this directly.
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
    /// TLS configuration for sentinel connections.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub(crate) sentinel_tls: Option<Arc<redis_tower_core::tls::TlsConfig>>,
    /// TLS configuration for node (master) connections.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub(crate) node_tls: Option<Arc<redis_tower_core::tls::TlsConfig>>,
}

/// Authenticate a freshly opened connection using the given credential provider.
///
/// Sends `AUTH [user] password` and checks for `+OK`. Returns `Ok(())` on
/// success, or a `RedisError` on auth failure or unexpected response.
pub(crate) async fn authenticate(
    conn: &mut RedisConnection,
    provider: &dyn CredentialProvider,
) -> Result<(), RedisError> {
    let creds: Credentials = provider.get_credentials().await?;
    let auth_cmd = match creds.username.as_deref() {
        Some(user) => Auth::credentials(user, &creds.password),
        None => Auth::password(&creds.password),
    };
    let responses = conn.execute_pipeline(vec![auth_cmd.to_frame()]).await?;
    match responses.into_iter().next() {
        Some(Frame::SimpleString(s)) if &s[..] == b"OK" => Ok(()),
        Some(Frame::Error(e)) => Err(RedisError::Redis(String::from_utf8_lossy(&e).into_owned())),
        Some(other) => Err(RedisError::UnexpectedResponse {
            expected: "OK",
            actual: format!("{other:?}"),
        }),
        None => Err(RedisError::ConnectionClosed),
    }
}

/// Open a connection to `addr`, optionally using TLS and/or authenticating.
///
/// When a TLS feature is enabled and `tls` is `Some`, the connection is
/// upgraded via `RedisConnection::connect_tls`. Otherwise a plain TCP
/// connection is made. If `credentials` is `Some`, `AUTH` is sent
/// immediately after the connection is established.
pub(crate) async fn connect_hop(
    addr: &str,
    credentials: Option<&Arc<dyn CredentialProvider>>,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))] tls: Option<
        &Arc<redis_tower_core::tls::TlsConfig>,
    >,
) -> Result<RedisConnection, RedisError> {
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    let mut conn = match tls {
        Some(tls_cfg) => {
            let hostname = addr
                .rsplit_once(':')
                .map(|(h, _)| h)
                .unwrap_or(addr)
                .to_string();
            RedisConnection::connect_tls(addr, &hostname, tls_cfg).await?
        }
        None => RedisConnection::connect(addr).await?,
    };
    #[cfg(not(any(feature = "tls-rustls", feature = "tls-native-tls")))]
    let mut conn = RedisConnection::connect(addr).await?;

    if let Some(provider) = credentials {
        authenticate(&mut conn, provider.as_ref()).await?;
    }
    // `connect`/`connect_tls` now negotiate RESP3 by default, but the SENTINEL
    // command parsers (and the master data path discovered through here) still
    // expect RESP2 frame shapes. Pin sentinel hops to RESP2 -- after AUTH, so
    // HELLO 2 cannot hit NOAUTH. Proper RESP3 sentinel support is a follow-up.
    if conn.is_resp3() {
        let _ = conn.hello(2).await;
    }
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

/// Parse a flat key-value array into a map.
fn parse_flat_map(frame: &Frame) -> Result<std::collections::HashMap<String, String>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "flat key-value array",
                actual: format!("{other:?}"),
            });
        }
    };

    let mut map = std::collections::HashMap::new();
    let mut i = 0;
    while i + 1 < items.len() {
        let key = extract_bulk_string(&items[i])?;
        let value = extract_bulk_string(&items[i + 1])?;
        map.insert(key, value);
        i += 2;
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

    // -- SentinelConfig unit tests --

    #[test]
    fn sentinel_config_default_has_no_credentials_or_tls() {
        let config = SentinelConfig::default();
        assert!(config.sentinel_credentials.is_none());
        assert!(config.node_credentials.is_none());
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
