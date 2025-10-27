//! Sentinel discovery protocol implementation

use super::commands::{ReplicaInfo, Role, RoleInfo, SentinelGetMasterAddrByName, SentinelReplicas};
use super::config::SentinelConfig;
use crate::client::RedisConnection;
use crate::tls::TlsConfig;
use crate::types::RedisError;
use std::time::Duration;
use tokio::time::timeout;
use tracing::{debug, warn};

/// Discovers the current master address from Sentinel nodes
pub async fn discover_master(config: &SentinelConfig) -> Result<(String, u16), RedisError> {
    let mut last_error = None;

    // Try each sentinel until we get a valid master address
    for (host, port) in &config.sentinels {
        match query_sentinel_for_master(
            host,
            *port,
            &config.master_name,
            config.sentinel_username.as_deref(),
            config.sentinel_password.as_deref(),
            config.sentinel_timeout,
        )
        .await
        {
            Ok(Some(addr)) => {
                debug!(
                    "Discovered master from sentinel {}:{}: {}:{}",
                    host, port, addr.0, addr.1
                );

                // Verify the discovered address is actually a master
                match verify_master(&addr.0, addr.1, config).await {
                    Ok(true) => {
                        debug!("Verified {}:{} is a master", addr.0, addr.1);
                        return Ok(addr);
                    }
                    Ok(false) => {
                        warn!(
                            "Node {}:{} is not a master, trying next sentinel",
                            addr.0, addr.1
                        );
                        last_error = Some(RedisError::Protocol(format!(
                            "Node {}:{} is not a master",
                            addr.0, addr.1
                        )));
                        continue;
                    }
                    Err(e) => {
                        warn!("Failed to verify master {}:{}: {}", addr.0, addr.1, e);
                        last_error = Some(e);
                        continue;
                    }
                }
            }
            Ok(None) => {
                warn!(
                    "Sentinel {}:{} does not know about master '{}'",
                    host, port, config.master_name
                );
                last_error = Some(RedisError::Protocol(format!(
                    "Master '{}' not found",
                    config.master_name
                )));
                continue;
            }
            Err(e) => {
                warn!("Failed to query sentinel {}:{}: {}", host, port, e);
                last_error = Some(e);
                continue;
            }
        }
    }

    // All sentinels failed
    Err(last_error.unwrap_or_else(|| RedisError::Connection("No sentinels available".to_string())))
}

/// Discovers all healthy replica addresses from Sentinel nodes
pub async fn discover_replicas(config: &SentinelConfig) -> Result<Vec<(String, u16)>, RedisError> {
    let mut last_error = None;

    // Try each sentinel until we get replica information
    for (host, port) in &config.sentinels {
        match query_sentinel_for_replicas(
            host,
            *port,
            &config.master_name,
            config.sentinel_username.as_deref(),
            config.sentinel_password.as_deref(),
            config.sentinel_timeout,
        )
        .await
        {
            Ok(replicas) => {
                debug!(
                    "Discovered {} replicas from sentinel {}:{}",
                    replicas.len(),
                    host,
                    port
                );

                let addrs: Vec<(String, u16)> =
                    replicas.into_iter().map(|r| (r.host, r.port)).collect();

                return Ok(addrs);
            }
            Err(e) => {
                warn!(
                    "Failed to query replicas from sentinel {}:{}: {}",
                    host, port, e
                );
                last_error = Some(e);
                continue;
            }
        }
    }

    // All sentinels failed
    Err(last_error.unwrap_or_else(|| RedisError::Connection("No sentinels available".to_string())))
}

/// Query a single Sentinel node for the master address
async fn query_sentinel_for_master(
    host: &str,
    port: u16,
    master_name: &str,
    username: Option<&str>,
    password: Option<&str>,
    sentinel_timeout: Duration,
) -> Result<Option<(String, u16)>, RedisError> {
    let addr = format!("{}:{}", host, port);

    debug!("Querying sentinel {} for master '{}'", addr, master_name);

    // Connect to sentinel with timeout
    let conn = timeout(
        sentinel_timeout,
        RedisConnection::connect_with_config(&addr, TlsConfig::None),
    )
    .await
    .map_err(|_| RedisError::Connection(format!("Timeout connecting to sentinel {}", addr)))??;

    // Authenticate if credentials provided
    if let Some(pass) = password {
        authenticate_sentinel(&conn, username, pass).await?;
    }

    // Query for master address
    let cmd = SentinelGetMasterAddrByName::new(master_name);
    let result = timeout(sentinel_timeout, conn.execute(cmd))
        .await
        .map_err(|_| RedisError::Connection(format!("Timeout querying sentinel {}", addr)))??;

    Ok(result)
}

/// Query a single Sentinel node for replica addresses
async fn query_sentinel_for_replicas(
    host: &str,
    port: u16,
    master_name: &str,
    username: Option<&str>,
    password: Option<&str>,
    sentinel_timeout: Duration,
) -> Result<Vec<ReplicaInfo>, RedisError> {
    let addr = format!("{}:{}", host, port);

    debug!(
        "Querying sentinel {} for replicas of '{}'",
        addr, master_name
    );

    // Connect to sentinel with timeout (sentinels don't use TLS typically)
    let conn = timeout(
        sentinel_timeout,
        RedisConnection::connect_with_config(&addr, TlsConfig::None),
    )
    .await
    .map_err(|_| RedisError::Connection(format!("Timeout connecting to sentinel {}", addr)))??;

    // Authenticate if credentials provided
    if let Some(pass) = password {
        authenticate_sentinel(&conn, username, pass).await?;
    }

    // Query for replicas
    let cmd = SentinelReplicas::new(master_name);
    let result = timeout(sentinel_timeout, conn.execute(cmd))
        .await
        .map_err(|_| RedisError::Connection(format!("Timeout querying sentinel {}", addr)))??;

    Ok(result)
}

/// Verify that a node is actually a master using the ROLE command
async fn verify_master(host: &str, port: u16, config: &SentinelConfig) -> Result<bool, RedisError> {
    let addr = format!("{}:{}", host, port);

    debug!("Verifying {} is a master", addr);

    // Connect with timeout
    let conn = timeout(
        config.connection_timeout,
        RedisConnection::connect_with_config(&addr, config.tls.clone()),
    )
    .await
    .map_err(|_| RedisError::Connection(format!("Timeout connecting to {}", addr)))??;

    // Authenticate if password provided
    if let Some(username) = &config.redis_username {
        if let Some(password) = &config.redis_password {
            authenticate_redis_acl(&conn, username, password).await?;
        }
    } else if let Some(password) = &config.redis_password {
        authenticate_redis(&conn, password).await?;
    }

    // Execute ROLE command
    let role = timeout(config.connection_timeout, conn.execute(Role::new()))
        .await
        .map_err(|_| RedisError::Connection(format!("Timeout executing ROLE on {}", addr)))??;

    // Check if role is master
    Ok(matches!(role, RoleInfo::Master { .. }))
}

/// Authenticate with a Sentinel node
///
/// Supports both simple authentication (password only) and ACL authentication (username + password).
/// When username is provided, uses ACL authentication (Redis 6.2+).
async fn authenticate_sentinel(
    conn: &RedisConnection,
    username: Option<&str>,
    password: &str,
) -> Result<(), RedisError> {
    match username {
        Some(user) => {
            // ACL authentication (Redis 6.2+)
            use crate::commands::AuthAcl;
            conn.execute(AuthAcl::new(user, password)).await
        }
        None => {
            // Simple authentication (pre-ACL)
            use crate::commands::Auth;
            conn.execute(Auth::new(password)).await
        }
    }
}

/// Authenticate with a Redis node (simple AUTH)
async fn authenticate_redis(conn: &RedisConnection, password: &str) -> Result<(), RedisError> {
    use crate::commands::Auth;

    conn.execute(Auth::new(password)).await
}

/// Authenticate with a Redis node (ACL AUTH with username)
async fn authenticate_redis_acl(
    conn: &RedisConnection,
    username: &str,
    password: &str,
) -> Result<(), RedisError> {
    use crate::commands::AuthAcl;

    conn.execute(AuthAcl::new(username, password)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires running Sentinel
    async fn test_discover_master() {
        let config = SentinelConfig::builder()
            .sentinel_node("localhost", 26379)
            .master_name("mymaster")
            .build()
            .unwrap();

        let result = discover_master(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore] // Requires running Sentinel
    async fn test_discover_replicas() {
        let config = SentinelConfig::builder()
            .sentinel_node("localhost", 26379)
            .master_name("mymaster")
            .read_from_replicas(true)
            .build()
            .unwrap();

        let result = discover_replicas(&config).await;
        assert!(result.is_ok());
    }
}
