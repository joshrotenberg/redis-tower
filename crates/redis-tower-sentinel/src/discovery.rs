//! Sentinel discovery: find the current master and replicas.

use std::time::Duration;

use redis_tower_core::{Frame, RedisConnection, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// Discover the current master address by querying sentinel nodes.
///
/// Tries each sentinel in order until one responds. Returns the
/// master's `"host:port"` address.
///
/// Uses a default per-sentinel timeout of 1 second so that an
/// unreachable sentinel fails fast rather than blocking on the OS TCP
/// connect timeout. See [`discover_master_with_timeout`] to customize.
pub async fn discover_master(
    sentinel_addrs: &[String],
    master_name: &str,
) -> Result<String, RedisError> {
    discover_master_with_timeout(sentinel_addrs, master_name, Duration::from_millis(1000)).await
}

/// Discover the current master address, with a per-sentinel timeout.
///
/// Like [`discover_master`], but each sentinel query is bounded by
/// `timeout`. A sentinel that does not respond within the timeout is
/// skipped and the next sentinel is tried. This prevents an unreachable
/// sentinel from blocking discovery on the OS TCP connect timeout.
pub async fn discover_master_with_timeout(
    sentinel_addrs: &[String],
    master_name: &str,
    timeout: Duration,
) -> Result<String, RedisError> {
    for addr in sentinel_addrs {
        match tokio::time::timeout(timeout, query_master_addr(addr, master_name)).await {
            Ok(Ok(master_addr)) => return Ok(master_addr),
            Ok(Err(_)) | Err(_) => continue,
        }
    }
    Err(RedisError::Redis(format!(
        "no sentinel responded for master '{master_name}'"
    )))
}

/// Discover replica addresses from a sentinel.
pub async fn discover_replicas(
    sentinel_addrs: &[String],
    master_name: &str,
) -> Result<Vec<String>, RedisError> {
    for addr in sentinel_addrs {
        match query_replicas(addr, master_name).await {
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
async fn query_master_addr(sentinel_addr: &str, master_name: &str) -> Result<String, RedisError> {
    let mut conn = RedisConnection::connect(sentinel_addr).await?;
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
async fn query_replicas(sentinel_addr: &str, master_name: &str) -> Result<Vec<String>, RedisError> {
    let mut conn = RedisConnection::connect(sentinel_addr).await?;

    // Try SENTINEL REPLICAS first (Redis 7+), fall back to SENTINEL SLAVES.
    let frame = array(vec![bulk("SENTINEL"), bulk("REPLICAS"), bulk(master_name)]);
    let responses = conn.execute_pipeline(vec![frame]).await?;
    let response = responses
        .into_iter()
        .next()
        .ok_or(RedisError::ConnectionClosed)?;

    // If REPLICAS fails (older Redis), try SLAVES.
    if let Frame::Error(_) = &response {
        let mut conn = RedisConnection::connect(sentinel_addr).await?;
        let frame = array(vec![bulk("SENTINEL"), bulk("SLAVES"), bulk(master_name)]);
        let responses = conn.execute_pipeline(vec![frame]).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

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
}
