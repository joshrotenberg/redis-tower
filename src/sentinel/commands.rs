//! Sentinel-specific Redis commands

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// SENTINEL get-master-addr-by-name command
///
/// Returns the IP address and port of the master with the given name.
#[derive(Debug, Clone)]
pub struct SentinelGetMasterAddrByName {
    master_name: String,
}

impl SentinelGetMasterAddrByName {
    /// Create a new SENTINEL get-master-addr-by-name command
    pub fn new(master_name: impl Into<String>) -> Self {
        Self {
            master_name: master_name.into(),
        }
    }
}

impl Command for SentinelGetMasterAddrByName {
    type Response = Option<(String, u16)>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SENTINEL"))),
            Frame::BulkString(Some(Bytes::from("get-master-addr-by-name"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.master_name.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(mut elements) if elements.len() == 2 => {
                let port_frame = elements.pop().unwrap();
                let host_frame = elements.pop().unwrap();

                let host = match host_frame {
                    Frame::BulkString(Some(data)) => String::from_utf8(data.as_ref().to_vec())
                        .map_err(|e| {
                            RedisError::Protocol(format!("Invalid UTF-8 in host: {}", e))
                        })?,
                    _ => {
                        return Err(RedisError::Protocol(
                            "Expected bulk string for host".to_string(),
                        ));
                    }
                };

                let port_str = match port_frame {
                    Frame::BulkString(Some(data)) => String::from_utf8(data.as_ref().to_vec())
                        .map_err(|e| {
                            RedisError::Protocol(format!("Invalid UTF-8 in port: {}", e))
                        })?,
                    _ => {
                        return Err(RedisError::Protocol(
                            "Expected bulk string for port".to_string(),
                        ));
                    }
                };

                let port = port_str
                    .parse::<u16>()
                    .map_err(|e| RedisError::Protocol(format!("Invalid port number: {}", e)))?;

                Ok(Some((host, port)))
            }
            Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol(
                "Unexpected response format".to_string(),
            )),
        }
    }
}

/// SENTINEL replicas command
///
/// Returns information about all replicas of the specified master.
#[derive(Debug, Clone)]
pub struct SentinelReplicas {
    master_name: String,
}

impl SentinelReplicas {
    /// Create a new SENTINEL replicas command
    pub fn new(master_name: impl Into<String>) -> Self {
        Self {
            master_name: master_name.into(),
        }
    }
}

/// Information about a Redis replica
#[derive(Debug, Clone, PartialEq)]
pub struct ReplicaInfo {
    /// Replica host address
    pub host: String,
    /// Replica port
    pub port: u16,
    /// Replica flags (e.g., "slave", "s_down", "o_down")
    pub flags: String,
}

impl Command for SentinelReplicas {
    type Response = Vec<ReplicaInfo>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SENTINEL"))),
            Frame::BulkString(Some(Bytes::from("replicas"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.master_name.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(replicas) => {
                let mut result = Vec::new();

                for replica_frame in replicas {
                    if let Frame::Array(fields) = replica_frame {
                        let replica = parse_replica_fields(fields)?;

                        // Filter out replicas marked as down
                        if !replica.flags.contains("s_down") && !replica.flags.contains("o_down") {
                            result.push(replica);
                        }
                    }
                }

                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Expected array response".to_string())),
        }
    }
}

fn parse_replica_fields(fields: Vec<Frame>) -> Result<ReplicaInfo, RedisError> {
    let mut host = None;
    let mut port = None;
    let mut flags = None;

    let mut i = 0;
    while i < fields.len() {
        if let Frame::BulkString(Some(key_data)) = &fields[i] {
            let key = String::from_utf8(key_data.as_ref().to_vec())
                .map_err(|e| RedisError::Protocol(format!("Invalid UTF-8: {}", e)))?;

            if i + 1 < fields.len() {
                if let Frame::BulkString(Some(value_data)) = &fields[i + 1] {
                    let value = String::from_utf8(value_data.as_ref().to_vec())
                        .map_err(|e| RedisError::Protocol(format!("Invalid UTF-8: {}", e)))?;

                    match key.as_str() {
                        "ip" => host = Some(value),
                        "port" => {
                            port = Some(value.parse::<u16>().map_err(|e| {
                                RedisError::Protocol(format!("Invalid port: {}", e))
                            })?);
                        }
                        "flags" => flags = Some(value),
                        _ => {}
                    }
                }
            }
            i += 2;
        } else {
            i += 1;
        }
    }

    Ok(ReplicaInfo {
        host: host.ok_or_else(|| RedisError::Protocol("Missing 'ip' field".to_string()))?,
        port: port.ok_or_else(|| RedisError::Protocol("Missing 'port' field".to_string()))?,
        flags: flags.ok_or_else(|| RedisError::Protocol("Missing 'flags' field".to_string()))?,
    })
}

/// SENTINEL sentinels command
///
/// Returns information about other Sentinels monitoring the same master.
#[derive(Debug, Clone)]
pub struct SentinelSentinels {
    master_name: String,
}

impl SentinelSentinels {
    /// Create a new SENTINEL sentinels command
    pub fn new(master_name: impl Into<String>) -> Self {
        Self {
            master_name: master_name.into(),
        }
    }
}

/// Information about a Sentinel node
#[derive(Debug, Clone, PartialEq)]
pub struct SentinelInfo {
    /// Sentinel host address
    pub host: String,
    /// Sentinel port
    pub port: u16,
}

impl Command for SentinelSentinels {
    type Response = Vec<SentinelInfo>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SENTINEL"))),
            Frame::BulkString(Some(Bytes::from("sentinels"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.master_name.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(sentinels) => {
                let mut result = Vec::new();

                for sentinel_frame in sentinels {
                    if let Frame::Array(fields) = sentinel_frame {
                        let sentinel = parse_sentinel_fields(fields)?;
                        result.push(sentinel);
                    }
                }

                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Expected array response".to_string())),
        }
    }
}

fn parse_sentinel_fields(fields: Vec<Frame>) -> Result<SentinelInfo, RedisError> {
    let mut host = None;
    let mut port = None;

    let mut i = 0;
    while i < fields.len() {
        if let Frame::BulkString(Some(key_data)) = &fields[i] {
            let key = String::from_utf8(key_data.as_ref().to_vec())
                .map_err(|e| RedisError::Protocol(format!("Invalid UTF-8: {}", e)))?;

            if i + 1 < fields.len() {
                if let Frame::BulkString(Some(value_data)) = &fields[i + 1] {
                    let value = String::from_utf8(value_data.as_ref().to_vec())
                        .map_err(|e| RedisError::Protocol(format!("Invalid UTF-8: {}", e)))?;

                    match key.as_str() {
                        "ip" => host = Some(value),
                        "port" => {
                            port = Some(value.parse::<u16>().map_err(|e| {
                                RedisError::Protocol(format!("Invalid port: {}", e))
                            })?);
                        }
                        _ => {}
                    }
                }
            }
            i += 2;
        } else {
            i += 1;
        }
    }

    Ok(SentinelInfo {
        host: host.ok_or_else(|| RedisError::Protocol("Missing 'ip' field".to_string()))?,
        port: port.ok_or_else(|| RedisError::Protocol("Missing 'port' field".to_string()))?,
    })
}

/// ROLE command
///
/// Returns the role of the Redis instance (master, replica, or sentinel).
#[derive(Debug, Clone, Copy)]
pub struct Role;

impl Role {
    /// Create a new ROLE command
    pub fn new() -> Self {
        Self
    }
}

impl Default for Role {
    fn default() -> Self {
        Self::new()
    }
}

/// The role of a Redis instance
#[derive(Debug, Clone, PartialEq)]
pub enum RoleInfo {
    /// Master role
    Master {
        /// Replication offset
        replication_offset: i64,
    },
    /// Replica role
    Replica {
        /// Master host address
        master_host: String,
        /// Master port
        master_port: u16,
        /// Replication state (e.g., "connected", "disconnected")
        replication_state: String,
        /// Replication offset
        replication_offset: i64,
    },
    /// Sentinel role
    Sentinel {
        /// List of master names this Sentinel is monitoring
        master_names: Vec<String>,
    },
}

impl Command for Role {
    type Response = RoleInfo;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("ROLE")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(mut elements) if !elements.is_empty() => {
                let role_frame = elements.remove(0);

                if let Frame::BulkString(Some(role_data)) = role_frame {
                    let role = String::from_utf8(role_data.as_ref().to_vec())
                        .map_err(|e| RedisError::Protocol(format!("Invalid UTF-8: {}", e)))?;

                    match role.as_str() {
                        "master" => {
                            if elements.len() >= 2 {
                                let offset = extract_integer(&elements[0])?;
                                Ok(RoleInfo::Master {
                                    replication_offset: offset,
                                })
                            } else {
                                Err(RedisError::Protocol(
                                    "Invalid master ROLE response".to_string(),
                                ))
                            }
                        }
                        "slave" => {
                            if elements.len() >= 4 {
                                let master_host = extract_string(&elements[0])?;
                                let master_port = extract_integer(&elements[1])? as u16;
                                let replication_state = extract_string(&elements[2])?;
                                let replication_offset = extract_integer(&elements[3])?;

                                Ok(RoleInfo::Replica {
                                    master_host,
                                    master_port,
                                    replication_state,
                                    replication_offset,
                                })
                            } else {
                                Err(RedisError::Protocol(
                                    "Invalid replica ROLE response".to_string(),
                                ))
                            }
                        }
                        "sentinel" => {
                            let master_names =
                                if let Some(Frame::Array(names_frames)) = elements.first() {
                                    names_frames
                                        .iter()
                                        .filter_map(|f| {
                                            if let Frame::BulkString(Some(data)) = f {
                                                String::from_utf8(data.as_ref().to_vec()).ok()
                                            } else {
                                                None
                                            }
                                        })
                                        .collect()
                                } else {
                                    Vec::new()
                                };

                            Ok(RoleInfo::Sentinel { master_names })
                        }
                        _ => Err(RedisError::Protocol(format!("Unknown role: {}", role))),
                    }
                } else {
                    Err(RedisError::Protocol(
                        "Expected bulk string for role".to_string(),
                    ))
                }
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol(
                "Invalid ROLE response format".to_string(),
            )),
        }
    }
}

fn extract_string(frame: &Frame) -> Result<String, RedisError> {
    match frame {
        Frame::BulkString(Some(data)) => String::from_utf8(data.as_ref().to_vec())
            .map_err(|e| RedisError::Protocol(format!("Invalid UTF-8: {}", e))),
        _ => Err(RedisError::Protocol("Expected bulk string".to_string())),
    }
}

fn extract_integer(frame: &Frame) -> Result<i64, RedisError> {
    match frame {
        Frame::Integer(n) => Ok(*n),
        Frame::BulkString(Some(data)) => {
            let s = String::from_utf8(data.as_ref().to_vec())
                .map_err(|e| RedisError::Protocol(format!("Invalid UTF-8: {}", e)))?;
            s.parse::<i64>()
                .map_err(|e| RedisError::Protocol(format!("Invalid integer: {}", e)))
        }
        _ => Err(RedisError::Protocol("Expected integer".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sentinel_get_master_addr_by_name_frame() {
        let cmd = SentinelGetMasterAddrByName::new("mymaster");
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 3);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_role_command_frame() {
        let cmd = Role::new();
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 1);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_parse_master_role() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("master"))),
            Frame::Integer(1000),
            Frame::Array(vec![]),
        ]);

        let role = Role::parse_response(frame).unwrap();

        assert!(matches!(
            role,
            RoleInfo::Master {
                replication_offset: 1000
            }
        ));
    }

    #[test]
    fn test_parse_replica_role() {
        let frame = Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("slave"))),
            Frame::BulkString(Some(Bytes::from("127.0.0.1"))),
            Frame::Integer(6379),
            Frame::BulkString(Some(Bytes::from("connected"))),
            Frame::Integer(500),
        ]);

        let role = Role::parse_response(frame).unwrap();

        match role {
            RoleInfo::Replica {
                master_host,
                master_port,
                ..
            } => {
                assert_eq!(master_host, "127.0.0.1");
                assert_eq!(master_port, 6379);
            }
            _ => panic!("Expected replica role"),
        }
    }
}
