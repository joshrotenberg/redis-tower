//! Redis Cluster-specific commands

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// CLUSTER SLOTS command - get cluster slot configuration
///
/// Returns information about which nodes serve which slot ranges.
///
/// # Response Format
///
/// Returns an array of slot ranges, each containing:
/// - Start slot
/// - End slot
/// - Master node (IP, port, node ID)
/// - Replica nodes (IP, port, node ID)
///
/// # Example
/// ```no_run
/// use redis_tower::cluster::commands::ClusterSlots;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = redis_tower::client::RedisConnection::connect("127.0.0.1:7000").await?;
/// let slots = client.execute(ClusterSlots).await?;
/// for range in slots {
///     println!("Slots {}-{}: master at {}:{}",
///         range.start_slot, range.end_slot,
///         range.master.host, range.master.port);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClusterSlots;

/// A slot range assignment in the cluster
#[derive(Debug, Clone)]
pub struct SlotRange {
    /// First slot in the range (inclusive)
    pub start_slot: u16,
    /// Last slot in the range (inclusive)
    pub end_slot: u16,
    /// Master node serving this range
    pub master: NodeInfo,
    /// Replica nodes (may be empty)
    pub replicas: Vec<NodeInfo>,
}

/// Information about a cluster node
#[derive(Debug, Clone)]
pub struct NodeInfo {
    /// IP address or hostname
    pub host: String,
    /// Port number
    pub port: u16,
    /// Node ID (40-character hex string)
    pub node_id: Option<String>,
}

impl Command for ClusterSlots {
    type Response = Vec<SlotRange>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("SLOTS"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(ranges) => {
                let mut slot_ranges = Vec::new();

                for range_frame in ranges {
                    if let Frame::Array(mut range_parts) = range_frame {
                        if range_parts.len() < 3 {
                            return Err(RedisError::Protocol(
                                "Invalid CLUSTER SLOTS response".to_string(),
                            ));
                        }

                        // Parse start slot
                        let start_slot = match range_parts.remove(0) {
                            Frame::Integer(n) => n as u16,
                            _ => return Err(RedisError::UnexpectedResponse),
                        };

                        // Parse end slot
                        let end_slot = match range_parts.remove(0) {
                            Frame::Integer(n) => n as u16,
                            _ => return Err(RedisError::UnexpectedResponse),
                        };

                        // Parse master node
                        let master = Self::parse_node_info(&range_parts.remove(0))?;

                        // Parse replica nodes
                        let mut replicas = Vec::new();
                        for node_frame in range_parts {
                            replicas.push(Self::parse_node_info(&node_frame)?);
                        }

                        slot_ranges.push(SlotRange {
                            start_slot,
                            end_slot,
                            master,
                            replicas,
                        });
                    } else {
                        return Err(RedisError::UnexpectedResponse);
                    }
                }

                Ok(slot_ranges)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ClusterSlots {
    fn parse_node_info(frame: &Frame) -> Result<NodeInfo, RedisError> {
        match frame {
            Frame::Array(parts) => {
                if parts.len() < 2 {
                    return Err(RedisError::Protocol("Invalid node info".to_string()));
                }

                let host = match &parts[0] {
                    Frame::BulkString(Some(bytes)) => String::from_utf8_lossy(bytes).to_string(),
                    _ => return Err(RedisError::UnexpectedResponse),
                };

                let port = match &parts[1] {
                    Frame::Integer(n) => *n as u16,
                    _ => return Err(RedisError::UnexpectedResponse),
                };

                let node_id = if parts.len() > 2 {
                    match &parts[2] {
                        Frame::BulkString(Some(bytes)) => {
                            Some(String::from_utf8_lossy(bytes).to_string())
                        }
                        _ => None,
                    }
                } else {
                    None
                };

                Ok(NodeInfo {
                    host,
                    port,
                    node_id,
                })
            }
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ASKING command - used before ASK redirect retry
///
/// When a client receives an ASK redirect, it should send ASKING to the
/// target node before retrying the command.
///
/// # Example
/// ```no_run
/// use redis_tower::cluster::commands::Asking;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = redis_tower::client::RedisConnection::connect("127.0.0.1:7000").await?;
/// // After receiving ASK redirect to another node:
/// client.execute(Asking).await?;
/// // Now retry the original command
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Asking;

impl Command for Asking {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("ASKING")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER NODES command - get cluster topology
///
/// Returns information about all nodes in the cluster, including their roles,
/// status, and slot assignments.
///
/// # Example
/// ```no_run
/// use redis_tower::cluster::commands::ClusterNodes;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = redis_tower::client::RedisConnection::connect("127.0.0.1:7000").await?;
/// let nodes = client.execute(ClusterNodes).await?;
/// println!("Cluster has {} nodes", nodes.lines().count());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClusterNodes;

impl Command for ClusterNodes {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("NODES"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(bytes)) => Ok(String::from_utf8_lossy(&bytes).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER INFO command - get cluster state information
///
/// Returns overall cluster state including:
/// - cluster_state (ok/fail)
/// - cluster_slots_assigned
/// - cluster_slots_ok
/// - cluster_known_nodes
/// - etc.
#[derive(Debug, Clone, Copy)]
pub struct ClusterInfo;

impl Command for ClusterInfo {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("INFO"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(bytes)) => Ok(String::from_utf8_lossy(&bytes).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_slots_frame() {
        let cmd = ClusterSlots;
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2); // CLUSTER, SLOTS
            }
            _ => panic!("Expected array frame"),
        }
    }

    #[test]
    fn test_asking_frame() {
        let cmd = Asking;
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1); // ASKING
            }
            _ => panic!("Expected array frame"),
        }
    }

    #[test]
    fn test_cluster_nodes_frame() {
        let cmd = ClusterNodes;
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2); // CLUSTER, NODES
            }
            _ => panic!("Expected array frame"),
        }
    }
}
