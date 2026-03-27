//! Cluster topology discovery via CLUSTER SLOTS.

use redis_tower_core::{Frame, RedisConnection, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// A slot range owned by a node.
#[derive(Debug, Clone)]
pub struct SlotRange {
    /// Start slot (inclusive).
    pub start: u16,
    /// End slot (inclusive).
    pub end: u16,
    /// Master node address.
    pub master: NodeAddr,
    /// Replica node addresses.
    pub replicas: Vec<NodeAddr>,
}

/// Address of a cluster node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeAddr {
    pub host: String,
    pub port: u16,
}

impl NodeAddr {
    pub fn addr_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl std::fmt::Display for NodeAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// The full cluster topology: a list of slot ranges with their owners.
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    pub slot_ranges: Vec<SlotRange>,
}

impl ClusterTopology {
    /// Find the master node responsible for a given slot.
    pub fn master_for_slot(&self, slot: u16) -> Option<&NodeAddr> {
        self.slot_ranges
            .iter()
            .find(|r| slot >= r.start && slot <= r.end)
            .map(|r| &r.master)
    }

    /// Get all unique master addresses.
    pub fn master_addrs(&self) -> Vec<&NodeAddr> {
        let mut addrs: Vec<&NodeAddr> = self.slot_ranges.iter().map(|r| &r.master).collect();
        addrs.dedup_by(|a, b| a == b);
        addrs
    }
}

/// Discover the cluster topology by sending CLUSTER SLOTS to a node.
pub async fn discover_topology(conn: &RedisConnection) -> Result<ClusterTopology, RedisError> {
    let frame = array(vec![bulk("CLUSTER"), bulk("SLOTS")]);
    conn.execute_pipeline(vec![frame]).await.and_then(|frames| {
        if frames.len() != 1 {
            return Err(RedisError::UnexpectedResponse {
                expected: "single CLUSTER SLOTS response",
                actual: format!("{} frames", frames.len()),
            });
        }
        parse_cluster_slots(&frames[0])
    })
}

/// Parse the response from CLUSTER SLOTS into a `ClusterTopology`.
///
/// The response is an array of slot ranges, each of which is:
/// `[start_slot, end_slot, [master_host, master_port, ...], [replica_host, replica_port, ...], ...]`
fn parse_cluster_slots(frame: &Frame) -> Result<ClusterTopology, RedisError> {
    let ranges = match frame {
        Frame::Array(Some(items)) => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of slot ranges",
                actual: format!("{other:?}"),
            });
        }
    };

    let mut slot_ranges = Vec::with_capacity(ranges.len());

    for range_frame in ranges {
        let range_items = match range_frame {
            Frame::Array(Some(items)) if items.len() >= 3 => items,
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "slot range array with >= 3 elements",
                    actual: format!("{other:?}"),
                });
            }
        };

        let start = extract_integer(&range_items[0])? as u16;
        let end = extract_integer(&range_items[1])? as u16;
        let master = parse_node_addr(&range_items[2])?;

        let mut replicas = Vec::new();
        for node_frame in &range_items[3..] {
            if let Ok(addr) = parse_node_addr(node_frame) {
                replicas.push(addr);
            }
        }

        slot_ranges.push(SlotRange {
            start,
            end,
            master,
            replicas,
        });
    }

    Ok(ClusterTopology { slot_ranges })
}

/// Parse a node address from a CLUSTER SLOTS node entry.
///
/// Each node entry is: `[host, port, node_id]` (host is bulk string, port is integer).
fn parse_node_addr(frame: &Frame) -> Result<NodeAddr, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) if items.len() >= 2 => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "node array [host, port, ...]",
                actual: format!("{other:?}"),
            });
        }
    };

    let host = match &items[0] {
        Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "bulk string host",
                actual: format!("{other:?}"),
            });
        }
    };

    let port = extract_integer(&items[1])? as u16;

    Ok(NodeAddr { host, port })
}

fn extract_integer(frame: &Frame) -> Result<i64, RedisError> {
    match frame {
        Frame::Integer(n) => Ok(*n),
        other => Err(RedisError::UnexpectedResponse {
            expected: "integer",
            actual: format!("{other:?}"),
        }),
    }
}
