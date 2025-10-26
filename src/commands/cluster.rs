//! Redis Cluster management commands
//!
//! Commands for managing and monitoring Redis Cluster nodes, slots, and failover.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// Information about a cluster node
#[derive(Debug, Clone)]
pub struct ClusterNodeInfo {
    /// Node ID
    pub id: String,
    /// Node address (host:port)
    pub address: String,
    /// Node flags (master, slave, myself, fail, etc.)
    pub flags: Vec<String>,
    /// Master ID if this is a replica
    pub master_id: Option<String>,
    /// Ping sent timestamp
    pub ping_sent: i64,
    /// Pong received timestamp
    pub pong_recv: i64,
    /// Configuration epoch
    pub config_epoch: i64,
    /// Link state (connected/disconnected)
    pub link_state: String,
    /// Assigned slot ranges
    pub slots: Vec<(u16, u16)>,
}

/// Information about a cluster slot range
#[derive(Debug, Clone)]
pub struct ClusterSlotInfo {
    /// Start slot
    pub start: u16,
    /// End slot
    pub end: u16,
    /// Master node (host, port, id)
    pub master: (String, u16, String),
    /// Replica nodes
    pub replicas: Vec<(String, u16, String)>,
}

/// Information about a cluster shard
#[derive(Debug, Clone)]
pub struct ClusterShardInfo {
    /// Shard slots
    pub slots: Vec<(u16, u16)>,
    /// Nodes in this shard
    pub nodes: Vec<ClusterShardNode>,
}

/// Information about a node in a shard
#[derive(Debug, Clone)]
pub struct ClusterShardNode {
    /// Node ID
    pub id: String,
    /// Node endpoint (host, port)
    pub endpoint: (String, u16),
    /// Node role (master/replica)
    pub role: String,
    /// Replication offset
    pub replication_offset: i64,
    /// Health status
    pub health: String,
}

/// CLUSTER INFO - Get cluster information
///
/// Returns information about the cluster node state including assigned slots,
/// cluster state, size, and known nodes.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterInfo;
///
/// let cmd = ClusterInfo::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClusterInfo;

impl ClusterInfo {
    /// Create a new CLUSTER INFO command
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClusterInfo {
    fn default() -> Self {
        Self::new()
    }
}

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
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER NODES - Get cluster nodes configuration
///
/// Returns the cluster configuration for the node in a serialized format.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterNodes;
///
/// let cmd = ClusterNodes::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClusterNodes;

impl ClusterNodes {
    /// Create a new CLUSTER NODES command
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClusterNodes {
    fn default() -> Self {
        Self::new()
    }
}

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
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER SLOTS - Get cluster slots mapping
///
/// Returns the mapping of cluster slots to nodes.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterSlots;
///
/// let cmd = ClusterSlots::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClusterSlots;

impl ClusterSlots {
    /// Create a new CLUSTER SLOTS command
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClusterSlots {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClusterSlots {
    type Response = String; // Complex nested array, simplified

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("SLOTS"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(format!("{:?}", frame))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_info_frame() {
        let cmd = ClusterInfo::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CLUSTER"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("INFO"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_nodes_frame() {
        let cmd = ClusterNodes::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("NODES"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_slots_frame() {
        let cmd = ClusterSlots::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("SLOTS"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_myid_frame() {
        let cmd = ClusterMyId::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("MYID"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_addslots_frame() {
        let cmd = ClusterAddSlots::new(vec![1, 2, 100]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("ADDSLOTS"))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("1")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("100")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_addslotsrange_frame() {
        let cmd = ClusterAddSlotsRange::new(vec![(0, 5000), (10000, 15000)]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[1],
                    Frame::BulkString(Some(Bytes::from("ADDSLOTSRANGE")))
                );
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("0")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("5000")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_delslots_frame() {
        let cmd = ClusterDelSlots::new(vec![1, 2, 3]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("DELSLOTS"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_keyslot_frame() {
        let cmd = ClusterKeySlot::new("mykey");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("KEYSLOT"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("mykey"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_keyslot_response() {
        let frame = Frame::Integer(7000);
        let slot = ClusterKeySlot::parse_response(frame).unwrap();
        assert_eq!(slot, 7000);
    }

    #[test]
    fn test_cluster_countkeysinslot_frame() {
        let cmd = ClusterCountKeysInSlot::new(7000);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[1],
                    Frame::BulkString(Some(Bytes::from("COUNTKEYSINSLOT")))
                );
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("7000"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_getkeysinslot_frame() {
        let cmd = ClusterGetKeysInSlot::new(7000, 10);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(
                    parts[1],
                    Frame::BulkString(Some(Bytes::from("GETKEYSINSLOT")))
                );
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("7000"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("10"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_setslot_migrating_frame() {
        let cmd = ClusterSetSlot::new(100, ClusterSetSlotState::Migrating("node-id".to_string()));
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("SETSLOT"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("100"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("MIGRATING"))));
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("node-id"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_setslot_stable_frame() {
        let cmd = ClusterSetSlot::new(100, ClusterSetSlotState::Stable);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("STABLE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_meet_frame() {
        let cmd = ClusterMeet::new("127.0.0.1", 7000);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("MEET"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("127.0.0.1"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("7000"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_meet_with_bus_port_frame() {
        let cmd = ClusterMeet::new("127.0.0.1", 7000).cluster_bus_port(17000);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("17000"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_forget_frame() {
        let cmd = ClusterForget::new("node-id");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("FORGET"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("node-id"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_replicate_frame() {
        let cmd = ClusterReplicate::new("master-id");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("REPLICATE"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("master-id"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_failover_frame() {
        let cmd = ClusterFailover::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("FAILOVER"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_failover_force_frame() {
        let cmd = ClusterFailover::with_option(ClusterFailoverOption::Force);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("FORCE"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_reset_frame() {
        let cmd = ClusterReset::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("RESET"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_reset_hard_frame() {
        let cmd = ClusterReset::hard();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("HARD"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_saveconfig_frame() {
        let cmd = ClusterSaveConfig::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("SAVECONFIG"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_set_config_epoch_frame() {
        let cmd = ClusterSetConfigEpoch::new(100);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[1],
                    Frame::BulkString(Some(Bytes::from("SET-CONFIG-EPOCH")))
                );
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("100"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_bumpepoch_frame() {
        let cmd = ClusterBumpEpoch::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("BUMPEPOCH"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_cluster_count_failure_reports_frame() {
        let cmd = ClusterCountFailureReports::new("node-id");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[1],
                    Frame::BulkString(Some(Bytes::from("COUNT-FAILURE-REPORTS")))
                );
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("node-id"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }
}

/// CLUSTER SHARDS - Get cluster shards information (Redis 7.0+)
///
/// Returns information about the cluster shards.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterShards;
///
/// let cmd = ClusterShards::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClusterShards;

impl ClusterShards {
    /// Create a new CLUSTER SHARDS command
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClusterShards {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClusterShards {
    type Response = String; // Complex nested array

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("SHARDS"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(format!("{:?}", frame))
    }
}

/// CLUSTER MYID - Get the node ID
///
/// Returns the unique node ID.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterMyId;
///
/// let cmd = ClusterMyId::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClusterMyId;

impl ClusterMyId {
    /// Create a new CLUSTER MYID command
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClusterMyId {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClusterMyId {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("MYID"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER MYSHARDID - Get the shard ID (Redis 7.2+)
///
/// Returns the unique shard ID of the current node.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterMyShardId;
///
/// let cmd = ClusterMyShardId::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClusterMyShardId;

impl ClusterMyShardId {
    /// Create a new CLUSTER MYSHARDID command
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClusterMyShardId {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClusterMyShardId {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("MYSHARDID"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER ADDSLOTS - Assign slots to the node
///
/// Assigns hash slots to the receiving node.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterAddSlots;
///
/// let cmd = ClusterAddSlots::new(vec![1, 2, 3, 100]);
/// ```
#[derive(Debug, Clone)]
pub struct ClusterAddSlots {
    slots: Vec<u16>,
}

impl ClusterAddSlots {
    /// Create a new CLUSTER ADDSLOTS command
    pub fn new(slots: Vec<u16>) -> Self {
        Self { slots }
    }
}

impl Command for ClusterAddSlots {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("ADDSLOTS"))),
        ];

        for slot in &self.slots {
            frames.push(Frame::BulkString(Some(Bytes::from(slot.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER ADDSLOTSRANGE - Assign slot ranges to the node (Redis 7.0+)
///
/// Assigns hash slot ranges to the receiving node.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterAddSlotsRange;
///
/// let cmd = ClusterAddSlotsRange::new(vec![(0, 5460), (10923, 16383)]);
/// ```
#[derive(Debug, Clone)]
pub struct ClusterAddSlotsRange {
    ranges: Vec<(u16, u16)>,
}

impl ClusterAddSlotsRange {
    /// Create a new CLUSTER ADDSLOTSRANGE command
    pub fn new(ranges: Vec<(u16, u16)>) -> Self {
        Self { ranges }
    }
}

impl Command for ClusterAddSlotsRange {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("ADDSLOTSRANGE"))),
        ];

        for (start, end) in &self.ranges {
            frames.push(Frame::BulkString(Some(Bytes::from(start.to_string()))));
            frames.push(Frame::BulkString(Some(Bytes::from(end.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER DELSLOTS - Remove slots from the node
///
/// Removes hash slots from the receiving node.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterDelSlots;
///
/// let cmd = ClusterDelSlots::new(vec![1, 2, 3]);
/// ```
#[derive(Debug, Clone)]
pub struct ClusterDelSlots {
    slots: Vec<u16>,
}

impl ClusterDelSlots {
    /// Create a new CLUSTER DELSLOTS command
    pub fn new(slots: Vec<u16>) -> Self {
        Self { slots }
    }
}

impl Command for ClusterDelSlots {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("DELSLOTS"))),
        ];

        for slot in &self.slots {
            frames.push(Frame::BulkString(Some(Bytes::from(slot.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER DELSLOTSRANGE - Remove slot ranges from the node (Redis 7.0+)
///
/// Removes hash slot ranges from the receiving node.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterDelSlotsRange;
///
/// let cmd = ClusterDelSlotsRange::new(vec![(0, 100), (200, 300)]);
/// ```
#[derive(Debug, Clone)]
pub struct ClusterDelSlotsRange {
    ranges: Vec<(u16, u16)>,
}

impl ClusterDelSlotsRange {
    /// Create a new CLUSTER DELSLOTSRANGE command
    pub fn new(ranges: Vec<(u16, u16)>) -> Self {
        Self { ranges }
    }
}

impl Command for ClusterDelSlotsRange {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("DELSLOTSRANGE"))),
        ];

        for (start, end) in &self.ranges {
            frames.push(Frame::BulkString(Some(Bytes::from(start.to_string()))));
            frames.push(Frame::BulkString(Some(Bytes::from(end.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER FLUSHSLOTS - Delete all slots information
///
/// Deletes all slots information from the node.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterFlushSlots;
///
/// let cmd = ClusterFlushSlots::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClusterFlushSlots;

impl ClusterFlushSlots {
    /// Create a new CLUSTER FLUSHSLOTS command
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClusterFlushSlots {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClusterFlushSlots {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("FLUSHSLOTS"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER KEYSLOT - Get the hash slot of a key
///
/// Returns the hash slot for the specified key.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterKeySlot;
///
/// let cmd = ClusterKeySlot::new("mykey");
/// ```
#[derive(Debug, Clone)]
pub struct ClusterKeySlot {
    key: String,
}

impl ClusterKeySlot {
    /// Create a new CLUSTER KEYSLOT command
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for ClusterKeySlot {
    type Response = u16;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("KEYSLOT"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n as u16),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER COUNTKEYSINSLOT - Get count of keys in a slot
///
/// Returns the number of keys in the specified hash slot.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterCountKeysInSlot;
///
/// let cmd = ClusterCountKeysInSlot::new(7000);
/// ```
#[derive(Debug, Clone)]
pub struct ClusterCountKeysInSlot {
    slot: u16,
}

impl ClusterCountKeysInSlot {
    /// Create a new CLUSTER COUNTKEYSINSLOT command
    pub fn new(slot: u16) -> Self {
        Self { slot }
    }
}

impl Command for ClusterCountKeysInSlot {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("COUNTKEYSINSLOT"))),
            Frame::BulkString(Some(Bytes::from(self.slot.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER GETKEYSINSLOT - Get keys in a slot
///
/// Returns keys in the specified hash slot.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterGetKeysInSlot;
///
/// let cmd = ClusterGetKeysInSlot::new(7000, 10);
/// ```
#[derive(Debug, Clone)]
pub struct ClusterGetKeysInSlot {
    slot: u16,
    count: i64,
}

impl ClusterGetKeysInSlot {
    /// Create a new CLUSTER GETKEYSINSLOT command
    pub fn new(slot: u16, count: i64) -> Self {
        Self { slot, count }
    }
}

impl Command for ClusterGetKeysInSlot {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("GETKEYSINSLOT"))),
            Frame::BulkString(Some(Bytes::from(self.slot.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.count.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut keys = Vec::new();
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            keys.push(String::from_utf8_lossy(&data).into_owned());
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(keys)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER SETSLOT - Set slot state
///
/// Binds a hash slot to a specific node.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::{ClusterSetSlot, ClusterSetSlotState};
///
/// // Migrate slot to a node
/// let cmd = ClusterSetSlot::new(100, ClusterSetSlotState::Migrating("node-id".to_string()));
///
/// // Import slot from a node
/// let cmd = ClusterSetSlot::new(100, ClusterSetSlotState::Importing("node-id".to_string()));
///
/// // Assign slot to a node
/// let cmd = ClusterSetSlot::new(100, ClusterSetSlotState::Node("node-id".to_string()));
///
/// // Clear slot state
/// let cmd = ClusterSetSlot::new(100, ClusterSetSlotState::Stable);
/// ```
#[derive(Debug, Clone)]
pub struct ClusterSetSlot {
    slot: u16,
    state: ClusterSetSlotState,
}

/// Slot state for CLUSTER SETSLOT
#[derive(Debug, Clone)]
pub enum ClusterSetSlotState {
    /// MIGRATING - Set slot as migrating to node
    Migrating(String),
    /// IMPORTING - Set slot as importing from node
    Importing(String),
    /// NODE - Assign slot to node
    Node(String),
    /// STABLE - Clear slot migration state
    Stable,
}

impl ClusterSetSlot {
    /// Create a new CLUSTER SETSLOT command
    pub fn new(slot: u16, state: ClusterSetSlotState) -> Self {
        Self { slot, state }
    }
}

impl Command for ClusterSetSlot {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("SETSLOT"))),
            Frame::BulkString(Some(Bytes::from(self.slot.to_string()))),
        ];

        match &self.state {
            ClusterSetSlotState::Migrating(node_id) => {
                frames.push(Frame::BulkString(Some(Bytes::from("MIGRATING"))));
                frames.push(Frame::BulkString(Some(Bytes::from(node_id.clone()))));
            }
            ClusterSetSlotState::Importing(node_id) => {
                frames.push(Frame::BulkString(Some(Bytes::from("IMPORTING"))));
                frames.push(Frame::BulkString(Some(Bytes::from(node_id.clone()))));
            }
            ClusterSetSlotState::Node(node_id) => {
                frames.push(Frame::BulkString(Some(Bytes::from("NODE"))));
                frames.push(Frame::BulkString(Some(Bytes::from(node_id.clone()))));
            }
            ClusterSetSlotState::Stable => {
                frames.push(Frame::BulkString(Some(Bytes::from("STABLE"))));
            }
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER MEET - Add a node to the cluster
///
/// Forces a node to handshake with another node.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterMeet;
///
/// // Meet node at IP and port
/// let cmd = ClusterMeet::new("127.0.0.1", 7000);
///
/// // Meet node with cluster bus port
/// let cmd = ClusterMeet::new("127.0.0.1", 7000).cluster_bus_port(17000);
/// ```
#[derive(Debug, Clone)]
pub struct ClusterMeet {
    ip: String,
    port: u16,
    cluster_bus_port: Option<u16>,
}

impl ClusterMeet {
    /// Create a new CLUSTER MEET command
    pub fn new(ip: impl Into<String>, port: u16) -> Self {
        Self {
            ip: ip.into(),
            port,
            cluster_bus_port: None,
        }
    }

    /// Set the cluster bus port (Redis 4.0+)
    pub fn cluster_bus_port(mut self, port: u16) -> Self {
        self.cluster_bus_port = Some(port);
        self
    }
}

impl Command for ClusterMeet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("MEET"))),
            Frame::BulkString(Some(Bytes::from(self.ip.clone()))),
            Frame::BulkString(Some(Bytes::from(self.port.to_string()))),
        ];

        if let Some(bus_port) = self.cluster_bus_port {
            frames.push(Frame::BulkString(Some(Bytes::from(bus_port.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER FORGET - Remove a node from the cluster
///
/// Removes a node from the nodes table.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterForget;
///
/// let cmd = ClusterForget::new("node-id-to-forget");
/// ```
#[derive(Debug, Clone)]
pub struct ClusterForget {
    node_id: String,
}

impl ClusterForget {
    /// Create a new CLUSTER FORGET command
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }
}

impl Command for ClusterForget {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("FORGET"))),
            Frame::BulkString(Some(Bytes::from(self.node_id.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER REPLICATE - Make node a replica
///
/// Reconfigures a node as a replica of the specified master node.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterReplicate;
///
/// let cmd = ClusterReplicate::new("master-node-id");
/// ```
#[derive(Debug, Clone)]
pub struct ClusterReplicate {
    node_id: String,
}

impl ClusterReplicate {
    /// Create a new CLUSTER REPLICATE command
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }
}

impl Command for ClusterReplicate {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("REPLICATE"))),
            Frame::BulkString(Some(Bytes::from(self.node_id.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER REPLICAS - Get replicas of a node
///
/// Returns the list of replica nodes for the specified master.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterReplicas;
///
/// let cmd = ClusterReplicas::new("master-node-id");
/// ```
#[derive(Debug, Clone)]
pub struct ClusterReplicas {
    node_id: String,
}

impl ClusterReplicas {
    /// Create a new CLUSTER REPLICAS command
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }
}

impl Command for ClusterReplicas {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("REPLICAS"))),
            Frame::BulkString(Some(Bytes::from(self.node_id.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(format!("{:?}", frame))
    }
}

/// CLUSTER FAILOVER - Force a replica to perform a failover
///
/// Forces a replica to perform a manual failover of its master.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::{ClusterFailover, ClusterFailoverOption};
///
/// // Normal failover (wait for offset sync)
/// let cmd = ClusterFailover::new();
///
/// // Force failover without waiting
/// let cmd = ClusterFailover::with_option(ClusterFailoverOption::Force);
///
/// // Takeover without consensus
/// let cmd = ClusterFailover::with_option(ClusterFailoverOption::Takeover);
/// ```
#[derive(Debug, Clone)]
pub struct ClusterFailover {
    option: Option<ClusterFailoverOption>,
}

/// Failover options for CLUSTER FAILOVER
#[derive(Debug, Clone, Copy)]
pub enum ClusterFailoverOption {
    /// Force failover without waiting for offset sync
    Force,
    /// Takeover without master consensus
    Takeover,
}

impl ClusterFailover {
    /// Create a new CLUSTER FAILOVER command (normal failover)
    pub fn new() -> Self {
        Self { option: None }
    }

    /// Create a CLUSTER FAILOVER command with an option
    pub fn with_option(option: ClusterFailoverOption) -> Self {
        Self {
            option: Some(option),
        }
    }
}

impl Default for ClusterFailover {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClusterFailover {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("FAILOVER"))),
        ];

        if let Some(option) = self.option {
            let option_str = match option {
                ClusterFailoverOption::Force => "FORCE",
                ClusterFailoverOption::Takeover => "TAKEOVER",
            };
            frames.push(Frame::BulkString(Some(Bytes::from(option_str))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER RESET - Reset cluster node
///
/// Resets a cluster node, clearing its cluster configuration.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterReset;
///
/// // Soft reset (default)
/// let cmd = ClusterReset::new();
///
/// // Hard reset
/// let cmd = ClusterReset::hard();
/// ```
#[derive(Debug, Clone)]
pub struct ClusterReset {
    hard: bool,
}

impl ClusterReset {
    /// Create a new CLUSTER RESET command (soft reset)
    pub fn new() -> Self {
        Self { hard: false }
    }

    /// Perform a hard reset
    pub fn hard() -> Self {
        Self { hard: true }
    }
}

impl Default for ClusterReset {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClusterReset {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("RESET"))),
        ];

        if self.hard {
            frames.push(Frame::BulkString(Some(Bytes::from("HARD"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER SAVECONFIG - Save cluster configuration
///
/// Forces the node to save cluster state to disk.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterSaveConfig;
///
/// let cmd = ClusterSaveConfig::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClusterSaveConfig;

impl ClusterSaveConfig {
    /// Create a new CLUSTER SAVECONFIG command
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClusterSaveConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClusterSaveConfig {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("SAVECONFIG"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER SET-CONFIG-EPOCH - Set config epoch
///
/// Sets the config epoch for the node.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterSetConfigEpoch;
///
/// let cmd = ClusterSetConfigEpoch::new(100);
/// ```
#[derive(Debug, Clone)]
pub struct ClusterSetConfigEpoch {
    epoch: u64,
}

impl ClusterSetConfigEpoch {
    /// Create a new CLUSTER SET-CONFIG-EPOCH command
    pub fn new(epoch: u64) -> Self {
        Self { epoch }
    }
}

impl Command for ClusterSetConfigEpoch {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("SET-CONFIG-EPOCH"))),
            Frame::BulkString(Some(Bytes::from(self.epoch.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER BUMPEPOCH - Advance config epoch
///
/// Advances the cluster config epoch.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterBumpEpoch;
///
/// let cmd = ClusterBumpEpoch::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClusterBumpEpoch;

impl ClusterBumpEpoch {
    /// Create a new CLUSTER BUMPEPOCH command
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClusterBumpEpoch {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClusterBumpEpoch {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("BUMPEPOCH"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) | Frame::SimpleString(data) => {
                Ok(String::from_utf8_lossy(&data).into_owned())
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER COUNT-FAILURE-REPORTS - Get failure report count
///
/// Returns the number of active failure reports for a node.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterCountFailureReports;
///
/// let cmd = ClusterCountFailureReports::new("node-id");
/// ```
#[derive(Debug, Clone)]
pub struct ClusterCountFailureReports {
    node_id: String,
}

impl ClusterCountFailureReports {
    /// Create a new CLUSTER COUNT-FAILURE-REPORTS command
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }
}

impl Command for ClusterCountFailureReports {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("COUNT-FAILURE-REPORTS"))),
            Frame::BulkString(Some(Bytes::from(self.node_id.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER LINKS - Get cluster link information (Redis 7.0+)
///
/// Returns information about the TCP links with other nodes.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterLinks;
///
/// let cmd = ClusterLinks::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClusterLinks;

impl ClusterLinks {
    /// Create a new CLUSTER LINKS command
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClusterLinks {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClusterLinks {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("LINKS"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(format!("{:?}", frame))
    }
}

/// CLUSTER SLOT-STATS - Get slot statistics (Redis 7.0+)
///
/// Returns statistics about slots.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClusterSlotStats;
///
/// // Get stats for specific slots
/// let cmd = ClusterSlotStats::new(vec![1, 2, 100]);
/// ```
#[derive(Debug, Clone)]
pub struct ClusterSlotStats {
    slots: Vec<u16>,
}

impl ClusterSlotStats {
    /// Create a new CLUSTER SLOT-STATS command for specific slots
    pub fn new(slots: Vec<u16>) -> Self {
        Self { slots }
    }
}

impl Command for ClusterSlotStats {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("SLOT-STATS"))),
            Frame::BulkString(Some(Bytes::from("SLOTSRANGE"))),
        ];

        for slot in &self.slots {
            frames.push(Frame::BulkString(Some(Bytes::from(slot.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(format!("{:?}", frame))
    }
}

/// CLUSTER HELP command - Get help text for CLUSTER subcommands
///
/// Available since Redis 3.0.0.
#[derive(Debug, Clone, Copy)]
pub struct ClusterHelp;

impl ClusterHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClusterHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::commands::Command for ClusterHelp {
    type Response = Vec<String>;

    fn to_frame(&self) -> crate::codec::Frame {
        crate::codec::Frame::Array(vec![
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from("CLUSTER"))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from("HELP"))),
        ])
    }

    fn parse_response(
        frame: crate::codec::Frame,
    ) -> Result<Self::Response, crate::types::RedisError> {
        match frame {
            crate::codec::Frame::Array(items) => Ok(items
                .into_iter()
                .filter_map(|item| match item {
                    crate::codec::Frame::BulkString(Some(data)) => {
                        Some(String::from_utf8_lossy(&data).to_string())
                    }
                    _ => None,
                })
                .collect()),
            crate::codec::Frame::Error(e) => Err(crate::types::RedisError::from_redis_error(
                &String::from_utf8_lossy(&e),
            )),
            _ => Err(crate::types::RedisError::UnexpectedResponse),
        }
    }
}

/// CLUSTER SLAVES command - List replicas of a node (deprecated, use CLUSTER REPLICAS)
///
/// This command is deprecated. Use CLUSTER REPLICAS instead.
///
/// Available since Redis 3.0.0. Deprecated in Redis 5.0.0.
#[derive(Debug, Clone)]
pub struct ClusterSlaves {
    node_id: String,
}

impl ClusterSlaves {
    /// Create a new CLUSTER SLAVES command
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }
}

impl Command for ClusterSlaves {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLUSTER"))),
            Frame::BulkString(Some(Bytes::from("SLAVES"))),
            Frame::BulkString(Some(Bytes::from(self.node_id.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).to_string()),
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Array(_) => Ok("OK".to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}
