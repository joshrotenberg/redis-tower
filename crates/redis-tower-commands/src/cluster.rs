use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// CLUSTER INFO
///
/// Returns information and statistics about the cluster.
/// The response is a bulk string of key-value pairs separated by `\r\n`.
pub struct ClusterInfo;

impl ClusterInfo {
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
        array(vec![bulk("CLUSTER"), bulk("INFO")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLUSTER INFO"
    }
}

/// CLUSTER NODES
///
/// Returns the cluster configuration as seen by the current node,
/// in a format that can be used as a node configuration file.
pub struct ClusterNodes;

impl ClusterNodes {
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
        array(vec![bulk("CLUSTER"), bulk("NODES")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLUSTER NODES"
    }
}

/// CLUSTER SLOTS
///
/// Returns details about which cluster slots map to which nodes.
/// Deprecated in Redis 7.0 in favor of CLUSTER SHARDS.
///
/// Returns an array of slot ranges, each containing:
/// `[start_slot, end_slot, [ip, port, node_id], ...]`
pub struct ClusterSlots;

impl ClusterSlots {
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
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CLUSTER"), bulk("SLOTS")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(_) => Ok(frame),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLUSTER SLOTS"
    }
}

/// CLUSTER SHARDS
///
/// Returns information about the shards of the cluster (Redis 7.0+).
/// This is the replacement for the deprecated CLUSTER SLOTS command.
pub struct ClusterShards;

impl ClusterShards {
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
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CLUSTER"), bulk("SHARDS")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(_) | Frame::Map(_) => Ok(frame),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or map",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLUSTER SHARDS"
    }
}

/// CLUSTER MYID
///
/// Returns the node's ID as a 40-character hex string.
pub struct ClusterMyId;

impl ClusterMyId {
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
        array(vec![bulk("CLUSTER"), bulk("MYID")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            Frame::SimpleString(data) => Ok(String::from_utf8_lossy(&data).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string or simple string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLUSTER MYID"
    }
}

/// CLUSTER MEET ip port
///
/// Introduces a new node to the cluster by connecting to the specified
/// address. The node will join the cluster handshake.
pub struct ClusterMeet {
    ip: String,
    port: u16,
}

impl ClusterMeet {
    pub fn new(ip: impl Into<String>, port: u16) -> Self {
        Self {
            ip: ip.into(),
            port,
        }
    }
}

impl Command for ClusterMeet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLUSTER"),
            bulk("MEET"),
            bulk(self.ip.as_str()),
            bulk(self.port.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLUSTER MEET"
    }
}

/// CLUSTER FORGET node-id
///
/// Removes a node from the cluster's nodes table.
pub struct ClusterForget {
    node_id: String,
}

impl ClusterForget {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }
}

impl Command for ClusterForget {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLUSTER"),
            bulk("FORGET"),
            bulk(self.node_id.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLUSTER FORGET"
    }
}

/// CLUSTER REPLICATE node-id
///
/// Configures the current node as a replica of the specified master node.
pub struct ClusterReplicate {
    node_id: String,
}

impl ClusterReplicate {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }
}

impl Command for ClusterReplicate {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLUSTER"),
            bulk("REPLICATE"),
            bulk(self.node_id.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLUSTER REPLICATE"
    }
}

/// CLUSTER FAILOVER [FORCE|TAKEOVER]
///
/// Triggers a manual failover of the master the current replica is
/// replicating from.
pub struct ClusterFailover {
    option: Option<FailoverOption>,
}

/// Options for CLUSTER FAILOVER.
pub enum FailoverOption {
    /// Force failover without agreement from the master.
    Force,
    /// Take over without cluster consensus (use with caution).
    Takeover,
}

impl ClusterFailover {
    pub fn new() -> Self {
        Self { option: None }
    }

    pub fn force() -> Self {
        Self {
            option: Some(FailoverOption::Force),
        }
    }

    pub fn takeover() -> Self {
        Self {
            option: Some(FailoverOption::Takeover),
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
        let mut args = vec![bulk("CLUSTER"), bulk("FAILOVER")];
        match &self.option {
            Some(FailoverOption::Force) => args.push(bulk("FORCE")),
            Some(FailoverOption::Takeover) => args.push(bulk("TAKEOVER")),
            None => {}
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLUSTER FAILOVER"
    }
}

/// CLUSTER RESET [HARD|SOFT]
///
/// Resets the cluster state. SOFT (default) resets the cluster
/// configuration. HARD also generates a new node ID.
pub struct ClusterReset {
    hard: bool,
}

impl ClusterReset {
    pub fn soft() -> Self {
        Self { hard: false }
    }

    pub fn hard() -> Self {
        Self { hard: true }
    }
}

impl Default for ClusterReset {
    fn default() -> Self {
        Self::soft()
    }
}

impl Command for ClusterReset {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("CLUSTER"), bulk("RESET")];
        if self.hard {
            args.push(bulk("HARD"));
        } else {
            args.push(bulk("SOFT"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLUSTER RESET"
    }
}

/// CLUSTER COUNTKEYSINSLOT slot
///
/// Returns the number of keys in the specified hash slot.
pub struct ClusterCountKeysInSlot {
    slot: u16,
}

impl ClusterCountKeysInSlot {
    pub fn new(slot: u16) -> Self {
        Self { slot }
    }
}

impl Command for ClusterCountKeysInSlot {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLUSTER"),
            bulk("COUNTKEYSINSLOT"),
            bulk(self.slot.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLUSTER COUNTKEYSINSLOT"
    }
}

/// CLUSTER GETKEYSINSLOT slot count
///
/// Returns up to `count` key names in the specified hash slot.
pub struct ClusterGetKeysInSlot {
    slot: u16,
    count: u32,
}

impl ClusterGetKeysInSlot {
    pub fn new(slot: u16, count: u32) -> Self {
        Self { slot, count }
    }
}

impl Command for ClusterGetKeysInSlot {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLUSTER"),
            bulk("GETKEYSINSLOT"),
            bulk(self.slot.to_string()),
            bulk(self.count.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => {
                let mut keys = Vec::with_capacity(frames.len());
                for f in frames {
                    match f {
                        Frame::BulkString(Some(data)) => keys.push(data),
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "bulk string",
                                actual: format!("{other:?}"),
                            });
                        }
                    }
                }
                Ok(keys)
            }
            Frame::Array(None) => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLUSTER GETKEYSINSLOT"
    }
}

/// CLUSTER KEYSLOT key
///
/// Returns the hash slot number for the given key.
pub struct ClusterKeySlot {
    key: String,
}

impl ClusterKeySlot {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for ClusterKeySlot {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLUSTER"),
            bulk("KEYSLOT"),
            bulk(self.key.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLUSTER KEYSLOT"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_info_frame() {
        let cmd = ClusterInfo::new();
        let frame = cmd.to_frame();
        assert_eq!(frame, array(vec![bulk("CLUSTER"), bulk("INFO")]));
        assert_eq!(cmd.name(), "CLUSTER INFO");
    }

    #[test]
    fn cluster_info_parse_response() {
        let cmd = ClusterInfo::new();
        let frame = Frame::BulkString(Some(Bytes::from("cluster_enabled:0\r\n")));
        let resp = cmd.parse_response(frame).unwrap();
        assert!(resp.contains("cluster_enabled:0"));
    }

    #[test]
    fn cluster_nodes_frame() {
        let cmd = ClusterNodes::new();
        let frame = cmd.to_frame();
        assert_eq!(frame, array(vec![bulk("CLUSTER"), bulk("NODES")]));
        assert_eq!(cmd.name(), "CLUSTER NODES");
    }

    #[test]
    fn cluster_slots_frame() {
        let cmd = ClusterSlots::new();
        let frame = cmd.to_frame();
        assert_eq!(frame, array(vec![bulk("CLUSTER"), bulk("SLOTS")]));
        assert_eq!(cmd.name(), "CLUSTER SLOTS");
    }

    #[test]
    fn cluster_shards_frame() {
        let cmd = ClusterShards::new();
        let frame = cmd.to_frame();
        assert_eq!(frame, array(vec![bulk("CLUSTER"), bulk("SHARDS")]));
        assert_eq!(cmd.name(), "CLUSTER SHARDS");
    }

    #[test]
    fn cluster_myid_frame() {
        let cmd = ClusterMyId::new();
        let frame = cmd.to_frame();
        assert_eq!(frame, array(vec![bulk("CLUSTER"), bulk("MYID")]));
        assert_eq!(cmd.name(), "CLUSTER MYID");
    }

    #[test]
    fn cluster_myid_parse_response() {
        let cmd = ClusterMyId::new();
        let frame = Frame::BulkString(Some(Bytes::from(
            "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca",
        )));
        let resp = cmd.parse_response(frame).unwrap();
        assert_eq!(resp, "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca");
    }

    #[test]
    fn cluster_meet_frame() {
        let cmd = ClusterMeet::new("127.0.0.1", 7001);
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            array(vec![
                bulk("CLUSTER"),
                bulk("MEET"),
                bulk("127.0.0.1"),
                bulk("7001"),
            ])
        );
        assert_eq!(cmd.name(), "CLUSTER MEET");
    }

    #[test]
    fn cluster_meet_parse_ok() {
        let cmd = ClusterMeet::new("127.0.0.1", 7001);
        let frame = Frame::SimpleString(Bytes::from("OK"));
        cmd.parse_response(frame).unwrap();
    }

    #[test]
    fn cluster_forget_frame() {
        let cmd = ClusterForget::new("e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca");
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            array(vec![
                bulk("CLUSTER"),
                bulk("FORGET"),
                bulk("e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca"),
            ])
        );
        assert_eq!(cmd.name(), "CLUSTER FORGET");
    }

    #[test]
    fn cluster_replicate_frame() {
        let cmd = ClusterReplicate::new("e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca");
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            array(vec![
                bulk("CLUSTER"),
                bulk("REPLICATE"),
                bulk("e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca"),
            ])
        );
        assert_eq!(cmd.name(), "CLUSTER REPLICATE");
    }

    #[test]
    fn cluster_failover_frame() {
        let cmd = ClusterFailover::new();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CLUSTER"), bulk("FAILOVER")])
        );
        assert_eq!(cmd.name(), "CLUSTER FAILOVER");
    }

    #[test]
    fn cluster_failover_force_frame() {
        let cmd = ClusterFailover::force();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CLUSTER"), bulk("FAILOVER"), bulk("FORCE")])
        );
    }

    #[test]
    fn cluster_failover_takeover_frame() {
        let cmd = ClusterFailover::takeover();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CLUSTER"), bulk("FAILOVER"), bulk("TAKEOVER")])
        );
    }

    #[test]
    fn cluster_reset_soft_frame() {
        let cmd = ClusterReset::soft();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CLUSTER"), bulk("RESET"), bulk("SOFT")])
        );
        assert_eq!(cmd.name(), "CLUSTER RESET");
    }

    #[test]
    fn cluster_reset_hard_frame() {
        let cmd = ClusterReset::hard();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CLUSTER"), bulk("RESET"), bulk("HARD")])
        );
    }

    #[test]
    fn cluster_count_keys_in_slot_frame() {
        let cmd = ClusterCountKeysInSlot::new(7000);
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            array(vec![bulk("CLUSTER"), bulk("COUNTKEYSINSLOT"), bulk("7000"),])
        );
        assert_eq!(cmd.name(), "CLUSTER COUNTKEYSINSLOT");
    }

    #[test]
    fn cluster_count_keys_in_slot_parse_response() {
        let cmd = ClusterCountKeysInSlot::new(7000);
        let frame = Frame::Integer(42);
        let resp = cmd.parse_response(frame).unwrap();
        assert_eq!(resp, 42);
    }

    #[test]
    fn cluster_get_keys_in_slot_frame() {
        let cmd = ClusterGetKeysInSlot::new(7000, 10);
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            array(vec![
                bulk("CLUSTER"),
                bulk("GETKEYSINSLOT"),
                bulk("7000"),
                bulk("10"),
            ])
        );
        assert_eq!(cmd.name(), "CLUSTER GETKEYSINSLOT");
    }

    #[test]
    fn cluster_get_keys_in_slot_parse_response() {
        let cmd = ClusterGetKeysInSlot::new(7000, 10);
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("key1"))),
            Frame::BulkString(Some(Bytes::from("key2"))),
        ]));
        let resp = cmd.parse_response(frame).unwrap();
        assert_eq!(resp, vec![Bytes::from("key1"), Bytes::from("key2")]);
    }

    #[test]
    fn cluster_get_keys_in_slot_parse_empty() {
        let cmd = ClusterGetKeysInSlot::new(7000, 10);
        let frame = Frame::Array(Some(vec![]));
        let resp = cmd.parse_response(frame).unwrap();
        assert!(resp.is_empty());
    }

    #[test]
    fn cluster_keyslot_frame() {
        let cmd = ClusterKeySlot::new("mykey");
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            array(vec![bulk("CLUSTER"), bulk("KEYSLOT"), bulk("mykey")])
        );
        assert_eq!(cmd.name(), "CLUSTER KEYSLOT");
    }

    #[test]
    fn cluster_keyslot_parse_response() {
        let cmd = ClusterKeySlot::new("mykey");
        let frame = Frame::Integer(14687);
        let resp = cmd.parse_response(frame).unwrap();
        assert_eq!(resp, 14687);
    }
}
