use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// CLUSTER INFO
///
/// Returns information and statistics about the cluster.
/// The response is a bulk string of key-value pairs separated by `\r\n`.
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
pub struct ClusterFailover {
    option: Option<FailoverOption>,
}

/// Options for CLUSTER FAILOVER.
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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

/// CLUSTER HELP
///
/// Returns helpful text describing the CLUSTER subcommands.
#[derive(Clone)]
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

impl Command for ClusterHelp {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CLUSTER"), bulk("HELP")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        crate::help::parse_help_lines(frame)
    }

    fn name(&self) -> &str {
        "CLUSTER HELP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// Slot assignment state for CLUSTER SETSLOT.
#[derive(Clone)]
pub enum SetSlotState {
    /// Mark the slot as importing from the node with the given ID.
    Importing(String),
    /// Mark the slot as migrating to the node with the given ID.
    Migrating(String),
    /// Clear any importing/migrating state on the slot.
    Stable,
    /// Bind the slot to the node with the given ID.
    Node(String),
}

/// CLUSTER SETSLOT slot IMPORTING|MIGRATING|STABLE|NODE [node-id]
///
/// Changes the state of a hash slot on the receiving node. Used by resharding
/// tooling to move a slot between nodes.
#[derive(Clone)]
pub struct ClusterSetSlot {
    slot: u16,
    state: SetSlotState,
}

impl ClusterSetSlot {
    pub fn new(slot: u16, state: SetSlotState) -> Self {
        Self { slot, state }
    }
}

impl Command for ClusterSetSlot {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("CLUSTER"),
            bulk("SETSLOT"),
            bulk(self.slot.to_string()),
        ];
        match &self.state {
            SetSlotState::Importing(id) => {
                args.push(bulk("IMPORTING"));
                args.push(bulk(id.as_str()));
            }
            SetSlotState::Migrating(id) => {
                args.push(bulk("MIGRATING"));
                args.push(bulk(id.as_str()));
            }
            SetSlotState::Stable => args.push(bulk("STABLE")),
            SetSlotState::Node(id) => {
                args.push(bulk("NODE"));
                args.push(bulk(id.as_str()));
            }
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_ok(frame)
    }

    fn name(&self) -> &str {
        "CLUSTER SETSLOT"
    }
}

/// CLUSTER ADDSLOTS slot [slot ...]
///
/// Assigns the given hash slots to the receiving node.
#[derive(Clone)]
pub struct ClusterAddSlots {
    slots: Vec<u16>,
}

impl ClusterAddSlots {
    pub fn new(slots: impl IntoIterator<Item = u16>) -> Self {
        Self {
            slots: slots.into_iter().collect(),
        }
    }
}

impl Command for ClusterAddSlots {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("CLUSTER"), bulk("ADDSLOTS")];
        args.extend(self.slots.iter().map(|s| bulk(s.to_string())));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_ok(frame)
    }

    fn name(&self) -> &str {
        "CLUSTER ADDSLOTS"
    }
}

/// CLUSTER DELSLOTS slot [slot ...]
///
/// Removes the given hash slots from the receiving node.
#[derive(Clone)]
pub struct ClusterDelSlots {
    slots: Vec<u16>,
}

impl ClusterDelSlots {
    pub fn new(slots: impl IntoIterator<Item = u16>) -> Self {
        Self {
            slots: slots.into_iter().collect(),
        }
    }
}

impl Command for ClusterDelSlots {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("CLUSTER"), bulk("DELSLOTS")];
        args.extend(self.slots.iter().map(|s| bulk(s.to_string())));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_ok(frame)
    }

    fn name(&self) -> &str {
        "CLUSTER DELSLOTS"
    }
}

/// CLUSTER ADDSLOTSRANGE start-slot end-slot [start-slot end-slot ...]
///
/// Assigns the given inclusive hash-slot ranges to the receiving node (Redis 7.0+).
#[derive(Clone)]
pub struct ClusterAddSlotsRange {
    ranges: Vec<(u16, u16)>,
}

impl ClusterAddSlotsRange {
    pub fn new(ranges: impl IntoIterator<Item = (u16, u16)>) -> Self {
        Self {
            ranges: ranges.into_iter().collect(),
        }
    }
}

impl Command for ClusterAddSlotsRange {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("CLUSTER"), bulk("ADDSLOTSRANGE")];
        for (start, end) in &self.ranges {
            args.push(bulk(start.to_string()));
            args.push(bulk(end.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_ok(frame)
    }

    fn name(&self) -> &str {
        "CLUSTER ADDSLOTSRANGE"
    }
}

/// CLUSTER DELSLOTSRANGE start-slot end-slot [start-slot end-slot ...]
///
/// Removes the given inclusive hash-slot ranges from the receiving node (Redis 7.0+).
#[derive(Clone)]
pub struct ClusterDelSlotsRange {
    ranges: Vec<(u16, u16)>,
}

impl ClusterDelSlotsRange {
    pub fn new(ranges: impl IntoIterator<Item = (u16, u16)>) -> Self {
        Self {
            ranges: ranges.into_iter().collect(),
        }
    }
}

impl Command for ClusterDelSlotsRange {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("CLUSTER"), bulk("DELSLOTSRANGE")];
        for (start, end) in &self.ranges {
            args.push(bulk(start.to_string()));
            args.push(bulk(end.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_ok(frame)
    }

    fn name(&self) -> &str {
        "CLUSTER DELSLOTSRANGE"
    }
}

/// CLUSTER REPLICAS node-id
///
/// Lists the replica nodes of the specified master, one node-description line
/// per replica (same format as CLUSTER NODES lines).
#[derive(Clone)]
pub struct ClusterReplicas {
    node_id: String,
}

impl ClusterReplicas {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }
}

impl Command for ClusterReplicas {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLUSTER"),
            bulk("REPLICAS"),
            bulk(self.node_id.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_string_array(frame)
    }

    fn name(&self) -> &str {
        "CLUSTER REPLICAS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// CLUSTER SLAVES node-id
///
/// Deprecated alias of [`ClusterReplicas`], retained for compatibility with
/// older Redis servers. Prefer `CLUSTER REPLICAS`.
#[derive(Clone)]
pub struct ClusterSlaves {
    node_id: String,
}

impl ClusterSlaves {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }
}

impl Command for ClusterSlaves {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLUSTER"),
            bulk("SLAVES"),
            bulk(self.node_id.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_string_array(frame)
    }

    fn name(&self) -> &str {
        "CLUSTER SLAVES"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// CLUSTER LINKS
///
/// Returns an array of maps describing the cluster bus links to and from each
/// peer node (Redis 7.0+). The raw [`Frame`] is returned because the reply is a
/// map array whose wire shape differs between RESP2 and RESP3.
#[derive(Clone)]
pub struct ClusterLinks;

impl ClusterLinks {
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
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CLUSTER"), bulk("LINKS")])
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
        "CLUSTER LINKS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// CLUSTER SET-CONFIG-EPOCH config-epoch
///
/// Sets the configuration epoch of a fresh node. Only valid on a node with a
/// zero epoch and no assigned slots.
#[derive(Clone)]
pub struct ClusterSetConfigEpoch {
    epoch: u64,
}

impl ClusterSetConfigEpoch {
    pub fn new(epoch: u64) -> Self {
        Self { epoch }
    }
}

impl Command for ClusterSetConfigEpoch {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLUSTER"),
            bulk("SET-CONFIG-EPOCH"),
            bulk(self.epoch.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_ok(frame)
    }

    fn name(&self) -> &str {
        "CLUSTER SET-CONFIG-EPOCH"
    }
}

/// CLUSTER BUMPEPOCH
///
/// Advances the configuration epoch of the node. The reply is a status string,
/// either `BUMPED <new-epoch>` or `STILL <current-epoch>`.
#[derive(Clone)]
pub struct ClusterBumpEpoch;

impl ClusterBumpEpoch {
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
        array(vec![bulk("CLUSTER"), bulk("BUMPEPOCH")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).into_owned()),
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "status string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLUSTER BUMPEPOCH"
    }
}

/// CLUSTER FLUSHSLOTS
///
/// Removes all hash-slot assignments from the receiving node. Only valid when
/// the node's slot table is empty of keys.
#[derive(Clone)]
pub struct ClusterFlushSlots;

impl ClusterFlushSlots {
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
        array(vec![bulk("CLUSTER"), bulk("FLUSHSLOTS")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_ok(frame)
    }

    fn name(&self) -> &str {
        "CLUSTER FLUSHSLOTS"
    }
}

/// CLUSTER SAVECONFIG
///
/// Forces the node to save the cluster configuration to disk.
#[derive(Clone)]
pub struct ClusterSaveConfig;

impl ClusterSaveConfig {
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
        array(vec![bulk("CLUSTER"), bulk("SAVECONFIG")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_ok(frame)
    }

    fn name(&self) -> &str {
        "CLUSTER SAVECONFIG"
    }
}

/// Parse a `+OK` status reply, accepting both the RESP simple-string and
/// bulk-string encodings.
fn parse_ok(frame: Frame) -> Result<(), RedisError> {
    match frame {
        Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
        Frame::BulkString(Some(data)) if &data[..] == b"OK" => Ok(()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "OK",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse an array of bulk strings into a `Vec<String>`.
fn parse_string_array(frame: Frame) -> Result<Vec<String>, RedisError> {
    match frame {
        Frame::Array(Some(frames)) => {
            let mut out = Vec::with_capacity(frames.len());
            for f in frames {
                match f {
                    Frame::BulkString(Some(data)) => {
                        out.push(String::from_utf8_lossy(&data).into_owned())
                    }
                    Frame::SimpleString(s) => out.push(String::from_utf8_lossy(&s).into_owned()),
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "bulk string",
                            actual: format!("{other:?}"),
                        });
                    }
                }
            }
            Ok(out)
        }
        Frame::Array(None) => Ok(Vec::new()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "array",
            actual: format!("{other:?}"),
        }),
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

    #[test]
    fn cluster_help_to_frame() {
        let cmd = ClusterHelp::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("CLUSTER"), bulk("HELP")]));
        assert!(cmd.idempotent());
    }

    #[test]
    fn cluster_help_parse_lines() {
        let cmd = ClusterHelp::new();
        let reply = array(vec![bulk("CLUSTER <subcommand>"), bulk("INFO")]);
        let lines = cmd.parse_response(reply).unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(&lines[1][..], b"INFO");
    }

    #[test]
    fn cluster_setslot_variants_frame() {
        assert_eq!(
            ClusterSetSlot::new(42, SetSlotState::Stable).to_frame(),
            array(vec![
                bulk("CLUSTER"),
                bulk("SETSLOT"),
                bulk("42"),
                bulk("STABLE")
            ])
        );
        assert_eq!(
            ClusterSetSlot::new(42, SetSlotState::Importing("node-a".into())).to_frame(),
            array(vec![
                bulk("CLUSTER"),
                bulk("SETSLOT"),
                bulk("42"),
                bulk("IMPORTING"),
                bulk("node-a"),
            ])
        );
        assert_eq!(
            ClusterSetSlot::new(42, SetSlotState::Migrating("node-b".into())).to_frame(),
            array(vec![
                bulk("CLUSTER"),
                bulk("SETSLOT"),
                bulk("42"),
                bulk("MIGRATING"),
                bulk("node-b"),
            ])
        );
        let node = ClusterSetSlot::new(42, SetSlotState::Node("node-c".into()));
        assert_eq!(
            node.to_frame(),
            array(vec![
                bulk("CLUSTER"),
                bulk("SETSLOT"),
                bulk("42"),
                bulk("NODE"),
                bulk("node-c"),
            ])
        );
        assert_eq!(node.name(), "CLUSTER SETSLOT");
        assert!(!node.idempotent());
        assert!(
            node.parse_response(Frame::SimpleString(Bytes::from("OK")))
                .is_ok()
        );
    }

    #[test]
    fn cluster_addslots_delslots_frame() {
        assert_eq!(
            ClusterAddSlots::new([1, 2, 3]).to_frame(),
            array(vec![
                bulk("CLUSTER"),
                bulk("ADDSLOTS"),
                bulk("1"),
                bulk("2"),
                bulk("3"),
            ])
        );
        assert_eq!(
            ClusterDelSlots::new([7]).to_frame(),
            array(vec![bulk("CLUSTER"), bulk("DELSLOTS"), bulk("7")])
        );
    }

    #[test]
    fn cluster_slots_range_frame() {
        assert_eq!(
            ClusterAddSlotsRange::new([(0, 100), (200, 300)]).to_frame(),
            array(vec![
                bulk("CLUSTER"),
                bulk("ADDSLOTSRANGE"),
                bulk("0"),
                bulk("100"),
                bulk("200"),
                bulk("300"),
            ])
        );
        assert_eq!(
            ClusterDelSlotsRange::new([(0, 16383)]).to_frame(),
            array(vec![
                bulk("CLUSTER"),
                bulk("DELSLOTSRANGE"),
                bulk("0"),
                bulk("16383"),
            ])
        );
    }

    #[test]
    fn cluster_replicas_frame_and_parse() {
        let cmd = ClusterReplicas::new("master-id");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CLUSTER"), bulk("REPLICAS"), bulk("master-id")])
        );
        assert!(cmd.idempotent());
        let reply = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("id1 1.2.3.4:6379@16379 slave ..."))),
            Frame::BulkString(Some(Bytes::from("id2 1.2.3.5:6379@16379 slave ..."))),
        ]));
        let out = cmd.parse_response(reply).unwrap();
        assert_eq!(out.len(), 2);
        assert!(out[0].starts_with("id1"));
    }

    #[test]
    fn cluster_slaves_is_replicas_alias() {
        let cmd = ClusterSlaves::new("master-id");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CLUSTER"), bulk("SLAVES"), bulk("master-id")])
        );
        assert!(cmd.idempotent());
        assert!(cmd.parse_response(Frame::Array(None)).unwrap().is_empty());
    }

    #[test]
    fn cluster_links_frame_and_parse() {
        let cmd = ClusterLinks::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("CLUSTER"), bulk("LINKS")]));
        assert!(cmd.idempotent());
        let reply = Frame::Array(Some(vec![]));
        assert!(matches!(
            cmd.parse_response(reply).unwrap(),
            Frame::Array(_)
        ));
    }

    #[test]
    fn cluster_epoch_commands_frame() {
        assert_eq!(
            ClusterSetConfigEpoch::new(5).to_frame(),
            array(vec![bulk("CLUSTER"), bulk("SET-CONFIG-EPOCH"), bulk("5"),])
        );
        let bump = ClusterBumpEpoch::new();
        assert_eq!(
            bump.to_frame(),
            array(vec![bulk("CLUSTER"), bulk("BUMPEPOCH")])
        );
        let resp = bump
            .parse_response(Frame::SimpleString(Bytes::from("BUMPED 6")))
            .unwrap();
        assert_eq!(resp, "BUMPED 6");
    }

    #[test]
    fn cluster_flushslots_saveconfig_frame() {
        assert_eq!(
            ClusterFlushSlots::new().to_frame(),
            array(vec![bulk("CLUSTER"), bulk("FLUSHSLOTS")])
        );
        assert_eq!(
            ClusterSaveConfig::new().to_frame(),
            array(vec![bulk("CLUSTER"), bulk("SAVECONFIG")])
        );
        assert!(
            ClusterSaveConfig::new()
                .parse_response(Frame::SimpleString(Bytes::from("OK")))
                .is_ok()
        );
    }

    #[test]
    fn parse_ok_accepts_bulk_and_simple() {
        assert!(parse_ok(Frame::SimpleString(Bytes::from("OK"))).is_ok());
        assert!(parse_ok(Frame::BulkString(Some(Bytes::from("OK")))).is_ok());
        assert!(parse_ok(Frame::Integer(1)).is_err());
    }
}
