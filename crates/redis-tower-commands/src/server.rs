use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// PING \[message\]
///
/// Returns PONG, or echoes the message if provided.
#[derive(Clone)]
pub struct Ping {
    message: Option<String>,
}

impl Ping {
    pub fn new() -> Self {
        Self { message: None }
    }

    pub fn with_message(message: impl Into<String>) -> Self {
        Self {
            message: Some(message.into()),
        }
    }
}

impl Default for Ping {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Ping {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("PING")];
        if let Some(ref msg) = self.message {
            args.push(bulk(msg.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).into_owned()),
            Frame::BulkString(Some(s)) => Ok(String::from_utf8_lossy(&s).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "simple string or bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "PING"
    }
}

/// FLUSHDB [ASYNC|SYNC]
///
/// Delete all keys in the current database.
#[derive(Clone)]
pub struct FlushDb {
    mode: Option<FlushMode>,
}

#[derive(Clone)]
pub enum FlushMode {
    Async,
    Sync,
}

impl FlushDb {
    pub fn new() -> Self {
        Self { mode: None }
    }

    pub fn async_mode(mut self) -> Self {
        self.mode = Some(FlushMode::Async);
        self
    }

    pub fn sync_mode(mut self) -> Self {
        self.mode = Some(FlushMode::Sync);
        self
    }
}

impl Default for FlushDb {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for FlushDb {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("FLUSHDB")];
        match &self.mode {
            Some(FlushMode::Async) => args.push(bulk("ASYNC")),
            Some(FlushMode::Sync) => args.push(bulk("SYNC")),
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
        "FLUSHDB"
    }
}

/// DBSIZE
///
/// Returns the number of keys in the current database.
#[derive(Clone)]
pub struct DbSize;

impl DbSize {
    pub fn new() -> Self {
        Self
    }
}

impl Default for DbSize {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for DbSize {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("DBSIZE")])
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
        "DBSIZE"
    }
}

/// SELECT index
///
/// Select the Redis database for the current connection.
#[derive(Clone)]
pub struct Select {
    db: u16,
}

impl Select {
    pub fn new(db: u16) -> Self {
        Self { db }
    }
}

impl Command for Select {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("SELECT"), bulk(self.db.to_string())])
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
        "SELECT"
    }
}

/// AUTH \[username\] password
///
/// Authenticate to the server. With Redis 6+ ACLs, pass both username
/// and password. For older versions, only pass the password.
#[derive(Clone)]
pub struct Auth {
    username: Option<String>,
    password: String,
}

impl Auth {
    /// Authenticate with password only (pre-Redis 6).
    pub fn password(password: impl Into<String>) -> Self {
        Self {
            username: None,
            password: password.into(),
        }
    }

    /// Authenticate with username and password (Redis 6+ ACL).
    pub fn credentials(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: Some(username.into()),
            password: password.into(),
        }
    }
}

impl Command for Auth {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("AUTH")];
        if let Some(ref user) = self.username {
            args.push(bulk(user.as_str()));
        }
        args.push(bulk(self.password.as_str()));
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
        "AUTH"
    }
}

/// CLIENT TRACKING ON|OFF \[REDIRECT client-id\] \[PREFIX prefix\] \[BCAST\] \[OPTIN\] \[OPTOUT\]
///
/// Enable or disable server-assisted client-side caching.
#[derive(Clone)]
pub struct ClientTracking {
    enabled: bool,
    bcast: bool,
    prefixes: Vec<String>,
    optin: bool,
    optout: bool,
}

impl ClientTracking {
    /// Enable client tracking.
    pub fn on() -> Self {
        Self {
            enabled: true,
            bcast: false,
            prefixes: Vec::new(),
            optin: false,
            optout: false,
        }
    }

    /// Disable client tracking.
    pub fn off() -> Self {
        Self {
            enabled: false,
            bcast: false,
            prefixes: Vec::new(),
            optin: false,
            optout: false,
        }
    }

    /// Enable broadcasting mode (invalidate all keys matching prefixes).
    pub fn bcast(mut self) -> Self {
        self.bcast = true;
        self
    }

    /// Add a key prefix to track (only with BCAST mode).
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefixes.push(prefix.into());
        self
    }

    /// Enable opt-in mode (only track keys after CLIENT CACHING YES).
    pub fn optin(mut self) -> Self {
        self.optin = true;
        self
    }

    /// Enable opt-out mode (track all keys, skip after CLIENT CACHING NO).
    pub fn optout(mut self) -> Self {
        self.optout = true;
        self
    }
}

impl Command for ClientTracking {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("CLIENT"),
            bulk("TRACKING"),
            bulk(if self.enabled { "ON" } else { "OFF" }),
        ];
        if self.bcast {
            args.push(bulk("BCAST"));
        }
        for prefix in &self.prefixes {
            args.push(bulk("PREFIX"));
            args.push(bulk(prefix.as_str()));
        }
        if self.optin {
            args.push(bulk("OPTIN"));
        }
        if self.optout {
            args.push(bulk("OPTOUT"));
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
        "CLIENT TRACKING"
    }
}

/// INFO \[section ...\]
///
/// Returns information and statistics about the server. An optional section
/// filter can be provided to limit the output (e.g. "server", "memory",
/// "replication"). Returns the raw bulk string; callers can parse the
/// key-value pairs from the line-oriented format.
#[derive(Clone)]
pub struct Info {
    sections: Vec<String>,
}

impl Info {
    /// Request all info sections.
    pub fn new() -> Self {
        Self {
            sections: Vec::new(),
        }
    }

    /// Request a specific section (e.g. "server", "memory", "replication").
    pub fn section(mut self, section: impl Into<String>) -> Self {
        self.sections.push(section.into());
        self
    }
}

impl Default for Info {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Info {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("INFO")];
        for s in &self.sections {
            args.push(bulk(s.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // RESP3 returns INFO as a verbatim string; RESP2 as a bulk string.
            Frame::BulkString(Some(s)) | Frame::VerbatimString(_, s) => {
                Ok(String::from_utf8_lossy(&s).into_owned())
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk or verbatim string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "INFO"
    }
}

/// TIME
///
/// Returns the current server time as a two-element array:
/// unix timestamp in seconds and microseconds.
#[derive(Clone)]
pub struct Time;

impl Time {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Time {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Time {
    /// (unix_seconds, microseconds)
    type Response = (i64, i64);

    fn to_frame(&self) -> Frame {
        array(vec![bulk("TIME")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) if frames.len() == 2 => {
                let secs = match &frames[0] {
                    Frame::BulkString(Some(s)) => String::from_utf8_lossy(s)
                        .parse::<i64>()
                        .map_err(|_| RedisError::UnexpectedResponse {
                            expected: "integer string",
                            actual: format!("{:?}", frames[0]),
                        })?,
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "bulk string",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                let micros = match &frames[1] {
                    Frame::BulkString(Some(s)) => String::from_utf8_lossy(s)
                        .parse::<i64>()
                        .map_err(|_| RedisError::UnexpectedResponse {
                            expected: "integer string",
                            actual: format!("{:?}", frames[1]),
                        })?,
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "bulk string",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                Ok((secs, micros))
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "array of two bulk strings",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "TIME"
    }
}

/// COMMAND COUNT
///
/// Returns the total number of commands supported by the server.
#[derive(Clone)]
pub struct CommandCount;

impl CommandCount {
    pub fn new() -> Self {
        Self
    }
}

impl Default for CommandCount {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for CommandCount {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("COMMAND"), bulk("COUNT")])
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
        "COMMAND COUNT"
    }
}

/// COMMAND DOCS \[command-name ...\]
///
/// Returns documentary information about one or more commands.
/// Each command's documentation is returned as a nested array of
/// key-value pairs.
#[derive(Clone)]
pub struct CommandDocs {
    commands: Vec<String>,
}

impl CommandDocs {
    /// Request docs for all commands.
    pub fn new() -> Self {
        Self {
            commands: Vec::new(),
        }
    }

    /// Request docs for a specific command.
    pub fn command(mut self, name: impl Into<String>) -> Self {
        self.commands.push(name.into());
        self
    }
}

impl Default for CommandDocs {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for CommandDocs {
    /// Raw frames -- the structure is deeply nested and command-specific.
    type Response = Vec<Frame>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("COMMAND"), bulk("DOCS")];
        for c in &self.commands {
            args.push(bulk(c.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => Ok(frames),
            Frame::Array(None) => Ok(Vec::new()),
            // RESP3 returns the docs as a map; flatten it to the RESP2
            // key/value array shape so callers see one stable layout.
            Frame::Map(pairs) => {
                let mut out = Vec::with_capacity(pairs.len() * 2);
                for (k, v) in pairs {
                    out.push(k);
                    out.push(v);
                }
                Ok(out)
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or map",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "COMMAND DOCS"
    }
}

/// COMMAND LIST \[FILTERBY MODULE module | ACLCAT category | PATTERN pattern\]
///
/// Returns a list of all command names supported by the server.
#[derive(Clone)]
pub struct CommandList {
    filter: Option<CommandListFilter>,
}

/// Filter for the COMMAND LIST command.
#[derive(Clone)]
pub enum CommandListFilter {
    /// Filter by module name.
    Module(String),
    /// Filter by ACL category.
    AclCat(String),
    /// Filter by glob-style pattern.
    Pattern(String),
}

impl CommandList {
    /// List all commands without filtering.
    pub fn new() -> Self {
        Self { filter: None }
    }

    /// Filter by module name.
    pub fn module(name: impl Into<String>) -> Self {
        Self {
            filter: Some(CommandListFilter::Module(name.into())),
        }
    }

    /// Filter by ACL category.
    pub fn aclcat(category: impl Into<String>) -> Self {
        Self {
            filter: Some(CommandListFilter::AclCat(category.into())),
        }
    }

    /// Filter by glob-style pattern.
    pub fn pattern(pattern: impl Into<String>) -> Self {
        Self {
            filter: Some(CommandListFilter::Pattern(pattern.into())),
        }
    }
}

impl Default for CommandList {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for CommandList {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("COMMAND"), bulk("LIST")];
        if let Some(ref filter) = self.filter {
            args.push(bulk("FILTERBY"));
            match filter {
                CommandListFilter::Module(m) => {
                    args.push(bulk("MODULE"));
                    args.push(bulk(m.as_str()));
                }
                CommandListFilter::AclCat(c) => {
                    args.push(bulk("ACLCAT"));
                    args.push(bulk(c.as_str()));
                }
                CommandListFilter::Pattern(p) => {
                    args.push(bulk("PATTERN"));
                    args.push(bulk(p.as_str()));
                }
            }
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(Some(data)) => {
                        Ok(String::from_utf8_lossy(&data).into_owned())
                    }
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string",
                        actual: format!("{other:?}"),
                    }),
                })
                .collect(),
            Frame::Array(None) => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "COMMAND LIST"
    }
}

/// BGSAVE \[SCHEDULE\]
///
/// Trigger a background save of the dataset. With `schedule`, the save
/// is queued if one is already in progress (instead of returning an error).
#[derive(Clone)]
pub struct BgSave {
    schedule: bool,
}

impl BgSave {
    pub fn new() -> Self {
        Self { schedule: false }
    }

    /// Queue the save if one is already in progress.
    pub fn schedule(mut self) -> Self {
        self.schedule = true;
        self
    }
}

impl Default for BgSave {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for BgSave {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("BGSAVE")];
        if self.schedule {
            args.push(bulk("SCHEDULE"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "simple string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "BGSAVE"
    }
}

/// BGREWRITEAOF
///
/// Trigger an Append Only File rewrite. The rewrite runs in the background.
#[derive(Clone)]
pub struct BgRewriteAof;

impl BgRewriteAof {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BgRewriteAof {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for BgRewriteAof {
    type Response = String;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("BGREWRITEAOF")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "simple string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "BGREWRITEAOF"
    }
}

/// LASTSAVE
///
/// Returns the Unix timestamp of the last successful save to disk.
#[derive(Clone)]
pub struct LastSave;

impl LastSave {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LastSave {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for LastSave {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("LASTSAVE")])
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
        "LASTSAVE"
    }
}

/// REPLICAOF host port
///
/// Configure the server as a replica of another Redis instance,
/// or promote it to a primary with `ReplicaOf::no_one()`.
#[derive(Clone)]
pub struct ReplicaOf {
    host: String,
    port: String,
}

impl ReplicaOf {
    /// Make this server a replica of the given host and port.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port: port.to_string(),
        }
    }

    /// Promote this server to primary (REPLICAOF NO ONE).
    pub fn no_one() -> Self {
        Self {
            host: "NO".to_string(),
            port: "ONE".to_string(),
        }
    }
}

impl Command for ReplicaOf {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("REPLICAOF"),
            bulk(self.host.as_str()),
            bulk(self.port.as_str()),
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
        "REPLICAOF"
    }
}

/// SWAPDB index1 index2
///
/// Swap two Redis databases atomically.
#[derive(Clone)]
pub struct SwapDb {
    db1: u16,
    db2: u16,
}

impl SwapDb {
    pub fn new(db1: u16, db2: u16) -> Self {
        Self { db1, db2 }
    }
}

impl Command for SwapDb {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("SWAPDB"),
            bulk(self.db1.to_string()),
            bulk(self.db2.to_string()),
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
        "SWAPDB"
    }
}

/// FAILOVER \[TO host port \[FORCE\]\] \[ABORT\] \[TIMEOUT milliseconds\]
///
/// Trigger a replica failover (Redis 6.2+). When run on a primary, it
/// coordinates with a replica to perform a graceful failover.
#[derive(Clone)]
pub struct Failover {
    to: Option<(String, u16)>,
    force: bool,
    abort: bool,
    timeout: Option<u64>,
}

impl Failover {
    /// Initiate a failover with default settings.
    pub fn new() -> Self {
        Self {
            to: None,
            force: false,
            abort: false,
            timeout: None,
        }
    }

    /// Abort an in-progress failover.
    pub fn abort() -> Self {
        Self {
            to: None,
            force: false,
            abort: true,
            timeout: None,
        }
    }

    /// Target a specific replica for the failover.
    pub fn to(mut self, host: impl Into<String>, port: u16) -> Self {
        self.to = Some((host.into(), port));
        self
    }

    /// Force the failover even if the target replica is unreachable.
    /// Only valid when a target is specified with `to()`.
    pub fn force(mut self) -> Self {
        self.force = true;
        self
    }

    /// Set a timeout in milliseconds for the failover operation.
    pub fn timeout(mut self, ms: u64) -> Self {
        self.timeout = Some(ms);
        self
    }
}

impl Default for Failover {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Failover {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("FAILOVER")];
        if let Some((ref host, port)) = self.to {
            args.push(bulk("TO"));
            args.push(bulk(host.as_str()));
            args.push(bulk(port.to_string()));
            if self.force {
                args.push(bulk("FORCE"));
            }
        }
        if self.abort {
            args.push(bulk("ABORT"));
        }
        if let Some(ms) = self.timeout {
            args.push(bulk("TIMEOUT"));
            args.push(bulk(ms.to_string()));
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
        "FAILOVER"
    }
}

/// WAIT numreplicas timeout
///
/// Blocks the current client until all previous write commands are acknowledged
/// by at least `numreplicas` replicas, or until the timeout (in milliseconds)
/// expires. Returns the number of replicas that acknowledged.
#[derive(Clone)]
pub struct Wait {
    numreplicas: i64,
    timeout: i64,
}

impl Wait {
    pub fn new(numreplicas: i64, timeout: i64) -> Self {
        Self {
            numreplicas,
            timeout,
        }
    }
}

impl Command for Wait {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("WAIT"),
            bulk(self.numreplicas.to_string()),
            bulk(self.timeout.to_string()),
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
        "WAIT"
    }
}

/// WAITAOF numlocal numreplicas timeout
///
/// Blocks the current client until all previous write commands are fsynced
/// to the AOF of the local host and/or at least `numreplicas` replicas.
/// Returns a tuple of (local, replicas) counts parsed from a two-element array.
#[derive(Clone)]
pub struct WaitAof {
    numlocal: i64,
    numreplicas: i64,
    timeout: i64,
}

impl WaitAof {
    pub fn new(numlocal: i64, numreplicas: i64, timeout: i64) -> Self {
        Self {
            numlocal,
            numreplicas,
            timeout,
        }
    }
}

impl Command for WaitAof {
    type Response = (i64, i64);

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("WAITAOF"),
            bulk(self.numlocal.to_string()),
            bulk(self.numreplicas.to_string()),
            bulk(self.timeout.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) if frames.len() == 2 => {
                let local = match &frames[0] {
                    Frame::Integer(n) => *n,
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "integer",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                let replicas = match &frames[1] {
                    Frame::Integer(n) => *n,
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "integer",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                Ok((local, replicas))
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "array of two integers",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "WAITAOF"
    }
}

// ---------------------------------------------------------------------------
// CLIENT subcommands
// ---------------------------------------------------------------------------

/// CLIENT ID
///
/// Returns the ID of the current connection.
#[derive(Clone)]
pub struct ClientId;

impl ClientId {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClientId {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClientId {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CLIENT"), bulk("ID")])
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
        "CLIENT ID"
    }
}

/// CLIENT GETNAME
///
/// Returns the name of the current connection as set by CLIENT SETNAME,
/// or None if no name is set.
#[derive(Clone)]
pub struct ClientGetName;

impl ClientGetName {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClientGetName {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClientGetName {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CLIENT"), bulk("GETNAME")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(Some(data)),
            Frame::BulkString(None) | Frame::Null => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLIENT GETNAME"
    }
}

/// CLIENT SETNAME connection-name
///
/// Set the name of the current connection.
#[derive(Clone)]
pub struct ClientSetName {
    name: String,
}

impl ClientSetName {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Command for ClientSetName {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLIENT"),
            bulk("SETNAME"),
            bulk(self.name.as_str()),
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
        "CLIENT SETNAME"
    }
}

/// Filter type for CLIENT LIST.
#[derive(Clone)]
pub enum ClientListType {
    Normal,
    Master,
    Replica,
    Pubsub,
}

impl ClientListType {
    fn as_str(&self) -> &str {
        match self {
            Self::Normal => "normal",
            Self::Master => "master",
            Self::Replica => "replica",
            Self::Pubsub => "pubsub",
        }
    }
}

/// CLIENT LIST \[TYPE normal|master|replica|pubsub\]
///
/// Returns information and statistics about client connections.
/// The response is raw text with one client per line.
#[derive(Clone)]
pub struct ClientList {
    client_type: Option<ClientListType>,
}

impl ClientList {
    pub fn new() -> Self {
        Self { client_type: None }
    }

    /// Filter clients by type.
    pub fn client_type(mut self, t: ClientListType) -> Self {
        self.client_type = Some(t);
        self
    }
}

impl Default for ClientList {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClientList {
    type Response = Bytes;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("CLIENT"), bulk("LIST")];
        if let Some(ref t) = self.client_type {
            args.push(bulk("TYPE"));
            args.push(bulk(t.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // RESP3 returns CLIENT LIST as a verbatim string.
            Frame::BulkString(Some(data)) | Frame::VerbatimString(_, data) => Ok(data),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk or verbatim string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLIENT LIST"
    }
}

/// CLIENT KILL \[ID id\] \[ADDR addr\] \[LADDR addr\] \[USER user\] \[SKIPME yes|no\]
///
/// Kill client connections matching the given filters.
/// Returns the number of clients killed.
#[derive(Clone)]
pub struct ClientKill {
    id: Option<i64>,
    addr: Option<String>,
    laddr: Option<String>,
    user: Option<String>,
    skipme: Option<bool>,
}

impl ClientKill {
    pub fn new() -> Self {
        Self {
            id: None,
            addr: None,
            laddr: None,
            user: None,
            skipme: None,
        }
    }

    /// Kill client by connection ID.
    pub fn id(mut self, id: i64) -> Self {
        self.id = Some(id);
        self
    }

    /// Kill client by remote address (ip:port).
    pub fn addr(mut self, addr: impl Into<String>) -> Self {
        self.addr = Some(addr.into());
        self
    }

    /// Kill client by local address (ip:port).
    pub fn laddr(mut self, laddr: impl Into<String>) -> Self {
        self.laddr = Some(laddr.into());
        self
    }

    /// Kill client by authenticated username.
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
    }

    /// Whether to skip the calling client (default yes).
    pub fn skipme(mut self, skipme: bool) -> Self {
        self.skipme = Some(skipme);
        self
    }
}

impl Default for ClientKill {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClientKill {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("CLIENT"), bulk("KILL")];
        if let Some(id) = self.id {
            args.push(bulk("ID"));
            args.push(bulk(id.to_string()));
        }
        if let Some(ref addr) = self.addr {
            args.push(bulk("ADDR"));
            args.push(bulk(addr.as_str()));
        }
        if let Some(ref laddr) = self.laddr {
            args.push(bulk("LADDR"));
            args.push(bulk(laddr.as_str()));
        }
        if let Some(ref user) = self.user {
            args.push(bulk("USER"));
            args.push(bulk(user.as_str()));
        }
        if let Some(skipme) = self.skipme {
            args.push(bulk("SKIPME"));
            args.push(bulk(if skipme { "yes" } else { "no" }));
        }
        array(args)
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
        "CLIENT KILL"
    }
}

/// CLIENT INFO
///
/// Returns information about the current client connection.
#[derive(Clone)]
pub struct ClientInfo;

impl ClientInfo {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClientInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClientInfo {
    type Response = Bytes;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CLIENT"), bulk("INFO")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // RESP3 returns CLIENT INFO as a verbatim string.
            Frame::BulkString(Some(data)) | Frame::VerbatimString(_, data) => Ok(data),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk or verbatim string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CLIENT INFO"
    }
}

/// CLIENT NO-EVICT ON|OFF
///
/// Set the client eviction mode for the current connection. When enabled,
/// the current client will not be evicted even when the maxmemory-clients
/// threshold is reached.
#[derive(Clone)]
pub struct ClientNoEvict {
    enabled: bool,
}

impl ClientNoEvict {
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }
}

impl Command for ClientNoEvict {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLIENT"),
            bulk("NO-EVICT"),
            bulk(if self.enabled { "ON" } else { "OFF" }),
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
        "CLIENT NO-EVICT"
    }
}

/// CLIENT NO-TOUCH ON|OFF
///
/// Control whether commands sent by the client affect LRU/LFU of accessed
/// keys. When enabled, accessed keys will not have their idle time or
/// frequency updated.
#[derive(Clone)]
pub struct ClientNoTouch {
    enabled: bool,
}

impl ClientNoTouch {
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }
}

impl Command for ClientNoTouch {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLIENT"),
            bulk("NO-TOUCH"),
            bulk(if self.enabled { "ON" } else { "OFF" }),
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
        "CLIENT NO-TOUCH"
    }
}

/// Pause mode for CLIENT PAUSE.
#[derive(Clone)]
pub enum ClientPauseMode {
    /// Pause all client commands.
    All,
    /// Only pause write commands.
    Write,
}

impl ClientPauseMode {
    fn as_str(&self) -> &str {
        match self {
            Self::All => "ALL",
            Self::Write => "WRITE",
        }
    }
}

/// CLIENT PAUSE timeout \[WRITE|ALL\]
///
/// Suspend all clients for the specified amount of time (in milliseconds).
#[derive(Clone)]
pub struct ClientPause {
    timeout: u64,
    mode: Option<ClientPauseMode>,
}

impl ClientPause {
    pub fn new(timeout: u64) -> Self {
        Self {
            timeout,
            mode: None,
        }
    }

    /// Set the pause mode.
    pub fn mode(mut self, mode: ClientPauseMode) -> Self {
        self.mode = Some(mode);
        self
    }
}

impl Command for ClientPause {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("CLIENT"),
            bulk("PAUSE"),
            bulk(self.timeout.to_string()),
        ];
        if let Some(ref mode) = self.mode {
            args.push(bulk(mode.as_str()));
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
        "CLIENT PAUSE"
    }
}

/// CLIENT UNPAUSE
///
/// Resume clients that were paused by CLIENT PAUSE.
#[derive(Clone)]
pub struct ClientUnpause;

impl ClientUnpause {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClientUnpause {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClientUnpause {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CLIENT"), bulk("UNPAUSE")])
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
        "CLIENT UNPAUSE"
    }
}

// ---------------------------------------------------------------------------
// CONFIG subcommands
// ---------------------------------------------------------------------------

/// CONFIG GET pattern
///
/// Returns configuration parameters matching the glob-style pattern.
/// The response is a list of key-value pairs.
#[derive(Clone)]
pub struct ConfigGet {
    pattern: String,
}

impl ConfigGet {
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
        }
    }
}

impl Command for ConfigGet {
    type Response = Vec<(Bytes, Bytes)>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CONFIG"),
            bulk("GET"),
            bulk(self.pattern.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // RESP2: flat array of alternating key, value bulk strings
            Frame::Array(Some(frames)) => {
                if frames.len() % 2 != 0 {
                    return Err(RedisError::UnexpectedResponse {
                        expected: "array with even number of elements",
                        actual: format!("array with {} elements", frames.len()),
                    });
                }
                frames
                    .chunks(2)
                    .map(|pair| {
                        let key = match &pair[0] {
                            Frame::BulkString(Some(data)) => data.clone(),
                            other => {
                                return Err(RedisError::UnexpectedResponse {
                                    expected: "bulk string",
                                    actual: format!("{other:?}"),
                                });
                            }
                        };
                        let value = match &pair[1] {
                            Frame::BulkString(Some(data)) => data.clone(),
                            other => {
                                return Err(RedisError::UnexpectedResponse {
                                    expected: "bulk string",
                                    actual: format!("{other:?}"),
                                });
                            }
                        };
                        Ok((key, value))
                    })
                    .collect()
            }
            // RESP3: Map of key-value pairs
            Frame::Map(pairs) => pairs
                .into_iter()
                .map(|(k, v)| {
                    let key = match k {
                        Frame::BulkString(Some(data)) => data,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "bulk string key",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    let value = match v {
                        Frame::BulkString(Some(data)) => data,
                        other => {
                            return Err(RedisError::UnexpectedResponse {
                                expected: "bulk string value",
                                actual: format!("{other:?}"),
                            });
                        }
                    };
                    Ok((key, value))
                })
                .collect(),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or map",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "CONFIG GET"
    }
}

/// CONFIG SET param value \[param value ...\]
///
/// Set one or more configuration parameters to the given values.
#[derive(Clone)]
pub struct ConfigSet {
    pairs: Vec<(String, String)>,
}

impl ConfigSet {
    /// Set a single configuration parameter.
    pub fn new(param: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            pairs: vec![(param.into(), value.into())],
        }
    }

    /// Add an additional parameter-value pair.
    pub fn param(mut self, param: impl Into<String>, value: impl Into<String>) -> Self {
        self.pairs.push((param.into(), value.into()));
        self
    }
}

impl Command for ConfigSet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("CONFIG"), bulk("SET")];
        for (param, value) in &self.pairs {
            args.push(bulk(param.as_str()));
            args.push(bulk(value.as_str()));
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
        "CONFIG SET"
    }
}

/// CONFIG RESETSTAT
///
/// Reset the statistics reported by the INFO command.
#[derive(Clone)]
pub struct ConfigResetStat;

impl ConfigResetStat {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ConfigResetStat {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ConfigResetStat {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CONFIG"), bulk("RESETSTAT")])
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
        "CONFIG RESETSTAT"
    }
}

/// CONFIG REWRITE
///
/// Rewrite the configuration file with the in-memory configuration.
#[derive(Clone)]
pub struct ConfigRewrite;

impl ConfigRewrite {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ConfigRewrite {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ConfigRewrite {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CONFIG"), bulk("REWRITE")])
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
        "CONFIG REWRITE"
    }
}

/// CLIENT SETINFO LIB-NAME name
///
/// Set the client library name. Sent automatically on connection to
/// identify the client library to the Redis server.
#[derive(Clone)]
pub struct ClientSetInfoLibName {
    name: String,
}

impl ClientSetInfoLibName {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Command for ClientSetInfoLibName {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLIENT"),
            bulk("SETINFO"),
            bulk("LIB-NAME"),
            bulk(self.name.as_str()),
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
        "CLIENT SETINFO"
    }
}

/// CLIENT SETINFO LIB-VER version
///
/// Set the client library version. Sent automatically on connection to
/// identify the client library version to the Redis server.
#[derive(Clone)]
pub struct ClientSetInfoLibVer {
    version: String,
}

impl ClientSetInfoLibVer {
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            version: version.into(),
        }
    }
}

impl Command for ClientSetInfoLibVer {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLIENT"),
            bulk("SETINFO"),
            bulk("LIB-VER"),
            bulk(self.version.as_str()),
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
        "CLIENT SETINFO"
    }
}

/// ECHO message
///
/// Returns `message` back to the client. Useful for testing connectivity.
#[derive(Clone)]
pub struct Echo {
    message: String,
}

impl Echo {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Command for Echo {
    type Response = Bytes;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("ECHO"), bulk(self.message.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(data),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "ECHO"
    }
}

/// FLUSHALL [ASYNC|SYNC]
///
/// Delete all keys in all databases.
#[derive(Clone)]
pub struct FlushAll {
    mode: Option<FlushMode>,
}

impl FlushAll {
    pub fn new() -> Self {
        Self { mode: None }
    }

    pub fn async_mode(mut self) -> Self {
        self.mode = Some(FlushMode::Async);
        self
    }

    pub fn sync_mode(mut self) -> Self {
        self.mode = Some(FlushMode::Sync);
        self
    }
}

impl Default for FlushAll {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for FlushAll {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("FLUSHALL")];
        match &self.mode {
            Some(FlushMode::Async) => args.push(bulk("ASYNC")),
            Some(FlushMode::Sync) => args.push(bulk("SYNC")),
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
        "FLUSHALL"
    }
}

/// SAVE
///
/// Synchronously save the dataset to disk.
#[derive(Clone)]
pub struct Save;

impl Save {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Save {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Save {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("SAVE")])
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
        "SAVE"
    }
}

/// Save behavior for SHUTDOWN.
#[derive(Clone)]
pub enum ShutdownMode {
    /// Do not save the dataset before shutting down.
    NoSave,
    /// Force a save of the dataset before shutting down.
    Save,
}

/// SHUTDOWN \[NOSAVE | SAVE\] \[NOW\] \[FORCE\] \[ABORT\]
///
/// Shuts down the server. On a successful shutdown the connection is closed and
/// no reply is received; this command therefore treats both an absent reply and
/// an `OK` reply as success.
#[derive(Clone)]
pub struct Shutdown {
    mode: Option<ShutdownMode>,
    now: bool,
    force: bool,
    abort: bool,
}

impl Shutdown {
    pub fn new() -> Self {
        Self {
            mode: None,
            now: false,
            force: false,
            abort: false,
        }
    }

    /// Skip saving the dataset (NOSAVE).
    pub fn nosave(mut self) -> Self {
        self.mode = Some(ShutdownMode::NoSave);
        self
    }

    /// Force a save of the dataset (SAVE).
    pub fn save_mode(mut self) -> Self {
        self.mode = Some(ShutdownMode::Save);
        self
    }

    /// Skip the graceful shutdown delay (NOW).
    pub fn now(mut self) -> Self {
        self.now = true;
        self
    }

    /// Force shutdown even if there are errors (FORCE).
    pub fn force(mut self) -> Self {
        self.force = true;
        self
    }

    /// Abort an in-progress shutdown (ABORT).
    pub fn abort(mut self) -> Self {
        self.abort = true;
        self
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Shutdown {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SHUTDOWN")];
        match &self.mode {
            Some(ShutdownMode::NoSave) => args.push(bulk("NOSAVE")),
            Some(ShutdownMode::Save) => args.push(bulk("SAVE")),
            None => {}
        }
        if self.now {
            args.push(bulk("NOW"));
        }
        if self.force {
            args.push(bulk("FORCE"));
        }
        if self.abort {
            args.push(bulk("ABORT"));
        }
        array(args)
    }

    fn parse_response(&self, _frame: Frame) -> Result<Self::Response, RedisError> {
        // A successful SHUTDOWN closes the connection without a reply. Any frame
        // received (e.g. an OK from SHUTDOWN ABORT) is treated as success.
        Ok(())
    }

    fn name(&self) -> &str {
        "SHUTDOWN"
    }
}

/// ROLE
///
/// Returns the role of the instance in the context of replication. The response
/// structure varies by role, so the raw `Frame` is returned.
#[derive(Clone)]
pub struct Role;

impl Role {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Role {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Role {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("ROLE")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "ROLE"
    }
}

/// HELLO \[protover \[AUTH username password\] \[SETNAME clientname\]\]
///
/// Switches the connection's protocol and returns a map of server properties.
/// The response is returned as a raw `Frame` (map or array depending on the
/// negotiated protocol version).
#[derive(Clone)]
pub struct Hello {
    protover: Option<u8>,
    auth: Option<(String, String)>,
    setname: Option<String>,
}

impl Hello {
    pub fn new() -> Self {
        Self {
            protover: None,
            auth: None,
            setname: None,
        }
    }

    /// Set the protocol version to negotiate.
    pub fn proto(mut self, version: u8) -> Self {
        self.protover = Some(version);
        self
    }

    /// Authenticate while switching protocols.
    pub fn auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = Some((username.into(), password.into()));
        self
    }

    /// Set the connection name.
    pub fn setname(mut self, name: impl Into<String>) -> Self {
        self.setname = Some(name.into());
        self
    }
}

impl Default for Hello {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Hello {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("HELLO")];
        if let Some(version) = self.protover {
            args.push(bulk(version.to_string()));
        }
        if let Some((ref user, ref pass)) = self.auth {
            args.push(bulk("AUTH"));
            args.push(bulk(user.as_str()));
            args.push(bulk(pass.as_str()));
        }
        if let Some(ref name) = self.setname {
            args.push(bulk("SETNAME"));
            args.push(bulk(name.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "HELLO"
    }
}

/// RESET
///
/// Resets the connection to its initial state. Returns the simple string
/// `"RESET"`.
#[derive(Clone)]
pub struct Reset;

impl Reset {
    pub fn new() -> Self {
        Self
    }
}

impl Default for Reset {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Reset {
    type Response = String;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("RESET")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "simple string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "RESET"
    }
}

/// COMMAND INFO command-name \[command-name ...\]
///
/// Returns details about the specified commands. The response is a nested,
/// command-specific structure returned as a raw `Frame`.
#[derive(Clone)]
pub struct CommandInfo {
    commands: Vec<String>,
}

impl CommandInfo {
    pub fn new(cmd: impl Into<String>) -> Self {
        Self {
            commands: vec![cmd.into()],
        }
    }

    /// Add another command to query.
    pub fn command(mut self, c: impl Into<String>) -> Self {
        self.commands.push(c.into());
        self
    }
}

impl Command for CommandInfo {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("COMMAND"), bulk("INFO")];
        for c in &self.commands {
            args.push(bulk(c.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "COMMAND INFO"
    }
}

/// COMMAND GETKEYS command \[arg ...\]
///
/// Returns the keys that would be accessed by the given command invocation.
#[derive(Clone)]
pub struct CommandGetKeys {
    command: String,
    args: Vec<String>,
}

impl CommandGetKeys {
    pub fn new(cmd: impl Into<String>) -> Self {
        Self {
            command: cmd.into(),
            args: Vec::new(),
        }
    }

    /// Add an argument to the command invocation being analyzed.
    pub fn arg(mut self, a: impl Into<String>) -> Self {
        self.args.push(a.into());
        self
    }
}

impl Command for CommandGetKeys {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("COMMAND"),
            bulk("GETKEYS"),
            bulk(self.command.as_str()),
        ];
        for a in &self.args {
            args.push(bulk(a.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(Some(data)) => Ok(data),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string",
                        actual: format!("{other:?}"),
                    }),
                })
                .collect(),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "COMMAND GETKEYS"
    }
}

/// Reply mode for CLIENT REPLY.
#[derive(Clone)]
pub enum ClientReplyMode {
    On,
    Off,
    Skip,
}

impl ClientReplyMode {
    fn as_str(&self) -> &str {
        match self {
            Self::On => "ON",
            Self::Off => "OFF",
            Self::Skip => "SKIP",
        }
    }
}

/// CLIENT REPLY ON|OFF|SKIP
///
/// Controls whether the server replies to commands from the current
/// connection.
#[derive(Clone)]
pub struct ClientReply {
    mode: ClientReplyMode,
}

impl ClientReply {
    pub fn new(mode: ClientReplyMode) -> Self {
        Self { mode }
    }
}

impl Command for ClientReply {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLIENT"),
            bulk("REPLY"),
            bulk(self.mode.as_str()),
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
        "CLIENT REPLY"
    }
}

/// CLIENT TRACKINGINFO
///
/// Returns information about the current connection's server-assisted
/// client-side caching state. Returned as a raw `Frame` map.
#[derive(Clone)]
pub struct ClientTrackingInfo;

impl ClientTrackingInfo {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClientTrackingInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClientTrackingInfo {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CLIENT"), bulk("TRACKINGINFO")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "CLIENT TRACKINGINFO"
    }
}

/// Mode for CLIENT UNBLOCK.
#[derive(Clone)]
pub enum UnblockMode {
    Timeout,
    Error,
}

impl UnblockMode {
    fn as_str(&self) -> &str {
        match self {
            Self::Timeout => "TIMEOUT",
            Self::Error => "ERROR",
        }
    }
}

/// CLIENT UNBLOCK client-id \[TIMEOUT | ERROR\]
///
/// Unblocks a different connection that is blocked in a blocking command.
/// Returns `1` if the client was unblocked, `0` otherwise.
#[derive(Clone)]
pub struct ClientUnblock {
    client_id: i64,
    mode: Option<UnblockMode>,
}

impl ClientUnblock {
    pub fn new(client_id: i64) -> Self {
        Self {
            client_id,
            mode: None,
        }
    }

    /// Set the unblock mode (TIMEOUT or ERROR).
    pub fn mode(mut self, m: UnblockMode) -> Self {
        self.mode = Some(m);
        self
    }
}

impl Command for ClientUnblock {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("CLIENT"),
            bulk("UNBLOCK"),
            bulk(self.client_id.to_string()),
        ];
        if let Some(ref mode) = self.mode {
            args.push(bulk(mode.as_str()));
        }
        array(args)
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
        "CLIENT UNBLOCK"
    }
}

/// CLIENT CACHING YES|NO
///
/// Controls tracking of keys in the next command when client tracking is in
/// OPTIN or OPTOUT mode.
#[derive(Clone)]
pub struct ClientCaching {
    yes: bool,
}

impl ClientCaching {
    pub fn new(yes: bool) -> Self {
        Self { yes }
    }
}

impl Command for ClientCaching {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CLIENT"),
            bulk("CACHING"),
            bulk(if self.yes { "yes" } else { "no" }),
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
        "CLIENT CACHING"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use redis_tower_protocol::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

    // -- Ping --

    #[test]
    fn ping_no_message_to_frame() {
        let cmd = Ping::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("PING")]));
    }

    #[test]
    fn ping_with_message_to_frame() {
        let cmd = Ping::with_message("hello");
        assert_eq!(cmd.to_frame(), array(vec![bulk("PING"), bulk("hello")]));
    }

    #[test]
    fn ping_parse_pong() {
        let cmd = Ping::new();
        let frame = Frame::SimpleString(Bytes::from("PONG"));
        assert_eq!(cmd.parse_response(frame).unwrap(), "PONG");
    }

    #[test]
    fn ping_parse_bulk_string() {
        let cmd = Ping::with_message("hello");
        let frame = Frame::BulkString(Some(Bytes::from("hello")));
        assert_eq!(cmd.parse_response(frame).unwrap(), "hello");
    }

    #[test]
    fn ping_parse_error_on_integer() {
        let cmd = Ping::new();
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    // -- FlushDb --

    #[test]
    fn flushdb_to_frame() {
        let cmd = FlushDb::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("FLUSHDB")]));
    }

    #[test]
    fn flushdb_async_to_frame() {
        let cmd = FlushDb::new().async_mode();
        assert_eq!(cmd.to_frame(), array(vec![bulk("FLUSHDB"), bulk("ASYNC")]));
    }

    #[test]
    fn flushdb_sync_to_frame() {
        let cmd = FlushDb::new().sync_mode();
        assert_eq!(cmd.to_frame(), array(vec![bulk("FLUSHDB"), bulk("SYNC")]));
    }

    #[test]
    fn flushdb_parse_ok() {
        let cmd = FlushDb::new();
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
    }

    #[test]
    fn flushdb_parse_error_on_integer() {
        let cmd = FlushDb::new();
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    // -- DbSize --

    #[test]
    fn dbsize_to_frame() {
        let cmd = DbSize::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("DBSIZE")]));
    }

    #[test]
    fn dbsize_parse_integer() {
        let cmd = DbSize::new();
        assert_eq!(cmd.parse_response(Frame::Integer(42)).unwrap(), 42);
    }

    // -- Select --

    #[test]
    fn select_to_frame() {
        let cmd = Select::new(3);
        assert_eq!(cmd.to_frame(), array(vec![bulk("SELECT"), bulk("3")]));
    }

    // -- Auth --

    #[test]
    fn auth_password_to_frame() {
        let cmd = Auth::password("secret");
        assert_eq!(cmd.to_frame(), array(vec![bulk("AUTH"), bulk("secret")]));
    }

    #[test]
    fn auth_credentials_to_frame() {
        let cmd = Auth::credentials("user", "pass");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("AUTH"), bulk("user"), bulk("pass")])
        );
    }

    // -- Info --

    #[test]
    fn info_no_section_to_frame() {
        let cmd = Info::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("INFO")]));
    }

    #[test]
    fn info_with_section_to_frame() {
        let cmd = Info::new().section("memory");
        assert_eq!(cmd.to_frame(), array(vec![bulk("INFO"), bulk("memory")]));
    }

    #[test]
    fn info_parse_bulk_string() {
        let cmd = Info::new();
        let frame = Frame::BulkString(Some(Bytes::from("# Server\nredis_version:7.0\n")));
        let result = cmd.parse_response(frame).unwrap();
        assert!(result.contains("redis_version"));
    }

    #[test]
    fn info_parse_error_on_integer() {
        let cmd = Info::new();
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    #[test]
    fn info_parse_verbatim_string_resp3() {
        // Under RESP3 INFO comes back as a verbatim string (=...txt:...).
        let cmd = Info::new();
        let frame = Frame::VerbatimString(
            Bytes::from("txt"),
            Bytes::from("# Server\nredis_version:7.4\n"),
        );
        let result = cmd.parse_response(frame).unwrap();
        assert!(result.contains("redis_version"));
    }

    #[test]
    fn command_docs_parse_map_resp3() {
        // Under RESP3 COMMAND DOCS comes back as a map; it flattens to the
        // RESP2 key/value array shape.
        let cmd = CommandDocs::new().command("get");
        let frame = Frame::Map(vec![(bulk("get"), Frame::Array(Some(vec![])))]);
        let out = cmd.parse_response(frame).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], bulk("get"));
    }

    #[test]
    fn client_info_parse_verbatim_string_resp3() {
        let cmd = ClientInfo;
        let frame = Frame::VerbatimString(Bytes::from("txt"), Bytes::from("id=3 addr=127.0.0.1"));
        let out = cmd.parse_response(frame).unwrap();
        assert_eq!(&out[..], b"id=3 addr=127.0.0.1");
    }

    #[test]
    fn client_list_parse_verbatim_string_resp3() {
        let cmd = ClientList::new();
        let frame = Frame::VerbatimString(Bytes::from("txt"), Bytes::from("id=3 addr=127.0.0.1\n"));
        let out = cmd.parse_response(frame).unwrap();
        assert_eq!(&out[..], b"id=3 addr=127.0.0.1\n");
    }

    // -- ClientId --

    #[test]
    fn client_id_to_frame() {
        let cmd = ClientId::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("CLIENT"), bulk("ID")]));
    }

    #[test]
    fn client_id_parse_integer() {
        let cmd = ClientId::new();
        assert_eq!(cmd.parse_response(Frame::Integer(42)).unwrap(), 42);
    }

    // -- ClientGetName --

    #[test]
    fn client_getname_to_frame() {
        let cmd = ClientGetName::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("CLIENT"), bulk("GETNAME")]));
    }

    #[test]
    fn client_getname_parse_name() {
        let cmd = ClientGetName::new();
        let frame = Frame::BulkString(Some(Bytes::from("myconn")));
        assert_eq!(
            cmd.parse_response(frame).unwrap(),
            Some(Bytes::from("myconn"))
        );
    }

    #[test]
    fn client_getname_parse_null() {
        let cmd = ClientGetName::new();
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    // -- ClientSetName --

    #[test]
    fn client_setname_to_frame() {
        let cmd = ClientSetName::new("myconn");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CLIENT"), bulk("SETNAME"), bulk("myconn")])
        );
    }

    // -- ConfigGet --

    #[test]
    fn config_get_to_frame() {
        let cmd = ConfigGet::new("maxmemory");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CONFIG"), bulk("GET"), bulk("maxmemory")])
        );
    }

    #[test]
    fn config_get_parse_flat_array() {
        let cmd = ConfigGet::new("maxmemory");
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("maxmemory"))),
            Frame::BulkString(Some(Bytes::from("0"))),
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![(Bytes::from("maxmemory"), Bytes::from("0"))]);
    }

    #[test]
    fn config_get_parse_error_on_odd_array() {
        let cmd = ConfigGet::new("*");
        let frame = array(vec![Frame::BulkString(Some(Bytes::from("only_one")))]);
        assert!(cmd.parse_response(frame).is_err());
    }

    // -- ConfigSet --

    #[test]
    fn config_set_to_frame() {
        let cmd = ConfigSet::new("hz", "100");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CONFIG"), bulk("SET"), bulk("hz"), bulk("100")])
        );
    }

    #[test]
    fn config_set_parse_ok() {
        let cmd = ConfigSet::new("hz", "100");
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
    }

    // -- Time --

    #[test]
    fn time_to_frame() {
        let cmd = Time::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("TIME")]));
    }

    #[test]
    fn time_parse_response() {
        let cmd = Time::new();
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("1700000000"))),
            Frame::BulkString(Some(Bytes::from("123456"))),
        ]);
        let (secs, micros) = cmd.parse_response(frame).unwrap();
        assert_eq!(secs, 1700000000);
        assert_eq!(micros, 123456);
    }

    #[test]
    fn time_parse_error_on_wrong_length() {
        let cmd = Time::new();
        let frame = array(vec![Frame::BulkString(Some(Bytes::from("123")))]);
        assert!(cmd.parse_response(frame).is_err());
    }

    // -- CommandCount --

    #[test]
    fn command_count_to_frame() {
        let cmd = CommandCount::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("COMMAND"), bulk("COUNT")]));
    }

    // -- ClientTracking --

    // -- ClientSetInfoLibName --

    #[test]
    fn client_setinfo_lib_name_to_frame() {
        let cmd = ClientSetInfoLibName::new("redis-tower");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("CLIENT"),
                bulk("SETINFO"),
                bulk("LIB-NAME"),
                bulk("redis-tower"),
            ])
        );
    }

    #[test]
    fn client_setinfo_lib_name_parse_ok() {
        let cmd = ClientSetInfoLibName::new("redis-tower");
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
    }

    #[test]
    fn client_setinfo_lib_name_parse_error_on_integer() {
        let cmd = ClientSetInfoLibName::new("redis-tower");
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    #[test]
    fn client_setinfo_lib_name_name() {
        let cmd = ClientSetInfoLibName::new("redis-tower");
        assert_eq!(cmd.name(), "CLIENT SETINFO");
    }

    // -- ClientSetInfoLibVer --

    #[test]
    fn client_setinfo_lib_ver_to_frame() {
        let cmd = ClientSetInfoLibVer::new("0.1.0");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("CLIENT"),
                bulk("SETINFO"),
                bulk("LIB-VER"),
                bulk("0.1.0"),
            ])
        );
    }

    #[test]
    fn client_setinfo_lib_ver_parse_ok() {
        let cmd = ClientSetInfoLibVer::new("0.1.0");
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
    }

    #[test]
    fn client_setinfo_lib_ver_parse_error_on_integer() {
        let cmd = ClientSetInfoLibVer::new("0.1.0");
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    #[test]
    fn client_setinfo_lib_ver_name() {
        let cmd = ClientSetInfoLibVer::new("0.1.0");
        assert_eq!(cmd.name(), "CLIENT SETINFO");
    }

    // -- ClientTracking --

    #[test]
    fn client_tracking_on_bcast_to_frame() {
        let cmd = ClientTracking::on().bcast().prefix("user:");
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(Some(args)) => {
                assert_eq!(args[0], bulk("CLIENT"));
                assert_eq!(args[1], bulk("TRACKING"));
                assert_eq!(args[2], bulk("ON"));
                assert!(args.contains(&bulk("BCAST")));
                assert!(args.contains(&bulk("PREFIX")));
                assert!(args.contains(&bulk("user:")));
            }
            _ => panic!("expected array"),
        }
    }

    // -- Echo --

    #[test]
    fn echo_to_frame() {
        let cmd = Echo::new("hello");
        assert_eq!(cmd.to_frame(), array(vec![bulk("ECHO"), bulk("hello")]));
    }

    #[test]
    fn echo_parse_response() {
        let cmd = Echo::new("hello");
        let frame = Frame::BulkString(Some(Bytes::from("hello")));
        assert_eq!(cmd.parse_response(frame).unwrap(), Bytes::from("hello"));
    }

    // -- FlushAll --

    #[test]
    fn flushall_to_frame() {
        let cmd = FlushAll::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("FLUSHALL")]));
    }

    #[test]
    fn flushall_async_to_frame() {
        let cmd = FlushAll::new().async_mode();
        assert_eq!(cmd.to_frame(), array(vec![bulk("FLUSHALL"), bulk("ASYNC")]));
    }

    #[test]
    fn flushall_parse_ok() {
        let cmd = FlushAll::new();
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
    }

    // -- Save --

    #[test]
    fn save_to_frame() {
        let cmd = Save::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("SAVE")]));
    }

    // -- Shutdown --

    #[test]
    fn shutdown_to_frame() {
        let cmd = Shutdown::new().nosave().now().force();
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("SHUTDOWN"),
                bulk("NOSAVE"),
                bulk("NOW"),
                bulk("FORCE"),
            ])
        );
    }

    #[test]
    fn shutdown_abort_to_frame() {
        let cmd = Shutdown::new().abort();
        assert_eq!(cmd.to_frame(), array(vec![bulk("SHUTDOWN"), bulk("ABORT")]));
    }

    #[test]
    fn shutdown_parse_any() {
        let cmd = Shutdown::new();
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
        cmd.parse_response(Frame::Null).unwrap();
    }

    // -- Role --

    #[test]
    fn role_to_frame() {
        let cmd = Role::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("ROLE")]));
    }

    #[test]
    fn role_parse_passthrough() {
        let cmd = Role::new();
        let frame = array(vec![Frame::BulkString(Some(Bytes::from("master")))]);
        assert_eq!(cmd.parse_response(frame.clone()).unwrap(), frame);
    }

    // -- Hello --

    #[test]
    fn hello_bare_to_frame() {
        let cmd = Hello::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("HELLO")]));
    }

    #[test]
    fn hello_full_to_frame() {
        let cmd = Hello::new().proto(3).auth("user", "pass").setname("conn");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("HELLO"),
                bulk("3"),
                bulk("AUTH"),
                bulk("user"),
                bulk("pass"),
                bulk("SETNAME"),
                bulk("conn"),
            ])
        );
    }

    // -- Reset --

    #[test]
    fn reset_to_frame() {
        let cmd = Reset::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("RESET")]));
    }

    #[test]
    fn reset_parse_response() {
        let cmd = Reset::new();
        let frame = Frame::SimpleString(Bytes::from("RESET"));
        assert_eq!(cmd.parse_response(frame).unwrap(), "RESET");
    }

    // -- CommandInfo --

    #[test]
    fn command_info_to_frame() {
        let cmd = CommandInfo::new("get").command("set");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("COMMAND"),
                bulk("INFO"),
                bulk("get"),
                bulk("set"),
            ])
        );
    }

    // -- CommandGetKeys --

    #[test]
    fn command_getkeys_to_frame() {
        let cmd = CommandGetKeys::new("SET").arg("k").arg("v");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("COMMAND"),
                bulk("GETKEYS"),
                bulk("SET"),
                bulk("k"),
                bulk("v"),
            ])
        );
    }

    #[test]
    fn command_getkeys_parse_array() {
        let cmd = CommandGetKeys::new("SET");
        let frame = array(vec![Frame::BulkString(Some(Bytes::from("k")))]);
        assert_eq!(cmd.parse_response(frame).unwrap(), vec![Bytes::from("k")]);
    }

    // -- ClientReply --

    #[test]
    fn client_reply_to_frame() {
        let cmd = ClientReply::new(ClientReplyMode::Skip);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CLIENT"), bulk("REPLY"), bulk("SKIP")])
        );
    }

    // -- ClientTrackingInfo --

    #[test]
    fn client_trackinginfo_to_frame() {
        let cmd = ClientTrackingInfo::new();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CLIENT"), bulk("TRACKINGINFO")])
        );
    }

    // -- ClientUnblock --

    #[test]
    fn client_unblock_to_frame() {
        let cmd = ClientUnblock::new(42).mode(UnblockMode::Error);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("CLIENT"),
                bulk("UNBLOCK"),
                bulk("42"),
                bulk("ERROR"),
            ])
        );
    }

    #[test]
    fn client_unblock_parse_integer() {
        let cmd = ClientUnblock::new(42);
        assert_eq!(cmd.parse_response(Frame::Integer(1)).unwrap(), 1);
    }

    // -- ClientCaching --

    #[test]
    fn client_caching_to_frame() {
        let cmd = ClientCaching::new(true);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CLIENT"), bulk("CACHING"), bulk("yes")])
        );
    }

    #[test]
    fn client_caching_no_to_frame() {
        let cmd = ClientCaching::new(false);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CLIENT"), bulk("CACHING"), bulk("no")])
        );
    }
}
