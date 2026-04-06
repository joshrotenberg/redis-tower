use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// PING \[message\]
///
/// Returns PONG, or echoes the message if provided.
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
pub struct FlushDb {
    mode: Option<FlushMode>,
}

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
            Frame::BulkString(Some(s)) => Ok(String::from_utf8_lossy(&s).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
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
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
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
pub struct CommandList {
    filter: Option<CommandListFilter>,
}

/// Filter for the COMMAND LIST command.
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
