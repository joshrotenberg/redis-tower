use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

fn parse_ok_response(frame: Frame) -> Result<(), RedisError> {
    match frame {
        Frame::SimpleString(value) if value.eq_ignore_ascii_case(b"OK") => Ok(()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "OK",
            actual: format!("{other:?}"),
        }),
    }
}

/// PING \[message\]
///
/// Returns PONG, or echoes the message if provided.
#[derive(Clone)]
pub struct Ping {
    message: Option<String>,
}

impl Ping {
    /// Create a new [`Ping`] command.
    pub fn new() -> Self {
        Self { message: None }
    }

    /// Create the [`Ping`] command using the `with_message` form.
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

/// MONITOR
///
/// Switches a connection into Redis's continuous command-monitoring mode.
/// This command never returns the connection to normal request/response use:
/// execute it only on a fresh, dedicated [`RedisConnection`](redis_tower_core::RedisConnection).
/// Applications using the `redis-tower` facade should prefer its
/// `MonitorStream`, which owns the dedicated connection and decodes events.
/// Executing `Monitor` through a shared or multiplexed client commandeers that
/// transport and will stall or corrupt unrelated command traffic.
#[derive(Debug, Clone)]
pub struct Monitor;

impl Monitor {
    /// Create a `MONITOR` request for a fresh dedicated connection.
    pub fn new() -> Self {
        Self
    }
}

impl Default for Monitor {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Monitor {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("MONITOR")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_ok_response(frame)
    }

    fn name(&self) -> &str {
        "MONITOR"
    }

    fn is_blocking(&self) -> bool {
        true
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
/// Execution mode for this Redis command.
pub enum FlushMode {
    /// Select the `Async` mode.
    Async,
    /// Select the `Sync` mode.
    Sync,
}

impl FlushDb {
    /// Create a new [`FlushDb`] command.
    pub fn new() -> Self {
        Self { mode: None }
    }

    /// Configure the `async_mode` option.
    pub fn async_mode(mut self) -> Self {
        self.mode = Some(FlushMode::Async);
        self
    }

    /// Configure the `sync_mode` option.
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
    /// Create a new [`DbSize`] command.
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
    /// Create a new [`Select`] command.
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

/// CLIENT TRACKING ON|OFF \[REDIRECT client-id\] \[PREFIX prefix\] \[BCAST\] \[OPTIN\] \[OPTOUT\] \[NOLOOP\]
///
/// Enable or disable server-assisted client-side caching.
#[derive(Clone)]
pub struct ClientTracking {
    enabled: bool,
    redirect: Option<i64>,
    bcast: bool,
    prefixes: Vec<Bytes>,
    optin: bool,
    optout: bool,
    noloop: bool,
}

impl ClientTracking {
    /// Enable client tracking.
    pub fn on() -> Self {
        Self {
            enabled: true,
            redirect: None,
            bcast: false,
            prefixes: Vec::new(),
            optin: false,
            optout: false,
            noloop: false,
        }
    }

    /// Disable client tracking.
    pub fn off() -> Self {
        Self {
            enabled: false,
            redirect: None,
            bcast: false,
            prefixes: Vec::new(),
            optin: false,
            optout: false,
            noloop: false,
        }
    }

    /// Redirect invalidation messages to another connection's client ID.
    ///
    /// The ID can be obtained with [`ClientId`]. Redirecting is useful when a
    /// dedicated RESP3 connection owns the invalidation push stream while a
    /// separate connection executes data commands.
    pub fn redirect(mut self, client_id: i64) -> Self {
        self.redirect = Some(client_id);
        self
    }

    /// Enable broadcasting mode (invalidate all keys matching prefixes).
    pub fn bcast(mut self) -> Self {
        self.bcast = true;
        self.optin = false;
        self.optout = false;
        self
    }

    /// Add a binary-safe key prefix to track and enable BCAST mode.
    pub fn prefix(mut self, prefix: impl AsRef<[u8]>) -> Self {
        self.bcast = true;
        self.optin = false;
        self.optout = false;
        self.prefixes.push(Bytes::copy_from_slice(prefix.as_ref()));
        self
    }

    /// Enable opt-in mode (only track keys after CLIENT CACHING YES).
    pub fn optin(mut self) -> Self {
        self.bcast = false;
        self.prefixes.clear();
        self.optin = true;
        self.optout = false;
        self
    }

    /// Enable opt-out mode (track all keys, skip after CLIENT CACHING NO).
    pub fn optout(mut self) -> Self {
        self.bcast = false;
        self.prefixes.clear();
        self.optin = false;
        self.optout = true;
        self
    }

    /// Suppress invalidations caused by writes on the tracked connection.
    ///
    /// Cached clients must pair this with synchronous local write invalidation
    /// so their own writes cannot leave stale entries behind.
    pub fn noloop(mut self) -> Self {
        self.noloop = true;
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
        // Redis accepts no options with OFF. Omitting any options that were
        // added to an OFF builder keeps the serialized command valid.
        if self.enabled {
            if let Some(client_id) = self.redirect {
                args.push(bulk("REDIRECT"));
                args.push(bulk(client_id.to_string()));
            }
            if self.bcast {
                args.push(bulk("BCAST"));
            }
            for prefix in &self.prefixes {
                args.push(bulk("PREFIX"));
                args.push(bulk(prefix));
            }
            if self.optin {
                args.push(bulk("OPTIN"));
            }
            if self.optout {
                args.push(bulk("OPTOUT"));
            }
            if self.noloop {
                args.push(bulk("NOLOOP"));
            }
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
    /// Create a new [`Time`] command.
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

/// COMMAND
///
/// Returns detailed metadata for every command known to the server. The
/// response is preserved as a raw frame because its deeply nested shape and
/// aggregate types differ between RESP2 and RESP3.
#[derive(Debug, Clone)]
pub struct CommandOverview;

impl CommandOverview {
    /// Create a root `COMMAND` request.
    pub fn new() -> Self {
        Self
    }
}

impl Default for CommandOverview {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for CommandOverview {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("COMMAND")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            frame @ (Frame::Array(Some(_)) | Frame::Map(_)) => Ok(frame),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or map",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "COMMAND"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// COMMAND COUNT
///
/// Returns the total number of commands supported by the server.
#[derive(Clone)]
pub struct CommandCount;

impl CommandCount {
    /// Create a new [`CommandCount`] command.
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

/// MODULE LIST
///
/// Returns information about the modules loaded into the server. The reply is
/// an array of per-module maps (`name`, `ver`, `path`, `args`) and is returned
/// as a raw [`Frame`] because its shape differs between RESP2 (flat arrays) and
/// RESP3 (maps).
///
/// # Example
///
/// ```rust,no_run
/// use redis_tower_commands::ModuleList;
///
/// let cmd = ModuleList::new();
/// ```
#[derive(Clone)]
pub struct ModuleList;

impl ModuleList {
    /// Create a `MODULE LIST` command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for ModuleList {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ModuleList {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("MODULE"), bulk("LIST")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "MODULE LIST"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// MODULE LOAD path \[arg \[arg ...\]\]
///
/// Loads a Redis module and passes the optional binary-safe arguments to its
/// initialization function.
#[derive(Debug, Clone)]
pub struct ModuleLoad {
    path: Bytes,
    args: Vec<Bytes>,
}

impl ModuleLoad {
    /// Create a module-load request for `path`.
    pub fn new(path: impl AsRef<[u8]>) -> Self {
        Self {
            path: Bytes::copy_from_slice(path.as_ref()),
            args: Vec::new(),
        }
    }

    /// Append one module initialization argument.
    pub fn arg(mut self, argument: impl AsRef<[u8]>) -> Self {
        self.args.push(Bytes::copy_from_slice(argument.as_ref()));
        self
    }

    /// Append multiple module initialization arguments.
    pub fn args<A, I>(mut self, arguments: I) -> Self
    where
        A: AsRef<[u8]>,
        I: IntoIterator<Item = A>,
    {
        self.args.extend(
            arguments
                .into_iter()
                .map(|argument| Bytes::copy_from_slice(argument.as_ref())),
        );
        self
    }
}

impl Command for ModuleLoad {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("MODULE"), bulk("LOAD"), bulk(&self.path)];
        args.extend(self.args.iter().map(bulk));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_ok_response(frame)
    }

    fn name(&self) -> &str {
        "MODULE LOAD"
    }
}

/// MODULE LOADEX path \[CONFIG name value \[CONFIG name value ...\]\]
/// \[ARGS arg \[arg ...\]\]
///
/// Loads a Redis module with optional module configuration values and binary-
/// safe initialization arguments.
#[derive(Debug, Clone)]
pub struct ModuleLoadEx {
    path: Bytes,
    configs: Vec<(Bytes, Bytes)>,
    args: Vec<Bytes>,
}

impl ModuleLoadEx {
    /// Create an extended module-load request for `path`.
    pub fn new(path: impl AsRef<[u8]>) -> Self {
        Self {
            path: Bytes::copy_from_slice(path.as_ref()),
            configs: Vec::new(),
            args: Vec::new(),
        }
    }

    /// Add one module configuration name/value pair.
    pub fn config(mut self, name: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Self {
        self.configs.push((
            Bytes::copy_from_slice(name.as_ref()),
            Bytes::copy_from_slice(value.as_ref()),
        ));
        self
    }

    /// Append one module initialization argument after the `ARGS` token.
    pub fn arg(mut self, argument: impl AsRef<[u8]>) -> Self {
        self.args.push(Bytes::copy_from_slice(argument.as_ref()));
        self
    }

    /// Append multiple module initialization arguments after the `ARGS` token.
    pub fn args<A, I>(mut self, arguments: I) -> Self
    where
        A: AsRef<[u8]>,
        I: IntoIterator<Item = A>,
    {
        self.args.extend(
            arguments
                .into_iter()
                .map(|argument| Bytes::copy_from_slice(argument.as_ref())),
        );
        self
    }
}

impl Command for ModuleLoadEx {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("MODULE"), bulk("LOADEX"), bulk(&self.path)];
        for (name, value) in &self.configs {
            args.push(bulk("CONFIG"));
            args.push(bulk(name));
            args.push(bulk(value));
        }
        if !self.args.is_empty() {
            args.push(bulk("ARGS"));
            args.extend(self.args.iter().map(bulk));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_ok_response(frame)
    }

    fn name(&self) -> &str {
        "MODULE LOADEX"
    }
}

/// MODULE UNLOAD name
///
/// Unloads a module by its registered name.
#[derive(Debug, Clone)]
pub struct ModuleUnload {
    name: Bytes,
}

impl ModuleUnload {
    /// Create a module-unload request.
    pub fn new(name: impl AsRef<[u8]>) -> Self {
        Self {
            name: Bytes::copy_from_slice(name.as_ref()),
        }
    }
}

impl Command for ModuleUnload {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("MODULE"), bulk("UNLOAD"), bulk(&self.name)])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_ok_response(frame)
    }

    fn name(&self) -> &str {
        "MODULE UNLOAD"
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
    /// Create a new [`BgSave`] command.
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
    /// Create a new [`BgRewriteAof`] command.
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
    /// Create a new [`LastSave`] command.
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
    /// Create a new [`SwapDb`] command.
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
    /// Create a new [`Wait`] command.
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
    /// Create a new [`WaitAof`] command.
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
    /// Create a new [`ClientId`] command.
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
    /// Create a new [`ClientGetName`] command.
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
    /// Create a new [`ClientSetName`] command.
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
    /// Select the `Normal` mode.
    Normal,
    /// Select the `Master` mode.
    Master,
    /// Select the `Replica` mode.
    Replica,
    /// Select the `Pubsub` mode.
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
    /// Create a new [`ClientList`] command.
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
    /// Create a new [`ClientKill`] command.
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
    /// Create a new [`ClientInfo`] command.
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
    /// Create a new [`ClientNoEvict`] command.
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
    /// Create a new [`ClientNoTouch`] command.
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
    /// Create a new [`ClientPause`] command.
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
    /// Create a new [`ClientUnpause`] command.
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
    /// Create a new [`ConfigGet`] command.
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
    /// Create a new [`ConfigResetStat`] command.
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
    /// Create a new [`ConfigRewrite`] command.
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
    /// Create a new [`ClientSetInfoLibName`] command.
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
    /// Create a new [`ClientSetInfoLibVer`] command.
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
    /// Create a new [`Echo`] command.
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
    /// Create a new [`FlushAll`] command.
    pub fn new() -> Self {
        Self { mode: None }
    }

    /// Configure the `async_mode` option.
    pub fn async_mode(mut self) -> Self {
        self.mode = Some(FlushMode::Async);
        self
    }

    /// Configure the `sync_mode` option.
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
    /// Create a new [`Save`] command.
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
    /// Create a new [`Shutdown`] command.
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
    /// Create a new [`Role`] command.
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
    /// Create a new [`Hello`] command.
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
    /// Create a new [`Reset`] command.
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
    /// Create a new [`CommandInfo`] command.
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
    /// Create a new [`CommandGetKeys`] command.
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

/// One key and its access flags returned by [`CommandGetKeysAndFlags`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandKeyFlags {
    /// The binary-safe key extracted from the inspected command invocation.
    pub key: Bytes,
    /// Redis key-spec flags such as `RW`, `access`, `update`, or `delete`.
    pub flags: Vec<Bytes>,
}

/// COMMAND GETKEYSANDFLAGS command \[arg \[arg ...\]\]
///
/// Asks Redis to extract both keys and key-access flags from an arbitrary
/// command invocation.
#[derive(Debug, Clone)]
pub struct CommandGetKeysAndFlags {
    command: Bytes,
    args: Vec<Bytes>,
}

impl CommandGetKeysAndFlags {
    /// Create an inspection request for `command`.
    pub fn new(command: impl AsRef<[u8]>) -> Self {
        Self {
            command: Bytes::copy_from_slice(command.as_ref()),
            args: Vec::new(),
        }
    }

    /// Append one binary-safe argument from the inspected invocation.
    pub fn arg(mut self, argument: impl AsRef<[u8]>) -> Self {
        self.args.push(Bytes::copy_from_slice(argument.as_ref()));
        self
    }

    /// Append multiple binary-safe arguments from the inspected invocation.
    pub fn args<A, I>(mut self, arguments: I) -> Self
    where
        A: AsRef<[u8]>,
        I: IntoIterator<Item = A>,
    {
        self.args.extend(
            arguments
                .into_iter()
                .map(|argument| Bytes::copy_from_slice(argument.as_ref())),
        );
        self
    }

    fn parse_bytes(frame: Frame) -> Result<Bytes, RedisError> {
        match frame {
            Frame::BulkString(Some(value)) | Frame::SimpleString(value) => Ok(value),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk or simple string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn parse_flags(frame: Frame) -> Result<Vec<Bytes>, RedisError> {
        let flags = match frame {
            Frame::Array(Some(flags)) | Frame::Set(flags) => flags,
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "array or set of flags",
                    actual: format!("{other:?}"),
                });
            }
        };
        flags.into_iter().map(Self::parse_bytes).collect()
    }
}

impl Command for CommandGetKeysAndFlags {
    type Response = Vec<CommandKeyFlags>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("COMMAND"),
            bulk("GETKEYSANDFLAGS"),
            bulk(&self.command),
        ];
        args.extend(self.args.iter().map(bulk));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        let entries = match frame {
            Frame::Array(Some(entries)) => entries,
            Frame::Array(None) => return Ok(Vec::new()),
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "array of key/flag pairs",
                    actual: format!("{other:?}"),
                });
            }
        };

        entries
            .into_iter()
            .map(|entry| {
                let mut fields = match entry {
                    Frame::Array(Some(fields)) if fields.len() == 2 => fields.into_iter(),
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "two-element key/flag array",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                let key = Self::parse_bytes(fields.next().expect("length checked"))?;
                let flags = Self::parse_flags(fields.next().expect("length checked"))?;
                Ok(CommandKeyFlags { key, flags })
            })
            .collect()
    }

    fn name(&self) -> &str {
        "COMMAND GETKEYSANDFLAGS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// Reply mode for CLIENT REPLY.
#[derive(Clone)]
pub enum ClientReplyMode {
    /// Select the `On` mode.
    On,
    /// Select the `Off` mode.
    Off,
    /// Select the `Skip` mode.
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
    /// Create a new [`ClientReply`] command.
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
    /// Create a new [`ClientTrackingInfo`] command.
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
    /// Select the `Timeout` mode.
    Timeout,
    /// Select the `Error` mode.
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
    /// Create a new [`ClientUnblock`] command.
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
    /// Create a new [`ClientCaching`] command.
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

/// CLIENT GETREDIR
///
/// Returns the client ID this connection's client-side-caching invalidation
/// messages are redirected to. Returns `-1` when tracking is disabled and `0`
/// when no redirection is set.
#[derive(Clone)]
pub struct ClientGetRedir;

impl ClientGetRedir {
    /// Create a new [`ClientGetRedir`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClientGetRedir {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClientGetRedir {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CLIENT"), bulk("GETREDIR")])
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
        "CLIENT GETREDIR"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// Metrics collected by [`HotkeysStart`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HotkeysMetrics {
    /// Track hotkeys by CPU time.
    Cpu,
    /// Track hotkeys by network bytes.
    Net,
    /// Track hotkeys by both CPU time and network bytes.
    CpuAndNet,
}

/// HOTKEYS START METRICS count \[CPU\] \[NET\] \[COUNT k\]
/// \[DURATION seconds\] \[SAMPLE ratio\] \[SLOTS count slot \[slot ...\]\]
///
/// Starts a Redis 8.6+ hotkey tracking session. The metrics count and optional
/// slots count are calculated automatically from the typed options.
#[derive(Clone)]
pub struct HotkeysStart {
    metrics: HotkeysMetrics,
    count: Option<u8>,
    duration: Option<u32>,
    sample: Option<u32>,
    slots: Option<Vec<u16>>,
}

impl HotkeysStart {
    /// Create a hotkey tracking command for the selected metrics.
    pub fn new(metrics: HotkeysMetrics) -> Self {
        Self {
            metrics,
            count: None,
            duration: None,
            sample: None,
            slots: None,
        }
    }

    /// Set the maximum number of hotkeys returned for each metric.
    ///
    /// Redis accepts values from 1 through 64 and defaults to 10.
    pub fn count(mut self, count: u8) -> Self {
        self.count = Some(count);
        self
    }

    /// Stop tracking automatically after this many seconds.
    ///
    /// Redis accepts values from 1 through 1,000,000. When omitted, tracking
    /// continues until [`HotkeysStop`] is executed.
    pub fn duration(mut self, seconds: u32) -> Self {
        self.duration = Some(seconds);
        self
    }

    /// Sample each key with probability `1 / ratio`.
    ///
    /// Redis requires a positive ratio and defaults to 1 (every key).
    pub fn sample(mut self, ratio: u32) -> Self {
        self.sample = Some(ratio);
        self
    }

    /// Restrict tracking to the supplied cluster hash slots.
    ///
    /// Redis only accepts this option in cluster mode. The slot count is
    /// calculated automatically.
    pub fn slots(mut self, slots: impl IntoIterator<Item = u16>) -> Self {
        self.slots = Some(slots.into_iter().collect());
        self
    }
}

impl Command for HotkeysStart {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("HOTKEYS"), bulk("START"), bulk("METRICS")];
        match self.metrics {
            HotkeysMetrics::Cpu => {
                args.push(bulk("1"));
                args.push(bulk("CPU"));
            }
            HotkeysMetrics::Net => {
                args.push(bulk("1"));
                args.push(bulk("NET"));
            }
            HotkeysMetrics::CpuAndNet => {
                args.push(bulk("2"));
                args.push(bulk("CPU"));
                args.push(bulk("NET"));
            }
        }
        if let Some(count) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(count.to_string()));
        }
        if let Some(seconds) = self.duration {
            args.push(bulk("DURATION"));
            args.push(bulk(seconds.to_string()));
        }
        if let Some(ratio) = self.sample {
            args.push(bulk("SAMPLE"));
            args.push(bulk(ratio.to_string()));
        }
        if let Some(ref slots) = self.slots {
            args.push(bulk("SLOTS"));
            args.push(bulk(slots.len().to_string()));
            args.extend(slots.iter().map(|slot| bulk(slot.to_string())));
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
        "HOTKEYS START"
    }
}

/// HOTKEYS STOP
///
/// Stops hotkey tracking while preserving the collected data. Returns `true`
/// when an active session was stopped and `false` when no session was active.
#[derive(Clone)]
pub struct HotkeysStop;

impl HotkeysStop {
    /// Create a new [`HotkeysStop`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for HotkeysStop {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for HotkeysStop {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("HOTKEYS"), bulk("STOP")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(true),
            Frame::Null | Frame::BulkString(None) | Frame::Array(None) => Ok(false),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "HOTKEYS STOP"
    }
}

/// Inclusive Redis Cluster hash-slot range reported by [`HotkeysGet`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HotkeysSlotRange {
    /// First hash slot in the inclusive range.
    pub start: u16,
    /// Last hash slot in the inclusive range.
    pub end: u16,
}

/// A key and its measured CPU time in microseconds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HotkeyCpuTime {
    /// Sampled key.
    pub key: Bytes,
    /// CPU time attributed to the key, in microseconds.
    pub microseconds: i64,
}

/// A key and its measured network traffic in bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HotkeyNetBytes {
    /// Sampled key.
    pub key: Bytes,
    /// Network traffic attributed to the key, in bytes.
    pub bytes: i64,
}

/// Statistics for one node in a [`HotkeysGet`] response.
///
/// Metric-specific and cluster-selection fields are `None` when the
/// corresponding START option was not enabled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HotkeysStats {
    /// Whether the server is currently collecting hot-key samples.
    pub tracking_active: bool,
    /// Configured command-sampling ratio.
    pub sample_ratio: i64,
    /// Cluster hash-slot ranges included in collection.
    pub selected_slots: Vec<HotkeysSlotRange>,
    /// CPU time for sampled commands in selected slots, in microseconds.
    pub sampled_commands_selected_slots_us: Option<i64>,
    /// CPU time for all commands in selected slots, in microseconds.
    pub all_commands_selected_slots_us: Option<i64>,
    /// CPU time for all commands in all slots, in microseconds.
    pub all_commands_all_slots_us: i64,
    /// Network bytes for sampled commands in selected slots.
    pub net_bytes_sampled_commands_selected_slots: Option<i64>,
    /// Network bytes for all commands in selected slots.
    pub net_bytes_all_commands_selected_slots: Option<i64>,
    /// Network bytes for all commands in all slots.
    pub net_bytes_all_commands_all_slots: i64,
    /// Collection start time as Unix epoch milliseconds.
    pub collection_start_time_unix_ms: i64,
    /// Elapsed collection time in milliseconds.
    pub collection_duration_ms: i64,
    /// Total user-mode CPU time, in milliseconds, when requested.
    pub total_cpu_time_user_ms: Option<i64>,
    /// Total system-mode CPU time, in milliseconds, when requested.
    pub total_cpu_time_sys_ms: Option<i64>,
    /// Total network traffic, in bytes, when requested.
    pub total_net_bytes: Option<i64>,
    /// Sampled keys ordered by attributed CPU time, when requested.
    pub by_cpu_time_us: Option<Vec<HotkeyCpuTime>>,
    /// Sampled keys ordered by attributed network traffic, when requested.
    pub by_net_bytes: Option<Vec<HotkeyNetBytes>>,
}

/// HOTKEYS GET
///
/// Returns the current or most recent Redis 8.6+ hotkey tracking results.
/// Redis wraps each node's statistics in an outer array so aggregated servers
/// can return multiple entries. A null reply means no session has been started
/// or the previous data was reset.
#[derive(Clone)]
pub struct HotkeysGet;

impl HotkeysGet {
    /// Create a new [`HotkeysGet`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for HotkeysGet {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for HotkeysGet {
    type Response = Option<Vec<HotkeysStats>>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("HOTKEYS"), bulk("GET")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_hotkeys_results(frame)
    }

    fn name(&self) -> &str {
        "HOTKEYS GET"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// HOTKEYS RESET
///
/// Releases the resources used for hotkey tracking. Redis requires an active
/// session to be stopped first.
#[derive(Clone)]
pub struct HotkeysReset;

impl HotkeysReset {
    /// Create a new [`HotkeysReset`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for HotkeysReset {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for HotkeysReset {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("HOTKEYS"), bulk("RESET")])
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
        "HOTKEYS RESET"
    }
}

/// HOTKEYS HELP
///
/// Returns helpful text describing the Redis 8.6.1+ HOTKEYS subcommands.
#[derive(Clone)]
pub struct HotkeysHelp;

impl HotkeysHelp {
    /// Create a new [`HotkeysHelp`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for HotkeysHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for HotkeysHelp {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("HOTKEYS"), bulk("HELP")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        crate::help::parse_help_lines(frame)
    }

    fn name(&self) -> &str {
        "HOTKEYS HELP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

fn parse_hotkeys_results(frame: Frame) -> Result<Option<Vec<HotkeysStats>>, RedisError> {
    let results = match frame {
        Frame::Null | Frame::BulkString(None) | Frame::Array(None) => return Ok(None),
        Frame::Array(Some(results)) => results,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of HOTKEYS results or null",
                actual: format!("{other:?}"),
            });
        }
    };

    results
        .iter()
        .map(parse_hotkeys_stats)
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn parse_hotkeys_stats(frame: &Frame) -> Result<HotkeysStats, RedisError> {
    let fields = hotkeys_field_pairs(frame)?;

    let mut tracking_active = None;
    let mut sample_ratio = None;
    let mut selected_slots = None;
    let mut sampled_commands_selected_slots_us = None;
    let mut all_commands_selected_slots_us = None;
    let mut all_commands_all_slots_us = None;
    let mut net_bytes_sampled_commands_selected_slots = None;
    let mut net_bytes_all_commands_selected_slots = None;
    let mut net_bytes_all_commands_all_slots = None;
    let mut collection_start_time_unix_ms = None;
    let mut collection_duration_ms = None;
    let mut total_cpu_time_user_ms = None;
    let mut total_cpu_time_sys_ms = None;
    let mut total_net_bytes = None;
    let mut by_cpu_time_us = None;
    let mut by_net_bytes = None;

    for (field, value) in fields {
        let name = hotkeys_bytes(field, "HOTKEYS field name")?;
        match &name[..] {
            b"tracking-active" => {
                tracking_active = Some(match hotkeys_integer(value, "tracking-active integer")? {
                    0 => false,
                    1 => true,
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "tracking-active integer 0 or 1",
                            actual: other.to_string(),
                        });
                    }
                });
            }
            b"sample-ratio" => sample_ratio = Some(hotkeys_integer(value, "sample-ratio integer")?),
            b"selected-slots" => selected_slots = Some(parse_hotkeys_slots(value)?),
            // Redis 8.6.0 used the singular "command"; later patches fixed
            // the response field to match the documented plural spelling.
            b"sampled-command-selected-slots-us" | b"sampled-commands-selected-slots-us" => {
                sampled_commands_selected_slots_us = Some(hotkeys_integer(
                    value,
                    "sampled-commands-selected-slots-us integer",
                )?)
            }
            b"all-commands-selected-slots-us" => {
                all_commands_selected_slots_us = Some(hotkeys_integer(
                    value,
                    "all-commands-selected-slots-us integer",
                )?)
            }
            b"all-commands-all-slots-us" => {
                all_commands_all_slots_us =
                    Some(hotkeys_integer(value, "all-commands-all-slots-us integer")?)
            }
            b"net-bytes-sampled-commands-selected-slots" => {
                net_bytes_sampled_commands_selected_slots = Some(hotkeys_integer(
                    value,
                    "net-bytes-sampled-commands-selected-slots integer",
                )?)
            }
            b"net-bytes-all-commands-selected-slots" => {
                net_bytes_all_commands_selected_slots = Some(hotkeys_integer(
                    value,
                    "net-bytes-all-commands-selected-slots integer",
                )?)
            }
            b"net-bytes-all-commands-all-slots" => {
                net_bytes_all_commands_all_slots = Some(hotkeys_integer(
                    value,
                    "net-bytes-all-commands-all-slots integer",
                )?)
            }
            b"collection-start-time-unix-ms" => {
                collection_start_time_unix_ms = Some(hotkeys_integer(
                    value,
                    "collection-start-time-unix-ms integer",
                )?)
            }
            b"collection-duration-ms" => {
                collection_duration_ms =
                    Some(hotkeys_integer(value, "collection-duration-ms integer")?)
            }
            b"total-cpu-time-user-ms" => {
                total_cpu_time_user_ms =
                    Some(hotkeys_integer(value, "total-cpu-time-user-ms integer")?)
            }
            b"total-cpu-time-sys-ms" => {
                total_cpu_time_sys_ms =
                    Some(hotkeys_integer(value, "total-cpu-time-sys-ms integer")?)
            }
            b"total-net-bytes" => {
                total_net_bytes = Some(hotkeys_integer(value, "total-net-bytes integer")?)
            }
            b"by-cpu-time-us" => {
                by_cpu_time_us = Some(
                    parse_hotkey_measurements(value, "by-cpu-time-us array")?
                        .into_iter()
                        .map(|(key, microseconds)| HotkeyCpuTime { key, microseconds })
                        .collect(),
                )
            }
            b"by-net-bytes" => {
                by_net_bytes = Some(
                    parse_hotkey_measurements(value, "by-net-bytes array")?
                        .into_iter()
                        .map(|(key, bytes)| HotkeyNetBytes { key, bytes })
                        .collect(),
                )
            }
            _ => {}
        }
    }

    Ok(HotkeysStats {
        tracking_active: required_hotkeys_field(tracking_active, "tracking-active")?,
        sample_ratio: required_hotkeys_field(sample_ratio, "sample-ratio")?,
        selected_slots: required_hotkeys_field(selected_slots, "selected-slots")?,
        sampled_commands_selected_slots_us,
        all_commands_selected_slots_us,
        all_commands_all_slots_us: required_hotkeys_field(
            all_commands_all_slots_us,
            "all-commands-all-slots-us",
        )?,
        net_bytes_sampled_commands_selected_slots,
        net_bytes_all_commands_selected_slots,
        net_bytes_all_commands_all_slots: required_hotkeys_field(
            net_bytes_all_commands_all_slots,
            "net-bytes-all-commands-all-slots",
        )?,
        collection_start_time_unix_ms: required_hotkeys_field(
            collection_start_time_unix_ms,
            "collection-start-time-unix-ms",
        )?,
        collection_duration_ms: required_hotkeys_field(
            collection_duration_ms,
            "collection-duration-ms",
        )?,
        total_cpu_time_user_ms,
        total_cpu_time_sys_ms,
        total_net_bytes,
        by_cpu_time_us,
        by_net_bytes,
    })
}

fn hotkeys_field_pairs(frame: &Frame) -> Result<Vec<(&Frame, &Frame)>, RedisError> {
    match frame {
        Frame::Map(pairs) => Ok(pairs.iter().map(|(key, value)| (key, value)).collect()),
        Frame::Array(Some(items)) => {
            if items.len() % 2 != 0 {
                return Err(RedisError::UnexpectedResponse {
                    expected: "even number of HOTKEYS field-value elements",
                    actual: format!("{} elements", items.len()),
                });
            }
            Ok(items
                .as_chunks::<2>()
                .0
                .iter()
                .map(|pair| (&pair[0], &pair[1]))
                .collect())
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "HOTKEYS field-value array or map",
            actual: format!("{other:?}"),
        }),
    }
}

fn parse_hotkeys_slots(frame: &Frame) -> Result<Vec<HotkeysSlotRange>, RedisError> {
    let ranges = match frame {
        Frame::Array(Some(ranges)) => ranges,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of HOTKEYS slot ranges",
                actual: format!("{other:?}"),
            });
        }
    };

    ranges
        .iter()
        .map(|range| {
            let values = match range {
                Frame::Array(Some(values)) if matches!(values.len(), 1 | 2) => values,
                other => {
                    return Err(RedisError::UnexpectedResponse {
                        expected: "one- or two-integer HOTKEYS slot range",
                        actual: format!("{other:?}"),
                    });
                }
            };
            let start = hotkeys_slot(&values[0])?;
            let end = if values.len() == 2 {
                hotkeys_slot(&values[1])?
            } else {
                start
            };
            Ok(HotkeysSlotRange { start, end })
        })
        .collect()
}

fn hotkeys_slot(frame: &Frame) -> Result<u16, RedisError> {
    match frame {
        Frame::Integer(slot) if (0..=16_383).contains(slot) => Ok(*slot as u16),
        other => Err(RedisError::UnexpectedResponse {
            expected: "Redis Cluster slot integer from 0 through 16383",
            actual: format!("{other:?}"),
        }),
    }
}

fn parse_hotkey_measurements(
    frame: &Frame,
    expected: &'static str,
) -> Result<Vec<(Bytes, i64)>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected,
                actual: format!("{other:?}"),
            });
        }
    };
    if items.len() % 2 != 0 {
        return Err(RedisError::UnexpectedResponse {
            expected: "even number of HOTKEYS key-measurement elements",
            actual: format!("{} elements", items.len()),
        });
    }

    items
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            Ok((
                hotkeys_bytes(&pair[0], "HOTKEYS key")?,
                hotkeys_integer(&pair[1], "HOTKEYS measurement integer")?,
            ))
        })
        .collect()
}

fn hotkeys_bytes(frame: &Frame, expected: &'static str) -> Result<Bytes, RedisError> {
    match frame {
        Frame::BulkString(Some(bytes)) | Frame::SimpleString(bytes) => Ok(bytes.clone()),
        other => Err(RedisError::UnexpectedResponse {
            expected,
            actual: format!("{other:?}"),
        }),
    }
}

fn hotkeys_integer(frame: &Frame, expected: &'static str) -> Result<i64, RedisError> {
    match frame {
        Frame::Integer(value) => Ok(*value),
        other => Err(RedisError::UnexpectedResponse {
            expected,
            actual: format!("{other:?}"),
        }),
    }
}

fn required_hotkeys_field<T>(value: Option<T>, field: &'static str) -> Result<T, RedisError> {
    value.ok_or_else(|| RedisError::UnexpectedResponse {
        expected: field,
        actual: "missing HOTKEYS field".to_string(),
    })
}

/// CLIENT HELP
///
/// Returns helpful text describing the CLIENT subcommands.
#[derive(Clone)]
pub struct ClientHelp;

impl ClientHelp {
    /// Create a new [`ClientHelp`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for ClientHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ClientHelp {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CLIENT"), bulk("HELP")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        crate::help::parse_help_lines(frame)
    }

    fn name(&self) -> &str {
        "CLIENT HELP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// COMMAND HELP
///
/// Returns helpful text describing the COMMAND subcommands.
#[derive(Clone)]
pub struct CommandHelp;

impl CommandHelp {
    /// Create a new [`CommandHelp`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for CommandHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for CommandHelp {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("COMMAND"), bulk("HELP")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        crate::help::parse_help_lines(frame)
    }

    fn name(&self) -> &str {
        "COMMAND HELP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// CONFIG HELP
///
/// Returns helpful text describing the CONFIG subcommands.
#[derive(Clone)]
pub struct ConfigHelp;

impl ConfigHelp {
    /// Create a new [`ConfigHelp`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for ConfigHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ConfigHelp {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CONFIG"), bulk("HELP")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        crate::help::parse_help_lines(frame)
    }

    fn name(&self) -> &str {
        "CONFIG HELP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// MODULE HELP
///
/// Returns helpful text describing the MODULE subcommands.
#[derive(Clone)]
pub struct ModuleHelp;

impl ModuleHelp {
    /// Create a new [`ModuleHelp`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for ModuleHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ModuleHelp {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("MODULE"), bulk("HELP")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        crate::help::parse_help_lines(frame)
    }

    fn name(&self) -> &str {
        "MODULE HELP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// LOLWUT \[VERSION version\]
///
/// Returns the Redis version and, depending on the server, a piece of
/// generative art rendered as text. Primarily a fun diagnostic that also
/// reports the server version string.
#[derive(Clone)]
pub struct Lolwut {
    version: Option<u32>,
}

impl Lolwut {
    /// Create a new [`Lolwut`] command.
    pub fn new() -> Self {
        Self { version: None }
    }

    /// Request a specific art version via the `VERSION` argument.
    pub fn version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }
}

impl Default for Lolwut {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Lolwut {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("LOLWUT")];
        if let Some(version) = self.version {
            args.push(bulk("VERSION"));
            args.push(bulk(version.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            // RESP3 returns LOLWUT as a verbatim string.
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
        "LOLWUT"
    }

    fn idempotent(&self) -> bool {
        true
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

    // -- Monitor --

    #[test]
    fn monitor_to_frame_parse_and_metadata() {
        let cmd = Monitor::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("MONITOR")]));
        assert_eq!(cmd.name(), "MONITOR");
        assert!(cmd.is_blocking());
        assert!(!cmd.idempotent());
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
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

    // -- CommandOverview --

    #[test]
    fn command_overview_preserves_resp2_and_resp3_shapes() {
        let cmd = CommandOverview::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("COMMAND")]));
        assert_eq!(cmd.name(), "COMMAND");
        assert!(cmd.idempotent());
        let resp2 = array(vec![array(vec![bulk("get")])]);
        assert_eq!(cmd.parse_response(resp2.clone()).unwrap(), resp2);
        let resp3 = Frame::Map(vec![(bulk("get"), array(vec![]))]);
        assert_eq!(cmd.parse_response(resp3.clone()).unwrap(), resp3);
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
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

    #[test]
    fn client_tracking_redirect_noloop_to_frame() {
        let cmd = ClientTracking::on().redirect(42).noloop();
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("CLIENT"),
                bulk("TRACKING"),
                bulk("ON"),
                bulk("REDIRECT"),
                bulk("42"),
                bulk("NOLOOP"),
            ])
        );
    }

    #[test]
    fn client_tracking_prefix_is_binary_safe() {
        let prefix = [0, 0xff, b':'];
        let cmd = ClientTracking::on().prefix(prefix);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("CLIENT"),
                bulk("TRACKING"),
                bulk("ON"),
                bulk("BCAST"),
                bulk("PREFIX"),
                bulk(prefix),
            ])
        );
    }

    #[test]
    fn client_tracking_mode_builders_keep_modes_compatible() {
        let optin = ClientTracking::on().bcast().prefix("ignored:").optin();
        assert_eq!(
            optin.to_frame(),
            array(vec![
                bulk("CLIENT"),
                bulk("TRACKING"),
                bulk("ON"),
                bulk("OPTIN"),
            ])
        );

        let bcast = ClientTracking::on().optout().prefix("active:");
        assert_eq!(
            bcast.to_frame(),
            array(vec![
                bulk("CLIENT"),
                bulk("TRACKING"),
                bulk("ON"),
                bulk("BCAST"),
                bulk("PREFIX"),
                bulk("active:"),
            ])
        );
    }

    #[test]
    fn client_tracking_off_omits_incompatible_options() {
        let cmd = ClientTracking::off()
            .redirect(42)
            .noloop()
            .prefix("unused:");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CLIENT"), bulk("TRACKING"), bulk("OFF")])
        );
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

    // -- CommandGetKeysAndFlags --

    #[test]
    fn command_getkeysandflags_serializes_binary_safe_arguments() {
        let cmd = CommandGetKeysAndFlags::new("MSET").arg(b"key\0one").args([
            b"value-one".as_slice(),
            b"key-two".as_slice(),
            b"value-two".as_slice(),
        ]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("COMMAND"),
                bulk("GETKEYSANDFLAGS"),
                bulk("MSET"),
                bulk(b"key\0one"),
                bulk("value-one"),
                bulk("key-two"),
                bulk("value-two"),
            ])
        );
        assert_eq!(cmd.name(), "COMMAND GETKEYSANDFLAGS");
        assert!(cmd.idempotent());
    }

    #[test]
    fn command_getkeysandflags_parses_array_and_resp3_set_flags() {
        let cmd = CommandGetKeysAndFlags::new("MSET");
        let response = array(vec![
            array(vec![
                bulk(b"key\0one"),
                array(vec![bulk("RW"), bulk("access"), bulk("update")]),
            ]),
            array(vec![
                bulk("key-two"),
                Frame::Set(vec![bulk("RW"), bulk("access")]),
            ]),
        ]);
        assert_eq!(
            cmd.parse_response(response).unwrap(),
            vec![
                CommandKeyFlags {
                    key: Bytes::from_static(b"key\0one"),
                    flags: vec![
                        Bytes::from("RW"),
                        Bytes::from("access"),
                        Bytes::from("update")
                    ],
                },
                CommandKeyFlags {
                    key: Bytes::from("key-two"),
                    flags: vec![Bytes::from("RW"), Bytes::from("access")],
                },
            ]
        );
        assert_eq!(
            cmd.parse_response(Frame::Array(None)).unwrap(),
            Vec::<CommandKeyFlags>::new()
        );
    }

    #[test]
    fn command_getkeysandflags_rejects_malformed_entries() {
        let cmd = CommandGetKeysAndFlags::new("GET");
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
        assert!(
            cmd.parse_response(array(vec![array(vec![bulk("only-key")])]))
                .is_err()
        );
        assert!(
            cmd.parse_response(array(vec![array(vec![bulk("key"), Frame::Integer(1)])]))
                .is_err()
        );
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

    // -- ModuleList --

    #[test]
    fn module_list_to_frame() {
        let cmd = ModuleList::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("MODULE"), bulk("LIST")]));
        assert!(cmd.idempotent());
    }

    #[test]
    fn module_list_parse_passthrough() {
        let cmd = ModuleList::new();
        let reply = array(vec![array(vec![bulk("name"), bulk("ReJSON")])]);
        assert_eq!(cmd.parse_response(reply.clone()).unwrap(), reply);
    }

    #[test]
    fn module_load_serializes_binary_safe_arguments_and_parses_ok() {
        let cmd = ModuleLoad::new(b"/tmp/module\0name.so")
            .arg(b"first\0arg")
            .args([b"second".as_slice(), b"third".as_slice()]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("MODULE"),
                bulk("LOAD"),
                bulk(b"/tmp/module\0name.so"),
                bulk(b"first\0arg"),
                bulk("second"),
                bulk("third"),
            ])
        );
        assert_eq!(cmd.name(), "MODULE LOAD");
        assert!(!cmd.idempotent());
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    #[test]
    fn module_loadex_orders_configs_before_args() {
        let cmd = ModuleLoadEx::new("/tmp/module.so")
            .config(b"setting\0name", b"setting\0value")
            .config("threads", "4")
            .arg(b"arg\0one")
            .args([b"arg-two".as_slice()]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("MODULE"),
                bulk("LOADEX"),
                bulk("/tmp/module.so"),
                bulk("CONFIG"),
                bulk(b"setting\0name"),
                bulk(b"setting\0value"),
                bulk("CONFIG"),
                bulk("threads"),
                bulk("4"),
                bulk("ARGS"),
                bulk(b"arg\0one"),
                bulk("arg-two"),
            ])
        );
        assert_eq!(cmd.name(), "MODULE LOADEX");
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
    }

    #[test]
    fn module_unload_serializes_binary_safe_name_and_parses_ok() {
        let cmd = ModuleUnload::new(b"module\0name");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("MODULE"), bulk("UNLOAD"), bulk(b"module\0name")])
        );
        assert_eq!(cmd.name(), "MODULE UNLOAD");
        assert!(!cmd.idempotent());
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
        assert!(cmd.parse_response(Frame::Null).is_err());
    }

    // -- HOTKEYS --

    #[test]
    fn hotkeys_start_cpu_to_frame() {
        let cmd = HotkeysStart::new(HotkeysMetrics::Cpu);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("HOTKEYS"),
                bulk("START"),
                bulk("METRICS"),
                bulk("1"),
                bulk("CPU"),
            ])
        );
        assert_eq!(cmd.name(), "HOTKEYS START");
    }

    #[test]
    fn hotkeys_start_net_to_frame() {
        let cmd = HotkeysStart::new(HotkeysMetrics::Net);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("HOTKEYS"),
                bulk("START"),
                bulk("METRICS"),
                bulk("1"),
                bulk("NET"),
            ])
        );
    }

    #[test]
    fn hotkeys_start_all_options_to_frame() {
        let cmd = HotkeysStart::new(HotkeysMetrics::CpuAndNet)
            .count(25)
            .duration(60)
            .sample(10)
            .slots([3, 7, 8]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("HOTKEYS"),
                bulk("START"),
                bulk("METRICS"),
                bulk("2"),
                bulk("CPU"),
                bulk("NET"),
                bulk("COUNT"),
                bulk("25"),
                bulk("DURATION"),
                bulk("60"),
                bulk("SAMPLE"),
                bulk("10"),
                bulk("SLOTS"),
                bulk("3"),
                bulk("3"),
                bulk("7"),
                bulk("8"),
            ])
        );
    }

    #[test]
    fn hotkeys_start_parse_ok_and_rejects_other_frames() {
        let cmd = HotkeysStart::new(HotkeysMetrics::Cpu);
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    #[test]
    fn hotkeys_stop_to_frame_and_parse_status() {
        let cmd = HotkeysStop::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("HOTKEYS"), bulk("STOP")]));
        assert_eq!(cmd.name(), "HOTKEYS STOP");
        assert!(
            cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
                .unwrap()
        );
        assert!(!cmd.parse_response(Frame::Null).unwrap());
        assert!(!cmd.parse_response(Frame::BulkString(None)).unwrap());
        assert!(cmd.parse_response(Frame::Integer(0)).is_err());
    }

    #[test]
    fn hotkeys_reset_to_frame_and_parse_ok() {
        let cmd = HotkeysReset::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("HOTKEYS"), bulk("RESET")]));
        assert_eq!(cmd.name(), "HOTKEYS RESET");
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
        assert!(cmd.parse_response(Frame::Null).is_err());
    }

    #[test]
    fn hotkeys_get_to_frame_and_parse_null() {
        let cmd = HotkeysGet::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("HOTKEYS"), bulk("GET")]));
        assert_eq!(cmd.name(), "HOTKEYS GET");
        assert!(cmd.idempotent());
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
        assert_eq!(cmd.parse_response(Frame::BulkString(None)).unwrap(), None);
    }

    #[test]
    fn hotkeys_get_parse_resp2_full_result() {
        let cmd = HotkeysGet::new();
        let reply = array(vec![array(vec![
            bulk("tracking-active"),
            Frame::Integer(0),
            bulk("sample-ratio"),
            Frame::Integer(10),
            bulk("selected-slots"),
            array(vec![
                array(vec![Frame::Integer(0), Frame::Integer(3)]),
                array(vec![Frame::Integer(7)]),
            ]),
            // Compatibility spelling emitted by Redis 8.6.0.
            bulk("sampled-command-selected-slots-us"),
            Frame::Integer(11),
            bulk("all-commands-selected-slots-us"),
            Frame::Integer(22),
            bulk("all-commands-all-slots-us"),
            Frame::Integer(33),
            bulk("net-bytes-sampled-commands-selected-slots"),
            Frame::Integer(44),
            bulk("net-bytes-all-commands-selected-slots"),
            Frame::Integer(55),
            bulk("net-bytes-all-commands-all-slots"),
            Frame::Integer(66),
            bulk("collection-start-time-unix-ms"),
            Frame::Integer(1_700_000_000_000),
            bulk("collection-duration-ms"),
            Frame::Integer(5_000),
            bulk("total-cpu-time-user-ms"),
            Frame::Integer(77),
            bulk("total-cpu-time-sys-ms"),
            Frame::Integer(88),
            bulk("total-net-bytes"),
            Frame::Integer(99),
            bulk("by-cpu-time-us"),
            array(vec![
                bulk("hot:key:1"),
                Frame::Integer(101),
                bulk("hot:key:2"),
                Frame::Integer(51),
            ]),
            bulk("by-net-bytes"),
            array(vec![bulk("hot:key:1"), Frame::Integer(2_048)]),
        ])]);

        assert_eq!(
            cmd.parse_response(reply).unwrap(),
            Some(vec![HotkeysStats {
                tracking_active: false,
                sample_ratio: 10,
                selected_slots: vec![
                    HotkeysSlotRange { start: 0, end: 3 },
                    HotkeysSlotRange { start: 7, end: 7 },
                ],
                sampled_commands_selected_slots_us: Some(11),
                all_commands_selected_slots_us: Some(22),
                all_commands_all_slots_us: 33,
                net_bytes_sampled_commands_selected_slots: Some(44),
                net_bytes_all_commands_selected_slots: Some(55),
                net_bytes_all_commands_all_slots: 66,
                collection_start_time_unix_ms: 1_700_000_000_000,
                collection_duration_ms: 5_000,
                total_cpu_time_user_ms: Some(77),
                total_cpu_time_sys_ms: Some(88),
                total_net_bytes: Some(99),
                by_cpu_time_us: Some(vec![
                    HotkeyCpuTime {
                        key: Bytes::from("hot:key:1"),
                        microseconds: 101,
                    },
                    HotkeyCpuTime {
                        key: Bytes::from("hot:key:2"),
                        microseconds: 51,
                    },
                ]),
                by_net_bytes: Some(vec![HotkeyNetBytes {
                    key: Bytes::from("hot:key:1"),
                    bytes: 2_048,
                }]),
            }])
        );
    }

    #[test]
    fn hotkeys_get_parse_resp3_map() {
        let cmd = HotkeysGet::new();
        let reply = array(vec![Frame::Map(vec![
            (bulk("tracking-active"), Frame::Integer(1)),
            (bulk("sample-ratio"), Frame::Integer(1)),
            (
                bulk("selected-slots"),
                array(vec![array(vec![Frame::Integer(0), Frame::Integer(16_383)])]),
            ),
            (bulk("all-commands-all-slots-us"), Frame::Integer(103)),
            (
                bulk("net-bytes-all-commands-all-slots"),
                Frame::Integer(2_042),
            ),
            (
                bulk("collection-start-time-unix-ms"),
                Frame::Integer(1_770_824_933_147),
            ),
            (bulk("collection-duration-ms"), Frame::Integer(250)),
            (bulk("total-cpu-time-user-ms"), Frame::Integer(23)),
            (bulk("total-cpu-time-sys-ms"), Frame::Integer(7)),
            (
                bulk("by-cpu-time-us"),
                array(vec![bulk("counter"), Frame::Integer(29)]),
            ),
            // Unknown fields are ignored for compatibility with future Redis
            // additions.
            (bulk("future-field"), Frame::Integer(1)),
        ])]);

        let stats = cmd.parse_response(reply).unwrap().unwrap().remove(0);
        assert!(stats.tracking_active);
        assert_eq!(stats.sample_ratio, 1);
        assert_eq!(
            stats.selected_slots,
            vec![HotkeysSlotRange {
                start: 0,
                end: 16_383,
            }]
        );
        assert_eq!(stats.all_commands_all_slots_us, 103);
        assert_eq!(stats.net_bytes_all_commands_all_slots, 2_042);
        assert_eq!(stats.total_cpu_time_user_ms, Some(23));
        assert_eq!(
            stats.by_cpu_time_us,
            Some(vec![HotkeyCpuTime {
                key: Bytes::from("counter"),
                microseconds: 29,
            }])
        );
        assert_eq!(stats.total_net_bytes, None);
        assert_eq!(stats.by_net_bytes, None);
    }

    #[test]
    fn hotkeys_get_parse_multiple_node_results() {
        fn node(active: i64, start: i64) -> Frame {
            Frame::Map(vec![
                (bulk("tracking-active"), Frame::Integer(active)),
                (bulk("sample-ratio"), Frame::Integer(1)),
                (bulk("selected-slots"), array(vec![])),
                (bulk("all-commands-all-slots-us"), Frame::Integer(0)),
                (bulk("net-bytes-all-commands-all-slots"), Frame::Integer(0)),
                (bulk("collection-start-time-unix-ms"), Frame::Integer(start)),
                (bulk("collection-duration-ms"), Frame::Integer(0)),
            ])
        }

        let results = HotkeysGet::new()
            .parse_response(array(vec![node(1, 100), node(0, 200)]))
            .unwrap()
            .unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].tracking_active);
        assert!(!results[1].tracking_active);
    }

    #[test]
    fn hotkeys_get_rejects_malformed_responses() {
        let cmd = HotkeysGet::new();
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
        assert!(
            cmd.parse_response(array(vec![array(vec![
                bulk("tracking-active"),
                Frame::Integer(1),
                bulk("sample-ratio"),
            ])]))
            .is_err()
        );
        assert!(
            cmd.parse_response(array(vec![Frame::Map(vec![
                (bulk("tracking-active"), Frame::Integer(2)),
                (bulk("sample-ratio"), Frame::Integer(1)),
            ])]))
            .is_err()
        );
        assert!(
            cmd.parse_response(array(vec![Frame::Map(vec![
                (bulk("tracking-active"), Frame::Integer(1)),
                (bulk("sample-ratio"), Frame::Integer(1)),
                (
                    bulk("selected-slots"),
                    array(vec![array(vec![Frame::Integer(16_384)])]),
                ),
            ])]))
            .is_err()
        );
    }

    #[test]
    fn hotkeys_help_to_frame_and_parse_lines() {
        let cmd = HotkeysHelp::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("HOTKEYS"), bulk("HELP")]));
        assert_eq!(cmd.name(), "HOTKEYS HELP");
        assert!(cmd.idempotent());
        let lines = cmd
            .parse_response(array(vec![bulk("START"), bulk("STOP")]))
            .unwrap();
        assert_eq!(lines, vec![Bytes::from("START"), Bytes::from("STOP")]);
    }

    // -- CLIENT GETREDIR --

    #[test]
    fn client_getredir_to_frame() {
        let cmd = ClientGetRedir::new();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("CLIENT"), bulk("GETREDIR")])
        );
        assert!(cmd.idempotent());
    }

    #[test]
    fn client_getredir_parse_integer() {
        let cmd = ClientGetRedir::new();
        assert_eq!(cmd.parse_response(Frame::Integer(-1)).unwrap(), -1);
    }

    // -- HELP subcommands --

    #[test]
    fn help_subcommands_to_frame() {
        assert_eq!(
            ClientHelp::new().to_frame(),
            array(vec![bulk("CLIENT"), bulk("HELP")])
        );
        assert_eq!(
            CommandHelp::new().to_frame(),
            array(vec![bulk("COMMAND"), bulk("HELP")])
        );
        assert_eq!(
            ConfigHelp::new().to_frame(),
            array(vec![bulk("CONFIG"), bulk("HELP")])
        );
        assert_eq!(
            ModuleHelp::new().to_frame(),
            array(vec![bulk("MODULE"), bulk("HELP")])
        );
        assert!(ClientHelp::new().idempotent());
    }

    #[test]
    fn client_help_parse_lines() {
        let cmd = ClientHelp::new();
        let reply = array(vec![bulk("CLIENT <subcommand>"), bulk("ID")]);
        let lines = cmd.parse_response(reply).unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(&lines[1][..], b"ID");
    }

    // -- LOLWUT --

    #[test]
    fn lolwut_to_frame() {
        assert_eq!(Lolwut::new().to_frame(), array(vec![bulk("LOLWUT")]));
        assert_eq!(
            Lolwut::new().version(5).to_frame(),
            array(vec![bulk("LOLWUT"), bulk("VERSION"), bulk("5")])
        );
    }

    #[test]
    fn lolwut_parse_strings() {
        let cmd = Lolwut::new();
        assert_eq!(
            cmd.parse_response(Frame::BulkString(Some(Bytes::from("Redis ver. 7.4.0"))))
                .unwrap(),
            "Redis ver. 7.4.0"
        );
        assert_eq!(
            cmd.parse_response(Frame::VerbatimString(
                Bytes::from("txt"),
                Bytes::from("Redis ver. 8.0.0")
            ))
            .unwrap(),
            "Redis ver. 8.0.0"
        );
    }
}
