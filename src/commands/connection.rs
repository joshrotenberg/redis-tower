//! Redis connection management commands

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// AUTH command - Authenticate to the server with a password
#[derive(Debug, Clone)]
pub struct Auth {
    password: String,
}

impl Auth {
    /// Create a new AUTH command
    pub fn new(password: impl Into<String>) -> Self {
        Self {
            password: password.into(),
        }
    }
}

impl Command for Auth {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("AUTH"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.password.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Expected OK response".to_string())),
        }
    }
}

/// AUTH command with username (ACL authentication)
#[derive(Debug, Clone)]
pub struct AuthAcl {
    username: String,
    password: String,
}

impl AuthAcl {
    /// Create a new AUTH command with username
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

impl Command for AuthAcl {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("AUTH"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.username.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.password.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Expected OK response".to_string())),
        }
    }
}

/// READONLY command - Enable read-only mode for replica connections
#[derive(Debug, Clone, Copy)]
pub struct ReadOnly;

impl Command for ReadOnly {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("READONLY")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Expected OK response".to_string())),
        }
    }
}

/// READWRITE command - Disable read-only mode for replica connections
#[derive(Debug, Clone, Copy)]
pub struct ReadWrite;

impl Command for ReadWrite {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("READWRITE")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Expected OK response".to_string())),
        }
    }
}

/// SELECT command - Change the selected database
#[derive(Debug, Clone)]
pub struct Select {
    db: u32,
}

impl Select {
    /// Create a new SELECT command
    pub fn new(db: u32) -> Self {
        Self { db }
    }
}

impl Command for Select {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SELECT"))),
            Frame::BulkString(Some(Bytes::from(self.db.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Expected OK response".to_string())),
        }
    }
}

/// QUIT command - Close the connection
#[derive(Debug, Clone, Copy, Default)]
pub struct Quit;

impl Quit {
    /// Create a new QUIT command
    pub fn new() -> Self {
        Self
    }
}

impl Command for Quit {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("QUIT")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Expected OK response".to_string())),
        }
    }
}

/// CLIENT GETNAME command - Get the current connection name
///
/// Returns the name of the current connection as set by CLIENT SETNAME,
/// or None if no name is set.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientGetName;
///
/// let cmd = ClientGetName;
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClientGetName;

impl Command for ClientGetName {
    type Response = Option<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("GETNAME"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(Some(String::from_utf8_lossy(&data).into_owned())),
            Frame::BulkString(None) | Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLIENT SETNAME command - Set the current connection name
///
/// Assigns a name to the current connection. The name can be displayed
/// in CLIENT LIST output and is useful for debugging and monitoring.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientSetName;
///
/// let cmd = ClientSetName::new("my-app-connection");
/// ```
#[derive(Debug, Clone)]
pub struct ClientSetName {
    name: String,
}

impl ClientSetName {
    /// Create a new CLIENT SETNAME command
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Command for ClientSetName {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("SETNAME"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.name.as_bytes()))),
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

/// CLIENT ID command - Get the current connection ID
///
/// Returns the unique client ID for this connection.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientId;
///
/// let cmd = ClientId;
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClientId;

impl Command for ClientId {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("ID"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(id) => Ok(id),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLIENT LIST command - Get list of client connections
///
/// Returns information about all connected clients.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientList;
///
/// let cmd = ClientList::new();
/// ```
#[derive(Debug, Clone, Default)]
pub struct ClientList {
    pub(crate) type_filter: Option<String>,
    pub(crate) ids: Vec<i64>,
}

impl ClientList {
    /// Create a new CLIENT LIST command
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by client type (normal, master, replica, pubsub)
    pub fn client_type(mut self, client_type: impl Into<String>) -> Self {
        self.type_filter = Some(client_type.into());
        self
    }

    /// Filter by specific client IDs
    pub fn id(mut self, id: i64) -> Self {
        self.ids.push(id);
        self
    }
}

impl Command for ClientList {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("LIST"))),
        ];

        if let Some(ref t) = self.type_filter {
            args.push(Frame::BulkString(Some(Bytes::from("TYPE"))));
            args.push(Frame::BulkString(Some(Bytes::from(t.clone()))));
        }

        if !self.ids.is_empty() {
            args.push(Frame::BulkString(Some(Bytes::from("ID"))));
            let ids_str = self
                .ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(" ");
            args.push(Frame::BulkString(Some(Bytes::from(ids_str))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).into_owned()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLIENT INFO command - Get information about the current client connection
#[derive(Debug, Clone, Copy)]
pub struct ClientInfo;

impl Command for ClientInfo {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
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

/// CLIENT KILL command - Close client connections
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientKill;
///
/// // Kill by address
/// let cmd = ClientKill::addr("127.0.0.1:6379");
///
/// // Kill by ID
/// let cmd = ClientKill::id(12345);
/// ```
#[derive(Debug, Clone)]
pub struct ClientKill {
    pub(crate) filter: ClientKillFilter,
}

/// Filter for CLIENT KILL
#[derive(Debug, Clone)]
pub enum ClientKillFilter {
    /// Kill by IP:port address
    Addr(String),
    /// Kill by client ID
    Id(i64),
    /// Kill by client type
    Type(String),
    /// Kill by username
    User(String),
    /// Skip current connection
    Skipme(bool),
}

impl ClientKill {
    /// Kill client by address
    pub fn addr(addr: impl Into<String>) -> Self {
        Self {
            filter: ClientKillFilter::Addr(addr.into()),
        }
    }

    /// Kill client by ID
    pub fn id(id: i64) -> Self {
        Self {
            filter: ClientKillFilter::Id(id),
        }
    }

    /// Kill by client type
    pub fn client_type(client_type: impl Into<String>) -> Self {
        Self {
            filter: ClientKillFilter::Type(client_type.into()),
        }
    }

    /// Kill by username
    pub fn user(username: impl Into<String>) -> Self {
        Self {
            filter: ClientKillFilter::User(username.into()),
        }
    }

    /// Skip killing the current connection
    pub fn skipme(skip: bool) -> Self {
        Self {
            filter: ClientKillFilter::Skipme(skip),
        }
    }
}

impl Command for ClientKill {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("KILL"))),
        ];

        match &self.filter {
            ClientKillFilter::Addr(addr) => {
                args.push(Frame::BulkString(Some(Bytes::from("ADDR"))));
                args.push(Frame::BulkString(Some(Bytes::from(addr.clone()))));
            }
            ClientKillFilter::Id(id) => {
                args.push(Frame::BulkString(Some(Bytes::from("ID"))));
                args.push(Frame::BulkString(Some(Bytes::from(id.to_string()))));
            }
            ClientKillFilter::Type(t) => {
                args.push(Frame::BulkString(Some(Bytes::from("TYPE"))));
                args.push(Frame::BulkString(Some(Bytes::from(t.clone()))));
            }
            ClientKillFilter::User(user) => {
                args.push(Frame::BulkString(Some(Bytes::from("USER"))));
                args.push(Frame::BulkString(Some(Bytes::from(user.clone()))));
            }
            ClientKillFilter::Skipme(skip) => {
                args.push(Frame::BulkString(Some(Bytes::from("SKIPME"))));
                args.push(Frame::BulkString(Some(Bytes::from(if *skip {
                    "YES"
                } else {
                    "NO"
                }))));
            }
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::SimpleString(_) => Ok(1), // Old format returns OK
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLIENT PAUSE command - Suspend client execution
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientPause;
///
/// // Pause for 5 seconds
/// let cmd = ClientPause::new(5000);
/// ```
#[derive(Debug, Clone)]
pub struct ClientPause {
    pub(crate) timeout_ms: i64,
    pub(crate) mode: Option<String>,
}

impl ClientPause {
    /// Create a new CLIENT PAUSE command
    pub fn new(timeout_ms: i64) -> Self {
        Self {
            timeout_ms,
            mode: None,
        }
    }

    /// Set pause mode (WRITE or ALL)
    pub fn mode(mut self, mode: impl Into<String>) -> Self {
        self.mode = Some(mode.into());
        self
    }
}

impl Command for ClientPause {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("PAUSE"))),
            Frame::BulkString(Some(Bytes::from(self.timeout_ms.to_string()))),
        ];

        if let Some(ref mode) = self.mode {
            args.push(Frame::BulkString(Some(Bytes::from(mode.clone()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLIENT UNPAUSE command - Resume client execution
#[derive(Debug, Clone, Copy)]
pub struct ClientUnpause;

impl Command for ClientUnpause {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("UNPAUSE"))),
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

/// CLIENT REPLY command - Control server replies to the current connection
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientReply;
///
/// // Turn off replies
/// let cmd = ClientReply::off();
///
/// // Skip next reply
/// let cmd = ClientReply::skip();
/// ```
#[derive(Debug, Clone)]
pub struct ClientReply {
    pub(crate) mode: String,
}

impl ClientReply {
    /// Don't send replies
    pub fn off() -> Self {
        Self {
            mode: "OFF".to_string(),
        }
    }

    /// Send replies (default)
    pub fn on() -> Self {
        Self {
            mode: "ON".to_string(),
        }
    }

    /// Skip the next reply
    pub fn skip() -> Self {
        Self {
            mode: "SKIP".to_string(),
        }
    }
}

impl Command for ClientReply {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("REPLY"))),
            Frame::BulkString(Some(Bytes::from(self.mode.clone()))),
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

/// CLIENT SETINFO command - Set client connection metadata
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientSetInfo;
///
/// let cmd = ClientSetInfo::lib_name("my-redis-client");
/// let cmd = ClientSetInfo::lib_ver("1.0.0");
/// ```
#[derive(Debug, Clone)]
pub struct ClientSetInfo {
    pub(crate) attr: String,
    pub(crate) value: String,
}

impl ClientSetInfo {
    /// Set library name
    pub fn lib_name(name: impl Into<String>) -> Self {
        Self {
            attr: "LIB-NAME".to_string(),
            value: name.into(),
        }
    }

    /// Set library version
    pub fn lib_ver(version: impl Into<String>) -> Self {
        Self {
            attr: "LIB-VER".to_string(),
            value: version.into(),
        }
    }
}

impl Command for ClientSetInfo {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("SETINFO"))),
            Frame::BulkString(Some(Bytes::from(self.attr.clone()))),
            Frame::BulkString(Some(Bytes::from(self.value.clone()))),
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

/// CLIENT UNBLOCK command - Unblock a blocked client
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientUnblock;
///
/// let cmd = ClientUnblock::new(12345);
/// let cmd = ClientUnblock::new(12345).timeout();
/// ```
#[derive(Debug, Clone)]
pub struct ClientUnblock {
    pub(crate) client_id: i64,
    pub(crate) unblock_type: Option<String>,
}

impl ClientUnblock {
    /// Create a new CLIENT UNBLOCK command
    pub fn new(client_id: i64) -> Self {
        Self {
            client_id,
            unblock_type: None,
        }
    }

    /// Unblock with TIMEOUT error
    pub fn timeout(mut self) -> Self {
        self.unblock_type = Some("TIMEOUT".to_string());
        self
    }

    /// Unblock with ERROR
    pub fn error(mut self) -> Self {
        self.unblock_type = Some("ERROR".to_string());
        self
    }
}

impl Command for ClientUnblock {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("UNBLOCK"))),
            Frame::BulkString(Some(Bytes::from(self.client_id.to_string()))),
        ];

        if let Some(ref t) = self.unblock_type {
            args.push(Frame::BulkString(Some(Bytes::from(t.clone()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLIENT NO-EVICT command - Set connection to not be evicted
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientNoEvict;
///
/// let cmd = ClientNoEvict::on();
/// let cmd = ClientNoEvict::off();
/// ```
#[derive(Debug, Clone)]
pub struct ClientNoEvict {
    pub(crate) enabled: bool,
}

impl ClientNoEvict {
    /// Enable no-evict mode
    pub fn on() -> Self {
        Self { enabled: true }
    }

    /// Disable no-evict mode
    pub fn off() -> Self {
        Self { enabled: false }
    }
}

impl Command for ClientNoEvict {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("NO-EVICT"))),
            Frame::BulkString(Some(Bytes::from(if self.enabled { "ON" } else { "OFF" }))),
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

/// HELLO command - Handshake with Redis (Redis 6.0+)
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Hello;
///
/// // Simple RESP3 handshake
/// let cmd = Hello::new(3);
///
/// // With AUTH
/// let cmd = Hello::new(3)
///     .auth("default", "password");
/// ```
#[derive(Debug, Clone)]
pub struct Hello {
    pub(crate) protocol_version: i32,
    pub(crate) auth: Option<(String, String)>,
    pub(crate) setname: Option<String>,
}

impl Hello {
    /// Create a new HELLO command
    pub fn new(protocol_version: i32) -> Self {
        Self {
            protocol_version,
            auth: None,
            setname: None,
        }
    }

    /// Authenticate with username and password
    pub fn auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = Some((username.into(), password.into()));
        self
    }

    /// Set connection name
    pub fn setname(mut self, name: impl Into<String>) -> Self {
        self.setname = Some(name.into());
        self
    }
}

impl Command for Hello {
    type Response = String; // Simplified - returns server info

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("HELLO"))),
            Frame::BulkString(Some(Bytes::from(self.protocol_version.to_string()))),
        ];

        if let Some((ref user, ref pass)) = self.auth {
            args.push(Frame::BulkString(Some(Bytes::from("AUTH"))));
            args.push(Frame::BulkString(Some(Bytes::from(user.clone()))));
            args.push(Frame::BulkString(Some(Bytes::from(pass.clone()))));
        }

        if let Some(ref name) = self.setname {
            args.push(Frame::BulkString(Some(Bytes::from("SETNAME"))));
            args.push(Frame::BulkString(Some(Bytes::from(name.clone()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        // HELLO returns a map/array - simplified to string for now
        Ok(format!("{:?}", frame))
    }
}

/// ASKING command - Signal cluster ASK redirect handling
///
/// When a cluster client receives an -ASK redirect, the ASKING command is sent
/// to the target node followed by the redirected command. This is a low-level
/// cluster command used internally by cluster clients.
///
/// Available since Redis 3.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Asking;
///
/// let cmd = Asking::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Asking;

impl Asking {
    /// Create a new ASKING command
    pub fn new() -> Self {
        Self
    }
}

impl Default for Asking {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for Asking {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("ASKING")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// RESET command - Reset connection state (Redis 6.2+)
///
/// Resets connection state including:
/// - Authentication
/// - Database selection
/// - WATCH
/// - Client tracking
/// - etc.
#[derive(Debug, Clone, Copy)]
pub struct Reset;

impl Command for Reset {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("RESET")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLIENT CACHING command - Control key tracking for next command
///
/// Instructs the server whether to track the keys in the next request,
/// when tracking is enabled in OPTIN or OPTOUT mode.
///
/// Available since Redis 6.0.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientCaching;
///
/// // Enable tracking for next command
/// let cmd = ClientCaching::yes();
///
/// // Disable tracking for next command
/// let cmd = ClientCaching::no();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClientCaching {
    enabled: bool,
}

impl ClientCaching {
    /// Enable key tracking for the next command
    pub fn yes() -> Self {
        Self { enabled: true }
    }

    /// Disable key tracking for the next command
    pub fn no() -> Self {
        Self { enabled: false }
    }
}

impl Command for ClientCaching {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("CACHING"))),
            Frame::BulkString(Some(Bytes::from(if self.enabled { "YES" } else { "NO" }))),
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

/// CLIENT GETREDIR command - Get tracking redirection client ID
///
/// Returns the client ID to which the connection's tracking notifications
/// are redirected.
///
/// Available since Redis 6.0.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientGetRedir;
///
/// let cmd = ClientGetRedir::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClientGetRedir;

impl ClientGetRedir {
    /// Create a new CLIENT GETREDIR command
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
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("GETREDIR"))),
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

/// CLIENT NO-TOUCH command - Control LRU/LFU updates
///
/// Controls whether commands sent by the client affect the LRU/LFU of accessed keys.
///
/// Available since Redis 7.2.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientNoTouch;
///
/// // Enable no-touch mode (don't update LRU/LFU)
/// let cmd = ClientNoTouch::on();
///
/// // Disable no-touch mode
/// let cmd = ClientNoTouch::off();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClientNoTouch {
    enabled: bool,
}

impl ClientNoTouch {
    /// Enable no-touch mode
    pub fn on() -> Self {
        Self { enabled: true }
    }

    /// Disable no-touch mode
    pub fn off() -> Self {
        Self { enabled: false }
    }
}

impl Command for ClientNoTouch {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("NO-TOUCH"))),
            Frame::BulkString(Some(Bytes::from(if self.enabled { "ON" } else { "OFF" }))),
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

/// CLIENT TRACKING command - Control server-assisted client-side caching
///
/// Enables or disables tracking for server-assisted client-side caching.
///
/// Available since Redis 6.0.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientTracking;
///
/// // Enable basic tracking
/// let cmd = ClientTracking::on();
///
/// // Disable tracking
/// let cmd = ClientTracking::off();
///
/// // Enable with options
/// let cmd = ClientTracking::on()
///     .redirect(123)
///     .prefix("user:")
///     .bcast()
///     .noloop();
/// ```
#[derive(Debug, Clone)]
pub struct ClientTracking {
    enabled: bool,
    redirect: Option<i64>,
    prefixes: Vec<String>,
    bcast: bool,
    optin: bool,
    optout: bool,
    noloop: bool,
}

impl ClientTracking {
    /// Enable tracking
    pub fn on() -> Self {
        Self {
            enabled: true,
            redirect: None,
            prefixes: Vec::new(),
            bcast: false,
            optin: false,
            optout: false,
            noloop: false,
        }
    }

    /// Disable tracking
    pub fn off() -> Self {
        Self {
            enabled: false,
            redirect: None,
            prefixes: Vec::new(),
            bcast: false,
            optin: false,
            optout: false,
            noloop: false,
        }
    }

    /// Redirect invalidation messages to another client
    pub fn redirect(mut self, client_id: i64) -> Self {
        self.redirect = Some(client_id);
        self
    }

    /// Add a key prefix to track
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefixes.push(prefix.into());
        self
    }

    /// Enable broadcasting mode
    pub fn bcast(mut self) -> Self {
        self.bcast = true;
        self
    }

    /// Enable opt-in mode (only track keys when CLIENT CACHING YES is used)
    pub fn optin(mut self) -> Self {
        self.optin = true;
        self
    }

    /// Enable opt-out mode (track all keys except when CLIENT CACHING NO is used)
    pub fn optout(mut self) -> Self {
        self.optout = true;
        self
    }

    /// Don't send invalidation messages for keys modified by this connection
    pub fn noloop(mut self) -> Self {
        self.noloop = true;
        self
    }
}

impl Command for ClientTracking {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("TRACKING"))),
            Frame::BulkString(Some(Bytes::from(if self.enabled { "ON" } else { "OFF" }))),
        ];

        if let Some(client_id) = self.redirect {
            args.push(Frame::BulkString(Some(Bytes::from("REDIRECT"))));
            args.push(Frame::BulkString(Some(Bytes::from(client_id.to_string()))));
        }

        for prefix in &self.prefixes {
            args.push(Frame::BulkString(Some(Bytes::from("PREFIX"))));
            args.push(Frame::BulkString(Some(Bytes::from(prefix.clone()))));
        }

        if self.bcast {
            args.push(Frame::BulkString(Some(Bytes::from("BCAST"))));
        }

        if self.optin {
            args.push(Frame::BulkString(Some(Bytes::from("OPTIN"))));
        }

        if self.optout {
            args.push(Frame::BulkString(Some(Bytes::from("OPTOUT"))));
        }

        if self.noloop {
            args.push(Frame::BulkString(Some(Bytes::from("NOLOOP"))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLIENT TRACKINGINFO command - Get tracking information
///
/// Returns information about the current client connection's use of
/// server-assisted client-side caching.
///
/// Available since Redis 6.2.0.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientTrackingInfo;
///
/// let cmd = ClientTrackingInfo::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClientTrackingInfo;

impl ClientTrackingInfo {
    /// Create a new CLIENT TRACKINGINFO command
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
    type Response = String; // Simplified - returns complex nested structure

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("TRACKINGINFO"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Ok(format!("{:?}", frame)),
        }
    }
}

/// CLIENT HELP command - Get help text for CLIENT subcommands
///
/// Available since Redis 5.0.0.
#[derive(Debug, Clone, Copy)]
pub struct ClientHelp;

impl ClientHelp {
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
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("HELP"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => Ok(items
                .into_iter()
                .filter_map(|item| match item {
                    Frame::BulkString(Some(data)) => {
                        Some(String::from_utf8_lossy(&data).to_string())
                    }
                    _ => None,
                })
                .collect()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// MONITOR command - Monitor all commands received by the server
///
/// Streams back every command processed by the Redis server in real-time.
/// **Warning**: This command is for debugging and has significant performance impact.
/// It should not be used in production environments.
///
/// Available since Redis 1.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::Monitor;
///
/// let cmd = Monitor::new();
/// // Server will stream commands until connection is closed
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Monitor;

impl Monitor {
    /// Create a new MONITOR command
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
    type Response = (); // Streams responses, not a single response

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("MONITOR")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_frame() {
        let cmd = Auth::new("mypassword");
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_auth_acl_frame() {
        let cmd = AuthAcl::new("default", "mypassword");
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 3);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_readonly_frame() {
        let cmd = ReadOnly;
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 1);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_select_frame() {
        let cmd = Select::new(1);
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_client_id_frame() {
        let cmd = ClientId;
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_client_list_frame() {
        let cmd = ClientList::new();
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_client_list_with_type_frame() {
        let cmd = ClientList::new().client_type("normal");
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert!(elements.len() >= 2);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_client_info_frame() {
        let cmd = ClientInfo;
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_client_kill_by_addr() {
        let cmd = ClientKill::addr("127.0.0.1:6379");
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 4); // CLIENT KILL ADDR value
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_client_kill_by_id() {
        let cmd = ClientKill::id(12345);
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 4);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_client_pause_frame() {
        let cmd = ClientPause::new(5000);
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 3);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_client_unpause_frame() {
        let cmd = ClientUnpause;
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_client_reply_off() {
        let cmd = ClientReply::off();
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 3);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_client_setinfo_frame() {
        let cmd = ClientSetInfo::lib_name("redis-tower");
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 4);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_client_unblock_frame() {
        let cmd = ClientUnblock::new(12345);
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 3);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_client_no_evict_on() {
        let cmd = ClientNoEvict::on();
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 3);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_hello_frame() {
        let cmd = Hello::new(3);
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_hello_with_auth() {
        let cmd = Hello::new(3).auth("default", "password");
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert!(elements.len() >= 2);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_reset_frame() {
        let cmd = Reset;
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 1);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_asking_frame() {
        let cmd = Asking::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("ASKING"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_client_caching_yes() {
        let cmd = ClientCaching::yes();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CLIENT"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("CACHING"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("YES"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_client_caching_no() {
        let cmd = ClientCaching::no();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("NO"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_client_getredir_frame() {
        let cmd = ClientGetRedir::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CLIENT"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("GETREDIR"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_client_getredir_parse() {
        let frame = Frame::Integer(123);
        let result = ClientGetRedir::parse_response(frame).unwrap();
        assert_eq!(result, 123);
    }

    #[test]
    fn test_client_no_touch_on() {
        let cmd = ClientNoTouch::on();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CLIENT"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("NO-TOUCH"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("ON"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_client_no_touch_off() {
        let cmd = ClientNoTouch::off();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("OFF"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_client_tracking_on() {
        let cmd = ClientTracking::on();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CLIENT"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("TRACKING"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("ON"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_client_tracking_off() {
        let cmd = ClientTracking::off();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("OFF"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_client_tracking_with_options() {
        let cmd = ClientTracking::on()
            .redirect(456)
            .prefix("user:")
            .prefix("session:")
            .bcast()
            .optin()
            .noloop();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("REDIRECT")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("456")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("PREFIX")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("user:")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("session:")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("BCAST")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("OPTIN")))));
                assert!(parts.contains(&Frame::BulkString(Some(Bytes::from("NOLOOP")))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_client_trackinginfo_frame() {
        let cmd = ClientTrackingInfo::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("CLIENT"))));
                assert_eq!(
                    parts[1],
                    Frame::BulkString(Some(Bytes::from("TRACKINGINFO")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }
}
