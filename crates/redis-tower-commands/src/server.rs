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
