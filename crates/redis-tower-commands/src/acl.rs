use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// ACL LIST
///
/// Returns a list of ACL rules for all users.
pub struct AclList;

impl AclList {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AclList {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for AclList {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("ACL"), bulk("LIST")])
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
        "ACL LIST"
    }
}

/// ACL GETUSER username
///
/// Returns the ACL rules for a specific user as a complex nested response.
pub struct AclGetUser {
    username: String,
}

impl AclGetUser {
    pub fn new(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
        }
    }
}

impl Command for AclGetUser {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ACL"),
            bulk("GETUSER"),
            bulk(self.username.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "ACL GETUSER"
    }
}

/// ACL SETUSER username [rule ...]
///
/// Create or modify an ACL user with the specified rules.
pub struct AclSetUser {
    username: String,
    rules: Vec<String>,
}

impl AclSetUser {
    pub fn new(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            rules: Vec::new(),
        }
    }

    /// Add a rule to the user definition (e.g. "on", "+@all", "~*").
    pub fn rule(mut self, rule: impl Into<String>) -> Self {
        self.rules.push(rule.into());
        self
    }

    /// Add multiple rules to the user definition.
    pub fn rules(mut self, rules: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.rules.extend(rules.into_iter().map(Into::into));
        self
    }
}

impl Command for AclSetUser {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ACL"), bulk("SETUSER"), bulk(self.username.as_str())];
        for rule in &self.rules {
            args.push(bulk(rule.as_str()));
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
        "ACL SETUSER"
    }
}

/// ACL DELUSER username [username ...]
///
/// Deletes one or more ACL users. Returns the number of users deleted.
pub struct AclDelUser {
    usernames: Vec<String>,
}

impl AclDelUser {
    pub fn new(username: impl Into<String>) -> Self {
        Self {
            usernames: vec![username.into()],
        }
    }

    pub fn usernames(usernames: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            usernames: usernames.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for AclDelUser {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ACL"), bulk("DELUSER")];
        for u in &self.usernames {
            args.push(bulk(u.as_str()));
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
        "ACL DELUSER"
    }
}

/// ACL CAT \[category\]
///
/// Lists ACL categories, or the commands within a given category.
pub struct AclCat {
    category: Option<String>,
}

impl AclCat {
    /// List all ACL categories.
    pub fn new() -> Self {
        Self { category: None }
    }

    /// List commands in the specified category.
    pub fn category(category: impl Into<String>) -> Self {
        Self {
            category: Some(category.into()),
        }
    }
}

impl Default for AclCat {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for AclCat {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ACL"), bulk("CAT")];
        if let Some(ref cat) = self.category {
            args.push(bulk(cat.as_str()));
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
        "ACL CAT"
    }
}

/// ACL LOG [count|RESET]
///
/// Returns recent ACL security events. Use `AclLogReset` to clear the log.
pub struct AclLog {
    count: Option<u64>,
}

impl AclLog {
    /// Return all recent ACL log entries.
    pub fn new() -> Self {
        Self { count: None }
    }

    /// Return at most `count` recent ACL log entries.
    pub fn count(count: u64) -> Self {
        Self { count: Some(count) }
    }
}

impl Default for AclLog {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for AclLog {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ACL"), bulk("LOG")];
        if let Some(count) = self.count {
            args.push(bulk(count.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "ACL LOG"
    }
}

/// ACL LOG RESET
///
/// Clears the ACL security event log.
pub struct AclLogReset;

impl AclLogReset {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AclLogReset {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for AclLogReset {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("ACL"), bulk("LOG"), bulk("RESET")])
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
        "ACL LOG RESET"
    }
}

/// ACL SAVE
///
/// Saves the current ACL rules to the configured ACL file.
pub struct AclSave;

impl AclSave {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AclSave {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for AclSave {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("ACL"), bulk("SAVE")])
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
        "ACL SAVE"
    }
}

/// ACL LOAD
///
/// Reloads ACL rules from the configured ACL file.
pub struct AclLoad;

impl AclLoad {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AclLoad {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for AclLoad {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("ACL"), bulk("LOAD")])
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
        "ACL LOAD"
    }
}

/// ACL WHOAMI
///
/// Returns the username of the current connection.
pub struct AclWhoAmI;

impl AclWhoAmI {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AclWhoAmI {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for AclWhoAmI {
    type Response = String;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("ACL"), bulk("WHOAMI")])
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
        "ACL WHOAMI"
    }
}

/// ACL GENPASS \[bits\]
///
/// Generates a random password. Optionally specify the number of bits
/// of pseudo-random data (default 256).
pub struct AclGenPass {
    bits: Option<u32>,
}

impl AclGenPass {
    /// Generate a password with default bit length (256).
    pub fn new() -> Self {
        Self { bits: None }
    }

    /// Generate a password with the specified number of bits.
    pub fn bits(bits: u32) -> Self {
        Self { bits: Some(bits) }
    }
}

impl Default for AclGenPass {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for AclGenPass {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ACL"), bulk("GENPASS")];
        if let Some(bits) = self.bits {
            args.push(bulk(bits.to_string()));
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
        "ACL GENPASS"
    }
}

/// ACL DRYRUN username command [arg ...]
///
/// Simulates the execution of a command by the specified user and reports
/// whether the user has permission. Returns "OK" on success or an error
/// message describing the permission failure.
pub struct AclDryRun {
    username: String,
    command: String,
    args: Vec<String>,
}

impl AclDryRun {
    pub fn new(username: impl Into<String>, command: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            command: command.into(),
            args: Vec::new(),
        }
    }

    /// Add an argument to the simulated command.
    pub fn arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// Add multiple arguments to the simulated command.
    pub fn args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }
}

impl Command for AclDryRun {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut frame_args = vec![
            bulk("ACL"),
            bulk("DRYRUN"),
            bulk(self.username.as_str()),
            bulk(self.command.as_str()),
        ];
        for arg in &self.args {
            frame_args.push(bulk(arg.as_str()));
        }
        array(frame_args)
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
        "ACL DRYRUN"
    }
}
