//! Redis ACL (Access Control List) commands (Redis 6.0+)
//!
//! ACL commands provide fine-grained access control for Redis users,
//! allowing you to specify which commands and keys each user can access.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// ACL SETUSER command - Create or modify a user
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::acl::AclSetUser;
/// // Create a user with password
/// let cmd = AclSetUser::new("alice")
///     .on()
///     .password("secret123")
///     .command("+get")
///     .command("+set");
///
/// // Create user with key patterns
/// let cmd = AclSetUser::new("bob")
///     .on()
///     .password("pass456")
///     .key_pattern("user:*")
///     .command("allcommands");
/// ```
#[derive(Debug, Clone)]
pub struct AclSetUser {
    pub(crate) username: String,
    pub(crate) rules: Vec<String>,
}

impl AclSetUser {
    /// Create a new ACL SETUSER command
    pub fn new(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            rules: Vec::new(),
        }
    }

    /// Enable the user (can authenticate)
    pub fn on(mut self) -> Self {
        self.rules.push("on".to_string());
        self
    }

    /// Disable the user (cannot authenticate)
    pub fn off(mut self) -> Self {
        self.rules.push("off".to_string());
        self
    }

    /// Add a password (can use multiple passwords)
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.rules.push(format!(">{}", password.into()));
        self
    }

    /// Remove a password
    pub fn remove_password(mut self, password: impl Into<String>) -> Self {
        self.rules.push(format!("<{}", password.into()));
        self
    }

    /// Set passwordless authentication
    pub fn nopass(mut self) -> Self {
        self.rules.push("nopass".to_string());
        self
    }

    /// Add allowed key pattern
    pub fn key_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.rules.push(format!("~{}", pattern.into()));
        self
    }

    /// Add channel pattern for pub/sub
    pub fn channel_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.rules.push(format!("&{}", pattern.into()));
        self
    }

    /// Add command permission
    /// Use "+command" to allow, "-command" to deny
    pub fn command(mut self, command: impl Into<String>) -> Self {
        self.rules.push(command.into());
        self
    }

    /// Reset user to default state
    pub fn reset(mut self) -> Self {
        self.rules.push("reset".to_string());
        self
    }
}

impl Command for AclSetUser {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("ACL"))),
            Frame::BulkString(Some(Bytes::from("SETUSER"))),
            Frame::BulkString(Some(Bytes::from(self.username.clone()))),
        ];

        for rule in &self.rules {
            args.push(Frame::BulkString(Some(Bytes::from(rule.clone()))));
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

/// ACL GETUSER command - Get user details
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::acl::AclGetUser;
/// let cmd = AclGetUser::new("alice");
/// ```
#[derive(Debug, Clone)]
pub struct AclGetUser {
    pub(crate) username: String,
}

impl AclGetUser {
    /// Create a new ACL GETUSER command
    pub fn new(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
        }
    }
}

impl Command for AclGetUser {
    type Response = String; // Simplified - actual response is array of key-value pairs

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("ACL"))),
            Frame::BulkString(Some(Bytes::from("GETUSER"))),
            Frame::BulkString(Some(Bytes::from(self.username.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        // Simplified - real implementation would parse the array structure
        Ok(format!("{:?}", frame))
    }
}

/// ACL DELUSER command - Delete users
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::acl::AclDelUser;
/// let cmd = AclDelUser::new()
///     .username("alice")
///     .username("bob");
/// ```
#[derive(Debug, Clone)]
pub struct AclDelUser {
    pub(crate) usernames: Vec<String>,
}

impl AclDelUser {
    /// Create a new ACL DELUSER command
    pub fn new() -> Self {
        Self {
            usernames: Vec::new(),
        }
    }

    /// Add a username to delete
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.usernames.push(username.into());
        self
    }
}

impl Default for AclDelUser {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for AclDelUser {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("ACL"))),
            Frame::BulkString(Some(Bytes::from("DELUSER"))),
        ];

        for username in &self.usernames {
            args.push(Frame::BulkString(Some(Bytes::from(username.clone()))));
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

/// ACL LIST command - List all users in ACL format
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::acl::AclList;
/// let cmd = AclList::new();
/// ```
#[derive(Debug, Clone)]
pub struct AclList;

impl AclList {
    /// Create a new ACL LIST command
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
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("ACL"))),
            Frame::BulkString(Some(Bytes::from("LIST"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    if let Frame::BulkString(Some(data)) = item {
                        result.push(String::from_utf8_lossy(&data).into_owned());
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ACL USERS command - List all usernames
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::acl::AclUsers;
/// let cmd = AclUsers::new();
/// ```
#[derive(Debug, Clone)]
pub struct AclUsers;

impl AclUsers {
    /// Create a new ACL USERS command
    pub fn new() -> Self {
        Self
    }
}

impl Default for AclUsers {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for AclUsers {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("ACL"))),
            Frame::BulkString(Some(Bytes::from("USERS"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    if let Frame::BulkString(Some(data)) = item {
                        result.push(String::from_utf8_lossy(&data).into_owned());
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ACL WHOAMI command - Get the current connection's username
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::acl::AclWhoAmI;
/// let cmd = AclWhoAmI::new();
/// ```
#[derive(Debug, Clone)]
pub struct AclWhoAmI;

impl AclWhoAmI {
    /// Create a new ACL WHOAMI command
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
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("ACL"))),
            Frame::BulkString(Some(Bytes::from("WHOAMI"))),
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

/// ACL CAT command - List available command categories
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::acl::AclCat;
/// // List all categories
/// let cmd = AclCat::new();
///
/// // List commands in a category
/// let cmd = AclCat::category("string");
/// ```
#[derive(Debug, Clone)]
pub struct AclCat {
    pub(crate) category: Option<String>,
}

impl AclCat {
    /// Create a new ACL CAT command (lists all categories)
    pub fn new() -> Self {
        Self { category: None }
    }

    /// List commands in a specific category
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
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("ACL"))),
            Frame::BulkString(Some(Bytes::from("CAT"))),
        ];

        if let Some(category) = &self.category {
            args.push(Frame::BulkString(Some(Bytes::from(category.clone()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    if let Frame::BulkString(Some(data)) = item {
                        result.push(String::from_utf8_lossy(&data).into_owned());
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// ACL LOG command - Show ACL security events log
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::acl::AclLog;
/// // Get last 10 log entries
/// let cmd = AclLog::new().count(10);
///
/// // Reset the log
/// let cmd = AclLog::reset();
/// ```
#[derive(Debug, Clone)]
pub struct AclLog {
    pub(crate) count: Option<i64>,
    pub(crate) reset: bool,
}

impl AclLog {
    /// Create a new ACL LOG command
    pub fn new() -> Self {
        Self {
            count: None,
            reset: false,
        }
    }

    /// Get N most recent log entries
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    /// Reset the log
    pub fn reset() -> Self {
        Self {
            count: None,
            reset: true,
        }
    }
}

impl Default for AclLog {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for AclLog {
    type Response = String; // Simplified

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("ACL"))),
            Frame::BulkString(Some(Bytes::from("LOG"))),
        ];

        if self.reset {
            args.push(Frame::BulkString(Some(Bytes::from("RESET"))));
        } else if let Some(count) = self.count {
            args.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(format!("{:?}", frame))
    }
}

/// ACL LOAD command - Reload ACL rules from configured ACL file
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::acl::AclLoad;
/// let cmd = AclLoad::new();
/// ```
#[derive(Debug, Clone)]
pub struct AclLoad;

impl AclLoad {
    /// Create a new ACL LOAD command
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
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("ACL"))),
            Frame::BulkString(Some(Bytes::from("LOAD"))),
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

/// ACL SAVE command - Save ACL rules to configured ACL file
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::acl::AclSave;
/// let cmd = AclSave::new();
/// ```
#[derive(Debug, Clone)]
pub struct AclSave;

impl AclSave {
    /// Create a new ACL SAVE command
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
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("ACL"))),
            Frame::BulkString(Some(Bytes::from("SAVE"))),
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

/// ACL GENPASS command - Generate a secure password
///
/// # Examples
///
/// ```rust,no_run
/// # use redis_tower::commands::acl::AclGenPass;
/// // Generate default 256-bit password
/// let cmd = AclGenPass::new();
///
/// // Generate 128-bit password
/// let cmd = AclGenPass::new().bits(128);
/// ```
#[derive(Debug, Clone)]
pub struct AclGenPass {
    pub(crate) bits: Option<i64>,
}

impl AclGenPass {
    /// Create a new ACL GENPASS command
    pub fn new() -> Self {
        Self { bits: None }
    }

    /// Specify number of bits (default 256)
    pub fn bits(mut self, bits: i64) -> Self {
        self.bits = Some(bits);
        self
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
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("ACL"))),
            Frame::BulkString(Some(Bytes::from("GENPASS"))),
        ];

        if let Some(bits) = self.bits {
            args.push(Frame::BulkString(Some(Bytes::from(bits.to_string()))));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acl_setuser_frame() {
        let cmd = AclSetUser::new("alice")
            .on()
            .password("secret")
            .command("+get");

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert_eq!(items[0], Frame::BulkString(Some(Bytes::from("ACL"))));
            assert_eq!(items[1], Frame::BulkString(Some(Bytes::from("SETUSER"))));
            assert_eq!(items[2], Frame::BulkString(Some(Bytes::from("alice"))));
            assert!(items.contains(&Frame::BulkString(Some(Bytes::from("on")))));
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_acl_getuser_frame() {
        let cmd = AclGetUser::new("bob");
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 3);
            assert_eq!(items[1], Frame::BulkString(Some(Bytes::from("GETUSER"))));
            assert_eq!(items[2], Frame::BulkString(Some(Bytes::from("bob"))));
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_acl_deluser_frame() {
        let cmd = AclDelUser::new().username("alice").username("bob");

        let frame = cmd.to_frame();
        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 4); // ACL DELUSER alice bob
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_acl_list_frame() {
        let cmd = AclList::new();
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items.len(), 2);
            assert_eq!(items[1], Frame::BulkString(Some(Bytes::from("LIST"))));
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_acl_users_frame() {
        let cmd = AclUsers::new();
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items[1], Frame::BulkString(Some(Bytes::from("USERS"))));
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_acl_whoami_frame() {
        let cmd = AclWhoAmI::new();
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items[1], Frame::BulkString(Some(Bytes::from("WHOAMI"))));
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_acl_cat_frame() {
        let cmd = AclCat::category("string");
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items[1], Frame::BulkString(Some(Bytes::from("CAT"))));
            assert_eq!(items[2], Frame::BulkString(Some(Bytes::from("string"))));
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_acl_log_frame() {
        let cmd = AclLog::new().count(10);
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items[1], Frame::BulkString(Some(Bytes::from("LOG"))));
            assert_eq!(items[2], Frame::BulkString(Some(Bytes::from("10"))));
        } else {
            panic!("Expected Array frame");
        }
    }

    #[test]
    fn test_acl_genpass_frame() {
        let cmd = AclGenPass::new().bits(128);
        let frame = cmd.to_frame();

        if let Frame::Array(items) = frame {
            assert_eq!(items[1], Frame::BulkString(Some(Bytes::from("GENPASS"))));
            assert_eq!(items[2], Frame::BulkString(Some(Bytes::from("128"))));
        } else {
            panic!("Expected Array frame");
        }
    }
}
