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
/// Creates or modifies a Redis user with specified permissions, passwords, and access rules.
/// Users can be enabled/disabled, assigned passwords, restricted to key patterns, and granted
/// specific command permissions. This is the primary command for managing user access control.
///
/// # Request
/// - `username`: The username to create or modify
/// - `rules`: ACL rules to apply (on/off, passwords, key patterns, command permissions)
///
/// # Response
/// Returns `()` - Always returns OK on success
///
/// # Redis Version
/// Available since Redis 6.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::acl::AclSetUser;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Create a user with password and limited commands
/// let cmd = AclSetUser::new("alice")
///     .on()
///     .password("secret123")
///     .command("+get")
///     .command("+set");
/// client.call(cmd).await?;
///
/// // Create user with key pattern restrictions
/// let cmd = AclSetUser::new("bob")
///     .on()
///     .password("pass456")
///     .key_pattern("user:*")
///     .command("allcommands");
/// client.call(cmd).await?;
/// # Ok(())
/// # }
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
/// Returns all the rules and settings for a specific user, including flags, passwords (hashed),
/// command permissions, and key patterns. Returns null if the user doesn't exist.
///
/// # Request
/// - `username`: The username to retrieve details for
///
/// # Response
/// Returns `String` - Debug representation of user details array (simplified)
///
/// # Redis Version
/// Available since Redis 6.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::acl::AclGetUser;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let cmd = AclGetUser::new("alice");
/// let details = client.call(cmd).await?;
/// println!("User details: {}", details);
/// # Ok(())
/// # }
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
/// Deletes one or more users from the ACL list. The default user cannot be deleted.
/// Returns the number of users that were deleted.
///
/// # Request
/// - `usernames`: One or more usernames to delete
///
/// # Response
/// Returns `i64` - Number of users successfully deleted
///
/// # Redis Version
/// Available since Redis 6.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::acl::AclDelUser;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let cmd = AclDelUser::new()
///     .username("alice")
///     .username("bob");
/// let deleted = client.call(cmd).await?;
/// println!("Deleted {} users", deleted);
/// # Ok(())
/// # }
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
/// Returns an array where each element is a string representing one user's ACL rules
/// in the same format accepted by ACL SETUSER. Useful for backing up or inspecting
/// the entire ACL configuration.
///
/// # Request
/// (no parameters)
///
/// # Response
/// Returns `Vec<String>` - Array of ACL rule strings, one per user
///
/// # Redis Version
/// Available since Redis 6.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::acl::AclList;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let cmd = AclList::new();
/// let users = client.call(cmd).await?;
/// for user_acl in users {
///     println!("User ACL: {}", user_acl);
/// }
/// # Ok(())
/// # }
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
/// Returns a simple list of all defined usernames. This is faster than ACL LIST
/// when you only need the usernames without their full ACL rules.
///
/// # Request
/// (no parameters)
///
/// # Response
/// Returns `Vec<String>` - Array of usernames
///
/// # Redis Version
/// Available since Redis 6.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::acl::AclUsers;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let cmd = AclUsers::new();
/// let usernames = client.call(cmd).await?;
/// println!("Existing users: {:?}", usernames);
/// # Ok(())
/// # }
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
/// Returns the username of the current connection. Useful for debugging authentication
/// or determining which user a connection is authenticated as.
///
/// # Request
/// (no parameters)
///
/// # Response
/// Returns `String` - The username of the authenticated user ("default" if not authenticated)
///
/// # Redis Version
/// Available since Redis 6.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::acl::AclWhoAmI;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let cmd = AclWhoAmI::new();
/// let username = client.call(cmd).await?;
/// println!("Currently authenticated as: {}", username);
/// # Ok(())
/// # }
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
/// Returns a list of command categories (e.g., "read", "write", "admin") when called without
/// arguments. When called with a category name, returns all commands in that category.
/// Categories are used in ACL rules to grant or deny groups of related commands.
///
/// # Request
/// - `category` (optional): Specific category to list commands for
///
/// # Response
/// Returns `Vec<String>`:
/// - Without category: List of all available categories
/// - With category: List of all commands in that category
///
/// # Redis Version
/// Available since Redis 6.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::acl::AclCat;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // List all categories
/// let categories = client.call(AclCat::new()).await?;
/// println!("Available categories: {:?}", categories);
///
/// // List commands in a category
/// let cmd = AclCat::category("string");
/// let commands = client.call(cmd).await?;
/// println!("String commands: {:?}", commands);
/// # Ok(())
/// # }
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
/// Returns a log of ACL security events such as authentication failures and command rejections.
/// Can retrieve a specific number of recent entries or reset the entire log.
///
/// # Request
/// - `count` (optional): Number of recent log entries to return
/// - `reset` (method): Reset the log instead of reading it
///
/// # Response
/// Returns `String` - Debug representation of log entries (simplified)
///
/// # Redis Version
/// Available since Redis 6.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::acl::AclLog;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Get last 10 log entries
/// let cmd = AclLog::new().count(10);
/// let log = client.call(cmd).await?;
/// println!("Recent ACL events: {}", log);
///
/// // Reset the log
/// let cmd = AclLog::reset();
/// client.call(cmd).await?;
/// # Ok(())
/// # }
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
/// Reloads the ACL configuration from the file specified in the aclfile configuration directive.
/// All current ACL configuration is replaced with the contents of the file. If the file has
/// errors, the command fails and the old ACL configuration remains in effect.
///
/// # Request
/// (no parameters)
///
/// # Response
/// Returns `()` - Always returns OK on success
///
/// # Redis Version
/// Available since Redis 6.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::acl::AclLoad;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let cmd = AclLoad::new();
/// client.call(cmd).await?;
/// println!("ACL configuration reloaded from file");
/// # Ok(())
/// # }
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
/// Saves the current ACL configuration to the file specified in the aclfile configuration
/// directive. The file is completely overwritten with the current in-memory ACL configuration.
///
/// # Request
/// (no parameters)
///
/// # Response
/// Returns `()` - Always returns OK on success
///
/// # Redis Version
/// Available since Redis 6.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::acl::AclSave;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// let cmd = AclSave::new();
/// client.call(cmd).await?;
/// println!("ACL configuration saved to file");
/// # Ok(())
/// # }
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
/// Generates a cryptographically secure pseudorandom password suitable for use with ACL SETUSER.
/// The default output is a 256-bit (64 hex characters) password, but you can specify a different
/// bit length. Useful for creating strong passwords programmatically.
///
/// # Request
/// - `bits` (optional): Number of bits for the password (default: 256)
///
/// # Response
/// Returns `String` - The generated password as a hexadecimal string
///
/// # Redis Version
/// Available since Redis 6.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::acl::AclGenPass;
/// use redis_tower::RedisClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("127.0.0.1:6379").await?;
/// // Generate default 256-bit password
/// let password = client.call(AclGenPass::new()).await?;
/// println!("Generated password: {}", password);
///
/// // Generate 128-bit password
/// let cmd = AclGenPass::new().bits(128);
/// let password = client.call(cmd).await?;
/// # Ok(())
/// # }
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

/// ACL HELP command - Get help text for ACL subcommands
///
/// Available since Redis 6.0.0.
#[derive(Debug, Clone, Copy)]
pub struct AclHelp;

impl AclHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for AclHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::commands::Command for AclHelp {
    type Response = Vec<String>;

    fn to_frame(&self) -> crate::codec::Frame {
        crate::codec::Frame::Array(vec![
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from("ACL"))),
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

/// ACL DRYRUN command - Test command permissions without execution (Redis 7.0+)
///
/// Simulates command execution to test if a user has permission to run it.
///
/// Available since Redis 7.0.0.
#[derive(Debug, Clone)]
pub struct AclDryRun {
    username: String,
    command: Vec<String>,
}

impl AclDryRun {
    /// Create a new ACL DRYRUN command
    pub fn new(username: impl Into<String>, command: Vec<impl Into<String>>) -> Self {
        Self {
            username: username.into(),
            command: command.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl Command for AclDryRun {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("ACL"))),
            Frame::BulkString(Some(Bytes::from("DRYRUN"))),
            Frame::BulkString(Some(Bytes::from(self.username.clone()))),
        ];
        for arg in &self.command {
            frames.push(Frame::BulkString(Some(Bytes::from(arg.clone()))));
        }
        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}
