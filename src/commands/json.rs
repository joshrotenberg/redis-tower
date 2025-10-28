//! JSON commands for serde integration
//!
//! This module provides convenient wrappers around Redis string commands (SET, GET, MSET)
//! that automatically serialize/deserialize Rust structs as JSON. These are NOT RedisJSON
//! module commands - they use standard Redis strings to store JSON-encoded data.
//!
//! # Use Cases
//!
//! - **Storing structured data** - Save Rust structs directly to Redis
//! - **Type-safe retrieval** - Get type-checked data back from Redis
//! - **API caching** - Cache API responses as structs
//! - **Session storage** - Store session data with strong typing
//! - **Configuration** - Store app configuration as JSON strings
//!
//! # Feature Flag
//!
//! This module requires the `serde-json` feature to be enabled.
//!
//! # vs RedisJSON Module
//!
//! **This module (JSON helpers)**:
//! - Uses standard Redis STRING commands
//! - Stores entire struct as JSON string
//! - No JSON path operations
//! - Works with any Redis instance
//! - Good for simple get/set of entire objects
//!
//! **RedisJSON module** (see `modules::json`):
//! - Requires RedisJSON module installed
//! - Supports JSON path queries (JSONPath)
//! - Can update nested fields atomically
//! - Better for complex JSON operations
//!
//! # Complete Example
//!
//! ```no_run
//! use redis_tower::commands::{GetJson, SetJson, MSetJson};
//! use redis_tower::RedisClient;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, Debug, PartialEq)]
//! struct User {
//!     id: u64,
//!     name: String,
//!     email: String,
//! }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("localhost:6379").await?;
//!
//! // Store struct as JSON
//! let user = User {
//!     id: 123,
//!     name: "Alice".to_string(),
//!     email: "alice@example.com".to_string(),
//! };
//! client.call(SetJson::new("user:123", &user)?).await?;
//!
//! // Retrieve and deserialize
//! let stored: Option<User> = client.call(GetJson::new("user:123")).await?;
//! assert_eq!(stored, Some(user));
//!
//! // Bulk store multiple structs
//! let users = vec![
//!     ("user:1", User { id: 1, name: "Alice".into(), email: "alice@example.com".into() }),
//!     ("user:2", User { id: 2, name: "Bob".into(), email: "bob@example.com".into() }),
//! ];
//! client.call(MSetJson::new(users)?).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Error Handling
//!
//! JSON commands can fail at two points:
//! 1. **Serialization** - When creating the command (returns `RedisError`)
//! 2. **Deserialization** - When parsing the response (returns `RedisError`)
//!
//! ```no_run
//! use redis_tower::commands::{SetJson, GetJson};
//! use redis_tower::RedisClient;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Config {
//!     timeout: u64,
//! }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let client = RedisClient::connect("localhost:6379").await?;
//! let config = Config { timeout: 5000 };
//!
//! // Serialization error caught here
//! let cmd = SetJson::new("config", &config)?;
//! client.call(cmd).await?;
//!
//! // Deserialization error caught here
//! let retrieved: Option<Config> = client.call(GetJson::new("config")).await?;
//! # Ok(())
//! # }
//! ```

use crate::codec::Frame;
use crate::commands::Command;
use crate::serde::{from_json_option, to_json};
use crate::types::RedisError;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// SET command with JSON serialization
///
/// Stores a Rust struct as a JSON string in Redis using the SET command. The struct
/// is serialized to JSON using serde_json and stored as a Redis string value.
///
/// **Important**: This replaces any existing value at the key. Use with TTL or other
/// SET options by using the regular `Set` command with manually serialized JSON.
///
/// # Request
/// - `key`: Redis key to store the JSON value
/// - `value`: Reference to any type that implements `Serialize`
///
/// # Response
/// Returns `()` on success
///
/// # Errors
/// - Serialization fails if the struct cannot be serialized to JSON
/// - Redis error if the SET command fails
///
/// # Examples
///
/// Basic struct storage:
/// ```no_run
/// use redis_tower::commands::SetJson;
/// use redis_tower::RedisClient;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Config {
///     timeout: u64,
///     retries: u32,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("localhost:6379").await?;
/// let config = Config { timeout: 5000, retries: 3 };
/// client.call(SetJson::new("app:config", &config)?).await?;
/// # Ok(())
/// # }
/// ```
///
/// Storing API response:
/// ```no_run
/// use redis_tower::commands::SetJson;
/// use redis_tower::RedisClient;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct ApiResponse {
///     status: u16,
///     data: Vec<String>,
///     timestamp: u64,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("localhost:6379").await?;
/// let response = ApiResponse {
///     status: 200,
///     data: vec!["item1".into(), "item2".into()],
///     timestamp: 1234567890,
/// };
///
/// client.call(SetJson::new("cache:api:users", &response)?).await?;
/// # Ok(())
/// # }
/// ```
///
/// Storing nested structures:
/// ```no_run
/// use redis_tower::commands::SetJson;
/// use redis_tower::RedisClient;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Address {
///     street: String,
///     city: String,
///     zip: String,
/// }
///
/// #[derive(Serialize)]
/// struct User {
///     id: u64,
///     name: String,
///     address: Address,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("localhost:6379").await?;
/// let user = User {
///     id: 123,
///     name: "Alice".into(),
///     address: Address {
///         street: "123 Main St".into(),
///         city: "Springfield".into(),
///         zip: "12345".into(),
///     },
/// };
///
/// client.call(SetJson::new("user:123", &user)?).await?;
/// # Ok(())
/// # }
/// ```
pub struct SetJson {
    key: String,
    value: Bytes,
}

impl SetJson {
    /// Create a new SET JSON command
    ///
    /// Serializes the value to JSON. Returns an error if serialization fails.
    pub fn new<T: Serialize>(key: impl Into<String>, value: &T) -> Result<Self, RedisError> {
        Ok(Self {
            key: key.into(),
            value: to_json(value)?,
        })
    }
}

impl Command for SetJson {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(self.value.clone())),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::Redis(String::from_utf8_lossy(&e).to_string())),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// GET command with JSON deserialization
///
/// Retrieves a JSON string from Redis and deserializes it to a Rust struct using
/// serde_json. The type parameter `T` specifies the target struct type.
///
/// **Important**: Returns `Option<T>` - `None` if the key doesn't exist, `Some(T)` if
/// deserialization succeeds. Deserialization errors return a `RedisError`.
///
/// # Request
/// - `key`: Redis key containing the JSON value
///
/// # Response
/// Returns `Option<T>`:
/// - `Some(T)`: Key exists and JSON was successfully deserialized
/// - `None`: Key does not exist in Redis
///
/// # Errors
/// - Deserialization fails if JSON is malformed or doesn't match struct shape
/// - Redis error if the GET command fails
///
/// # Examples
///
/// Basic struct retrieval:
/// ```no_run
/// use redis_tower::commands::GetJson;
/// use redis_tower::RedisClient;
/// use serde::Deserialize;
///
/// #[derive(Deserialize, Debug)]
/// struct Config {
///     timeout: u64,
///     retries: u32,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("localhost:6379").await?;
/// let config: Option<Config> = client.call(GetJson::new("app:config")).await?;
///
/// match config {
///     Some(cfg) => println!("Timeout: {}, Retries: {}", cfg.timeout, cfg.retries),
///     None => println!("Config not found"),
/// }
/// # Ok(())
/// # }
/// ```
///
/// Handling missing keys:
/// ```no_run
/// use redis_tower::commands::GetJson;
/// use redis_tower::RedisClient;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct User {
///     id: u64,
///     name: String,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("localhost:6379").await?;
/// let user: Option<User> = client.call(GetJson::new("user:999")).await?;
///
/// let user = user.unwrap_or(User {
///     id: 0,
///     name: "Guest".into(),
/// });
/// # Ok(())
/// # }
/// ```
///
/// Type inference from variable:
/// ```no_run
/// use redis_tower::commands::GetJson;
/// use redis_tower::RedisClient;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct ApiResponse {
///     status: u16,
///     data: Vec<String>,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("localhost:6379").await?;
/// // Type is inferred from variable type
/// let response: Option<ApiResponse> = client.call(GetJson::new("cache:api:users")).await?;
///
/// if let Some(resp) = response {
///     println!("Status: {}, {} items", resp.status, resp.data.len());
/// }
/// # Ok(())
/// # }
/// ```
///
/// Turbofish syntax:
/// ```no_run
/// use redis_tower::commands::GetJson;
/// use redis_tower::RedisClient;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Session {
///     user_id: u64,
///     expires: u64,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("localhost:6379").await?;
/// // Explicit type with turbofish
/// let session = client.call(GetJson::<Session>::new("session:abc123")).await?;
/// # Ok(())
/// # }
/// ```
pub struct GetJson<T> {
    key: String,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> GetJson<T> {
    /// Create a new GET JSON command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Command for GetJson<T>
where
    T: for<'de> Deserialize<'de>,
{
    type Response = Option<T>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("GET"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(data) => from_json_option(&data),
            Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::Redis(String::from_utf8_lossy(&e).to_string())),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// MSET command with JSON serialization
///
/// Sets multiple key-value pairs atomically with JSON serialization. All structs are
/// serialized to JSON and stored using a single MSET command.
///
/// **Important**: This is an atomic operation - all keys are set or none are. All values
/// must be the same type `T`.
///
/// # Request
/// - `pairs`: Vec of (key, value) tuples where all values implement `Serialize`
///
/// # Response
/// Returns `()` on success
///
/// # Errors
/// - Serialization fails if any struct cannot be serialized to JSON
/// - Redis error if the MSET command fails
///
/// # Examples
///
/// Bulk storing multiple structs:
/// ```no_run
/// use redis_tower::commands::MSetJson;
/// use redis_tower::RedisClient;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Status {
///     code: u16,
///     message: String,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("localhost:6379").await?;
/// let pairs = vec![
///     ("status:200", Status { code: 200, message: "OK".into() }),
///     ("status:404", Status { code: 404, message: "Not Found".into() }),
///     ("status:500", Status { code: 500, message: "Internal Server Error".into() }),
/// ];
///
/// client.call(MSetJson::new(pairs)?).await?;
/// # Ok(())
/// # }
/// ```
///
/// Caching multiple API responses:
/// ```no_run
/// use redis_tower::commands::MSetJson;
/// use redis_tower::RedisClient;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct UserProfile {
///     id: u64,
///     name: String,
///     email: String,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("localhost:6379").await?;
/// let users = vec![
///     ("user:1", UserProfile { id: 1, name: "Alice".into(), email: "alice@example.com".into() }),
///     ("user:2", UserProfile { id: 2, name: "Bob".into(), email: "bob@example.com".into() }),
///     ("user:3", UserProfile { id: 3, name: "Carol".into(), email: "carol@example.com".into() }),
/// ];
///
/// // Atomically store all user profiles
/// client.call(MSetJson::new(users)?).await?;
/// # Ok(())
/// # }
/// ```
///
/// Initializing application state:
/// ```no_run
/// use redis_tower::commands::MSetJson;
/// use redis_tower::RedisClient;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Config {
///     max_connections: u32,
///     timeout_ms: u64,
///     retry_count: u8,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("localhost:6379").await?;
/// // Set up different configs for different environments
/// let configs = vec![
///     ("config:dev", Config { max_connections: 10, timeout_ms: 5000, retry_count: 3 }),
///     ("config:staging", Config { max_connections: 50, timeout_ms: 3000, retry_count: 5 }),
///     ("config:prod", Config { max_connections: 100, timeout_ms: 1000, retry_count: 10 }),
/// ];
///
/// client.call(MSetJson::new(configs)?).await?;
/// # Ok(())
/// # }
/// ```
///
/// Building from iterator:
/// ```no_run
/// use redis_tower::commands::MSetJson;
/// use redis_tower::RedisClient;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Score {
///     user_id: u64,
///     points: u32,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// # let client = RedisClient::connect("localhost:6379").await?;
/// // Collect from iterator
/// let scores: Vec<_> = (1..=5)
///     .map(|id| {
///         let key = format!("score:{}", id);
///         let score = Score { user_id: id, points: id as u32 * 100 };
///         (key, score)
///     })
///     .collect();
///
/// client.call(MSetJson::new(scores)?).await?;
/// # Ok(())
/// # }
/// ```
pub struct MSetJson {
    pairs: Vec<(String, Bytes)>,
}

impl MSetJson {
    /// Create a new MSET JSON command
    ///
    /// Serializes all values to JSON. Returns an error if any serialization fails.
    pub fn new<T: Serialize>(pairs: Vec<(impl Into<String>, T)>) -> Result<Self, RedisError> {
        let mut serialized_pairs = Vec::with_capacity(pairs.len());

        for (key, value) in pairs {
            serialized_pairs.push((key.into(), to_json(&value)?));
        }

        Ok(Self {
            pairs: serialized_pairs,
        })
    }
}

impl Command for MSetJson {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![Frame::BulkString(Some(Bytes::from("MSET")))];

        for (key, value) in &self.pairs {
            args.push(Frame::BulkString(Some(Bytes::from(key.clone()))));
            args.push(Frame::BulkString(Some(value.clone())));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::Redis(String::from_utf8_lossy(&e).to_string())),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestUser {
        id: u64,
        name: String,
    }

    #[test]
    fn test_set_json_frame() {
        let user = TestUser {
            id: 123,
            name: "Alice".to_string(),
        };

        let cmd = SetJson::new("user:123", &user).unwrap();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("SET"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("user:123"))));
                // parts[2] is the JSON bytes
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_get_json_frame() {
        let cmd = GetJson::<TestUser>::new("user:123");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("GET"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("user:123"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_get_json_parse_response() {
        let user = TestUser {
            id: 456,
            name: "Bob".to_string(),
        };

        let json = to_json(&user).unwrap();
        let frame = Frame::BulkString(Some(json));

        let result: Option<TestUser> = GetJson::parse_response(frame).unwrap();
        assert!(result.is_some());

        let decoded = result.unwrap();
        assert_eq!(decoded, user);
    }

    #[test]
    fn test_get_json_parse_null() {
        let frame = Frame::Null;
        let result: Option<TestUser> = GetJson::parse_response(frame).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_mset_json_frame() {
        let user1 = TestUser {
            id: 1,
            name: "Alice".to_string(),
        };
        let user2 = TestUser {
            id: 2,
            name: "Bob".to_string(),
        };

        let pairs = vec![("user:1", user1), ("user:2", user2)];
        let cmd = MSetJson::new(pairs).unwrap();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5); // MSET + 2 pairs (key + value each)
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MSET"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("user:1"))));
                // parts[2] is JSON for user1
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("user:2"))));
                // parts[4] is JSON for user2
            }
            _ => panic!("Expected Array frame"),
        }
    }
}
