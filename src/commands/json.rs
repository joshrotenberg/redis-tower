//! JSON commands for serde integration
//!
//! These commands provide convenient JSON serialization/deserialization
//! for storing and retrieving Rust structs in Redis.
//!
//! # Feature Flag
//!
//! This module requires the `serde-json` feature to be enabled.
//!
//! # Example
//!
//! ```no_run
//! use redis_tower::commands::{GetJson, SetJson};
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
//! let user = User {
//!     id: 123,
//!     name: "Alice".to_string(),
//!     email: "alice@example.com".to_string(),
//! };
//!
//! // Store as JSON
//! client.call(SetJson::new("user:123", &user)?).await?;
//!
//! // Retrieve and deserialize
//! let stored: User = client.call(GetJson::new("user:123")).await?;
//! assert_eq!(user, stored);
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
/// Stores a Rust struct as JSON in Redis.
///
/// # Example
///
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
/// let client = RedisClient::connect("localhost:6379").await?;
///
/// let config = Config { timeout: 5000, retries: 3 };
/// client.call(SetJson::new("config", &config)?).await?;
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
/// Retrieves a JSON string from Redis and deserializes it to a Rust struct.
///
/// # Example
///
/// ```no_run
/// use redis_tower::commands::GetJson;
/// use redis_tower::RedisClient;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Config {
///     timeout: u64,
///     retries: u32,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = RedisClient::connect("localhost:6379").await?;
///
/// let config: Config = client.call(GetJson::new("config")).await?;
/// println!("Timeout: {}, Retries: {}", config.timeout, config.retries);
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
/// Sets multiple key-value pairs with JSON serialization.
///
/// # Example
///
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
/// let client = RedisClient::connect("localhost:6379").await?;
///
/// let pairs = vec![
///     ("status:200", Status { code: 200, message: "OK".into() }),
///     ("status:404", Status { code: 404, message: "Not Found".into() }),
/// ];
///
/// client.call(MSetJson::new(pairs)?).await?;
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
