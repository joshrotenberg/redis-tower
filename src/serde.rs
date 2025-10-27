//! Serde integration for Redis types
//!
//! Provides JSON serialization/deserialization support for storing and retrieving
//! Rust structs in Redis. This is an optional feature enabled with the `serde-json`
//! feature flag.
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
//! let stored: Option<User> = client.call(GetJson::new("user:123")).await?;
//! assert_eq!(Some(user), stored);
//! # Ok(())
//! # }
//! ```

use crate::types::RedisError;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Serialize a value to JSON bytes
///
/// # Example
///
/// ```
/// use redis_tower::serde_support::to_json;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct User {
///     name: String,
/// }
///
/// let user = User { name: "Alice".to_string() };
/// let bytes = to_json(&user).unwrap();
/// ```
pub fn to_json<T: Serialize>(value: &T) -> Result<Bytes, RedisError> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(|e| RedisError::Protocol(format!("JSON serialization error: {}", e)))
}

/// Deserialize JSON bytes to a value
///
/// # Example
///
/// ```
/// use redis_tower::serde_support::from_json;
/// use bytes::Bytes;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct User {
///     name: String,
/// }
///
/// let json = br#"{"name":"Alice"}"#;
/// let user: User = from_json(&Bytes::from(&json[..])).unwrap();
/// ```
pub fn from_json<T: for<'de> Deserialize<'de>>(bytes: &Bytes) -> Result<T, RedisError> {
    serde_json::from_slice(bytes)
        .map_err(|e| RedisError::Protocol(format!("JSON deserialization error: {}", e)))
}

/// Deserialize optional JSON bytes to a value
///
/// Returns Ok(None) if bytes is None, otherwise deserializes the JSON.
pub fn from_json_option<T: for<'de> Deserialize<'de>>(
    bytes: &Option<Bytes>,
) -> Result<Option<T>, RedisError> {
    match bytes {
        None => Ok(None),
        Some(b) => from_json(b).map(Some),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestStruct {
        id: u64,
        name: String,
        active: bool,
    }

    #[test]
    fn test_to_json() {
        let data = TestStruct {
            id: 123,
            name: "test".to_string(),
            active: true,
        };

        let bytes = to_json(&data).unwrap();
        let json_str = String::from_utf8(bytes.to_vec()).unwrap();

        assert!(json_str.contains(r#""id":123"#));
        assert!(json_str.contains(r#""name":"test"#));
        assert!(json_str.contains(r#""active":true"#));
    }

    #[test]
    fn test_from_json() {
        let json = br#"{"id":123,"name":"test","active":true}"#;
        let bytes = Bytes::from(&json[..]);

        let data: TestStruct = from_json(&bytes).unwrap();

        assert_eq!(data.id, 123);
        assert_eq!(data.name, "test");
        assert!(data.active);
    }

    #[test]
    fn test_roundtrip() {
        let original = TestStruct {
            id: 456,
            name: "roundtrip".to_string(),
            active: false,
        };

        let bytes = to_json(&original).unwrap();
        let decoded: TestStruct = from_json(&bytes).unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_from_json_option_none() {
        let result: Result<Option<TestStruct>, RedisError> = from_json_option(&None);
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_from_json_option_some() {
        let json = br#"{"id":789,"name":"optional","active":true}"#;
        let bytes = Some(Bytes::from(&json[..]));

        let result: Option<TestStruct> = from_json_option(&bytes).unwrap();
        assert!(result.is_some());

        let data = result.unwrap();
        assert_eq!(data.id, 789);
        assert_eq!(data.name, "optional");
        assert!(data.active);
    }

    #[test]
    fn test_invalid_json() {
        let invalid = Bytes::from("not valid json");
        let result: Result<TestStruct, RedisError> = from_json(&invalid);

        assert!(result.is_err());
        match result {
            Err(RedisError::Protocol(msg)) => {
                assert!(msg.contains("JSON deserialization error"));
            }
            _ => panic!("Expected Protocol error"),
        }
    }

    #[test]
    fn test_nested_structures() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Address {
            street: String,
            city: String,
        }

        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Person {
            name: String,
            addresses: Vec<Address>,
        }

        let person = Person {
            name: "Bob".to_string(),
            addresses: vec![
                Address {
                    street: "123 Main St".to_string(),
                    city: "Springfield".to_string(),
                },
                Address {
                    street: "456 Oak Ave".to_string(),
                    city: "Shelbyville".to_string(),
                },
            ],
        };

        let bytes = to_json(&person).unwrap();
        let decoded: Person = from_json(&bytes).unwrap();

        assert_eq!(person, decoded);
        assert_eq!(decoded.addresses.len(), 2);
    }
}
