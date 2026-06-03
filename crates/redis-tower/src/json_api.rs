//! High-level RedisJSON API with automatic serde serialization/deserialization.
//!
//! This module provides [`Json`], a typed wrapper around the raw `JSON.*`
//! commands that automatically serializes values with [`serde_json`] on write
//! and deserializes on read.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::Json;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize, Debug, PartialEq)]
//! struct User { name: String, age: u32 }
//!
//! let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let mut json = Json::new(&mut conn);
//!
//! json.set("user:1", "$", &User { name: "Alice".into(), age: 30 }).await?;
//! let user: User = json.get("user:1", "$").await?;
//! assert_eq!(user, User { name: "Alice".into(), age: 30 });
//! ```

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use redis_tower_commands::{JsonDel, JsonGet, JsonMGet, JsonNumIncrBy, JsonSet};
use redis_tower_core::RedisError;

use crate::RedisExecutor;

/// High-level RedisJSON API with automatic serde serialization.
///
/// Wraps a mutable reference to any [`RedisExecutor`] and provides typed
/// `set`/`get`/`del` methods that handle JSON serialization transparently.
///
/// # Example
///
/// ```ignore
/// use redis_tower::Json;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize, Debug, PartialEq)]
/// struct User { name: String, age: u32 }
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
/// let mut json = Json::new(&mut conn);
///
/// json.set("user:1", "$", &User { name: "Alice".into(), age: 30 }).await?;
/// let user: User = json.get("user:1", "$").await?;
/// ```
pub struct Json<'a, C> {
    conn: &'a mut C,
}

impl<'a, C: RedisExecutor> Json<'a, C> {
    /// Create a new JSON API handle wrapping the given executor.
    pub fn new(conn: &'a mut C) -> Self {
        Self { conn }
    }

    /// Set a value at a JSON path, serializing with serde.
    ///
    /// Serializes `value` to a JSON string and stores it at `path` in the
    /// given key. Creates the key if it does not exist.
    pub async fn set<T: Serialize>(
        &mut self,
        key: &str,
        path: &str,
        value: &T,
    ) -> Result<(), RedisError> {
        let json_str = serde_json::to_string(value)?;
        self.conn.execute(JsonSet::new(key, path, json_str)).await
    }

    /// Get a value at a JSON path, deserializing with serde.
    ///
    /// `JSON.GET` with `$` paths returns a JSON array wrapping the result
    /// (e.g. `[{"name":"Alice"}]`). This method automatically unwraps the
    /// outer array for paths starting with `$`.
    pub async fn get<T: DeserializeOwned>(
        &mut self,
        key: &str,
        path: &str,
    ) -> Result<T, RedisError> {
        let response = self.conn.execute(JsonGet::new(key).path(path)).await?;
        let bytes = response.ok_or(RedisError::TypeMismatch {
            expected: "JSON value",
        })?;
        parse_json_response::<T>(&bytes, path)
    }

    /// Get a value, returning `None` if the key does not exist.
    pub async fn get_opt<T: DeserializeOwned>(
        &mut self,
        key: &str,
        path: &str,
    ) -> Result<Option<T>, RedisError> {
        let response = self.conn.execute(JsonGet::new(key).path(path)).await?;
        match response {
            None => Ok(None),
            Some(bytes) => parse_json_response::<T>(&bytes, path).map(Some),
        }
    }

    /// Delete a JSON path. Returns the number of paths deleted.
    pub async fn del(&mut self, key: &str, path: &str) -> Result<i64, RedisError> {
        self.conn.execute(JsonDel::new(key).path(path)).await
    }

    /// Increment a numeric value at a path by `value`. Returns the raw
    /// response bytes from the server.
    pub async fn incr_by(
        &mut self,
        key: &str,
        path: &str,
        value: f64,
    ) -> Result<Bytes, RedisError> {
        self.conn
            .execute(JsonNumIncrBy::new(key, path, value))
            .await
    }

    /// Get values from multiple keys at the same path, deserializing each.
    ///
    /// Returns `None` for keys where the path does not exist.
    pub async fn mget<T: DeserializeOwned>(
        &mut self,
        keys: &[&str],
        path: &str,
    ) -> Result<Vec<Option<T>>, RedisError> {
        let responses = self
            .conn
            .execute(JsonMGet::new(keys.iter().copied(), path))
            .await?;
        responses
            .into_iter()
            .map(|opt_bytes| match opt_bytes {
                None => Ok(None),
                Some(bytes) => parse_json_response::<T>(&bytes, path).map(Some),
            })
            .collect()
    }
}

/// Parse a JSON response from Redis, handling the `$` path array wrapper.
fn parse_json_response<T: DeserializeOwned>(bytes: &[u8], path: &str) -> Result<T, RedisError> {
    let json_str = std::str::from_utf8(bytes).map_err(|_| RedisError::TypeMismatch {
        expected: "valid UTF-8",
    })?;

    if path.starts_with('$') {
        // JSON.GET with $ paths returns "[value]" -- unwrap the outer array.
        let arr: Vec<serde_json::Value> = serde_json::from_str(json_str)?;
        let first = arr.into_iter().next().ok_or(RedisError::TypeMismatch {
            expected: "non-empty JSON array",
        })?;
        Ok(serde_json::from_value(first)?)
    } else {
        Ok(serde_json::from_str(json_str)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::{Command, Frame};
    use serde::{Deserialize, Serialize};
    use std::collections::VecDeque;
    use std::future::Future;

    /// A mock executor that returns pre-configured frames.
    struct MockRedis {
        responses: VecDeque<Frame>,
    }

    impl MockRedis {
        fn new(responses: Vec<Frame>) -> Self {
            Self {
                responses: VecDeque::from(responses),
            }
        }
    }

    impl RedisExecutor for MockRedis {
        fn execute<Cmd: Command>(
            &mut self,
            cmd: Cmd,
        ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
            let frame = self.responses.pop_front().unwrap_or(Frame::Null);
            async move { cmd.parse_response(frame) }
        }
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct User {
        name: String,
        age: u32,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Address {
        city: String,
        zip: String,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Profile {
        user: User,
        address: Address,
    }

    #[tokio::test]
    async fn set_get_roundtrip() {
        let user = User {
            name: "Alice".into(),
            age: 30,
        };
        let serialized = serde_json::to_string(&user).unwrap();
        // JSON.GET with $ returns an array-wrapped value.
        let get_response = format!("[{serialized}]");

        let mut mock = MockRedis::new(vec![
            // Response for JSON.SET: OK
            Frame::SimpleString(Bytes::from("OK")),
            // Response for JSON.GET: the array-wrapped JSON
            Frame::BulkString(Some(Bytes::from(get_response))),
        ]);

        let mut json = Json::new(&mut mock);
        json.set("user:1", "$", &user).await.unwrap();
        let result: User = json.get("user:1", "$").await.unwrap();
        assert_eq!(result, user);
    }

    #[tokio::test]
    async fn get_opt_returns_none_for_missing_key() {
        let mut mock = MockRedis::new(vec![Frame::Null]);
        let mut json = Json::new(&mut mock);
        let result: Option<User> = json.get_opt("missing", "$").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn get_opt_returns_some_for_existing_key() {
        let user = User {
            name: "Bob".into(),
            age: 25,
        };
        let response = format!("[{}]", serde_json::to_string(&user).unwrap());
        let mut mock = MockRedis::new(vec![Frame::BulkString(Some(Bytes::from(response)))]);
        let mut json = Json::new(&mut mock);
        let result: Option<User> = json.get_opt("user:2", "$").await.unwrap();
        assert_eq!(result, Some(user));
    }

    #[tokio::test]
    async fn get_with_nested_struct() {
        let profile = Profile {
            user: User {
                name: "Charlie".into(),
                age: 40,
            },
            address: Address {
                city: "Portland".into(),
                zip: "97201".into(),
            },
        };
        let response = format!("[{}]", serde_json::to_string(&profile).unwrap());
        let mut mock = MockRedis::new(vec![Frame::BulkString(Some(Bytes::from(response)))]);
        let mut json = Json::new(&mut mock);
        let result: Profile = json.get("profile:1", "$").await.unwrap();
        assert_eq!(result, profile);
    }

    #[tokio::test]
    async fn set_with_dollar_path() {
        let user = User {
            name: "Dana".into(),
            age: 22,
        };
        let mut mock = MockRedis::new(vec![Frame::SimpleString(Bytes::from("OK"))]);
        let mut json = Json::new(&mut mock);
        // Should not panic or error.
        json.set("user:3", "$", &user).await.unwrap();
    }

    #[tokio::test]
    async fn del_removes_value() {
        // JSON.DEL returns the number of paths deleted.
        let mut mock = MockRedis::new(vec![Frame::Integer(1)]);
        let mut json = Json::new(&mut mock);
        let count = json.del("user:1", "$").await.unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn incr_by_works() {
        let mut mock = MockRedis::new(vec![Frame::BulkString(Some(Bytes::from("[42]")))]);
        let mut json = Json::new(&mut mock);
        let result = json.incr_by("counter", "$.value", 5.0).await.unwrap();
        assert_eq!(result, Bytes::from("[42]"));
    }

    #[tokio::test]
    async fn get_without_dollar_path() {
        // Legacy-style path without $ should not unwrap an array.
        let user = User {
            name: "Eve".into(),
            age: 35,
        };
        let response = serde_json::to_string(&user).unwrap();
        let mut mock = MockRedis::new(vec![Frame::BulkString(Some(Bytes::from(response)))]);
        let mut json = Json::new(&mut mock);
        let result: User = json.get("user:4", ".").await.unwrap();
        assert_eq!(result, user);
    }

    #[tokio::test]
    async fn mget_deserializes_multiple_keys() {
        let alice = User {
            name: "Alice".into(),
            age: 30,
        };
        let bob = User {
            name: "Bob".into(),
            age: 25,
        };
        let alice_json = format!("[{}]", serde_json::to_string(&alice).unwrap());
        let bob_json = format!("[{}]", serde_json::to_string(&bob).unwrap());

        // JSON.MGET returns an array of bulk strings (or nulls).
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from(alice_json))),
            Frame::Null,
            Frame::BulkString(Some(Bytes::from(bob_json))),
        ]))]);

        let mut json = Json::new(&mut mock);
        let results: Vec<Option<User>> = json.mget(&["u:1", "u:2", "u:3"], "$").await.unwrap();
        assert_eq!(results, vec![Some(alice), None, Some(bob)]);
    }
}
