//! # JSON Client
//!
//! Ergonomic, typed client over RedisJSON. Values are serialized to and from
//! JSON with `serde_json`, so callers work with their own Rust types rather
//! than raw JSON strings or [`Frame`] values.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::RedisClient;
//! use redis_tower_modules::json::JsonClient;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, Debug, PartialEq)]
//! struct User {
//!     name: String,
//!     age: u32,
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("redis://127.0.0.1:6379").await?;
//! let mut json = JsonClient::new(client);
//!
//! let user = User { name: "Ada".into(), age: 36 };
//! json.set("user:1", "$", &user).await?;
//!
//! let fetched: Option<User> = json.get("user:1", "$").await?;
//! assert_eq!(fetched.unwrap().name, "Ada");
//! # Ok(())
//! # }
//! ```

use redis_tower::RedisExecutor;
use redis_tower::commands::{
    JsonArrAppend, JsonArrLen, JsonDel, JsonGet, JsonMGet, JsonMerge, JsonObjKeys, JsonSet,
    JsonStrLen, JsonType,
};
use redis_tower_core::{Frame, RedisError};
use serde::{Serialize, de::DeserializeOwned};

/// High-level client for RedisJSON operations.
///
/// Wraps any underlying executor `C` and exposes typed `get`/`set`/`del`
/// operations that handle serialization and response parsing internally.
///
/// Accepts both owned executors (e.g. `MultiplexedClient`, which is `Clone`)
/// and mutable references (e.g. `&mut RedisConnection`, `&mut RedisClient`),
/// thanks to the blanket `impl RedisExecutor for &mut C`.
///
/// # Example
///
/// ```ignore
/// use redis_tower_modules::json::JsonClient;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize, Debug, PartialEq)]
/// struct Point { x: f64, y: f64 }
///
/// # async fn run(mut conn: impl redis_tower::RedisExecutor) -> Result<(), Box<dyn std::error::Error>> {
/// let mut json = JsonClient::new(&mut conn);
/// json.set("pt:1", "$", &Point { x: 1.0, y: 2.0 }).await?;
/// let pt: Option<Point> = json.get("pt:1", "$").await?;
/// assert!(pt.is_some());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct JsonClient<C> {
    client: C,
}

impl<C: RedisExecutor> JsonClient<C> {
    /// Create a new [`JsonClient`] wrapping the given executor.
    ///
    /// # Example
    ///
    /// ```ignore
    /// # use redis_tower::RedisClient;
    /// # use redis_tower_modules::json::JsonClient;
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = RedisClient::connect("redis://127.0.0.1:6379").await?;
    /// let mut json = JsonClient::new(client);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(client: C) -> Self {
        Self { client }
    }

    /// Set a JSON value at `key` and `path`, serializing `value` with `serde_json`.
    ///
    /// Creates the key if it does not exist. Returns `Ok(())` on success.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use redis_tower_modules::json::JsonClient;
    /// # use serde::Serialize;
    /// # #[derive(Serialize)]
    /// # struct User { name: String }
    /// # async fn run(mut json: JsonClient<impl redis_tower::RedisExecutor>) -> Result<(), redis_tower_core::RedisError> {
    /// let user = User { name: "Alice".into() };
    /// json.set("user:1", "$", &user).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set<T: Serialize>(
        &mut self,
        key: impl Into<String>,
        path: impl Into<String>,
        value: &T,
    ) -> Result<(), RedisError> {
        let json_str = serde_json::to_string(value)
            .map_err(|e| RedisError::Redis(format!("JSON serialization error: {e}")))?;
        self.client
            .execute(JsonSet::new(key.into(), path.into(), json_str))
            .await
    }

    /// Get the JSON value at `key` and `path`, deserializing the result.
    ///
    /// Returns `None` if the key or path does not exist. For `$` paths,
    /// automatically unwraps the outer array Redis returns.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use redis_tower_modules::json::JsonClient;
    /// # use serde::Deserialize;
    /// # #[derive(Deserialize)]
    /// # struct User { name: String }
    /// # async fn run(mut json: JsonClient<impl redis_tower::RedisExecutor>) -> Result<(), redis_tower_core::RedisError> {
    /// let user: Option<User> = json.get("user:1", "$").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get<T: DeserializeOwned>(
        &mut self,
        key: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<Option<T>, RedisError> {
        let path = path.into();
        let response = self
            .client
            .execute(JsonGet::new(key.into()).path(path.clone()))
            .await?;
        match response {
            None => Ok(None),
            Some(bytes) => parse_json_response::<T>(&bytes, &path).map(Some),
        }
    }

    /// Delete the JSON value at `key` and `path`.
    ///
    /// Returns the number of paths deleted (as `u64`).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use redis_tower_modules::json::JsonClient;
    /// # async fn run(mut json: JsonClient<impl redis_tower::RedisExecutor>) -> Result<(), redis_tower_core::RedisError> {
    /// let deleted = json.del("user:1", "$").await?;
    /// assert_eq!(deleted, 1);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn del(
        &mut self,
        key: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<u64, RedisError> {
        let count = self
            .client
            .execute(JsonDel::new(key.into()).path(path.into()))
            .await?;
        Ok(count as u64)
    }

    /// Get JSON values for multiple keys at the given path.
    ///
    /// Returns `None` for keys where the path does not exist.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use redis_tower_modules::json::JsonClient;
    /// # use serde::Deserialize;
    /// # #[derive(Deserialize)]
    /// # struct User { name: String }
    /// # async fn run(mut json: JsonClient<impl redis_tower::RedisExecutor>) -> Result<(), redis_tower_core::RedisError> {
    /// let users: Vec<Option<User>> = json.mget(["user:1", "user:2"], "$").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn mget<T: DeserializeOwned>(
        &mut self,
        keys: impl IntoIterator<Item = impl Into<String>>,
        path: impl Into<String>,
    ) -> Result<Vec<Option<T>>, RedisError> {
        let path = path.into();
        let responses = self
            .client
            .execute(JsonMGet::new(keys, path.clone()))
            .await?;
        responses
            .into_iter()
            .map(|opt_bytes| match opt_bytes {
                None => Ok(None),
                Some(bytes) => parse_json_response::<T>(&bytes, &path).map(Some),
            })
            .collect()
    }

    /// Merge a JSON value into an existing document at `key` and `path`.
    ///
    /// Existing keys are overwritten, new keys are added, and setting a key
    /// to `null` removes it. Returns `Ok(())` on success.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use redis_tower_modules::json::JsonClient;
    /// # use serde::Serialize;
    /// # use std::collections::HashMap;
    /// # async fn run(mut json: JsonClient<impl redis_tower::RedisExecutor>) -> Result<(), redis_tower_core::RedisError> {
    /// let mut patch = HashMap::new();
    /// patch.insert("age", 31u32);
    /// json.merge("user:1", "$", &patch).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn merge<T: Serialize>(
        &mut self,
        key: impl Into<String>,
        path: impl Into<String>,
        value: &T,
    ) -> Result<(), RedisError> {
        let json_str = serde_json::to_string(value)
            .map_err(|e| RedisError::Redis(format!("JSON serialization error: {e}")))?;
        self.client
            .execute(JsonMerge::new(key.into(), path.into(), json_str))
            .await
    }

    /// Append values to a JSON array at `key` and `path`.
    ///
    /// Serializes each element in `values` to JSON and appends them.
    /// Returns the new length of the array at each matching path, or `None`
    /// if the path does not exist.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use redis_tower_modules::json::JsonClient;
    /// # async fn run(mut json: JsonClient<impl redis_tower::RedisExecutor>) -> Result<(), redis_tower_core::RedisError> {
    /// let lengths = json.arr_append("list:1", "$.items", &[4i32, 5, 6]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn arr_append<T: Serialize>(
        &mut self,
        key: impl Into<String>,
        path: impl Into<String>,
        values: &[T],
    ) -> Result<Vec<Option<i64>>, RedisError> {
        let mut cmd = JsonArrAppend::new(key.into(), path.into());
        for v in values {
            let json_str = serde_json::to_string(v)
                .map_err(|e| RedisError::Redis(format!("JSON serialization error: {e}")))?;
            cmd = cmd.value(json_str);
        }
        let frame = self.client.execute(cmd).await?;
        parse_optional_i64_array(frame)
    }

    /// Get the length of the JSON array at `key` and `path`.
    ///
    /// Returns `None` if the path does not exist or is not an array.
    /// For `$` paths that match multiple locations, returns the length
    /// of the first match.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use redis_tower_modules::json::JsonClient;
    /// # async fn run(mut json: JsonClient<impl redis_tower::RedisExecutor>) -> Result<(), redis_tower_core::RedisError> {
    /// let len = json.arr_len("list:1", "$.items").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn arr_len(
        &mut self,
        key: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<Option<i64>, RedisError> {
        let frame = self
            .client
            .execute(JsonArrLen::new(key.into()).path(path.into()))
            .await?;
        parse_first_optional_i64(frame)
    }

    /// Get the keys of the JSON object at `key` and `path`.
    ///
    /// Returns a flat list of key names. For `$` paths, returns the keys
    /// of the first matching object.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use redis_tower_modules::json::JsonClient;
    /// # async fn run(mut json: JsonClient<impl redis_tower::RedisExecutor>) -> Result<(), redis_tower_core::RedisError> {
    /// let keys = json.obj_keys("user:1", "$").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn obj_keys(
        &mut self,
        key: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<Vec<String>, RedisError> {
        let frame = self
            .client
            .execute(JsonObjKeys::new(key.into()).path(path.into()))
            .await?;
        parse_obj_keys(frame)
    }

    /// Get the length of the JSON string at `key` and `path`.
    ///
    /// Returns `None` if the path does not exist or is not a string.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use redis_tower_modules::json::JsonClient;
    /// # async fn run(mut json: JsonClient<impl redis_tower::RedisExecutor>) -> Result<(), redis_tower_core::RedisError> {
    /// let len = json.str_len("user:1", "$.name").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn str_len(
        &mut self,
        key: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<Option<i64>, RedisError> {
        let frame = self
            .client
            .execute(JsonStrLen::new(key.into()).path(path.into()))
            .await?;
        parse_first_optional_i64(frame)
    }

    /// Check if a path exists in the JSON document at `key`.
    ///
    /// Returns `true` if `JSON.TYPE` returns a type for the path, `false`
    /// if the key or path does not exist.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use redis_tower_modules::json::JsonClient;
    /// # async fn run(mut json: JsonClient<impl redis_tower::RedisExecutor>) -> Result<(), redis_tower_core::RedisError> {
    /// let exists = json.path_exists("user:1", "$.name").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn path_exists(
        &mut self,
        key: impl Into<String>,
        path: impl Into<String>,
    ) -> Result<bool, RedisError> {
        let result = self
            .client
            .execute(JsonType::new(key.into()).path(path.into()))
            .await?;
        Ok(result.is_some())
    }
}

/// Parse a JSON response from Redis, handling the `$` path array wrapper.
///
/// `JSON.GET` with `$` paths returns `"[value]"` — this unwraps the outer
/// array and returns the first element deserialized as `T`.
fn parse_json_response<T: DeserializeOwned>(bytes: &[u8], path: &str) -> Result<T, RedisError> {
    let json_str = std::str::from_utf8(bytes).map_err(|_| RedisError::TypeMismatch {
        expected: "valid UTF-8",
    })?;

    if path.starts_with('$') {
        // JSON.GET with $ paths returns "[value]" -- unwrap the outer array.
        let arr: Vec<serde_json::Value> = serde_json::from_str(json_str)
            .map_err(|e| RedisError::Redis(format!("JSON parse error: {e}")))?;
        let first = arr.into_iter().next().ok_or(RedisError::TypeMismatch {
            expected: "non-empty JSON array",
        })?;
        serde_json::from_value(first)
            .map_err(|e| RedisError::Redis(format!("JSON deserialize error: {e}")))
    } else {
        serde_json::from_str(json_str)
            .map_err(|e| RedisError::Redis(format!("JSON deserialize error: {e}")))
    }
}

/// Parse a `Frame` as `Vec<Option<i64>>`.
///
/// Used for commands like `JSON.ARRAPPEND` that return an array of integer
/// lengths, where `null` indicates a path that did not exist.
fn parse_optional_i64_array(frame: Frame) -> Result<Vec<Option<i64>>, RedisError> {
    match frame {
        Frame::Array(Some(frames)) => frames
            .into_iter()
            .map(|f| match f {
                Frame::Integer(n) => Ok(Some(n)),
                Frame::Null => Ok(None),
                other => Err(RedisError::UnexpectedResponse {
                    expected: "integer or null",
                    actual: format!("{other:?}"),
                }),
            })
            .collect(),
        Frame::Null => Ok(vec![]),
        Frame::Integer(n) => Ok(vec![Some(n)]),
        other => Err(RedisError::UnexpectedResponse {
            expected: "array of integers",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse a `Frame` and return the first `Option<i64>` value.
///
/// Handles both:
/// - `$` path responses: `Frame::Array(Some([Frame::Integer(n) | Frame::Null, ...]))`
/// - Legacy path responses: `Frame::Integer(n)` or `Frame::Null`
fn parse_first_optional_i64(frame: Frame) -> Result<Option<i64>, RedisError> {
    match frame {
        Frame::Integer(n) => Ok(Some(n)),
        Frame::Null => Ok(None),
        Frame::Array(Some(mut frames)) => {
            if frames.is_empty() {
                return Ok(None);
            }
            match frames.swap_remove(0) {
                Frame::Integer(n) => Ok(Some(n)),
                Frame::Null => Ok(None),
                other => Err(RedisError::UnexpectedResponse {
                    expected: "integer or null",
                    actual: format!("{other:?}"),
                }),
            }
        }
        Frame::Array(None) => Ok(None),
        other => Err(RedisError::UnexpectedResponse {
            expected: "integer, null, or array",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse a `Frame` from `JSON.OBJKEYS` into a flat `Vec<String>`.
///
/// Handles both:
/// - `$` path responses: `Frame::Array(Some([Frame::Array(Some([bulk strings]))]))` —
///   the outer array is the JSONPath match list; we take the first inner array's keys.
/// - Legacy path responses: `Frame::Array(Some([bulk strings]))` — a flat array of keys.
fn parse_obj_keys(frame: Frame) -> Result<Vec<String>, RedisError> {
    fn bulk_to_string(f: Frame) -> Result<String, RedisError> {
        match f {
            Frame::BulkString(Some(b)) => {
                String::from_utf8(b.to_vec()).map_err(|_| RedisError::TypeMismatch {
                    expected: "valid UTF-8 key",
                })
            }
            Frame::SimpleString(b) => {
                String::from_utf8(b.to_vec()).map_err(|_| RedisError::TypeMismatch {
                    expected: "valid UTF-8 key",
                })
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string key",
                actual: format!("{other:?}"),
            }),
        }
    }

    match frame {
        Frame::Null => Ok(vec![]),
        Frame::Array(None) => Ok(vec![]),
        Frame::Array(Some(frames)) => {
            // Determine whether this is a $ response (outer = array of arrays)
            // or a legacy response (outer = array of bulk strings).
            match frames.first() {
                Some(Frame::Array(_)) => {
                    // $ path: outer array contains inner arrays of keys.
                    // Take the first match.
                    match frames.into_iter().next() {
                        Some(Frame::Array(Some(inner))) => {
                            inner.into_iter().map(bulk_to_string).collect()
                        }
                        Some(Frame::Array(None)) | Some(Frame::Null) | None => Ok(vec![]),
                        Some(other) => Err(RedisError::UnexpectedResponse {
                            expected: "array of keys",
                            actual: format!("{other:?}"),
                        }),
                    }
                }
                _ => {
                    // Legacy path: flat array of key bulk strings.
                    frames.into_iter().map(bulk_to_string).collect()
                }
            }
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "array or null",
            actual: format!("{other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
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

    #[tokio::test]
    async fn set_get_roundtrip() {
        let user = User {
            name: "Alice".into(),
            age: 30,
        };
        let serialized = serde_json::to_string(&user).unwrap();
        let get_response = format!("[{serialized}]");

        let mut mock = MockRedis::new(vec![
            Frame::SimpleString(Bytes::from("OK")),
            Frame::BulkString(Some(Bytes::from(get_response))),
        ]);

        let mut json = JsonClient::new(&mut mock);
        json.set("user:1", "$", &user).await.unwrap();
        let result: Option<User> = json.get("user:1", "$").await.unwrap();
        assert_eq!(result, Some(user));
    }

    #[tokio::test]
    async fn get_returns_none_for_missing_key() {
        let mut mock = MockRedis::new(vec![Frame::Null]);
        let mut json = JsonClient::new(&mut mock);
        let result: Option<User> = json.get("missing", "$").await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn del_returns_count() {
        let mut mock = MockRedis::new(vec![Frame::Integer(1)]);
        let mut json = JsonClient::new(&mut mock);
        let count = json.del("user:1", "$").await.unwrap();
        assert_eq!(count, 1u64);
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

        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from(alice_json))),
            Frame::Null,
            Frame::BulkString(Some(Bytes::from(bob_json))),
        ]))]);

        let mut json = JsonClient::new(&mut mock);
        let results: Vec<Option<User>> = json.mget(["u:1", "u:2", "u:3"], "$").await.unwrap();
        assert_eq!(results, vec![Some(alice), None, Some(bob)]);
    }

    #[tokio::test]
    async fn merge_sends_command() {
        let mut mock = MockRedis::new(vec![Frame::SimpleString(Bytes::from("OK"))]);
        let mut json = JsonClient::new(&mut mock);
        let patch = serde_json::json!({ "age": 31 });
        json.merge("user:1", "$", &patch).await.unwrap();
    }

    #[tokio::test]
    async fn arr_append_returns_lengths() {
        // JSON.ARRAPPEND returns an array of new lengths.
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![Frame::Integer(4)]))]);
        let mut json = JsonClient::new(&mut mock);
        let lengths = json
            .arr_append("list:1", "$.items", &[10i32, 20, 30])
            .await
            .unwrap();
        assert_eq!(lengths, vec![Some(4)]);
    }

    #[tokio::test]
    async fn arr_append_with_null_path() {
        // Path not found → null in the response array.
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![Frame::Null]))]);
        let mut json = JsonClient::new(&mut mock);
        let lengths = json
            .arr_append("list:1", "$.missing", &[1i32])
            .await
            .unwrap();
        assert_eq!(lengths, vec![None]);
    }

    #[tokio::test]
    async fn arr_len_returns_length() {
        // $ path: array wrapping an integer.
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![Frame::Integer(3)]))]);
        let mut json = JsonClient::new(&mut mock);
        let len = json.arr_len("list:1", "$.items").await.unwrap();
        assert_eq!(len, Some(3));
    }

    #[tokio::test]
    async fn arr_len_returns_none_for_missing_path() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![Frame::Null]))]);
        let mut json = JsonClient::new(&mut mock);
        let len = json.arr_len("list:1", "$.missing").await.unwrap();
        assert_eq!(len, None);
    }

    #[tokio::test]
    async fn arr_len_legacy_path() {
        let mut mock = MockRedis::new(vec![Frame::Integer(5)]);
        let mut json = JsonClient::new(&mut mock);
        let len = json.arr_len("list:1", ".items").await.unwrap();
        assert_eq!(len, Some(5));
    }

    #[tokio::test]
    async fn obj_keys_returns_keys_dollar_path() {
        // $ path: outer array contains inner array of keys.
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("name"))),
            Frame::BulkString(Some(Bytes::from("age"))),
        ]))]))]);
        let mut json = JsonClient::new(&mut mock);
        let keys = json.obj_keys("user:1", "$").await.unwrap();
        assert_eq!(keys, vec!["name", "age"]);
    }

    #[tokio::test]
    async fn obj_keys_returns_keys_legacy_path() {
        // Legacy path: flat array of keys.
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("name"))),
            Frame::BulkString(Some(Bytes::from("age"))),
        ]))]);
        let mut json = JsonClient::new(&mut mock);
        let keys = json.obj_keys("user:1", ".").await.unwrap();
        assert_eq!(keys, vec!["name", "age"]);
    }

    #[tokio::test]
    async fn obj_keys_returns_empty_for_null() {
        let mut mock = MockRedis::new(vec![Frame::Null]);
        let mut json = JsonClient::new(&mut mock);
        let keys = json.obj_keys("missing", "$").await.unwrap();
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn str_len_returns_length() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![Frame::Integer(5)]))]);
        let mut json = JsonClient::new(&mut mock);
        let len = json.str_len("user:1", "$.name").await.unwrap();
        assert_eq!(len, Some(5));
    }

    #[tokio::test]
    async fn str_len_returns_none_for_missing() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![Frame::Null]))]);
        let mut json = JsonClient::new(&mut mock);
        let len = json.str_len("user:1", "$.missing").await.unwrap();
        assert_eq!(len, None);
    }

    #[tokio::test]
    async fn path_exists_true_when_type_returned() {
        let mut mock = MockRedis::new(vec![Frame::BulkString(Some(Bytes::from("string")))]);
        let mut json = JsonClient::new(&mut mock);
        let exists = json.path_exists("user:1", "$.name").await.unwrap();
        assert!(exists);
    }

    #[tokio::test]
    async fn path_exists_false_for_null() {
        let mut mock = MockRedis::new(vec![Frame::Null]);
        let mut json = JsonClient::new(&mut mock);
        let exists = json.path_exists("user:1", "$.missing").await.unwrap();
        assert!(!exists);
    }
}
