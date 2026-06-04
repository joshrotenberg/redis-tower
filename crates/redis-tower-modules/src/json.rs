//! # JSON Client
//!
//! Ergonomic, typed client over RedisJSON. Values are serialized to and from
//! JSON with `serde_json`, so callers work with their own Rust types rather
//! than raw JSON strings or [`Frame`](redis_tower::Frame) values.
#![allow(unused_variables, dead_code)]

use redis_tower_core::RedisError;

/// High-level client for RedisJSON operations.
///
/// Wraps any underlying executor `C` and exposes typed `get`/`set`/`del`
/// operations that handle serialization and response parsing internally.
#[derive(Debug, Clone)]
pub struct JsonClient<C> {
    client: C,
}

impl<C> JsonClient<C> {
    /// Create a new [`JsonClient`] wrapping the given executor.
    pub fn new(client: C) -> Self {
        Self { client }
    }

    /// Set a JSON value at `key` and `path`, serializing `value` with `serde_json`.
    pub async fn set<T: serde::Serialize>(
        &mut self,
        key: impl AsRef<str>,
        path: impl AsRef<str>,
        value: &T,
    ) -> Result<(), RedisError> {
        todo!()
    }

    /// Get the JSON value at `key` and `path`, deserializing the result.
    pub async fn get<T: serde::de::DeserializeOwned>(
        &mut self,
        key: impl AsRef<str>,
        path: impl AsRef<str>,
    ) -> Result<T, RedisError> {
        todo!()
    }

    /// Delete the JSON value at `key` and `path`.
    pub async fn del(
        &mut self,
        key: impl AsRef<str>,
        path: impl AsRef<str>,
    ) -> Result<u64, RedisError> {
        todo!()
    }

    /// Get JSON values for multiple keys at the given path.
    pub async fn mget<T: serde::de::DeserializeOwned>(
        &mut self,
        keys: &[impl AsRef<str>],
        path: impl AsRef<str>,
    ) -> Result<Vec<Option<T>>, RedisError> {
        todo!()
    }
}
