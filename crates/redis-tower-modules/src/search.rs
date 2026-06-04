//! # Search Client
//!
//! Ergonomic, typed client over RediSearch. Query results are deserialized
//! into caller-supplied Rust types with `serde_json`, hiding the raw
//! [`Frame`](redis_tower::Frame) reply structure.
#![allow(unused_variables, dead_code)]

use redis_tower_core::RedisError;

/// High-level client for RediSearch operations.
///
/// Wraps any underlying executor `C` and exposes index management and typed
/// search operations.
#[derive(Debug, Clone)]
pub struct SearchClient<C> {
    client: C,
}

impl<C> SearchClient<C> {
    /// Create a new [`SearchClient`] wrapping the given executor.
    pub fn new(client: C) -> Self {
        Self { client }
    }

    /// Execute a full-text search on `index` with `query`, deserializing results.
    pub async fn search<T: serde::de::DeserializeOwned>(
        &mut self,
        index: impl AsRef<str>,
        query: impl AsRef<str>,
    ) -> Result<Vec<T>, RedisError> {
        todo!()
    }

    /// Create a search index with the given schema definition.
    pub async fn create_index(
        &mut self,
        index: impl AsRef<str>,
        schema: impl AsRef<str>,
    ) -> Result<(), RedisError> {
        todo!()
    }

    /// Drop a search index.
    pub async fn drop_index(&mut self, index: impl AsRef<str>) -> Result<(), RedisError> {
        todo!()
    }

    /// Return info about a search index.
    pub async fn info(
        &mut self,
        index: impl AsRef<str>,
    ) -> Result<std::collections::HashMap<String, String>, RedisError> {
        todo!()
    }
}
