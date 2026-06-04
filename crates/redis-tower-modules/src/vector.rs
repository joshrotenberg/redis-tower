//! # Vector Set Client
//!
//! Ergonomic client over Redis Vector Sets. Vectors are exchanged as `&[f32]`
//! slices and named elements as strings, hiding the raw
//! [`Frame`](redis_tower::Frame) reply structure.
#![allow(unused_variables, dead_code)]

use redis_tower_core::RedisError;

/// High-level client for Vector Set operations.
///
/// Wraps any underlying executor `C` and exposes typed add/get/knn operations
/// over named vector elements.
#[derive(Debug, Clone)]
pub struct VectorSetClient<C> {
    client: C,
}

impl<C> VectorSetClient<C> {
    /// Create a new [`VectorSetClient`] wrapping the given executor.
    pub fn new(client: C) -> Self {
        Self { client }
    }

    /// Add or update a vector element in the set.
    pub async fn add(
        &mut self,
        key: impl AsRef<str>,
        element: impl AsRef<str>,
        vector: &[f32],
    ) -> Result<(), RedisError> {
        todo!()
    }

    /// Retrieve the vector for a named element.
    pub async fn get(
        &mut self,
        key: impl AsRef<str>,
        element: impl AsRef<str>,
    ) -> Result<Option<Vec<f32>>, RedisError> {
        todo!()
    }

    /// Return the k nearest neighbours to the given query vector.
    pub async fn knn(
        &mut self,
        key: impl AsRef<str>,
        vector: &[f32],
        k: usize,
    ) -> Result<Vec<String>, RedisError> {
        todo!()
    }

    /// Remove a vector element from the set.
    pub async fn del(
        &mut self,
        key: impl AsRef<str>,
        element: impl AsRef<str>,
    ) -> Result<bool, RedisError> {
        todo!()
    }
}
