//! # Vector Set Client
//!
//! Ergonomic, typed client over Redis [Vector Sets]. Vectors are exchanged as
//! `Vec<f32>` and named elements as strings, hiding the raw
//! [`Frame`] reply structure. Similarity searches return
//! typed [`SimilarityResult`] values and introspection returns a structured
//! [`VectorSetInfo`].
//!
//! Vector Sets require Redis 8.0 or later.
//!
//! [Vector Sets]: https://redis.io/docs/latest/develop/data-types/vector-sets/
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::RedisConnection;
//! use redis_tower_modules::vector::{VectorSetClient, VectorQuery};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let mut vset = VectorSetClient::new(&mut conn, "points");
//!
//! // Add a few 3-dimensional vectors.
//! vset.add(vec![1.0, 0.0, 0.0], "a").await?;
//! vset.add(vec![0.9, 0.1, 0.0], "b").await?;
//! vset.add(vec![0.0, 1.0, 0.0], "c").await?;
//!
//! // Find the elements most similar to a query vector, with scores.
//! let query = VectorQuery::by_vector(vec![1.0, 0.0, 0.0])
//!     .count(2)
//!     .withscores();
//! let hits = vset.search(query).await?;
//! for hit in hits {
//!     println!("{} -> {:?}", hit.element, hit.score);
//! }
//! # Ok(())
//! # }
//! ```

use redis_tower::RedisExecutor;
use redis_tower::commands::{
    VAdd, VCard, VDelAttr, VDim, VEmb, VGetAttr, VInfo, VLinks, VRandMember, VRem, VSetAttr, VSim,
};
use redis_tower_core::{Frame, RedisError};

/// Quantization type for vector storage, re-exported from the command layer.
pub use redis_tower::commands::VQuantization;

/// A similarity search result: an element name and, optionally, its score.
#[derive(Debug, Clone)]
pub struct SimilarityResult {
    /// The element name.
    pub element: String,
    /// The similarity score, present only when the query requested scores.
    pub score: Option<f64>,
}

/// Information about a vector set, returned by [`VectorSetClient::info`].
#[derive(Debug, Clone, Default)]
pub struct VectorSetInfo {
    /// The quantization type in use (e.g. `"int8"`, `"f32"`, `"bin"`).
    pub quant_type: String,
    /// The dimensionality of the stored vectors.
    pub vector_dim: i64,
    /// The number of elements in the set.
    pub size: i64,
    /// The maximum node UID allocated in the underlying HNSW graph.
    pub max_node_uid: i64,
    /// The internal UID of the vector set.
    pub vset_uid: i64,
}

/// Options for adding a vector element with [`VectorSetClient::add_with_options`].
///
/// `Debug` and `Clone` are implemented by hand because [`VQuantization`] (from
/// the command layer) does not derive them.
#[derive(Default)]
pub struct VAddOptions {
    /// Reduce the stored vector to this many dimensions (random projection).
    pub reduce_dim: Option<u64>,
    /// Use check-and-set semantics when inserting.
    pub cas: bool,
    /// Maximum number of links per HNSW node.
    pub m: Option<u64>,
    /// EF construction parameter for the HNSW graph.
    pub ef: Option<u64>,
    /// Quantization type for the stored vector.
    pub quant: Option<VQuantization>,
}

/// Copy a [`VQuantization`] variant (it is a plain enum without `Clone`).
fn copy_quant(q: &VQuantization) -> VQuantization {
    match q {
        VQuantization::Q8 => VQuantization::Q8,
        VQuantization::Bf16 => VQuantization::Bf16,
        VQuantization::NoQuant => VQuantization::NoQuant,
    }
}

/// A short debug label for a [`VQuantization`] variant.
fn quant_label(q: &VQuantization) -> &'static str {
    match q {
        VQuantization::Q8 => "Q8",
        VQuantization::Bf16 => "Bf16",
        VQuantization::NoQuant => "NoQuant",
    }
}

impl Clone for VAddOptions {
    fn clone(&self) -> Self {
        Self {
            reduce_dim: self.reduce_dim,
            cas: self.cas,
            m: self.m,
            ef: self.ef,
            quant: self.quant.as_ref().map(copy_quant),
        }
    }
}

impl std::fmt::Debug for VAddOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VAddOptions")
            .field("reduce_dim", &self.reduce_dim)
            .field("cas", &self.cas)
            .field("m", &self.m)
            .field("ef", &self.ef)
            .field("quant", &self.quant.as_ref().map(quant_label))
            .finish()
    }
}

/// Target for a similarity search query.
pub enum QueryTarget {
    /// Search by an existing element name already present in the set.
    Element(String),
    /// Search by a raw vector of values.
    Values(Vec<f32>),
}

/// Builder for similarity search queries (`VSIM`).
///
/// Construct with [`VectorQuery::by_element`] or [`VectorQuery::by_vector`],
/// then chain options before passing to [`VectorSetClient::search`].
pub struct VectorQuery {
    target: QueryTarget,
    count: Option<u64>,
    ef: Option<u64>,
    filter: Option<String>,
    filter_ef: Option<u64>,
    withscores: bool,
    nothread: bool,
    truth: bool,
}

impl VectorQuery {
    /// Search for elements similar to an existing element in the set.
    pub fn by_element(element: impl Into<String>) -> Self {
        Self::with_target(QueryTarget::Element(element.into()))
    }

    /// Search for elements similar to the given vector.
    pub fn by_vector(vector: Vec<f32>) -> Self {
        Self::with_target(QueryTarget::Values(vector))
    }

    fn with_target(target: QueryTarget) -> Self {
        Self {
            target,
            count: None,
            ef: None,
            filter: None,
            filter_ef: None,
            withscores: false,
            nothread: false,
            truth: false,
        }
    }

    /// Limit the number of results returned.
    pub fn count(mut self, n: u64) -> Self {
        self.count = Some(n);
        self
    }

    /// Set the EF search parameter (search effort).
    pub fn ef(mut self, n: u64) -> Self {
        self.ef = Some(n);
        self
    }

    /// Filter results by an attribute expression.
    pub fn filter(mut self, expr: impl Into<String>) -> Self {
        self.filter = Some(expr.into());
        self
    }

    /// Set the EF parameter used while filtering.
    pub fn filter_ef(mut self, n: u64) -> Self {
        self.filter_ef = Some(n);
        self
    }

    /// Include similarity scores in the results.
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }

    /// Disable multi-threading for this query.
    pub fn nothread(mut self) -> Self {
        self.nothread = true;
        self
    }

    /// Use brute-force (exact) search instead of approximate search.
    pub fn truth(mut self) -> Self {
        self.truth = true;
        self
    }
}

/// High-level client for Vector Set operations, bound to a single key.
///
/// Holds a mutable borrow of an underlying [`RedisExecutor`] and a vector set
/// key, exposing typed add/query/introspection operations over the elements of
/// that set.
pub struct VectorSetClient<'a, C> {
    key: String,
    conn: &'a mut C,
}

impl<'a, C: RedisExecutor> VectorSetClient<'a, C> {
    /// Create a new [`VectorSetClient`] for `key`, borrowing `conn`.
    pub fn new(conn: &'a mut C, key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            conn,
        }
    }

    /// Add or update a vector element. Returns `true` if the element was newly
    /// added, `false` if an existing element was updated.
    pub async fn add(
        &mut self,
        vector: Vec<f32>,
        element: impl Into<String>,
    ) -> Result<bool, RedisError> {
        self.conn
            .execute(VAdd::new(self.key.clone(), vector, element.into()))
            .await
    }

    /// Add or update a vector element with explicit [`VAddOptions`].
    pub async fn add_with_options(
        &mut self,
        vector: Vec<f32>,
        element: impl Into<String>,
        options: VAddOptions,
    ) -> Result<bool, RedisError> {
        let mut cmd = VAdd::new(self.key.clone(), vector, element.into());
        if let Some(dim) = options.reduce_dim {
            cmd = cmd.reduce(dim);
        }
        if options.cas {
            cmd = cmd.cas();
        }
        if let Some(m) = options.m {
            cmd = cmd.m(m);
        }
        if let Some(ef) = options.ef {
            cmd = cmd.ef(ef);
        }
        if let Some(quant) = options.quant {
            cmd = cmd.quant(quant);
        }
        self.conn.execute(cmd).await
    }

    /// Remove an element. Returns `true` if it existed and was removed.
    pub async fn remove(&mut self, element: &str) -> Result<bool, RedisError> {
        self.conn
            .execute(VRem::new(self.key.clone(), element))
            .await
    }

    /// Find the elements most similar to the query target.
    pub async fn search(
        &mut self,
        query: VectorQuery,
    ) -> Result<Vec<SimilarityResult>, RedisError> {
        let mut cmd = match query.target {
            QueryTarget::Element(element) => VSim::by_element(self.key.clone(), element),
            QueryTarget::Values(vector) => VSim::by_values(self.key.clone(), vector),
        };
        if let Some(n) = query.count {
            cmd = cmd.count(n);
        }
        if let Some(n) = query.ef {
            cmd = cmd.ef(n);
        }
        if let Some(expr) = query.filter {
            cmd = cmd.filter(expr);
        }
        if let Some(n) = query.filter_ef {
            cmd = cmd.filter_ef(n);
        }
        if query.withscores {
            cmd = cmd.withscores();
        }
        if query.nothread {
            cmd = cmd.nothread();
        }
        if query.truth {
            cmd = cmd.truth();
        }
        let raw = self.conn.execute(cmd).await?;
        Ok(to_results(raw))
    }

    /// Retrieve the embedding (vector) for an element.
    pub async fn embedding(&mut self, element: &str) -> Result<Vec<f64>, RedisError> {
        self.conn
            .execute(VEmb::new(self.key.clone(), element))
            .await
    }

    /// Return the neighbour links of an element, with similarity scores.
    pub async fn links(&mut self, element: &str) -> Result<Vec<SimilarityResult>, RedisError> {
        let raw = self
            .conn
            .execute(VLinks::new(self.key.clone(), element).withscores())
            .await?;
        Ok(to_results(raw))
    }

    /// Return one or more random elements from the set. A negative `count`
    /// allows duplicates.
    pub async fn random(&mut self, count: Option<i64>) -> Result<Vec<String>, RedisError> {
        let mut cmd = VRandMember::new(self.key.clone());
        if let Some(n) = count {
            cmd = cmd.count(n);
        }
        let raw = self.conn.execute(cmd).await?;
        Ok(raw
            .into_iter()
            .map(|b| String::from_utf8_lossy(&b).into_owned())
            .collect())
    }

    /// Set a JSON attribute string on an element. Returns `true` on success.
    pub async fn set_attr(&mut self, element: &str, json: &str) -> Result<bool, RedisError> {
        self.conn
            .execute(VSetAttr::new(self.key.clone(), element, json))
            .await
    }

    /// Get the JSON attribute string for an element, or `None` if unset.
    pub async fn get_attr(&mut self, element: &str) -> Result<Option<String>, RedisError> {
        self.conn
            .execute(VGetAttr::new(self.key.clone(), element))
            .await
    }

    /// Delete the attribute from an element. Returns `true` if one was removed.
    pub async fn del_attr(&mut self, element: &str) -> Result<bool, RedisError> {
        self.conn
            .execute(VDelAttr::new(self.key.clone(), element))
            .await
    }

    /// Return the number of elements in the set.
    pub async fn cardinality(&mut self) -> Result<i64, RedisError> {
        self.conn.execute(VCard::new(self.key.clone())).await
    }

    /// Return the dimensionality of the vectors in the set.
    pub async fn dimensions(&mut self) -> Result<i64, RedisError> {
        self.conn.execute(VDim::new(self.key.clone())).await
    }

    /// Return structured information about the set.
    pub async fn info(&mut self) -> Result<VectorSetInfo, RedisError> {
        let frames = self.conn.execute(VInfo::new(self.key.clone())).await?;
        Ok(parse_info(frames))
    }

    /// Set a typed attribute on an element, serializing with `serde_json`.
    #[cfg(feature = "serde")]
    pub async fn set_attr_typed<T: serde::Serialize>(
        &mut self,
        element: &str,
        attr: &T,
    ) -> Result<bool, RedisError> {
        let json = serde_json::to_string(attr)
            .map_err(|e| RedisError::Redis(format!("JSON serialization error: {e}")))?;
        self.set_attr(element, &json).await
    }

    /// Get a typed attribute from an element, deserializing with `serde_json`.
    /// Returns `None` if no attribute is set.
    #[cfg(feature = "serde")]
    pub async fn get_attr_typed<T: serde::de::DeserializeOwned>(
        &mut self,
        element: &str,
    ) -> Result<Option<T>, RedisError> {
        match self.get_attr(element).await? {
            None => Ok(None),
            Some(json) => {
                let value = serde_json::from_str(&json)
                    .map_err(|e| RedisError::Redis(format!("JSON deserialize error: {e}")))?;
                Ok(Some(value))
            }
        }
    }
}

/// Convert a raw `(bytes, score)` response into typed [`SimilarityResult`]s.
fn to_results<B: AsRef<[u8]>>(raw: Vec<(B, Option<f64>)>) -> Vec<SimilarityResult> {
    raw.into_iter()
        .map(|(name, score)| SimilarityResult {
            element: String::from_utf8_lossy(name.as_ref()).into_owned(),
            score,
        })
        .collect()
}

/// Parse a flat alternating key/value `VINFO` reply into [`VectorSetInfo`].
fn parse_info(frames: Vec<Frame>) -> VectorSetInfo {
    let mut info = VectorSetInfo::default();
    let mut iter = frames.into_iter();
    while let Some(key_frame) = iter.next() {
        let Some(value_frame) = iter.next() else {
            break;
        };
        let Some(key) = frame_to_string(&key_frame) else {
            continue;
        };
        match key.as_str() {
            "quant-type" | "quant_type" => {
                if let Some(s) = frame_to_string(&value_frame) {
                    info.quant_type = s;
                }
            }
            "vector-dim" | "vector_dim" => {
                if let Some(n) = frame_to_i64(&value_frame) {
                    info.vector_dim = n;
                }
            }
            "size" => {
                if let Some(n) = frame_to_i64(&value_frame) {
                    info.size = n;
                }
            }
            "max-node-uid" | "max_node_uid" => {
                if let Some(n) = frame_to_i64(&value_frame) {
                    info.max_node_uid = n;
                }
            }
            "vset-uid" | "vset_uid" => {
                if let Some(n) = frame_to_i64(&value_frame) {
                    info.vset_uid = n;
                }
            }
            _ => {}
        }
    }
    info
}

/// Extract a UTF-8 string from a string-like frame.
fn frame_to_string(frame: &Frame) -> Option<String> {
    match frame {
        Frame::BulkString(Some(b)) => Some(String::from_utf8_lossy(b).into_owned()),
        Frame::SimpleString(b) => Some(String::from_utf8_lossy(b).into_owned()),
        _ => None,
    }
}

/// Extract an integer from a numeric frame (integer, double, or numeric string).
fn frame_to_i64(frame: &Frame) -> Option<i64> {
    match frame {
        Frame::Integer(n) => Some(*n),
        Frame::Double(d) => Some(*d as i64),
        Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).parse().ok(),
        Frame::SimpleString(b) => String::from_utf8_lossy(b).parse().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use redis_tower_core::Command;
    use std::collections::VecDeque;
    use std::future::Future;

    /// A mock executor that returns pre-configured frames in order.
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

    fn bulk(s: &str) -> Frame {
        Frame::BulkString(Some(Bytes::from(s.to_string())))
    }

    #[tokio::test]
    async fn add_returns_true_when_added() {
        let mut mock = MockRedis::new(vec![Frame::Integer(1)]);
        let mut vset = VectorSetClient::new(&mut mock, "k");
        let added = vset.add(vec![1.0, 2.0, 3.0], "a").await.unwrap();
        assert!(added);
    }

    #[tokio::test]
    async fn add_returns_false_when_updated() {
        let mut mock = MockRedis::new(vec![Frame::Integer(0)]);
        let mut vset = VectorSetClient::new(&mut mock, "k");
        let added = vset.add(vec![1.0, 2.0, 3.0], "a").await.unwrap();
        assert!(!added);
    }

    #[tokio::test]
    async fn remove_returns_true_when_removed() {
        let mut mock = MockRedis::new(vec![Frame::Integer(1)]);
        let mut vset = VectorSetClient::new(&mut mock, "k");
        assert!(vset.remove("a").await.unwrap());
    }

    #[tokio::test]
    async fn search_without_scores() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![bulk("a"), bulk("b")]))]);
        let mut vset = VectorSetClient::new(&mut mock, "k");
        let results = vset
            .search(VectorQuery::by_vector(vec![1.0, 0.0, 0.0]))
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].element, "a");
        assert_eq!(results[0].score, None);
        assert_eq!(results[1].element, "b");
        assert_eq!(results[1].score, None);
    }

    #[tokio::test]
    async fn search_with_scores() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![
            bulk("a"),
            bulk("0.99"),
            bulk("b"),
            bulk("0.50"),
        ]))]);
        let mut vset = VectorSetClient::new(&mut mock, "k");
        let results = vset
            .search(VectorQuery::by_vector(vec![1.0, 0.0, 0.0]).withscores())
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].element, "a");
        assert_eq!(results[0].score, Some(0.99));
        assert_eq!(results[1].element, "b");
        assert_eq!(results[1].score, Some(0.50));
    }

    #[tokio::test]
    async fn embedding_returns_floats() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![
            bulk("1.5"),
            bulk("2.5"),
            bulk("3.5"),
        ]))]);
        let mut vset = VectorSetClient::new(&mut mock, "k");
        let emb = vset.embedding("a").await.unwrap();
        assert_eq!(emb, vec![1.5, 2.5, 3.5]);
    }

    #[tokio::test]
    async fn cardinality_returns_count() {
        let mut mock = MockRedis::new(vec![Frame::Integer(3)]);
        let mut vset = VectorSetClient::new(&mut mock, "k");
        assert_eq!(vset.cardinality().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn info_parses_flat_kv() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![
            bulk("quant-type"),
            bulk("int8"),
            bulk("vector-dim"),
            Frame::Integer(3),
            bulk("size"),
            Frame::Integer(42),
            bulk("max-node-uid"),
            Frame::Integer(7),
            bulk("vset-uid"),
            Frame::Integer(9),
        ]))]);
        let mut vset = VectorSetClient::new(&mut mock, "k");
        let info = vset.info().await.unwrap();
        assert_eq!(info.quant_type, "int8");
        assert_eq!(info.vector_dim, 3);
        assert_eq!(info.size, 42);
        assert_eq!(info.max_node_uid, 7);
        assert_eq!(info.vset_uid, 9);
    }

    #[tokio::test]
    async fn random_returns_strings() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![bulk("a"), bulk("b")]))]);
        let mut vset = VectorSetClient::new(&mut mock, "k");
        let members = vset.random(Some(2)).await.unwrap();
        assert_eq!(members, vec!["a".to_string(), "b".to_string()]);
    }

    #[tokio::test]
    async fn get_attr_returns_none_for_null() {
        let mut mock = MockRedis::new(vec![Frame::Null]);
        let mut vset = VectorSetClient::new(&mut mock, "k");
        assert_eq!(vset.get_attr("a").await.unwrap(), None);
    }

    #[tokio::test]
    async fn get_attr_returns_string() {
        let mut mock = MockRedis::new(vec![Frame::BulkString(Some(Bytes::from("{\"x\":1}")))]);
        let mut vset = VectorSetClient::new(&mut mock, "k");
        assert_eq!(
            vset.get_attr("a").await.unwrap(),
            Some("{\"x\":1}".to_string())
        );
    }
}
