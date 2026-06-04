//! # Search Client
//!
//! Ergonomic, typed client over RediSearch. Query results are deserialized
//! into caller-supplied Rust types with `serde_json`, hiding the raw
//! [`Frame`] reply structure.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::RedisClient;
//! use redis_tower_modules::search::{IndexBuilder, SearchClient, SearchQuery, SortDir};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct Product {
//!     name: String,
//!     price: String,
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut client = RedisClient::connect("redis://127.0.0.1:6379").await?;
//! let mut search = SearchClient::new(&mut client);
//!
//! search
//!     .create_index(
//!         IndexBuilder::new("products_idx")
//!             .on_hash()
//!             .prefix("product:")
//!             .text_field("name")
//!             .numeric_field("price"),
//!     )
//!     .await?;
//!
//! let results = search
//!     .search::<Product>(
//!         SearchQuery::new("products_idx", "shoes")
//!             .sort_by("price", SortDir::Asc)
//!             .limit(0, 10),
//!     )
//!     .await?;
//!
//! for doc in &results.docs {
//!     println!("{}: {} ({})", doc.key, doc.doc.name, doc.doc.price);
//! }
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;

use redis_tower::RedisExecutor;
use redis_tower::commands::{
    FieldType, FtAggregate, FtAliasAdd, FtAliasDel, FtAliasUpdate, FtAlter, FtConfigGet,
    FtConfigSet, FtCreate, FtDropIndex, FtInfo, FtList, FtSearch, FtSpellCheck, FtSugAdd, FtSugDel,
    FtSugGet, FtSugLen, SchemaField, SortOrder,
};
use redis_tower_core::{Frame, RedisError};
use serde::de::DeserializeOwned;

/// Sort direction for search and aggregate results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    /// Ascending order.
    Asc,
    /// Descending order.
    Desc,
}

impl From<SortDir> for SortOrder {
    fn from(dir: SortDir) -> Self {
        match dir {
            SortDir::Asc => SortOrder::Asc,
            SortDir::Desc => SortOrder::Desc,
        }
    }
}

/// Fluent builder for an `FT.CREATE` index definition.
///
/// Wraps the low-level [`FtCreate`] command and exposes typed field helpers
/// that map onto RediSearch [`FieldType`] variants.
pub struct IndexBuilder {
    inner: FtCreate,
}

impl IndexBuilder {
    /// Start building an index with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            inner: FtCreate::new(name),
        }
    }

    /// Index `HASH` keys.
    pub fn on_hash(mut self) -> Self {
        self.inner = self.inner.on_hash();
        self
    }

    /// Index `JSON` keys.
    pub fn on_json(mut self) -> Self {
        self.inner = self.inner.on_json();
        self
    }

    /// Restrict the index to keys matching the given prefix.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.inner = self.inner.prefix(prefix);
        self
    }

    /// Add a full-text `TEXT` field.
    pub fn text_field(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.field(name, FieldType::Text);
        self
    }

    /// Add a `NUMERIC` field.
    pub fn numeric_field(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.field(name, FieldType::Numeric);
        self
    }

    /// Add a `TAG` field.
    pub fn tag_field(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.field(name, FieldType::Tag);
        self
    }

    /// Add a `GEO` field.
    pub fn geo_field(mut self, name: impl Into<String>) -> Self {
        self.inner = self.inner.field(name, FieldType::Geo);
        self
    }

    /// Consume the builder and produce the underlying [`FtCreate`] command.
    pub fn build(self) -> FtCreate {
        self.inner
    }
}

/// Fluent builder for an `FT.SEARCH` query with typed result deserialization.
pub struct SearchQuery {
    index: String,
    query: String,
    filters: Vec<String>,
    sort_field: Option<String>,
    sort_dir: Option<SortDir>,
    return_fields: Vec<String>,
    offset: Option<u64>,
    count: Option<u64>,
    nocontent: bool,
    verbatim: bool,
    withscores: bool,
}

impl SearchQuery {
    /// Create a new search query for the given index and query string.
    pub fn new(index: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            query: query.into(),
            filters: Vec::new(),
            sort_field: None,
            sort_dir: None,
            return_fields: Vec::new(),
            offset: None,
            count: None,
            nocontent: false,
            verbatim: false,
            withscores: false,
        }
    }

    /// Append a filter expression to the query (joined with a space).
    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filters.push(filter.into());
        self
    }

    /// Sort results by a field in the given direction.
    pub fn sort_by(mut self, field: impl Into<String>, dir: SortDir) -> Self {
        self.sort_field = Some(field.into());
        self.sort_dir = Some(dir);
        self
    }

    /// Specify which fields to return in results.
    pub fn return_fields(mut self, fields: &[&str]) -> Self {
        self.return_fields = fields.iter().map(|f| (*f).to_string()).collect();
        self
    }

    /// Limit results with an offset and count.
    pub fn limit(mut self, offset: u64, count: u64) -> Self {
        self.offset = Some(offset);
        self.count = Some(count);
        self
    }

    /// Return only document IDs, not field content.
    pub fn nocontent(mut self) -> Self {
        self.nocontent = true;
        self
    }

    /// Disable stemming for query expansion.
    pub fn verbatim(mut self) -> Self {
        self.verbatim = true;
        self
    }

    /// Include relevance scores in the results.
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }

    /// Build the effective query string by appending filters.
    fn effective_query(&self) -> String {
        if self.filters.is_empty() {
            return self.query.clone();
        }
        let mut q = self.query.clone();
        for filter in &self.filters {
            q.push(' ');
            q.push_str(filter);
        }
        q
    }

    /// Build the underlying [`FtSearch`] command from builder state.
    fn build(&self) -> FtSearch {
        let query = self.effective_query();
        let mut cmd = FtSearch::new(&self.index, &query);

        if let Some(offset) = self.offset {
            cmd = cmd.limit(offset, self.count.unwrap_or(10));
        }

        if !self.return_fields.is_empty() {
            let field_refs: Vec<&str> = self.return_fields.iter().map(String::as_str).collect();
            cmd = cmd.return_fields(&field_refs);
        }

        if let Some(field) = &self.sort_field {
            cmd = cmd.sortby(field, self.sort_dir.unwrap_or(SortDir::Asc).into());
        }

        if self.nocontent {
            cmd = cmd.nocontent();
        }
        if self.verbatim {
            cmd = cmd.verbatim();
        }
        if self.withscores {
            cmd = cmd.withscores();
        }

        cmd
    }
}

/// Search results containing deserialized documents.
#[derive(Debug)]
pub struct SearchResults<T> {
    /// Total number of matching documents (may exceed `docs.len()`).
    pub total: i64,
    /// The matching documents.
    pub docs: Vec<SearchDoc<T>>,
}

/// A single document from search results.
#[derive(Debug)]
pub struct SearchDoc<T> {
    /// The document key in Redis.
    pub key: String,
    /// The deserialized document.
    pub doc: T,
    /// Optional relevance score (present when `WITHSCORES` was used).
    pub score: Option<f64>,
}

/// Parsed statistics from an `FT.INFO` reply.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct IndexInfo {
    /// The index name.
    pub index_name: String,
    /// Number of documents indexed.
    pub num_docs: u64,
    /// Number of distinct terms.
    pub num_terms: u64,
    /// Number of inverted-index records.
    pub num_records: u64,
    /// Size of the inverted index in megabytes.
    pub inverted_sz_mb: f64,
    /// Whether the index is currently being (re)built.
    pub indexing: bool,
    /// Percentage of the index that has been built (0.0 – 1.0).
    pub percent_indexed: f64,
}

/// Fluent builder for an `FT.AGGREGATE` query.
///
/// Aggregation replies are returned as a raw [`Frame`]; typed deserialization
/// is intentionally out of scope.
pub struct AggregateQuery {
    inner: FtAggregate,
}

impl AggregateQuery {
    /// Create a new aggregate query for the given index and query string.
    pub fn new(index: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            inner: FtAggregate::new(index, query),
        }
    }

    /// Group results by the given properties.
    pub fn groupby(mut self, properties: &[&str]) -> Self {
        self.inner = self.inner.groupby(properties);
        self
    }

    /// Add a `REDUCE` function with arguments and an optional alias.
    pub fn reduce(mut self, func: impl Into<String>, args: &[&str], alias: Option<&str>) -> Self {
        self.inner = self.inner.reduce(func, args, alias);
        self
    }

    /// Sort results by a field in the given direction.
    pub fn sort_by(mut self, field: impl Into<String>, dir: SortDir) -> Self {
        self.inner = self.inner.sortby(field, dir.into());
        self
    }

    /// Limit results with an offset and count.
    pub fn limit(mut self, offset: u64, count: u64) -> Self {
        self.inner = self.inner.limit(offset, count);
        self
    }

    /// Add an `APPLY` expression with an alias.
    pub fn apply(mut self, expr: impl Into<String>, alias: impl Into<String>) -> Self {
        self.inner = self.inner.apply(expr, alias);
        self
    }

    /// Consume the builder and produce the underlying [`FtAggregate`] command.
    fn build(self) -> FtAggregate {
        self.inner
    }
}

/// A single auto-complete suggestion from `FT.SUGGET`.
#[derive(Debug, Clone, PartialEq)]
pub struct Suggestion {
    /// The suggested string.
    pub string: String,
    /// The score (present when `WITHSCORES` was requested).
    pub score: Option<f64>,
    /// The payload (present when `WITHPAYLOADS` was requested).
    pub payload: Option<String>,
}

/// Options for an `FT.SUGGET` request.
#[derive(Debug, Clone, Default)]
pub struct SugGetOptions {
    /// Perform fuzzy prefix matching.
    pub fuzzy: bool,
    /// Include scores in the response.
    pub withscores: bool,
    /// Include payloads in the response.
    pub withpayloads: bool,
    /// Maximum number of suggestions to return.
    pub max: Option<u64>,
}

/// Options for an `FT.SPELLCHECK` request.
#[derive(Debug, Clone, Default)]
pub struct SpellCheckOptions {
    /// Maximum Levenshtein distance for suggestions (1–4).
    pub distance: Option<u64>,
    /// Name of a dictionary whose terms should be included.
    pub include_terms: Option<String>,
    /// Name of a dictionary whose terms should be excluded.
    pub exclude_terms: Option<String>,
}

/// A spelling-correction result for one misspelled term.
#[derive(Debug, Clone, PartialEq)]
pub struct SpellCheckResult {
    /// The misspelled term.
    pub term: String,
    /// Candidate corrections as `(score, suggestion)` pairs.
    pub suggestions: Vec<(f64, String)>,
}

/// High-level client for RediSearch operations.
///
/// Borrows an underlying executor `C` for the duration of the operations and
/// exposes index lifecycle management, typed search, aggregation,
/// auto-complete, spellcheck, alias, and config operations.
pub struct SearchClient<'a, C> {
    conn: &'a mut C,
}

impl<'a, C: RedisExecutor> SearchClient<'a, C> {
    /// Create a new [`SearchClient`] borrowing the given executor.
    pub fn new(conn: &'a mut C) -> Self {
        Self { conn }
    }

    // -- Index lifecycle --

    /// Create a search index from an [`IndexBuilder`].
    pub async fn create_index(&mut self, builder: IndexBuilder) -> Result<(), RedisError> {
        self.conn.execute(builder.build()).await
    }

    /// Drop a search index, optionally deleting the indexed documents.
    pub async fn drop_index(&mut self, index: &str, delete_docs: bool) -> Result<(), RedisError> {
        let mut cmd = FtDropIndex::new(index);
        if delete_docs {
            cmd = cmd.dd();
        }
        self.conn.execute(cmd).await
    }

    /// Add new fields to an existing index schema.
    pub async fn alter_index(
        &mut self,
        index: &str,
        fields: Vec<SchemaField>,
    ) -> Result<(), RedisError> {
        let mut cmd = FtAlter::new(index);
        for field in fields {
            cmd = cmd.schema_field(field);
        }
        self.conn.execute(cmd).await
    }

    /// Fetch and parse statistics about an index.
    pub async fn index_info(&mut self, index: &str) -> Result<IndexInfo, RedisError> {
        let frame = self.conn.execute(FtInfo::new(index)).await?;
        parse_index_info(frame)
    }

    /// List all existing index names.
    pub async fn list_indexes(&mut self) -> Result<Vec<String>, RedisError> {
        let names = self.conn.execute(FtList::new()).await?;
        names
            .into_iter()
            .map(|b| String::from_utf8(b.to_vec()).map_err(|e| RedisError::Redis(format!("{e}"))))
            .collect()
    }

    // -- Search --

    /// Execute a search query, deserializing each document into `T`.
    pub async fn search<T: DeserializeOwned>(
        &mut self,
        query: SearchQuery,
    ) -> Result<SearchResults<T>, RedisError> {
        let withscores = query.withscores;
        let cmd = query.build();
        let frame = self.conn.execute(cmd).await?;
        parse_search_results(frame, withscores)
    }

    // -- Aggregate --

    /// Execute an aggregate query, returning the raw reply [`Frame`].
    pub async fn aggregate(&mut self, query: AggregateQuery) -> Result<Frame, RedisError> {
        self.conn.execute(query.build()).await
    }

    // -- Autocomplete --

    /// Add a suggestion string to an auto-complete dictionary.
    ///
    /// Returns the current size of the dictionary.
    pub async fn sug_add(
        &mut self,
        key: &str,
        string: &str,
        score: f64,
    ) -> Result<i64, RedisError> {
        self.conn.execute(FtSugAdd::new(key, string, score)).await
    }

    /// Get completion suggestions for a prefix.
    pub async fn sug_get(
        &mut self,
        key: &str,
        prefix: &str,
        options: SugGetOptions,
    ) -> Result<Vec<Suggestion>, RedisError> {
        let mut cmd = FtSugGet::new(key, prefix);
        if options.fuzzy {
            cmd = cmd.fuzzy();
        }
        if options.withscores {
            cmd = cmd.withscores();
        }
        if options.withpayloads {
            cmd = cmd.withpayloads();
        }
        if let Some(max) = options.max {
            cmd = cmd.max(max);
        }
        let frame = self.conn.execute(cmd).await?;
        parse_suggestions(frame, options.withscores, options.withpayloads)
    }

    /// Delete a string from an auto-complete dictionary.
    ///
    /// Returns `true` if the string was found and removed.
    pub async fn sug_del(&mut self, key: &str, string: &str) -> Result<bool, RedisError> {
        self.conn.execute(FtSugDel::new(key, string)).await
    }

    /// Return the number of entries in an auto-complete dictionary.
    pub async fn sug_len(&mut self, key: &str) -> Result<i64, RedisError> {
        self.conn.execute(FtSugLen::new(key)).await
    }

    // -- Spellcheck --

    /// Run spelling correction over a query, returning suggestions per term.
    pub async fn spellcheck(
        &mut self,
        index: &str,
        query: &str,
        options: SpellCheckOptions,
    ) -> Result<Vec<SpellCheckResult>, RedisError> {
        let mut cmd = FtSpellCheck::new(index, query);
        if let Some(dist) = options.distance {
            cmd = cmd.distance(dist);
        }
        if let Some(dict) = options.include_terms {
            cmd = cmd.include_terms(dict);
        }
        if let Some(dict) = options.exclude_terms {
            cmd = cmd.exclude_terms(dict);
        }
        let frame = self.conn.execute(cmd).await?;
        parse_spellcheck(frame)
    }

    // -- Alias management --

    /// Add an alias pointing to an index.
    pub async fn alias_add(&mut self, alias: &str, index: &str) -> Result<(), RedisError> {
        self.conn.execute(FtAliasAdd::new(alias, index)).await
    }

    /// Remove an alias.
    pub async fn alias_del(&mut self, alias: &str) -> Result<(), RedisError> {
        self.conn.execute(FtAliasDel::new(alias)).await
    }

    /// Update an alias to point to a different index.
    pub async fn alias_update(&mut self, alias: &str, index: &str) -> Result<(), RedisError> {
        self.conn.execute(FtAliasUpdate::new(alias, index)).await
    }

    // -- Config --

    /// Set a RediSearch configuration option.
    pub async fn config_set(&mut self, option: &str, value: &str) -> Result<(), RedisError> {
        self.conn.execute(FtConfigSet::new(option, value)).await
    }

    /// Get a RediSearch configuration option as a raw reply [`Frame`].
    pub async fn config_get(&mut self, option: &str) -> Result<Frame, RedisError> {
        self.conn.execute(FtConfigGet::new(option)).await
    }
}

// -- Parsing helpers --

/// Extract a UTF-8 string from a scalar (string-like, integer, or double) frame.
fn frame_to_string(frame: &Frame) -> Result<String, RedisError> {
    match frame {
        Frame::BulkString(Some(data)) | Frame::SimpleString(data) => {
            String::from_utf8(data.to_vec()).map_err(|e| RedisError::Redis(format!("{e}")))
        }
        Frame::Integer(n) => Ok(n.to_string()),
        Frame::Double(d) => Ok(d.to_string()),
        other => Err(RedisError::UnexpectedResponse {
            expected: "string-like frame",
            actual: format!("{other:?}"),
        }),
    }
}

/// Extract an `f64` from a numeric or string-like frame, if possible.
fn frame_to_f64(frame: &Frame) -> Option<f64> {
    match frame {
        Frame::Double(d) => Some(*d),
        Frame::Integer(n) => Some(*n as f64),
        _ => frame_to_string(frame).ok().and_then(|s| s.parse().ok()),
    }
}

/// Parse an `FT.SEARCH` reply into typed search results.
///
/// `FT.SEARCH` returns:
/// `[total, key1, [field1, val1, ...], key2, [...], ...]`
///
/// With `WITHSCORES` a score is interleaved after each key:
/// `[total, key1, score1, [field1, val1, ...], ...]`
fn parse_search_results<T: DeserializeOwned>(
    frame: Frame,
    withscores: bool,
) -> Result<SearchResults<T>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            });
        }
    };

    if items.is_empty() {
        return Err(RedisError::UnexpectedResponse {
            expected: "non-empty array with total count",
            actual: "empty array".to_string(),
        });
    }

    let total = match &items[0] {
        Frame::Integer(n) => *n,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "integer (total count)",
                actual: format!("{other:?}"),
            });
        }
    };

    let mut docs = Vec::new();
    let mut i = 1;

    while i < items.len() {
        let key = frame_to_string(&items[i])?;
        i += 1;

        let score = if withscores {
            if i >= items.len() {
                return Err(RedisError::UnexpectedResponse {
                    expected: "score value",
                    actual: "end of array".to_string(),
                });
            }
            let s = frame_to_f64(&items[i]).unwrap_or(0.0);
            i += 1;
            Some(s)
        } else {
            None
        };

        if i >= items.len() {
            return Err(RedisError::UnexpectedResponse {
                expected: "field/value array",
                actual: "end of array".to_string(),
            });
        }

        let fields = match &items[i] {
            Frame::Array(Some(fields)) => fields,
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "array of field/value pairs",
                    actual: format!("{other:?}"),
                });
            }
        };
        i += 1;

        let mut map = serde_json::Map::new();
        for chunk in fields.chunks(2) {
            if chunk.len() < 2 {
                continue;
            }
            let k = frame_to_string(&chunk[0])?;
            let v = frame_to_string(&chunk[1])?;
            map.insert(k, serde_json::Value::String(v));
        }

        let doc: T = serde_json::from_value(serde_json::Value::Object(map))
            .map_err(|e| RedisError::Redis(format!("deserialize error: {e}")))?;

        docs.push(SearchDoc { key, doc, score });
    }

    Ok(SearchResults { total, docs })
}

/// Build a flat string key/value map from an `FT.INFO`-style reply.
///
/// Accepts both the RESP2 flat alternating array and the RESP3 map form.
/// Non-scalar values (nested arrays) are skipped.
fn frame_to_kv_map(frame: Frame) -> Result<HashMap<String, String>, RedisError> {
    let mut map = HashMap::new();
    match frame {
        Frame::Array(Some(items)) => {
            for chunk in items.chunks(2) {
                if chunk.len() < 2 {
                    continue;
                }
                let key = frame_to_string(&chunk[0])?;
                if let Ok(value) = frame_to_string(&chunk[1]) {
                    map.insert(key, value);
                }
            }
            Ok(map)
        }
        Frame::Map(pairs) => {
            for (k, v) in &pairs {
                let key = frame_to_string(k)?;
                if let Ok(value) = frame_to_string(v) {
                    map.insert(key, value);
                }
            }
            Ok(map)
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "array or map",
            actual: format!("{other:?}"),
        }),
    }
}

/// Parse an `FT.INFO` reply into an [`IndexInfo`], filling missing fields with
/// defaults.
fn parse_index_info(frame: Frame) -> Result<IndexInfo, RedisError> {
    let map = frame_to_kv_map(frame)?;
    let parse_u64 = |k: &str| map.get(k).and_then(|s| s.parse::<u64>().ok()).unwrap_or(0);
    let parse_f64 = |k: &str| {
        map.get(k)
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    Ok(IndexInfo {
        index_name: map.get("index_name").cloned().unwrap_or_default(),
        num_docs: parse_u64("num_docs"),
        num_terms: parse_u64("num_terms"),
        num_records: parse_u64("num_records"),
        inverted_sz_mb: parse_f64("inverted_sz_mb"),
        indexing: map
            .get("indexing")
            .map(|s| s != "0" && !s.eq_ignore_ascii_case("false"))
            .unwrap_or(false),
        percent_indexed: parse_f64("percent_indexed"),
    })
}

/// Parse an `FT.SUGGET` reply into a list of [`Suggestion`]s.
///
/// The layout depends on the requested options:
/// - neither: `[str1, str2, ...]`
/// - scores: `[str1, score1, ...]`
/// - payloads: `[str1, payload1, ...]`
/// - both: `[str1, score1, payload1, ...]`
fn parse_suggestions(
    frame: Frame,
    withscores: bool,
    withpayloads: bool,
) -> Result<Vec<Suggestion>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) => items,
        Frame::Array(None) | Frame::Null => return Ok(Vec::new()),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            });
        }
    };

    let stride = 1 + usize::from(withscores) + usize::from(withpayloads);
    let mut out = Vec::new();
    let mut i = 0;
    while i < items.len() {
        let string = frame_to_string(&items[i])?;
        let mut j = i + 1;

        let score = if withscores {
            let s = items.get(j).and_then(frame_to_f64);
            j += 1;
            s
        } else {
            None
        };

        let payload = if withpayloads {
            let p = match items.get(j) {
                Some(f) => Some(frame_to_string(f)?),
                None => None,
            };
            j += 1;
            p
        } else {
            None
        };

        let _ = j;
        out.push(Suggestion {
            string,
            score,
            payload,
        });
        i += stride;
    }

    Ok(out)
}

/// Parse an `FT.SPELLCHECK` reply into a list of [`SpellCheckResult`]s.
///
/// Each entry is `["TERM", term, [[score1, suggestion1], ...]]`.
fn parse_spellcheck(frame: Frame) -> Result<Vec<SpellCheckResult>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) => items,
        Frame::Array(None) | Frame::Null => return Ok(Vec::new()),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            });
        }
    };

    let mut out = Vec::new();
    for item in &items {
        let entry = match item {
            Frame::Array(Some(entry)) if entry.len() >= 3 => entry,
            _ => continue,
        };

        let term = frame_to_string(&entry[1])?;
        let suggestions = match &entry[2] {
            Frame::Array(Some(pairs)) => {
                let mut v = Vec::new();
                for pair in pairs {
                    if let Frame::Array(Some(pair)) = pair
                        && pair.len() >= 2
                    {
                        let score = frame_to_f64(&pair[0]).unwrap_or(0.0);
                        let suggestion = frame_to_string(&pair[1])?;
                        v.push((score, suggestion));
                    }
                }
                v
            }
            _ => Vec::new(),
        };

        out.push(SpellCheckResult { term, suggestions });
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use serde::Deserialize;
    use std::collections::VecDeque;

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
        ) -> impl std::future::Future<Output = Result<Cmd::Response, RedisError>> + Send {
            let frame = self.responses.pop_front().unwrap_or(Frame::Null);
            async move { cmd.parse_response(frame) }
        }
    }

    /// Build a `BulkString` frame from a `&str` (Bytes type inferred).
    fn bs(s: &str) -> Frame {
        Frame::BulkString(Some(s.as_bytes().to_vec().into()))
    }

    /// Build a `SimpleString` frame from a `&str`.
    fn ss(s: &str) -> Frame {
        Frame::SimpleString(s.as_bytes().to_vec().into())
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Doc {
        name: String,
    }

    #[tokio::test]
    async fn search_basic() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![
            Frame::Integer(2),
            bs("doc:1"),
            Frame::Array(Some(vec![bs("name"), bs("Widget")])),
            bs("doc:2"),
            Frame::Array(Some(vec![bs("name"), bs("Gizmo")])),
        ]))]);
        let mut client = SearchClient::new(&mut mock);

        let results: SearchResults<Doc> =
            client.search(SearchQuery::new("idx", "*")).await.unwrap();

        assert_eq!(results.total, 2);
        assert_eq!(results.docs.len(), 2);
        assert_eq!(results.docs[0].key, "doc:1");
        assert_eq!(results.docs[0].doc.name, "Widget");
        assert!(results.docs[0].score.is_none());
        assert_eq!(results.docs[1].key, "doc:2");
        assert_eq!(results.docs[1].doc.name, "Gizmo");
    }

    #[tokio::test]
    async fn search_empty() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![Frame::Integer(0)]))]);
        let mut client = SearchClient::new(&mut mock);

        let results: SearchResults<Doc> = client
            .search(SearchQuery::new("idx", "nomatch"))
            .await
            .unwrap();

        assert_eq!(results.total, 0);
        assert!(results.docs.is_empty());
    }

    #[tokio::test]
    async fn search_withscores() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![
            Frame::Integer(1),
            bs("doc:1"),
            bs("0.75"),
            Frame::Array(Some(vec![bs("name"), bs("Widget")])),
        ]))]);
        let mut client = SearchClient::new(&mut mock);

        let results: SearchResults<Doc> = client
            .search(SearchQuery::new("idx", "*").withscores())
            .await
            .unwrap();

        assert_eq!(results.total, 1);
        assert_eq!(results.docs.len(), 1);
        assert!((results.docs[0].score.unwrap() - 0.75).abs() < f64::EPSILON);
        assert_eq!(results.docs[0].doc.name, "Widget");
    }

    #[tokio::test]
    async fn create_index_ok() {
        let mut mock = MockRedis::new(vec![ss("OK")]);
        let mut client = SearchClient::new(&mut mock);

        let result = client
            .create_index(
                IndexBuilder::new("idx")
                    .on_hash()
                    .prefix("doc:")
                    .text_field("name")
                    .numeric_field("price"),
            )
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn drop_index_ok() {
        let mut mock = MockRedis::new(vec![ss("OK")]);
        let mut client = SearchClient::new(&mut mock);

        let result = client.drop_index("idx", true).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn sug_add_returns_count() {
        let mut mock = MockRedis::new(vec![Frame::Integer(5)]);
        let mut client = SearchClient::new(&mut mock);

        let count = client.sug_add("sug", "hello", 1.0).await.unwrap();
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn sug_get_basic() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![bs("hello"), bs("help")]))]);
        let mut client = SearchClient::new(&mut mock);

        let suggestions = client
            .sug_get("sug", "hel", SugGetOptions::default())
            .await
            .unwrap();

        assert_eq!(suggestions.len(), 2);
        assert_eq!(suggestions[0].string, "hello");
        assert!(suggestions[0].score.is_none());
        assert!(suggestions[0].payload.is_none());
        assert_eq!(suggestions[1].string, "help");
    }

    #[tokio::test]
    async fn index_info_parses_fields() {
        let mut mock = MockRedis::new(vec![Frame::Array(Some(vec![
            bs("index_name"),
            bs("idx"),
            bs("num_docs"),
            Frame::Integer(42),
            bs("num_terms"),
            Frame::Integer(7),
            bs("num_records"),
            Frame::Integer(100),
            bs("inverted_sz_mb"),
            bs("1.5"),
            bs("indexing"),
            Frame::Integer(0),
            bs("percent_indexed"),
            bs("1"),
        ]))]);
        let mut client = SearchClient::new(&mut mock);

        let info = client.index_info("idx").await.unwrap();

        assert_eq!(info.index_name, "idx");
        assert_eq!(info.num_docs, 42);
        assert_eq!(info.num_terms, 7);
        assert_eq!(info.num_records, 100);
        assert!((info.inverted_sz_mb - 1.5).abs() < f64::EPSILON);
        assert!(!info.indexing);
        assert!((info.percent_indexed - 1.0).abs() < f64::EPSILON);
    }
}
