//! High-level Search API with a typed query builder and result deserialization.
//!
//! This module provides a [`Search`] builder that wraps the low-level
//! [`FtSearch`] command with automatic deserialization of results into
//! user-defined types via serde.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::search_api::{Search, SearchResults, SortDir};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct Product {
//!     name: String,
//!     price: String,
//!     category: String,
//! }
//!
//! let results: SearchResults<Product> = Search::new("products_idx", "shoes")
//!     .filter("@price:[0 100]")
//!     .sort_by("price", SortDir::Asc)
//!     .return_fields(&["name", "price", "category"])
//!     .limit(0, 10)
//!     .search(&mut conn)
//!     .await?;
//!
//! for doc in &results.docs {
//!     println!("{}: {:?}", doc.key, doc.doc);
//! }
//! ```

use redis_tower_commands::{FtSearch, SortOrder};
use redis_tower_core::{Frame, RedisError};
use serde::de::DeserializeOwned;

use crate::RedisExecutor;

/// Sort direction for search results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDir {
    /// Ascending order.
    Asc,
    /// Descending order.
    Desc,
}

/// A search result containing deserialized documents.
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
    /// Optional score (present when WITHSCORES was used).
    pub score: Option<f64>,
}

/// High-level search query builder.
///
/// Wraps [`FtSearch`] with a fluent API and automatic result deserialization.
///
/// # Example
///
/// ```ignore
/// use redis_tower::search_api::{Search, SearchResults, SortDir};
///
/// let results: SearchResults<Product> = Search::new("products_idx", "shoes")
///     .filter("@price:[0 100]")
///     .sort_by("price", SortDir::Asc)
///     .return_fields(&["name", "price", "category"])
///     .limit(0, 10)
///     .search(&mut conn)
///     .await?;
/// ```
pub struct Search {
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

impl Search {
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

    /// Add a filter expression (appended to the query with a space).
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

    /// Limit results with offset and count.
    pub fn limit(mut self, offset: u64, count: u64) -> Self {
        self.offset = Some(offset);
        self.count = Some(count);
        self
    }

    /// Return only document IDs, not content.
    pub fn nocontent(mut self) -> Self {
        self.nocontent = true;
        self
    }

    /// Disable stemming for query expansion.
    pub fn verbatim(mut self) -> Self {
        self.verbatim = true;
        self
    }

    /// Include scores in results.
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
    fn to_ft_search(&self) -> FtSearch {
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
            let order = match self.sort_dir.unwrap_or(SortDir::Asc) {
                SortDir::Asc => SortOrder::Asc,
                SortDir::Desc => SortOrder::Desc,
            };
            cmd = cmd.sortby(field, order);
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

    /// Execute the search and deserialize results into `T`.
    ///
    /// Parses the FT.SEARCH response (total count + alternating key/fields
    /// pairs) and deserializes the field maps into `T` using serde_json.
    pub async fn search<T: DeserializeOwned>(
        self,
        conn: &mut impl RedisExecutor,
    ) -> Result<SearchResults<T>, RedisError> {
        let withscores = self.withscores;
        let cmd = self.to_ft_search();
        let frame = conn.execute(cmd).await?;
        parse_search_results(frame, withscores)
    }
}

/// Extract a UTF-8 string from a BulkString frame.
fn extract_string(frame: &Frame) -> Result<String, RedisError> {
    match frame {
        Frame::BulkString(Some(data)) => {
            String::from_utf8(data.to_vec()).map_err(|e| RedisError::Redis(format!("{e}")))
        }
        _ => Err(RedisError::UnexpectedResponse {
            expected: "bulk string",
            actual: format!("{frame:?}"),
        }),
    }
}

/// Parse an FT.SEARCH response frame into typed search results.
///
/// FT.SEARCH returns:
/// `[total_count, key1, [field1, val1, field2, val2, ...], key2, [...], ...]`
///
/// With WITHSCORES:
/// `[total_count, key1, score1, [field1, val1, ...], key2, score2, [...], ...]`
fn parse_search_results<T: DeserializeOwned>(
    frame: Frame,
    withscores: bool,
) -> Result<SearchResults<T>, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) => items,
        _ => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{frame:?}"),
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
        _ => {
            return Err(RedisError::UnexpectedResponse {
                expected: "integer (total count)",
                actual: format!("{:?}", items[0]),
            });
        }
    };

    let mut docs = Vec::new();
    let mut i = 1;

    while i < items.len() {
        let key = extract_string(&items[i])?;
        i += 1;

        let score = if withscores {
            if i >= items.len() {
                return Err(RedisError::UnexpectedResponse {
                    expected: "score value",
                    actual: "end of array".to_string(),
                });
            }
            let s = extract_string(&items[i])?;
            i += 1;
            Some(s.parse::<f64>().unwrap_or(0.0))
        } else {
            None
        };

        if i >= items.len() {
            return Err(RedisError::UnexpectedResponse {
                expected: "field/value array",
                actual: "end of array".to_string(),
            });
        }

        // Parse field/value array into a JSON map.
        let fields = match &items[i] {
            Frame::Array(Some(fields)) => fields,
            _ => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "array of field/value pairs",
                    actual: format!("{:?}", items[i]),
                });
            }
        };
        i += 1;

        let mut map = serde_json::Map::new();
        for chunk in fields.chunks(2) {
            if chunk.len() < 2 {
                continue;
            }
            let k = extract_string(&chunk[0])?;
            let v = extract_string(&chunk[1])?;
            map.insert(k, serde_json::Value::String(v));
        }

        let doc: T = serde_json::from_value(serde_json::Value::Object(map))?;

        docs.push(SearchDoc { key, doc, score });
    }

    Ok(SearchResults { total, docs })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use redis_tower_core::{Command, Frame};
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq)]
    struct Product {
        name: String,
        price: String,
        category: String,
    }

    /// Helper to build a BulkString frame from a &str.
    fn bs(s: &str) -> Frame {
        Frame::BulkString(Some(Bytes::from(s.to_string())))
    }

    #[test]
    fn search_builds_basic_ft_search() {
        let search = Search::new("idx", "*");
        let cmd = search.to_ft_search();
        let frame = cmd.to_frame();

        // Should produce: FT.SEARCH idx *
        let args = match frame {
            Frame::Array(Some(items)) => items,
            _ => panic!("expected array frame"),
        };
        assert_eq!(args.len(), 3);
        assert_eq!(extract_string(&args[0]).unwrap(), "FT.SEARCH");
        assert_eq!(extract_string(&args[1]).unwrap(), "idx");
        assert_eq!(extract_string(&args[2]).unwrap(), "*");
    }

    #[test]
    fn search_builds_with_all_options() {
        let search = Search::new("products", "shoes")
            .filter("@price:[0 100]")
            .sort_by("price", SortDir::Asc)
            .return_fields(&["name", "price"])
            .limit(0, 5)
            .verbatim()
            .withscores();

        let cmd = search.to_ft_search();
        let frame = cmd.to_frame();

        let args = match frame {
            Frame::Array(Some(items)) => items,
            _ => panic!("expected array frame"),
        };

        let strs: Vec<String> = args.iter().map(|f| extract_string(f).unwrap()).collect();

        assert_eq!(strs[0], "FT.SEARCH");
        assert_eq!(strs[1], "products");
        // Query should have filter appended.
        assert_eq!(strs[2], "shoes @price:[0 100]");
        // VERBATIM and WITHSCORES should be present.
        assert!(strs.contains(&"VERBATIM".to_string()));
        assert!(strs.contains(&"WITHSCORES".to_string()));
        // LIMIT should be present.
        assert!(strs.contains(&"LIMIT".to_string()));
        // RETURN should be present.
        assert!(strs.contains(&"RETURN".to_string()));
        // SORTBY should be present.
        assert!(strs.contains(&"SORTBY".to_string()));
    }

    #[test]
    fn parse_basic_search_results() {
        // Simulate: [2, "doc:1", ["name", "Shoe A", "price", "50", "category", "footwear"],
        //               "doc:2", ["name", "Shoe B", "price", "75", "category", "footwear"]]
        let frame = Frame::Array(Some(vec![
            Frame::Integer(2),
            bs("doc:1"),
            Frame::Array(Some(vec![
                bs("name"),
                bs("Shoe A"),
                bs("price"),
                bs("50"),
                bs("category"),
                bs("footwear"),
            ])),
            bs("doc:2"),
            Frame::Array(Some(vec![
                bs("name"),
                bs("Shoe B"),
                bs("price"),
                bs("75"),
                bs("category"),
                bs("footwear"),
            ])),
        ]));

        let results: SearchResults<Product> = parse_search_results(frame, false).unwrap();
        assert_eq!(results.total, 2);
        assert_eq!(results.docs.len(), 2);

        assert_eq!(results.docs[0].key, "doc:1");
        assert_eq!(
            results.docs[0].doc,
            Product {
                name: "Shoe A".into(),
                price: "50".into(),
                category: "footwear".into(),
            }
        );
        assert!(results.docs[0].score.is_none());

        assert_eq!(results.docs[1].key, "doc:2");
        assert_eq!(
            results.docs[1].doc,
            Product {
                name: "Shoe B".into(),
                price: "75".into(),
                category: "footwear".into(),
            }
        );
    }

    #[test]
    fn parse_empty_results() {
        let frame = Frame::Array(Some(vec![Frame::Integer(0)]));

        let results: SearchResults<Product> = parse_search_results(frame, false).unwrap();
        assert_eq!(results.total, 0);
        assert!(results.docs.is_empty());
    }

    #[test]
    fn parse_withscores_results() {
        // Simulate: [1, "doc:1", "0.95", ["name", "Shoe A", "price", "50", "category", "footwear"]]
        let frame = Frame::Array(Some(vec![
            Frame::Integer(1),
            bs("doc:1"),
            bs("0.95"),
            Frame::Array(Some(vec![
                bs("name"),
                bs("Shoe A"),
                bs("price"),
                bs("50"),
                bs("category"),
                bs("footwear"),
            ])),
        ]));

        let results: SearchResults<Product> = parse_search_results(frame, true).unwrap();
        assert_eq!(results.total, 1);
        assert_eq!(results.docs.len(), 1);
        assert_eq!(results.docs[0].key, "doc:1");
        assert!((results.docs[0].score.unwrap() - 0.95).abs() < f64::EPSILON);
        assert_eq!(results.docs[0].doc.name, "Shoe A");
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct MixedDoc {
        title: String,
        count: String,
        active: String,
    }

    #[test]
    fn deserialize_mixed_field_types() {
        // All Redis fields come back as strings, so the struct must use String
        // for all fields. This test verifies that numeric and boolean-like
        // values are correctly deserialized as strings.
        let frame = Frame::Array(Some(vec![
            Frame::Integer(1),
            bs("item:1"),
            Frame::Array(Some(vec![
                bs("title"),
                bs("Widget"),
                bs("count"),
                bs("42"),
                bs("active"),
                bs("true"),
            ])),
        ]));

        let results: SearchResults<MixedDoc> = parse_search_results(frame, false).unwrap();
        assert_eq!(results.docs.len(), 1);
        assert_eq!(
            results.docs[0].doc,
            MixedDoc {
                title: "Widget".into(),
                count: "42".into(),
                active: "true".into(),
            }
        );
    }

    #[test]
    fn parse_error_on_non_array_frame() {
        let frame = Frame::Integer(42);
        let result: Result<SearchResults<Product>, _> = parse_search_results(frame, false);
        assert!(result.is_err());
    }

    #[test]
    fn filter_appends_to_query() {
        let search = Search::new("idx", "*")
            .filter("@status:{active}")
            .filter("@price:[10 100]");
        assert_eq!(
            search.effective_query(),
            "* @status:{active} @price:[10 100]"
        );
    }
}
