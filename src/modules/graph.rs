//! RedisGraph module - Graph database with Cypher query language
//!
//! **DEPRECATED**: RedisGraph has reached end-of-life and is no longer maintained by Redis.
//! Consider migrating to alternatives like FalkorDB (RedisGraph fork) or other graph databases.
//!
//! This module provides RedisGraph functionality with type-safe command APIs.

#![allow(deprecated)]
//!
//! # Deprecation Notice
//!
//! RedisGraph was deprecated by Redis, Inc. and reached end-of-life in 2024.
//! This implementation is provided for backward compatibility only.
//!
//! **Migration Options:**
//! - **FalkorDB** - Community fork of RedisGraph (compatible API)
//! - **Neo4j** - Popular graph database with Cypher support
//! - **Amazon Neptune** - Managed graph database service
//!
//! # Examples
//!
//! ## Create nodes and relationships
//! ```no_run
//! use redis_tower::modules::graph::GraphQuery;
//! use redis_tower::RedisClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = RedisClient::connect("localhost:6379").await?;
//!
//! // Create nodes
//! let query = r#"CREATE (:Person {name: 'Alice', age: 30})"#;
//! client.call(GraphQuery::new("social", query)).await?;
//!
//! // Create relationship
//! let query = r#"
//!     MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
//!     CREATE (a)-[:KNOWS]->(b)
//! "#;
//! client.call(GraphQuery::new("social", query)).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Query graph
//! ```no_run
//! use redis_tower::modules::graph::GraphRoQuery;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let client = redis_tower::RedisClient::connect("localhost:6379").await?;
//! // Read-only query
//! let query = r#"MATCH (p:Person) RETURN p.name, p.age"#;
//! let result = client.call(GraphRoQuery::new("social", query)).await?;
//! # Ok(())
//! # }
//! ```

use crate::codec::Frame;
use crate::commands::Command;
use crate::read_preference::ReadOnly;
use crate::types::RedisError;
use bytes::Bytes;
use std::collections::HashMap;

// ============================================================================
// RESPONSE TYPES
// ============================================================================

/// Result from a graph query execution
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    /// Result set data (array of records)
    pub data: Vec<Vec<String>>,
    /// Query statistics
    pub statistics: QueryStatistics,
}

/// Statistics from query execution
#[derive(Debug, Clone, PartialEq, Default)]
pub struct QueryStatistics {
    /// Nodes created
    pub nodes_created: i64,
    /// Nodes deleted
    pub nodes_deleted: i64,
    /// Relationships created
    pub relationships_created: i64,
    /// Relationships deleted
    pub relationships_deleted: i64,
    /// Properties set
    pub properties_set: i64,
    /// Labels added
    pub labels_added: i64,
    /// Execution time in milliseconds
    pub query_internal_execution_time: f64,
}

/// Slowlog entry from GRAPH.SLOWLOG
#[derive(Debug, Clone, PartialEq)]
pub struct SlowlogEntry {
    /// Unix timestamp when logged
    pub timestamp: i64,
    /// Command executed
    pub command: String,
    /// Query string
    pub query: String,
    /// Execution time in milliseconds
    pub execution_time_ms: f64,
}

// ============================================================================
// GRAPH.QUERY - Execute write query
// ============================================================================

/// GRAPH.QUERY - Execute a query against a graph (read/write)
///
/// **DEPRECATED**: RedisGraph has reached end-of-life
///
/// Available since: RedisGraph 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::graph::GraphQuery;
///
/// // Create nodes
/// let cmd = GraphQuery::new("social", "CREATE (:Person {name: 'Alice'})");
///
/// // With timeout
/// let cmd = GraphQuery::new("social", "MATCH (p:Person) RETURN p")
///     .timeout(1000); // 1 second
/// ```
#[deprecated(
    since = "0.1.0",
    note = "RedisGraph has reached end-of-life. Consider migrating to FalkorDB or other graph databases."
)]
#[derive(Debug, Clone)]
pub struct GraphQuery {
    graph_name: String,
    query: String,
    timeout: Option<u64>,
}

impl GraphQuery {
    /// Create a new GRAPH.QUERY command
    pub fn new(graph_name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            graph_name: graph_name.into(),
            query: query.into(),
            timeout: None,
        }
    }

    /// Set query timeout in milliseconds
    pub fn timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout = Some(timeout_ms);
        self
    }
}

impl Command for GraphQuery {
    type Response = QueryResult;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("GRAPH.QUERY"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.graph_name.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.query.as_bytes()))),
        ];

        if let Some(timeout) = self.timeout {
            frames.push(Frame::BulkString(Some(Bytes::from("--timeout"))));
            frames.push(Frame::BulkString(Some(Bytes::from(timeout.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        // Simplified response parsing - actual response is complex nested structure
        // Real implementation would parse result set and statistics
        match frame {
            Frame::Array(_items) => {
                // TODO: Parse actual result structure
                Ok(QueryResult {
                    data: Vec::new(),
                    statistics: QueryStatistics::default(),
                })
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// GRAPH.RO_QUERY - Execute read-only query
// ============================================================================

/// GRAPH.RO_QUERY - Execute a read-only query against a graph
///
/// **DEPRECATED**: RedisGraph has reached end-of-life
///
/// Available since: RedisGraph 2.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::graph::GraphRoQuery;
///
/// let cmd = GraphRoQuery::new("social", "MATCH (p:Person) RETURN p.name");
/// let cmd = GraphRoQuery::new("social", "MATCH (p:Person) RETURN p").timeout(500);
/// ```
#[deprecated(
    since = "0.1.0",
    note = "RedisGraph has reached end-of-life. Consider migrating to FalkorDB or other graph databases."
)]
#[derive(Debug, Clone)]
pub struct GraphRoQuery {
    graph_name: String,
    query: String,
    timeout: Option<u64>,
}

impl GraphRoQuery {
    /// Create a new GRAPH.RO_QUERY command
    pub fn new(graph_name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            graph_name: graph_name.into(),
            query: query.into(),
            timeout: None,
        }
    }

    /// Set query timeout in milliseconds
    pub fn timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout = Some(timeout_ms);
        self
    }
}

impl Command for GraphRoQuery {
    type Response = QueryResult;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("GRAPH.RO_QUERY"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.graph_name.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.query.as_bytes()))),
        ];

        if let Some(timeout) = self.timeout {
            frames.push(Frame::BulkString(Some(Bytes::from("--timeout"))));
            frames.push(Frame::BulkString(Some(Bytes::from(timeout.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        GraphQuery::parse_response(frame)
    }
}

impl ReadOnly for GraphRoQuery {
    fn is_read_only(&self) -> bool {
        true
    }
}

// ============================================================================
// GRAPH.DELETE - Delete entire graph
// ============================================================================

/// GRAPH.DELETE - Completely remove a graph and all its data
///
/// **DEPRECATED**: RedisGraph has reached end-of-life
///
/// Available since: RedisGraph 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::graph::GraphDelete;
///
/// let cmd = GraphDelete::new("social");
/// ```
#[deprecated(
    since = "0.1.0",
    note = "RedisGraph has reached end-of-life. Consider migrating to FalkorDB or other graph databases."
)]
#[derive(Debug, Clone)]
pub struct GraphDelete {
    graph_name: String,
}

impl GraphDelete {
    /// Create a new GRAPH.DELETE command
    pub fn new(graph_name: impl Into<String>) -> Self {
        Self {
            graph_name: graph_name.into(),
        }
    }
}

impl Command for GraphDelete {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("GRAPH.DELETE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.graph_name.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// GRAPH.EXPLAIN - Get query execution plan
// ============================================================================

/// GRAPH.EXPLAIN - Get execution plan for a query without running it
///
/// **DEPRECATED**: RedisGraph has reached end-of-life
///
/// Available since: RedisGraph 1.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::graph::GraphExplain;
///
/// let cmd = GraphExplain::new("social", "MATCH (p:Person) RETURN p");
/// ```
#[deprecated(
    since = "0.1.0",
    note = "RedisGraph has reached end-of-life. Consider migrating to FalkorDB or other graph databases."
)]
#[derive(Debug, Clone)]
pub struct GraphExplain {
    graph_name: String,
    query: String,
}

impl GraphExplain {
    /// Create a new GRAPH.EXPLAIN command
    pub fn new(graph_name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            graph_name: graph_name.into(),
            query: query.into(),
        }
    }
}

impl Command for GraphExplain {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("GRAPH.EXPLAIN"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.graph_name.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.query.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let plan = items
                    .iter()
                    .filter_map(|f| {
                        if let Frame::BulkString(Some(data)) = f {
                            Some(String::from_utf8_lossy(data).to_string())
                        } else {
                            None
                        }
                    })
                    .collect();
                Ok(plan)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for GraphExplain {
    fn is_read_only(&self) -> bool {
        true
    }
}

// ============================================================================
// GRAPH.PROFILE - Profile query execution
// ============================================================================

/// GRAPH.PROFILE - Execute query and return execution plan with metrics
///
/// **DEPRECATED**: RedisGraph has reached end-of-life
///
/// Available since: RedisGraph 2.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::graph::GraphProfile;
///
/// let cmd = GraphProfile::new("social", "MATCH (p:Person) RETURN p");
/// ```
#[deprecated(
    since = "0.1.0",
    note = "RedisGraph has reached end-of-life. Consider migrating to FalkorDB or other graph databases."
)]
#[derive(Debug, Clone)]
pub struct GraphProfile {
    graph_name: String,
    query: String,
}

impl GraphProfile {
    /// Create a new GRAPH.PROFILE command
    pub fn new(graph_name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            graph_name: graph_name.into(),
            query: query.into(),
        }
    }
}

impl Command for GraphProfile {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("GRAPH.PROFILE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.graph_name.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.query.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        GraphExplain::parse_response(frame)
    }
}

impl ReadOnly for GraphProfile {
    fn is_read_only(&self) -> bool {
        true
    }
}

// ============================================================================
// GRAPH.SLOWLOG - Get slow query log
// ============================================================================

/// GRAPH.SLOWLOG - Get slow query log entries
///
/// **DEPRECATED**: RedisGraph has reached end-of-life
///
/// Available since: RedisGraph 2.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::graph::GraphSlowlog;
///
/// let cmd = GraphSlowlog::new("social");
/// ```
#[deprecated(
    since = "0.1.0",
    note = "RedisGraph has reached end-of-life. Consider migrating to FalkorDB or other graph databases."
)]
#[derive(Debug, Clone)]
pub struct GraphSlowlog {
    graph_name: String,
}

impl GraphSlowlog {
    /// Create a new GRAPH.SLOWLOG command
    pub fn new(graph_name: impl Into<String>) -> Self {
        Self {
            graph_name: graph_name.into(),
        }
    }
}

impl Command for GraphSlowlog {
    type Response = Vec<SlowlogEntry>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("GRAPH.SLOWLOG"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.graph_name.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut entries = Vec::new();
                for item in items {
                    if let Frame::Array(parts) = item {
                        if parts.len() >= 4 {
                            let timestamp = if let Frame::Integer(n) = parts[0] {
                                n
                            } else {
                                continue;
                            };
                            let command = if let Frame::BulkString(Some(data)) = &parts[1] {
                                String::from_utf8_lossy(data).to_string()
                            } else {
                                continue;
                            };
                            let query = if let Frame::BulkString(Some(data)) = &parts[2] {
                                String::from_utf8_lossy(data).to_string()
                            } else {
                                continue;
                            };
                            let execution_time_ms = if let Frame::BulkString(Some(data)) = &parts[3]
                            {
                                String::from_utf8_lossy(data).parse().unwrap_or(0.0)
                            } else if let Frame::Integer(n) = parts[3] {
                                n as f64
                            } else {
                                continue;
                            };

                            entries.push(SlowlogEntry {
                                timestamp,
                                command,
                                query,
                                execution_time_ms,
                            });
                        }
                    }
                }
                Ok(entries)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for GraphSlowlog {
    fn is_read_only(&self) -> bool {
        true
    }
}

// ============================================================================
// GRAPH.CONFIG - Get/Set configuration
// ============================================================================

/// GRAPH.CONFIG GET - Get configuration value
///
/// **DEPRECATED**: RedisGraph has reached end-of-life
///
/// Available since: RedisGraph 2.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::graph::GraphConfigGet;
///
/// let cmd = GraphConfigGet::new("TIMEOUT");
/// let cmd = GraphConfigGet::all(); // Get all config
/// ```
#[deprecated(
    since = "0.1.0",
    note = "RedisGraph has reached end-of-life. Consider migrating to FalkorDB or other graph databases."
)]
#[derive(Debug, Clone)]
pub struct GraphConfigGet {
    param: Option<String>,
}

impl GraphConfigGet {
    /// Create a new GRAPH.CONFIG GET command for specific parameter
    pub fn new(param: impl Into<String>) -> Self {
        Self {
            param: Some(param.into()),
        }
    }

    /// Get all configuration parameters
    pub fn all() -> Self {
        Self { param: None }
    }
}

impl Command for GraphConfigGet {
    type Response = HashMap<String, String>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("GRAPH.CONFIG"))),
            Frame::BulkString(Some(Bytes::from("GET"))),
        ];

        if let Some(ref param) = self.param {
            frames.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                param.as_bytes(),
            ))));
        } else {
            frames.push(Frame::BulkString(Some(Bytes::from("*"))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut config = HashMap::new();
                for pair in items.chunks(2) {
                    if pair.len() == 2 {
                        if let (Frame::BulkString(Some(k)), Frame::BulkString(Some(v))) =
                            (&pair[0], &pair[1])
                        {
                            config.insert(
                                String::from_utf8_lossy(k).to_string(),
                                String::from_utf8_lossy(v).to_string(),
                            );
                        }
                    }
                }
                Ok(config)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for GraphConfigGet {
    fn is_read_only(&self) -> bool {
        true
    }
}

/// GRAPH.CONFIG SET - Set configuration value
///
/// **DEPRECATED**: RedisGraph has reached end-of-life
///
/// Available since: RedisGraph 2.0.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::graph::GraphConfigSet;
///
/// let cmd = GraphConfigSet::new("TIMEOUT", "1000");
/// let cmd = GraphConfigSet::new("MAX_QUEUED_QUERIES", "25");
/// ```
#[deprecated(
    since = "0.1.0",
    note = "RedisGraph has reached end-of-life. Consider migrating to FalkorDB or other graph databases."
)]
#[derive(Debug, Clone)]
pub struct GraphConfigSet {
    param: String,
    value: String,
}

impl GraphConfigSet {
    /// Create a new GRAPH.CONFIG SET command
    pub fn new(param: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            param: param.into(),
            value: value.into(),
        }
    }
}

impl Command for GraphConfigSet {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("GRAPH.CONFIG"))),
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.param.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.value.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// ============================================================================
// GRAPH.LIST - List all graphs
// ============================================================================

/// GRAPH.LIST - List all graph keys in the database
///
/// **DEPRECATED**: RedisGraph has reached end-of-life
///
/// Available since: RedisGraph 2.2.0
///
/// # Examples
/// ```no_run
/// use redis_tower::modules::graph::GraphList;
///
/// let cmd = GraphList::new();
/// ```
#[deprecated(
    since = "0.1.0",
    note = "RedisGraph has reached end-of-life. Consider migrating to FalkorDB or other graph databases."
)]
#[derive(Debug, Clone)]
pub struct GraphList;

impl GraphList {
    /// Create a new GRAPH.LIST command
    pub fn new() -> Self {
        Self
    }
}

impl Default for GraphList {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for GraphList {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("GRAPH.LIST")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let graphs = items
                    .iter()
                    .filter_map(|f| {
                        if let Frame::BulkString(Some(data)) = f {
                            Some(String::from_utf8_lossy(data).to_string())
                        } else {
                            None
                        }
                    })
                    .collect();
                Ok(graphs)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for GraphList {
    fn is_read_only(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graph_query_basic() {
        let cmd = GraphQuery::new("social", "MATCH (p:Person) RETURN p");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("GRAPH.QUERY")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("social"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_graph_query_with_timeout() {
        let cmd = GraphQuery::new("social", "MATCH (p:Person) RETURN p").timeout(1000);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("--timeout"))))
                );
                assert!(
                    parts
                        .iter()
                        .any(|f| f == &Frame::BulkString(Some(Bytes::from("1000"))))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_graph_ro_query() {
        let cmd = GraphRoQuery::new("social", "MATCH (p:Person) RETURN p.name");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("GRAPH.RO_QUERY")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("social"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_graph_delete() {
        let cmd = GraphDelete::new("social");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("GRAPH.DELETE")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("social"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_graph_explain() {
        let cmd = GraphExplain::new("social", "MATCH (p:Person) RETURN p");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("GRAPH.EXPLAIN")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_graph_profile() {
        let cmd = GraphProfile::new("social", "MATCH (p:Person) RETURN p");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("GRAPH.PROFILE")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_graph_slowlog() {
        let cmd = GraphSlowlog::new("social");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("GRAPH.SLOWLOG")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("social"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_graph_config_get() {
        let cmd = GraphConfigGet::new("TIMEOUT");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("GRAPH.CONFIG")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("GET"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("TIMEOUT"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_graph_config_get_all() {
        let cmd = GraphConfigGet::all();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("*"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_graph_config_set() {
        let cmd = GraphConfigSet::new("TIMEOUT", "1000");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 4);
                assert_eq!(
                    parts[0],
                    Frame::BulkString(Some(Bytes::from("GRAPH.CONFIG")))
                );
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("SET"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("TIMEOUT"))));
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("1000"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_graph_list() {
        let cmd = GraphList::new();
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 1);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("GRAPH.LIST"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }
}
