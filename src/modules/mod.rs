//! Redis Stack module support (feature-gated)
//!
//! This module provides support for Redis Stack modules like RedisBloom, RedisJSON,
//! RediSearch, RedisTimeSeries, and RedisGraph.
//!
//! Each module is behind a feature flag to keep binary size minimal:
//! ```toml
//! redis-tower = { version = "0.1", features = ["bloom"] }
//! redis-tower = { version = "0.1", features = ["json", "search"] }
//! ```
//!
//! # Available Modules
//!
//! ## RedisBloom (`bloom` feature)
//! Probabilistic data structures for efficient membership testing:
//! - Bloom filters - Space-efficient set membership with configurable false positive rate
//! - Cuckoo filters - Better deletion support than Bloom filters
//! - Count-Min Sketch - Frequency estimation
//! - Top-K - Track most frequent items
//!
//! ## RedisJSON (`json` feature) - Coming Soon
//! Native JSON document storage and manipulation:
//! - Store, update, and fetch JSON values
//! - JSONPath queries
//! - Atomic operations on JSON documents
//!
//! ## RediSearch (`search` feature) - Coming Soon
//! Full-text search and secondary indexing:
//! - Full-text search with stemming and phonetic matching
//! - Geo-spatial queries
//! - Aggregations and transformations
//! - Auto-complete and suggestions
//!
//! ## RedisTimeSeries (`timeseries` feature) - Coming Soon
//! Time-series data storage and queries:
//! - High-performance time-series ingestion
//! - Downsampling and compaction
//! - Aggregations over time windows
//! - Built-in retention policies
//!
//! ## RedisGraph (`graph` feature) - Coming Soon
//! Graph database using Cypher query language:
//! - Property graph model
//! - Cypher query support
//! - Path queries and graph algorithms

#[cfg(feature = "bloom")]
pub mod bloom;

// Coming soon - uncomment when implemented:
// #[cfg(feature = "json")]
// pub mod json;
//
// #[cfg(feature = "search")]
// pub mod search;
//
// #[cfg(feature = "timeseries")]
// pub mod timeseries;
//
// #[cfg(feature = "graph")]
// pub mod graph;

// Re-export module types for convenience
#[cfg(feature = "bloom")]
pub use bloom::{
    BfAdd, BfCard, BfDebug, BfExists, BfInfo, BfInfoResult, BfInsert, BfLoadChunk, BfMadd,
    BfMexists, BfReserve, BfScanDump, BfScanDumpResult,
};
