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
//! ## RedisTimeSeries (`timeseries` feature)
//! Time-series data storage and queries with automatic downsampling:
//! - High-performance time-series ingestion with configurable retention
//! - Automatic downsampling and compaction with aggregation rules
//! - Aggregations over time windows (AVG, SUM, MIN, MAX, COUNT, etc.)
//! - Label-based indexing for multi-series queries
//! - Built-in retention policies and memory optimization
//!
//! ## RedisGraph (`graph` feature) - DEPRECATED
//! **DEPRECATED**: RedisGraph has reached end-of-life and is no longer maintained.
//! Graph database using Cypher query language (deprecated, use FalkorDB as alternative):
//! - Property graph model with nodes and relationships
//! - Cypher query support for pattern matching
//! - Execution plan analysis and profiling
//! - Read-only queries and configuration management
//!
//! **Note**: RedisGraph was deprecated by Redis, Inc. in 2024. Consider:
//! - **FalkorDB** - Community fork with compatible API
//! - **Neo4j** - Popular graph database with Cypher
//! - **Amazon Neptune** - Managed graph database service

#[cfg(feature = "bloom")]
pub mod bloom;

#[cfg(feature = "bloom")]
pub mod cuckoo;

#[cfg(feature = "bloom")]
pub mod cms;

#[cfg(feature = "bloom")]
pub mod topk;

#[cfg(feature = "bloom")]
pub mod tdigest;

#[cfg(feature = "json")]
pub mod json;

#[cfg(feature = "search")]
pub mod search;

#[cfg(feature = "timeseries")]
pub mod timeseries;

#[cfg(feature = "graph")]
pub mod graph;

pub mod vector;

// Re-export module types for convenience
#[cfg(feature = "bloom")]
pub use bloom::{
    BfAdd, BfCard, BfDebug, BfExists, BfInfo, BfInfoResult, BfInsert, BfLoadChunk, BfMadd,
    BfMexists, BfReserve, BfScanDump, BfScanDumpResult,
};

#[cfg(feature = "bloom")]
pub use cuckoo::{
    CfAdd, CfAddNx, CfCount, CfDel, CfExists, CfInfo, CfInfoResult, CfInsert, CfInsertNx, CfReserve,
};

#[cfg(feature = "bloom")]
pub use cms::{CmsIncrBy, CmsInfo, CmsInfoResult, CmsInitByDim, CmsInitByProb, CmsMerge, CmsQuery};

#[cfg(feature = "bloom")]
pub use topk::{
    TopKAdd, TopKCount, TopKIncrBy, TopKInfo, TopKInfoResult, TopKList, TopKListResult, TopKQuery,
    TopKReserve,
};

#[cfg(feature = "bloom")]
pub use tdigest::{
    TDigestAdd, TDigestByRank, TDigestByRevRank, TDigestCdf, TDigestCreate, TDigestInfo,
    TDigestInfoResult, TDigestMax, TDigestMerge, TDigestMin, TDigestQuantile, TDigestRank,
    TDigestReset, TDigestRevRank, TDigestTrimmedMean,
};

#[cfg(feature = "json")]
pub use json::{
    JsonArrAppend, JsonArrIndex, JsonArrInsert, JsonArrLen, JsonArrPop, JsonArrTrim, JsonClear,
    JsonDebug, JsonDebugHelp, JsonDebugSubcommand, JsonDel, JsonForget, JsonGet, JsonMGet,
    JsonMSet, JsonMerge, JsonNumIncrBy, JsonNumMultBy, JsonObjKeys, JsonObjLen, JsonResp, JsonSet,
    JsonStrAppend, JsonStrLen, JsonToggle, JsonType,
};

#[cfg(feature = "timeseries")]
pub use timeseries::{
    Aggregator, BucketTimestamp, CompactionRule, DuplicatePolicy, Encoding, MGetResult,
    MRangeResult, Sample, TimeSeriesInfo, TsAdd, TsAlter, TsCreate, TsCreateRule, TsDecrBy, TsDel,
    TsDeleteRule, TsGet, TsIncrBy, TsInfo, TsMAdd, TsMGet, TsMRange, TsMRevRange, TsQueryIndex,
    TsRange, TsRevRange,
};

#[cfg(feature = "graph")]
#[allow(deprecated)]
pub use graph::{
    GraphConfigGet, GraphConfigSet, GraphDelete, GraphExplain, GraphList, GraphProfile, GraphQuery,
    GraphRoQuery, GraphSlowlog, QueryResult, QueryStatistics, SlowlogEntry,
};

pub use vector::{
    Vadd, Vcard, Vdim, Vemb, Vgetattr, Vinfo, Vismember, Vlinks, Vrandmember, Vrem, Vsetattr, Vsim,
};
