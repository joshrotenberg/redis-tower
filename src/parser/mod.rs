//! High-performance RESP (REdis Serialization Protocol) parser
//!
//! A production-ready, zero-copy parser for RESP2 and RESP3 protocols with comprehensive
//! test coverage including property-based testing, large payload validation, and full
//! RESP3 specification compliance.
//!
//! # Features
//!
//! - **Zero-copy parsing**: Uses `bytes::Bytes` for efficient memory management
//! - **RESP2 and RESP3**: Full support for both protocol versions
//! - **Streaming support**: Handle large payloads and streaming sequences
//! - **Type safety**: Strong typing for all RESP data types
//! - **High performance**: 4.8-8.0 GB/s throughput, 34-48ns/iter
//! - **Battle-tested**: 104 parser tests including property tests
//!
//! # Quick Start
//!
//! ## Parsing a single frame
//!
//! ```
//! use bytes::Bytes;
//! use redis_tower::parser::resp3::parse_frame;
//!
//! let data = Bytes::from("+OK\r\n");
//! let (frame, remaining) = parse_frame(data).unwrap();
//! ```
//!
//! ## Streaming parser for incremental data
//!
//! ```
//! use bytes::Bytes;
//! use redis_tower::parser::{Parser, Frame};
//!
//! let mut parser = Parser::new();
//!
//! // Feed data incrementally
//! parser.feed(Bytes::from("+HEL"));
//! assert!(parser.next_frame().is_none()); // Incomplete
//!
//! parser.feed(Bytes::from("LO\r\n"));
//! let frame = parser.next_frame().unwrap(); // Complete!
//! ```
//!
//! # Architecture
//!
//! - [`resp3`]: Main RESP3 parser implementation
//! - [`frame`]: Frame types and utilities
//! - [`serializer`]: Frame serialization to wire format
//! - [`error`]: Error types and handling

pub mod error;
pub mod frame;
pub mod resp3;
pub mod serializer;

// Re-export commonly used types
pub use error::{RespError, Result};
pub use frame::RespFrame;
pub use resp3::{Frame, ParseError, Parser, parse_frame};
pub use serializer::RespSerializer;
