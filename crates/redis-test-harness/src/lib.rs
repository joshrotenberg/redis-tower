//! Test utilities for redis-tower.
//!
//! - [`mock::MockConnection`] -- in-memory frame queue for unit testing
//! - [`command_tests!`] -- macro for generating async command integration tests
//! - on Unix, `cluster::ClusterFixture` -- managed six-node Redis Cluster for
//!   live tests and benchmarks
//! - [`port_ranges`] -- registry of the fixed ports live-server fixtures reserve,
//!   with a regression test guarding against a new fixture colliding with one
//!   that already exists
//!
//! # Quick start
//!
//! [`mock::MockConnection`] runs typed response parsing without a Redis server:
//!
//! ```
//! use redis_tower_test::mock::MockConnection;
//! use redis_tower_protocol::Frame;
//!
//! let mut mock = MockConnection::new();
//! mock.enqueue(Frame::Integer(42));
//! assert!(matches!(mock.next_response()?, Frame::Integer(42)));
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! On Unix, [`cluster::ClusterFixture`] provides an owned six-node Cluster for
//! topology, resharding, failover, and benchmark tests. Use [`command_tests!`]
//! when the same typed command contract should run against several backends.

#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[macro_use]
/// Reusable command-contract test macros for standalone and clustered clients.
pub mod command_tests;
#[cfg(unix)]
#[cfg_attr(docsrs, doc(cfg(unix)))]
pub mod cluster;
pub mod mock;
/// Fixed port ranges reserved by live-server tests and benchmarks.
pub mod port_ranges;
