//! Test utilities for redis-tower.
//!
//! - [`mock::MockConnection`] -- in-memory frame queue for unit testing
//! - [`command_tests!`] -- macro for generating async command integration tests
//! - [`cluster::ClusterFixture`] -- managed six-node Redis Cluster for live tests
//!   and benchmarks
//! - [`port_ranges`] -- registry of the fixed ports live-server fixtures reserve,
//!   with a regression test guarding against a new fixture colliding with one
//!   that already exists

#![forbid(unsafe_code)]
#![deny(missing_docs)]

#[macro_use]
/// Reusable command-contract test macros for standalone and clustered clients.
pub mod command_tests;
pub mod cluster;
pub mod mock;
/// Fixed port ranges reserved by live-server tests and benchmarks.
pub mod port_ranges;
