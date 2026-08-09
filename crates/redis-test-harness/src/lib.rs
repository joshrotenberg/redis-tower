//! Test utilities for redis-tower.
//!
//! - [`mock::MockConnection`] -- in-memory frame queue for unit testing
//! - [`command_tests!`] -- macro for generating async command integration tests
//! - [`cluster::ClusterFixture`] -- managed six-node Redis Cluster for live tests
//!   and benchmarks

#![forbid(unsafe_code)]

#[macro_use]
pub mod command_tests;
pub mod cluster;
pub mod mock;
pub mod ports;
