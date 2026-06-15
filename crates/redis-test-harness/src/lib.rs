//! Test utilities for redis-tower.
//!
//! - [`mock::MockConnection`] -- in-memory frame queue for unit testing
//! - [`command_tests!`] -- macro for generating async command integration tests

#![forbid(unsafe_code)]

#[macro_use]
pub mod command_tests;
pub mod mock;
