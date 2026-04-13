//! Test utilities for redis-tower.
//!
//! - [`mock::MockConnection`] -- in-memory frame queue for unit testing
//! - [`command_tests!`] -- macro for generating async command integration tests

#[macro_use]
pub mod command_tests;
pub mod mock;
