//! Type-safe wrappers for `redis-server` and `redis-cli` CLI tools.
//!
//! These provide builder-pattern interfaces for starting and managing
//! Redis server processes. No Docker, no dependencies beyond `redis-server`
//! and `redis-cli` on PATH.

pub mod cli;
pub mod cluster;
pub mod sentinel;
pub mod server;
