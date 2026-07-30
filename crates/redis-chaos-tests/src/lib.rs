//! Docker-backed compatibility and fault-injection tests for redis-tower.
//!
//! This crate deliberately contains no runtime API. Its Docker tests are
//! `#[ignore]`-gated so ordinary workspace test runs remain infrastructure
//! free.
