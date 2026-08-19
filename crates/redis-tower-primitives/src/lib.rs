//! Distributed coordination primitives built on `redis-tower`.
//!
//! This crate provides an expiring [`DistributedLock`] with fencing tokens and
//! a Redis-time [`GcraRateLimiter`]. Both primitives execute published Lua
//! through [`redis_tower::Script`], so calls use EVALSHA first and fall back to
//! EVAL only when Redis reports `NOSCRIPT`.
//!
//! # Failure model
//!
//! Redis is not a consensus system. A process pause or network partition can
//! outlive a lock TTL, and failover can lose writes that were not durably
//! replicated. Lock users must pass [`LockLease::fencing_token`] to the guarded
//! resource and have that resource reject stale tokens. Rate-limit calls return
//! Redis failures to the caller; the application must deliberately choose
//! fail-open or fail-closed behavior rather than receiving an implicit policy.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod error;
pub mod lock;
pub mod rate_limiter;

pub use error::ConfigurationError;
pub use lock::{
    ACQUIRE_SCRIPT, DistributedLock, EXTEND_SCRIPT, LockLease, LockRenewalHandle, RELEASE_SCRIPT,
    RenewalOutcome,
};
pub use rate_limiter::{GCRA_SCRIPT, GcraRateLimiter, RateLimitDecision};
