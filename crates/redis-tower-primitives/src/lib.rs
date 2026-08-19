//! Distributed coordination primitives built on `redis-tower`.
//!
//! This crate provides expiring locks, leader election, expirable semaphores,
//! countdown latches, and Redis-time rate limiting. Every primitive executes
//! published Lua through [`redis_tower::Script`], so calls use EVALSHA first
//! and fall back to EVAL only when Redis reports `NOSCRIPT`.
//!
//! # Failure model
//!
//! Redis is not a consensus system. A process pause or network partition can
//! outlive a lease TTL, and failover can lose writes that were not durably
//! replicated. Lock users must pass [`LockLease::fencing_token`] to the guarded
//! resource and have that resource reject stale tokens. Leader and semaphore
//! users must likewise tolerate work resumed by an expired holder. Latch
//! expiry is reported separately from release, and rate-limit calls return
//! Redis failures to the caller; applications must choose recovery and
//! fail-open or fail-closed behavior explicitly.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod error;
pub mod latch;
pub mod leader;
pub mod lock;
pub mod rate_limiter;
pub mod semaphore;

pub use error::ConfigurationError;
pub use latch::{
    COUNT_DOWN_SCRIPT, CountDownLatch, INITIALIZE_LATCH_SCRIPT, LatchCountDown, LatchWaitError,
    LatchWaitOutcome, READ_LATCH_SCRIPT,
};
pub use leader::{
    ABDICATE_SCRIPT, CAMPAIGN_SCRIPT, Campaign, LeaderElection, Leadership, LeadershipEvent,
    LeadershipEvents, LeadershipOutcome, RENEW_LEADERSHIP_SCRIPT,
};
pub use lock::{
    ACQUIRE_SCRIPT, DistributedLock, EXTEND_SCRIPT, LockLease, LockRenewalHandle, RELEASE_SCRIPT,
    RenewalOutcome,
};
pub use rate_limiter::{GCRA_SCRIPT, GcraRateLimiter, RateLimitDecision};
pub use semaphore::{
    ACQUIRE_PERMIT_SCRIPT, ExpirableSemaphore, RELEASE_PERMIT_SCRIPT, RENEW_PERMIT_SCRIPT,
    SemaphorePermit,
};
