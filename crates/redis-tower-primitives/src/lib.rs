//! Distributed coordination primitives built on `redis-tower`.
//!
//! This crate provides expiring locks, leader election, expirable semaphores,
//! countdown latches, delayed queues, block-allocated IDs, and Redis-time rate
//! limiting. Scripted primitives execute published Lua through
//! [`redis_tower::Script`], so calls use EVALSHA first and fall back to EVAL
//! only when Redis reports `NOSCRIPT`.
//!
//! # Choose a primitive
//!
//! | Need | Module |
//! |---|---|
//! | Exclusive ownership with fencing | [`lock`] |
//! | One renewable active leader | [`leader`] |
//! | A bounded number of expiring holders | [`semaphore`] |
//! | Wait for a distributed count to reach zero | [`latch`] |
//! | Claim due payloads in deadline order | [`delayed_queue`] |
//! | Allocate IDs in local blocks | [`id_generator`] |
//! | Enforce a shared Redis-time quota | [`rate_limiter`] |
//!
//! # Quick start
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::time::Duration;
//! use redis_tower::MultiplexedClient;
//! use redis_tower_primitives::DistributedLock;
//!
//! let mut client = MultiplexedClient::connect("127.0.0.1:6379").await?;
//! let lock = DistributedLock::new(
//!     "{job:17}:lock",
//!     "{job:17}:fence",
//!     Duration::from_secs(15),
//! )?;
//!
//! if let Some(lease) = lock.acquire(&mut client).await? {
//!     let fencing_token = lease.fencing_token();
//!     // The guarded resource must reject fencing tokens older than this one.
//!     # let _ = fencing_token;
//!     lease.release(&mut client).await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Failure model
//!
//! Redis is not a consensus system. A process pause or network partition can
//! outlive a lease TTL, and failover can lose writes that were not durably
//! replicated. Lock users must pass [`LockLease::fencing_token`] to the guarded
//! resource and have that resource reject stale tokens. Leader and semaphore
//! users must likewise tolerate work resumed by an expired holder. Latch
//! expiry is reported separately from release. Delayed claims are at-most-once,
//! and ID uniqueness depends on a persistent counter that failover cannot roll
//! back. Rate-limit calls return Redis failures to the caller; applications
//! must choose recovery and fail-open or fail-closed behavior explicitly.
//!
//! The [distributed primitives guide](https://github.com/joshrotenberg/redis-tower/blob/main/docs/PRIMITIVES.md)
//! documents lifecycle, Cluster key placement, and failure semantics for every
//! primitive.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod delayed_queue;
mod error;
pub mod id_generator;
pub mod latch;
pub mod leader;
pub mod lock;
pub mod rate_limiter;
pub mod semaphore;

pub use delayed_queue::{
    CLAIM_DUE_SCRIPT, ClaimBatch, DelayedQueue, DelayedQueueError, ENQUEUE_DELAYED_SCRIPT,
};
pub use error::ConfigurationError;
pub use id_generator::{ID_ALLOCATION_COMMAND, IdBlock, IdGenerator};
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
