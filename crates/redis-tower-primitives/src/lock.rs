//! Expiring distributed lock with fencing.
//!
//! [`DistributedLock::acquire`] atomically combines `SET NX PX` with an
//! `INCR` fencing counter. Release and extension compare the random owner token
//! before mutating the key. The TTL is mandatory and renewal exists only via
//! [`LockLease::spawn_renewal`], which consumes the lease and returns an owned
//! handle whose drop cancels and aborts the task.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::time::Duration;
//! use redis_tower::MultiplexedClient;
//! use redis_tower_primitives::DistributedLock;
//!
//! let mut client = MultiplexedClient::connect("127.0.0.1:6379").await?;
//! let lock = DistributedLock::new(
//!     "{invoice:42}:lock",
//!     "{invoice:42}:fence",
//!     Duration::from_secs(15),
//! )?;
//!
//! if let Some(lease) = lock.acquire(&mut client).await? {
//!     let fencing_token = lease.fencing_token();
//!     // Pass `fencing_token` to the resource protected by the lock.
//!     # let _ = fencing_token;
//!     lease.release(&mut client).await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Cluster keys
//!
//! Acquisition touches the lock and fencing keys in one script. Redis Cluster
//! therefore requires both keys in the same slot. Use an identical hash tag,
//! for example `{invoice:42}:lock` and `{invoice:42}:fence`.
//!
//! # Failure mode
//!
//! Mutual exclusion expires with the TTL. A paused or partitioned owner can
//! resume after a replacement owner acquires the lock, so the lock alone does
//! not prevent stale writes. The guarded system must persist and reject lower
//! [`LockLease::fencing_token`] values. Redis failover or loss of persistence
//! can also roll back the counter; configure Redis durability to match the
//! required safety level.

use std::fmt;
use std::time::Duration;

use redis_tower::{RedisError, RedisExecutor, Script};
use redis_tower_core::FromFrame;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::error::{ConfigurationError, duration_millis, require_key};

/// Atomic lock-acquisition Lua source.
///
/// The script is intentionally public so deployments can audit or preload it.
/// Its lines perform the following operations:
///
/// 1. Read the current owner from `KEYS[1]` (the lock key).
/// 2. If an owner exists, begin the contention branch.
/// 3. Return `0` without changing the fencing counter.
/// 4. End the contention branch.
/// 5. Increment `KEYS[2]` and retain the new fencing token.
/// 6. Set the owner token from `ARGV[1]` with `NX` and the required
///    millisecond TTL from `ARGV[2]`.
/// 7. If the defensive `NX` condition did not acquire the key, begin the
///    contention branch.
/// 8. Return `0` (a skipped fencing value is safe if this branch is reached).
/// 9. End the defensive contention branch.
/// 10. Return the positive fencing token to the caller.
pub const ACQUIRE_SCRIPT: &str = r#"local owner = redis.call('GET', KEYS[1])
if owner then
  return 0
end
local fencing = redis.call('INCR', KEYS[2])
local acquired = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
if not acquired then
  return 0
end
return fencing"#;

/// Compare-and-delete lock-release Lua source.
///
/// Its lines perform the following operations:
///
/// 1. Compare the value in `KEYS[1]` with the owner token in `ARGV[1]`.
/// 2. Delete the lock and return `1` only for the current owner.
/// 3. End the owner branch.
/// 4. Return `0` for an expired lease or a stale owner.
pub const RELEASE_SCRIPT: &str = r#"if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0"#;

/// Compare-and-extend lock-renewal Lua source.
///
/// Its lines perform the following operations:
///
/// 1. Compare the value in `KEYS[1]` with the owner token in `ARGV[1]`.
/// 2. Apply the required millisecond TTL from `ARGV[2]` and return `1` only
///    for the current owner.
/// 3. End the owner branch.
/// 4. Return `0` for an expired lease or a stale owner.
pub const EXTEND_SCRIPT: &str = r#"if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0"#;

/// Configuration for one expiring distributed lock.
#[derive(Clone)]
pub struct DistributedLock {
    lock_key: String,
    fencing_key: String,
    ttl: Duration,
    ttl_millis: u64,
    acquire_script: Script,
    release_script: Script,
    extend_script: Script,
}

impl DistributedLock {
    /// Create a lock with explicit Redis keys and a required TTL.
    ///
    /// In Redis Cluster, `lock_key` and `fencing_key` must contain the same
    /// hash tag so both keys route to one slot.
    pub fn new(
        lock_key: impl Into<String>,
        fencing_key: impl Into<String>,
        ttl: Duration,
    ) -> Result<Self, ConfigurationError> {
        let lock_key = require_key(lock_key, "lock_key")?;
        let fencing_key = require_key(fencing_key, "fencing_key")?;
        if lock_key == fencing_key {
            return Err(ConfigurationError::SameLockAndFencingKey);
        }
        let ttl_millis = duration_millis(ttl, "ttl")?;

        Ok(Self {
            lock_key,
            fencing_key,
            ttl,
            ttl_millis,
            acquire_script: Script::new(ACQUIRE_SCRIPT),
            release_script: Script::new(RELEASE_SCRIPT),
            extend_script: Script::new(EXTEND_SCRIPT),
        })
    }

    /// Return the Redis key that stores the expiring owner token.
    pub fn lock_key(&self) -> &str {
        &self.lock_key
    }

    /// Return the Redis key that stores the monotonic fencing counter.
    pub fn fencing_key(&self) -> &str {
        &self.fencing_key
    }

    /// Return the required lease TTL.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Try to acquire the lock atomically.
    ///
    /// `Ok(None)` means another owner currently holds the lock. A successful
    /// acquisition returns a lease carrying the fencing token that must be
    /// checked by the guarded resource.
    ///
    /// A connection error is indeterminate: Redis may have committed the
    /// script before the response was lost. Do not blindly retry with a new
    /// owner token when duplicate ownership would be unsafe.
    pub async fn acquire<E: RedisExecutor>(
        &self,
        executor: &mut E,
    ) -> Result<Option<LockLease>, RedisError> {
        let owner_token = random_token();
        let ttl = self.ttl_millis.to_string();
        let frame = self
            .acquire_script
            .execute(
                executor,
                &[self.lock_key.as_str(), self.fencing_key.as_str()],
                &[owner_token.as_str(), ttl.as_str()],
            )
            .await?;
        let fencing_token = u64::from_frame(frame)?;
        if fencing_token == 0 {
            return Ok(None);
        }

        Ok(Some(LockLease {
            lock_key: self.lock_key.clone(),
            owner_token,
            fencing_token,
            ttl: self.ttl,
            ttl_millis: self.ttl_millis,
            release_script: self.release_script.clone(),
            extend_script: self.extend_script.clone(),
        }))
    }
}

impl fmt::Debug for DistributedLock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DistributedLock")
            .field("lock_key", &self.lock_key)
            .field("fencing_key", &self.fencing_key)
            .field("ttl", &self.ttl)
            .finish()
    }
}

/// An acquired lock and its fencing token.
///
/// Dropping a lease does not contact Redis; its key expires at the configured
/// TTL. Call [`release`](Self::release) for early release or consume it with
/// [`spawn_renewal`](Self::spawn_renewal) for explicit background renewal.
pub struct LockLease {
    lock_key: String,
    owner_token: String,
    fencing_token: u64,
    ttl: Duration,
    ttl_millis: u64,
    release_script: Script,
    extend_script: Script,
}

impl LockLease {
    /// Return the Redis lock key.
    pub fn lock_key(&self) -> &str {
        &self.lock_key
    }

    /// Return the monotonic token that the guarded resource must enforce.
    pub fn fencing_token(&self) -> u64 {
        self.fencing_token
    }

    /// Return the TTL applied on acquisition and each extension.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Release the lock if this lease is still the owner.
    ///
    /// Returns `false` when the lease expired or another owner replaced it.
    pub async fn release<E: RedisExecutor>(&self, executor: &mut E) -> Result<bool, RedisError> {
        release(
            executor,
            &self.release_script,
            self.lock_key.as_str(),
            self.owner_token.as_str(),
        )
        .await
    }

    /// Extend the lock to its original required TTL if this lease still owns it.
    ///
    /// Returns `false` when the lease expired or another owner replaced it.
    pub async fn extend<E: RedisExecutor>(&self, executor: &mut E) -> Result<bool, RedisError> {
        extend(
            executor,
            &self.extend_script,
            self.lock_key.as_str(),
            self.owner_token.as_str(),
            self.ttl_millis,
        )
        .await
    }

    /// Consume this lease and start explicit periodic renewal.
    ///
    /// `interval` must be positive and shorter than the lock TTL. The returned
    /// handle owns the lease lifecycle; dropping it cancels and aborts the
    /// background task. Use [`LockRenewalHandle::shutdown`] to stop cleanly and
    /// recover the lease for explicit release. The task consumes and drops
    /// `executor`; pass a cheap client clone or a dedicated connection.
    ///
    /// Renewal stops on the first Redis error or ownership mismatch. A process
    /// pause or partition can still exceed the TTL, so fencing remains required.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime.
    pub fn spawn_renewal<E>(
        self,
        mut executor: E,
        interval: Duration,
    ) -> Result<LockRenewalHandle, ConfigurationError>
    where
        E: RedisExecutor + Send + 'static,
    {
        if interval.is_zero() {
            return Err(ConfigurationError::ZeroDuration {
                parameter: "renewal interval",
            });
        }
        if interval >= self.ttl {
            return Err(ConfigurationError::RenewalIntervalNotShorter {
                interval,
                ttl: self.ttl,
            });
        }

        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let lock_key = self.lock_key.clone();
        let owner_token = self.owner_token.clone();
        let ttl_millis = self.ttl_millis;
        let extend_script = self.extend_script.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = task_cancellation.cancelled() => {
                        return RenewalOutcome::Stopped;
                    }
                    () = tokio::time::sleep(interval) => {}
                }

                match extend(
                    &mut executor,
                    &extend_script,
                    lock_key.as_str(),
                    owner_token.as_str(),
                    ttl_millis,
                )
                .await
                {
                    Ok(true) => {}
                    Ok(false) => return RenewalOutcome::OwnershipLost,
                    Err(error) => return RenewalOutcome::RedisError(error),
                }
            }
        });

        Ok(LockRenewalHandle {
            lease: Some(self),
            cancellation,
            task: Some(task),
        })
    }
}

impl fmt::Debug for LockLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LockLease")
            .field("lock_key", &self.lock_key)
            .field("owner_token", &"[REDACTED]")
            .field("fencing_token", &self.fencing_token)
            .field("ttl", &self.ttl)
            .finish()
    }
}

/// Terminal result from an owned lock-renewal task.
#[derive(Debug)]
pub enum RenewalOutcome {
    /// The owner explicitly stopped the renewal handle.
    Stopped,
    /// The lock expired or a different owner token replaced it.
    OwnershipLost,
    /// Redis could not process a renewal.
    RedisError(RedisError),
}

/// Owned handle for an explicitly spawned lock-renewal task.
///
/// Dropping this handle cancels and aborts renewal. It intentionally does not
/// release the Redis key during drop because destructors cannot perform async
/// I/O; the key remains bounded by its required TTL.
#[must_use = "dropping the handle stops renewal and leaves release to the TTL"]
pub struct LockRenewalHandle {
    lease: Option<LockLease>,
    cancellation: CancellationToken,
    task: Option<JoinHandle<RenewalOutcome>>,
}

impl LockRenewalHandle {
    /// Return the Redis lock key owned by this renewal handle.
    pub fn lock_key(&self) -> &str {
        self.lease().lock_key()
    }

    /// Return the fencing token owned by this renewal handle.
    pub fn fencing_token(&self) -> u64 {
        self.lease().fencing_token()
    }

    /// Return whether the background task has already terminated.
    pub fn is_finished(&self) -> bool {
        self.task.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Stop renewal, wait for the task, and return the lease and outcome.
    ///
    /// The returned lease can then be explicitly released. If the task had
    /// already observed an error or ownership loss, its outcome preserves that
    /// fact and releasing the stale lease safely returns `false`.
    pub async fn shutdown(mut self) -> Result<(LockLease, RenewalOutcome), JoinError> {
        self.cancellation.cancel();
        self.finish().await
    }

    /// Wait for renewal to terminate because ownership was lost or Redis
    /// failed, then return the lease and outcome.
    pub async fn wait(mut self) -> Result<(LockLease, RenewalOutcome), JoinError> {
        self.finish().await
    }

    async fn finish(&mut self) -> Result<(LockLease, RenewalOutcome), JoinError> {
        let outcome = self.task.take().expect("renewal task is present").await?;
        let lease = self.lease.take().expect("renewal lease is present");
        Ok((lease, outcome))
    }

    fn lease(&self) -> &LockLease {
        self.lease.as_ref().expect("renewal lease is present")
    }
}

impl fmt::Debug for LockRenewalHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LockRenewalHandle")
            .field("lease", &self.lease)
            .field("is_finished", &self.is_finished())
            .finish()
    }
}

impl Drop for LockRenewalHandle {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn release<E: RedisExecutor>(
    executor: &mut E,
    script: &Script,
    lock_key: &str,
    owner_token: &str,
) -> Result<bool, RedisError> {
    let frame = script
        .execute(executor, &[lock_key], &[owner_token])
        .await?;
    bool::from_frame(frame)
}

async fn extend<E: RedisExecutor>(
    executor: &mut E,
    script: &Script,
    lock_key: &str,
    owner_token: &str,
    ttl_millis: u64,
) -> Result<bool, RedisError> {
    let ttl = ttl_millis.to_string();
    let frame = script
        .execute(executor, &[lock_key], &[owner_token, ttl.as_str()])
        .await?;
    bool::from_frame(frame)
}

fn random_token() -> String {
    format!("{:032x}", rand::random::<u128>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_configuration_requires_distinct_keys_and_ttl() {
        assert_eq!(
            DistributedLock::new("same", "same", Duration::from_secs(1)).unwrap_err(),
            ConfigurationError::SameLockAndFencingKey
        );
        assert_eq!(
            DistributedLock::new("lock", "fence", Duration::ZERO).unwrap_err(),
            ConfigurationError::ZeroDuration { parameter: "ttl" }
        );
    }

    #[test]
    fn owner_tokens_are_opaque_and_debug_is_redacted() {
        let first = random_token();
        let second = random_token();
        assert_ne!(first, second);
        assert_eq!(first.len(), 32);

        let lease = LockLease {
            lock_key: "{x}:lock".into(),
            owner_token: first.clone(),
            fencing_token: 7,
            ttl: Duration::from_secs(1),
            ttl_millis: 1_000,
            release_script: Script::new(RELEASE_SCRIPT),
            extend_script: Script::new(EXTEND_SCRIPT),
        };
        let debug = format!("{lease:?}");
        assert!(!debug.contains(&first));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn public_scripts_express_required_atomic_operations() {
        assert!(ACQUIRE_SCRIPT.contains("'SET', KEYS[1]"));
        assert!(ACQUIRE_SCRIPT.contains("'NX', 'PX'"));
        assert!(ACQUIRE_SCRIPT.contains("'INCR', KEYS[2]"));
        assert!(RELEASE_SCRIPT.contains("'DEL', KEYS[1]"));
        assert!(EXTEND_SCRIPT.contains("'PEXPIRE', KEYS[1]"));
    }
}
