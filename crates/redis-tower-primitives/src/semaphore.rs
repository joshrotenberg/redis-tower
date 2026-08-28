//! Single-key semaphore whose permits expire after holder failure.
//!
//! [`ExpirableSemaphore`] stores random permit tokens in a sorted set scored by
//! Redis-time lease deadlines. Acquisition prunes expired holders before
//! checking capacity. A permit can be renewed or explicitly released; dropping
//! it performs no I/O and leaves recovery to its required TTL.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::time::Duration;
//! use redis_tower::MultiplexedClient;
//! use redis_tower_primitives::ExpirableSemaphore;
//!
//! let mut client = MultiplexedClient::connect("127.0.0.1:6379").await?;
//! let semaphore = ExpirableSemaphore::new(
//!     "workers:permits",
//!     16,
//!     Duration::from_secs(30),
//! )?;
//!
//! if let Some(permit) = semaphore.try_acquire(&mut client).await? {
//!     # let _remaining = permit.remaining_at_acquire();
//!     permit.release(&mut client).await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Cluster keys
//!
//! All operations touch one key and are Redis Cluster safe without a hash tag.
//! Every participant sharing a key must use the same permit limit and TTL.
//!
//! # Failure mode
//!
//! Permit expiry restores capacity after a crashed holder, but a paused holder
//! can resume after its permit expires. The protected resource must tolerate or
//! independently reject stale work. Connection failures are indeterminate:
//! Redis may have acquired, renewed, or released a permit before the response
//! was lost.

use std::fmt;
use std::time::Duration;

use redis_tower::{RedisError, RedisExecutor, Script};
use redis_tower_core::FromFrame;

use crate::error::{ConfigurationError, duration_millis, require_key};

/// Atomic expirable-permit acquisition Lua source.
///
/// The script is public for auditing and preloading. Its lines perform the
/// following operations:
///
/// 1. Read Redis server time.
/// 2. Convert seconds and microseconds into one millisecond timestamp.
/// 3. Parse the configured permit limit from `ARGV[1]`.
/// 4. Parse the required permit TTL from `ARGV[2]`.
/// 5. Remove permit members whose lease deadline has passed.
/// 6. Count the permits that remain in use.
/// 7. Begin the contention branch when capacity is exhausted.
/// 8. Return not-acquired and zero available permits.
/// 9. End the contention branch.
/// 10. Calculate the new permit's Redis-time lease deadline.
/// 11. Add the random permit token from `ARGV[3]` only if it is new.
/// 12. Bound the sorted-set key by the latest possible configured lease.
/// 13. Return acquired and the capacity remaining after acquisition.
pub const ACQUIRE_PERMIT_SCRIPT: &str = r#"local server_time = redis.call('TIME')
local now = (tonumber(server_time[1]) * 1000) + math.floor(tonumber(server_time[2]) / 1000)
local limit = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local used = redis.call('ZCARD', KEYS[1])
if used >= limit then
  return {0, 0}
end
local expires_at = now + ttl
redis.call('ZADD', KEYS[1], 'NX', expires_at, ARGV[3])
redis.call('PEXPIRE', KEYS[1], ttl)
return {1, limit - used - 1}"#;

/// Compare-and-renew permit Lua source.
///
/// Its lines perform the following operations:
///
/// 1. Read Redis server time.
/// 2. Convert seconds and microseconds into one millisecond timestamp.
/// 3. Remove expired permit members.
/// 4. Begin the stale-permit branch when `ARGV[1]` is no longer present.
/// 5. Return `0` without recreating an expired permit.
/// 6. End the stale-permit branch.
/// 7. Calculate a new deadline using the required TTL in `ARGV[2]`.
/// 8. Update only the existing permit token's score.
/// 9. Bound the sorted-set key by the renewed lease.
/// 10. Return `1` for the renewed permit.
pub const RENEW_PERMIT_SCRIPT: &str = r#"local server_time = redis.call('TIME')
local now = (tonumber(server_time[1]) * 1000) + math.floor(tonumber(server_time[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  return 0
end
local expires_at = now + tonumber(ARGV[2])
redis.call('ZADD', KEYS[1], 'XX', expires_at, ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1"#;

/// Permit release Lua source.
///
/// Its lines perform the following operations:
///
/// 1. Remove the exact random permit token in `ARGV[1]` from `KEYS[1]`.
/// 2. Begin cleanup when the sorted set is now empty.
/// 3. Delete the empty key rather than retaining metadata.
/// 4. End the empty-set cleanup branch.
/// 5. Return whether the caller's permit token was present.
pub const RELEASE_PERMIT_SCRIPT: &str = r#"local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if redis.call('ZCARD', KEYS[1]) == 0 then
  redis.call('DEL', KEYS[1])
end
return removed"#;

/// Configuration for one shared semaphore with expiring permits.
#[derive(Clone)]
pub struct ExpirableSemaphore {
    key: String,
    permit_limit: u32,
    ttl: Duration,
    ttl_millis: u64,
    acquire_script: Script,
    renew_script: Script,
    release_script: Script,
}

impl ExpirableSemaphore {
    /// Create a semaphore with an explicit limit and required permit TTL.
    pub fn new(
        key: impl Into<String>,
        permit_limit: u32,
        ttl: Duration,
    ) -> Result<Self, ConfigurationError> {
        let key = require_key(key, "key")?;
        if permit_limit == 0 {
            return Err(ConfigurationError::ZeroPermitLimit);
        }
        let ttl_millis = duration_millis(ttl, "ttl")?;

        Ok(Self {
            key,
            permit_limit,
            ttl,
            ttl_millis,
            acquire_script: Script::new(ACQUIRE_PERMIT_SCRIPT),
            renew_script: Script::new(RENEW_PERMIT_SCRIPT),
            release_script: Script::new(RELEASE_PERMIT_SCRIPT),
        })
    }

    /// Return the single Redis sorted-set key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Return the configured shared permit limit.
    pub fn permit_limit(&self) -> u32 {
        self.permit_limit
    }

    /// Return the required permit TTL.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Try to acquire one expiring permit atomically.
    ///
    /// `Ok(None)` means all permits are currently held. Expired holders are
    /// pruned against Redis server time before capacity is checked.
    pub async fn try_acquire<E: RedisExecutor>(
        &self,
        executor: &mut E,
    ) -> Result<Option<SemaphorePermit>, RedisError> {
        let token = random_token();
        let limit = self.permit_limit.to_string();
        let ttl = self.ttl_millis.to_string();
        let frame = self
            .acquire_script
            .execute(
                executor,
                &[self.key.as_str()],
                &[limit.as_str(), ttl.as_str(), token.as_str()],
            )
            .await?;
        let values = Vec::<u64>::from_frame(frame)?;
        let valid = match values.as_slice() {
            [0, 0] => true,
            [1, remaining] => *remaining < u64::from(self.permit_limit),
            _ => false,
        };
        if !valid {
            return Err(RedisError::UnexpectedResponse {
                expected: "semaphore array [0, 0] or [1, remaining below permit limit]",
                actual: format!("{values:?}"),
            });
        }
        if values[0] == 0 {
            return Ok(None);
        }

        Ok(Some(SemaphorePermit {
            key: self.key.clone(),
            token,
            ttl: self.ttl,
            ttl_millis: self.ttl_millis,
            remaining_at_acquire: values[1] as u32,
            renew_script: self.renew_script.clone(),
            release_script: self.release_script.clone(),
        }))
    }
}

impl fmt::Debug for ExpirableSemaphore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExpirableSemaphore")
            .field("key", &self.key)
            .field("permit_limit", &self.permit_limit)
            .field("ttl", &self.ttl)
            .finish()
    }
}

/// One expiring semaphore permit identified by a random token.
///
/// Dropping a permit performs no Redis I/O. Call [`release`](Self::release) for
/// early capacity recovery; otherwise the required TTL bounds the permit.
pub struct SemaphorePermit {
    key: String,
    token: String,
    ttl: Duration,
    ttl_millis: u64,
    remaining_at_acquire: u32,
    renew_script: Script,
    release_script: Script,
}

impl SemaphorePermit {
    /// Return the Redis semaphore key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Return the permit TTL.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Return the server-reported capacity immediately after this permit was
    /// acquired.
    pub fn remaining_at_acquire(&self) -> u32 {
        self.remaining_at_acquire
    }

    /// Renew this permit to its original TTL if it has not expired.
    ///
    /// Returns `false` rather than recreating a stale permit.
    pub async fn renew<E: RedisExecutor>(&self, executor: &mut E) -> Result<bool, RedisError> {
        let ttl = self.ttl_millis.to_string();
        let frame = self
            .renew_script
            .execute(
                executor,
                &[self.key.as_str()],
                &[self.token.as_str(), ttl.as_str()],
            )
            .await?;
        bool::from_frame(frame)
    }

    /// Release this exact permit token.
    ///
    /// Returns `false` when the permit already expired or was released.
    pub async fn release<E: RedisExecutor>(&self, executor: &mut E) -> Result<bool, RedisError> {
        let frame = self
            .release_script
            .execute(executor, &[self.key.as_str()], &[self.token.as_str()])
            .await?;
        bool::from_frame(frame)
    }
}

impl fmt::Debug for SemaphorePermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SemaphorePermit")
            .field("key", &self.key)
            .field("token", &"[REDACTED]")
            .field("ttl", &self.ttl)
            .field("remaining_at_acquire", &self.remaining_at_acquire)
            .finish()
    }
}

fn random_token() -> String {
    format!("{:032x}", rand::random::<u128>())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_requires_key_limit_and_ttl() {
        assert_eq!(
            ExpirableSemaphore::new("", 1, Duration::from_secs(1)).unwrap_err(),
            ConfigurationError::EmptyKey { parameter: "key" }
        );
        assert_eq!(
            ExpirableSemaphore::new("semaphore", 0, Duration::from_secs(1)).unwrap_err(),
            ConfigurationError::ZeroPermitLimit
        );
        assert_eq!(
            ExpirableSemaphore::new("semaphore", 1, Duration::ZERO).unwrap_err(),
            ConfigurationError::ZeroDuration { parameter: "ttl" }
        );
    }

    #[test]
    fn public_scripts_use_one_sorted_set_key_and_redis_time() {
        assert!(ACQUIRE_PERMIT_SCRIPT.contains("redis.call('TIME')"));
        assert!(ACQUIRE_PERMIT_SCRIPT.contains("'ZADD', KEYS[1], 'NX'"));
        assert!(RENEW_PERMIT_SCRIPT.contains("'ZADD', KEYS[1], 'XX'"));
        assert!(RELEASE_PERMIT_SCRIPT.contains("'ZREM', KEYS[1]"));
        assert!(!ACQUIRE_PERMIT_SCRIPT.contains("KEYS[2]"));
    }
}
