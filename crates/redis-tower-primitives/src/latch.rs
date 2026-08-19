//! TTL-bounded distributed countdown latch.
//!
//! A latch is initialized once with a positive count, decremented atomically,
//! and observed through an explicit polling wait. The caller supplies both the
//! poll interval and timeout; no hidden task or default timing policy exists.
//!
//! # Cluster keys
//!
//! Every operation touches one Redis key and is cluster-safe without a hash
//! tag.
//!
//! # Failure mode
//!
//! The required TTL prevents abandoned latches from leaking forever. Expiry is
//! distinct from successful release, so waiters receive
//! [`LatchWaitOutcome::Expired`] rather than treating a missing key as count
//! zero. Connection failures are indeterminate because Redis may have applied
//! a countdown before its response was lost.

use std::fmt;
use std::time::Duration;

use redis_tower::{RedisError, RedisExecutor, Script};
use redis_tower_core::FromFrame;
use tokio::time::Instant;

use crate::error::{ConfigurationError, duration_millis, require_key};

/// Initialize-if-absent latch Lua source.
///
/// The script is public for auditing and preloading. Its lines perform the
/// following operations:
///
/// 1. Set the required initial count from `ARGV[1]` only when `KEYS[1]` is
///    absent, applying the required millisecond TTL from `ARGV[2]`.
/// 2. Begin the initialized branch when Redis returned `OK`.
/// 3. Return `1` for the caller that created the latch.
/// 4. End the initialized branch.
/// 5. Return `0` when the latch already exists.
pub const INITIALIZE_LATCH_SCRIPT: &str = r#"local initialized = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
if initialized then
  return 1
end
return 0"#;

/// Atomic latch countdown Lua source.
///
/// Its lines perform the following operations:
///
/// 1. Read the current count from `KEYS[1]`.
/// 2. Begin the expired/uninitialized branch when the key is absent.
/// 3. Return status `-1` and count zero for a missing latch.
/// 4. End the missing-latch branch.
/// 5. Parse the stored count.
/// 6. Begin the already-released branch when the count is not positive.
/// 7. Return status `0` and count zero without decrementing below zero.
/// 8. End the already-released branch.
/// 9. Atomically decrement the count with `DECR`.
/// 10. Return status `1` and the remaining count.
pub const COUNT_DOWN_SCRIPT: &str = r#"local current = redis.call('GET', KEYS[1])
if not current then
  return {-1, 0}
end
local count = tonumber(current)
if count <= 0 then
  return {0, 0}
end
local remaining = redis.call('DECR', KEYS[1])
return {1, remaining}"#;

/// Read latch state Lua source.
///
/// Its lines perform the following operations:
///
/// 1. Read the current count from `KEYS[1]`.
/// 2. Begin the expired/uninitialized branch when the key is absent.
/// 3. Return status `-1` and count zero for a missing latch.
/// 4. End the missing-latch branch.
/// 5. Return status `0` and the current non-negative count.
pub const READ_LATCH_SCRIPT: &str = r#"local current = redis.call('GET', KEYS[1])
if not current then
  return {-1, 0}
end
return {0, tonumber(current)}"#;

/// Configuration for one TTL-bounded countdown latch.
#[derive(Clone)]
pub struct CountDownLatch {
    key: String,
    initial_count: u64,
    ttl: Duration,
    ttl_millis: u64,
    initialize_script: Script,
    count_down_script: Script,
    read_script: Script,
}

impl CountDownLatch {
    /// Create a latch with an explicit positive count and required TTL.
    pub fn new(
        key: impl Into<String>,
        initial_count: u64,
        ttl: Duration,
    ) -> Result<Self, ConfigurationError> {
        let key = require_key(key, "key")?;
        if initial_count == 0 {
            return Err(ConfigurationError::ZeroInitialCount);
        }
        if initial_count > i64::MAX as u64 {
            return Err(ConfigurationError::InitialCountTooLarge);
        }
        let ttl_millis = duration_millis(ttl, "ttl")?;

        Ok(Self {
            key,
            initial_count,
            ttl,
            ttl_millis,
            initialize_script: Script::new(INITIALIZE_LATCH_SCRIPT),
            count_down_script: Script::new(COUNT_DOWN_SCRIPT),
            read_script: Script::new(READ_LATCH_SCRIPT),
        })
    }

    /// Return the single Redis latch key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Return the count used by successful initialization.
    pub fn initial_count(&self) -> u64 {
        self.initial_count
    }

    /// Return the absolute latch TTL.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Initialize the latch only when its key is absent.
    ///
    /// Returns `false` when a latch with this key already exists. Initialization
    /// never resets an active latch or extends its TTL.
    pub async fn initialize<E: RedisExecutor>(&self, executor: &mut E) -> Result<bool, RedisError> {
        let count = self.initial_count.to_string();
        let ttl = self.ttl_millis.to_string();
        let frame = self
            .initialize_script
            .execute(
                executor,
                &[self.key.as_str()],
                &[count.as_str(), ttl.as_str()],
            )
            .await?;
        bool::from_frame(frame)
    }

    /// Decrement the latch once without allowing the count below zero.
    pub async fn count_down<E: RedisExecutor>(
        &self,
        executor: &mut E,
    ) -> Result<LatchCountDown, RedisError> {
        let frame = self
            .count_down_script
            .execute(executor, &[self.key.as_str()], &[])
            .await?;
        parse_count_down(frame)
    }

    /// Read the current count, returning `None` for an uninitialized or expired
    /// latch.
    pub async fn current<E: RedisExecutor>(
        &self,
        executor: &mut E,
    ) -> Result<Option<u64>, RedisError> {
        let frame = self
            .read_script
            .execute(executor, &[self.key.as_str()], &[])
            .await?;
        parse_current(frame)
    }

    /// Poll until the latch releases, expires, or reaches the caller's timeout.
    ///
    /// Both durations are required and must be positive. This method owns no
    /// background task: canceling its future stops polling immediately. The
    /// timeout bounds polling sleeps, but does not cancel an in-flight Redis
    /// request; an unresponsive executor can therefore delay the return.
    pub async fn wait<E: RedisExecutor>(
        &self,
        executor: &mut E,
        poll_interval: Duration,
        timeout: Duration,
    ) -> Result<LatchWaitOutcome, LatchWaitError> {
        duration_millis(poll_interval, "poll interval")?;
        duration_millis(timeout, "timeout")?;
        let started = Instant::now();
        let deadline =
            started
                .checked_add(timeout)
                .ok_or(ConfigurationError::DurationTooLarge {
                    parameter: "timeout",
                })?;

        loop {
            match self.current(executor).await? {
                None => return Ok(LatchWaitOutcome::Expired),
                Some(0) => return Ok(LatchWaitOutcome::Released),
                Some(remaining) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Ok(LatchWaitOutcome::TimedOut { remaining });
                    }
                    tokio::time::sleep(poll_interval.min(deadline - now)).await;
                }
            }
        }
    }
}

impl fmt::Debug for CountDownLatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CountDownLatch")
            .field("key", &self.key)
            .field("initial_count", &self.initial_count)
            .field("ttl", &self.ttl)
            .finish()
    }
}

/// Result of one atomic latch countdown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LatchCountDown {
    /// The latch was not initialized or its TTL expired.
    Missing,
    /// The latch remains closed with this positive count.
    Waiting {
        /// Count remaining after the decrement.
        remaining: u64,
    },
    /// The decrement released the latch, or it was already at zero.
    Released,
}

/// Terminal result of an explicit latch wait.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LatchWaitOutcome {
    /// The observed count reached zero.
    Released,
    /// The key disappeared before release, normally because its TTL expired.
    Expired,
    /// The caller's timeout elapsed while the latch remained closed.
    TimedOut {
        /// Last positive count observed before returning.
        remaining: u64,
    },
}

/// Error returned while validating or executing a latch wait.
#[derive(Debug, thiserror::Error)]
pub enum LatchWaitError {
    /// The poll interval or timeout was invalid.
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    /// Redis could not read the latch state.
    #[error(transparent)]
    Redis(#[from] RedisError),
}

fn parse_count_down(frame: redis_tower::Frame) -> Result<LatchCountDown, RedisError> {
    let values = Vec::<i64>::from_frame(frame)?;
    let valid = match values.as_slice() {
        [-1 | 0, 0] => true,
        [1, remaining] => *remaining >= 0,
        _ => false,
    };
    if !valid {
        return Err(RedisError::UnexpectedResponse {
            expected: "latch array [-1, 0], [0, 0], or [1, non-negative remaining]",
            actual: format!("{values:?}"),
        });
    }
    match (values[0], values[1]) {
        (-1, _) => Ok(LatchCountDown::Missing),
        (_, 0) => Ok(LatchCountDown::Released),
        (_, remaining) => Ok(LatchCountDown::Waiting {
            remaining: remaining as u64,
        }),
    }
}

fn parse_current(frame: redis_tower::Frame) -> Result<Option<u64>, RedisError> {
    let values = Vec::<i64>::from_frame(frame)?;
    let valid = match values.as_slice() {
        [-1, 0] => true,
        [0, count] => *count >= 0,
        _ => false,
    };
    if !valid {
        return Err(RedisError::UnexpectedResponse {
            expected: "latch array [-1, 0] or [0, non-negative count]",
            actual: format!("{values:?}"),
        });
    }
    if values[0] == -1 {
        Ok(None)
    } else {
        Ok(Some(values[1] as u64))
    }
}

#[cfg(test)]
mod tests {
    use redis_tower::Frame;

    use super::*;

    #[test]
    fn configuration_requires_key_count_and_ttl() {
        assert_eq!(
            CountDownLatch::new("", 1, Duration::from_secs(1)).unwrap_err(),
            ConfigurationError::EmptyKey { parameter: "key" }
        );
        assert_eq!(
            CountDownLatch::new("latch", 0, Duration::from_secs(1)).unwrap_err(),
            ConfigurationError::ZeroInitialCount
        );
        assert_eq!(
            CountDownLatch::new("latch", i64::MAX as u64 + 1, Duration::from_secs(1)).unwrap_err(),
            ConfigurationError::InitialCountTooLarge
        );
    }

    #[test]
    fn parses_countdown_states() {
        assert_eq!(
            parse_count_down(Frame::Array(Some(vec![
                Frame::Integer(-1),
                Frame::Integer(0),
            ])))
            .unwrap(),
            LatchCountDown::Missing
        );
        assert_eq!(
            parse_count_down(Frame::Array(Some(vec![
                Frame::Integer(1),
                Frame::Integer(2),
            ])))
            .unwrap(),
            LatchCountDown::Waiting { remaining: 2 }
        );
        assert_eq!(
            parse_count_down(Frame::Array(Some(vec![
                Frame::Integer(1),
                Frame::Integer(0),
            ])))
            .unwrap(),
            LatchCountDown::Released
        );

        assert!(
            parse_count_down(Frame::Array(Some(vec![
                Frame::Integer(-1),
                Frame::Integer(2),
            ])))
            .is_err()
        );
        assert!(
            parse_current(Frame::Array(Some(vec![
                Frame::Integer(0),
                Frame::Integer(-1),
            ])))
            .is_err()
        );
    }

    #[test]
    fn public_scripts_initialize_decrement_and_read_one_key() {
        assert!(INITIALIZE_LATCH_SCRIPT.contains("'NX', 'PX'"));
        assert!(COUNT_DOWN_SCRIPT.contains("'DECR', KEYS[1]"));
        assert!(READ_LATCH_SCRIPT.contains("'GET', KEYS[1]"));
        assert!(!COUNT_DOWN_SCRIPT.contains("KEYS[2]"));
    }
}
