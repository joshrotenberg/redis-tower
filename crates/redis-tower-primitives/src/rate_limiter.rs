//! Redis-time Generic Cell Rate Algorithm (GCRA) limiter.
//!
//! [`GcraRateLimiter`] stores virtual arrival times in one sorted-set key. A
//! Lua script reads Redis `TIME`, prunes virtual arrivals that no longer affect
//! admission, and atomically admits or rejects the request. The required quota
//! and window define both the sustained rate and the maximum initial burst.
//!
//! The distributed limiter protects a quota shared by many processes. Pair it
//! with `tower_resilience_ratelimiter::RateLimiterLayer` in backpressure mode
//! when each process also needs local admission pressure; local backpressure
//! and shared quota solve different problems.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::time::Duration;
//! use redis_tower::MultiplexedClient;
//! use redis_tower_primitives::GcraRateLimiter;
//!
//! let mut client = MultiplexedClient::connect("127.0.0.1:6379").await?;
//! let limiter = GcraRateLimiter::new(
//!     "tenant:42:rate",
//!     100,
//!     Duration::from_secs(1),
//! )?;
//!
//! let decision = limiter.check(&mut client).await?;
//! if !decision.is_allowed() {
//!     tokio::time::sleep(decision.retry_after()).await;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Cluster keys
//!
//! Each decision touches one Redis key and is cluster-safe without a hash tag.
//! If related application keys must share its slot, give them the same explicit
//! tag, for example `{tenant:7}:rate` and `{tenant:7}:work`.
//!
//! # Failure mode
//!
//! Redis errors are returned rather than interpreted as allow or deny. The
//! caller must choose fail-open or fail-closed behavior. A connection failure
//! is indeterminate because the script may have admitted the request before
//! its response was lost; blindly retrying can consume an additional cell.
//! Failover can also lose recently admitted cells when Redis durability permits
//! data loss.

use std::fmt;
use std::time::Duration;

use redis_tower::{RedisError, RedisExecutor, Script};
use redis_tower_core::FromFrame;

use crate::error::{ConfigurationError, duration_micros, require_key};

/// Atomic Redis-time GCRA Lua source.
///
/// The sorted set contains virtual arrival times as scores and random request
/// IDs as members. The script is public for auditing and preloading. Its lines
/// perform the following operations:
///
/// 1. Read Redis server time.
/// 2. Convert seconds and microseconds into one microsecond timestamp.
/// 3. Parse the required quota from `ARGV[1]`.
/// 4. Parse the required window in microseconds from `ARGV[2]`.
/// 5. Round the per-cell emission interval up to a whole microsecond.
/// 6. Derive the burst tolerance for `quota` immediately available cells.
/// 7. Prune virtual arrivals that can no longer affect the current TAT.
/// 8. Read the greatest remaining virtual arrival and its score.
/// 9. Initialize the theoretical arrival time (TAT) to server time.
/// 10. Begin the branch for existing limiter state.
/// 11. Advance TAT to the later of server time or the stored score.
/// 12. End the existing-state branch.
/// 13. Calculate when another request may be admitted.
/// 14. Begin the rejection branch when the request is too early.
/// 15. Calculate microseconds until a request can be retried.
/// 16. Calculate microseconds until all committed cells have drained.
/// 17. Return denied, zero remaining cells, retry delay, and reset delay.
/// 18. End the rejection branch.
/// 19. Advance TAT by one emission interval for the admitted request.
/// 20. Add the request ID from `ARGV[3]` at the new virtual-arrival score.
/// 21. Derive a positive millisecond TTL from the remaining virtual backlog.
/// 22. Bound the sorted-set lifetime to the time its state still matters.
/// 23. Calculate the admitted virtual backlog.
/// 24. Convert the backlog into the number of currently occupied cells.
/// 25. Calculate immediately available cells without going below zero.
/// 26. Return allowed, remaining cells, zero retry delay, and reset delay.
pub const GCRA_SCRIPT: &str = r#"local server_time = redis.call('TIME')
local now = (tonumber(server_time[1]) * 1000000) + tonumber(server_time[2])
local quota = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local interval = math.ceil(window / quota)
local tolerance = interval * (quota - 1)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local latest = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
local tat = now
if #latest == 2 then
  tat = math.max(now, tonumber(latest[2]))
end
local allowed_at = tat - tolerance
if now < allowed_at then
  local retry = allowed_at - now
  local reset = tat - now
  return {0, 0, retry, reset}
end
local new_tat = tat + interval
redis.call('ZADD', KEYS[1], new_tat, ARGV[3])
local ttl = math.max(1, math.ceil((new_tat - now) / 1000))
redis.call('PEXPIRE', KEYS[1], ttl)
local backlog = new_tat - now
local used = math.ceil(backlog / interval)
local remaining = math.max(0, quota - used)
return {1, remaining, 0, backlog}"#;

/// Distributed GCRA limiter with a required quota and window.
///
/// A configuration of `quota = 100` and `window = 1 second` sustains 100
/// requests per second and permits an initial burst of up to 100 requests.
#[derive(Clone)]
pub struct GcraRateLimiter {
    key: String,
    quota: u32,
    window: Duration,
    window_micros: u64,
    script: Script,
}

impl GcraRateLimiter {
    /// Create a distributed limiter with required quota and window parameters.
    ///
    /// Windows are rounded up to Redis's microsecond clock resolution. A quota
    /// finer than one cell per microsecond is rejected instead of silently
    /// changing the requested rate.
    pub fn new(
        key: impl Into<String>,
        quota: u32,
        window: Duration,
    ) -> Result<Self, ConfigurationError> {
        let key = require_key(key, "key")?;
        if quota == 0 {
            return Err(ConfigurationError::ZeroQuota);
        }
        let window_micros = duration_micros(window, "window")?;
        if u64::from(quota) > window_micros {
            return Err(ConfigurationError::QuotaExceedsWindowResolution {
                quota,
                window_micros,
            });
        }

        Ok(Self {
            key,
            quota,
            window,
            window_micros,
            script: Script::new(GCRA_SCRIPT),
        })
    }

    /// Return the single Redis sorted-set key holding shared limiter state.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Return the required request quota.
    pub fn quota(&self) -> u32 {
        self.quota
    }

    /// Return the required quota window.
    pub fn window(&self) -> Duration {
        self.window
    }

    /// Atomically check and consume one cell of shared quota.
    ///
    /// All timing comes from Redis `TIME`; the local clock is not consulted.
    /// The result includes the immediately available cell count plus retry and
    /// full-reset delays measured from the server timestamp.
    pub async fn check<E: RedisExecutor>(
        &self,
        executor: &mut E,
    ) -> Result<RateLimitDecision, RedisError> {
        let quota = self.quota.to_string();
        let window = self.window_micros.to_string();
        let request_id = random_request_id();
        let frame = self
            .script
            .execute(
                executor,
                &[self.key.as_str()],
                &[quota.as_str(), window.as_str(), request_id.as_str()],
            )
            .await?;
        RateLimitDecision::from_frame(frame)
    }
}

impl fmt::Debug for GcraRateLimiter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GcraRateLimiter")
            .field("key", &self.key)
            .field("quota", &self.quota)
            .field("window", &self.window)
            .finish()
    }
}

/// Result of one distributed rate-limit decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitDecision {
    allowed: bool,
    remaining: u32,
    retry_after: Duration,
    reset_after: Duration,
}

impl RateLimitDecision {
    /// Return whether this request consumed a cell and may proceed.
    pub fn is_allowed(&self) -> bool {
        self.allowed
    }

    /// Return the number of cells available immediately after this decision.
    pub fn remaining(&self) -> u32 {
        self.remaining
    }

    /// Return the server-computed delay until the next request may succeed.
    ///
    /// This is zero for an allowed request.
    pub fn retry_after(&self) -> Duration {
        self.retry_after
    }

    /// Return the server-computed delay until all committed cells drain.
    pub fn reset_after(&self) -> Duration {
        self.reset_after
    }

    fn from_frame(frame: redis_tower::Frame) -> Result<Self, RedisError> {
        let values = Vec::<u64>::from_frame(frame)?;
        if values.len() != 4 || values[0] > 1 || values[1] > u32::MAX as u64 {
            return Err(RedisError::UnexpectedResponse {
                expected: "GCRA array [allowed, remaining, retry_us, reset_us]",
                actual: format!("{values:?}"),
            });
        }

        Ok(Self {
            allowed: values[0] == 1,
            remaining: values[1] as u32,
            retry_after: Duration::from_micros(values[2]),
            reset_after: Duration::from_micros(values[3]),
        })
    }
}

fn random_request_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower::Frame;

    #[test]
    fn configuration_requires_key_quota_and_window() {
        assert_eq!(
            GcraRateLimiter::new("", 1, Duration::from_secs(1)).unwrap_err(),
            ConfigurationError::EmptyKey { parameter: "key" }
        );
        assert_eq!(
            GcraRateLimiter::new("rate", 0, Duration::from_secs(1)).unwrap_err(),
            ConfigurationError::ZeroQuota
        );
        assert_eq!(
            GcraRateLimiter::new("rate", 1, Duration::ZERO).unwrap_err(),
            ConfigurationError::ZeroDuration {
                parameter: "window"
            }
        );
        assert_eq!(
            GcraRateLimiter::new("rate", 2, Duration::from_micros(1)).unwrap_err(),
            ConfigurationError::QuotaExceedsWindowResolution {
                quota: 2,
                window_micros: 1
            }
        );
    }

    #[test]
    fn parses_script_decision() {
        let decision = RateLimitDecision::from_frame(Frame::Array(Some(vec![
            Frame::Integer(0),
            Frame::Integer(0),
            Frame::Integer(12_000),
            Frame::Integer(30_000),
        ])))
        .unwrap();
        assert!(!decision.is_allowed());
        assert_eq!(decision.remaining(), 0);
        assert_eq!(decision.retry_after(), Duration::from_millis(12));
        assert_eq!(decision.reset_after(), Duration::from_millis(30));
    }

    #[test]
    fn rejects_malformed_script_decision() {
        let error =
            RateLimitDecision::from_frame(Frame::Array(Some(vec![Frame::Integer(1)]))).unwrap_err();
        assert!(matches!(error, RedisError::UnexpectedResponse { .. }));
    }

    #[test]
    fn script_uses_server_time_and_one_sorted_set_key() {
        assert!(GCRA_SCRIPT.contains("redis.call('TIME')"));
        assert!(GCRA_SCRIPT.contains("'ZADD', KEYS[1]"));
        assert!(GCRA_SCRIPT.contains("'ZREMRANGEBYSCORE', KEYS[1]"));
        assert!(!GCRA_SCRIPT.contains("KEYS[2]"));
    }
}
