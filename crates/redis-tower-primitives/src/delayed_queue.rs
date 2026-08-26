//! Redis-time delayed queue with caller-owned polling.
//!
//! [`DelayedQueue::enqueue`] stores a binary payload in one sorted set using a
//! Redis-time delivery deadline. [`DelayedQueue::claim_due`] atomically removes
//! a bounded batch whose deadlines have arrived. There is no transfer thread,
//! hidden poll loop, acknowledgement phase, or automatic retry.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use std::time::Duration;
//! use redis_tower::MultiplexedClient;
//! use redis_tower_primitives::DelayedQueue;
//!
//! let mut client = MultiplexedClient::connect("127.0.0.1:6379").await?;
//! let queue = DelayedQueue::new("emails:delayed", Duration::from_secs(3600))?;
//! queue
//!     .enqueue(&mut client, b"welcome:user:42", Duration::from_secs(30))
//!     .await?;
//!
//! let batch = queue.claim_due(&mut client, 100).await?;
//! for payload in batch.payloads() {
//!     println!("claimed {} bytes", payload.len());
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Delivery and retention
//!
//! Claims are at-most-once: the claim script removes messages before returning
//! them. A lost response is therefore indeterminate and can lose a claimed
//! batch. The required retention window is measured after each message's due
//! time; a claim reports how many older messages it pruned. The queue key is
//! also bounded by the latest scheduled deadline plus retention.
//!
//! # Cluster keys
//!
//! Every operation touches one Redis key and is cluster-safe without a hash
//! tag. Every participant sharing a queue key must use the same retention.

use std::fmt;
use std::time::Duration;

use redis_tower::{Frame, RedisError, RedisExecutor, Script};
use redis_tower_core::FromFrame;

use crate::error::{ConfigurationError, duration_millis, duration_millis_allow_zero, require_key};

const MEMBER_TOKEN_HEX_LEN: usize = 32;
const MAX_SAFE_LUA_DURATION_MILLIS: u64 = 1_u64 << 52;

/// Redis-time delayed enqueue Lua source.
///
/// Payloads are hex encoded because the current public script helper accepts
/// textual arguments. The random 32-character prefix makes duplicate payloads
/// distinct sorted-set members. The script is public for auditing and
/// preloading. Its lines perform the following operations:
///
/// 1. Read Redis server time.
/// 2. Convert seconds and microseconds into one millisecond timestamp.
/// 3. Parse the caller-selected delay from `ARGV[1]`.
/// 4. Parse the required post-deadline retention from `ARGV[2]`.
/// 5. Calculate the delivery deadline score.
/// 6. Prefix the encoded payload in `ARGV[4]` with the random token in
///    `ARGV[3]`, preserving duplicate payloads.
/// 7. Add the unique message member at its deadline score.
/// 8. Calculate the key lifetime through deadline plus retention.
/// 9. Read the queue key's current remaining TTL.
/// 10. Begin the branch that installs or extends the key bound.
/// 11. Apply the longer millisecond TTL.
/// 12. End the TTL branch.
/// 13. Return the absolute Redis-time deadline in milliseconds.
pub const ENQUEUE_DELAYED_SCRIPT: &str = r#"local server_time = redis.call('TIME')
local now = (tonumber(server_time[1]) * 1000) + math.floor(tonumber(server_time[2]) / 1000)
local delay = tonumber(ARGV[1])
local retention = tonumber(ARGV[2])
local deadline = now + delay
local member = ARGV[3] .. ARGV[4]
redis.call('ZADD', KEYS[1], deadline, member)
local lifetime = delay + retention
local current_ttl = redis.call('PTTL', KEYS[1])
if current_ttl < lifetime then
  redis.call('PEXPIRE', KEYS[1], lifetime)
end
return deadline"#;

/// Atomic due-message claim Lua source.
///
/// Its lines perform the following operations:
///
/// 1. Read Redis server time.
/// 2. Convert seconds and microseconds into one millisecond timestamp.
/// 3. Parse the required retention from `ARGV[1]`.
/// 4. Parse the caller's maximum batch size from `ARGV[2]`.
/// 5. Remove and count messages whose deadline plus retention elapsed.
/// 6. Read at most the requested number of due members in score order.
/// 7. Begin the branch for a non-empty due batch.
/// 8. Remove exactly the selected leading members atomically.
/// 9. End the due-batch branch.
/// 10. Create the returned payload array.
/// 11. Iterate over each claimed unique member.
/// 12. Strip its fixed 32-character random prefix.
/// 13. End payload extraction.
/// 14. Return the expired count and encoded claimed payloads.
pub const CLAIM_DUE_SCRIPT: &str = r#"local server_time = redis.call('TIME')
local now = (tonumber(server_time[1]) * 1000) + math.floor(tonumber(server_time[2]) / 1000)
local retention = tonumber(ARGV[1])
local maximum = tonumber(ARGV[2])
local expired = redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - retention)
local members = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, maximum)
if #members > 0 then
  redis.call('ZREMRANGEBYRANK', KEYS[1], 0, #members - 1)
end
local payloads = {}
for index, member in ipairs(members) do
  payloads[index] = string.sub(member, 33)
end
return {expired, payloads}"#;

/// Configuration for one retained delayed queue.
#[derive(Clone)]
pub struct DelayedQueue {
    key: String,
    retention: Duration,
    retention_millis: u64,
    enqueue_script: Script,
    claim_script: Script,
}

impl DelayedQueue {
    /// Create a queue with an explicit post-deadline retention window.
    ///
    /// The retention is required and positive. It bounds how late a message
    /// may be claimed after becoming due.
    pub fn new(key: impl Into<String>, retention: Duration) -> Result<Self, ConfigurationError> {
        let key = require_key(key, "key")?;
        let retention_millis = duration_millis(retention, "retention")?;
        Ok(Self {
            key,
            retention,
            retention_millis,
            enqueue_script: Script::new(ENQUEUE_DELAYED_SCRIPT),
            claim_script: Script::new(CLAIM_DUE_SCRIPT),
        })
    }

    /// Return the queue's single Redis sorted-set key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Return the required post-deadline retention window.
    pub fn retention(&self) -> Duration {
        self.retention
    }

    /// Enqueue one binary payload after a caller-selected delay.
    ///
    /// A zero delay makes the message immediately claimable. The returned
    /// value is the Redis-time Unix deadline in milliseconds. Duplicate and
    /// empty payloads are preserved as separate messages.
    ///
    /// A lost response is indeterminate: Redis may already contain the
    /// message, so blindly retrying can enqueue a duplicate.
    pub async fn enqueue<E: RedisExecutor>(
        &self,
        executor: &mut E,
        payload: impl AsRef<[u8]>,
        delay: Duration,
    ) -> Result<u64, DelayedQueueError> {
        let delay_millis = duration_millis_allow_zero(delay, "delay")?;
        if delay_millis > MAX_SAFE_LUA_DURATION_MILLIS - self.retention_millis {
            return Err(ConfigurationError::DurationTooLarge {
                parameter: "delay plus retention",
            }
            .into());
        }

        let delay = delay_millis.to_string();
        let retention = self.retention_millis.to_string();
        let token = random_token();
        let payload = hex::encode(payload);
        let frame = self
            .enqueue_script
            .execute(
                executor,
                &[self.key.as_str()],
                &[
                    delay.as_str(),
                    retention.as_str(),
                    token.as_str(),
                    payload.as_str(),
                ],
            )
            .await?;
        Ok(u64::from_frame(frame)?)
    }

    /// Atomically claim up to `maximum` due messages.
    ///
    /// The caller owns the poll loop and chooses every batch size. The result
    /// reports messages pruned after their retention window as well as the
    /// binary payloads removed for this at-most-once claim.
    ///
    /// A lost response is indeterminate: the selected messages may have been
    /// removed even though the caller did not receive them.
    pub async fn claim_due<E: RedisExecutor>(
        &self,
        executor: &mut E,
        maximum: u32,
    ) -> Result<ClaimBatch, DelayedQueueError> {
        if maximum == 0 {
            return Err(ConfigurationError::ZeroClaimLimit.into());
        }
        let retention = self.retention_millis.to_string();
        let maximum = maximum.to_string();
        let frame = self
            .claim_script
            .execute(
                executor,
                &[self.key.as_str()],
                &[retention.as_str(), maximum.as_str()],
            )
            .await?;
        ClaimBatch::from_frame(frame)
    }
}

impl fmt::Debug for DelayedQueue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DelayedQueue")
            .field("key", &self.key)
            .field("retention", &self.retention)
            .finish()
    }
}

/// One atomic due-message claim result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimBatch {
    expired: u64,
    payloads: Vec<Vec<u8>>,
}

impl ClaimBatch {
    /// Return the number of messages pruned because retention elapsed.
    pub fn expired(&self) -> u64 {
        self.expired
    }

    /// Borrow the binary payloads claimed in deadline order.
    pub fn payloads(&self) -> &[Vec<u8>] {
        &self.payloads
    }

    /// Consume the result and return its binary payloads.
    pub fn into_payloads(self) -> Vec<Vec<u8>> {
        self.payloads
    }

    fn from_frame(frame: Frame) -> Result<Self, DelayedQueueError> {
        let outer = Vec::<Frame>::from_frame(frame)?;
        let actual = outer.len();
        let pair: Result<[Frame; 2], _> = outer.try_into();
        let Ok([expired_frame, payload_frame]) = pair else {
            return Err(RedisError::UnexpectedResponse {
                expected: "delayed-queue array [expired, payload array]",
                actual: format!("array of {actual} elements"),
            }
            .into());
        };
        let expired = u64::from_frame(expired_frame)?;
        let encoded = Vec::<String>::from_frame(payload_frame)?;
        let payloads = encoded
            .into_iter()
            .map(|payload| {
                hex::decode(&payload).map_err(|error| DelayedQueueError::InvalidStoredPayload {
                    value: payload,
                    error,
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { expired, payloads })
    }
}

/// Error returned while validating or executing a delayed-queue operation.
#[derive(Debug, thiserror::Error)]
pub enum DelayedQueueError {
    /// Queue timing or batch configuration was invalid.
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    /// Redis could not execute or decode the queue script.
    #[error(transparent)]
    Redis(#[from] RedisError),
    /// A queue member did not contain the expected encoded payload.
    #[error("queue contains an invalid encoded payload {value:?}: {error}")]
    InvalidStoredPayload {
        /// Invalid encoded value returned by the claim script.
        value: String,
        /// Hex-decoding failure.
        #[source]
        error: hex::FromHexError,
    },
}

fn random_token() -> String {
    let token = format!("{:032x}", rand::random::<u128>());
    debug_assert_eq!(token.len(), MEMBER_TOKEN_HEX_LEN);
    token
}

#[cfg(test)]
mod tests {
    use redis_tower::Command;

    use super::*;

    struct NeverExecutor;

    impl RedisExecutor for NeverExecutor {
        async fn execute<Cmd: Command>(&mut self, _cmd: Cmd) -> Result<Cmd::Response, RedisError> {
            panic!("invalid queue input must not contact Redis")
        }
    }

    #[test]
    fn configuration_requires_key_and_retention() {
        assert_eq!(
            DelayedQueue::new("", Duration::from_secs(1)).unwrap_err(),
            ConfigurationError::EmptyKey { parameter: "key" }
        );
        assert_eq!(
            DelayedQueue::new("queue", Duration::ZERO).unwrap_err(),
            ConfigurationError::ZeroDuration {
                parameter: "retention"
            }
        );
    }

    #[test]
    fn parses_binary_claim_batch_and_rejects_bad_encoding() {
        let batch = ClaimBatch::from_frame(Frame::Array(Some(vec![
            Frame::Integer(2),
            Frame::Array(Some(vec![Frame::BulkString(Some("00ff".into()))])),
        ])))
        .unwrap();
        assert_eq!(batch.expired(), 2);
        assert_eq!(batch.payloads(), &[vec![0, 255]]);

        assert!(matches!(
            ClaimBatch::from_frame(Frame::Array(Some(vec![
                Frame::Integer(0),
                Frame::Array(Some(vec![Frame::BulkString(Some("xyz".into()))])),
            ]))),
            Err(DelayedQueueError::InvalidStoredPayload { .. })
        ));
    }

    #[test]
    fn public_scripts_use_one_sorted_set_and_redis_time() {
        assert!(ENQUEUE_DELAYED_SCRIPT.contains("redis.call('TIME')"));
        assert!(ENQUEUE_DELAYED_SCRIPT.contains("'ZADD', KEYS[1]"));
        assert!(CLAIM_DUE_SCRIPT.contains("'ZREMRANGEBYRANK', KEYS[1]"));
        assert!(!CLAIM_DUE_SCRIPT.contains("KEYS[2]"));
    }

    #[tokio::test]
    async fn operation_configuration_fails_before_redis() {
        let queue = DelayedQueue::new("queue", Duration::from_millis(1)).unwrap();
        let mut executor = NeverExecutor;
        assert!(matches!(
            queue.claim_due(&mut executor, 0).await,
            Err(DelayedQueueError::Configuration(
                ConfigurationError::ZeroClaimLimit
            ))
        ));
        assert!(matches!(
            queue
                .enqueue(
                    &mut executor,
                    b"payload",
                    Duration::from_millis(MAX_SAFE_LUA_DURATION_MILLIS),
                )
                .await,
            Err(DelayedQueueError::Configuration(
                ConfigurationError::DurationTooLarge {
                    parameter: "delay plus retention"
                }
            ))
        ));
    }
}
