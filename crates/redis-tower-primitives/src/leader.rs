//! TTL-bounded leader election with an owned renewal lifecycle.
//!
//! [`LeaderElection::campaign`] is the only operation that starts a background
//! task. A successful campaign returns a [`Campaign`] containing the owned
//! [`Leadership`] handle and a separate [`LeadershipEvents`] receiver. This
//! lets another task observe demotion even when the leadership handle is
//! dropped.
//!
//! # Cluster keys
//!
//! Every operation touches one Redis key and is cluster-safe without a hash
//! tag. Use an explicit tag when the election key must share a slot with
//! related data.
//!
//! # Failure mode
//!
//! Redis is not a consensus system. A process pause or partition can outlive
//! the TTL, and failover can lose a successful campaign or renewal. Treat
//! [`LeadershipEvent::RenewalFailed`] as immediate loss of authority. Dropping
//! a handle requests compare-and-delete abdication in its owned task, but that
//! cleanup is best effort; the required TTL is the final bound if the runtime
//! or Redis is unavailable.

use std::fmt;
use std::time::Duration;

use redis_tower::{RedisError, RedisExecutor, Script};
use redis_tower_core::FromFrame;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::error::{ConfigurationError, duration_millis, require_key};

/// Atomic leader-campaign Lua source.
///
/// The script is public for auditing and preloading. Its lines perform the
/// following operations:
///
/// 1. Set the random owner token in `KEYS[1]` only when the key is absent,
///    applying the required millisecond TTL from `ARGV[2]`.
/// 2. Begin the successful-campaign branch when Redis returned `OK`.
/// 3. Return `1` to identify the caller as elected.
/// 4. End the successful-campaign branch.
/// 5. Return `0` when another leader still owns the key.
pub const CAMPAIGN_SCRIPT: &str = r#"local elected = redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2])
if elected then
  return 1
end
return 0"#;

/// Compare-and-renew leadership Lua source.
///
/// Its lines perform the following operations:
///
/// 1. Compare the value in `KEYS[1]` with the owner token in `ARGV[1]`.
/// 2. Apply the required millisecond TTL from `ARGV[2]` and return `1` only
///    for the current leader.
/// 3. End the owner branch.
/// 4. Return `0` after expiration or replacement by another leader.
pub const RENEW_LEADERSHIP_SCRIPT: &str = r#"if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0"#;

/// Compare-and-delete leadership-abdication Lua source.
///
/// Its lines perform the following operations:
///
/// 1. Compare the value in `KEYS[1]` with the owner token in `ARGV[1]`.
/// 2. Delete the election key and return `1` only for the current leader.
/// 3. End the owner branch.
/// 4. Return `0` after expiration or replacement by another leader.
pub const ABDICATE_SCRIPT: &str = r#"if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0"#;

/// Configuration for one TTL-bounded leader election.
#[derive(Clone)]
pub struct LeaderElection {
    key: String,
    ttl: Duration,
    ttl_millis: u64,
    renewal_interval: Duration,
    campaign_script: Script,
    renew_script: Script,
    abdicate_script: Script,
}

impl LeaderElection {
    /// Create an election with explicit TTL and renewal interval.
    ///
    /// The renewal interval must be positive and shorter than the TTL. No task
    /// is started until [`campaign`](Self::campaign) successfully acquires the
    /// election key.
    pub fn new(
        key: impl Into<String>,
        ttl: Duration,
        renewal_interval: Duration,
    ) -> Result<Self, ConfigurationError> {
        let key = require_key(key, "key")?;
        let ttl_millis = duration_millis(ttl, "ttl")?;
        if renewal_interval.is_zero() {
            return Err(ConfigurationError::ZeroDuration {
                parameter: "renewal interval",
            });
        }
        if renewal_interval >= ttl {
            return Err(ConfigurationError::LeadershipRenewalIntervalNotShorter {
                interval: renewal_interval,
                ttl,
            });
        }

        Ok(Self {
            key,
            ttl,
            ttl_millis,
            renewal_interval,
            campaign_script: Script::new(CAMPAIGN_SCRIPT),
            renew_script: Script::new(RENEW_LEADERSHIP_SCRIPT),
            abdicate_script: Script::new(ABDICATE_SCRIPT),
        })
    }

    /// Return the single Redis election key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Return the leadership TTL.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Return the explicit renewal interval.
    pub fn renewal_interval(&self) -> Duration {
        self.renewal_interval
    }

    /// Attempt to become leader and start owned renewal on success.
    ///
    /// `Ok(None)` means another leader currently owns the election key. A
    /// successful result queues [`LeadershipEvent::Elected`] before returning.
    /// The task consumes `executor`; pass a cheap client clone or a dedicated
    /// connection.
    ///
    /// A connection error is indeterminate: Redis may have committed the
    /// campaign before the response was lost. The caller must not assume it is
    /// leader without receiving a successful result.
    ///
    /// # Panics
    ///
    /// Panics if a successful campaign is executed outside a Tokio runtime.
    pub async fn campaign<E>(&self, mut executor: E) -> Result<Option<Campaign>, RedisError>
    where
        E: RedisExecutor + Send + 'static,
    {
        let owner_token = random_token();
        let ttl = self.ttl_millis.to_string();
        let frame = self
            .campaign_script
            .execute(
                &mut executor,
                &[self.key.as_str()],
                &[owner_token.as_str(), ttl.as_str()],
            )
            .await?;
        if !bool::from_frame(frame)? {
            return Ok(None);
        }

        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let key = self.key.clone();
        let task_owner_token = owner_token.clone();
        let ttl_millis = self.ttl_millis;
        let interval = self.renewal_interval;
        let renew_script = self.renew_script.clone();
        let abdicate_script = self.abdicate_script.clone();
        let (event_sender, event_receiver) = unbounded_channel();
        let _ = event_sender.send(LeadershipEvent::Elected);

        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = task_cancellation.cancelled() => {
                        return finish_abdication(
                            &mut executor,
                            &abdicate_script,
                            key.as_str(),
                            task_owner_token.as_str(),
                            &event_sender,
                        ).await;
                    }
                    () = tokio::time::sleep(interval) => {}
                }

                match renew(
                    &mut executor,
                    &renew_script,
                    key.as_str(),
                    task_owner_token.as_str(),
                    ttl_millis,
                )
                .await
                {
                    Ok(true) => {}
                    Ok(false) => {
                        let _ = event_sender.send(LeadershipEvent::Demoted);
                        return LeadershipOutcome::Demoted;
                    }
                    Err(error) => {
                        let _ = event_sender.send(LeadershipEvent::RenewalFailed {
                            error: error.to_string(),
                        });
                        return LeadershipOutcome::RedisError(error);
                    }
                }
            }
        });

        Ok(Some(Campaign {
            leadership: Leadership {
                key: self.key.clone(),
                ttl: self.ttl,
                cancellation,
                task: Some(task),
            },
            events: LeadershipEvents {
                receiver: event_receiver,
            },
        }))
    }
}

impl fmt::Debug for LeaderElection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LeaderElection")
            .field("key", &self.key)
            .field("ttl", &self.ttl)
            .field("renewal_interval", &self.renewal_interval)
            .finish()
    }
}

/// A successful campaign containing leadership and its observable events.
pub struct Campaign {
    leadership: Leadership,
    events: LeadershipEvents,
}

impl Campaign {
    /// Borrow the owned leadership handle.
    pub fn leadership(&self) -> &Leadership {
        &self.leadership
    }

    /// Mutably borrow the campaign's event receiver.
    pub fn events(&mut self) -> &mut LeadershipEvents {
        &mut self.events
    }

    /// Split the campaign so leadership and event observation can be owned by
    /// separate tasks.
    pub fn into_parts(self) -> (Leadership, LeadershipEvents) {
        (self.leadership, self.events)
    }
}

impl fmt::Debug for Campaign {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Campaign")
            .field("leadership", &self.leadership)
            .finish_non_exhaustive()
    }
}

/// Event emitted by an owned leadership lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeadershipEvent {
    /// The campaign acquired the election key and renewal task.
    Elected,
    /// Redis could not process a renewal or best-effort drop abdication.
    RenewalFailed {
        /// Display form of the Redis error; the structured error remains in
        /// [`LeadershipOutcome::RedisError`].
        error: String,
    },
    /// The lease was replaced, expired, or successfully abdicated.
    Demoted,
}

/// Receiver for leadership lifecycle events.
pub struct LeadershipEvents {
    receiver: UnboundedReceiver<LeadershipEvent>,
}

impl LeadershipEvents {
    /// Wait for the next event, returning `None` after the lifecycle task ends
    /// and all queued events have been consumed.
    pub async fn recv(&mut self) -> Option<LeadershipEvent> {
        self.receiver.recv().await
    }
}

impl fmt::Debug for LeadershipEvents {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("LeadershipEvents").finish()
    }
}

/// Terminal result from an owned leadership task.
#[derive(Debug)]
pub enum LeadershipOutcome {
    /// Compare-and-delete abdication removed the election key.
    Abdicated,
    /// Leadership had already expired or been replaced.
    Demoted,
    /// Redis could not process a renewal or abdication.
    RedisError(RedisError),
}

/// Owned handle for one elected leader and its renewal task.
///
/// Dropping the handle requests asynchronous compare-and-delete abdication and
/// detaches the task so it can complete. Use [`abdicate`](Self::abdicate) when
/// the caller must await the result. If cleanup cannot run, the required TTL
/// still bounds the election key.
#[must_use = "dropping leadership requests best-effort abdication"]
pub struct Leadership {
    key: String,
    ttl: Duration,
    cancellation: CancellationToken,
    task: Option<JoinHandle<LeadershipOutcome>>,
}

impl Leadership {
    /// Return the Redis election key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Return the leadership TTL.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Return whether the renewal task has already terminated.
    pub fn is_finished(&self) -> bool {
        self.task.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Request compare-and-delete abdication and wait for its terminal result.
    pub async fn abdicate(mut self) -> Result<LeadershipOutcome, JoinError> {
        self.cancellation.cancel();
        self.finish().await
    }

    /// Wait until renewal fails or the lease is demoted.
    pub async fn wait(mut self) -> Result<LeadershipOutcome, JoinError> {
        self.finish().await
    }

    async fn finish(&mut self) -> Result<LeadershipOutcome, JoinError> {
        self.task.take().expect("leadership task is present").await
    }
}

impl fmt::Debug for Leadership {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Leadership")
            .field("key", &self.key)
            .field("ttl", &self.ttl)
            .field("is_finished", &self.is_finished())
            .finish()
    }
}

impl Drop for Leadership {
    fn drop(&mut self) {
        self.cancellation.cancel();
        // Dropping a Tokio JoinHandle detaches rather than aborts the task. The
        // task retains its executor long enough to attempt compare-and-delete.
        let _ = self.task.take();
    }
}

async fn renew<E: RedisExecutor>(
    executor: &mut E,
    script: &Script,
    key: &str,
    owner_token: &str,
    ttl_millis: u64,
) -> Result<bool, RedisError> {
    let ttl = ttl_millis.to_string();
    let frame = script
        .execute(executor, &[key], &[owner_token, ttl.as_str()])
        .await?;
    bool::from_frame(frame)
}

async fn finish_abdication<E: RedisExecutor>(
    executor: &mut E,
    script: &Script,
    key: &str,
    owner_token: &str,
    event_sender: &UnboundedSender<LeadershipEvent>,
) -> LeadershipOutcome {
    match script.execute(executor, &[key], &[owner_token]).await {
        Ok(frame) => match bool::from_frame(frame) {
            Ok(true) => {
                let _ = event_sender.send(LeadershipEvent::Demoted);
                LeadershipOutcome::Abdicated
            }
            Ok(false) => {
                let _ = event_sender.send(LeadershipEvent::Demoted);
                LeadershipOutcome::Demoted
            }
            Err(error) => {
                let _ = event_sender.send(LeadershipEvent::RenewalFailed {
                    error: error.to_string(),
                });
                LeadershipOutcome::RedisError(error)
            }
        },
        Err(error) => {
            let _ = event_sender.send(LeadershipEvent::RenewalFailed {
                error: error.to_string(),
            });
            LeadershipOutcome::RedisError(error)
        }
    }
}

fn random_token() -> String {
    format!("{:032x}", rand::random::<u128>())
}

#[cfg(test)]
mod tests {
    use std::future::Future;

    use redis_tower::Command;
    use redis_tower_core::Frame;

    use super::*;

    #[test]
    fn configuration_requires_key_ttl_and_shorter_interval() {
        assert_eq!(
            LeaderElection::new("", Duration::from_secs(1), Duration::from_millis(100))
                .unwrap_err(),
            ConfigurationError::EmptyKey { parameter: "key" }
        );
        assert_eq!(
            LeaderElection::new("leader", Duration::ZERO, Duration::from_millis(1)).unwrap_err(),
            ConfigurationError::ZeroDuration { parameter: "ttl" }
        );
        assert_eq!(
            LeaderElection::new("leader", Duration::from_secs(1), Duration::from_secs(1))
                .unwrap_err(),
            ConfigurationError::LeadershipRenewalIntervalNotShorter {
                interval: Duration::from_secs(1),
                ttl: Duration::from_secs(1),
            }
        );
    }

    #[test]
    fn public_scripts_are_owner_checked() {
        assert!(CAMPAIGN_SCRIPT.contains("'NX', 'PX'"));
        assert!(RENEW_LEADERSHIP_SCRIPT.contains("'PEXPIRE', KEYS[1]"));
        assert!(ABDICATE_SCRIPT.contains("'DEL', KEYS[1]"));
    }

    struct RenewalFailureExecutor {
        calls: usize,
    }

    impl RedisExecutor for RenewalFailureExecutor {
        fn execute<Cmd: Command>(
            &mut self,
            cmd: Cmd,
        ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
            self.calls += 1;
            let result = if self.calls == 1 {
                Ok(Frame::Integer(1))
            } else {
                Err(RedisError::ConnectionClosed)
            };
            async move {
                match result {
                    Ok(frame) => cmd.parse_response(frame),
                    Err(error) => Err(error),
                }
            }
        }
    }

    #[tokio::test]
    async fn renewal_failure_is_observable() {
        let election = LeaderElection::new(
            "leader",
            Duration::from_millis(20),
            Duration::from_millis(1),
        )
        .unwrap();
        let campaign = election
            .campaign(RenewalFailureExecutor { calls: 0 })
            .await
            .unwrap()
            .unwrap();
        let (leadership, mut events) = campaign.into_parts();
        assert_eq!(events.recv().await, Some(LeadershipEvent::Elected));
        assert!(matches!(
            events.recv().await,
            Some(LeadershipEvent::RenewalFailed { .. })
        ));
        assert!(matches!(
            leadership.wait().await.unwrap(),
            LeadershipOutcome::RedisError(RedisError::ConnectionClosed)
        ));
    }
}
