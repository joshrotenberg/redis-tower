//! Redis Smart Client Handoff maintenance notifications.
//!
//! Maintenance handling is deliberately opt-in and currently targets one
//! factory-backed [`MultiplexedClient`](crate::MultiplexedClient) connection.
//! The existing auto-pipeline worker remains the sole socket reader. Construct
//! the client with
//! [`MultiplexedClient::from_factory_with_maintenance`](crate::MultiplexedClient::from_factory_with_maintenance)
//! and retain the returned [`MaintenanceListenerHandle`] to keep it enabled.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use redis_tower_core::{Frame, ReceivedPushFrame, RedisConnection, RedisError};
use redis_tower_protocol::helpers::{array, bulk};
use tokio::sync::{mpsc, oneshot};

static NEXT_LISTENER_ID: AtomicU64 = AtomicU64::new(1);

/// The supported Redis maintenance-notification kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MaintenanceNotificationKind {
    /// Redis is preparing this connection for migration.
    ///
    /// This is observational in the current implementation; it does not
    /// trigger a reconnect.
    Migrating,
    /// Redis asked this connection to move before the supplied TTL expires.
    ///
    /// With endpoint type `none`, redis-tower reconnects through the original
    /// factory halfway through the TTL.
    Moving,
}

/// Owned capability that keeps maintenance-notification handling enabled.
///
/// Dropping the handle disables future handling and cancels a handoff whose
/// half-TTL boundary has not yet been reached. It does not shut down the
/// client, keep the client worker alive, or perform network I/O. Call
/// [`shutdown`](Self::shutdown) to wait until the worker acknowledges the
/// disabled state.
#[must_use = "dropping the handle immediately disables maintenance handling"]
pub struct MaintenanceListenerHandle {
    control: mpsc::UnboundedSender<MaintenanceControl>,
    listener_id: u64,
    armed: bool,
}

impl MaintenanceListenerHandle {
    pub(crate) fn new(
        control: mpsc::UnboundedSender<MaintenanceControl>,
        listener_id: u64,
    ) -> Self {
        Self {
            control,
            listener_id,
            armed: true,
        }
    }

    /// Disable maintenance handling and wait for worker acknowledgement.
    ///
    /// If a handoff already crossed its safe batch boundary, shutdown waits
    /// for that replacement attempt to reach a connected or terminal state.
    /// This method does not shut down the multiplexed client itself.
    pub async fn shutdown(mut self) {
        self.armed = false;
        let (ack_tx, ack_rx) = oneshot::channel();
        if self
            .control
            .send(MaintenanceControl::Disable {
                listener_id: self.listener_id,
                ack: Some(ack_tx),
            })
            .is_ok()
        {
            let _ = ack_rx.await;
        }
    }
}

impl Drop for MaintenanceListenerHandle {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.control.send(MaintenanceControl::Disable {
                listener_id: self.listener_id,
                ack: None,
            });
        }
    }
}

pub(crate) enum MaintenanceControl {
    Disable {
        listener_id: u64,
        ack: Option<oneshot::Sender<()>>,
    },
}

pub(crate) fn next_listener_id() -> u64 {
    NEXT_LISTENER_ID.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParsedMaintenanceNotification {
    Migrating { sequence: u64, ttl: Duration },
    Moving { sequence: u64, ttl: Duration },
}

impl ParsedMaintenanceNotification {
    pub(crate) fn kind(self) -> MaintenanceNotificationKind {
        match self {
            Self::Migrating { .. } => MaintenanceNotificationKind::Migrating,
            Self::Moving { .. } => MaintenanceNotificationKind::Moving,
        }
    }

    pub(crate) fn sequence(self) -> u64 {
        match self {
            Self::Migrating { sequence, .. } | Self::Moving { sequence, .. } => sequence,
        }
    }

    pub(crate) fn ttl(self) -> Duration {
        match self {
            Self::Migrating { ttl, .. } | Self::Moving { ttl, .. } => ttl,
        }
    }
}

pub(crate) struct ReceivedMaintenanceNotification {
    pub(crate) received_at: std::time::Instant,
    pub(crate) notification: ParsedMaintenanceNotification,
}

pub(crate) struct PendingHandoff {
    pub(crate) sequence: u64,
    pub(crate) deadline: tokio::time::Instant,
}

pub(crate) struct MaintenanceState {
    pub(crate) listener_id: u64,
    pub(crate) enabled: bool,
    seen: VecDeque<(MaintenanceNotificationKind, u64)>,
    pub(crate) pending: Option<PendingHandoff>,
}

impl MaintenanceState {
    pub(crate) fn new(listener_id: u64) -> Self {
        Self {
            listener_id,
            enabled: true,
            seen: VecDeque::with_capacity(64),
            pending: None,
        }
    }

    pub(crate) fn accept(
        &mut self,
        received: ReceivedMaintenanceNotification,
    ) -> Option<ParsedMaintenanceNotification> {
        if !self.enabled {
            return None;
        }
        let notification = received.notification;
        let key = (notification.kind(), notification.sequence());
        if self.seen.contains(&key) {
            return None;
        }

        let moving_deadline = match notification {
            ParsedMaintenanceNotification::Moving { ttl, .. } => {
                let half_ttl = ttl / 2;
                let deadline = received.received_at.checked_add(half_ttl)?;
                Some(tokio::time::Instant::from_std(deadline))
            }
            ParsedMaintenanceNotification::Migrating { .. } => None,
        };
        if self.seen.len() == 64 {
            self.seen.pop_front();
        }
        self.seen.push_back(key);

        if let ParsedMaintenanceNotification::Moving { sequence, .. } = notification {
            let deadline = moving_deadline.expect("MOVING notifications compute a deadline");
            if self
                .pending
                .as_ref()
                .is_none_or(|pending| deadline < pending.deadline)
            {
                self.pending = Some(PendingHandoff { sequence, deadline });
            }
        }
        Some(notification)
    }
}

pub(crate) fn parse_maintenance_push(
    received: ReceivedPushFrame,
) -> Option<ReceivedMaintenanceNotification> {
    let Frame::Push(items) = received.frame else {
        return None;
    };
    let kind = frame_bytes(items.first()?)?;
    let notification = match kind {
        b"MIGRATING" if items.len() == 3 => ParsedMaintenanceNotification::Migrating {
            sequence: frame_u64(&items[1])?,
            ttl: Duration::from_secs(frame_u64(&items[2])?),
        },
        b"MOVING" if items.len() == 4 && is_null(&items[3]) => {
            ParsedMaintenanceNotification::Moving {
                sequence: frame_u64(&items[1])?,
                ttl: Duration::from_secs(frame_u64(&items[2])?),
            }
        }
        _ => return None,
    };
    Some(ReceivedMaintenanceNotification {
        received_at: received.received_at,
        notification,
    })
}

fn frame_bytes(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::SimpleString(value) | Frame::BulkString(Some(value)) => Some(value),
        _ => None,
    }
}

fn frame_u64(frame: &Frame) -> Option<u64> {
    match frame {
        Frame::Integer(value) => u64::try_from(*value).ok(),
        other => std::str::from_utf8(frame_bytes(other)?).ok()?.parse().ok(),
    }
}

fn is_null(frame: &Frame) -> bool {
    matches!(
        frame,
        Frame::Null | Frame::BulkString(None) | Frame::Array(None)
    )
}

pub(crate) async fn register_connection(
    connection: &mut RedisConnection,
) -> Result<tokio::sync::broadcast::Receiver<ReceivedPushFrame>, RedisError> {
    if !connection.is_resp3() {
        return Err(RedisError::UnexpectedResponse {
            expected: "a RESP3 connection for maintenance notifications",
            actual: "RESP2 connection".to_owned(),
        });
    }

    // Install the feed before registration so a push arriving immediately
    // after the OK response cannot race past the listener.
    let pushes = connection.subscribe_received_pushes();
    let mut responses = connection
        .execute_pipeline(vec![array(vec![
            bulk("CLIENT"),
            bulk("MAINT_NOTIFICATIONS"),
            bulk("ON"),
            bulk("moving-endpoint-type"),
            bulk("none"),
        ])])
        .await?;
    let response = responses.pop().ok_or(RedisError::UnexpectedResponse {
        expected: "CLIENT MAINT_NOTIFICATIONS OK response",
        actual: "empty response pipeline".to_owned(),
    })?;
    match response {
        Frame::SimpleString(value) if value.as_ref() == b"OK" => Ok(pushes),
        other => Err(RedisError::UnexpectedResponse {
            expected: "successful CLIENT MAINT_NOTIFICATIONS registration",
            actual: format!("{other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn simple(value: &'static [u8]) -> Frame {
        Frame::SimpleString(Bytes::from_static(value))
    }

    #[test]
    fn parses_integer_and_string_maintenance_fields() {
        let received_at = std::time::Instant::now();
        let moving = parse_maintenance_push(ReceivedPushFrame {
            received_at,
            frame: Frame::Push(vec![
                simple(b"MOVING"),
                Frame::Integer(17),
                Frame::BulkString(Some(Bytes::from_static(b"9"))),
                Frame::Null,
            ]),
        })
        .expect("valid MOVING push");
        assert_eq!(moving.received_at, received_at);
        assert_eq!(
            moving.notification,
            ParsedMaintenanceNotification::Moving {
                sequence: 17,
                ttl: Duration::from_secs(9),
            }
        );

        let migrating = parse_maintenance_push(ReceivedPushFrame {
            received_at,
            frame: Frame::Push(vec![
                simple(b"MIGRATING"),
                Frame::BulkString(Some(Bytes::from_static(b"18"))),
                Frame::Integer(10),
            ]),
        })
        .expect("valid MIGRATING push");
        assert_eq!(
            migrating.notification,
            ParsedMaintenanceNotification::Migrating {
                sequence: 18,
                ttl: Duration::from_secs(10),
            }
        );
    }

    #[test]
    fn rejects_non_null_endpoint_and_malformed_fields() {
        for frame in [
            Frame::Push(vec![
                simple(b"MOVING"),
                Frame::Integer(1),
                Frame::Integer(2),
                simple(b"other:6379"),
            ]),
            Frame::Push(vec![
                simple(b"MOVING"),
                Frame::Integer(-1),
                Frame::Integer(2),
                Frame::Null,
            ]),
            Frame::Push(vec![simple(b"MIGRATING"), Frame::Integer(1)]),
            Frame::Push(vec![simple(b"OTHER"), Frame::Integer(1), Frame::Integer(2)]),
        ] {
            assert!(
                parse_maintenance_push(ReceivedPushFrame {
                    received_at: std::time::Instant::now(),
                    frame,
                })
                .is_none()
            );
        }
    }

    // `std::time::Instant` has a much wider representable range on Windows,
    // so even `Duration::MAX / 2` does not exercise checked-add overflow there.
    #[cfg(not(windows))]
    #[test]
    fn ignores_moving_deadline_overflow_without_poisoning_dedup_state() {
        let mut state = MaintenanceState::new(1);
        let received = ReceivedMaintenanceNotification {
            received_at: std::time::Instant::now(),
            notification: ParsedMaintenanceNotification::Moving {
                sequence: 99,
                ttl: Duration::MAX,
            },
        };
        assert_eq!(state.accept(received), None);
        assert!(state.pending.is_none());

        let retry = ReceivedMaintenanceNotification {
            received_at: std::time::Instant::now(),
            notification: ParsedMaintenanceNotification::Moving {
                sequence: 99,
                ttl: Duration::ZERO,
            },
        };
        assert!(matches!(
            state.accept(retry),
            Some(ParsedMaintenanceNotification::Moving { sequence: 99, .. })
        ));
    }
}
