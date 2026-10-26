//! Commands that cannot run on a connection shared by concurrent callers.
//!
//! [`AutoPipelineService`](crate::AutoPipelineService) multiplexes independent
//! callers onto one socket and pairs replies with requests by position. Some
//! commands break that pairing or the shared session for every clone:
//!
//! - subscriptions and `MONITOR` turn the connection into a push stream, so
//!   later replies never line up again (under RESP3 the subscription reply
//!   itself is a push frame, so its caller waits forever);
//! - `CLIENT REPLY OFF|SKIP` suppresses replies, so one caller receives
//!   another caller's value;
//! - `RESET`, `HELLO`, and `QUIT` reset identity, switch protocol, or close
//!   the socket that every clone uses;
//! - blocking commands hold the worker for their whole wait;
//! - `MULTI`, `EXEC`, `DISCARD`, and `WATCH` sent as separate requests leave
//!   transaction state that other callers' commands land inside.
//!
//! These are rejected before anything is written. `AUTH` stays allowed: it is
//! how proactive credential rotation re-authenticates a shared worker.

use redis_tower_core::{Frame, RedisError};

/// Why a command cannot share the multiplexed connection.
#[derive(Clone, Copy)]
enum Unshareable {
    PubSub,
    Monitor,
    Replication,
    Session(&'static str),
    Blocking,
    Transaction,
}

const COMMANDS: &[(&[u8], &str, Unshareable)] = &[
    (b"SUBSCRIBE", "SUBSCRIBE", Unshareable::PubSub),
    (b"PSUBSCRIBE", "PSUBSCRIBE", Unshareable::PubSub),
    (b"SSUBSCRIBE", "SSUBSCRIBE", Unshareable::PubSub),
    (b"UNSUBSCRIBE", "UNSUBSCRIBE", Unshareable::PubSub),
    (b"PUNSUBSCRIBE", "PUNSUBSCRIBE", Unshareable::PubSub),
    (b"SUNSUBSCRIBE", "SUNSUBSCRIBE", Unshareable::PubSub),
    (b"MONITOR", "MONITOR", Unshareable::Monitor),
    (b"SYNC", "SYNC", Unshareable::Replication),
    (b"PSYNC", "PSYNC", Unshareable::Replication),
    (
        b"RESET",
        "RESET",
        Unshareable::Session(
            "it resets authentication, the selected database, and client state for every clone",
        ),
    ),
    (
        b"HELLO",
        "HELLO",
        Unshareable::Session(
            "it switches protocol and identity for every clone; configure the protocol and credentials on the connection or its factory",
        ),
    ),
    (
        b"QUIT",
        "QUIT",
        Unshareable::Session(
            "it closes the socket every clone uses; use shutdown() or a shutdown handle",
        ),
    ),
    (b"BLPOP", "BLPOP", Unshareable::Blocking),
    (b"BRPOP", "BRPOP", Unshareable::Blocking),
    (b"BRPOPLPUSH", "BRPOPLPUSH", Unshareable::Blocking),
    (b"BLMOVE", "BLMOVE", Unshareable::Blocking),
    (b"BLMPOP", "BLMPOP", Unshareable::Blocking),
    (b"BZPOPMIN", "BZPOPMIN", Unshareable::Blocking),
    (b"BZPOPMAX", "BZPOPMAX", Unshareable::Blocking),
    (b"BZMPOP", "BZMPOP", Unshareable::Blocking),
    (b"MULTI", "MULTI", Unshareable::Transaction),
    (b"EXEC", "EXEC", Unshareable::Transaction),
    (b"DISCARD", "DISCARD", Unshareable::Transaction),
    (b"WATCH", "WATCH", Unshareable::Transaction),
];

fn bulk_arg(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::BulkString(Some(value)) => Some(value.as_ref()),
        _ => None,
    }
}

fn command_args(frame: &Frame) -> Option<&[Frame]> {
    match frame {
        Frame::Array(Some(items)) if !items.is_empty() => Some(items),
        _ => None,
    }
}

/// Return whether `XREAD`/`XREADGROUP` arguments request `BLOCK` before
/// `STREAMS`. Option values are skipped so a group or consumer named `BLOCK`
/// does not count.
fn stream_read_blocks(args: &[Frame], group: bool) -> bool {
    let mut index = if group { 3 } else { 0 };
    while let Some(arg) = args.get(index).and_then(bulk_arg) {
        if arg.eq_ignore_ascii_case(b"BLOCK") {
            return true;
        }
        if arg.eq_ignore_ascii_case(b"STREAMS") {
            return false;
        }
        index += if arg.eq_ignore_ascii_case(b"COUNT") {
            2
        } else {
            1
        };
    }
    false
}

/// Classify one request frame. `CLIENT REPLY` and stream reads with `BLOCK`
/// are recognized by their arguments.
fn classify(frame: &Frame) -> Option<(&'static str, Unshareable)> {
    let items = command_args(frame)?;
    let name = bulk_arg(&items[0])?;
    if name.eq_ignore_ascii_case(b"CLIENT") {
        let subcommand = items.get(1).and_then(bulk_arg)?;
        return subcommand.eq_ignore_ascii_case(b"REPLY").then_some((
            "CLIENT REPLY",
            Unshareable::Session(
                "it suppresses replies, so one caller would receive another caller's response",
            ),
        ));
    }
    if name.eq_ignore_ascii_case(b"XREAD") {
        return stream_read_blocks(&items[1..], false)
            .then_some(("XREAD BLOCK", Unshareable::Blocking));
    }
    if name.eq_ignore_ascii_case(b"XREADGROUP") {
        return stream_read_blocks(&items[1..], true)
            .then_some(("XREADGROUP BLOCK", Unshareable::Blocking));
    }
    COMMANDS
        .iter()
        .find(|(wire, _, _)| name.eq_ignore_ascii_case(wire))
        .map(|(_, display, kind)| (*display, *kind))
}

fn rejection(command: &str, kind: Unshareable) -> RedisError {
    let message = match kind {
        Unshareable::PubSub => format!(
            "ERR {command} is not allowed on a shared multiplexed connection: subscription \
             replies would desynchronize every caller; use PubSubConnection (or the cluster \
             Pub/Sub connections) for subscriptions"
        ),
        Unshareable::Monitor => format!(
            "ERR {command} is not allowed on a shared multiplexed connection: it turns the \
             connection into an event stream; use MonitorStream on a dedicated connection"
        ),
        Unshareable::Replication => format!(
            "ERR {command} is not allowed on a shared multiplexed connection: it turns the \
             connection into a replication stream; use a dedicated RedisConnection"
        ),
        Unshareable::Session(reason) => format!(
            "ERR {command} is not allowed on a shared multiplexed connection: {reason}; use a \
             dedicated RedisConnection if you need it"
        ),
        Unshareable::Blocking => format!(
            "ERR {command} is not allowed on a shared multiplexed connection: a blocking \
             command holds the connection and stalls every other caller; use a dedicated \
             RedisConnection or a ConnectionPool for blocking commands"
        ),
        Unshareable::Transaction => format!(
            "ERR {command} sent on its own would leave transaction state on a shared \
             multiplexed connection; use Transaction, which submits WATCH/MULTI/EXEC as one \
             contiguous batch"
        ),
    };
    RedisError::Redis(message)
}

/// Reject a frame submitted as one independent request on a shared
/// multiplexed connection.
pub(crate) fn validate_shared_request(frame: &Frame) -> Result<(), RedisError> {
    match classify(frame) {
        Some((command, kind)) => Err(rejection(command, kind)),
        None => Ok(()),
    }
}

/// Reject a contiguous multi-frame request that would leave the shared
/// connection unusable for other callers.
///
/// A batch is flushed back-to-back, so a complete `[WATCH...] MULTI ... EXEC`
/// (or `DISCARD`) sequence is safe, and blocking commands queued inside
/// `MULTI` do not block. A batch that leaves `MULTI` or `WATCH` open, or that
/// contains any other unshareable command, is rejected before it is sent.
pub(crate) fn validate_shared_batch(frames: &[Frame]) -> Result<(), RedisError> {
    let mut in_multi = false;
    let mut watching = false;
    for frame in frames {
        let Some((command, kind)) = classify(frame) else {
            let unwatch = command_args(frame)
                .and_then(|items| bulk_arg(&items[0]))
                .is_some_and(|name| name.eq_ignore_ascii_case(b"UNWATCH"));
            if unwatch && !in_multi {
                watching = false;
            }
            continue;
        };
        match kind {
            Unshareable::Transaction => match command {
                "MULTI" => in_multi = true,
                "WATCH" if !in_multi => watching = true,
                "EXEC" | "DISCARD" if in_multi => {
                    in_multi = false;
                    watching = false;
                }
                _ => {}
            },
            Unshareable::Blocking if in_multi => {}
            kind => return Err(rejection(command, kind)),
        }
    }
    if in_multi || watching {
        return Err(RedisError::Redis(
            "ERR a batch that leaves MULTI or WATCH open would leak transaction state to other \
             callers of a shared multiplexed connection; end it with EXEC or DISCARD, or use \
             Transaction"
                .to_string(),
        ));
    }
    Ok(())
}

/// Reject a typed command that declares [`Command::is_blocking`] before it is
/// lowered to a frame, naming the dedicated API where one exists.
///
/// [`Command::is_blocking`]: redis_tower_core::Command::is_blocking
#[doc(hidden)]
pub fn reject_blocking_command(name: &str) -> RedisError {
    let kind = COMMANDS
        .iter()
        .find(|(wire, _, _)| name.as_bytes().eq_ignore_ascii_case(wire))
        .map(|(_, _, kind)| *kind)
        .unwrap_or(Unshareable::Blocking);
    rejection(&name.to_ascii_uppercase(), kind)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_protocol::helpers::{array, bulk};

    fn cmd(args: &[&str]) -> Frame {
        array(args.iter().map(|arg| bulk(*arg)).collect())
    }

    fn rejected(args: &[&str]) -> String {
        match validate_shared_request(&cmd(args)) {
            Err(RedisError::Redis(message)) => message,
            other => panic!("{args:?} was not rejected: {other:?}"),
        }
    }

    #[test]
    fn connection_breaking_commands_are_rejected_with_their_alternative() {
        assert!(rejected(&["subscribe", "a", "b"]).contains("PubSubConnection"));
        assert!(rejected(&["SSUBSCRIBE", "a"]).contains("PubSubConnection"));
        assert!(rejected(&["UNSUBSCRIBE"]).contains("PubSubConnection"));
        assert!(rejected(&["MONITOR"]).contains("MonitorStream"));
        assert!(rejected(&["PSYNC", "?", "-1"]).contains("replication stream"));
        assert!(rejected(&["RESET"]).contains("every clone"));
        assert!(rejected(&["HELLO", "3"]).contains("factory"));
        assert!(rejected(&["QUIT"]).contains("shutdown()"));
        assert!(rejected(&["CLIENT", "REPLY", "SKIP"]).contains("another caller"));
        assert!(rejected(&["BLPOP", "k", "0"]).contains("ConnectionPool"));
        assert!(rejected(&["bzpopmin", "k", "0"]).contains("ConnectionPool"));
        assert!(rejected(&["MULTI"]).contains("Transaction"));
        assert!(rejected(&["WATCH", "k"]).contains("Transaction"));
        assert!(rejected(&["EXEC"]).contains("Transaction"));
    }

    #[test]
    fn ordinary_and_reauthentication_commands_pass() {
        for args in [
            &["GET", "k"][..],
            &["AUTH", "user", "pass"],
            &["CLIENT", "SETNAME", "app"],
            &["CLIENT", "TRACKING", "ON"],
            &["PUBLISH", "ch", "msg"],
            &["UNWATCH"],
            &["SLOWLOG", "RESET"],
            &["CLUSTER", "RESET"],
            &["XREAD", "COUNT", "10", "STREAMS", "s", "0"],
        ] {
            validate_shared_request(&cmd(args)).unwrap();
        }
        validate_shared_request(&Frame::SimpleString(bytes::Bytes::from_static(b"PING"))).unwrap();
    }

    #[test]
    fn stream_reads_are_blocking_only_with_a_block_option() {
        assert!(
            rejected(&["XREAD", "COUNT", "5", "BLOCK", "0", "STREAMS", "s", "$"])
                .contains("XREAD BLOCK")
        );
        assert!(
            rejected(&[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "BLOCK",
                "10",
                "STREAMS",
                "s",
                ">"
            ])
            .contains("XREADGROUP BLOCK")
        );
        // A group, consumer, or stream named BLOCK is not the option.
        validate_shared_request(&cmd(&[
            "XREADGROUP",
            "GROUP",
            "BLOCK",
            "BLOCK",
            "STREAMS",
            "BLOCK",
            ">",
        ]))
        .unwrap();
        validate_shared_request(&cmd(&["XREAD", "STREAMS", "BLOCK", "0"])).unwrap();
    }

    #[test]
    fn complete_transactions_pass_as_one_batch() {
        validate_shared_batch(&[
            cmd(&["WATCH", "k"]),
            cmd(&["MULTI"]),
            cmd(&["SET", "k", "v"]),
            cmd(&["BLPOP", "q", "0"]),
            cmd(&["EXEC"]),
        ])
        .unwrap();
        validate_shared_batch(&[cmd(&["MULTI"]), cmd(&["INCR", "k"]), cmd(&["DISCARD"])]).unwrap();
        validate_shared_batch(&[cmd(&["ASKING"]), cmd(&["GET", "k"])]).unwrap();
        validate_shared_batch(&[cmd(&["WATCH", "k"]), cmd(&["UNWATCH"])]).unwrap();
    }

    #[test]
    fn batches_cannot_leave_state_open_or_smuggle_unshareable_commands() {
        for frames in [
            vec![cmd(&["MULTI"]), cmd(&["SET", "k", "v"])],
            vec![cmd(&["WATCH", "k"]), cmd(&["GET", "k"])],
            vec![cmd(&["GET", "k"]), cmd(&["SUBSCRIBE", "ch"])],
            vec![cmd(&["BLPOP", "q", "0"])],
            vec![cmd(&["CLIENT", "REPLY", "OFF"]), cmd(&["SET", "k", "v"])],
        ] {
            assert!(
                matches!(validate_shared_batch(&frames), Err(RedisError::Redis(_))),
                "{frames:?}"
            );
        }
    }

    #[test]
    fn typed_blocking_commands_name_their_alternative() {
        let RedisError::Redis(monitor) = reject_blocking_command("monitor") else {
            panic!("expected a Redis error");
        };
        assert!(monitor.contains("MonitorStream"));
        let RedisError::Redis(custom) = reject_blocking_command("my.blocking.pop") else {
            panic!("expected a Redis error");
        };
        assert!(custom.contains("MY.BLOCKING.POP") && custom.contains("ConnectionPool"));
    }
}
