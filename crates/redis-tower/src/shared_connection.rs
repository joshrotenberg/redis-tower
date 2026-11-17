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
//! - blocking commands, including `WAIT` and `WAITAOF`, hold the worker for
//!   their whole wait;
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
    // WAIT/WAITAOF hold the connection until replicas (or the AOF) acknowledge
    // or their timeout ends; a zero timeout with no replicas never returns.
    (b"WAIT", "WAIT", Unshareable::Blocking),
    (b"WAITAOF", "WAITAOF", Unshareable::Blocking),
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
/// (or `DISCARD`) sequence is supported. Blocking commands are rejected even
/// inside `MULTI`: Redis could refuse `MULTI` and then execute them outside a
/// transaction, stalling every caller before replies can be checked.
/// A batch that leaves `MULTI` or `WATCH` open, or that
/// contains any other unshareable command, is rejected before it is sent.
pub(crate) fn validate_shared_batch(frames: &[Frame]) -> Result<(), RedisError> {
    let mut in_multi = false;
    let mut watching = false;
    for frame in frames {
        let Some((command, kind)) = classify(frame) else {
            let unwatch = command_args(frame)
                .and_then(|items| bulk_arg(&items[0]))
                .is_some_and(|name| name.eq_ignore_ascii_case(b"UNWATCH"));
            if unwatch {
                if command_args(frame).map_or(0, <[Frame]>::len) != 1 {
                    return Err(wrong_control_arity("UNWATCH"));
                }
                if !in_multi {
                    watching = false;
                }
            }
            continue;
        };
        match kind {
            Unshareable::Transaction => {
                // Redis refuses a malformed DISCARD (and, on older servers,
                // EXEC) without ending the transaction, so a terminator that
                // only looks right would leave every later caller in MULTI.
                let argc = command_args(frame).map_or(0, <[Frame]>::len);
                let well_formed = if command == "WATCH" {
                    argc >= 2
                } else {
                    argc == 1
                };
                if !well_formed {
                    return Err(wrong_control_arity(command));
                }
                match command {
                    "MULTI" => in_multi = true,
                    "WATCH" if !in_multi => watching = true,
                    "EXEC" | "DISCARD" if in_multi => {
                        in_multi = false;
                        watching = false;
                    }
                    _ => {}
                }
            }
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

fn wrong_control_arity(command: &str) -> RedisError {
    RedisError::Redis(format!(
        "ERR wrong number of arguments for {command} in a batch on a shared \
         multiplexed connection: Redis can refuse it without clearing transaction state"
    ))
}

/// One transaction-control command in a contiguous caller request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionCommand {
    Multi,
    Watch,
    Unwatch,
    Exec,
    Discard,
}

/// Controls whose actual replies must leave a caller's shared session clean.
/// Syntactic MULTI state is insufficient: Redis may refuse MULTI, allowing a
/// later WATCH in the same request to run outside a transaction.
#[derive(Debug)]
pub(crate) struct SessionCheck {
    pub(crate) end: usize,
    commands: Vec<(usize, SessionCommand)>,
}

impl SessionCheck {
    pub(crate) fn offset(mut self, by: usize) -> Self {
        self.end += by;
        for (position, _) in &mut self.commands {
            *position += by;
        }
        self
    }
}

pub(crate) fn session_check(frames: &[Frame]) -> Option<SessionCheck> {
    let commands: Vec<_> = frames
        .iter()
        .enumerate()
        .filter_map(|(index, frame)| {
            let name = command_args(frame).and_then(|items| bulk_arg(&items[0]))?;
            let command = if name.eq_ignore_ascii_case(b"MULTI") {
                SessionCommand::Multi
            } else if name.eq_ignore_ascii_case(b"WATCH") {
                SessionCommand::Watch
            } else if name.eq_ignore_ascii_case(b"UNWATCH") {
                SessionCommand::Unwatch
            } else if name.eq_ignore_ascii_case(b"EXEC") {
                SessionCommand::Exec
            } else if name.eq_ignore_ascii_case(b"DISCARD") {
                SessionCommand::Discard
            } else {
                return None;
            };
            Some((index, command))
        })
        .collect();
    (!commands.is_empty()).then(|| SessionCheck {
        end: frames.len() - 1,
        commands,
    })
}

/// Whether actual control replies leave MULTI or WATCH state at the caller's
/// request boundary. Errors preserve existing state unless EXECABORT confirms
/// that Redis discarded the transaction. Unknown replies fail closed.
pub(crate) fn session_left_open(check: &SessionCheck, replies: &[Frame]) -> bool {
    let mut in_multi = false;
    let mut watching = false;
    for &(position, command) in &check.commands {
        let Some(reply) = replies.get(position) else {
            return true;
        };
        let ok = matches!(reply, Frame::SimpleString(text) if text.as_ref() == b"OK");
        let error = matches!(reply, Frame::Error(_));
        match command {
            SessionCommand::Multi if ok && !in_multi => in_multi = true,
            SessionCommand::Watch if ok && !in_multi => watching = true,
            SessionCommand::Unwatch if ok && !in_multi => watching = false,
            SessionCommand::Unwatch
                if in_multi
                    && matches!(reply, Frame::SimpleString(text) if text.as_ref() == b"QUEUED") => {
            }
            SessionCommand::Exec
                if (in_multi && matches!(reply, Frame::Array(_) | Frame::Null))
                    || matches!(reply, Frame::Error(text) if text.starts_with(b"EXECABORT ")) =>
            {
                in_multi = false;
                watching = false;
            }
            SessionCommand::Discard if ok => {
                in_multi = false;
                watching = false;
            }
            _ if error => {}
            _ => return true,
        }
    }
    in_multi || watching
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
        assert!(rejected(&["WAIT", "1", "0"]).contains("ConnectionPool"));
        assert!(rejected(&["waitaof", "0", "1", "0"]).contains("ConnectionPool"));
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
    fn malformed_transaction_control_is_rejected_in_batches() {
        for frames in [
            vec![
                cmd(&["MULTI"]),
                cmd(&["SET", "k", "v"]),
                cmd(&["DISCARD", "x"]),
            ],
            vec![
                cmd(&["MULTI"]),
                cmd(&["SET", "k", "v"]),
                cmd(&["EXEC", "x"]),
            ],
            vec![
                cmd(&["MULTI", "x"]),
                cmd(&["SET", "k", "v"]),
                cmd(&["EXEC"]),
            ],
            vec![cmd(&["WATCH"]), cmd(&["MULTI"]), cmd(&["EXEC"])],
        ] {
            assert!(
                matches!(validate_shared_batch(&frames), Err(RedisError::Redis(m)) if m.contains("arguments")),
                "{frames:?}"
            );
        }
    }

    fn reply(text: &'static [u8]) -> Frame {
        Frame::SimpleString(bytes::Bytes::from_static(text))
    }

    fn error(text: &'static [u8]) -> Frame {
        Frame::Error(bytes::Bytes::from_static(text))
    }

    #[test]
    fn review_guard_blocking_batches_cannot_depend_on_multi_being_accepted() {
        for blocking in [
            cmd(&["BLPOP", "q", "0"]),
            cmd(&["WAIT", "1", "0"]),
            cmd(&["WAITAOF", "0", "1", "0"]),
            cmd(&["XREAD", "BLOCK", "0", "STREAMS", "s", "$"]),
        ] {
            assert!(
                matches!(
                    validate_shared_batch(&[cmd(&["MULTI"]), blocking, cmd(&["EXEC"])]),
                    Err(RedisError::Redis(message)) if message.contains("ConnectionPool")
                ),
                "a refused MULTI would let the command block outside the transaction"
            );
        }
    }

    #[test]
    fn review_guard_malformed_unwatch_is_rejected_before_any_watch() {
        for frames in [
            vec![cmd(&["WATCH", "k"]), cmd(&["UNWATCH", "extra"])],
            vec![cmd(&["MULTI"]), cmd(&["UNWATCH", "extra"]), cmd(&["EXEC"])],
        ] {
            assert!(
                matches!(validate_shared_batch(&frames), Err(RedisError::Redis(message))
                    if message.contains("UNWATCH") && message.contains("arguments")),
                "{frames:?}"
            );
        }
    }

    #[test]
    fn review_guard_actual_replies_determine_watch_and_multi_cleanup() {
        let check = |frames: &[Frame], replies: &[Frame]| {
            session_left_open(&session_check(frames).unwrap(), replies)
        };
        let watch_unwatch = [cmd(&["WATCH", "k"]), cmd(&["UNWATCH"])];
        assert!(!check(&watch_unwatch, &[reply(b"OK"), reply(b"OK")]));
        assert!(check(
            &watch_unwatch,
            &[reply(b"OK"), error(b"NOPERM denied")]
        ));
        assert!(!check(
            &watch_unwatch,
            &[error(b"NOPERM denied"), error(b"NOPERM denied")]
        ));
        let denied_multi_watch = [cmd(&["MULTI"]), cmd(&["WATCH", "k"]), cmd(&["EXEC"])];
        assert!(check(
            &denied_multi_watch,
            &[
                error(b"NOPERM denied"),
                reply(b"OK"),
                error(b"ERR EXEC without MULTI")
            ]
        ));
        let queued_unwatch = [
            cmd(&["WATCH", "k"]),
            cmd(&["MULTI"]),
            cmd(&["UNWATCH"]),
            cmd(&["EXEC"]),
        ];
        assert!(check(
            &queued_unwatch,
            &[
                reply(b"OK"),
                reply(b"OK"),
                reply(b"QUEUED"),
                error(b"NOPERM denied")
            ]
        ));
        for end in [
            Frame::Array(Some(vec![reply(b"OK")])),
            Frame::Array(None),
            Frame::Null,
            error(b"EXECABORT Transaction discarded"),
        ] {
            assert!(!check(
                &queued_unwatch,
                &[reply(b"OK"), reply(b"OK"), reply(b"QUEUED"), end]
            ));
        }
        let nested = [cmd(&["MULTI"]), cmd(&["MULTI"]), cmd(&["DISCARD"])];
        assert!(check(
            &nested,
            &[
                reply(b"OK"),
                error(b"ERR nested MULTI"),
                error(b"NOPERM denied")
            ]
        ));
        assert!(!check(
            &nested,
            &[reply(b"OK"), error(b"ERR nested MULTI"), reply(b"OK")]
        ));
        assert!(check(&watch_unwatch, &[]));
        assert!(check(&watch_unwatch, &[reply(b"OK"), reply(b"unexpected")]));
        assert!(session_check(&[cmd(&["GET", "k"])]).is_none());
        let offset = session_check(&watch_unwatch).unwrap().offset(1);
        assert_eq!(offset.end, 2);
        assert!(!session_left_open(
            &offset,
            &[Frame::Null, reply(b"OK"), reply(b"OK")]
        ));
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
