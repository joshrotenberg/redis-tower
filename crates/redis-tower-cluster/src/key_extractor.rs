//! Extract the routing key from a command frame.
//!
//! For cluster routing we need the command's first key so we can compute its
//! hash slot. Most commands follow `COMMAND key [args...]`, so the key is at
//! `argv[1]` -- but a meaningful minority do not, and blindly hashing
//! `argv[1]` routes them to the wrong node and eats a guaranteed MOVED
//! round-trip (often on the hottest commands: Lua scripts, stream consumers,
//! multi-key set ops). [`extract_key`] uses a per-command table, modelled on
//! the Redis command key specs, for the families where the first key is not
//! `argv[1]`:
//!
//! | Family | Commands | First key |
//! |---|---|---|
//! | script / function | `EVAL[_RO]`, `EVALSHA[_RO]`, `FCALL[_RO]` | after `numkeys` at `argv[2]` |
//! | numkeys-first | `LMPOP`, `ZMPOP`, `SINTERCARD`, `ZUNION`, `ZINTER`, `ZDIFF`, `MSETEX` | after `numkeys` at `argv[1]` |
//! | blocking numkeys | `BLMPOP`, `BZMPOP` | after `numkeys` at `argv[2]` |
//! | streams | `XREAD`, `XREADGROUP` | first token after `STREAMS` |
//! | subcommand + key | `OBJECT <sub> key`, `MEMORY USAGE key` | `argv[2]` |
//! | op + dest | `BITOP <op> dest src...` | `argv[2]` |
//! | migration | `MIGRATE host port key ...`, `MIGRATE ... "" ... KEYS key...` | `argv[3]` or first key after `KEYS` |
//!
//! When `numkeys` is `0` (a keyless script, e.g. `EVAL "..." 0`) there is no
//! key, so routing falls back to the default node.
//!
//! # Example
//!
//! ```
//! use redis_tower_cluster::key_extractor::{CommandKeys, extract_keys};
//! use redis_tower_protocol::helpers::{array, bulk};
//!
//! let command = array(vec![
//!     bulk("EVAL"),
//!     bulk("return redis.call('GET', KEYS[1])"),
//!     bulk("1"),
//!     bulk("{user:42}:profile"),
//! ]);
//!
//! let CommandKeys::Known(keys) = extract_keys(&command)? else {
//!     panic!("EVAL has a known key specification");
//! };
//! assert_eq!(keys, vec![b"{user:42}:profile".as_slice()]);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use redis_tower_core::Frame;

use crate::slot::{extract_hash_tag, slot_for_key};

/// All Redis keys referenced by one serialized command.
///
/// Unlike [`extract_key`], this result distinguishes commands whose complete
/// key specification is known from commands that merely have a routable first
/// argument. Pipelines may use the latter for backwards-compatible routing;
/// transactions must reject it because they cannot prove that every key hashes
/// to the same slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandKeys<'a> {
    /// A known command that never addresses a Redis key.
    Keyless,
    /// A known command and every key it addresses, in wire order.
    ///
    /// Order and duplicates are retained so callers can split and reassemble
    /// commands such as `MGET` without losing positional semantics.
    Known(Vec<&'a [u8]>),
    /// A command outside the maintained key-spec table.
    ///
    /// `first_key` mirrors the legacy `argv[1]` routing fallback when that
    /// argument is a bulk string, but it is not proof that the command has only
    /// one key.
    Unknown {
        /// Command name exactly as serialized by the caller.
        command: &'a [u8],
        /// Legacy first-key fallback, if present and binary-safe.
        first_key: Option<&'a [u8]>,
    },
}

/// Hash slots referenced by one serialized command.
///
/// This is the slot-level counterpart of [`CommandKeys`]. Key order and
/// duplicates are retained.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandSlots<'a> {
    /// A known keyless command.
    Keyless,
    /// Slots for every key in a known command, in key order.
    Known(Vec<u16>),
    /// A command outside the maintained key-spec table.
    Unknown {
        /// Command name exactly as serialized by the caller.
        command: &'a [u8],
        /// Slot for the legacy first-key fallback, when one exists.
        first_slot: Option<u16>,
    },
}

/// A malformed command frame or key layout.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyExtractionError {
    /// The request was not a non-empty RESP array.
    #[error("invalid Redis command frame: {0}")]
    InvalidFrame(&'static str),
    /// A required argument is absent.
    #[error("malformed {command} command: missing argument {index} ({expected})")]
    MissingArgument {
        /// Uppercase Redis command name.
        command: String,
        /// Zero-based position of the missing argument.
        index: usize,
        /// Human-readable description of the required argument.
        expected: &'static str,
    },
    /// An argument has the wrong RESP type.
    #[error("malformed {command} command: argument {index} must be {expected}")]
    InvalidArgument {
        /// Uppercase Redis command name.
        command: String,
        /// Zero-based position of the invalid argument.
        index: usize,
        /// Human-readable description of the required argument type.
        expected: &'static str,
    },
    /// A `numkeys` argument is not a valid non-negative integer.
    #[error("malformed {command} command: argument {index} is not a valid numkeys value")]
    InvalidNumKeys {
        /// Uppercase Redis command name.
        command: String,
        /// Zero-based position of the invalid `numkeys` argument.
        index: usize,
    },
    /// The frame contains fewer key arguments than its declared count.
    #[error(
        "malformed {command} command: declares {declared} keys but only {available} are present"
    )]
    KeyCountMismatch {
        /// Uppercase Redis command name.
        command: String,
        /// Number of keys declared by the command.
        declared: usize,
        /// Number of key arguments actually present.
        available: usize,
    },
    /// A command-specific dynamic layout is invalid.
    #[error("malformed {command} command: {detail}")]
    InvalidLayout {
        /// Uppercase Redis command name.
        command: String,
        /// Static explanation of the malformed layout.
        detail: &'static str,
    },
}

/// Failure to prove that a group of commands belongs to one hash slot.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SlotExtractionError {
    /// One frame has an invalid key layout.
    #[error(transparent)]
    Malformed(#[from] KeyExtractionError),
    /// Unknown commands cannot be declared transaction-safe from raw frames.
    #[error(
        "cannot determine every Redis key for unknown command {command}; slot pinning is unsafe"
    )]
    UnknownCommand {
        /// Command name exactly as serialized by the caller.
        command: String,
    },
    /// At least two keys hash to different cluster slots.
    #[error("CROSSSLOT keys in request hash to different slots ({first} and {second})")]
    CrossSlot {
        /// First conflicting hash slot.
        first: u16,
        /// Second conflicting hash slot.
        second: u16,
    },
}

/// Extract every Redis key addressed by `frame`.
///
/// Known command layouts follow Redis key specifications, including dynamic
/// `numkeys` forms, scripts/functions, stream reads, destination/source
/// commands, and the multi-key commands used by cluster pipelines. Malformed
/// known commands return an error rather than silently falling back to a
/// default node. Commands outside the table return [`CommandKeys::Unknown`].
pub fn extract_keys(frame: &Frame) -> Result<CommandKeys<'_>, KeyExtractionError> {
    let items = command_items(frame)?;
    let command = command_bytes(items)?;
    let upper = command
        .iter()
        .map(u8::to_ascii_uppercase)
        .collect::<Vec<_>>();
    let name = upper.as_slice();

    let keys = match name {
        // Server, connection, transaction-boundary, and cluster-wide commands.
        b"ACL" | b"AUTH" | b"BGREWRITEAOF" | b"BGSAVE" | b"CLIENT" | b"CLUSTER" | b"COMMAND"
        | b"CONFIG" | b"DBSIZE" | b"DISCARD" | b"ECHO" | b"EXEC" | b"FAILOVER" | b"FLUSHALL"
        | b"FLUSHDB" | b"FT.CONFIG" | b"FT._LIST" | b"FUNCTION" | b"HELLO" | b"HOTKEYS"
        | b"INFO" | b"KEYS" | b"LASTSAVE" | b"LATENCY" | b"LOLWUT" | b"MODULE" | b"MONITOR"
        | b"MULTI" | b"PING" | b"PUBLISH" | b"PUBSUB" | b"RANDOMKEY" | b"READONLY"
        | b"READWRITE" | b"REPLICAOF" | b"RESET" | b"ROLE" | b"SAVE" | b"SCAN" | b"SCRIPT"
        | b"SELECT" | b"SHUTDOWN" | b"SLOWLOG" | b"SWAPDB" | b"TIME" | b"TS.MGET"
        | b"TS.MRANGE" | b"TS.MREVRANGE" | b"TS.QUERYINDEX" | b"UNWATCH" | b"WAIT" | b"WAITAOF" => {
            CommandKeys::Keyless
        }

        // Every argument is a key.
        b"DEL" | b"EXISTS" | b"MGET" | b"PFCOUNT" | b"SDIFF" | b"SINTER" | b"SUNION" | b"TOUCH"
        | b"UNLINK" | b"WATCH" => {
            CommandKeys::Known(keys_in_range(items, command, 1, items.len())?)
        }

        // Alternating key/value forms.
        b"MSET" | b"MSETNX" => {
            CommandKeys::Known(strided_keys(items, command, 1, 2, "key/value pairs")?)
        }
        // JSON.MSET key path value [key path value ...].
        b"JSON.MSET" | b"TS.MADD" => CommandKeys::Known(strided_keys(
            items,
            command,
            1,
            3,
            "key/path/value triples",
        )?),
        // JSON.MGET key [key ...] path: the final argument is not a key.
        b"JSON.MGET" => {
            if items.len() < 3 {
                return Err(invalid_layout(
                    command,
                    "requires at least one key and a path",
                ));
            }
            CommandKeys::Known(keys_in_range(items, command, 1, items.len() - 1)?)
        }

        // Script/function body/name numkeys key [key ...] arg [arg ...].
        b"EVAL" | b"EVALSHA" | b"EVAL_RO" | b"EVALSHA_RO" | b"FCALL" | b"FCALL_RO" => {
            CommandKeys::Known(keys_after_numkeys(items, command, 2, 3, true)?)
        }

        // numkeys key [key ...] [options].
        b"LMPOP" | b"SINTERCARD" | b"ZDIFF" | b"ZINTER" | b"ZINTERCARD" | b"ZMPOP" | b"ZUNION" => {
            CommandKeys::Known(keys_after_numkeys(items, command, 1, 2, false)?)
        }
        // timeout numkeys key [key ...] [options].
        b"BLMPOP" | b"BZMPOP" => {
            CommandKeys::Known(keys_after_numkeys(items, command, 2, 3, false)?)
        }
        // destination numkeys source [source ...] [options].
        b"CMS.MERGE" | b"TDIGEST.MERGE" | b"ZDIFFSTORE" | b"ZINTERSTORE" | b"ZUNIONSTORE" => {
            let mut keys = vec![required_key(items, command, 1, "destination key")?];
            keys.extend(keys_after_numkeys(items, command, 2, 3, false)?);
            CommandKeys::Known(keys)
        }
        // MSETEX numkeys key value [key value ...] [options].
        b"MSETEX" => CommandKeys::Known(msetex_keys(items, command)?),

        // Blocking key lists end with a timeout argument.
        b"BLPOP" | b"BRPOP" | b"BZPOPMAX" | b"BZPOPMIN" => {
            if items.len() < 3 {
                return Err(invalid_layout(
                    command,
                    "requires at least one key and a timeout",
                ));
            }
            CommandKeys::Known(keys_in_range(items, command, 1, items.len() - 1)?)
        }

        // Commands with two fixed key arguments.
        b"BLMOVE" | b"BRPOPLPUSH" | b"COPY" | b"GEOSEARCHSTORE" | b"LCS" | b"LMOVE" | b"RENAME"
        | b"RENAMENX" | b"RPOPLPUSH" | b"SMOVE" | b"TS.CREATERULE" | b"TS.DELETERULE"
        | b"ZRANGESTORE" => CommandKeys::Known(vec![
            required_key(items, command, 1, "first key")?,
            required_key(items, command, 2, "second key")?,
        ]),

        // Destination plus every source key.
        b"BITOP" => {
            if items.len() < 4 {
                return Err(invalid_layout(
                    command,
                    "requires an operation, destination key, and source key",
                ));
            }
            CommandKeys::Known(keys_in_range(items, command, 2, items.len())?)
        }
        b"PFMERGE" | b"SDIFFSTORE" | b"SINTERSTORE" | b"SUNIONSTORE" => {
            CommandKeys::Known(keys_in_range(items, command, 1, items.len())?)
        }

        // Stream reads encode N keys followed by N IDs after STREAMS.
        b"XREAD" | b"XREADGROUP" => {
            CommandKeys::Known(stream_read_keys(items, command, name == b"XREADGROUP")?)
        }

        // MIGRATE has either one source key or a KEYS tail after options.
        b"MIGRATE" => CommandKeys::Known(migrate_keys_strict(items, command)?),

        // Optional destination keys introduced by STORE/STOREDIST.
        b"SORT" | b"SORT_RO" => CommandKeys::Known(sort_keys(items, command)?),
        b"GEORADIUS" => CommandKeys::Known(keys_with_optional_store(items, command, 6)?),
        b"GEORADIUSBYMEMBER" => CommandKeys::Known(keys_with_optional_store(items, command, 5)?),

        // Subcommand-first families.
        b"MEMORY" => {
            if matches_token(items.get(1), b"USAGE") {
                CommandKeys::Known(vec![required_key(items, command, 2, "key")?])
            } else {
                CommandKeys::Keyless
            }
        }
        b"OBJECT" => {
            if matches_token(items.get(1), b"HELP") {
                CommandKeys::Keyless
            } else {
                CommandKeys::Known(vec![required_key(items, command, 2, "key")?])
            }
        }
        b"XGROUP" | b"XINFO" => {
            if matches_token(items.get(1), b"HELP") {
                CommandKeys::Keyless
            } else {
                CommandKeys::Known(vec![required_key(items, command, 2, "stream key")?])
            }
        }
        b"FT.CURSOR" => CommandKeys::Known(vec![required_key(items, command, 2, "index name")?]),

        // Every remaining command exported by redis-tower-commands has one
        // routing key at argv[1]. Fields, members, paths, and options that
        // follow are deliberately not keys.
        _ if is_known_single_key(name) => {
            CommandKeys::Known(vec![required_key(items, command, 1, "key")?])
        }

        // Preserve the original router's permissive argv[1] fallback for
        // custom/module commands, but mark it unknown so an atomic transaction
        // cannot mistake a partial key list for a complete one.
        _ => CommandKeys::Unknown {
            command,
            first_key: items.get(1).and_then(as_key),
        },
    };

    Ok(keys)
}

/// Extract every hash slot addressed by `frame`.
pub fn extract_slots(frame: &Frame) -> Result<CommandSlots<'_>, KeyExtractionError> {
    Ok(match extract_keys(frame)? {
        CommandKeys::Keyless => CommandSlots::Keyless,
        CommandKeys::Known(keys) => {
            CommandSlots::Known(keys.into_iter().map(slot_for_key).collect())
        }
        CommandKeys::Unknown { command, first_key } => CommandSlots::Unknown {
            command,
            first_slot: first_key.map(slot_for_key),
        },
    })
}

/// Determine the authoritative routing slot for one explicit pipeline frame.
///
/// Known command layouts are validated in full: every key must hash to the
/// same slot or the frame is rejected with [`SlotExtractionError::CrossSlot`].
/// Commands outside the maintained key-spec table retain the cluster client's
/// backwards-compatible routing behavior and use their first bulk-string
/// argument as the routing key. Unknown commands without such an argument, and
/// known keyless commands, return `Ok(None)` so the caller can use its default
/// node.
///
/// This is deliberately less strict than [`common_slot`]. A non-atomic
/// pipeline can safely preserve legacy routing for custom commands, while a
/// transaction must reject them because it cannot prove that every key is
/// pinned to one slot.
pub fn pipeline_routing_slot(frame: &Frame) -> Result<Option<u16>, SlotExtractionError> {
    match extract_slots(frame)? {
        CommandSlots::Keyless => Ok(None),
        CommandSlots::Known(slots) => common_known_slot(slots),
        CommandSlots::Unknown { first_slot, .. } => Ok(first_slot),
    }
}

/// Prove that every key in `frames` hashes to one cluster slot.
///
/// Returns `Ok(None)` for an empty/keyless group. Unknown commands are rejected
/// even when they have a routable first argument: raw frames do not provide
/// enough information to prove that a custom command has no additional keys.
pub fn common_slot(frames: &[Frame]) -> Result<Option<u16>, SlotExtractionError> {
    let mut common = None;
    for frame in frames {
        match extract_slots(frame)? {
            CommandSlots::Keyless => {}
            CommandSlots::Known(slots) => {
                if let Some(slot) = common_known_slot(slots)? {
                    if let Some(first) = common {
                        if first != slot {
                            return Err(SlotExtractionError::CrossSlot {
                                first,
                                second: slot,
                            });
                        }
                    } else {
                        common = Some(slot);
                    }
                }
            }
            CommandSlots::Unknown { command, .. } => {
                return Err(SlotExtractionError::UnknownCommand {
                    command: String::from_utf8_lossy(command).into_owned(),
                });
            }
        }
    }
    Ok(common)
}

fn common_known_slot(
    slots: impl IntoIterator<Item = u16>,
) -> Result<Option<u16>, SlotExtractionError> {
    let mut common = None;
    for slot in slots {
        if let Some(first) = common {
            if first != slot {
                return Err(SlotExtractionError::CrossSlot {
                    first,
                    second: slot,
                });
            }
        } else {
            common = Some(slot);
        }
    }
    Ok(common)
}

fn command_items(frame: &Frame) -> Result<&[Frame], KeyExtractionError> {
    match frame {
        Frame::Array(Some(items)) if !items.is_empty() => Ok(items),
        Frame::Array(Some(_)) => Err(KeyExtractionError::InvalidFrame(
            "expected a non-empty command array",
        )),
        Frame::Array(None) => Err(KeyExtractionError::InvalidFrame(
            "null arrays are not command requests",
        )),
        _ => Err(KeyExtractionError::InvalidFrame(
            "expected a non-empty RESP array",
        )),
    }
}

fn command_bytes(items: &[Frame]) -> Result<&[u8], KeyExtractionError> {
    match items.first() {
        Some(Frame::BulkString(Some(command))) if !command.is_empty() => Ok(command),
        Some(Frame::BulkString(Some(_))) => Err(KeyExtractionError::InvalidFrame(
            "command name must not be empty",
        )),
        Some(_) => Err(KeyExtractionError::InvalidFrame(
            "command name must be a non-null bulk string",
        )),
        None => Err(KeyExtractionError::InvalidFrame(
            "expected a non-empty command array",
        )),
    }
}

fn command_display(command: &[u8]) -> String {
    String::from_utf8_lossy(command).into_owned()
}

fn invalid_layout(command: &[u8], detail: &'static str) -> KeyExtractionError {
    KeyExtractionError::InvalidLayout {
        command: command_display(command),
        detail,
    }
}

fn required_key<'a>(
    items: &'a [Frame],
    command: &[u8],
    index: usize,
    expected: &'static str,
) -> Result<&'a [u8], KeyExtractionError> {
    match items.get(index) {
        Some(Frame::BulkString(Some(key))) => Ok(key),
        Some(_) => Err(KeyExtractionError::InvalidArgument {
            command: command_display(command),
            index,
            expected: "a non-null bulk-string key",
        }),
        None => Err(KeyExtractionError::MissingArgument {
            command: command_display(command),
            index,
            expected,
        }),
    }
}

fn keys_in_range<'a>(
    items: &'a [Frame],
    command: &[u8],
    start: usize,
    end: usize,
) -> Result<Vec<&'a [u8]>, KeyExtractionError> {
    if start >= end {
        return Err(invalid_layout(command, "requires at least one key"));
    }
    (start..end)
        .map(|index| required_key(items, command, index, "key"))
        .collect()
}

fn strided_keys<'a>(
    items: &'a [Frame],
    command: &[u8],
    start: usize,
    stride: usize,
    layout: &'static str,
) -> Result<Vec<&'a [u8]>, KeyExtractionError> {
    let remaining = items.len().saturating_sub(start);
    if remaining == 0 || !remaining.is_multiple_of(stride) {
        return Err(invalid_layout(command, layout));
    }
    (start..items.len())
        .step_by(stride)
        .map(|index| required_key(items, command, index, "key"))
        .collect()
}

fn strict_numkeys(
    items: &[Frame],
    command: &[u8],
    index: usize,
    allow_zero: bool,
) -> Result<usize, KeyExtractionError> {
    let Some(value) = items.get(index) else {
        return Err(KeyExtractionError::MissingArgument {
            command: command_display(command),
            index,
            expected: "numkeys",
        });
    };
    let Some(parsed) = parse_int(value) else {
        return Err(KeyExtractionError::InvalidNumKeys {
            command: command_display(command),
            index,
        });
    };
    if parsed < 0 || (!allow_zero && parsed == 0) {
        return Err(KeyExtractionError::InvalidNumKeys {
            command: command_display(command),
            index,
        });
    }
    usize::try_from(parsed).map_err(|_| KeyExtractionError::InvalidNumKeys {
        command: command_display(command),
        index,
    })
}

fn keys_after_numkeys<'a>(
    items: &'a [Frame],
    command: &[u8],
    numkeys_index: usize,
    first_key: usize,
    allow_zero: bool,
) -> Result<Vec<&'a [u8]>, KeyExtractionError> {
    let count = strict_numkeys(items, command, numkeys_index, allow_zero)?;
    let available = items.len().saturating_sub(first_key);
    if available < count {
        return Err(KeyExtractionError::KeyCountMismatch {
            command: command_display(command),
            declared: count,
            available,
        });
    }
    (first_key..first_key + count)
        .map(|index| required_key(items, command, index, "declared key"))
        .collect()
}

fn msetex_keys<'a>(
    items: &'a [Frame],
    command: &[u8],
) -> Result<Vec<&'a [u8]>, KeyExtractionError> {
    let count = strict_numkeys(items, command, 1, false)?;
    let first_key = 2;
    let required_args = count.checked_mul(2).ok_or_else(|| {
        invalid_layout(command, "declared key count overflows the command layout")
    })?;
    let available_args = items.len().saturating_sub(first_key);
    if available_args < required_args {
        return Err(KeyExtractionError::KeyCountMismatch {
            command: command_display(command),
            declared: count,
            available: available_args / 2,
        });
    }
    (0..count)
        .map(|offset| required_key(items, command, first_key + offset * 2, "declared key"))
        .collect()
}

fn stream_read_keys<'a>(
    items: &'a [Frame],
    command: &[u8],
    grouped: bool,
) -> Result<Vec<&'a [u8]>, KeyExtractionError> {
    let mut index = if grouped {
        if !matches_token(items.get(1), b"GROUP") {
            return Err(invalid_layout(
                command,
                "XREADGROUP requires GROUP group consumer before options",
            ));
        }
        // GROUP, group, consumer. The names are binary-safe and may themselves
        // equal STREAMS, so begin option parsing after both of them.
        if items.len() < 4 {
            return Err(invalid_layout(
                command,
                "XREADGROUP requires a group and consumer",
            ));
        }
        4
    } else {
        1
    };

    while index < items.len() && !matches_token(items.get(index), b"STREAMS") {
        if matches_token(items.get(index), b"COUNT") || matches_token(items.get(index), b"BLOCK") {
            if items.get(index + 1).is_none() {
                return Err(KeyExtractionError::MissingArgument {
                    command: command_display(command),
                    index: index + 1,
                    expected: "option value",
                });
            }
            index += 2;
        } else if grouped && matches_token(items.get(index), b"NOACK") {
            index += 1;
        } else {
            return Err(invalid_layout(
                command,
                "expected COUNT, BLOCK, NOACK, or STREAMS",
            ));
        }
    }

    if !matches_token(items.get(index), b"STREAMS") {
        return Err(invalid_layout(command, "missing STREAMS section"));
    }
    let first_key = index + 1;
    let key_and_id_count = items.len().saturating_sub(first_key);
    if key_and_id_count < 2 || key_and_id_count % 2 != 0 {
        return Err(invalid_layout(
            command,
            "STREAMS must be followed by equally many keys and IDs",
        ));
    }
    let key_count = key_and_id_count / 2;
    keys_in_range(items, command, first_key, first_key + key_count)
}

fn migrate_keys_strict<'a>(
    items: &'a [Frame],
    command: &[u8],
) -> Result<Vec<&'a [u8]>, KeyExtractionError> {
    if items.len() < 6 {
        return Err(invalid_layout(
            command,
            "requires host, port, key, database, and timeout",
        ));
    }
    let direct = required_key(items, command, 3, "source key")?;
    if !direct.is_empty() {
        return Ok(vec![direct]);
    }

    let mut option = 6;
    while option < items.len() {
        if matches_token(items.get(option), b"KEYS") {
            return keys_in_range(items, command, option + 1, items.len());
        }
        if matches_token(items.get(option), b"COPY") || matches_token(items.get(option), b"REPLACE")
        {
            option += 1;
        } else if matches_token(items.get(option), b"AUTH") {
            if items.get(option + 1).is_none() {
                return Err(KeyExtractionError::MissingArgument {
                    command: command_display(command),
                    index: option + 1,
                    expected: "AUTH password",
                });
            }
            option += 2;
        } else if matches_token(items.get(option), b"AUTH2") {
            if items.get(option + 2).is_none() {
                return Err(KeyExtractionError::MissingArgument {
                    command: command_display(command),
                    index: option + 2,
                    expected: "AUTH2 username and password",
                });
            }
            option += 3;
        } else {
            return Err(invalid_layout(
                command,
                "empty source key requires a KEYS section after valid options",
            ));
        }
    }
    Err(invalid_layout(
        command,
        "empty source key requires a non-empty KEYS section",
    ))
}

fn sort_keys<'a>(items: &'a [Frame], command: &[u8]) -> Result<Vec<&'a [u8]>, KeyExtractionError> {
    let mut keys = vec![required_key(items, command, 1, "source key")?];
    let mut option = 2;
    while option < items.len() {
        let is_by = matches_token(items.get(option), b"BY");
        let is_get = matches_token(items.get(option), b"GET");
        if is_by || is_get {
            let pattern = required_key(items, command, option + 1, "BY/GET pattern")?;
            // Redis performs pattern substitution only when `*` is present.
            // Constant BY/GET values (including BY nosort and GET #) do not
            // address an external key.
            if pattern.contains(&b'*') {
                let tag = extract_hash_tag(pattern);
                if tag == pattern || tag.contains(&b'*') {
                    return Err(invalid_layout(
                        command,
                        "substituting BY/GET patterns require a fixed non-empty hash tag",
                    ));
                }
                // Redis derives concrete external keys by substituting the
                // sorted element into the pattern. A fixed tag pins every such
                // key, so retaining the pattern gives slot_for_key the exact
                // routing input needed for cross-slot validation.
                keys.push(pattern);
            }
            option += 2;
        } else if matches_token(items.get(option), b"LIMIT") {
            if items.get(option + 2).is_none() {
                return Err(KeyExtractionError::MissingArgument {
                    command: command_display(command),
                    index: option + 2,
                    expected: "LIMIT offset and count",
                });
            }
            option += 3;
        } else if matches_token(items.get(option), b"ASC")
            || matches_token(items.get(option), b"DESC")
            || matches_token(items.get(option), b"ALPHA")
        {
            option += 1;
        } else if matches_token(items.get(option), b"STORE") {
            keys.push(required_key(
                items,
                command,
                option + 1,
                "STORE destination key",
            )?);
            option += 2;
        } else {
            return Err(invalid_layout(command, "unrecognized SORT option layout"));
        }
    }
    Ok(keys)
}

fn keys_with_optional_store<'a>(
    items: &'a [Frame],
    command: &[u8],
    options_start: usize,
) -> Result<Vec<&'a [u8]>, KeyExtractionError> {
    if items.len() < options_start {
        return Err(invalid_layout(command, "missing required radius arguments"));
    }
    let mut keys = vec![required_key(items, command, 1, "source key")?];
    let mut option = options_start;
    while option < items.len() {
        if matches_token(items.get(option), b"WITHCOORD")
            || matches_token(items.get(option), b"WITHDIST")
            || matches_token(items.get(option), b"WITHHASH")
            || matches_token(items.get(option), b"ASC")
            || matches_token(items.get(option), b"DESC")
        {
            option += 1;
        } else if matches_token(items.get(option), b"COUNT") {
            if items.get(option + 1).is_none() {
                return Err(KeyExtractionError::MissingArgument {
                    command: command_display(command),
                    index: option + 1,
                    expected: "COUNT value",
                });
            }
            option += 2;
            if matches_token(items.get(option), b"ANY") {
                option += 1;
            }
        } else if matches_token(items.get(option), b"STORE")
            || matches_token(items.get(option), b"STOREDIST")
        {
            keys.push(required_key(
                items,
                command,
                option + 1,
                "STORE destination key",
            )?);
            option += 2;
        } else {
            return Err(invalid_layout(
                command,
                "unrecognized GEORADIUS option layout",
            ));
        }
    }
    Ok(keys)
}

fn is_known_single_key(command: &[u8]) -> bool {
    matches!(
        command,
        // Core strings, generic keyspace, hashes, lists, sets, sorted sets.
        b"APPEND" | b"BITCOUNT" | b"BITFIELD" | b"BITFIELD_RO" | b"BITPOS"
        | b"DECR" | b"DECRBY" | b"DELEX" | b"DIGEST" | b"DUMP" | b"EXPIRE"
        | b"EXPIREAT" | b"EXPIRETIME" | b"GET" | b"GETBIT" | b"GETDEL" | b"GETEX"
        | b"GETRANGE" | b"GETSET" | b"INCR" | b"INCRBY" | b"INCRBYFLOAT"
        | b"INCREX" | b"MOVE" | b"PERSIST" | b"PEXPIRE" | b"PEXPIREAT"
        | b"PEXPIRETIME" | b"PFADD" | b"PSETEX" | b"PTTL" | b"RESTORE" | b"SET"
        | b"SETBIT" | b"SETEX" | b"SETNX" | b"SETRANGE" | b"STRLEN" | b"SUBSTR"
        | b"TTL" | b"TYPE"
        | b"HDEL" | b"HEXISTS" | b"HEXPIRE" | b"HEXPIREAT" | b"HEXPIRETIME"
        | b"HGET" | b"HGETALL" | b"HGETDEL" | b"HGETEX" | b"HINCRBY"
        | b"HINCRBYFLOAT" | b"HKEYS" | b"HLEN" | b"HMGET" | b"HPERSIST"
        | b"HPEXPIRE" | b"HPEXPIREAT" | b"HPEXPIRETIME" | b"HPTTL"
        | b"HRANDFIELD" | b"HSCAN" | b"HSET" | b"HSETEX" | b"HSETNX"
        | b"HSTRLEN" | b"HTTL" | b"HVALS"
        | b"LINDEX" | b"LINSERT" | b"LLEN" | b"LPOP" | b"LPOS" | b"LPUSH"
        | b"LPUSHX" | b"LRANGE" | b"LREM" | b"LSET" | b"LTRIM" | b"RPOP"
        | b"RPUSH" | b"RPUSHX"
        | b"SADD" | b"SCARD" | b"SISMEMBER" | b"SMEMBERS" | b"SMISMEMBER"
        | b"SPOP" | b"SPUBLISH" | b"SRANDMEMBER" | b"SREM" | b"SSCAN"
        | b"ZADD" | b"ZCARD" | b"ZCOUNT" | b"ZINCRBY" | b"ZLEXCOUNT" | b"ZMSCORE"
        | b"ZPOPMAX" | b"ZPOPMIN" | b"ZRANDMEMBER" | b"ZRANGE"
        | b"ZRANGEBYLEX" | b"ZRANGEBYSCORE" | b"ZRANK" | b"ZREM"
        | b"ZREMRANGEBYLEX" | b"ZREMRANGEBYRANK" | b"ZREMRANGEBYSCORE"
        | b"ZREVRANGE" | b"ZREVRANGEBYLEX" | b"ZREVRANGEBYSCORE" | b"ZREVRANK"
        | b"ZSCAN" | b"ZSCORE"
        // Streams (subcommand-first and multi-stream reads are handled above).
        | b"XACK" | b"XACKDEL" | b"XADD" | b"XAUTOCLAIM" | b"XCFGSET"
        | b"XCLAIM" | b"XDEL" | b"XDELEX" | b"XIDMPRECORD" | b"XLEN" | b"XNACK"
        | b"XPENDING" | b"XRANGE" | b"XREVRANGE" | b"XSETID" | b"XTRIM"
        // Geo commands without a destination key.
        | b"GEOADD" | b"GEODIST" | b"GEOHASH" | b"GEOPOS" | b"GEOSEARCH"
        | b"GEORADIUS_RO" | b"GEORADIUSBYMEMBER_RO"
        // Redis JSON.
        | b"JSON.ARRAPPEND" | b"JSON.ARRINDEX" | b"JSON.ARRINSERT"
        | b"JSON.ARRLEN" | b"JSON.ARRPOP" | b"JSON.ARRTRIM" | b"JSON.CLEAR"
        | b"JSON.DEL" | b"JSON.FORGET" | b"JSON.GET" | b"JSON.MERGE"
        | b"JSON.NUMINCRBY" | b"JSON.OBJKEYS" | b"JSON.OBJLEN" | b"JSON.RESP"
        | b"JSON.SET" | b"JSON.STRAPPEND" | b"JSON.STRLEN" | b"JSON.TOGGLE"
        | b"JSON.TYPE"
        // Search/index commands use their index/alias as their routing key.
        | b"FT.AGGREGATE" | b"FT.ALIASADD" | b"FT.ALIASDEL" | b"FT.ALIASUPDATE"
        | b"FT.ALTER" | b"FT.CREATE" | b"FT.DICTADD" | b"FT.DICTDEL"
        | b"FT.DICTDUMP" | b"FT.DROPINDEX" | b"FT.EXPLAIN" | b"FT.EXPLAINCLI"
        | b"FT.HYBRID" | b"FT.INFO" | b"FT.PROFILE" | b"FT.SEARCH"
        | b"FT.SPELLCHECK" | b"FT.SUGADD" | b"FT.SUGDEL" | b"FT.SUGGET"
        | b"FT.SUGLEN" | b"FT.SYNDUMP" | b"FT.SYNUPDATE" | b"FT.TAGVALS"
        // Time series commands with one key.
        | b"TS.ADD" | b"TS.ALTER" | b"TS.CREATE" | b"TS.DECRBY" | b"TS.DEL"
        | b"TS.GET" | b"TS.INCRBY" | b"TS.INFO" | b"TS.RANGE" | b"TS.REVRANGE"
        // Probabilistic structures.
        | b"BF.ADD" | b"BF.CARD" | b"BF.EXISTS" | b"BF.INFO" | b"BF.INSERT"
        | b"BF.LOADCHUNK" | b"BF.MADD" | b"BF.MEXISTS" | b"BF.RESERVE"
        | b"BF.SCANDUMP" | b"CF.ADD" | b"CF.ADDNX" | b"CF.COUNT" | b"CF.DEL"
        | b"CF.EXISTS" | b"CF.INFO" | b"CF.INSERT" | b"CF.INSERTNX"
        | b"CF.LOADCHUNK" | b"CF.MEXISTS" | b"CF.RESERVE" | b"CF.SCANDUMP"
        | b"CMS.INCRBY" | b"CMS.INFO" | b"CMS.INITBYDIM" | b"CMS.INITBYPROB"
        | b"CMS.QUERY" | b"TDIGEST.ADD" | b"TDIGEST.BYRANK"
        | b"TDIGEST.BYREVRANK" | b"TDIGEST.CDF" | b"TDIGEST.CREATE"
        | b"TDIGEST.INFO" | b"TDIGEST.MAX" | b"TDIGEST.MIN" | b"TDIGEST.QUANTILE"
        | b"TDIGEST.RANK" | b"TDIGEST.RESET" | b"TDIGEST.REVRANK"
        | b"TDIGEST.TRIMMED_MEAN" | b"TOPK.ADD" | b"TOPK.COUNT" | b"TOPK.INCRBY"
        | b"TOPK.INFO" | b"TOPK.LIST" | b"TOPK.QUERY" | b"TOPK.RESERVE"
        // Redis 8.8 arrays and vector sets.
        | b"ARCOUNT" | b"ARDEL" | b"ARDELRANGE" | b"ARGET" | b"ARGETRANGE"
        | b"ARGREP" | b"ARINFO" | b"ARINSERT" | b"ARLASTITEMS" | b"ARLEN"
        | b"ARMGET" | b"ARMSET" | b"ARNEXT" | b"AROP" | b"ARRING" | b"ARSCAN"
        | b"ARSEEK" | b"ARSET" | b"VADD" | b"VCARD" | b"VDIM" | b"VEMB"
        | b"VGETATTR" | b"VINFO" | b"VISMEMBER" | b"VLINKS" | b"VRANDMEMBER"
        | b"VRANGE" | b"VREM" | b"VSETATTR" | b"VSIM"
    )
}

/// Extract the first key from a command frame.
///
/// Returns `None` for keyless commands (`PING`, `FLUSHDB`, ...), for
/// numkeys-style commands invoked with zero keys, and for malformed frames --
/// in every case the caller routes to the default node. See the
/// [module docs](self) for the per-command key table.
pub fn extract_key(frame: &Frame) -> Option<&[u8]> {
    let items = match frame {
        Frame::Array(Some(items)) if !items.is_empty() => items.as_slice(),
        _ => return None,
    };

    // Get the command name.
    let cmd_name = match &items[0] {
        Frame::BulkString(Some(b)) => b.as_ref(),
        _ => return None,
    };

    // Uppercase into a stack buffer to avoid a heap allocation on every routed
    // command. Every command matched below fits in this buffer; a longer name
    // can only fall through to the default arm (key at argv[1]), so route it
    // there directly rather than allocating to compare a name that cannot match.
    let mut buf = [0u8; 24];
    if cmd_name.len() > buf.len() {
        return as_key(items.get(1)?);
    }
    for (i, b) in cmd_name.iter().enumerate() {
        buf[i] = b.to_ascii_uppercase();
    }
    match &buf[..cmd_name.len()] {
        // Keyless commands route to the default node.
        //
        // MULTI/EXEC/DISCARD are keyless. Driving them as independent raw
        // commands would still scatter queued commands across nodes; use
        // `redis_tower::Transaction`, whose ClusterConnection executor proves
        // one common slot and pins the complete exchange to its master.
        b"PING" | b"ECHO" | b"AUTH" | b"SELECT" | b"FLUSHDB" | b"FLUSHALL" | b"DBSIZE"
        | b"INFO" | b"CONFIG" | b"CLUSTER" | b"CLIENT" | b"COMMAND" | b"TIME" | b"MULTI"
        | b"EXEC" | b"DISCARD" | b"HOTKEYS" | b"ACL" | b"FUNCTION" | b"LATENCY" | b"MODULE"
        | b"MONITOR" => None,

        // Script / function: `CMD body numkeys key...` -- key follows numkeys
        // at argv[2]. argv[1] is the script text / SHA / function name.
        b"EVAL" | b"EVALSHA" | b"EVAL_RO" | b"EVALSHA_RO" | b"FCALL" | b"FCALL_RO" => {
            key_after_numkeys(items, 2)
        }

        // numkeys-first: `CMD numkeys key...` -- key follows numkeys at argv[1].
        b"LMPOP" | b"ZMPOP" | b"SINTERCARD" | b"ZDIFF" | b"ZINTER" | b"ZUNION" | b"MSETEX" => {
            key_after_numkeys(items, 1)
        }

        // Blocking numkeys: `CMD timeout numkeys key...` -- numkeys at argv[2].
        b"BLMPOP" | b"BZMPOP" => key_after_numkeys(items, 2),

        // Streams: keys follow the STREAMS token, after optional COUNT/BLOCK.
        b"XREAD" | b"XREADGROUP" => key_after_token(items, b"STREAMS"),

        // Subcommand then key: `CMD SUB key` -- key at argv[2]. OBJECT HELP and
        // MEMORY DOCTOR/STATS/... have no key.
        b"OBJECT" => as_key(items.get(2)?),
        b"MEMORY" => {
            if matches_token(items.get(1), b"USAGE") {
                as_key(items.get(2)?)
            } else {
                None
            }
        }

        // BITOP op dest src...: argv[1] is the operation (AND/OR/XOR/NOT), so
        // the first key is the destination at argv[2].
        b"BITOP" => as_key(items.get(2)?),

        // MIGRATE host port key db timeout [...], or the multi-key form where
        // argv[3] is empty and the real keys follow a later KEYS token.
        b"MIGRATE" => migrate_key(items),

        // Default: the key is at argv[1].
        _ => as_key(items.get(1)?),
    }
}

/// Interpret a frame as a binary-safe key (a bulk string).
fn as_key(frame: &Frame) -> Option<&[u8]> {
    match frame {
        Frame::BulkString(Some(b)) => Some(b.as_ref()),
        _ => None,
    }
}

/// Parse a frame as an integer argument (`numkeys`, encoded as a bulk string
/// in a request, though an `Integer` frame is accepted defensively).
fn parse_int(frame: &Frame) -> Option<i64> {
    match frame {
        Frame::BulkString(Some(b)) => std::str::from_utf8(b).ok()?.trim().parse().ok(),
        Frame::Integer(n) => Some(*n),
        _ => None,
    }
}

/// True when `frame` is a bulk string equal to `token`, case-insensitively.
fn matches_token(frame: Option<&Frame>, token: &[u8]) -> bool {
    matches!(frame, Some(Frame::BulkString(Some(b))) if b.eq_ignore_ascii_case(token))
}

/// First key for a `... numkeys key [key ...]` command, where `numkeys` is at
/// `numkeys_idx`. Returns `None` when `numkeys` is `< 1`, unparseable, or the
/// key slot is absent.
fn key_after_numkeys(items: &[Frame], numkeys_idx: usize) -> Option<&[u8]> {
    if parse_int(items.get(numkeys_idx)?)? < 1 {
        return None;
    }
    as_key(items.get(numkeys_idx + 1)?)
}

/// First key appearing immediately after `token` (e.g. `STREAMS`) in the
/// argument list.
fn key_after_token<'a>(items: &'a [Frame], token: &[u8]) -> Option<&'a [u8]> {
    let pos = items.iter().position(|f| matches_token(Some(f), token))?;
    as_key(items.get(pos + 1)?)
}

/// Extract the source key from either form of `MIGRATE`.
fn migrate_key(items: &[Frame]) -> Option<&[u8]> {
    let direct = as_key(items.get(3)?)?;
    if !direct.is_empty() {
        return Some(direct);
    }

    // Parse option arities instead of blindly searching for `KEYS`: AUTH and
    // AUTH2 credentials are binary-safe and may themselves equal that token.
    // Once the real KEYS marker is reached, every remaining argument is a key.
    let mut option = 6;
    while option < items.len() {
        if matches_token(items.get(option), b"KEYS") {
            return as_key(items.get(option + 1)?);
        }
        if matches_token(items.get(option), b"COPY") || matches_token(items.get(option), b"REPLACE")
        {
            option += 1;
        } else if matches_token(items.get(option), b"AUTH") {
            items.get(option + 1)?;
            option += 2;
        } else if matches_token(items.get(option), b"AUTH2") {
            items.get(option + 2)?;
            option += 3;
        } else {
            return None;
        }
    }
    None
}

/// Returns true if the command is read-only, and so safe to route to a replica
/// under [`ReadPreference::Replica`](crate::ReadPreference).
///
/// Defined in `redis-tower` (shared with `redis-tower-sentinel`'s replica
/// routing) and re-exported here under its original path.
pub use redis_tower::is_readonly_command;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use redis_tower_protocol::helpers::{array, bulk};

    fn known_keys<'a>(result: &'a CommandKeys<'a>) -> &'a [&'a [u8]] {
        match result {
            CommandKeys::Known(keys) => keys,
            other => panic!("expected known keys, got {other:?}"),
        }
    }

    // --- comprehensive key/slot extraction used by pipelines/transactions ---

    #[test]
    fn extract_keys_distinguishes_known_keyless_and_unknown_commands() {
        assert_eq!(
            extract_keys(&array(vec![bulk("PING")])).unwrap(),
            CommandKeys::Keyless
        );

        let get = array(vec![bulk("GET"), bulk("key")]);
        assert_eq!(
            extract_keys(&get).unwrap(),
            CommandKeys::Known(vec![b"key".as_slice()])
        );

        let custom = array(vec![bulk("CUSTOM.CMD"), bulk("route"), bulk("maybe-key")]);
        assert_eq!(
            extract_keys(&custom).unwrap(),
            CommandKeys::Unknown {
                command: b"CUSTOM.CMD",
                first_key: Some(b"route"),
            }
        );

        let custom_without_args = array(vec![bulk("CUSTOM.NOARGS")]);
        assert_eq!(
            extract_keys(&custom_without_args).unwrap(),
            CommandKeys::Unknown {
                command: b"CUSTOM.NOARGS",
                first_key: None,
            }
        );
    }

    #[test]
    fn extract_keys_covers_legacy_single_key_commands() {
        for command in [
            "GEORADIUS_RO",
            "GEORADIUSBYMEMBER_RO",
            "SUBSTR",
            "JSON.RESP",
        ] {
            assert_eq!(
                extract_keys(&array(vec![bulk(command), bulk("key")])).unwrap(),
                CommandKeys::Known(vec![b"key".as_slice()]),
                "{command}"
            );
        }
    }

    #[test]
    fn extract_keys_is_binary_safe_and_preserves_order_and_duplicates() {
        let first = Bytes::from_static(b"\0{same}\xff");
        let second = Bytes::from_static(b"other\0key");
        let frame = Frame::Array(Some(vec![
            bulk("MGET"),
            Frame::BulkString(Some(first.clone())),
            Frame::BulkString(Some(second.clone())),
            Frame::BulkString(Some(first.clone())),
        ]));

        let result = extract_keys(&frame).unwrap();
        assert_eq!(
            known_keys(&result),
            &[first.as_ref(), second.as_ref(), first.as_ref()]
        );
    }

    #[test]
    fn extract_keys_handles_pipeline_split_commands() {
        let mget = array(vec![bulk("MGET"), bulk("a"), bulk("b"), bulk("a")]);
        assert_eq!(
            extract_keys(&mget).unwrap(),
            CommandKeys::Known(vec![b"a".as_slice(), b"b".as_slice(), b"a".as_slice()])
        );

        let del = array(vec![bulk("DEL"), bulk("a"), bulk("b")]);
        assert_eq!(
            extract_keys(&del).unwrap(),
            CommandKeys::Known(vec![b"a".as_slice(), b"b".as_slice()])
        );

        let mset = array(vec![
            bulk("MSET"),
            bulk("a"),
            bulk("one"),
            bulk("b"),
            bulk("two"),
        ]);
        assert_eq!(
            extract_keys(&mset).unwrap(),
            CommandKeys::Known(vec![b"a".as_slice(), b"b".as_slice()])
        );

        let malformed = array(vec![bulk("MSET"), bulk("a"), bulk("one"), bulk("b")]);
        assert!(matches!(
            extract_keys(&malformed),
            Err(KeyExtractionError::InvalidLayout { .. })
        ));
    }

    #[test]
    fn extract_keys_handles_strided_module_multi_key_commands() {
        let json = array(vec![
            bulk("JSON.MSET"),
            bulk("json-a"),
            bulk("$.x"),
            bulk("1"),
            bulk("json-b"),
            bulk("$.y"),
            bulk("2"),
        ]);
        assert_eq!(
            extract_keys(&json).unwrap(),
            CommandKeys::Known(vec![b"json-a".as_slice(), b"json-b".as_slice()])
        );

        let ts = array(vec![
            bulk("TS.MADD"),
            bulk("ts-a"),
            bulk("1"),
            bulk("1.5"),
            bulk("ts-b"),
            bulk("2"),
            bulk("2.5"),
        ]);
        assert_eq!(
            extract_keys(&ts).unwrap(),
            CommandKeys::Known(vec![b"ts-a".as_slice(), b"ts-b".as_slice()])
        );

        let json_mget = array(vec![
            bulk("JSON.MGET"),
            bulk("a"),
            bulk("b"),
            bulk("$.field"),
        ]);
        assert_eq!(
            extract_keys(&json_mget).unwrap(),
            CommandKeys::Known(vec![b"a".as_slice(), b"b".as_slice()])
        );
    }

    #[test]
    fn extract_keys_handles_scripts_and_validates_numkeys() {
        let script = array(vec![
            bulk("EVAL"),
            bulk("return 1"),
            bulk("2"),
            bulk("{u}:a"),
            bulk("{u}:b"),
            bulk("arg"),
        ]);
        assert_eq!(
            extract_keys(&script).unwrap(),
            CommandKeys::Known(vec![b"{u}:a".as_slice(), b"{u}:b".as_slice()])
        );

        let keyless = array(vec![bulk("FCALL"), bulk("f"), bulk("0"), bulk("arg")]);
        assert_eq!(
            extract_keys(&keyless).unwrap(),
            CommandKeys::Known(Vec::new())
        );

        for malformed in [
            array(vec![bulk("EVAL"), bulk("return 1"), bulk("nope")]),
            array(vec![bulk("EVAL"), bulk("return 1"), bulk("-1")]),
        ] {
            assert!(matches!(
                extract_keys(&malformed),
                Err(KeyExtractionError::InvalidNumKeys { .. })
            ));
        }

        let missing = array(vec![
            bulk("EVALSHA"),
            bulk("sha"),
            bulk("2"),
            bulk("only-one"),
        ]);
        assert!(matches!(
            extract_keys(&missing),
            Err(KeyExtractionError::KeyCountMismatch {
                declared: 2,
                available: 1,
                ..
            })
        ));
    }

    #[test]
    fn extract_keys_handles_numkeys_command_families() {
        let lmpop = array(vec![
            bulk("LMPOP"),
            bulk("2"),
            bulk("list-a"),
            bulk("list-b"),
            bulk("LEFT"),
        ]);
        assert_eq!(
            extract_keys(&lmpop).unwrap(),
            CommandKeys::Known(vec![b"list-a".as_slice(), b"list-b".as_slice()])
        );

        let blocking = array(vec![
            bulk("BZMPOP"),
            bulk("1.0"),
            Frame::Integer(2),
            bulk("z-a"),
            bulk("z-b"),
            bulk("MAX"),
        ]);
        assert_eq!(
            extract_keys(&blocking).unwrap(),
            CommandKeys::Known(vec![b"z-a".as_slice(), b"z-b".as_slice()])
        );

        let store = array(vec![
            bulk("ZINTERSTORE"),
            bulk("dest"),
            bulk("2"),
            bulk("z-a"),
            bulk("z-b"),
            bulk("WEIGHTS"),
            bulk("1"),
            bulk("2"),
        ]);
        assert_eq!(
            extract_keys(&store).unwrap(),
            CommandKeys::Known(vec![
                b"dest".as_slice(),
                b"z-a".as_slice(),
                b"z-b".as_slice(),
            ])
        );

        let zero = array(vec![bulk("ZMPOP"), bulk("0"), bulk("MIN")]);
        assert!(matches!(
            extract_keys(&zero),
            Err(KeyExtractionError::InvalidNumKeys { .. })
        ));
    }

    #[test]
    fn extract_keys_handles_msetex_key_value_stride() {
        let frame = array(vec![
            bulk("MSETEX"),
            bulk("2"),
            bulk("a"),
            bulk("one"),
            bulk("b"),
            bulk("two"),
            bulk("EX"),
            bulk("60"),
        ]);
        assert_eq!(
            extract_keys(&frame).unwrap(),
            CommandKeys::Known(vec![b"a".as_slice(), b"b".as_slice()])
        );

        let missing_pair = array(vec![
            bulk("MSETEX"),
            bulk("2"),
            bulk("a"),
            bulk("one"),
            bulk("b"),
        ]);
        assert!(matches!(
            extract_keys(&missing_pair),
            Err(KeyExtractionError::KeyCountMismatch { .. })
        ));
    }

    #[test]
    fn extract_keys_handles_streams_without_token_collisions() {
        let xread = array(vec![
            bulk("XREAD"),
            bulk("COUNT"),
            bulk("2"),
            bulk("STREAMS"),
            bulk("stream-a"),
            bulk("stream-b"),
            bulk("0"),
            bulk("$"),
        ]);
        assert_eq!(
            extract_keys(&xread).unwrap(),
            CommandKeys::Known(vec![b"stream-a".as_slice(), b"stream-b".as_slice()])
        );

        // Group and consumer names are binary-safe and may equal STREAMS; they
        // must not be mistaken for the structural STREAMS token.
        let group = array(vec![
            bulk("XREADGROUP"),
            bulk("GROUP"),
            bulk("STREAMS"),
            bulk("STREAMS"),
            bulk("NOACK"),
            bulk("STREAMS"),
            bulk("real-a"),
            bulk("real-b"),
            bulk(">"),
            bulk(">"),
        ]);
        assert_eq!(
            extract_keys(&group).unwrap(),
            CommandKeys::Known(vec![b"real-a".as_slice(), b"real-b".as_slice()])
        );

        let uneven = array(vec![
            bulk("XREAD"),
            bulk("STREAMS"),
            bulk("a"),
            bulk("b"),
            bulk("0"),
        ]);
        assert!(matches!(
            extract_keys(&uneven),
            Err(KeyExtractionError::InvalidLayout { .. })
        ));
    }

    #[test]
    fn extract_keys_handles_source_destination_and_optional_store_layouts() {
        for command in [
            "BRPOPLPUSH",
            "COPY",
            "LMOVE",
            "RENAME",
            "SMOVE",
            "ZRANGESTORE",
        ] {
            let frame = array(vec![bulk(command), bulk("source"), bulk("destination")]);
            assert_eq!(
                extract_keys(&frame).unwrap(),
                CommandKeys::Known(vec![b"source".as_slice(), b"destination".as_slice(),]),
                "{command}"
            );
        }

        let bitop = array(vec![
            bulk("BITOP"),
            bulk("AND"),
            bulk("destination"),
            bulk("source-a"),
            bulk("source-b"),
        ]);
        assert_eq!(
            extract_keys(&bitop).unwrap(),
            CommandKeys::Known(vec![
                b"destination".as_slice(),
                b"source-a".as_slice(),
                b"source-b".as_slice(),
            ])
        );

        let sort = array(vec![
            bulk("SORT"),
            bulk("{sort}:source"),
            bulk("BY"),
            bulk("{sort}:weight_*"),
            bulk("GET"),
            bulk("#"),
            bulk("STORE"),
            bulk("{sort}:destination"),
        ]);
        assert_eq!(
            extract_keys(&sort).unwrap(),
            CommandKeys::Known(vec![
                b"{sort}:source".as_slice(),
                b"{sort}:weight_*".as_slice(),
                b"{sort}:destination".as_slice(),
            ])
        );

        let radius = array(vec![
            bulk("GEORADIUS"),
            bulk("geo"),
            bulk("1"),
            bulk("2"),
            bulk("3"),
            bulk("km"),
            bulk("COUNT"),
            bulk("10"),
            bulk("ANY"),
            bulk("STOREDIST"),
            bulk("geo-result"),
        ]);
        assert_eq!(
            extract_keys(&radius).unwrap(),
            CommandKeys::Known(vec![b"geo".as_slice(), b"geo-result".as_slice()])
        );
    }

    #[test]
    fn sort_external_patterns_with_a_fixed_tag_are_binary_safe_and_pinned() {
        let source = Bytes::from_static(b"\0{sort-slot}\xff:source");
        let by = Bytes::from_static(b"\xff{sort-slot}\0:weight_*");
        let get = Bytes::from_static(b"{sort-slot}:object_*->field");
        let destination = Bytes::from_static(b"{sort-slot}:destination");
        let frame = Frame::Array(Some(vec![
            bulk("SORT"),
            Frame::BulkString(Some(source.clone())),
            bulk("BY"),
            Frame::BulkString(Some(by.clone())),
            bulk("GET"),
            Frame::BulkString(Some(get.clone())),
            bulk("STORE"),
            Frame::BulkString(Some(destination.clone())),
        ]));

        let result = extract_keys(&frame).unwrap();
        assert_eq!(
            known_keys(&result),
            &[
                source.as_ref(),
                by.as_ref(),
                get.as_ref(),
                destination.as_ref(),
            ]
        );
        assert_eq!(
            common_slot(&[frame]).unwrap(),
            Some(slot_for_key(b"{sort-slot}"))
        );
    }

    #[test]
    fn sort_external_patterns_are_checked_for_cross_slot_access() {
        let frame = array(vec![
            bulk("SORT"),
            bulk("{source}:items"),
            bulk("BY"),
            bulk("{other}:weight_*"),
            bulk("GET"),
            bulk("{source}:object_*"),
            bulk("STORE"),
            bulk("{source}:result"),
        ]);

        assert!(matches!(
            common_slot(&[frame]),
            Err(SlotExtractionError::CrossSlot { .. })
        ));
    }

    #[test]
    fn sort_get_hash_is_keyless_and_sort_ro_patterns_are_validated() {
        let frame = array(vec![
            bulk("SORT_RO"),
            bulk("{sort-ro}:items"),
            bulk("BY"),
            bulk("{sort-ro}:weight_*"),
            bulk("GET"),
            bulk("#"),
            bulk("GET"),
            bulk("{sort-ro}:object_*"),
        ]);
        assert_eq!(
            extract_keys(&frame).unwrap(),
            CommandKeys::Known(vec![
                b"{sort-ro}:items".as_slice(),
                b"{sort-ro}:weight_*".as_slice(),
                b"{sort-ro}:object_*".as_slice(),
            ])
        );
        assert_eq!(
            common_slot(&[frame]).unwrap(),
            Some(slot_for_key(b"{sort-ro}"))
        );

        let cross_slot = array(vec![
            bulk("SORT_RO"),
            bulk("{sort-ro}:items"),
            bulk("GET"),
            bulk("{other}:object_*"),
        ]);
        assert!(matches!(
            common_slot(&[cross_slot]),
            Err(SlotExtractionError::CrossSlot { .. })
        ));
    }

    #[test]
    fn sort_constant_by_and_get_patterns_do_not_address_external_keys() {
        let frame = array(vec![
            bulk("SORT"),
            bulk("{sort}:items"),
            bulk("BY"),
            bulk("nosort"),
            bulk("GET"),
            bulk("constant"),
            bulk("GET"),
            bulk("#"),
            bulk("STORE"),
            bulk("{sort}:result"),
        ]);

        assert_eq!(
            extract_keys(&frame).unwrap(),
            CommandKeys::Known(vec![
                b"{sort}:items".as_slice(),
                b"{sort}:result".as_slice(),
            ])
        );
        assert_eq!(
            common_slot(&[frame]).unwrap(),
            Some(slot_for_key(b"{sort}"))
        );
    }

    #[test]
    fn sort_rejects_patterns_without_a_fixed_non_empty_tag() {
        for frame in [
            array(vec![
                bulk("SORT"),
                bulk("{sort}:items"),
                bulk("BY"),
                bulk("weight_*"),
            ]),
            array(vec![
                bulk("SORT"),
                bulk("{sort}:items"),
                bulk("GET"),
                bulk("{tenant_*}:object_*"),
            ]),
            array(vec![
                bulk("SORT"),
                bulk("{sort}:items"),
                bulk("GET"),
                bulk("{}:object_*"),
            ]),
            array(vec![
                bulk("SORT_RO"),
                bulk("{sort}:items"),
                bulk("GET"),
                bulk("object_*"),
            ]),
        ] {
            assert!(matches!(
                extract_keys(&frame),
                Err(KeyExtractionError::InvalidLayout {
                    detail: "substituting BY/GET patterns require a fixed non-empty hash tag",
                    ..
                })
            ));
        }
    }

    #[test]
    fn comprehensive_migrate_parser_is_binary_safe_and_option_aware() {
        let frame = array(vec![
            bulk("MIGRATE"),
            bulk("127.0.0.1"),
            bulk("6380"),
            bulk(""),
            bulk("0"),
            bulk("5000"),
            bulk("AUTH2"),
            bulk("KEYS"),
            bulk("KEYS"),
            bulk("COPY"),
            bulk("KEYS"),
            bulk("first"),
            bulk("KEYS"),
            bulk(""),
        ]);
        assert_eq!(
            extract_keys(&frame).unwrap(),
            CommandKeys::Known(vec![
                b"first".as_slice(),
                b"KEYS".as_slice(),
                b"".as_slice(),
            ])
        );
    }

    #[test]
    fn malformed_known_frames_return_clear_client_side_errors() {
        for frame in [
            Frame::Null,
            Frame::Array(None),
            Frame::Array(Some(Vec::new())),
            Frame::Array(Some(vec![Frame::Integer(1)])),
        ] {
            let error = extract_keys(&frame).unwrap_err();
            assert!(error.to_string().starts_with("invalid Redis command frame"));
        }

        let missing = array(vec![bulk("GET")]);
        let error = extract_keys(&missing).unwrap_err();
        assert_eq!(
            error.to_string(),
            "malformed GET command: missing argument 1 (key)"
        );

        let wrong_type = Frame::Array(Some(vec![bulk("GET"), Frame::Integer(1)]));
        let error = extract_keys(&wrong_type).unwrap_err();
        assert!(error.to_string().contains("argument 1 must be"));
    }

    #[test]
    fn extract_slots_preserves_order_duplicates_and_unknown_status() {
        let frame = array(vec![
            bulk("MGET"),
            bulk("{a}:1"),
            bulk("{b}:1"),
            bulk("{a}:1"),
        ]);
        assert_eq!(
            extract_slots(&frame).unwrap(),
            CommandSlots::Known(vec![
                slot_for_key(b"{a}:1"),
                slot_for_key(b"{b}:1"),
                slot_for_key(b"{a}:1"),
            ])
        );

        let unknown = array(vec![bulk("CUSTOM.CMD"), bulk("route")]);
        assert_eq!(
            extract_slots(&unknown).unwrap(),
            CommandSlots::Unknown {
                command: b"CUSTOM.CMD",
                first_slot: Some(slot_for_key(b"route")),
            }
        );
    }

    #[test]
    fn pipeline_routing_slot_validates_known_keys_and_preserves_custom_routing() {
        let same_slot = array(vec![
            bulk("MGET"),
            bulk("{pipeline}:one"),
            bulk("{pipeline}:two"),
        ]);
        assert_eq!(
            pipeline_routing_slot(&same_slot).unwrap(),
            Some(slot_for_key(b"{pipeline}"))
        );

        let cross_slot = array(vec![bulk("MGET"), bulk("{a}:one"), bulk("{b}:two")]);
        assert!(matches!(
            pipeline_routing_slot(&cross_slot),
            Err(SlotExtractionError::CrossSlot { .. })
        ));

        let custom = array(vec![
            bulk("CUSTOM.CMD"),
            bulk("{custom}:route"),
            bulk("arg"),
        ]);
        assert_eq!(
            pipeline_routing_slot(&custom).unwrap(),
            Some(slot_for_key(b"{custom}:route"))
        );
        assert_eq!(
            pipeline_routing_slot(&array(vec![bulk("CUSTOM.NOARGS")])).unwrap(),
            None
        );
        assert_eq!(
            pipeline_routing_slot(&array(vec![bulk("PING")])).unwrap(),
            None
        );
    }

    #[test]
    fn common_slot_accepts_keyless_and_same_slot_frames() {
        let frames = vec![
            array(vec![bulk("PING")]),
            array(vec![bulk("SET"), bulk("{user}:a"), bulk("1")]),
            array(vec![bulk("MGET"), bulk("{user}:b"), bulk("{user}:a")]),
            array(vec![
                bulk("EVAL"),
                bulk("return 1"),
                bulk("1"),
                bulk("{user}:script"),
            ]),
        ];
        assert_eq!(common_slot(&frames).unwrap(), Some(slot_for_key(b"{user}")));
        assert_eq!(common_slot(&[]).unwrap(), None);
        assert_eq!(
            common_slot(&[array(vec![bulk("PING")]), array(vec![bulk("TIME")])]).unwrap(),
            None
        );
    }

    #[test]
    fn common_slot_crossslot_display_matches_redis_error_contract() {
        let frames = [array(vec![bulk("MGET"), bulk("{a}:1"), bulk("{b}:1")])];
        let error = common_slot(&frames).unwrap_err();
        assert!(matches!(error, SlotExtractionError::CrossSlot { .. }));
        assert!(error.to_string().starts_with("CROSSSLOT"));
    }

    #[test]
    fn common_slot_rejects_unknown_and_malformed_commands() {
        let unknown = [array(vec![bulk("CUSTOM.CMD"), bulk("route")])];
        let error = common_slot(&unknown).unwrap_err();
        assert_eq!(
            error.to_string(),
            "cannot determine every Redis key for unknown command CUSTOM.CMD; slot pinning is unsafe"
        );

        let malformed = [array(vec![bulk("MSET"), bulk("key")])];
        let error = common_slot(&malformed).unwrap_err();
        assert!(error.to_string().starts_with("malformed MSET command:"));
    }

    #[test]
    fn extract_key_from_get() {
        let frame = array(vec![bulk("GET"), bulk("mykey")]);
        assert_eq!(extract_key(&frame), Some(b"mykey".as_slice()));
    }

    #[test]
    fn extract_key_from_set() {
        let frame = array(vec![bulk("SET"), bulk("mykey"), bulk("value")]);
        assert_eq!(extract_key(&frame), Some(b"mykey".as_slice()));
    }

    #[test]
    fn no_key_for_ping() {
        let frame = array(vec![bulk("PING")]);
        assert_eq!(extract_key(&frame), None);
    }

    #[test]
    fn no_key_for_flushdb() {
        let frame = array(vec![bulk("FLUSHDB")]);
        assert_eq!(extract_key(&frame), None);
    }

    #[test]
    fn extract_key_case_insensitive() {
        let frame = array(vec![bulk("ping")]);
        assert_eq!(extract_key(&frame), None);
    }

    #[test]
    fn readonly_commands() {
        assert!(is_readonly_command(&array(vec![bulk("GET"), bulk("k")])));
        assert!(is_readonly_command(&array(vec![
            bulk("HGETALL"),
            bulk("k")
        ])));
        assert!(is_readonly_command(&array(vec![
            bulk("LRANGE"),
            bulk("k"),
            bulk("0"),
            bulk("-1")
        ])));
        assert!(is_readonly_command(&array(vec![
            bulk("ZRANGE"),
            bulk("k"),
            bulk("0"),
            bulk("-1")
        ])));
    }

    #[test]
    fn write_commands_not_readonly() {
        assert!(!is_readonly_command(&array(vec![
            bulk("SET"),
            bulk("k"),
            bulk("v")
        ])));
        assert!(!is_readonly_command(&array(vec![bulk("DEL"), bulk("k")])));
        assert!(!is_readonly_command(&array(vec![bulk("INCR"), bulk("k")])));
        assert!(!is_readonly_command(&array(vec![
            bulk("LPUSH"),
            bulk("k"),
            bulk("v")
        ])));
    }

    #[test]
    fn readonly_case_insensitive() {
        assert!(is_readonly_command(&array(vec![bulk("get"), bulk("k")])));
        assert!(is_readonly_command(&array(vec![bulk("Get"), bulk("k")])));
    }

    #[test]
    fn empty_frame_not_readonly() {
        assert!(!is_readonly_command(&Frame::Array(Some(vec![]))));
        assert!(!is_readonly_command(&Frame::Null));
    }

    #[test]
    fn expanded_readonly_coverage_engages_replicas() {
        // The reads that were previously missing -- replicas sat idle for these.
        for cmd in [
            "GETBIT",
            "BITCOUNT",
            "BITPOS",
            "SMISMEMBER",
            "SINTERCARD",
            "ZMSCORE",
            "ZRANDMEMBER",
            "ZDIFF",
            "ZUNION",
            "XLEN",
            "XRANGE",
            "XREAD",
            "XINFO",
            "GEOPOS",
            "GEODIST",
            "GEOSEARCH",
            "PFCOUNT",
            "OBJECT",
            "HRANDFIELD",
            "LPOS",
            "DUMP",
            "EXPIRETIME",
            "DIGEST",
        ] {
            assert!(
                is_readonly_command(&array(vec![bulk(cmd), bulk("k")])),
                "{cmd} should be read-only"
            );
        }
    }

    #[test]
    fn readonly_ro_variants_engage_replicas_but_base_does_not() {
        // The base commands can mutate (STORE / TTL / consumer-group), so they
        // route to the master; the dedicated _RO variants are read-only.
        for ro in [
            "EVAL_RO",
            "EVALSHA_RO",
            "FCALL_RO",
            "GEORADIUS_RO",
            "GEORADIUSBYMEMBER_RO",
            "BITFIELD_RO",
            "SORT_RO",
        ] {
            assert!(
                is_readonly_command(&array(vec![bulk(ro), bulk("k")])),
                "{ro} should be read-only"
            );
        }
        for write in [
            "GEORADIUS",
            "BITFIELD",
            "SORT",
            "XREADGROUP",
            "GETEX",
            "PFADD",
        ] {
            assert!(
                !is_readonly_command(&array(vec![bulk(write), bulk("k")])),
                "{write} can mutate and must route to the master"
            );
        }
    }

    #[test]
    fn readonly_covers_redis_stack_reads() {
        for cmd in [
            "JSON.GET",
            "JSON.TYPE",
            "FT.SEARCH",
            "FT.AGGREGATE",
            "FT.PROFILE",
            "FT.EXPLAIN",
            "FT.EXPLAINCLI",
            "FT.HYBRID",
            "FT.TAGVALS",
            "TS.RANGE",
            "TS.GET",
            "BF.EXISTS",
            "CF.COUNT",
            "CMS.QUERY",
            "TOPK.LIST",
            "VSIM",
            "VISMEMBER",
            "VRANGE",
        ] {
            assert!(
                is_readonly_command(&array(vec![bulk(cmd), bulk("k")])),
                "{cmd} should be read-only"
            );
        }
        // Stack writes still route to the master.
        for cmd in ["JSON.SET", "TS.ADD", "BF.ADD", "FT.CREATE"] {
            assert!(
                !is_readonly_command(&array(vec![bulk(cmd), bulk("k")])),
                "{cmd} should route to the master"
            );
        }
    }

    #[test]
    fn array_commands_extract_their_key_and_route_reads_to_replicas() {
        let read_commands = [
            "ARCOUNT",
            "ARGET",
            "ARGETRANGE",
            "ARGREP",
            "ARINFO",
            "ARLASTITEMS",
            "ARLEN",
            "ARMGET",
            "ARNEXT",
            "AROP",
            "ARSCAN",
        ];
        for cmd in read_commands {
            let frame = array(vec![bulk(cmd), bulk("array-key")]);
            assert_eq!(
                extract_key(&frame),
                Some(b"array-key".as_slice()),
                "{cmd} should route by its array key"
            );
            assert!(
                is_readonly_command(&frame),
                "{cmd} should be safe to route to a replica"
            );
        }

        for cmd in [
            "ARDEL",
            "ARDELRANGE",
            "ARINSERT",
            "ARMSET",
            "ARRING",
            "ARSEEK",
            "ARSET",
        ] {
            let frame = array(vec![bulk(cmd), bulk("array-key")]);
            assert_eq!(
                extract_key(&frame),
                Some(b"array-key".as_slice()),
                "{cmd} should route by its array key"
            );
            assert!(
                !is_readonly_command(&frame),
                "{cmd} mutates the array and must route to the master"
            );
        }
    }

    #[test]
    fn overlong_command_name_is_not_readonly() {
        // Longer than the stack buffer -- must return false, not panic.
        let long = "X".repeat(64);
        assert!(!is_readonly_command(&array(vec![bulk(long), bulk("k")])));
        // The longest real read-only name still fits and matches.
        assert!(is_readonly_command(&array(vec![
            bulk("GEORADIUSBYMEMBER_RO"),
            bulk("k")
        ])));
    }

    #[test]
    fn extract_key_from_hset() {
        let frame = array(vec![bulk("HSET"), bulk("hash"), bulk("field"), bulk("val")]);
        assert_eq!(extract_key(&frame), Some(b"hash".as_slice()));
    }

    #[test]
    fn extract_key_from_lpush() {
        let frame = array(vec![bulk("LPUSH"), bulk("list"), bulk("item")]);
        assert_eq!(extract_key(&frame), Some(b"list".as_slice()));
    }

    #[test]
    fn extract_key_from_zadd() {
        let frame = array(vec![
            bulk("ZADD"),
            bulk("zset"),
            bulk("1.0"),
            bulk("member"),
        ]);
        assert_eq!(extract_key(&frame), Some(b"zset".as_slice()));
    }

    #[test]
    fn no_key_for_cluster_commands() {
        assert_eq!(
            extract_key(&array(vec![bulk("CLUSTER"), bulk("SLOTS")])),
            None
        );
        assert_eq!(
            extract_key(&array(vec![bulk("CLUSTER"), bulk("INFO")])),
            None
        );
    }

    #[test]
    fn no_key_for_hotkeys_subcommands() {
        assert_eq!(
            extract_key(&array(vec![bulk("HOTKEYS"), bulk("GET")])),
            None
        );
        assert_eq!(
            extract_key(&array(vec![
                bulk("HOTKEYS"),
                bulk("START"),
                bulk("METRICS"),
                bulk("1"),
                bulk("CPU"),
            ])),
            None
        );
    }

    #[test]
    fn no_key_for_multi_exec() {
        assert_eq!(extract_key(&array(vec![bulk("MULTI")])), None);
        assert_eq!(extract_key(&array(vec![bulk("EXEC")])), None);
        assert_eq!(extract_key(&array(vec![bulk("DISCARD")])), None);
    }

    #[test]
    fn no_key_for_auth_select() {
        assert_eq!(
            extract_key(&array(vec![bulk("AUTH"), bulk("password")])),
            None
        );
        assert_eq!(extract_key(&array(vec![bulk("SELECT"), bulk("0")])), None);
    }

    #[test]
    fn server_operations_families_are_keyless() {
        for frame in [
            array(vec![bulk("ACL"), bulk("USERS")]),
            array(vec![bulk("FUNCTION"), bulk("KILL")]),
            array(vec![bulk("LATENCY"), bulk("DOCTOR")]),
            array(vec![bulk("MODULE"), bulk("LOAD"), bulk("/tmp/module.so")]),
            array(vec![bulk("MONITOR")]),
        ] {
            assert_eq!(extract_key(&frame), None);
        }
    }

    #[test]
    fn null_frame_returns_none() {
        assert_eq!(extract_key(&Frame::Null), None);
    }

    #[test]
    fn single_element_array_no_key() {
        // Only command name, no key argument.
        assert_eq!(extract_key(&array(vec![bulk("RANDOMKEY")])), None);
    }

    // --- script / function: key follows numkeys at argv[2] ---

    #[test]
    fn eval_hashes_the_key_not_the_script() {
        // EVAL script numkeys key [key ...] arg [arg ...]
        let frame = array(vec![
            bulk("EVAL"),
            bulk("return redis.call('GET', KEYS[1])"),
            bulk("1"),
            bulk("mykey"),
            bulk("extra-arg"),
        ]);
        // The old heuristic hashed argv[1] (the script text); the key is argv[3].
        assert_eq!(extract_key(&frame), Some(b"mykey".as_slice()));
    }

    #[test]
    fn evalsha_first_of_multiple_keys() {
        let frame = array(vec![
            bulk("EVALSHA"),
            bulk("abc123"),
            bulk("2"),
            bulk("k1"),
            bulk("k2"),
        ]);
        assert_eq!(extract_key(&frame), Some(b"k1".as_slice()));
    }

    #[test]
    fn eval_with_zero_keys_has_no_key() {
        // A keyless script routes to the default node.
        let frame = array(vec![bulk("EVAL"), bulk("return 1"), bulk("0")]);
        assert_eq!(extract_key(&frame), None);
    }

    #[test]
    fn fcall_and_ro_variants() {
        assert_eq!(
            extract_key(&array(vec![
                bulk("FCALL"),
                bulk("myfunc"),
                bulk("1"),
                bulk("fkey"),
            ])),
            Some(b"fkey".as_slice())
        );
        assert_eq!(
            extract_key(&array(vec![
                bulk("EVAL_RO"),
                bulk("return 1"),
                bulk("1"),
                bulk("rokey"),
            ])),
            Some(b"rokey".as_slice())
        );
        assert_eq!(
            extract_key(&array(vec![
                bulk("FCALL_RO"),
                bulk("f"),
                bulk("1"),
                bulk("frokey"),
            ])),
            Some(b"frokey".as_slice())
        );
    }

    // --- numkeys-first: key follows numkeys at argv[1] ---

    #[test]
    fn lmpop_zmpop_key_after_numkeys() {
        assert_eq!(
            extract_key(&array(vec![
                bulk("LMPOP"),
                bulk("2"),
                bulk("list1"),
                bulk("list2"),
                bulk("LEFT"),
            ])),
            Some(b"list1".as_slice())
        );
        assert_eq!(
            extract_key(&array(vec![
                bulk("ZMPOP"),
                bulk("1"),
                bulk("zset"),
                bulk("MIN"),
            ])),
            Some(b"zset".as_slice())
        );
    }

    #[test]
    fn sintercard_and_zsetops_key_after_numkeys() {
        for cmd in ["SINTERCARD", "ZUNION", "ZINTER", "ZDIFF"] {
            let frame = array(vec![bulk(cmd), bulk("2"), bulk("a"), bulk("b")]);
            assert_eq!(
                extract_key(&frame),
                Some(b"a".as_slice()),
                "{cmd} should route by its first key"
            );
        }
    }

    #[test]
    fn msetex_routes_by_first_key_after_numkeys() {
        let frame = array(vec![
            bulk("MSETEX"),
            bulk("2"),
            bulk("first"),
            bulk("one"),
            bulk("second"),
            bulk("two"),
            bulk("EX"),
            bulk("60"),
        ]);
        assert_eq!(extract_key(&frame), Some(b"first".as_slice()));
    }

    #[test]
    fn blocking_numkeys_key_at_argv3() {
        // BLMPOP timeout numkeys key [key ...] <LEFT|RIGHT>
        assert_eq!(
            extract_key(&array(vec![
                bulk("BLMPOP"),
                bulk("0"),
                bulk("2"),
                bulk("l1"),
                bulk("l2"),
                bulk("LEFT"),
            ])),
            Some(b"l1".as_slice())
        );
        assert_eq!(
            extract_key(&array(vec![
                bulk("BZMPOP"),
                bulk("1.5"),
                bulk("1"),
                bulk("z"),
                bulk("MAX"),
            ])),
            Some(b"z".as_slice())
        );
    }

    #[test]
    fn numkeys_unparseable_or_zero_is_none() {
        // numkeys "0" => no keys.
        assert_eq!(
            extract_key(&array(vec![bulk("SINTERCARD"), bulk("0")])),
            None
        );
        // garbage numkeys => can't determine the key, route to default.
        assert_eq!(
            extract_key(&array(vec![bulk("LMPOP"), bulk("notanint"), bulk("k")])),
            None
        );
    }

    // --- streams: key follows the STREAMS token ---

    #[test]
    fn xread_key_after_streams_token() {
        // XREAD COUNT 2 STREAMS s1 s2 0 0
        let frame = array(vec![
            bulk("XREAD"),
            bulk("COUNT"),
            bulk("2"),
            bulk("STREAMS"),
            bulk("s1"),
            bulk("s2"),
            bulk("0"),
            bulk("0"),
        ]);
        assert_eq!(extract_key(&frame), Some(b"s1".as_slice()));
    }

    #[test]
    fn xread_with_block_and_lowercase_streams() {
        let frame = array(vec![
            bulk("XREAD"),
            bulk("BLOCK"),
            bulk("100"),
            bulk("streams"),
            bulk("mystream"),
            bulk("$"),
        ]);
        assert_eq!(extract_key(&frame), Some(b"mystream".as_slice()));
    }

    #[test]
    fn xreadgroup_key_after_streams() {
        let frame = array(vec![
            bulk("XREADGROUP"),
            bulk("GROUP"),
            bulk("g"),
            bulk("c"),
            bulk("COUNT"),
            bulk("1"),
            bulk("STREAMS"),
            bulk("stream"),
            bulk(">"),
        ]);
        assert_eq!(extract_key(&frame), Some(b"stream".as_slice()));
    }

    // --- subcommand + key, and op + dest ---

    #[test]
    fn object_routes_by_key_not_subcommand() {
        assert_eq!(
            extract_key(&array(vec![
                bulk("OBJECT"),
                bulk("ENCODING"),
                bulk("mykey"),
            ])),
            Some(b"mykey".as_slice())
        );
        // OBJECT HELP has no key.
        assert_eq!(
            extract_key(&array(vec![bulk("OBJECT"), bulk("HELP")])),
            None
        );
    }

    #[test]
    fn memory_usage_has_key_other_subcommands_do_not() {
        assert_eq!(
            extract_key(&array(vec![bulk("MEMORY"), bulk("USAGE"), bulk("mykey"),])),
            Some(b"mykey".as_slice())
        );
        assert_eq!(
            extract_key(&array(vec![bulk("MEMORY"), bulk("DOCTOR")])),
            None
        );
        assert_eq!(
            extract_key(&array(vec![bulk("MEMORY"), bulk("STATS")])),
            None
        );
    }

    #[test]
    fn bitop_routes_by_destination_not_operation() {
        // BITOP AND dest src1 src2 -- argv[1] is the operation, argv[2] the dest.
        let frame = array(vec![
            bulk("BITOP"),
            bulk("AND"),
            bulk("dest"),
            bulk("src1"),
            bulk("src2"),
        ]);
        assert_eq!(extract_key(&frame), Some(b"dest".as_slice()));
    }

    #[test]
    fn migrate_routes_by_direct_or_keys_form_source_key() {
        let single = array(vec![
            bulk("MIGRATE"),
            bulk("127.0.0.1"),
            bulk("6380"),
            bulk("source-key"),
            bulk("0"),
            bulk("5000"),
        ]);
        assert_eq!(extract_key(&single), Some(b"source-key".as_slice()));

        let multiple = array(vec![
            bulk("migrate"),
            bulk("127.0.0.1"),
            bulk("6380"),
            bulk(""),
            bulk("0"),
            bulk("5000"),
            bulk("COPY"),
            bulk("keys"),
            bulk("first-key"),
            bulk("second-key"),
        ]);
        assert_eq!(extract_key(&multiple), Some(b"first-key".as_slice()));
    }

    #[test]
    fn migrate_skips_keys_tokens_in_credentials_and_key_values() {
        let auth = array(vec![
            bulk("MIGRATE"),
            bulk("127.0.0.1"),
            bulk("6380"),
            bulk(""),
            bulk("0"),
            bulk("5000"),
            bulk("AUTH"),
            bulk("KEYS"),
            bulk("KEYS"),
            bulk("first-key"),
            bulk("KEYS"),
        ]);
        assert_eq!(extract_key(&auth), Some(b"first-key".as_slice()));

        let auth2 = array(vec![
            bulk("MIGRATE"),
            bulk("127.0.0.1"),
            bulk("6380"),
            bulk(""),
            bulk("0"),
            bulk("5000"),
            bulk("AUTH2"),
            bulk("KEYS"),
            bulk("KEYS"),
            bulk("KEYS"),
            bulk("first-key"),
        ]);
        assert_eq!(extract_key(&auth2), Some(b"first-key".as_slice()));
    }

    #[test]
    fn migrate_routes_an_empty_source_key() {
        let frame = array(vec![
            bulk("MIGRATE"),
            bulk("127.0.0.1"),
            bulk("6380"),
            bulk(""),
            bulk("0"),
            bulk("5000"),
            bulk("KEYS"),
            bulk(""),
        ]);
        assert_eq!(extract_key(&frame), Some(b"".as_slice()));
    }

    #[test]
    fn malformed_migrate_without_a_source_key_is_keyless() {
        assert_eq!(
            extract_key(&array(vec![
                bulk("MIGRATE"),
                bulk("127.0.0.1"),
                bulk("6380"),
                bulk(""),
            ])),
            None
        );
        assert_eq!(
            extract_key(&array(vec![
                bulk("MIGRATE"),
                bulk("127.0.0.1"),
                bulk("6380"),
                bulk(""),
                bulk("0"),
                bulk("5000"),
            ])),
            None
        );
    }

    #[test]
    fn integer_numkeys_frame_is_accepted() {
        // Requests encode numkeys as a bulk string, but accept an Integer too.
        let frame = Frame::Array(Some(vec![
            bulk("LMPOP"),
            Frame::Integer(1),
            bulk("list"),
            bulk("LEFT"),
        ]));
        assert_eq!(extract_key(&frame), Some(b"list".as_slice()));
    }
}
