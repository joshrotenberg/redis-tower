//! Extract the routing key from a command frame.
//!
//! For cluster routing, we need the first key argument from the command
//! to calculate its slot. Most Redis commands have the key as the first
//! argument after the command name.

use redis_tower_core::Frame;

/// Extract the first key from a command frame.
///
/// Most Redis commands use the pattern `COMMAND key [args...]`, so the
/// key is at index 1 of the array. Some commands (like PING, FLUSHDB)
/// have no key -- these return `None`.
pub fn extract_key(frame: &Frame) -> Option<&[u8]> {
    let items = match frame {
        Frame::Array(Some(items)) if items.len() >= 2 => items,
        _ => return None,
    };

    // Get the command name.
    let cmd_name = match &items[0] {
        Frame::BulkString(Some(b)) => b.as_ref(),
        _ => return None,
    };

    // Commands with no key (return None to route to any node).
    let upper: Vec<u8> = cmd_name.iter().map(|b| b.to_ascii_uppercase()).collect();
    match upper.as_slice() {
        b"PING" | b"ECHO" | b"AUTH" | b"SELECT" | b"FLUSHDB" | b"FLUSHALL" | b"DBSIZE"
        | b"INFO" | b"CONFIG" | b"CLUSTER" | b"CLIENT" | b"COMMAND" | b"TIME" | b"MULTI"
        | b"EXEC" | b"DISCARD" => return None,
        _ => {}
    }

    // For most commands, the key is at index 1.
    match &items[1] {
        Frame::BulkString(Some(b)) => Some(b.as_ref()),
        _ => None,
    }
}

/// Returns true if the command is read-only (safe to route to a replica).
pub fn is_readonly_command(frame: &Frame) -> bool {
    let items = match frame {
        Frame::Array(Some(items)) if !items.is_empty() => items,
        _ => return false,
    };

    let cmd_name = match &items[0] {
        Frame::BulkString(Some(b)) => b.as_ref(),
        _ => return false,
    };

    let upper: Vec<u8> = cmd_name.iter().map(|b| b.to_ascii_uppercase()).collect();
    matches!(
        upper.as_slice(),
        b"GET"
            | b"MGET"
            | b"STRLEN"
            | b"EXISTS"
            | b"GETRANGE"
            | b"HGET"
            | b"HGETALL"
            | b"HKEYS"
            | b"HVALS"
            | b"HLEN"
            | b"HEXISTS"
            | b"HMGET"
            | b"LRANGE"
            | b"LLEN"
            | b"LINDEX"
            | b"SMEMBERS"
            | b"SISMEMBER"
            | b"SCARD"
            | b"SINTER"
            | b"SUNION"
            | b"SDIFF"
            | b"SRANDMEMBER"
            | b"ZRANGE"
            | b"ZRANGEBYSCORE"
            | b"ZRANGEBYLEX"
            | b"ZREVRANGE"
            | b"ZREVRANGEBYSCORE"
            | b"ZSCORE"
            | b"ZCARD"
            | b"ZRANK"
            | b"ZREVRANK"
            | b"ZCOUNT"
            | b"TTL"
            | b"PTTL"
            | b"TYPE"
            | b"DBSIZE"
            | b"PING"
            | b"ECHO"
            | b"INFO"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_protocol::helpers::{array, bulk};

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
}
