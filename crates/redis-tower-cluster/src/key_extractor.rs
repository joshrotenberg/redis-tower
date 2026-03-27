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
}
