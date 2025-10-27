//! Unit tests for SCAN commands

use bytes::Bytes;
use redis_tower::Command;
use redis_tower::commands::{HScan, SScan, Scan, ZScan};
use redis_tower::parser::Frame;

#[test]
fn test_scan_frame_generation() {
    let scan = Scan::new(0);
    let frame = scan.to_frame();

    match frame {
        Frame::Array(args) => {
            assert_eq!(args.len(), 2);
            assert_eq!(args[0], Frame::BulkString(Some(b"SCAN".to_vec().into())));
            assert_eq!(args[1], Frame::BulkString(Some(b"0".to_vec().into())));
        }
        _ => panic!("Expected Array frame"),
    }
}

#[test]
fn test_scan_with_pattern_and_count() {
    let scan = Scan::new(10).pattern("user:*").count(100);
    let frame = scan.to_frame();

    match frame {
        Frame::Array(args) => {
            assert_eq!(args.len(), 6);
            assert_eq!(args[0], Frame::BulkString(Some(b"SCAN".to_vec().into())));
            assert_eq!(args[1], Frame::BulkString(Some(b"10".to_vec().into())));
            assert_eq!(args[2], Frame::BulkString(Some(b"MATCH".to_vec().into())));
            assert_eq!(args[3], Frame::BulkString(Some(b"user:*".to_vec().into())));
            assert_eq!(args[4], Frame::BulkString(Some(b"COUNT".to_vec().into())));
            assert_eq!(args[5], Frame::BulkString(Some(b"100".to_vec().into())));
        }
        _ => panic!("Expected Array frame"),
    }
}

#[test]
fn test_scan_response_parsing() {
    let response = Frame::Array(vec![
        Frame::BulkString(Some(b"42".to_vec().into())),
        Frame::Array(vec![
            Frame::BulkString(Some(b"key1".to_vec().into())),
            Frame::BulkString(Some(b"key2".to_vec().into())),
            Frame::BulkString(Some(b"key3".to_vec().into())),
        ]),
    ]);

    let result = Scan::parse_response(response).expect("Should parse successfully");
    assert_eq!(result.cursor, 42);
    assert_eq!(result.keys.len(), 3);
    assert_eq!(result.keys[0], Bytes::from("key1"));
    assert_eq!(result.keys[1], Bytes::from("key2"));
    assert_eq!(result.keys[2], Bytes::from("key3"));
}

#[test]
fn test_sscan_frame_generation() {
    let sscan = SScan::new("myset", 0);
    let frame = sscan.to_frame();

    match frame {
        Frame::Array(args) => {
            assert_eq!(args.len(), 3);
            assert_eq!(args[0], Frame::BulkString(Some(b"SSCAN".to_vec().into())));
            assert_eq!(args[1], Frame::BulkString(Some(b"myset".to_vec().into())));
            assert_eq!(args[2], Frame::BulkString(Some(b"0".to_vec().into())));
        }
        _ => panic!("Expected Array frame"),
    }
}

#[test]
fn test_sscan_with_pattern_and_count() {
    let sscan = SScan::new("myset", 5).pattern("prefix:*").count(50);
    let frame = sscan.to_frame();

    match frame {
        Frame::Array(args) => {
            assert_eq!(args.len(), 7);
            assert_eq!(args[0], Frame::BulkString(Some(b"SSCAN".to_vec().into())));
            assert_eq!(args[1], Frame::BulkString(Some(b"myset".to_vec().into())));
            assert_eq!(args[2], Frame::BulkString(Some(b"5".to_vec().into())));
            assert_eq!(args[3], Frame::BulkString(Some(b"MATCH".to_vec().into())));
            assert_eq!(
                args[4],
                Frame::BulkString(Some(b"prefix:*".to_vec().into()))
            );
            assert_eq!(args[5], Frame::BulkString(Some(b"COUNT".to_vec().into())));
            assert_eq!(args[6], Frame::BulkString(Some(b"50".to_vec().into())));
        }
        _ => panic!("Expected Array frame"),
    }
}

#[test]
fn test_sscan_response_parsing() {
    let response = Frame::Array(vec![
        Frame::BulkString(Some(b"10".to_vec().into())),
        Frame::Array(vec![
            Frame::BulkString(Some(b"member1".to_vec().into())),
            Frame::BulkString(Some(b"member2".to_vec().into())),
        ]),
    ]);

    let result = SScan::parse_response(response).expect("Should parse successfully");
    assert_eq!(result.cursor, 10);
    assert_eq!(result.members.len(), 2);
    assert_eq!(result.members[0], Bytes::from("member1"));
    assert_eq!(result.members[1], Bytes::from("member2"));
}

#[test]
fn test_zscan_frame_generation() {
    let zscan = ZScan::new("leaderboard", 0);
    let frame = zscan.to_frame();

    match frame {
        Frame::Array(args) => {
            assert_eq!(args.len(), 3);
            assert_eq!(args[0], Frame::BulkString(Some(b"ZSCAN".to_vec().into())));
            assert_eq!(
                args[1],
                Frame::BulkString(Some(b"leaderboard".to_vec().into()))
            );
            assert_eq!(args[2], Frame::BulkString(Some(b"0".to_vec().into())));
        }
        _ => panic!("Expected Array frame"),
    }
}

#[test]
fn test_zscan_with_pattern_and_count() {
    let zscan = ZScan::new("scores", 8).pattern("player:*").count(20);
    let frame = zscan.to_frame();

    match frame {
        Frame::Array(args) => {
            assert_eq!(args.len(), 7);
            assert_eq!(args[0], Frame::BulkString(Some(b"ZSCAN".to_vec().into())));
            assert_eq!(args[1], Frame::BulkString(Some(b"scores".to_vec().into())));
            assert_eq!(args[2], Frame::BulkString(Some(b"8".to_vec().into())));
            assert_eq!(args[3], Frame::BulkString(Some(b"MATCH".to_vec().into())));
            assert_eq!(
                args[4],
                Frame::BulkString(Some(b"player:*".to_vec().into()))
            );
            assert_eq!(args[5], Frame::BulkString(Some(b"COUNT".to_vec().into())));
            assert_eq!(args[6], Frame::BulkString(Some(b"20".to_vec().into())));
        }
        _ => panic!("Expected Array frame"),
    }
}

#[test]
fn test_zscan_response_parsing() {
    let response = Frame::Array(vec![
        Frame::BulkString(Some(b"0".to_vec().into())),
        Frame::Array(vec![
            Frame::BulkString(Some(b"player1".to_vec().into())),
            Frame::BulkString(Some(b"100.5".to_vec().into())),
            Frame::BulkString(Some(b"player2".to_vec().into())),
            Frame::BulkString(Some(b"200.75".to_vec().into())),
        ]),
    ]);

    let result = ZScan::parse_response(response).expect("Should parse successfully");
    assert_eq!(result.cursor, 0);
    assert_eq!(result.members.len(), 2);
    assert_eq!(result.members[0].0, Bytes::from("player1"));
    assert_eq!(result.members[0].1, 100.5);
    assert_eq!(result.members[1].0, Bytes::from("player2"));
    assert_eq!(result.members[1].1, 200.75);
}

#[test]
fn test_hscan_frame_generation() {
    let hscan = HScan::new("myhash", 0);
    let frame = hscan.to_frame();

    match frame {
        Frame::Array(args) => {
            assert_eq!(args.len(), 3);
            assert_eq!(args[0], Frame::BulkString(Some(b"HSCAN".to_vec().into())));
            assert_eq!(args[1], Frame::BulkString(Some(b"myhash".to_vec().into())));
            assert_eq!(args[2], Frame::BulkString(Some(b"0".to_vec().into())));
        }
        _ => panic!("Expected Array frame"),
    }
}

#[test]
fn test_hscan_response_parsing() {
    let response = Frame::Array(vec![
        Frame::BulkString(Some(b"15".to_vec().into())),
        Frame::Array(vec![
            Frame::BulkString(Some(b"field1".to_vec().into())),
            Frame::BulkString(Some(b"value1".to_vec().into())),
            Frame::BulkString(Some(b"field2".to_vec().into())),
            Frame::BulkString(Some(b"value2".to_vec().into())),
        ]),
    ]);

    let result = HScan::parse_response(response).expect("Should parse successfully");
    assert_eq!(result.cursor, 15);
    assert_eq!(result.fields.len(), 2);
    assert_eq!(result.fields[0].0, Bytes::from("field1"));
    assert_eq!(result.fields[0].1, Bytes::from("value1"));
    assert_eq!(result.fields[1].0, Bytes::from("field2"));
    assert_eq!(result.fields[1].1, Bytes::from("value2"));
}
