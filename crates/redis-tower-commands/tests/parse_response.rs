//! Tests for parse_response error branches using MockConnection.
//!
//! These test the UnexpectedResponse paths that can't be triggered
//! against a real Redis server.

use bytes::Bytes;
use redis_test_harness::mock::MockConnection;
use redis_tower_commands::*;
use redis_tower_core::Frame;

// -- Strings --

#[test]
fn get_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42)); // GET expects BulkString
    let result = mock.execute(Get::new("key"));
    assert!(result.is_err());
}

#[test]
fn set_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42)); // SET expects SimpleString OK
    let result = mock.execute(Set::new("key", "val"));
    assert!(result.is_err());
}

#[test]
fn incr_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK"))); // INCR expects Integer
    let result = mock.execute(Incr::new("key"));
    assert!(result.is_err());
}

#[test]
fn append_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK"))); // APPEND expects Integer
    let result = mock.execute(Append::new("key", "val"));
    assert!(result.is_err());
}

#[test]
fn mget_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42)); // MGET expects Array
    let result = mock.execute(MGet::new(["a", "b"]));
    assert!(result.is_err());
}

#[test]
fn mset_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42)); // MSET expects OK
    let result = mock.execute(MSet::new([("a", "1")]));
    assert!(result.is_err());
}

// -- Keys --

#[test]
fn del_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK"))); // DEL expects Integer
    let result = mock.execute(Del::new("key"));
    assert!(result.is_err());
}

#[test]
fn exists_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(Exists::new("key"));
    assert!(result.is_err());
}

#[test]
fn expire_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(Expire::new("key", 60));
    assert!(result.is_err());
}

#[test]
fn ttl_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(Ttl::new("key"));
    assert!(result.is_err());
}

#[test]
fn rename_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(1));
    let result = mock.execute(Rename::new("a", "b"));
    assert!(result.is_err());
}

#[test]
fn type_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(1)); // TYPE expects SimpleString
    let result = mock.execute(Type::new("key"));
    assert!(result.is_err());
}

// -- Server --

#[test]
fn ping_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42)); // PING expects SimpleString or BulkString
    let result = mock.execute(Ping::new());
    assert!(result.is_err());
}

#[test]
fn flushdb_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42)); // FLUSHDB expects OK
    let result = mock.execute(FlushDb::new());
    assert!(result.is_err());
}

#[test]
fn dbsize_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK"))); // DBSIZE expects Integer
    let result = mock.execute(DbSize::new());
    assert!(result.is_err());
}

#[test]
fn select_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(1)); // SELECT expects OK
    let result = mock.execute(Select::new(0));
    assert!(result.is_err());
}

// -- Hashes --

#[test]
fn hget_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42));
    let result = mock.execute(HGet::new("key", "field"));
    assert!(result.is_err());
}

#[test]
fn hset_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(HSet::new("key", "f", "v"));
    assert!(result.is_err());
}

#[test]
fn hdel_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(HDel::new("key", "field"));
    assert!(result.is_err());
}

#[test]
fn hexists_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(HExists::new("key", "field"));
    assert!(result.is_err());
}

#[test]
fn hgetall_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42));
    let result = mock.execute(HGetAll::new("key"));
    assert!(result.is_err());
}

#[test]
fn hgetall_odd_length() {
    let mut mock = MockConnection::new();
    // HGetAll expects even number of elements (key-value pairs).
    mock.enqueue(Frame::Array(Some(vec![
        Frame::BulkString(Some(Bytes::from("field1"))),
        Frame::BulkString(Some(Bytes::from("value1"))),
        Frame::BulkString(Some(Bytes::from("orphan"))),
    ])));
    let result = mock.execute(HGetAll::new("key"));
    assert!(result.is_err());
}

#[test]
fn hincrby_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(HIncrBy::new("key", "field", 1));
    assert!(result.is_err());
}

#[test]
fn hkeys_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42));
    let result = mock.execute(HKeys::new("key"));
    assert!(result.is_err());
}

#[test]
fn hvals_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42));
    let result = mock.execute(HVals::new("key"));
    assert!(result.is_err());
}

#[test]
fn hlen_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(HLen::new("key"));
    assert!(result.is_err());
}

// -- Lists --

#[test]
fn lpush_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(LPush::new("key", "val"));
    assert!(result.is_err());
}

#[test]
fn lpop_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42));
    let result = mock.execute(LPop::new("key"));
    assert!(result.is_err());
}

#[test]
fn lrange_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42));
    let result = mock.execute(LRange::new("key", 0, -1));
    assert!(result.is_err());
}

#[test]
fn llen_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(LLen::new("key"));
    assert!(result.is_err());
}

#[test]
fn lset_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42));
    let result = mock.execute(LSet::new("key", 0, "val"));
    assert!(result.is_err());
}

// -- Sets --

#[test]
fn sadd_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(SAdd::new("key", "member"));
    assert!(result.is_err());
}

#[test]
fn smembers_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42));
    let result = mock.execute(SMembers::new("key"));
    assert!(result.is_err());
}

#[test]
fn sismember_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(SIsMember::new("key", "member"));
    assert!(result.is_err());
}

#[test]
fn scard_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(SCard::new("key"));
    assert!(result.is_err());
}

// -- Sorted Sets --

#[test]
fn zadd_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(ZAdd::new("key").member(1.0, "a"));
    assert!(result.is_err());
}

#[test]
fn zscore_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42)); // ZScore expects BulkString or Null
    let result = mock.execute(ZScore::new("key", "member"));
    assert!(result.is_err());
}

#[test]
fn zcard_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(ZCard::new("key"));
    assert!(result.is_err());
}

#[test]
fn zincrby_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42)); // ZINCRBY expects BulkString (float)
    let result = mock.execute(ZIncrBy::new("key", 1.0, "member"));
    assert!(result.is_err());
}

#[test]
fn zrank_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("OK")));
    let result = mock.execute(ZRank::new("key", "member"));
    assert!(result.is_err());
}

#[test]
fn zrange_wrong_type() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42));
    let result = mock.execute(ZRange::new("key", 0, -1));
    assert!(result.is_err());
}

// -- Redis error responses --

#[test]
fn redis_error_is_propagated() {
    let mut mock = MockConnection::new();
    mock.enqueue_error("ERR some error");
    let result = mock.execute(Get::new("key"));
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("some error"));
}

// -- Empty queue --

#[test]
fn empty_queue_returns_connection_closed() {
    let mut mock = MockConnection::new();
    let result = mock.execute(Ping::new());
    assert!(result.is_err());
}

// -- Successful parsing through mock --

#[test]
fn mock_get_success() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::BulkString(Some(Bytes::from("hello"))));
    let result: Option<Bytes> = mock.execute(Get::new("key")).unwrap();
    assert_eq!(result, Some(Bytes::from("hello")));
}

#[test]
fn mock_get_null() {
    let mut mock = MockConnection::new();
    mock.enqueue_null();
    let result: Option<Bytes> = mock.execute(Get::new("key")).unwrap();
    assert_eq!(result, None);
}

#[test]
fn mock_incr_success() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Integer(42));
    let result: i64 = mock.execute(Incr::new("key")).unwrap();
    assert_eq!(result, 42);
}

#[test]
fn mock_ping_success() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::SimpleString(Bytes::from("PONG")));
    let result: String = mock.execute(Ping::new()).unwrap();
    assert_eq!(result, "PONG");
}

// -- RESP3 frame variants --

#[test]
fn resp3_expire_boolean() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Boolean(true));
    assert!(mock.execute(Expire::new("key", 60)).unwrap());

    mock.enqueue(Frame::Boolean(false));
    assert!(!mock.execute(Expire::new("key", 60)).unwrap());
}

#[test]
fn resp3_hexists_boolean() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Boolean(true));
    assert!(mock.execute(HExists::new("key", "field")).unwrap());

    mock.enqueue(Frame::Boolean(false));
    assert!(!mock.execute(HExists::new("key", "field")).unwrap());
}

#[test]
fn resp3_sismember_boolean() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Boolean(true));
    assert!(mock.execute(SIsMember::new("key", "member")).unwrap());

    mock.enqueue(Frame::Boolean(false));
    assert!(!mock.execute(SIsMember::new("key", "member")).unwrap());
}

#[test]
fn resp3_hgetall_map() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Map(vec![
        (
            Frame::BulkString(Some(Bytes::from("field1"))),
            Frame::BulkString(Some(Bytes::from("value1"))),
        ),
        (
            Frame::BulkString(Some(Bytes::from("field2"))),
            Frame::BulkString(Some(Bytes::from("value2"))),
        ),
    ]));
    let pairs: Vec<(Bytes, Bytes)> = mock.execute(HGetAll::new("key")).unwrap();
    assert_eq!(pairs.len(), 2);
    assert!(pairs.contains(&(Bytes::from("field1"), Bytes::from("value1"))));
    assert!(pairs.contains(&(Bytes::from("field2"), Bytes::from("value2"))));
}

#[test]
fn resp3_zscore_double() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Double(42.5));
    let score: Option<f64> = mock.execute(ZScore::new("key", "member")).unwrap();
    assert_eq!(score, Some(42.5));
}

#[test]
fn resp3_zincrby_double() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Double(15.5));
    let score: f64 = mock.execute(ZIncrBy::new("key", 5.5, "member")).unwrap();
    assert!((score - 15.5).abs() < f64::EPSILON);
}

#[test]
fn resp3_smembers_set() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Set(vec![
        Frame::BulkString(Some(Bytes::from("a"))),
        Frame::BulkString(Some(Bytes::from("b"))),
        Frame::BulkString(Some(Bytes::from("c"))),
    ]));
    let members: Vec<Bytes> = mock.execute(SMembers::new("key")).unwrap();
    assert_eq!(members.len(), 3);
}

#[test]
fn resp3_sinter_set() {
    let mut mock = MockConnection::new();
    mock.enqueue(Frame::Set(vec![
        Frame::BulkString(Some(Bytes::from("b"))),
        Frame::BulkString(Some(Bytes::from("c"))),
    ]));
    let members: Vec<Bytes> = mock.execute(SInter::new("key")).unwrap();
    assert_eq!(members.len(), 2);
}
