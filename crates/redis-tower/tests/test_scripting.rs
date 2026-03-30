mod common;

use bytes::Bytes;
use common::conn;
use redis_tower::Frame;
use redis_tower::commands::*;

#[tokio::test]
async fn eval_basic() {
    let c = conn().await;

    let result = c.execute(Eval::new("return 42")).await.unwrap();
    assert_eq!(result, Frame::Integer(42));
}

#[tokio::test]
async fn eval_with_keys() {
    let c = conn().await;
    let key = "cover2:scripting:eval_keys";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(Set::new(key, "hello")).await.unwrap();

    let result = c
        .execute(Eval::new("return redis.call('GET', KEYS[1])").key(key))
        .await
        .unwrap();
    assert_eq!(result, Frame::BulkString(Some(Bytes::from("hello"))));
}

#[tokio::test]
async fn script_load_evalsha() {
    let c = conn().await;

    let script = "return 99";
    let sha = c.execute(ScriptLoad::new(script)).await.unwrap();
    assert!(!sha.is_empty(), "SCRIPT LOAD should return a SHA1 hash");

    let result = c.execute(EvalSha::new(&sha)).await.unwrap();
    assert_eq!(result, Frame::Integer(99));
}
