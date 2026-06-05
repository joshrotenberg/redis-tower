//! Integration tests for EVAL/EVALSHA/SCRIPT and Redis Functions commands.
//!
//! Run all tests in this file serially because FUNCTION FLUSH operates on the
//! global function namespace. Running it concurrently with other function tests
//! causes intermittent failures. Use:
//!
//!   cargo test --test test_scripting --all-features -- --test-threads=1

mod common;

use bytes::Bytes;
use common::conn;
use redis_tower::Frame;
use redis_tower::commands::*;

#[tokio::test]
async fn eval_basic() {
    let mut c = conn().await;

    let result = c.execute(Eval::new("return 42")).await.unwrap();
    assert_eq!(result, Frame::Integer(42));
}

#[tokio::test]
async fn eval_with_keys() {
    let mut c = conn().await;
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
    let mut c = conn().await;

    let script = "return 99";
    let sha = c.execute(ScriptLoad::new(script)).await.unwrap();
    assert!(!sha.is_empty(), "SCRIPT LOAD should return a SHA1 hash");

    let result = c.execute(EvalSha::new(&sha)).await.unwrap();
    assert_eq!(result, Frame::Integer(99));
}

#[tokio::test]
async fn eval_ro_reads_key() {
    let mut c = conn().await;
    let key = "cover2:scripting:eval_ro_reads";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(Set::new(key, "ro-hello")).await.unwrap();

    // Read-only script: EVAL_RO rejects writes but permits GET.
    let result = c
        .execute(EvalRo::new("return redis.call('GET', KEYS[1])").key(key))
        .await
        .unwrap();
    assert_eq!(result, Frame::BulkString(Some(Bytes::from("ro-hello"))));

    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn eval_ro_rejects_writes() {
    let mut c = conn().await;
    let key = "cover2:scripting:eval_ro_rejects";

    c.execute(Del::new(key)).await.unwrap();

    // A write command inside EVAL_RO must be rejected by the server.
    let result = c
        .execute(EvalRo::new("return redis.call('SET', KEYS[1], 'nope')").key(key))
        .await;
    assert!(
        result.is_err(),
        "EVAL_RO should reject a script that issues a write command"
    );

    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn script_load_evalsha_ro() {
    let mut c = conn().await;
    let key = "cover2:scripting:evalsha_ro";

    c.execute(Del::new(key)).await.unwrap();
    c.execute(Set::new(key, "sha-hello")).await.unwrap();

    // Load a read-only script and execute it by SHA via EVALSHA_RO.
    let script = "return redis.call('GET', KEYS[1])";
    let sha = c.execute(ScriptLoad::new(script)).await.unwrap();
    assert!(!sha.is_empty(), "SCRIPT LOAD should return a SHA1 hash");

    let result = c.execute(EvalShaRo::new(&sha).key(key)).await.unwrap();
    assert_eq!(result, Frame::BulkString(Some(Bytes::from("sha-hello"))));

    c.execute(Del::new(key)).await.unwrap();
}

#[tokio::test]
async fn function_load() {
    let mut c = conn().await;

    let code = "#!lua name=mylib_load\nredis.register_function('func_load', function(keys, args) return args[1] end)";
    let lib_name = c.execute(FunctionLoad::new(code).replace()).await.unwrap();
    assert_eq!(lib_name, "mylib_load");

    // Best-effort cleanup: another parallel test (e.g. function_flush) may have
    // already removed this library.
    let _ = c.execute(FunctionDelete::new("mylib_load")).await;
}

#[tokio::test]
async fn fcall() {
    let mut c = conn().await;

    let code = "#!lua name=mylib_fcall\nredis.register_function('func_fcall', function(keys, args) return args[1] end)";
    c.execute(FunctionLoad::new(code).replace()).await.unwrap();

    let result = c
        .execute(FCall::new("func_fcall").arg("hello"))
        .await
        .unwrap();
    assert_eq!(result, Frame::BulkString(Some(Bytes::from("hello"))));

    // Best-effort cleanup.
    let _ = c.execute(FunctionDelete::new("mylib_fcall")).await;
}

#[tokio::test]
async fn fcall_ro() {
    let mut c = conn().await;

    let code = "#!lua name=mylib_fcall_ro\nredis.register_function{function_name='func_fcall_ro', callback=function(keys, args) return args[1] end, flags={'no-writes'}}";
    c.execute(FunctionLoad::new(code).replace()).await.unwrap();

    let result = c
        .execute(FCallRo::new("func_fcall_ro").arg("world"))
        .await
        .unwrap();
    assert_eq!(result, Frame::BulkString(Some(Bytes::from("world"))));

    // Best-effort cleanup.
    let _ = c.execute(FunctionDelete::new("mylib_fcall_ro")).await;
}

#[tokio::test]
async fn function_list() {
    let mut c = conn().await;

    let code = "#!lua name=mylib_list\nredis.register_function('func_list', function(keys, args) return args[1] end)";
    c.execute(FunctionLoad::new(code).replace()).await.unwrap();

    let libs = c
        .execute(FunctionList::new().library("mylib_list"))
        .await
        .unwrap();
    assert!(
        !libs.is_empty(),
        "FUNCTION LIST should return mylib_list after loading"
    );

    // Best-effort cleanup.
    let _ = c.execute(FunctionDelete::new("mylib_list")).await;
}

#[tokio::test]
async fn function_delete() {
    let mut c = conn().await;

    let code = "#!lua name=mylib_delete\nredis.register_function('func_delete', function(keys, args) return args[1] end)";
    let lib_name = c.execute(FunctionLoad::new(code).replace()).await.unwrap();
    assert_eq!(lib_name, "mylib_delete");

    c.execute(FunctionDelete::new("mylib_delete"))
        .await
        .unwrap();

    let libs = c
        .execute(FunctionList::new().library("mylib_delete"))
        .await
        .unwrap();
    assert!(
        libs.is_empty(),
        "FUNCTION LIST should return empty after deleting the library"
    );
}

#[tokio::test]
async fn function_flush() {
    let mut c = conn().await;

    let code = "#!lua name=mylib_flush\nredis.register_function('func_flush', function(keys, args) return args[1] end)";
    c.execute(FunctionLoad::new(code).replace()).await.unwrap();

    c.execute(FunctionFlush::new()).await.unwrap();

    // Verify that the specific library loaded in this test is gone.
    let libs = c
        .execute(FunctionList::new().library("mylib_flush"))
        .await
        .unwrap();
    assert!(
        libs.is_empty(),
        "mylib_flush should be gone after FUNCTION FLUSH"
    );
}

#[tokio::test]
async fn function_stats() {
    let mut c = conn().await;

    let stats = c.execute(FunctionStats::new()).await.unwrap();
    // The response is a complex nested structure; just verify it returns without error.
    assert!(
        !stats.is_empty(),
        "FUNCTION STATS should return a non-empty response"
    );
}

#[tokio::test]
async fn function_dump_restore() {
    let mut c = conn().await;

    let code = "#!lua name=mylib_dump\nredis.register_function('func_dump', function(keys, args) return args[1] end)";
    c.execute(FunctionLoad::new(code).replace()).await.unwrap();

    let payload = c.execute(FunctionDump::new()).await.unwrap();
    assert!(
        !payload.is_empty(),
        "FUNCTION DUMP should return a non-empty payload"
    );

    // Delete only the library this test owns to avoid disrupting parallel tests.
    c.execute(FunctionDelete::new("mylib_dump")).await.unwrap();

    let libs_before = c
        .execute(FunctionList::new().library("mylib_dump"))
        .await
        .unwrap();
    assert!(
        libs_before.is_empty(),
        "mylib_dump should be gone after delete"
    );

    // Restore using REPLACE policy so the payload can be loaded even if mylib_dump
    // somehow exists (e.g. from a concurrent test re-run).
    c.execute(FunctionRestore::new(payload).policy(RestorePolicy::Replace))
        .await
        .unwrap();

    let libs_after = c
        .execute(FunctionList::new().library("mylib_dump"))
        .await
        .unwrap();
    assert!(
        !libs_after.is_empty(),
        "mylib_dump should be present after FUNCTION RESTORE"
    );

    c.execute(FunctionDelete::new("mylib_dump")).await.unwrap();
}
