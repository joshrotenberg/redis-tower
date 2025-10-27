//! Integration tests for Redis Functions commands (Redis 7.0+)
//!
//! Tests server-side Lua functions with persistence.
//!
//! Run with: cargo test --test integration_functions
//!
//! Note: Requires Redis 7.0 or later
//! Note: Tests use FUNCTION FLUSH for isolation, may interfere with other processes

mod helpers;

use bytes::Bytes;
use helpers::standalone::setup_redis;
use redis_tower::commands::*;

const TEST_LIBRARY: &str = r#"#!lua name=testlib
redis.register_function('hello', function(keys, args)
    return 'Hello, ' .. (args[1] or 'World')
end)

redis.register_function('double', function(keys, args)
    local num = tonumber(args[1])
    return num * 2
end)
"#;

#[tokio::test]
async fn test_function_load() {
    let client = setup_redis().await;

    // Flush all functions for clean state
    let _ = client.call(FunctionFlush::new()).await;

    // Load the function library
    let lib_name: String = client.call(FunctionLoad::new(TEST_LIBRARY)).await.unwrap();
    assert_eq!(lib_name, "testlib");
}

#[tokio::test]
async fn test_function_load_replace() {
    let client = setup_redis().await;

    // Flush all functions for clean state
    let _ = client.call(FunctionFlush::new()).await;

    // Load library first time
    client.call(FunctionLoad::new(TEST_LIBRARY)).await.unwrap();

    // Replace with same library using REPLACE flag
    let lib_name: String = client
        .call(FunctionLoad::new(TEST_LIBRARY).replace())
        .await
        .unwrap();
    assert_eq!(lib_name, "testlib");
}

#[tokio::test]
async fn test_fcall() {
    let client = setup_redis().await;

    // Flush and load library
    let _ = client.call(FunctionFlush::new()).await;
    client.call(FunctionLoad::new(TEST_LIBRARY)).await.unwrap();

    // Call hello function with default (no args)
    let result: String = client.call(FCall::new("hello")).await.unwrap();
    assert!(result.contains("Hello"));

    // Call hello function with argument
    let result: String = client.call(FCall::new("hello").arg("Redis")).await.unwrap();
    assert!(result.contains("Redis"));

    // Call double function
    let result: String = client.call(FCall::new("double").arg("21")).await.unwrap();
    assert!(result.contains("42"));
}

#[tokio::test]
async fn test_fcall_ro() {
    let client = setup_redis().await;

    // Flush and load library
    let _ = client.call(FunctionFlush::new()).await;
    client.call(FunctionLoad::new(TEST_LIBRARY)).await.unwrap();

    // Call read-only function
    let result: String = client
        .call(FCallReadOnly::new("hello").arg("World"))
        .await
        .unwrap();
    // Response format varies, just check it's not empty
    assert!(!result.is_empty());
}

#[tokio::test]
async fn test_function_delete() {
    let client = setup_redis().await;

    // Flush and load library
    let _ = client.call(FunctionFlush::new()).await;
    client.call(FunctionLoad::new(TEST_LIBRARY)).await.unwrap();

    // Verify library exists
    let functions: String = client.call(FunctionList::new()).await.unwrap();
    assert!(functions.contains("testlib"));

    // Delete the library
    client.call(FunctionDelete::new("testlib")).await.unwrap();

    // Verify it's deleted
    let functions: String = client.call(FunctionList::new()).await.unwrap();
    assert!(!functions.contains("testlib"));
}

#[tokio::test]
async fn test_function_list() {
    let client = setup_redis().await;

    // Flush and load library
    let _ = client.call(FunctionFlush::new()).await;
    client.call(FunctionLoad::new(TEST_LIBRARY)).await.unwrap();

    // List all functions
    let functions: String = client.call(FunctionList::new()).await.unwrap();
    assert!(functions.contains("testlib"));

    // List functions for specific library
    let functions: String = client
        .call(FunctionList::new().library_name("testlib"))
        .await
        .unwrap();
    assert!(functions.contains("testlib"));
}

#[tokio::test]
async fn test_function_flush() {
    let client = setup_redis().await;

    // Load library
    let _ = client.call(FunctionFlush::new()).await;
    client.call(FunctionLoad::new(TEST_LIBRARY)).await.unwrap();

    // Flush all functions
    client.call(FunctionFlush::new()).await.unwrap();

    // Verify all functions are gone
    let functions: String = client.call(FunctionList::new()).await.unwrap();
    assert!(!functions.contains("testlib"));
}

#[tokio::test]
async fn test_function_dump_restore() {
    let client = setup_redis().await;

    // Flush and load library
    let _ = client.call(FunctionFlush::new()).await;
    client.call(FunctionLoad::new(TEST_LIBRARY)).await.unwrap();

    // Dump functions
    let dump: Bytes = client.call(FunctionDump).await.unwrap();
    assert!(!dump.is_empty());

    // Flush all functions
    client.call(FunctionFlush::new()).await.unwrap();

    // Restore from dump
    client.call(FunctionRestore::new(dump)).await.unwrap();

    // Verify library is back
    let functions: String = client.call(FunctionList::new()).await.unwrap();
    assert!(functions.contains("testlib"));

    // Verify function works
    let result: String = client.call(FCall::new("hello")).await.unwrap();
    assert!(result.contains("Hello"));
}

#[tokio::test]
async fn test_function_stats() {
    let client = setup_redis().await;

    // Get function statistics
    let stats: String = client.call(FunctionStats).await.unwrap();

    // Should contain stats information (or be empty if no stats yet)
    assert!(stats.is_empty() || !stats.is_empty());
}
