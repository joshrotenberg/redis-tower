//! Integration tests for Redis Lua scripting commands
//!
//! These tests cover Redis scripting functionality:
//! - EVAL (execute Lua script)
//! - EVALSHA (execute cached script)
//! - SCRIPT LOAD (cache script)
//! - SCRIPT EXISTS (check if script is cached)
//! - SCRIPT FLUSH (clear script cache)
//!
//! Run with: cargo test --test integration_scripting

use redis_tower::client::RedisClient;
use redis_tower::commands::*;
use redis_tower::types::RedisValue;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::redis::Redis;

/// Helper to create a Redis client connected to a testcontainer
async fn setup_redis() -> RedisClient {
    let container = Redis::default()
        .start()
        .await
        .expect("Failed to start Redis container");

    let host = container.get_host().await.expect("Failed to get host");
    let port = container
        .get_host_port_ipv4(6379)
        .await
        .expect("Failed to get port");

    let client = RedisClient::connect(&format!("{}:{}", host, port))
        .await
        .expect("Failed to connect to Redis");

    // Keep container alive by leaking it (tests are short-lived)
    std::mem::forget(container);

    client
}

#[tokio::test]
async fn test_eval_simple_return() {
    let client = setup_redis().await;

    // Simple script that returns a value
    let script = "return 42";
    let result: RedisValue = client.call(Eval::new(script)).await.unwrap();

    match result {
        RedisValue::Integer(n) => assert_eq!(n, 42),
        _ => panic!("Expected Integer, got {:?}", result),
    }
}

#[tokio::test]
async fn test_eval_with_keys() {
    let client = setup_redis().await;

    // Set a key first
    client.call(Set::new("lua_key", "lua_value")).await.unwrap();

    // Script that reads a key
    let script = "return redis.call('GET', KEYS[1])";
    let result: RedisValue = client.call(Eval::new(script).key("lua_key")).await.unwrap();

    match result {
        RedisValue::BulkString(b) => assert_eq!(b.as_ref(), b"lua_value"),
        _ => panic!("Expected BulkString, got {:?}", result),
    }
}

#[tokio::test]
async fn test_eval_with_args() {
    let client = setup_redis().await;

    // Script that uses ARGV
    let script = "return ARGV[1] .. ' ' .. ARGV[2]";
    let result: RedisValue = client
        .call(Eval::new(script).arg("Hello").arg("World"))
        .await
        .unwrap();

    match result {
        RedisValue::BulkString(b) => {
            assert_eq!(String::from_utf8_lossy(&b), "Hello World");
        }
        _ => panic!("Expected BulkString, got {:?}", result),
    }
}

#[tokio::test]
async fn test_eval_set_and_get() {
    let client = setup_redis().await;

    // Script that sets and returns a value
    let script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])";
    let result: RedisValue = client
        .call(Eval::new(script).key("script_key").arg("script_value"))
        .await
        .unwrap();

    match result {
        RedisValue::BulkString(b) => assert_eq!(b.as_ref(), b"script_value"),
        _ => panic!("Expected BulkString, got {:?}", result),
    }

    // Verify it was actually set
    let value: Option<bytes::Bytes> = client.call(Get::new("script_key")).await.unwrap();
    assert_eq!(
        value.as_ref().map(|b| b.as_ref()),
        Some(b"script_value".as_ref())
    );
}

#[tokio::test]
async fn test_eval_arithmetic() {
    let client = setup_redis().await;

    // Script that does arithmetic
    let script =
        "local sum = 0; for i, v in ipairs(ARGV) do sum = sum + tonumber(v) end; return sum";
    let result: RedisValue = client
        .call(Eval::new(script).arg("10").arg("20").arg("30"))
        .await
        .unwrap();

    match result {
        RedisValue::Integer(n) => assert_eq!(n, 60),
        _ => panic!("Expected Integer, got {:?}", result),
    }
}

#[tokio::test]
async fn test_eval_return_array() {
    let client = setup_redis().await;

    // Script that returns an array
    let script = "return {1, 2, 3, 'four', 'five'}";
    let result: RedisValue = client.call(Eval::new(script)).await.unwrap();

    match result {
        RedisValue::Array(arr) => {
            assert_eq!(arr.len(), 5);
            match &arr[0] {
                RedisValue::Integer(n) => assert_eq!(*n, 1),
                _ => panic!("Expected Integer"),
            }
            match &arr[3] {
                RedisValue::BulkString(b) => assert_eq!(b.as_ref(), b"four"),
                _ => panic!("Expected BulkString"),
            }
        }
        _ => panic!("Expected Array, got {:?}", result),
    }
}

#[tokio::test]
async fn test_script_load_and_evalsha() {
    let client = setup_redis().await;

    // Load a script
    let script = "return 'Script executed successfully'";
    let sha: String = client.call(ScriptLoad::new(script)).await.unwrap();

    // SHA should be 40 characters (SHA1 hash)
    assert_eq!(sha.len(), 40);

    // Execute the cached script using its SHA
    let result: RedisValue = client.call(EvalSha::new(&sha)).await.unwrap();

    match result {
        RedisValue::BulkString(b) => {
            assert_eq!(String::from_utf8_lossy(&b), "Script executed successfully");
        }
        _ => panic!("Expected BulkString, got {:?}", result),
    }
}

#[tokio::test]
async fn test_script_exists() {
    let client = setup_redis().await;

    // Load a script
    let script = "return 42";
    let sha: String = client.call(ScriptLoad::new(script)).await.unwrap();

    // Check if script exists
    let exists: Vec<bool> = client
        .call(ScriptExists::new().sha1(sha.clone()))
        .await
        .unwrap();

    assert_eq!(exists.len(), 1);
    assert_eq!(exists[0], true);

    // Check non-existent script
    let fake_sha = "0".repeat(40);
    let exists: Vec<bool> = client
        .call(ScriptExists::new().sha1(fake_sha))
        .await
        .unwrap();

    assert_eq!(exists.len(), 1);
    assert_eq!(exists[0], false);
}

#[tokio::test]
async fn test_script_exists_multiple() {
    let client = setup_redis().await;

    // Load two scripts
    let sha1: String = client.call(ScriptLoad::new("return 1")).await.unwrap();
    let sha2: String = client.call(ScriptLoad::new("return 2")).await.unwrap();
    let fake_sha = "0".repeat(40);

    // Check all three
    let exists: Vec<bool> = client
        .call(ScriptExists::new().sha1(sha1).sha1(sha2).sha1(fake_sha))
        .await
        .unwrap();

    assert_eq!(exists.len(), 3);
    assert_eq!(exists[0], true);
    assert_eq!(exists[1], true);
    assert_eq!(exists[2], false);
}

#[tokio::test]
async fn test_script_flush() {
    let client = setup_redis().await;

    // Load a script
    let script = "return 'cached'";
    let sha: String = client.call(ScriptLoad::new(script)).await.unwrap();

    // Verify it exists
    let exists: Vec<bool> = client
        .call(ScriptExists::new().sha1(sha.clone()))
        .await
        .unwrap();
    assert_eq!(exists[0], true);

    // Flush all scripts
    client.call(ScriptFlush::new()).await.unwrap();

    // Script should no longer exist
    let exists: Vec<bool> = client
        .call(ScriptExists::new().sha1(sha.clone()))
        .await
        .unwrap();
    assert_eq!(exists[0], false);

    // EVALSHA should fail
    let result: Result<RedisValue, _> = client.call(EvalSha::new(&sha)).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_eval_incr_counter() {
    let client = setup_redis().await;

    // Set initial counter
    client.call(Set::new("lua_counter", "0")).await.unwrap();

    // Script that increments and returns new value
    let script = "return redis.call('INCR', KEYS[1])";

    let result: RedisValue = client
        .call(Eval::new(script).key("lua_counter"))
        .await
        .unwrap();

    match result {
        RedisValue::Integer(n) => assert_eq!(n, 1),
        _ => panic!("Expected Integer"),
    }

    // Run it again
    let result: RedisValue = client
        .call(Eval::new(script).key("lua_counter"))
        .await
        .unwrap();

    match result {
        RedisValue::Integer(n) => assert_eq!(n, 2),
        _ => panic!("Expected Integer"),
    }
}

#[tokio::test]
async fn test_eval_conditional_logic() {
    let client = setup_redis().await;

    // Script with conditional
    let script = r#"
        local val = tonumber(ARGV[1])
        if val > 10 then
            return "high"
        elseif val > 5 then
            return "medium"
        else
            return "low"
        end
    "#;

    let result: RedisValue = client.call(Eval::new(script).arg("15")).await.unwrap();
    match result {
        RedisValue::BulkString(b) => assert_eq!(b.as_ref(), b"high"),
        _ => panic!("Expected BulkString"),
    }

    let result: RedisValue = client.call(Eval::new(script).arg("7")).await.unwrap();
    match result {
        RedisValue::BulkString(b) => assert_eq!(b.as_ref(), b"medium"),
        _ => panic!("Expected BulkString"),
    }

    let result: RedisValue = client.call(Eval::new(script).arg("3")).await.unwrap();
    match result {
        RedisValue::BulkString(b) => assert_eq!(b.as_ref(), b"low"),
        _ => panic!("Expected BulkString"),
    }
}

#[tokio::test]
async fn test_evalsha_with_keys_and_args() {
    let client = setup_redis().await;

    // Load script that uses both KEYS and ARGV
    let script = "redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])";
    let sha: String = client.call(ScriptLoad::new(script)).await.unwrap();

    // Execute with SHA
    let result: RedisValue = client
        .call(EvalSha::new(&sha).key("sha_key").arg("sha_value"))
        .await
        .unwrap();

    match result {
        RedisValue::BulkString(b) => assert_eq!(b.as_ref(), b"sha_value"),
        _ => panic!("Expected BulkString"),
    }
}

#[tokio::test]
async fn test_script_load_deterministic_sha() {
    let client = setup_redis().await;

    // Same script should always produce same SHA
    let script = "return 'deterministic'";

    let sha1: String = client.call(ScriptLoad::new(script)).await.unwrap();
    let sha2: String = client.call(ScriptLoad::new(script)).await.unwrap();

    assert_eq!(sha1, sha2);
    assert_eq!(sha1.len(), 40);
}

#[tokio::test]
async fn test_eval_multi_command() {
    let client = setup_redis().await;

    // Script that executes multiple Redis commands
    let script = r#"
        redis.call('SET', 'k1', 'v1')
        redis.call('SET', 'k2', 'v2')
        redis.call('SET', 'k3', 'v3')
        return redis.call('MGET', 'k1', 'k2', 'k3')
    "#;

    let result: RedisValue = client.call(Eval::new(script)).await.unwrap();

    match result {
        RedisValue::Array(arr) => {
            assert_eq!(arr.len(), 3);
            match &arr[0] {
                RedisValue::BulkString(b) => assert_eq!(b.as_ref(), b"v1"),
                _ => panic!("Expected BulkString"),
            }
        }
        _ => panic!("Expected Array"),
    }
}
