//! Example demonstrating Redis Lua scripting (Level 5 commands)
//!
//! This showcases:
//! - EVAL: Execute Lua scripts with dynamic return types
//! - EVALSHA: Cache and reuse scripts by SHA1 hash
//! - SCRIPT LOAD/EXISTS/FLUSH: Script management
//! - RedisValue: Type-safe handling of dynamic script returns

use redis_tower::client::RedisConnection;
use redis_tower::commands::{Del, Eval, EvalSha, ScriptExists, ScriptFlush, ScriptLoad, Set};
use redis_tower::types::RedisValue;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let client = RedisConnection::connect("127.0.0.1:6379").await?;
    println!("Connected to Redis");

    // Clean up from previous runs
    let _ = client.execute(Del::new(vec!["counter".to_string()])).await;
    let _ = client
        .execute(Del::new(vec!["user:1:name".to_string()]))
        .await;
    let _ = client
        .execute(Del::new(vec!["user:1:email".to_string()]))
        .await;

    println!("\n=== Example 1: Simple EVAL returning integer ===");
    let script = r#"
        local key = KEYS[1]
        local increment = tonumber(ARGV[1])
        redis.call('SET', key, 0)
        for i = 1, increment do
            redis.call('INCR', key)
        end
        return redis.call('GET', key)
    "#;

    let result: RedisValue = client
        .execute(Eval::new(script).key("counter").arg(b"5".to_vec()))
        .await?;

    println!("Script result: {:?}", result);
    if let Ok(count) = result.as_bytes() {
        println!(
            "Counter value: {}",
            String::from_utf8_lossy(count.as_ref().unwrap())
        );
    }

    println!("\n=== Example 2: EVAL returning array ===");
    let script = r#"
        local keys = {}
        for i = 1, #KEYS do
            table.insert(keys, KEYS[i])
        end
        return keys
    "#;

    let result: RedisValue = client
        .execute(Eval::new(script).keys(vec!["key1", "key2", "key3"]))
        .await?;

    println!("Script result: {:?}", result);
    if let Ok(arr) = result.as_array() {
        println!("Returned {} keys", arr.len());
    }

    println!("\n=== Example 3: EVAL with multiple return types ===");
    let script = r#"
        return {
            redis.call('SET', KEYS[1], ARGV[1]),
            redis.call('GET', KEYS[1]),
            42,
            "hello",
            {1, 2, 3}
        }
    "#;

    let result: RedisValue = client
        .execute(Eval::new(script).key("user:1:name").arg(b"Alice".to_vec()))
        .await?;

    println!("Complex result: {:?}", result);

    println!("\n=== Example 4: SCRIPT LOAD and EVALSHA ===");
    // Create a useful script for atomic operations
    let atomic_script = r#"
        local key = KEYS[1]
        local field = ARGV[1]
        local value = ARGV[2]

        local old = redis.call('HGET', key, field)
        redis.call('HSET', key, field, value)

        return old
    "#;

    // Calculate SHA1 locally
    let eval = Eval::new(atomic_script);
    let expected_sha = eval.sha1();
    println!("Calculated SHA1: {}", expected_sha);

    // Load script into Redis
    let loaded_sha: String = client.execute(ScriptLoad::new(atomic_script)).await?;
    println!("Loaded SHA1: {}", loaded_sha);
    assert_eq!(expected_sha, loaded_sha);

    // Use EVALSHA instead of EVAL
    let result: RedisValue = client
        .execute(
            EvalSha::new(&loaded_sha)
                .key("user:1")
                .arg(b"email".to_vec())
                .arg(b"alice@example.com".to_vec()),
        )
        .await?;

    println!("EVALSHA result (old value): {:?}", result);

    // Run again to see the old value returned
    let result: RedisValue = client
        .execute(
            EvalSha::new(&loaded_sha)
                .key("user:1")
                .arg(b"email".to_vec())
                .arg(b"alice@newdomain.com".to_vec()),
        )
        .await?;

    println!("EVALSHA result (old value): {:?}", result);

    println!("\n=== Example 5: SCRIPT EXISTS ===");
    let exists: Vec<bool> = client
        .execute(
            ScriptExists::new()
                .sha1(&loaded_sha)
                .sha1("nonexistent1234567890abcdef1234567890abcd"),
        )
        .await?;

    println!("Script exists check: {:?}", exists);
    assert!(exists[0]); // Our loaded script exists
    assert!(!exists[1]); // Fake SHA doesn't exist

    println!("\n=== Example 6: Conditional logic in Lua ===");
    let conditional_script = r#"
        local key = KEYS[1]
        local threshold = tonumber(ARGV[1])

        local current = redis.call('GET', key)
        if current == false then
            current = 0
        else
            current = tonumber(current)
        end

        if current >= threshold then
            return {1, current, "threshold reached"}
        else
            redis.call('INCR', key)
            return {0, current + 1, "incremented"}
        end
    "#;

    client
        .execute(Set::new("threshold_test", b"0".to_vec()))
        .await?;

    // Run script multiple times
    for i in 1..=6 {
        let result: RedisValue = client
            .execute(
                Eval::new(conditional_script)
                    .key("threshold_test")
                    .arg(b"5".to_vec()),
            )
            .await?;
        println!("Iteration {}: {:?}", i, result);
    }

    println!("\n=== Example 7: Error handling - NOSCRIPT ===");
    match client
        .execute(EvalSha::new("nonexistent1234567890abcdef1234567890abcd"))
        .await
    {
        Ok(result) => println!("Unexpected success: {:?}", result),
        Err(e) => println!("Expected NOSCRIPT error: {}", e),
    }

    println!("\n=== Example 8: SCRIPT FLUSH ===");
    let flush_result: String = client.execute(ScriptFlush::new()).await?;
    println!("SCRIPT FLUSH result: {}", flush_result);

    // Verify script was flushed
    let exists: Vec<bool> = client
        .execute(ScriptExists::new().sha1(&loaded_sha))
        .await?;
    println!("Script exists after flush: {:?}", exists);
    assert!(!exists[0]); // Script should be gone

    println!("\n=== Example 9: Complex data structures ===");
    let complex_script = r#"
        -- Build a complex nested structure
        local result = {}
        result.status = "ok"
        result.count = 42
        result.items = {"apple", "banana", "cherry"}
        result.nested = {
            x = 10,
            y = 20
        }
        return result
    "#;

    let result: RedisValue = client.execute(Eval::new(complex_script)).await?;
    println!("Complex structure: {:#?}", result);

    println!("\n=== Example 10: Rate limiting with Lua ===");
    let rate_limit_script = r#"
        local key = KEYS[1]
        local limit = tonumber(ARGV[1])
        local window = tonumber(ARGV[2])

        local current = redis.call('GET', key)
        if current == false then
            redis.call('SETEX', key, window, 1)
            return {1, limit - 1}
        else
            current = tonumber(current)
            if current < limit then
                redis.call('INCR', key)
                return {1, limit - current - 1}
            else
                local ttl = redis.call('TTL', key)
                return {0, ttl}
            end
        end
    "#;

    println!("Simulating rate limiting (5 requests per 10 seconds):");
    for i in 1..=7 {
        let result: RedisValue = client
            .execute(
                Eval::new(rate_limit_script)
                    .key("rate:user:123")
                    .arg(b"5".to_vec())
                    .arg(b"10".to_vec()),
            )
            .await?;

        if let Ok(arr) = result.as_array()
            && let (Ok(allowed), Ok(remaining)) = (arr[0].as_i64(), arr[1].as_i64())
        {
            if allowed == 1 {
                println!("  Request {}: ALLOWED (remaining: {})", i, remaining);
            } else {
                println!(
                    "  Request {}: RATE LIMITED (retry in {} seconds)",
                    i, remaining
                );
            }
        }
    }

    println!("\n=== All scripting examples completed! ===");
    Ok(())
}
