use redis_tower::commands::{Get, Incr, Ping, Set};
use redis_tower::tcp::TcpConfig;
use redis_tower::{ClientConfig, RedisClient};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("tcp_tuning=debug,redis_tower=debug")
        .init();

    println!("TCP Configuration Examples for redis-tower\n");

    // Example 1: Default configuration (no tuning)
    println!("=== Example 1: Default Configuration ===");
    let client = RedisClient::connect("localhost:6379").await?;
    let start = Instant::now();
    for _ in 0..100 {
        client.call(Ping).await?;
    }
    let default_time = start.elapsed();
    println!("100 PING commands (default): {:?}\n", default_time);

    // Example 2: Low latency configuration (TCP_NODELAY enabled)
    println!("=== Example 2: Low Latency Configuration ===");
    println!("Enables TCP_NODELAY to disable Nagle's algorithm");
    println!("Best for: Real-time applications, gaming, chat systems\n");

    let tcp_config = TcpConfig::low_latency();
    let config = ClientConfig::builder().tcp(tcp_config).build();
    let client = RedisClient::connect_with_config("localhost:6379", config).await?;

    let start = Instant::now();
    for _ in 0..100 {
        client.call(Ping).await?;
    }
    let low_latency_time = start.elapsed();
    println!("100 PING commands (low latency): {:?}", low_latency_time);
    println!(
        "Improvement: {:?} ({:.1}% faster)\n",
        default_time.saturating_sub(low_latency_time),
        (1.0 - low_latency_time.as_secs_f64() / default_time.as_secs_f64()) * 100.0
    );

    // Example 3: Custom TCP configuration
    println!("=== Example 3: Custom TCP Configuration ===");
    let tcp_config = TcpConfig::new()
        .with_nodelay(true) // Disable Nagle's algorithm
        .with_ttl(64) // Set IP TTL
        .with_linger(Some(Duration::from_secs(30))); // Graceful close with timeout

    let config = ClientConfig::builder().tcp(tcp_config).build();
    let client = RedisClient::connect_with_config("localhost:6379", config).await?;

    println!("Testing with custom TCP configuration...");
    client.call(Set::new("tcp_test", "configured")).await?;
    let value: Option<Vec<u8>> = client.call(Get::new("tcp_test")).await?;
    println!(
        "SET/GET test passed: {}",
        String::from_utf8_lossy(&value.unwrap())
    );

    // Example 4: Linux-specific TCP_USER_TIMEOUT
    #[cfg(target_os = "linux")]
    {
        println!("\n=== Example 4: Linux TCP_USER_TIMEOUT ===");
        println!("Sets timeout for unacknowledged data");
        println!("Best for: Detecting connection failures faster\n");

        let tcp_config = TcpConfig::new()
            .with_nodelay(true)
            .with_user_timeout(Duration::from_secs(10)); // 10 second timeout

        let config = ClientConfig::builder().tcp(tcp_config).build();
        let client = RedisClient::connect_with_config("localhost:6379", config).await?;

        println!("Testing with TCP_USER_TIMEOUT set to 10 seconds...");
        for i in 0..5 {
            client.call(Incr::new("counter")).await?;
            println!("Command {} succeeded", i + 1);
        }
        println!("All commands succeeded with user timeout\n");
    }

    // Example 5: When NOT to use TCP_NODELAY
    println!("=== Best Practices ===");
    println!("Enable TCP_NODELAY (disable Nagle) when:");
    println!("  - You need low latency (< 100ms response times)");
    println!("  - You send many small commands");
    println!("  - You're building real-time applications");
    println!();
    println!("Keep Nagle enabled (default) when:");
    println!("  - You send large payloads (> 1KB)");
    println!("  - You batch commands in pipelines");
    println!("  - Network efficiency matters more than latency");
    println!();
    println!("TCP_USER_TIMEOUT (Linux only) when:");
    println!("  - You need fast failure detection");
    println!("  - Default TCP timeout (15-20 min) is too long");
    println!("  - You're in a cloud environment with transient failures");

    Ok(())
}
