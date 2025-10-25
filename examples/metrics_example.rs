use redis_tower::client::ResilientRedisClient;
use redis_tower::commands::{Get, Incr, Ping, Set};
use redis_tower::config::ClientConfig;
use redis_tower::metrics::{MetricsCollector, MetricsConfig};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Redis Tower Metrics Example ===\n");

    // Example 1: Default metrics (enabled)
    println!("Example 1: Default Metrics Collection");
    println!("--------------------------------------");

    let metrics = MetricsCollector::new();
    let config = ClientConfig::builder().metrics(metrics.clone()).build();

    match ResilientRedisClient::connect_with_full_config("localhost:6379", config).await {
        Ok(client) => {
            println!("Connected to Redis with metrics enabled\n");

            // Execute some commands
            let _: String = client.call(Ping::new()).await?;
            let _: () = client.call(Set::new("metrics_test", "hello")).await?;
            let _: Option<bytes::Bytes> = client.call(Get::new("metrics_test")).await?;
            let _: i64 = client.call(Incr::new("metrics_counter")).await?;
            let _: i64 = client.call(Incr::new("metrics_counter")).await?;

            // Get metrics snapshot
            let snapshot = metrics.snapshot();
            println!("Metrics after 5 commands:");
            println!("  Total commands: {}", snapshot.commands.total_commands);
            println!(
                "  Average duration: {:?}",
                snapshot.commands.average_duration()
            );
            println!(
                "  Connections created: {}",
                snapshot.connections.connections_created
            );
            println!(
                "  Connections active: {}",
                snapshot.connections.connections_active
            );
            println!("  Total errors: {}", snapshot.total_errors());
        }
        Err(e) => println!("Failed to connect: {}", e),
    }

    println!("\n\nExample 2: Detailed Metrics with Multiple Operations");
    println!("-----------------------------------------------------");

    let metrics = MetricsCollector::with_config(MetricsConfig::all());
    let config = ClientConfig::builder().metrics(metrics.clone()).build();

    match ResilientRedisClient::connect_with_full_config("localhost:6379", config).await {
        Ok(client) => {
            println!("Running 100 commands...\n");

            for i in 0..100 {
                let key = format!("bench_key_{}", i);
                let _: () = client.call(Set::new(&key, "value")).await?;
                let _: Option<bytes::Bytes> = client.call(Get::new(&key)).await?;
            }

            let snapshot = metrics.snapshot();
            println!("Metrics after 200 commands:");
            println!("  Total commands: {}", snapshot.commands.total_commands);
            println!(
                "  Average duration: {:?}",
                snapshot.commands.average_duration()
            );
            println!(
                "  Connections created: {}",
                snapshot.connections.connections_created
            );
            println!(
                "  Connections active: {}",
                snapshot.connections.connections_active
            );
            println!("  Reconnections: {}", snapshot.connections.reconnections);

            println!("\nError breakdown:");
            println!("  Connection errors: {}", snapshot.errors.connection_errors);
            println!("  Timeout errors: {}", snapshot.errors.timeout_errors);
            println!("  Parse errors: {}", snapshot.errors.parse_errors);
            println!("  Redis errors: {}", snapshot.errors.redis_errors);
            println!("  Other errors: {}", snapshot.errors.other_errors);
        }
        Err(e) => println!("Failed to connect: {}", e),
    }

    println!("\n\nExample 3: Metrics with Reset");
    println!("------------------------------");

    let metrics = MetricsCollector::new();
    let config = ClientConfig::builder().metrics(metrics.clone()).build();

    match ResilientRedisClient::connect_with_full_config("localhost:6379", config).await {
        Ok(client) => {
            // First batch
            for _ in 0..10 {
                let _: String = client.call(Ping::new()).await?;
            }

            let snapshot = metrics.snapshot();
            println!(
                "After 10 commands: {} total",
                snapshot.commands.total_commands
            );

            // Reset metrics
            metrics.reset();
            println!("Metrics reset!\n");

            // Second batch
            for _ in 0..5 {
                let _: String = client.call(Ping::new()).await?;
            }

            let snapshot = metrics.snapshot();
            println!(
                "After reset + 5 commands: {} total",
                snapshot.commands.total_commands
            );
        }
        Err(e) => println!("Failed to connect: {}", e),
    }

    println!("\n\nExample 4: No Metrics (Disabled)");
    println!("---------------------------------");

    let config = ClientConfig::builder().no_metrics().build();

    match ResilientRedisClient::connect_with_full_config("localhost:6379", config).await {
        Ok(client) => {
            let _: String = client.call(Ping::new()).await?;
            let _: () = client.call(Set::new("no_metrics", "test")).await?;
            println!("Commands executed without metrics collection");
        }
        Err(e) => println!("Failed to connect: {}", e),
    }

    println!("\n\nExample 5: Shared Metrics Across Clients");
    println!("-----------------------------------------");

    let metrics = MetricsCollector::new();

    // Create two clients sharing the same metrics collector
    let config1 = ClientConfig::builder().metrics(metrics.clone()).build();
    let config2 = ClientConfig::builder().metrics(metrics.clone()).build();

    let client1 = ResilientRedisClient::connect_with_full_config("localhost:6379", config1).await?;
    let client2 = ResilientRedisClient::connect_with_full_config("localhost:6379", config2).await?;

    // Execute commands from both clients
    let _: () = client1.call(Set::new("client1_key", "value1")).await?;
    let _: () = client2.call(Set::new("client2_key", "value2")).await?;

    let snapshot = metrics.snapshot();
    println!("Shared metrics from 2 clients:");
    println!("  Total commands: {}", snapshot.commands.total_commands);
    println!(
        "  Connections created: {}",
        snapshot.connections.connections_created
    );
    println!(
        "  Connections active: {}",
        snapshot.connections.connections_active
    );

    println!("\n=== Metrics Examples Complete ===");
    println!("\nUse cases:");
    println!("- Monitor production performance");
    println!("- Track connection health and reconnections");
    println!("- Debug latency issues");
    println!("- Measure error rates by type");
    println!("- Export to monitoring systems (Prometheus, etc.)");

    // Demonstrate periodic monitoring
    println!("\n\nExample 6: Periodic Metrics Monitoring");
    println!("---------------------------------------");

    let metrics = MetricsCollector::new();
    let config = ClientConfig::builder().metrics(metrics.clone()).build();

    match ResilientRedisClient::connect_with_full_config("localhost:6379", config).await {
        Ok(client) => {
            println!("Running commands with periodic metrics output...\n");

            for i in 0..3 {
                // Execute some commands
                for _ in 0..10 {
                    let _: String = client.call(Ping::new()).await?;
                }

                // Print metrics every iteration
                let snapshot = metrics.snapshot();
                println!(
                    "Iteration {}: {} commands, avg {:?}",
                    i + 1,
                    snapshot.commands.total_commands,
                    snapshot.commands.average_duration()
                );

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        Err(e) => println!("Failed to connect: {}", e),
    }

    Ok(())
}
