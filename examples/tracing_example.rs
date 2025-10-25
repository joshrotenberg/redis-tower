use redis_tower::client::ResilientRedisClient;
use redis_tower::commands::{Get, Incr, Ping, Set};
use redis_tower::config::ClientConfig;
use redis_tower::tracing::TracingConfig;
use tracing::Level;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Redis Tower Tracing Example ===\n");

    // Example 1: Default tracing (commands and connections, not network)
    println!("Example 1: Default Tracing Configuration");
    println!("-----------------------------------------");

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env().add_directive(Level::DEBUG.into()))
        .init();

    let config = ClientConfig::builder().build();

    match ResilientRedisClient::connect_with_full_config("localhost:6379", config).await {
        Ok(client) => {
            println!("\nExecuting commands with default tracing...\n");

            let _: String = client.call(Ping::new()).await?;
            let _: () = client.call(Set::new("traced_key", "traced_value")).await?;
            let value: Option<bytes::Bytes> = client.call(Get::new("traced_key")).await?;
            let value_str = value.map(|b| String::from_utf8_lossy(&b).to_string());
            println!("\nRetrieved value: {:?}", value_str);

            let count: i64 = client.call(Incr::new("traced_counter")).await?;
            println!("Counter: {}", count);
        }
        Err(e) => println!("Failed to connect: {}", e),
    }

    println!("\n\nExample 2: All Tracing Enabled (verbose)");
    println!("-----------------------------------------");
    println!("This would show network-level tracing too.");
    println!("Uncomment the code below to see TRACE level events:\n");

    // Uncomment to see all tracing including network operations:
    /*
    let verbose_config = ClientConfig::builder()
        .tracing(TracingConfig::all())
        .build();

    match ResilientRedisClient::connect_with_full_config("localhost:6379", verbose_config).await {
        Ok(client) => {
            println!("\nExecuting with verbose tracing...\n");
            let _: String = client.call(Ping::new()).await?;
        }
        Err(e) => println!("Failed to connect: {}", e),
    }
    */

    println!("\n\nExample 3: No Tracing");
    println!("---------------------");
    println!("No trace output for these operations:\n");

    let no_trace_config = ClientConfig::builder().no_tracing().build();

    match ResilientRedisClient::connect_with_full_config("localhost:6379", no_trace_config).await {
        Ok(client) => {
            let _: String = client.call(Ping::new()).await?;
            let _: () = client.call(Set::new("silent_key", "silent_value")).await?;
            println!("Commands executed silently (no trace output)");
        }
        Err(e) => println!("Failed to connect: {}", e),
    }

    println!("\n\nExample 4: Custom Tracing Levels");
    println!("----------------------------------");
    println!("Commands at WARN, connections at INFO, network disabled:\n");

    let custom_config = ClientConfig::builder()
        .tracing(
            TracingConfig::builder()
                .trace_commands(true)
                .trace_connections(true)
                .trace_network(false)
                .command_level(Level::WARN)
                .connection_level(Level::INFO)
                .build(),
        )
        .build();

    match ResilientRedisClient::connect_with_full_config("localhost:6379", custom_config).await {
        Ok(client) => {
            let _: String = client.call(Ping::new()).await?;
            println!("Command executed with custom trace levels");
        }
        Err(e) => println!("Failed to connect: {}", e),
    }

    println!("\n=== Tracing Examples Complete ===");
    println!("\nTips:");
    println!("- Set RUST_LOG environment variable to control output");
    println!("- Use RUST_LOG=redis_tower=trace to see all events");
    println!("- Use RUST_LOG=redis_tower=debug for commands and connections");
    println!("- Use RUST_LOG=redis_tower=info for connections only");

    Ok(())
}
