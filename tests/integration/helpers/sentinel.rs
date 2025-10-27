use docker_wrapper::RedisSentinelTemplate;
use redis_tower::sentinel::{SentinelClient, SentinelConfig};
use std::sync::OnceLock;
use tokio::sync::Mutex;

static SENTINEL_INIT: OnceLock<Mutex<Option<Vec<u16>>>> = OnceLock::new();

/// Setup Redis Sentinel using docker-wrapper
///
/// This will start a Redis Sentinel setup (1 master + 1 replica + 3 sentinels)
/// using docker-wrapper's RedisSentinelTemplate.
/// The setup is started once and reused across all tests.
pub async fn setup_sentinel() -> SentinelClient {
    // Get or initialize the mutex
    let mutex = SENTINEL_INIT.get_or_init(|| Mutex::new(None));

    // Lock and check if we need to initialize
    let mut guard = mutex.lock().await;

    if guard.is_none() {
        // First time - start the sentinel
        let ports = start_sentinel().await;
        *guard = Some(ports);
    }

    // Get the ports
    let ports = guard.as_ref().unwrap();

    // Build sentinel config
    let mut config_builder = SentinelConfig::builder().master_name("mymaster");

    for port in ports {
        config_builder = config_builder.sentinel_node("localhost", *port);
    }

    let config = config_builder
        .build()
        .expect("Failed to build sentinel config");

    SentinelClient::new(config)
}

/// Start the Redis Sentinel setup using docker-wrapper
async fn start_sentinel() -> Vec<u16> {
    println!("Starting Redis Sentinel with docker-wrapper...");

    let sentinel = RedisSentinelTemplate::new("redis-tower-test")
        .num_sentinels(3)
        .master_name("mymaster")
        .master_port(6380)
        .sentinel_port_base(26379);

    match sentinel.start().await {
        Ok(info) => {
            println!("✓ Sentinel started successfully");
            println!("  Master: {}:{}", info.master_host, info.master_port);
            println!("  Sentinels: {} nodes", info.sentinels.len());

            // Extract sentinel ports
            let ports: Vec<u16> = info.sentinels.iter().map(|s| s.port).collect();

            ports
        }
        Err(e) => {
            eprintln!("✗ Failed to start sentinel: {}", e);
            panic!("Cannot run tests without sentinel");
        }
    }
}

// Note: Sentinel cleanup not implemented yet in docker-wrapper
// Containers will be cleaned up manually or on system restart
