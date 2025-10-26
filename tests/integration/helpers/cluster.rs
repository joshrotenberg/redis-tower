use docker_wrapper::{RedisClusterConnection, RedisClusterTemplate, Template};
use redis_tower::cluster::ClusterClient;
use std::sync::OnceLock;
use tokio::sync::Mutex;

static CLUSTER_INIT: OnceLock<Mutex<Option<RedisClusterTemplate>>> = OnceLock::new();

/// Setup Redis cluster using docker-wrapper
///
/// This will start a 3-master Redis cluster using docker-wrapper's RedisClusterTemplate.
/// The cluster is started once and reused across all tests.
pub async fn setup_cluster() -> ClusterClient {
    // Get or initialize the mutex
    let mutex = CLUSTER_INIT.get_or_init(|| Mutex::new(None));

    // Lock and check if we need to initialize
    let mut guard = mutex.lock().await;

    if guard.is_none() {
        // First time - start the cluster
        let template = start_cluster().await;
        *guard = Some(template);
    }

    // For now, use localhost with mapped ports
    // TODO: This has issues with CLUSTER SLOTS returning internal Docker IPs
    // We need to either:
    // 1. Run tests inside Docker network
    // 2. Fix announce-ip propagation in docker-wrapper
    // 3. Use a different testing approach
    let seeds = vec![
        "localhost:7100".to_string(),
        "localhost:7101".to_string(),
        "localhost:7102".to_string(),
    ];

    println!("Connecting to cluster with seeds: {:?}", seeds);
    println!("NOTE: Cluster tests may fail due to internal Docker IP issues");

    ClusterClient::new(seeds)
        .await
        .expect("Failed to connect to cluster")
}

/// Start the Redis cluster using docker-wrapper
async fn start_cluster() -> RedisClusterTemplate {
    println!("Starting Redis cluster with docker-wrapper...");

    // Clean up any existing containers/network from previous failed runs
    let template = RedisClusterTemplate::new("redis-tower-test")
        .num_masters(3)
        .num_replicas(0)
        .port_base(7100);
    // Note: Not using cluster_announce_ip - cluster nodes will use internal Docker IPs.
    // This means tests must run from within the Docker network or we need a different approach.

    let _ = cleanup_cluster().await;

    match template.start().await {
        Ok(info) => {
            println!("✓ Cluster started successfully");
            println!("  {}", info);
            template
        }
        Err(e) => {
            eprintln!("✗ Failed to start cluster: {}", e);
            // Try to clean up on error
            let _ = cleanup_cluster().await;
            panic!("Cannot run tests without cluster");
        }
    }
}

/// Cleanup function - removes all cluster containers and network
async fn cleanup_cluster() {
    let cluster = RedisClusterTemplate::new("redis-tower-test");
    let _ = cluster.stop().await;
    let _ = cluster.remove().await;
}
