use redis_tower::cluster::ClusterClient;

/// Setup Redis cluster using local redis-server instances
///
/// This expects a Redis cluster to be running on ports 7100-7105.
/// Start the cluster with: ./scripts/setup-test-cluster.sh start
///
/// The cluster configuration is:
/// - Masters: 7100, 7101, 7102
/// - Replicas: 7103, 7104, 7105
pub async fn setup_cluster() -> ClusterClient {
    let seeds = vec![
        "localhost:7100".to_string(),
        "localhost:7101".to_string(),
        "localhost:7102".to_string(),
    ];

    println!("Connecting to local cluster on ports 7100-7102");
    println!("Make sure cluster is running: ./scripts/setup-test-cluster.sh start");

    ClusterClient::new(seeds)
        .await
        .expect("Failed to connect to cluster. Did you start it with ./scripts/setup-test-cluster.sh start?")
}
