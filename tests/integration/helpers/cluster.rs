use redis_tower::cluster::ClusterClient;

/// Setup Redis cluster client
///
/// **Prerequisites**: This assumes a Redis Cluster is already running on ports 7100-7105.
///
/// You can start the cluster using docker-compose:
/// ```bash
/// docker-compose --profile cluster up -d
/// ```
pub async fn setup_cluster() -> ClusterClient {
    // Connect to cluster - assumes it's already running
    let seeds = vec![
        "localhost:7100".to_string(),
        "localhost:7101".to_string(),
        "localhost:7102".to_string(),
    ];

    ClusterClient::new(seeds)
        .await
        .expect("Failed to connect to cluster - make sure cluster is running on ports 7100-7105")
}
