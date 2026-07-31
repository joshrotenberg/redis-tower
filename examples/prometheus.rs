//! Export redis-tower metrics for Prometheus on `http://127.0.0.1:9000/metrics`.
//!
//! Run with:
//!
//! ```text
//! cargo run -p redis-tower-examples --example prometheus --features prometheus
//! ```
//!
//! A Redis server must be listening on `127.0.0.1:6379`.

use std::sync::Arc;
use std::time::Duration;

use metrics_exporter_prometheus::PrometheusBuilder;
use redis_tower::commands::Ping;
use redis_tower::pool::{ConnectionPool, PoolConfig};
use redis_tower::{
    AutoPipelineConfig, AutoPipelineService, MetricsFacadeRecorder, MetricsLayer,
    MultiplexedClient, RedisConnection, spawn_pool_stats_exporter, spawn_queue_depth_exporter,
};
use tower::Layer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    PrometheusBuilder::new()
        .with_http_listener(([127, 0, 0, 1], 9000))
        .with_recommended_naming(true)
        .install()?;

    let recorder = Arc::new(MetricsFacadeRecorder::new());

    // AutoPipelineConfig reports batch sizes. MetricsLayer adds command
    // duration, count, outcome, and error classification.
    let conn = RedisConnection::connect("127.0.0.1:6379").await?;
    let pipeline = AutoPipelineService::new(
        conn,
        AutoPipelineConfig {
            metrics_recorder: Some(recorder.clone()),
            ..AutoPipelineConfig::default()
        },
    );
    let queue_probe = MultiplexedClient::from_layered(pipeline.clone());
    let client =
        MultiplexedClient::from_layered(MetricsLayer::new(recorder.clone()).layer(pipeline));

    // A stable pool name becomes the db.client.connection.pool.name label.
    let pool = ConnectionPool::connect_with_config(
        PoolConfig::default()
            .size(4)
            .name("primary")
            .metrics_recorder(recorder),
        || async { RedisConnection::connect("127.0.0.1:6379").await },
    )
    .await?;
    let _pool_stats = spawn_pool_stats_exporter(pool.clone(), Duration::from_secs(5));
    let _queue_stats =
        spawn_queue_depth_exporter(queue_probe.clone(), "commands", Duration::from_secs(5));

    println!("Prometheus scrape endpoint: http://127.0.0.1:9000/metrics");
    println!("Press Ctrl-C to stop.");

    loop {
        client.execute(Ping::new()).await?;
        pool.execute(Ping::new()).await?;
        println!("auto-pipeline queue depth: {}", queue_probe.queue_depth());
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
