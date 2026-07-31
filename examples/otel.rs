//! Export redis-tower metrics through OpenTelemetry to stdout.
//!
//! Run with:
//!
//! ```text
//! cargo run -p redis-tower-examples --example otel --features otel
//! ```
//!
//! A Redis server must be listening on `127.0.0.1:6379`.

use std::sync::Arc;
use std::time::Duration;

use metrics_opentelemetry::opentelemetry::metrics::MeterProvider;
use metrics_opentelemetry::{OpenTelemetryMetrics, OpenTelemetryRecorder, metrics};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use redis_tower::commands::Ping;
use redis_tower::pool::{ConnectionPool, PoolConfig};
use redis_tower::{
    AutoPipelineConfig, AutoPipelineService, MetricsFacadeRecorder, MetricsLayer,
    MultiplexedClient, RedisConnection, spawn_pool_stats_exporter, spawn_queue_depth_exporter,
};
use tower::Layer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(opentelemetry_stdout::MetricExporter::default())
        .build();
    let meter = provider.meter("redis-tower-example");
    metrics::set_global_recorder(OpenTelemetryRecorder::new(OpenTelemetryMetrics::new(meter)))?;

    let recorder = Arc::new(MetricsFacadeRecorder::new());

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

    let pool = ConnectionPool::connect_with_config(
        PoolConfig::default()
            .size(4)
            .name("primary")
            .metrics_recorder(recorder),
        || async { RedisConnection::connect("127.0.0.1:6379").await },
    )
    .await?;
    let pool_stats = spawn_pool_stats_exporter(pool.clone(), Duration::from_secs(5));
    let queue_stats =
        spawn_queue_depth_exporter(queue_probe.clone(), "commands", Duration::from_secs(5));

    client.execute(Ping::new()).await?;
    pool.execute(Ping::new()).await?;
    println!("auto-pipeline queue depth: {}", queue_probe.queue_depth());

    // Let the stats tasks publish their immediate snapshots. Explicit
    // shutdown emits a final snapshot before the provider flushes.
    tokio::time::sleep(Duration::from_millis(10)).await;
    queue_stats.shutdown().await;
    pool_stats.shutdown().await;
    drop(client);
    queue_probe.shutdown().await;
    provider.force_flush()?;
    provider.shutdown()?;
    Ok(())
}
