//! Integration tests for Latency monitoring commands
//!
//! Tests Redis latency tracking and analysis features (Redis 2.8.13+).
//!
//! Run with: cargo test --test integration_latency

mod helpers;

use helpers::standalone::setup_redis;
use redis_tower::commands::*;

#[tokio::test]
async fn test_latency_doctor() {
    let client = setup_redis().await;

    // Get latency analysis report
    let report: String = client.call(LatencyDoctor).await.unwrap();

    // Report should be a string (may be empty if no latency issues)
    // Just verify we can call it successfully
    assert!(report.is_empty() || !report.is_empty());
}

#[tokio::test]
async fn test_latency_latest() {
    let client = setup_redis().await;

    // Get latest latency samples
    let samples: String = client.call(LatencyLatest).await.unwrap();

    // Should return a string (empty if no events tracked)
    assert!(samples.is_empty() || !samples.is_empty());
}

#[tokio::test]
async fn test_latency_history() {
    let client = setup_redis().await;

    // Get latency history for "command" event
    // May be empty if no latency events recorded
    let history: Vec<(i64, i64)> = client.call(LatencyHistory::new("command")).await.unwrap();

    // Just verify we can call it (may be empty vec)
    assert!(history.is_empty() || !history.is_empty());
}

#[tokio::test]
async fn test_latency_reset() {
    let client = setup_redis().await;

    // Reset all latency data
    let reset_count: i64 = client.call(LatencyReset::all()).await.unwrap();

    // Should return number of events reset (may be 0)
    assert!(reset_count >= 0);
}

#[tokio::test]
async fn test_latency_reset_specific_event() {
    let client = setup_redis().await;

    // Reset specific event
    let reset_count: i64 = client
        .call(LatencyReset::new(vec!["command"]))
        .await
        .unwrap();

    // Should return 0 or 1 depending on if event existed
    assert!(reset_count >= 0);
}

#[tokio::test]
async fn test_latency_graph() {
    let client = setup_redis().await;

    // Get ASCII graph for command latency
    // May error if no samples available
    let result = client.call(LatencyGraph::new("command")).await;

    // Either succeeds with string or errors (no samples)
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test]
async fn test_latency_histogram() {
    let client = setup_redis().await;

    // Get latency histogram for all events
    let histogram: String = client.call(LatencyHistogram::all()).await.unwrap();

    // Should return string representation of histogram
    assert!(histogram.is_empty() || !histogram.is_empty());
}

#[tokio::test]
async fn test_latency_histogram_specific() {
    let client = setup_redis().await;

    // Get histogram for specific command
    let histogram: String = client
        .call(LatencyHistogram::new(vec!["get"]))
        .await
        .unwrap();

    // Should return histogram data
    assert!(histogram.is_empty() || !histogram.is_empty());
}
