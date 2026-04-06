//! Composable resilience with tower-resilience.
//!
//! This example shows how to combine redis-tower with tower-resilience
//! (https://crates.io/crates/tower-resilience) for production-grade
//! fault tolerance: circuit breaker, retry with backoff, rate limiting,
//! and time limiting -- all as composable Tower layers.
//!
//! ```
//! cargo add tower-resilience-circuitbreaker
//! cargo add tower-resilience-retry
//! cargo add tower-resilience-ratelimiter
//! cargo add tower-resilience-timelimiter
//! ```
//!
//! Run: `cargo run --example resilience`

// NOTE: This example shows the pattern but won't compile without
// adding tower-resilience crates as dependencies. It's here as a
// reference for how the composition works.

fn main() {
    println!("tower-resilience + redis-tower composition patterns:");
    println!();
    println!("// Circuit breaker: trip after 5 failures in 10s, wait 30s before probing");
    println!(
        r#"
use tower::ServiceBuilder;
use tower_resilience_circuitbreaker::circuit_breaker_builder;
use redis_tower::{{FrameService, CommandAdapter}};

let cb_layer = circuit_breaker_builder()
    .failure_rate_threshold(50.0)
    .sliding_window_size(10)
    .wait_duration_in_open(Duration::from_secs(30))
    .minimum_number_of_calls(5)
    .build();

let svc = CommandAdapter::new(
    ServiceBuilder::new()
        .layer(cb_layer)
        .service(FrameService::connect("127.0.0.1:6379").await?)
);
"#
    );

    println!("// Retry with exponential backoff: 3 attempts, 100ms base delay");
    println!(
        r#"
use tower_resilience_retry::RetryLayer;

let retry_layer = RetryLayer::<Frame, Frame, RedisError>::builder()
    .max_attempts(3)
    .exponential_backoff(Duration::from_millis(100))
    .retry_on(|err: &RedisError| err.is_retryable())
    .build();
"#
    );

    println!("// Full production stack: retry -> circuit breaker -> tracing -> connection");
    println!(
        r#"
let svc = CommandAdapter::new(
    ServiceBuilder::new()
        .layer(retry_layer)
        .layer(cb_layer)
        .layer(TracingLayer::new())
        .layer(MetricsLayer::new(my_recorder))
        .service(FrameService::connect("127.0.0.1:6379").await?)
);
"#
    );
}
