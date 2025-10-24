//! Resilient Redis client example with Tower middleware
//!
//! This example demonstrates how to compose resilience patterns using tower-resilience:
//! - Timeout for slow operations
//! - Retry with exponential backoff
//! - Circuit breaker to prevent cascading failures
//!
//! Prerequisites:
//! - Redis server running on localhost:6379
//!
//! Run with: cargo run --example resilient

use std::time::Duration;
use tower::Service;
use tower_resilience::{
    circuitbreaker::CircuitBreakerLayer,
    retry::{ExponentialBackoff, RetryLayer},
    timelimiter::TimeLimiterLayer,
};

// Simple error type that implements Clone (required by retry layer)
#[derive(Debug, Clone)]
struct DemoError(String);

impl std::fmt::Display for DemoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for DemoError {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing to see what's happening
    tracing_subscriber::fmt::init();

    println!("Tower Resilience Patterns for Redis Client\n");
    println!("This example demonstrates three key resilience patterns:");
    println!("  1. Circuit Breaker - prevents cascading failures");
    println!("  2. Retry - handles transient failures");
    println!("  3. Timeout - prevents hanging requests\n");

    // Demo 1: Circuit Breaker
    demo_circuit_breaker().await;

    // Demo 2: Retry with backoff
    demo_retry().await;

    // Demo 3: Timeout
    demo_timeout().await;

    println!("\n=== Summary ===\n");
    println!("Tower middleware provides composable resilience:");
    println!("  ✓ Circuit Breaker - prevents cascading failures");
    println!("  ✓ Retry - handles transient failures automatically");
    println!("  ✓ Timeout - prevents hanging requests");
    println!("  ✓ All patterns compose cleanly with Layer trait\n");

    println!("To integrate with redis-tower:");
    println!("  1. Implement Tower Service trait for RedisConnection");
    println!("  2. Make error types Clone-able for retry compatibility");
    println!("  3. Add ServiceBuilder wrapper for easy middleware composition");
    println!("  4. Support connection pooling with Tower's Balance layer\n");

    Ok(())
}

async fn demo_circuit_breaker() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    println!("=== Demo 1: Circuit Breaker ===\n");
    println!("Simulating a service that fails 60% of the time...\n");

    let call_count = Arc::new(AtomicUsize::new(0));
    let failures = Arc::new(AtomicUsize::new(0));

    let cc = Arc::clone(&call_count);
    let fc = Arc::clone(&failures);

    let service = tower::service_fn(move |_req: ()| {
        let cc = Arc::clone(&cc);
        let fc = Arc::clone(&fc);
        async move {
            let count = cc.fetch_add(1, Ordering::SeqCst) + 1;

            // Fail 60% of the time
            if count % 10 < 6 {
                fc.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(DemoError("Simulated failure".to_string()))
            } else {
                Ok(())
            }
        }
    });

    let cb_layer = CircuitBreakerLayer::builder()
        .failure_rate_threshold(0.5) // Open at 50% failure rate
        .sliding_window_size(10) // Consider last 10 calls
        .wait_duration_in_open(Duration::from_secs(1))
        .build();

    let mut service = cb_layer.layer(service);

    // Send 20 requests
    for i in 1..=20 {
        match tower::ServiceExt::ready(&mut service)
            .await
            .unwrap()
            .call(())
            .await
        {
            Ok(()) => println!("  Request {}: Success", i),
            Err(_) => println!("  Request {}: Failed/Rejected", i),
        }
    }

    println!(
        "\nResults: {} service calls, {} failures",
        call_count.load(Ordering::SeqCst),
        failures.load(Ordering::SeqCst)
    );
    println!("Circuit breaker prevented some calls from reaching the failing service!\n");
}

async fn demo_retry() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tower::Layer;

    println!("=== Demo 2: Retry with Exponential Backoff ===\n");
    println!("Service fails first 2 attempts, succeeds on 3rd...\n");

    let call_count = Arc::new(AtomicUsize::new(0));
    let cc = Arc::clone(&call_count);

    let service = tower::service_fn(move |req: String| {
        let cc = Arc::clone(&cc);
        async move {
            let count = cc.fetch_add(1, Ordering::SeqCst) + 1;
            println!("  [Service] Attempt #{}", count);

            if count < 3 {
                Err::<String, _>(DemoError("Temporary failure".to_string()))
            } else {
                Ok(format!("Success after {} attempts: {}", count, req))
            }
        }
    });

    let retry_layer = RetryLayer::builder()
        .max_attempts(5)
        .backoff(ExponentialBackoff::new(Duration::from_millis(50)))
        .on_retry(|attempt, delay| {
            println!("  [Retry] Will retry attempt {} after {:?}", attempt, delay);
        })
        .build();

    let mut service = retry_layer.layer(service);

    match tower::ServiceExt::ready(&mut service)
        .await
        .unwrap()
        .call("GET mykey".to_string())
        .await
    {
        Ok(result) => println!("\n{}\n", result),
        Err(_) => println!("\nFailed after all retries\n"),
    }
}

async fn demo_timeout() {
    use tokio::time::sleep;
    use tower::Layer;

    println!("=== Demo 3: Timeout ===\n");

    let service = tower::service_fn(|duration: Duration| async move {
        println!("  [Service] Processing request (will take {:?})", duration);
        sleep(duration).await;
        Ok::<_, DemoError>("Completed")
    });

    let timeout_layer = TimeLimiterLayer::builder()
        .timeout_duration(Duration::from_millis(100))
        .on_timeout(|| println!("  [Timeout] Request exceeded timeout!"))
        .on_success(|duration| println!("  [Success] Completed in {:?}", duration))
        .build();

    let mut service = timeout_layer.layer(service);

    // Fast request - should succeed
    println!("Request 1: Fast (50ms)");
    let _ = tower::ServiceExt::ready(&mut service)
        .await
        .unwrap()
        .call(Duration::from_millis(50))
        .await;

    println!();

    // Slow request - should timeout
    println!("Request 2: Slow (200ms)");
    let _ = tower::ServiceExt::ready(&mut service)
        .await
        .unwrap()
        .call(Duration::from_millis(200))
        .await;

    println!();
}
