//! Live-server failure-injection tests for the resilience middleware.
//!
//! Unlike the mock unit tests in `circuit_breaker.rs` and `command_timeout.rs`,
//! these drive the layers against a real backend so a regression in the timeout
//! path or the open/half-open state machine would be caught end to end.
//!
//! - [`command_timeout_fires_on_slow_command`] wraps a real `FrameService` with
//!   a 50ms `CommandTimeoutLayer` and issues `DEBUG SLEEP 1`, which blocks far
//!   longer than the deadline, so the call must surface `CommandTimeout`.
//! - [`circuit_breaker_opens_on_repeated_failures`] drives a
//!   `CircuitBreakerLayer` to its failure threshold by connecting to a dead TCP
//!   port (genuine `RedisError::Connection` failures), asserts the circuit then
//!   rejects immediately with `CircuitOpen`, and finally verifies half-open
//!   recovery once the inner service is pointed back at the live server.

mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use common::redis_addr;
use redis_server_wrapper::RedisServer;
use redis_tower::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerLayer};
use redis_tower::{CommandTimeoutLayer, Frame, FrameService, RedisConnection, RedisError};
use redis_tower_protocol::helpers::{array, bulk};
use tower::{Layer, Service, ServiceExt};

/// Reserve a TCP port and immediately release it, returning the port number.
///
/// Used both to obtain a guaranteed-free port for a dedicated server and to
/// produce an address that is almost certainly refusing connections.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let port = listener.local_addr().expect("local_addr").port();
    drop(listener);
    port
}

/// `CommandTimeoutLayer` must fire when a real command runs past the deadline.
///
/// `DEBUG SLEEP 1` blocks the server for a full second; with a 50ms deadline the
/// layer cancels the in-flight `FrameService` future and returns `CommandTimeout`.
///
/// This uses a dedicated server started with `enable-debug-command yes` rather
/// than the shared `common::conn()` server: the `DEBUG` command is disabled by
/// default, and blocking the shared server for a full second would interfere
/// with any other test sharing it.
#[tokio::test]
async fn command_timeout_fires_on_slow_command() {
    let server = RedisServer::new()
        .port(free_port())
        .enable_debug_command("yes")
        .start()
        .await
        .expect("failed to start dedicated Redis server with DEBUG enabled");

    let svc = FrameService::connect(&server.addr())
        .await
        .expect("failed to connect FrameService to Redis");

    // 50ms deadline, command sleeps ~1s -> must time out.
    let mut svc = CommandTimeoutLayer::new(Duration::from_millis(50)).layer(svc);

    let request = array(vec![bulk("DEBUG"), bulk("SLEEP"), bulk("1")]);
    let result = svc.ready().await.unwrap().call(request).await;

    assert!(
        matches!(result, Err(RedisError::CommandTimeout)),
        "expected CommandTimeout, got {result:?}"
    );

    // The server is still busy completing DEBUG SLEEP on this connection's
    // socket; drop the service so the connection is torn down rather than
    // reused with a stale pending reply. The dedicated server shuts down when
    // `server` is dropped at the end of the test.
    drop(svc);
}

/// An inner `Service<Frame>` that connects fresh to a target address on every
/// `call`, pings it, and returns the reply.
///
/// The target lives behind an `Arc<Mutex<String>>` so the test can inject
/// failures (point it at a dead port) and then heal it (point it at the live
/// server) to exercise the circuit breaker's open -> half-open -> closed path.
#[derive(Clone)]
struct ConnectAndPing {
    target: Arc<Mutex<String>>,
}

impl Service<Frame> for ConnectAndPing {
    type Response = Frame;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: Frame) -> Self::Future {
        let target = self.target.lock().unwrap().clone();
        Box::pin(async move {
            // A failed connect to a dead port yields RedisError::Connection,
            // which the circuit breaker counts as a failure.
            let mut conn = RedisConnection::connect(&target).await?;
            conn.execute_pipeline(vec![array(vec![bulk("PING")])])
                .await
                .map(|mut frames| frames.remove(0))
        })
    }
}

/// `CircuitBreakerLayer` must open after repeated failures, reject immediately
/// while open, then recover via a successful half-open probe.
#[tokio::test]
async fn circuit_breaker_opens_on_repeated_failures() {
    // A free port with nothing listening -> connections are refused.
    let dead = format!("127.0.0.1:{}", free_port());
    let target = Arc::new(Mutex::new(dead));

    let layer = CircuitBreakerLayer::new(CircuitBreakerConfig {
        failure_threshold: 3,
        // Short probe interval so the test can reach half-open quickly.
        recovery_probe_interval: Duration::from_millis(150),
    });
    let mut svc = layer.layer(ConnectAndPing {
        target: Arc::clone(&target),
    });

    let ping = || array(vec![bulk("PING")]);

    // Drive the circuit to its failure threshold against the dead port. Each
    // call connects to a closed port and fails with RedisError::Connection.
    for i in 0..3 {
        let result = svc.ready().await.unwrap().call(ping()).await;
        assert!(
            matches!(result, Err(RedisError::Connection(_))),
            "call {i}: expected a Connection error from the dead port, got {result:?}"
        );
    }

    // The circuit is now open: poll_ready rejects immediately with CircuitOpen
    // (the inner service is never touched) before the probe interval elapses.
    let poll = svc.ready().await;
    assert!(
        matches!(poll, Err(RedisError::CircuitOpen)),
        "expected CircuitOpen while the circuit is open, got {:?}",
        poll.err()
    );

    // Heal the backend: point the inner service at the live server so the
    // half-open probe will succeed.
    {
        let live = redis_addr().await.to_string();
        *target.lock().unwrap() = live;
    }

    // Wait past the probe interval so the next poll_ready transitions to
    // half-open and allows a single probe through.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The half-open probe connects to the live server and PINGs successfully,
    // which closes the circuit again.
    let recovered = svc.ready().await.unwrap().call(ping()).await;
    assert!(
        matches!(&recovered, Ok(Frame::SimpleString(s)) if s.eq_ignore_ascii_case(b"PONG")),
        "expected a successful PONG after recovery, got {recovered:?}"
    );

    // Circuit is closed again: a follow-up call still succeeds.
    let after = svc.ready().await.unwrap().call(ping()).await;
    assert!(
        after.is_ok(),
        "expected the circuit to stay closed after recovery, got {after:?}"
    );
}
