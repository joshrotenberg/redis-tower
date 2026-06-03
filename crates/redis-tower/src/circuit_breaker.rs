//! Tower layer implementing a three-state circuit breaker for Redis commands.
//!
//! The circuit breaker has three states:
//! - **Closed** (normal): requests pass through; consecutive failures counted.
//! - **Open** (tripped): requests immediately return `RedisError::CircuitOpen`
//!   without touching the inner service; entered when failures exceed
//!   `failure_threshold`.
//! - **Half-open** (recovery probe): after `recovery_probe_interval` elapses,
//!   one probe request is allowed through; success → Closed, failure → Open.
//!
//! State is `Arc`-shared so all clones of a client trip together.
//!
//! # Example
//!
//! ```ignore
//! use std::time::Duration;
//! use tower::ServiceBuilder;
//! use redis_tower::circuit_breaker::{CircuitBreakerLayer, CircuitBreakerConfig};
//!
//! let svc = ServiceBuilder::new()
//!     .layer(CircuitBreakerLayer::new(CircuitBreakerConfig {
//!         failure_threshold: 5,
//!         recovery_probe_interval: Duration::from_secs(5),
//!     }))
//!     .service(inner_service);
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use redis_tower_core::{Frame, RedisError};
use tower_layer::Layer;
use tower_service::Service;

/// Configuration for the circuit breaker.
#[derive(Clone, Debug)]
pub struct CircuitBreakerConfig {
    /// Consecutive failures before the circuit opens (default: 5).
    pub failure_threshold: u32,
    /// How long to wait in open state before allowing a probe (default: 5s).
    pub recovery_probe_interval: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_probe_interval: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum CircuitState {
    Closed { consecutive_failures: u32 },
    Open { opened_at: Instant },
    HalfOpen,
}

/// Tower `Layer` that wraps a service with circuit breaker semantics.
///
/// All clones of the resulting [`CircuitBreakerService`] share the same
/// circuit state via an `Arc<Mutex<CircuitState>>`, so a single trip from
/// any clone opens the circuit for all.
#[derive(Clone, Debug)]
pub struct CircuitBreakerLayer {
    config: CircuitBreakerConfig,
    state: Arc<Mutex<CircuitState>>,
}

impl CircuitBreakerLayer {
    /// Create a new circuit breaker layer with the given configuration.
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(CircuitState::Closed {
                consecutive_failures: 0,
            })),
        }
    }
}

impl<S> Layer<S> for CircuitBreakerLayer {
    type Service = CircuitBreakerService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CircuitBreakerService {
            inner,
            config: self.config.clone(),
            state: Arc::clone(&self.state),
        }
    }
}

/// Tower `Service` that applies circuit breaker logic to every request.
///
/// Created by [`CircuitBreakerLayer`]. All clones share circuit state.
#[derive(Clone, Debug)]
pub struct CircuitBreakerService<S> {
    inner: S,
    config: CircuitBreakerConfig,
    state: Arc<Mutex<CircuitState>>,
}

impl<S> Service<Frame> for CircuitBreakerService<S>
where
    S: Service<Frame, Response = Frame, Error = RedisError> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Frame;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut state = self.state.lock().unwrap();
        match *state {
            CircuitState::Closed { .. } => {
                drop(state);
                self.inner.poll_ready(cx)
            }
            CircuitState::Open { opened_at } => {
                if opened_at.elapsed() >= self.config.recovery_probe_interval {
                    *state = CircuitState::HalfOpen;
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Ready(Err(RedisError::CircuitOpen))
                }
            }
            CircuitState::HalfOpen => {
                drop(state);
                self.inner.poll_ready(cx)
            }
        }
    }

    fn call(&mut self, request: Frame) -> Self::Future {
        let snapshot = self.state.lock().unwrap().clone();

        match snapshot {
            CircuitState::Open { .. } => Box::pin(async { Err(RedisError::CircuitOpen) }),
            CircuitState::Closed { .. } | CircuitState::HalfOpen => {
                let was_half_open = matches!(snapshot, CircuitState::HalfOpen);
                let state = Arc::clone(&self.state);
                let threshold = self.config.failure_threshold;
                let future = self.inner.call(request);

                Box::pin(async move {
                    match future.await {
                        Ok(frame) => {
                            *state.lock().unwrap() = CircuitState::Closed {
                                consecutive_failures: 0,
                            };
                            Ok(frame)
                        }
                        Err(e) => {
                            let mut s = state.lock().unwrap();
                            if was_half_open {
                                *s = CircuitState::Open {
                                    opened_at: Instant::now(),
                                };
                            } else {
                                let failures = match *s {
                                    CircuitState::Closed {
                                        consecutive_failures,
                                    } => consecutive_failures + 1,
                                    _ => 1,
                                };
                                if failures >= threshold {
                                    *s = CircuitState::Open {
                                        opened_at: Instant::now(),
                                    };
                                } else {
                                    *s = CircuitState::Closed {
                                        consecutive_failures: failures,
                                    };
                                }
                            }
                            Err(e)
                        }
                    }
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Context;

    struct OkService;

    impl Service<Frame> for OkService {
        type Response = Frame;
        type Error = RedisError;
        type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Frame) -> Self::Future {
            Box::pin(async { Ok(Frame::SimpleString("OK".into())) })
        }
    }

    struct ErrService;

    impl Service<Frame> for ErrService {
        type Response = Frame;
        type Error = RedisError;
        type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _request: Frame) -> Self::Future {
            Box::pin(async { Err(RedisError::ConnectionClosed) })
        }
    }

    fn dummy_frame() -> Frame {
        Frame::SimpleString("PING".into())
    }

    #[tokio::test]
    async fn passes_through_when_closed() {
        let mut svc = CircuitBreakerService {
            inner: OkService,
            config: CircuitBreakerConfig::default(),
            state: Arc::new(Mutex::new(CircuitState::Closed {
                consecutive_failures: 0,
            })),
        };
        let result = svc.call(dummy_frame()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn trips_after_threshold() {
        let mut svc = CircuitBreakerService {
            inner: ErrService,
            config: CircuitBreakerConfig {
                failure_threshold: 3,
                recovery_probe_interval: Duration::from_secs(5),
            },
            state: Arc::new(Mutex::new(CircuitState::Closed {
                consecutive_failures: 0,
            })),
        };

        // Three failures should trip the circuit.
        for _ in 0..3 {
            let _ = svc.call(dummy_frame()).await;
        }

        // poll_ready on an open circuit returns CircuitOpen.
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let poll = svc.poll_ready(&mut cx);
        match poll {
            Poll::Ready(Err(RedisError::CircuitOpen)) => {}
            other => panic!("expected CircuitOpen, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn open_rejects_immediately() {
        let mut svc = CircuitBreakerService {
            inner: OkService,
            config: CircuitBreakerConfig {
                failure_threshold: 5,
                recovery_probe_interval: Duration::from_secs(5),
            },
            state: Arc::new(Mutex::new(CircuitState::Open {
                opened_at: Instant::now(),
            })),
        };

        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let poll = svc.poll_ready(&mut cx);
        match poll {
            Poll::Ready(Err(RedisError::CircuitOpen)) => {}
            other => panic!("expected CircuitOpen, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn half_open_probe_success_closes() {
        let state = Arc::new(Mutex::new(CircuitState::Open {
            opened_at: Instant::now() - Duration::from_secs(10),
        }));
        let mut svc = CircuitBreakerService {
            inner: OkService,
            config: CircuitBreakerConfig {
                failure_threshold: 5,
                recovery_probe_interval: Duration::from_secs(5),
            },
            state: Arc::clone(&state),
        };

        // poll_ready should transition to HalfOpen.
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let poll = svc.poll_ready(&mut cx);
        assert!(matches!(poll, Poll::Ready(Ok(()))));
        assert_eq!(*state.lock().unwrap(), CircuitState::HalfOpen);

        // Successful probe call should close the circuit.
        let _ = svc.call(dummy_frame()).await;
        assert!(matches!(
            *state.lock().unwrap(),
            CircuitState::Closed {
                consecutive_failures: 0
            }
        ));
    }

    #[tokio::test]
    async fn half_open_probe_failure_reopens() {
        let state = Arc::new(Mutex::new(CircuitState::Open {
            opened_at: Instant::now() - Duration::from_secs(10),
        }));
        let mut svc = CircuitBreakerService {
            inner: ErrService,
            config: CircuitBreakerConfig {
                failure_threshold: 5,
                recovery_probe_interval: Duration::from_secs(5),
            },
            state: Arc::clone(&state),
        };

        // poll_ready transitions to HalfOpen.
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let _ = svc.poll_ready(&mut cx);
        assert_eq!(*state.lock().unwrap(), CircuitState::HalfOpen);

        // Failed probe should reopen the circuit.
        let _ = svc.call(dummy_frame()).await;
        assert!(matches!(*state.lock().unwrap(), CircuitState::Open { .. }));
    }
}
