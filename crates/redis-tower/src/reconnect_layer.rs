//! Tower Layer for automatic reconnection at the Frame level.
//!
//! Wraps a `FrameService` and reconnects when connection errors occur.
//! Composes with other Frame-level middleware (caching, metrics).
//!
//! # Example
//!
//! ```ignore
//! use tower::ServiceBuilder;
//! use redis_tower::reconnect_layer::ReconnectLayer;
//! use redis_tower::reconnect::{AddrConnectionFactory, ReconnectConfig};
//! use redis_tower::cache_layer::{CacheService, CacheConfig};
//! use redis_tower::command_adapter::CommandAdapter;
//!
//! let svc = CommandAdapter::new(
//!     CacheService::new(
//!         ReconnectService::new(factory, config).await?,
//!         CacheConfig::default(),
//!     )
//! );
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use redis_tower_core::{Frame, FrameService, RedisError};
use tower_service::Service;

use crate::reconnect::{ConnectionFactory, ReconnectConfig};

type ReconnectFuture = Pin<Box<dyn Future<Output = Result<FrameService, RedisError>> + Send>>;

enum State {
    Ready,
    WaitingToReconnect {
        attempt: usize,
        sleep: Pin<Box<tokio::time::Sleep>>,
    },
    Reconnecting {
        attempt: usize,
        future: ReconnectFuture,
    },
    Failed,
}

/// A `Service<Frame>` that automatically reconnects on connection errors.
///
/// Wraps a `FrameService` directly. When a connection error is detected,
/// `poll_ready` drives the reconnection state machine with configurable
/// exponential backoff.
pub struct ReconnectService {
    inner: FrameService,
    factory: Arc<dyn ConnectionFactory>,
    config: ReconnectConfig,
    state: State,
    needs_reconnect: Arc<AtomicBool>,
}

impl ReconnectService {
    /// Create a new reconnecting service.
    pub async fn new(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
    ) -> Result<Self, RedisError> {
        let factory = Arc::new(factory);
        let conn = factory.connect().await?;
        Ok(Self {
            inner: FrameService::from_connection(conn),
            factory,
            config,
            state: State::Ready,
            needs_reconnect: Arc::new(AtomicBool::new(false)),
        })
    }

    fn trigger_reconnect(&mut self, attempt: usize) {
        if let Some(max) = self.config.max_retries {
            if attempt > max {
                self.state = State::Failed;
                return;
            }
        }
        let delay = self.config.delay_for_attempt(attempt);
        self.state = State::WaitingToReconnect {
            attempt,
            sleep: Box::pin(tokio::time::sleep(delay)),
        };
    }
}

impl Service<Frame> for ReconnectService {
    type Response = Frame;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Check if a previous call signaled a connection error.
        // NOTE: There is a one-request-delay between error detection and
        // reconnection, because the flag is only checked here in poll_ready.
        if self.needs_reconnect.swap(false, Ordering::Acquire) && matches!(self.state, State::Ready)
        {
            self.trigger_reconnect(0);
        }

        loop {
            match &mut self.state {
                State::Ready => return self.inner.poll_ready(cx),
                State::Failed => {
                    return Poll::Ready(Err(RedisError::ReconnectFailed {
                        attempts: self.config.max_retries.unwrap_or(0),
                        last_error: Box::new(RedisError::ConnectionClosed),
                    }));
                }
                State::WaitingToReconnect { attempt, sleep } => match sleep.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        let attempt = *attempt;
                        let factory = Arc::clone(&self.factory);
                        let future: ReconnectFuture = Box::pin(async move {
                            let conn = factory.connect().await?;
                            Ok(FrameService::from_connection(conn))
                        });
                        self.state = State::Reconnecting { attempt, future };
                    }
                    Poll::Pending => return Poll::Pending,
                },
                State::Reconnecting { attempt, future } => match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(new_svc)) => {
                        self.inner = new_svc;
                        self.state = State::Ready;
                        return self.inner.poll_ready(cx);
                    }
                    Poll::Ready(Err(_)) => {
                        let next = *attempt + 1;
                        self.trigger_reconnect(next);
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }

    fn call(&mut self, request: Frame) -> Self::Future {
        let future = self.inner.call(request);
        let needs_reconnect = Arc::clone(&self.needs_reconnect);

        Box::pin(async move {
            let result = future.await;
            if let Err(ref e) = result {
                if is_connection_error(e) {
                    needs_reconnect.store(true, Ordering::Release);
                }
            }
            result
        })
    }
}

fn is_connection_error(err: &RedisError) -> bool {
    matches!(
        err,
        RedisError::Connection(_) | RedisError::ConnectionClosed
    )
}
