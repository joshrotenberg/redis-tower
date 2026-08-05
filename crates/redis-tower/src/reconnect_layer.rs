//! Tower Layer for automatic reconnection at the Frame level.
//!
//! Wraps a `FrameService` and reconnects when connection errors occur.
//! Composes with other Frame-level middleware (caching, metrics).
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::cache_layer::{CacheConfig, CacheService};
//! use redis_tower::command_adapter::CommandAdapter;
//! use redis_tower::reconnect::{AddrConnectionFactory, ReconnectConfig};
//! use redis_tower::reconnect_layer::ReconnectService;
//!
//! let factory = AddrConnectionFactory::new("127.0.0.1:6379");
//!
//! let svc = CommandAdapter::new(
//!     CacheService::new(
//!         ReconnectService::new(factory, ReconnectConfig::default()).await?,
//!         CacheConfig::default(),
//!     )
//! );
//! # let _ = svc;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use redis_tower_core::{Frame, FrameService, RedisError};
use tower_service::Service;

use crate::reconnect::{
    ConnectionEvent, ConnectionEventBus, ConnectionFactory, ReconnectConfig, connect_with_timeout,
    publish_disconnect_before_shutdown, publish_shutdown_once,
};

type ReconnectFuture = Pin<Box<dyn Future<Output = Result<FrameService, RedisError>> + Send>>;

enum State {
    Ready,
    WaitingToReconnect {
        attempt: usize,
        sleep: Pin<Box<tokio::time::Sleep>>,
        started: Instant,
    },
    Reconnecting {
        attempt: usize,
        future: ReconnectFuture,
        started: Instant,
    },
    Failed,
}

/// A `Service<Frame>` that automatically reconnects on connection errors.
///
/// Wraps a `FrameService` directly. When a connection error is detected,
/// `poll_ready` drives the reconnection state machine with configurable
/// exponential backoff.
///
/// # Factory Selection
///
/// The [`ConnectionFactory`] you provide determines what negotiation
/// happens on each reconnect. Use [`UrlConnectionFactory`](crate::reconnect::UrlConnectionFactory)
/// if your server requires AUTH or a specific database, so those are
/// replayed on every new connection. See the [`reconnect`](crate::reconnect)
/// module docs for the full factory comparison table.
pub struct ReconnectService {
    inner: FrameService,
    factory: Arc<dyn ConnectionFactory>,
    config: ReconnectConfig,
    state: State,
    needs_reconnect: Arc<AtomicBool>,
    event_bus: Option<ConnectionEventBus>,
    disconnect_reported: Option<Arc<AtomicBool>>,
    shutdown_reported: Option<Arc<AtomicBool>>,
    lifecycle_lock: Option<Arc<StdMutex<()>>>,
}

impl ReconnectService {
    /// Create a new reconnecting service.
    pub async fn new(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
    ) -> Result<Self, RedisError> {
        Self::new_inner(factory, config, None).await
    }

    /// Create a reconnecting frame service that publishes lifecycle events.
    ///
    /// Subscribe before calling this constructor to observe the initial
    /// [`ConnectionEvent::Connected`] or [`ConnectionEvent::ConnectFailed`]
    /// event.
    pub async fn new_with_events(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
        events: ConnectionEventBus,
    ) -> Result<Self, RedisError> {
        Self::new_inner(factory, config, Some(events)).await
    }

    async fn new_inner(
        factory: impl ConnectionFactory,
        config: ReconnectConfig,
        event_bus: Option<ConnectionEventBus>,
    ) -> Result<Self, RedisError> {
        let factory = Arc::new(factory);
        let conn = match connect_with_timeout(factory.as_ref(), config.connect_timeout).await {
            Ok(conn) => conn,
            Err(error) => {
                if let Some(events) = &event_bus {
                    events.publish_with(|| ConnectionEvent::ConnectFailed {
                        error: Arc::from(error.to_string()),
                    });
                }
                return Err(error);
            }
        };
        let inner = match FrameService::from_connection(conn) {
            Ok(inner) => inner,
            Err(error) => {
                if let Some(events) = &event_bus {
                    events.publish_with(|| ConnectionEvent::ConnectFailed {
                        error: Arc::from(error.to_string()),
                    });
                }
                return Err(error);
            }
        };
        if let Some(events) = &event_bus {
            events.publish(ConnectionEvent::Connected);
        }
        let disconnect_reported = event_bus.as_ref().map(|_| Arc::new(AtomicBool::new(false)));
        let shutdown_reported = event_bus.as_ref().map(|_| Arc::new(AtomicBool::new(false)));
        let lifecycle_lock = event_bus.as_ref().map(|_| Arc::new(StdMutex::new(())));
        Ok(Self {
            inner,
            factory,
            config,
            state: State::Ready,
            needs_reconnect: Arc::new(AtomicBool::new(false)),
            event_bus,
            disconnect_reported,
            shutdown_reported,
            lifecycle_lock,
        })
    }

    fn trigger_reconnect(&mut self, attempt: usize, started: Instant) {
        if self.config.attempt_exhausted(attempt) {
            if let Some(events) = &self.event_bus {
                events.publish(ConnectionEvent::ReconnectExhausted { attempts: attempt });
            }
            self.state = State::Failed;
            return;
        }
        let delay = self.config.delay_for_attempt(attempt);
        if let Some(events) = &self.event_bus {
            events.publish(ConnectionEvent::ReconnectAttempt {
                attempt: attempt + 1,
                delay,
            });
        }
        self.state = State::WaitingToReconnect {
            attempt,
            sleep: Box::pin(tokio::time::sleep(delay)),
            started,
        };
    }
}

impl Drop for ReconnectService {
    fn drop(&mut self) {
        let _guard = self.lifecycle_lock.as_ref().map(|lock| {
            lock.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        publish_shutdown_once(self.event_bus.as_ref(), self.shutdown_reported.as_deref());
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
            self.trigger_reconnect(0, Instant::now());
        }

        loop {
            match &mut self.state {
                State::Ready => return self.inner.poll_ready(cx),
                State::Failed => {
                    return Poll::Ready(Err(RedisError::ReconnectFailed {
                        attempts: self.config.total_attempt_budget().unwrap_or(0),
                        last_error: Arc::new(RedisError::ConnectionClosed),
                    }));
                }
                State::WaitingToReconnect {
                    attempt,
                    sleep,
                    started,
                } => match sleep.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        let attempt = *attempt;
                        let started = *started;
                        let factory = Arc::clone(&self.factory);
                        let connect_timeout = self.config.connect_timeout;
                        let future: ReconnectFuture = Box::pin(async move {
                            let conn =
                                connect_with_timeout(factory.as_ref(), connect_timeout).await?;
                            FrameService::from_connection(conn)
                        });
                        self.state = State::Reconnecting {
                            attempt,
                            future,
                            started,
                        };
                    }
                    Poll::Pending => return Poll::Pending,
                },
                State::Reconnecting {
                    attempt,
                    future,
                    started,
                } => match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(new_svc)) => {
                        let attempts = *attempt + 1;
                        let elapsed = started.elapsed();
                        self.inner = new_svc;
                        self.state = State::Ready;
                        if let Some(disconnect_reported) = &self.disconnect_reported {
                            disconnect_reported.store(false, Ordering::Release);
                        }
                        if let Some(events) = &self.event_bus {
                            events.publish(ConnectionEvent::Reconnected { attempts, elapsed });
                        }
                        return self.inner.poll_ready(cx);
                    }
                    Poll::Ready(Err(error)) => {
                        let next = *attempt + 1;
                        let started = *started;
                        if let Some(events) = &self.event_bus {
                            events.publish_with(|| ConnectionEvent::ReconnectFailed {
                                attempt: next,
                                error: Arc::from(error.to_string()),
                            });
                        }
                        self.trigger_reconnect(next, started);
                    }
                    Poll::Pending => return Poll::Pending,
                },
            }
        }
    }

    fn call(&mut self, request: Frame) -> Self::Future {
        let future = self.inner.call(request);
        let needs_reconnect = Arc::clone(&self.needs_reconnect);
        let event_bus = self.event_bus.clone();
        let disconnect_reported = self.disconnect_reported.clone();
        let shutdown_reported = self.shutdown_reported.clone();
        let lifecycle_lock = self.lifecycle_lock.clone();

        Box::pin(async move {
            let result = future.await;
            if let Err(ref e) = result
                && e.is_connection_error()
            {
                needs_reconnect.store(true, Ordering::Release);
                publish_disconnect_before_shutdown(
                    event_bus.as_ref(),
                    disconnect_reported.as_deref(),
                    shutdown_reported.as_deref(),
                    lifecycle_lock.as_deref(),
                    e,
                );
            }
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    #[tokio::test]
    async fn frame_service_events_cover_disconnect_and_reconnect() {
        use tokio::sync::oneshot;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (first_closed_tx, first_closed_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            drop(first);
            let _ = first_closed_tx.send(());

            let (second, _) = listener.accept().await.unwrap();
            let _second = second;
            futures::future::pending::<()>().await;
        });

        let calls = Arc::new(AtomicUsize::new(0));
        let factory_calls = Arc::clone(&calls);
        let factory = move || {
            let factory_calls = Arc::clone(&factory_calls);
            async move {
                factory_calls.fetch_add(1, Ordering::AcqRel);
                let stream = tokio::net::TcpStream::connect(addr)
                    .await
                    .map_err(|error| RedisError::connection(addr.to_string(), error))?;
                Ok(redis_tower_core::RedisConnection::from_stream(
                    redis_tower_core::RedisStream::Tcp(stream),
                ))
            }
        };

        let events = ConnectionEventBus::new(8);
        let mut event_stream = events.subscribe();
        let config = ReconnectConfig {
            max_retries: Some(2),
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            jitter: false,
            connect_timeout: None,
        };
        let mut service = ReconnectService::new_with_events(factory, config, events)
            .await
            .unwrap();
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Connected
        );

        first_closed_rx.await.unwrap();
        futures::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        let result = service
            .call(Frame::SimpleString(bytes::Bytes::from_static(b"PING")))
            .await;
        assert!(result.is_err());
        futures::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();

        assert!(matches!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected { .. }
        ));
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 1,
                delay: Duration::ZERO,
            }
        );
        assert!(matches!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Reconnected { attempts: 1, .. }
        ));
        assert_eq!(calls.load(Ordering::Acquire), 2);

        drop(service);
        assert_eq!(
            event_stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: crate::reconnect::ConnectionDisconnectReason::Shutdown,
            }
        );

        server.abort();
    }

    #[tokio::test]
    async fn initial_factory_timeout_publishes_connect_failed() {
        let factory = || async {
            futures::future::pending::<Result<redis_tower_core::RedisConnection, RedisError>>()
                .await
        };
        let events = ConnectionEventBus::new(4);
        let mut stream = events.subscribe();
        let config = ReconnectConfig::default().connect_timeout(Duration::from_millis(10));

        let result = ReconnectService::new_with_events(factory, config, events).await;
        assert!(matches!(result, Err(RedisError::ConnectTimeout)));
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ConnectFailed {
                error: Arc::from(RedisError::ConnectTimeout.to_string()),
            }
        );
    }

    #[tokio::test]
    async fn reconnect_factory_timeout_exhausts_with_complete_event_order() {
        use tokio::sync::oneshot;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (closed_tx, closed_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            drop(first);
            let _ = closed_tx.send(());
        });

        let calls = Arc::new(AtomicUsize::new(0));
        let factory_calls = Arc::clone(&calls);
        let factory = move || {
            let factory_calls = Arc::clone(&factory_calls);
            async move {
                if factory_calls.fetch_add(1, Ordering::AcqRel) > 0 {
                    return futures::future::pending::<
                        Result<redis_tower_core::RedisConnection, RedisError>,
                    >()
                    .await;
                }
                let stream = tokio::net::TcpStream::connect(addr)
                    .await
                    .map_err(|error| RedisError::connection(addr.to_string(), error))?;
                Ok(redis_tower_core::RedisConnection::from_stream(
                    redis_tower_core::RedisStream::Tcp(stream),
                ))
            }
        };

        let events = ConnectionEventBus::new(8);
        let mut stream = events.subscribe();
        let config = ReconnectConfig {
            max_retries: Some(0),
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            jitter: false,
            connect_timeout: Some(Duration::from_millis(10)),
        };
        let mut service = ReconnectService::new_with_events(factory, config, events)
            .await
            .unwrap();
        assert_eq!(stream.recv().await.unwrap(), ConnectionEvent::Connected);

        closed_rx.await.unwrap();
        futures::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        assert!(
            service
                .call(Frame::SimpleString(bytes::Bytes::from_static(b"PING")))
                .await
                .is_err()
        );
        let reconnect = futures::future::poll_fn(|cx| service.poll_ready(cx)).await;
        assert!(matches!(
            reconnect,
            Err(RedisError::ReconnectFailed { attempts: 1, .. })
        ));

        assert!(matches!(
            stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected { .. }
        ));
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectAttempt {
                attempt: 1,
                delay: Duration::ZERO,
            }
        );
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectFailed {
                attempt: 1,
                error: Arc::from(RedisError::ConnectTimeout.to_string()),
            }
        );
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::ReconnectExhausted { attempts: 1 }
        );
        assert_eq!(calls.load(Ordering::Acquire), 2);

        drop(service);
        assert_eq!(
            stream.recv().await.unwrap(),
            ConnectionEvent::Disconnected {
                reason: crate::reconnect::ConnectionDisconnectReason::Shutdown,
            }
        );
        server.await.unwrap();
    }

    #[test]
    fn trigger_reconnect_transitions_to_failed_when_max_exceeded() {
        let config = ReconnectConfig::default().max_retries(3);
        assert!(!config.attempt_exhausted(0));
        assert!(!config.attempt_exhausted(3));
        assert!(config.attempt_exhausted(4));
        assert_eq!(config.total_attempt_budget(), Some(4));
    }

    #[test]
    fn infinite_retries_never_transition_to_failed() {
        let config = ReconnectConfig {
            max_retries: None,
            base_delay: Duration::from_millis(10),
            ..Default::default()
        };
        assert!(!config.attempt_exhausted(1_000_000));
    }
}
