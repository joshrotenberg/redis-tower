//! A `Service<Frame, Response=Frame>` adapter for `RedisConnection`.
//!
//! Enables Frame-level Tower middleware (caching, logging, metrics)
//! that operates on raw RESP frames rather than typed commands.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Sink;
use futures::SinkExt;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

use crate::error::RedisError;
use crate::stream::RedisStream;
use redis_tower_protocol::{Frame, RespCodec};

/// A Tower `Service` that sends and receives raw RESP frames.
///
/// This is the lowest-level service primitive. It sends a `Frame` on the
/// wire and returns the response `Frame`. No command parsing, no type
/// safety -- just raw frame I/O.
///
/// Use this as the inner service for Frame-level middleware (caching,
/// logging, metrics), then wrap with `CommandAdapter` to restore
/// typed command dispatch.
///
/// # Example
///
/// ```no_run
/// use futures::future::poll_fn;
/// use redis_tower_core::FrameService;
/// use redis_tower_protocol::helpers::{array, bulk};
/// use tower_service::Service;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut svc = FrameService::connect("127.0.0.1:6379").await?;
/// poll_fn(|cx| svc.poll_ready(cx)).await?;
/// let response = svc.call(array(vec![bulk("PING")])).await?;
/// # let _ = response;
/// # Ok(())
/// # }
/// ```
pub struct FrameService {
    /// The framed transport. `None` while a `Service::call` future is in flight.
    framed: Option<Framed<RedisStream, RespCodec>>,
    /// Optional sender for RESP3 push messages.
    push_tx: Option<tokio::sync::mpsc::UnboundedSender<Frame>>,
    /// Channel to reclaim the framed transport after a `Service::call` completes.
    inflight: Option<oneshot::Receiver<Framed<RedisStream, RespCodec>>>,
}

impl FrameService {
    /// Connect to a Redis server and create a FrameService.
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        let conn = crate::connection::RedisConnection::connect(addr).await?;
        Self::from_connection(conn)
    }

    /// Create from an existing `RedisConnection`, consuming it.
    pub fn from_connection(conn: crate::connection::RedisConnection) -> Result<Self, RedisError> {
        let framed = conn.into_framed()?;
        Ok(Self {
            framed: Some(framed),
            push_tx: None,
            inflight: None,
        })
    }

    /// Subscribe to RESP3 push messages.
    ///
    /// Returns a receiver for out-of-band push frames (e.g., invalidation
    /// messages from CLIENT TRACKING). Push frames received during normal
    /// command execution are automatically routed to this channel.
    pub fn subscribe_pushes(&mut self) -> tokio::sync::mpsc::UnboundedReceiver<Frame> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.push_tx = Some(tx);
        rx
    }
}

/// Guard that returns the framed transport via the oneshot channel on drop.
///
/// This ensures the transport is not leaked when a `Service::call` future is
/// cancelled (e.g., by `tokio::time::timeout`, `select!`, or task abort).
/// On the success path the future takes the fields out of the guard before
/// it is dropped, so the `Drop` impl becomes a no-op.
struct FrameGuardFs {
    framed: Option<Framed<RedisStream, RespCodec>>,
    return_tx: Option<oneshot::Sender<Framed<RedisStream, RespCodec>>>,
}

impl Drop for FrameGuardFs {
    fn drop(&mut self) {
        if let (Some(framed), Some(tx)) = (self.framed.take(), self.return_tx.take()) {
            let _ = tx.send(framed);
        }
    }
}

impl tower_service::Service<Frame> for FrameService {
    type Response = Frame;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Reclaim the transport from an in-flight future if needed.
        if self.framed.is_none() {
            if let Some(ref mut rx) = self.inflight {
                match Pin::new(rx).poll(cx) {
                    Poll::Ready(Ok(framed)) => {
                        self.framed = Some(framed);
                        self.inflight = None;
                    }
                    Poll::Ready(Err(_)) => {
                        self.inflight = None;
                        return Poll::Ready(Err(RedisError::ConnectionClosed));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            } else {
                return Poll::Ready(Err(RedisError::ConnectionClosed));
            }
        }

        let framed = self.framed.as_mut().unwrap();
        Pin::new(framed).poll_ready(cx).map_err(RedisError::from)
    }

    fn call(&mut self, request: Frame) -> Self::Future {
        let mut framed = self
            .framed
            .take()
            .expect("call() invoked without successful poll_ready()");
        let push_tx = self.push_tx.clone();

        if let Err(e) = Pin::new(&mut framed).start_send(request) {
            self.framed = Some(framed);
            return Box::pin(async move { Err(RedisError::from(e)) });
        }

        let (return_tx, return_rx) = oneshot::channel();
        self.inflight = Some(return_rx);

        // Use a guard to ensure the framed transport is returned even if the
        // future is dropped (e.g., timeout, select!, task cancellation).
        let mut guard = FrameGuardFs {
            framed: Some(framed),
            return_tx: Some(return_tx),
        };

        Box::pin(async move {
            let framed = guard.framed.as_mut().unwrap();

            framed.flush().await.map_err(RedisError::from)?;

            // Read response, routing push frames.
            let response = loop {
                let frame = framed
                    .next()
                    .await
                    .ok_or(RedisError::ConnectionClosed)?
                    .map_err(RedisError::from)?;

                if let Frame::Push(_) = &frame {
                    if let Some(ref tx) = push_tx {
                        let _ = tx.send(frame);
                    }
                    continue;
                }

                break frame;
            };

            // Explicitly return the transport on success (disarms the guard).
            let _ = guard
                .return_tx
                .take()
                .unwrap()
                .send(guard.framed.take().unwrap());
            Ok(response)
        })
    }
}
