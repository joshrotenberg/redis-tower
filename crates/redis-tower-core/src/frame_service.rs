//! A `Service<Frame, Response=Frame>` adapter for `RedisConnection`.
//!
//! Enables Frame-level Tower middleware (caching, logging, metrics)
//! that operates on raw RESP frames rather than typed commands.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::SinkExt;
use tokio::sync::Mutex;
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
/// ```ignore
/// use redis_tower_core::FrameService;
/// use redis_tower_protocol::helpers::{array, bulk};
/// use tower::ServiceExt;
///
/// let svc = FrameService::connect("127.0.0.1:6379").await?;
/// let response = svc.oneshot(array(vec![bulk("PING")])).await?;
/// ```
pub struct FrameService {
    framed: Arc<Mutex<Framed<RedisStream, RespCodec>>>,
    push_tx: Arc<Mutex<Option<tokio::sync::mpsc::UnboundedSender<Frame>>>>,
}

impl FrameService {
    /// Connect to a Redis server and create a FrameService.
    pub async fn connect(addr: &str) -> Result<Self, RedisError> {
        let conn = crate::connection::RedisConnection::connect(addr).await?;
        Ok(Self::from_connection(conn))
    }

    /// Create from an existing `RedisConnection`.
    pub fn from_connection(conn: crate::connection::RedisConnection) -> Self {
        // We need access to the inner Arc<Mutex<Framed>> and push_tx.
        // RedisConnection exposes these via into_framed, but that consumes it.
        // Instead, we'll clone the Arcs.
        //
        // For now, create a new FrameService that shares the same internals.
        // We need RedisConnection to expose its fields for this.
        //
        // Actually, let's just store the connection and delegate.
        Self {
            framed: conn.framed_arc(),
            push_tx: conn.push_tx_arc(),
        }
    }

    /// Subscribe to RESP3 push messages.
    pub async fn subscribe_pushes(&self) -> tokio::sync::mpsc::UnboundedReceiver<Frame> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut guard = self.push_tx.lock().await;
        *guard = Some(tx);
        rx
    }
}

impl tower_service::Service<Frame> for FrameService {
    type Response = Frame;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Frame, RedisError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Frame) -> Self::Future {
        let framed = Arc::clone(&self.framed);
        let push_tx = Arc::clone(&self.push_tx);
        Box::pin(async move {
            let mut guard = framed.lock().await;
            guard.send(request).await.map_err(RedisError::from)?;

            // Read response, routing push frames.
            loop {
                let frame = guard
                    .next()
                    .await
                    .ok_or(RedisError::ConnectionClosed)?
                    .map_err(RedisError::from)?;

                if let Frame::Push(_) = &frame {
                    let ptx = push_tx.lock().await;
                    if let Some(ref tx) = *ptx {
                        let _ = tx.send(frame);
                    }
                    continue;
                }

                return Ok(frame);
            }
        })
    }
}
