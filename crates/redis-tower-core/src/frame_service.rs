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
/// Cancelling an in-flight call closes the connection. Once a request may
/// have reached Redis, reusing its transport could let the next call consume
/// the cancelled request's late response. A subsequent `poll_ready` therefore
/// returns [`RedisError::ConnectionClosed`].
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

/// Owns the framed transport while a `Service::call` future is in flight.
///
/// The transport is returned through `return_tx` only after a complete
/// response frame has been read. Dropping the future, or encountering a
/// transport/protocol error before that boundary, drops both fields and closes
/// the connection rather than leaving a late response for the next request.
struct FrameGuardFs {
    framed: Option<Framed<RedisStream, RespCodec>>,
    return_tx: Option<oneshot::Sender<Framed<RedisStream, RespCodec>>>,
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
            // A sink error leaves the connection state uncertain. Quarantine
            // it rather than allowing a later call to reuse the transport.
            return Box::pin(async move { Err(RedisError::from(e)) });
        }

        let (return_tx, return_rx) = oneshot::channel();
        self.inflight = Some(return_rx);

        // Keep the transport owned by the call future. Cancellation or an I/O
        // error before a complete response drops it; a complete response
        // restores protocol alignment regardless of the frame's contents.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tower_service::Service;

    async fn stream_pair() -> (RedisStream, RedisStream) {
        #[cfg(unix)]
        {
            let (client, server) = tokio::net::UnixStream::pair().unwrap();
            (RedisStream::Unix(client), RedisStream::Unix(server))
        }
        #[cfg(not(unix))]
        {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let client = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (server, _) = listener.accept().await.unwrap();
            (RedisStream::Tcp(client), RedisStream::Tcp(server))
        }
    }

    #[tokio::test]
    async fn timeout_after_wire_quarantines_frame_transport() {
        let (client, server) = stream_pair().await;
        let (wire_tx, mut wire_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (late_attempt_tx, late_attempt_rx) = oneshot::channel();

        let server_task = tokio::spawn(async move {
            let mut framed = Framed::new(server, RespCodec::new());
            framed
                .next()
                .await
                .expect("client closed before sending its frame")
                .expect("client sent an invalid frame");
            wire_tx.send(()).unwrap();

            release_rx.await.unwrap();
            let late_result = framed.send(Frame::SimpleString(b"LATE"[..].into())).await;
            late_attempt_tx.send(()).unwrap();

            if late_result.is_ok() {
                match tokio::time::timeout(Duration::from_secs(1), framed.next()).await {
                    Ok(None) | Ok(Some(Err(_))) => {}
                    Ok(Some(Ok(frame))) => panic!("timed-out socket was reused: {frame:?}"),
                    Err(_) => panic!("timed-out socket remained open"),
                }
            }
        });

        let conn = crate::connection::RedisConnection::from_stream(client);
        let mut service = FrameService::from_connection(conn).unwrap();
        futures::future::poll_fn(|cx| Service::<Frame>::poll_ready(&mut service, cx))
            .await
            .unwrap();
        let mut call =
            Service::<Frame>::call(&mut service, Frame::SimpleString(b"REQUEST"[..].into()));

        // First drive the call until the server has decoded the request. The
        // response remains withheld, so the timeout deterministically owns
        // cancellation after the write has reached the peer.
        tokio::select! {
            result = &mut call => panic!("call completed before timeout: {result:?}"),
            observed = &mut wire_rx => observed.unwrap(),
        }
        let timeout_result = tokio::time::timeout(Duration::from_millis(10), call).await;
        assert!(
            timeout_result.is_err(),
            "withheld response did not time out"
        );

        release_tx.send(()).unwrap();
        late_attempt_rx.await.unwrap();

        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        let readiness = Service::<Frame>::poll_ready(&mut service, &mut cx);
        let is_closed = matches!(readiness, Poll::Ready(Err(RedisError::ConnectionClosed)));
        drop(service);
        server_task.await.unwrap();

        assert!(
            is_closed,
            "timed-out connection was reusable: {readiness:?}"
        );
    }
}
