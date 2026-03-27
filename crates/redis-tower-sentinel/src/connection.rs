//! Sentinel-managed Redis connection with automatic failover.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use redis_tower_core::{Command, Frame, RedisConnection, RedisError};

use crate::discovery;

/// A Redis connection managed by Sentinel.
///
/// Discovers the current master via Sentinel and connects to it.
/// When a command fails with a connection error, the next call
/// rediscovers the master (which may have changed due to failover).
///
/// # Example
///
/// ```ignore
/// use redis_tower_sentinel::SentinelConnection;
///
/// let mut conn = SentinelConnection::connect(
///     &["127.0.0.1:26379", "127.0.0.1:26380", "127.0.0.1:26381"],
///     "mymaster",
/// ).await?;
///
/// conn.execute(Set::new("key", "value")).await?;
/// ```
pub struct SentinelConnection {
    /// Current connection to the master.
    conn: RedisConnection,
    /// Sentinel addresses for rediscovery.
    sentinel_addrs: Vec<String>,
    /// Monitored master name.
    master_name: String,
    /// Whether the connection needs rediscovery.
    needs_rediscovery: bool,
}

impl SentinelConnection {
    /// Connect to the Redis master discovered via Sentinel.
    pub async fn connect(
        sentinel_addrs: &[impl AsRef<str>],
        master_name: &str,
    ) -> Result<Self, RedisError> {
        let addrs: Vec<String> = sentinel_addrs
            .iter()
            .map(|a| a.as_ref().to_string())
            .collect();
        let master_addr = discovery::discover_master(&addrs, master_name).await?;
        let conn = RedisConnection::connect(&master_addr).await?;

        Ok(Self {
            conn,
            sentinel_addrs: addrs,
            master_name: master_name.to_string(),
            needs_rediscovery: false,
        })
    }

    /// Execute a command against the current master.
    ///
    /// If the connection was marked as needing rediscovery (after a
    /// previous connection error), rediscovers the master first.
    pub async fn execute<Cmd: Command>(&mut self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        if self.needs_rediscovery {
            self.rediscover().await?;
        }

        let result = self.conn.execute(cmd).await;
        if let Err(ref e) = result {
            if is_connection_error(e) {
                self.needs_rediscovery = true;
            }
        }
        result
    }

    /// Force rediscovery of the master and reconnect.
    pub async fn rediscover(&mut self) -> Result<(), RedisError> {
        let master_addr =
            discovery::discover_master(&self.sentinel_addrs, &self.master_name).await?;
        self.conn = RedisConnection::connect(&master_addr).await?;
        self.needs_rediscovery = false;
        Ok(())
    }

    /// Get the sentinel addresses.
    pub fn sentinel_addrs(&self) -> &[String] {
        &self.sentinel_addrs
    }

    /// Get the monitored master name.
    pub fn master_name(&self) -> &str {
        &self.master_name
    }

    /// Discover current replica addresses from sentinel.
    pub async fn discover_replicas(&self) -> Result<Vec<String>, RedisError> {
        discovery::discover_replicas(&self.sentinel_addrs, &self.master_name).await
    }
}

impl<Cmd: Command + 'static> tower_service::Service<Cmd> for SentinelConnection {
    type Response = Cmd::Response;
    type Error = RedisError;
    type Future = Pin<Box<dyn Future<Output = Result<Cmd::Response, RedisError>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, cmd: Cmd) -> Self::Future {
        // Delegate to the inner RedisConnection's Service impl.
        let framed = self.conn.framed_arc();
        let push_tx = self.conn.push_tx_arc();

        Box::pin(async move {
            use futures::SinkExt;
            use tokio_stream::StreamExt;

            let frame = cmd.to_frame();
            let mut guard = framed.lock().await;
            guard.send(frame).await.map_err(RedisError::from)?;

            // Read response, routing push frames.
            let response = loop {
                let f = guard
                    .next()
                    .await
                    .ok_or(RedisError::ConnectionClosed)?
                    .map_err(RedisError::from)?;
                if let Frame::Push(_) = &f {
                    let ptx = push_tx.lock().await;
                    if let Some(ref tx) = *ptx {
                        let _ = tx.send(f);
                    }
                    continue;
                }
                break f;
            };

            if let Frame::Error(ref e) = response {
                return Err(RedisError::Redis(String::from_utf8_lossy(e).into_owned()));
            }

            cmd.parse_response(response)
        })
    }
}

fn is_connection_error(err: &RedisError) -> bool {
    matches!(
        err,
        RedisError::Connection(_) | RedisError::ConnectionClosed
    )
}
