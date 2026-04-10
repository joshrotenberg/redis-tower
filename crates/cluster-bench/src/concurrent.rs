//! Local experiment: per-node locking cluster client.
//!
//! Same public surface as `ClusterClient` for the narrow `get`/`set` paths the
//! bench exercises, but routes and executes commands under per-node locks
//! instead of a single cluster-wide mutex. This is *not* a production client --
//! it exists only to measure whether removing the global lock changes the
//! throughput picture enough to justify promoting the pattern upstream.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use redis_tower_cluster::slot::slot_for_key;
use redis_tower_cluster::topology::{ClusterTopology, NodeAddr, discover_topology};
use redis_tower_commands::{Get, Set};
use redis_tower_core::{Command, RedisConnection, RedisError};
use tokio::sync::{Mutex, RwLock};

const MAX_REDIRECTS: usize = 5;

struct Inner {
    topology: ClusterTopology,
    nodes: HashMap<String, Arc<Mutex<RedisConnection>>>,
    default_node: String,
}

pub struct ConcurrentClusterClient {
    inner: RwLock<Inner>,
}

impl ConcurrentClusterClient {
    pub async fn connect(seed_addr: &str) -> Result<Self, RedisError> {
        let mut seed_conn = RedisConnection::connect(seed_addr).await?;
        let topology = discover_topology(&mut seed_conn).await?;

        let mut nodes: HashMap<String, Arc<Mutex<RedisConnection>>> = HashMap::new();
        for addr in topology.master_addrs() {
            let addr_str = addr.addr_string();
            let conn = if addr_str == seed_addr {
                // Reuse the seed connection for its own node.
                std::mem::replace(&mut seed_conn, RedisConnection::connect(seed_addr).await?)
            } else {
                RedisConnection::connect(&addr_str).await?
            };
            nodes.insert(addr_str, Arc::new(Mutex::new(conn)));
        }

        let default_node = topology
            .master_addrs()
            .first()
            .map(|a| a.addr_string())
            .unwrap_or_else(|| seed_addr.to_string());

        Ok(Self {
            inner: RwLock::new(Inner {
                topology,
                nodes,
                default_node,
            }),
        })
    }

    async fn node_for_key(&self, key: &str) -> Option<(String, Arc<Mutex<RedisConnection>>)> {
        let slot = slot_for_key(key.as_bytes());
        let inner = self.inner.read().await;
        let addr_str = inner
            .topology
            .master_for_slot(slot)
            .map(|a| a.addr_string())
            .unwrap_or_else(|| inner.default_node.clone());
        inner.nodes.get(&addr_str).cloned().map(|c| (addr_str, c))
    }

    async fn handle_moved(&self, slot: u16, addr: &str) -> Result<(), RedisError> {
        let mut inner = self.inner.write().await;
        if !inner.nodes.contains_key(addr) {
            let conn = RedisConnection::connect(addr).await?;
            inner
                .nodes
                .insert(addr.to_string(), Arc::new(Mutex::new(conn)));
        }
        if let Some((host, port_str)) = addr.rsplit_once(':')
            && let Ok(port) = port_str.parse::<u16>()
        {
            for range in &mut inner.topology.slot_ranges {
                if slot >= range.start && slot <= range.end {
                    range.master = NodeAddr {
                        host: host.to_string(),
                        port,
                    };
                    break;
                }
            }
        }
        Ok(())
    }

    async fn execute_raw<Cmd: Command>(
        &self,
        cmd: Cmd,
        key: &str,
    ) -> Result<Cmd::Response, RedisError> {
        let frame = cmd.to_frame();
        let mut target = self
            .node_for_key(key)
            .await
            .ok_or_else(|| RedisError::Redis("no route".into()))?;

        for _ in 0..MAX_REDIRECTS {
            let response = {
                let mut guard = target.1.lock().await;
                let mut responses = guard.execute_pipeline(vec![frame.clone()]).await?;
                responses.pop().ok_or(RedisError::ConnectionClosed)?
            };

            match parse_redirect(&response) {
                Some((slot, addr)) => {
                    self.handle_moved(slot, &addr).await?;
                    let inner = self.inner.read().await;
                    let conn = inner
                        .nodes
                        .get(&addr)
                        .cloned()
                        .ok_or_else(|| RedisError::Redis("redirect target missing".into()))?;
                    target = (addr, conn);
                    continue;
                }
                None => {
                    if let redis_tower_core::Frame::Error(bytes) = &response {
                        return Err(RedisError::Redis(
                            String::from_utf8_lossy(bytes).into_owned(),
                        ));
                    }
                    return cmd.parse_response(response);
                }
            }
        }
        Err(RedisError::Redis("too many redirects".into()))
    }

    pub async fn set(&self, key: &str, value: &str) -> Result<(), RedisError> {
        self.execute_raw(Set::new(key, value), key)
            .await
            .map(|_| ())
    }

    pub async fn get(&self, key: &str) -> Result<Option<Bytes>, RedisError> {
        self.execute_raw(Get::new(key), key).await
    }
}

/// Minimal MOVED parser -- ignores ASK for the bench.
fn parse_redirect(frame: &redis_tower_core::Frame) -> Option<(u16, String)> {
    if let redis_tower_core::Frame::Error(bytes) = frame {
        let s = String::from_utf8_lossy(bytes);
        if let Some(rest) = s.strip_prefix("MOVED ") {
            let mut parts = rest.splitn(2, ' ');
            let slot = parts.next()?.parse::<u16>().ok()?;
            let addr = parts.next()?.trim().to_string();
            return Some((slot, addr));
        }
    }
    None
}
