//! Local experiment: per-node multiplexed cluster client.
//!
//! Same shape as `ConcurrentClusterClient`, but each per-node entry is a
//! `MultiplexedClient` (background worker + auto-pipeline) instead of a raw
//! `RedisConnection` behind a mutex. The lookup path is read-lock only, and
//! the per-node client handles concurrent in-flight requests over a single
//! TCP connection via batching.

use std::collections::HashMap;

use bytes::Bytes;
use redis_tower::MultiplexedClient;
use redis_tower_cluster::slot::slot_for_key;
use redis_tower_cluster::topology::{ClusterTopology, NodeAddr, discover_topology};
use redis_tower_commands::{Get, Set};
use redis_tower_core::{RedisConnection, RedisError};
use tokio::sync::RwLock;

const MAX_REDIRECTS: usize = 5;

struct Inner {
    topology: ClusterTopology,
    nodes: HashMap<String, MultiplexedClient>,
    default_node: String,
}

pub struct MultiplexedClusterClient {
    inner: RwLock<Inner>,
}

impl MultiplexedClusterClient {
    pub async fn connect(seed_addr: &str) -> Result<Self, RedisError> {
        // Use a short-lived raw connection to discover topology.
        let mut seed_conn = RedisConnection::connect(seed_addr).await?;
        let topology = discover_topology(&mut seed_conn).await?;
        drop(seed_conn);

        let mut nodes: HashMap<String, MultiplexedClient> = HashMap::new();
        for addr in topology.master_addrs() {
            let addr_str = addr.addr_string();
            let mclient = MultiplexedClient::connect(&addr_str).await?;
            nodes.insert(addr_str, mclient);
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

    async fn client_for_key(&self, key: &str) -> Result<MultiplexedClient, RedisError> {
        let slot = slot_for_key(key.as_bytes());
        let inner = self.inner.read().await;
        let addr_str = inner
            .topology
            .master_for_slot(slot)
            .map(|a| a.addr_string())
            .unwrap_or_else(|| inner.default_node.clone());
        inner
            .nodes
            .get(&addr_str)
            .cloned()
            .ok_or_else(|| RedisError::Redis(format!("no client for {addr_str}")))
    }

    async fn handle_moved(&self, slot: u16, addr: &str) -> Result<(), RedisError> {
        let mut inner = self.inner.write().await;
        if !inner.nodes.contains_key(addr) {
            let mclient = MultiplexedClient::connect(addr).await?;
            inner.nodes.insert(addr.to_string(), mclient);
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

    pub async fn set(&self, key: &str, value: &str) -> Result<(), RedisError> {
        for _ in 0..MAX_REDIRECTS {
            let client = self.client_for_key(key).await?;
            match client.execute(Set::new(key, value)).await {
                Ok(_) => return Ok(()),
                Err(RedisError::Redis(ref e)) if e.starts_with("MOVED ") => {
                    if let Some((slot, addr)) = parse_moved(e) {
                        self.handle_moved(slot, &addr).await?;
                    } else {
                        return Err(RedisError::Redis(e.clone()));
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Err(RedisError::Redis("too many redirects".into()))
    }

    pub async fn get(&self, key: &str) -> Result<Option<Bytes>, RedisError> {
        for _ in 0..MAX_REDIRECTS {
            let client = self.client_for_key(key).await?;
            match client.execute(Get::new(key)).await {
                Ok(v) => return Ok(v),
                Err(RedisError::Redis(ref e)) if e.starts_with("MOVED ") => {
                    if let Some((slot, addr)) = parse_moved(e) {
                        self.handle_moved(slot, &addr).await?;
                    } else {
                        return Err(RedisError::Redis(e.clone()));
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Err(RedisError::Redis("too many redirects".into()))
    }
}

fn parse_moved(msg: &str) -> Option<(u16, String)> {
    let rest = msg.strip_prefix("MOVED ")?;
    let mut parts = rest.splitn(2, ' ');
    let slot = parts.next()?.parse::<u16>().ok()?;
    let addr = parts.next()?.trim().to_string();
    Some((slot, addr))
}
