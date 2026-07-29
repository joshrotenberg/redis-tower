//! Cluster-wide SCAN iteration.
//!
//! `SCAN` iterates the keyspace of the single node it is sent to. It carries no
//! key, so slot routing has nothing to route on and
//! [`MultiplexedClusterClient::execute`] sends it to the default node --
//! returning roughly a third of a three-master cluster's keys with no
//! indication that anything was missed.
//!
//! [`ScanClusterStream`] runs a `SCAN` cursor loop against every master in
//! turn and yields each key tagged with the node it came from.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use futures::StreamExt;
//! use redis_tower_cluster::{MultiplexedClusterClient, ScanClusterStream};
//!
//! let client = MultiplexedClusterClient::connect("127.0.0.1:7000").await?;
//! let mut stream = std::pin::pin!(ScanClusterStream::scan(&client, "user:*"));
//! while let Some(item) = stream.next().await {
//!     let item = item?;
//!     println!("{} on {}", String::from_utf8_lossy(&item.key), item.node);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Guarantees
//!
//! Redis's `SCAN` guarantee -- a key present for the whole iteration is
//! returned at least once, and a key may be returned more than once -- holds
//! per node, and so holds cluster-wide for a cluster whose slot assignment does
//! not change during the scan.
//!
//! The node set is snapshotted once, when the stream is first polled. A slot
//! migrating between masters mid-scan can therefore be missed (if it moves from
//! a node not yet visited to one already visited) or seen twice. Re-checking
//! membership mid-scan is left for a follow-up; until then, treat a scan run
//! during a live resharding as approximate.

use bytes::Bytes;
use futures::Stream;
use redis_tower_commands::Scan;
use redis_tower_core::RedisError;

use crate::multiplexed::MultiplexedClusterClient;

/// One key produced by a cluster-wide scan, tagged with its source node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterScanItem {
    /// The `host:port` of the master node this key was scanned from.
    ///
    /// This is the address the client holds a service for, so it reflects any
    /// `host_override` or `address_map` remapping rather than the address the
    /// node reports for itself.
    pub node: String,
    /// The key.
    pub key: Bytes,
}

/// Async stream wrappers for cluster-wide SCAN iteration.
///
/// Each method returns an owned `impl Stream` -- the client is cheap to clone
/// and every call takes `&self`, so unlike
/// [`ScanStream`](redis_tower::ScanStream) the returned stream does not borrow
/// the client.
pub struct ScanClusterStream;

impl ScanClusterStream {
    /// Iterate over all keys matching a pattern across every master node.
    ///
    /// Yields one [`ClusterScanItem`] per key. Nodes are visited sequentially,
    /// in sorted address order, each driven to its own cursor `"0"` before the
    /// next begins.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use futures::StreamExt;
    /// use redis_tower_cluster::{MultiplexedClusterClient, ScanClusterStream};
    ///
    /// let client = MultiplexedClusterClient::connect("127.0.0.1:7000").await?;
    /// let mut stream = std::pin::pin!(ScanClusterStream::scan(&client, "user:*"));
    /// while let Some(item) = stream.next().await {
    ///     let item = item?;
    ///     println!("{}", String::from_utf8_lossy(&item.key));
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn scan(
        client: &MultiplexedClusterClient,
        pattern: impl Into<String>,
    ) -> impl Stream<Item = Result<ClusterScanItem, RedisError>> + 'static {
        scan_inner(client.clone(), pattern.into(), None)
    }

    /// Iterate over all keys matching a pattern across every master node, with
    /// a `COUNT` hint.
    ///
    /// The hint is passed to each per-node `SCAN`, so it bounds the work per
    /// round trip per node, not the total. Redis may return more or fewer.
    pub fn scan_with_count(
        client: &MultiplexedClusterClient,
        pattern: impl Into<String>,
        count: u64,
    ) -> impl Stream<Item = Result<ClusterScanItem, RedisError>> + 'static {
        scan_inner(client.clone(), pattern.into(), Some(count))
    }
}

/// The per-node cursor loop shared by both entry points.
///
/// An error from any node ends the whole stream: a partial cluster-wide scan
/// that reported success would be indistinguishable from a complete one, and
/// the caller cannot tell which keys are missing.
fn scan_inner(
    client: MultiplexedClusterClient,
    pattern: String,
    count: Option<u64>,
) -> impl Stream<Item = Result<ClusterScanItem, RedisError>> + 'static {
    async_stream::try_stream! {
        // Snapshotted once, on first poll. See the module docs on resharding.
        let nodes = client.master_service_addrs().await;
        for node in nodes {
            let mut cursor = "0".to_string();
            loop {
                let mut cmd = Scan::new().match_pattern(&pattern).cursor(&cursor);
                if let Some(n) = count {
                    cmd = cmd.count(n);
                }
                let result = client.execute_on_node(&node, cmd).await?;
                cursor = result.cursor;
                for key in result.results {
                    yield ClusterScanItem {
                        node: node.clone(),
                        key,
                    };
                }
                if cursor == "0" {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Every SCAN command each fake node received, keyed by node address and
    /// recorded as the raw argument list.
    type ScanLog = Arc<Mutex<HashMap<String, Vec<Vec<String>>>>>;

    /// Pull one complete RESP array-of-bulk-strings request off the front of
    /// `buf`, consuming its bytes. Returns `None` (consuming nothing) when the
    /// buffer does not yet hold a whole request.
    ///
    /// Clients only ever send arrays of bulk strings, so this deliberately
    /// handles nothing else. It is a request reader, not a RESP parser -- but
    /// it does have to be exact, because the auto-pipeline worker can deliver
    /// several commands in one read and one reply per read would desync.
    fn take_request(buf: &mut Vec<u8>) -> Option<Vec<String>> {
        fn read_line(buf: &[u8], pos: &mut usize) -> Option<String> {
            let start = *pos;
            let idx = buf
                .get(start..)?
                .windows(2)
                .position(|w| w == b"\r\n")
                .map(|i| start + i)?;
            *pos = idx + 2;
            Some(String::from_utf8_lossy(&buf[start..idx]).into_owned())
        }

        let mut pos = 0usize;
        let count: usize = read_line(buf, &mut pos)?
            .strip_prefix('*')
            .and_then(|n| n.parse().ok())?;

        let mut args = Vec::with_capacity(count);
        for _ in 0..count {
            let len: usize = read_line(buf, &mut pos)?
                .strip_prefix('$')
                .and_then(|n| n.parse().ok())?;
            if buf.len() < pos + len + 2 {
                return None;
            }
            args.push(String::from_utf8_lossy(&buf[pos..pos + len]).into_owned());
            pos += len + 2;
        }

        buf.drain(..pos);
        Some(args)
    }

    fn bulk_bytes(s: &str) -> Vec<u8> {
        format!("${}\r\n{}\r\n", s.len(), s).into_bytes()
    }

    /// A `[cursor, [keys...]]` SCAN reply.
    fn scan_reply(cursor: &str, keys: &[String]) -> Vec<u8> {
        let mut out = b"*2\r\n".to_vec();
        out.extend_from_slice(&bulk_bytes(cursor));
        out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
        for k in keys {
            out.extend_from_slice(&bulk_bytes(k));
        }
        out
    }

    /// A CLUSTER SLOTS reply splitting the slot space evenly across `addrs`.
    fn cluster_slots_reply(addrs: &[String]) -> Vec<u8> {
        let per = 16384 / addrs.len();
        let mut out = format!("*{}\r\n", addrs.len()).into_bytes();
        for (i, addr) in addrs.iter().enumerate() {
            let start = i * per;
            let end = if i == addrs.len() - 1 {
                16383
            } else {
                (i + 1) * per - 1
            };
            let (host, port) = addr.rsplit_once(':').unwrap();
            out.extend_from_slice(b"*3\r\n");
            out.extend_from_slice(format!(":{start}\r\n:{end}\r\n").as_bytes());
            out.extend_from_slice(b"*2\r\n");
            out.extend_from_slice(&bulk_bytes(host));
            out.extend_from_slice(format!(":{port}\r\n").as_bytes());
        }
        out
    }

    /// How a fake node answers SCAN.
    #[derive(Clone, Copy, PartialEq)]
    enum ScanBehavior {
        /// Serve three keys over two pages, each key named after this node.
        TwoPages,
        /// Reply with an error frame.
        Fail,
    }

    /// Spawn a fake cluster master.
    ///
    /// `RedisConnection::connect` opens with two best-effort `CLIENT SETINFO`
    /// and a `HELLO 3` whose failure is the RESP2 fallback, so answering `-ERR`
    /// to everything not implemented here is enough for a connect to succeed.
    ///
    /// The node names its own keys `<addr>:a|b|c`, which is what makes the
    /// per-key node tag exactly checkable rather than merely plausible.
    ///
    /// SCAN pages are counted per connection. The client holds one long-lived
    /// connection per node, and the short-lived topology-discovery connection
    /// never sends SCAN, so per-connection state is per-node state here.
    async fn spawn_node(
        behavior: ScanBehavior,
        slots: Arc<Mutex<Option<Vec<u8>>>>,
        log: ScanLog,
    ) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let node_addr = addr.clone();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let slots = slots.clone();
                let log = log.clone();
                let node_addr = node_addr.clone();
                tokio::spawn(async move {
                    let pages = [
                        (
                            "1".to_string(),
                            vec![format!("{node_addr}:a"), format!("{node_addr}:b")],
                        ),
                        ("0".to_string(), vec![format!("{node_addr}:c")]),
                    ];
                    let mut buf: Vec<u8> = Vec::new();
                    let mut chunk = [0u8; 4096];
                    let mut page = 0usize;
                    loop {
                        let n = match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        let mut out: Vec<u8> = Vec::new();
                        while let Some(args) = take_request(&mut buf) {
                            match args[0].to_uppercase().as_str() {
                                "CLUSTER" => match slots.lock().unwrap().clone() {
                                    Some(bytes) => out.extend_from_slice(&bytes),
                                    None => out.extend_from_slice(b"-ERR no topology\r\n"),
                                },
                                "SCAN" => {
                                    log.lock()
                                        .unwrap()
                                        .entry(node_addr.clone())
                                        .or_default()
                                        .push(args.clone());
                                    if behavior == ScanBehavior::Fail {
                                        out.extend_from_slice(b"-ERR scan refused\r\n");
                                    } else {
                                        let (cursor, keys) = pages
                                            .get(page)
                                            .cloned()
                                            .unwrap_or_else(|| ("0".to_string(), Vec::new()));
                                        page += 1;
                                        out.extend_from_slice(&scan_reply(&cursor, &keys));
                                    }
                                }
                                _ => out.extend_from_slice(b"-ERR fake node\r\n"),
                            }
                        }
                        if !out.is_empty() && sock.write_all(&out).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        addr
    }

    /// A connected client over three fake masters, plus their addresses in the
    /// order the scan should visit them and the shared SCAN log.
    async fn fake_cluster(
        behaviors: [ScanBehavior; 3],
    ) -> (MultiplexedClusterClient, Vec<String>, ScanLog) {
        let slots: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
        let log: ScanLog = Arc::new(Mutex::new(HashMap::new()));

        let mut addrs = Vec::new();
        for behavior in behaviors {
            addrs.push(spawn_node(behavior, slots.clone(), log.clone()).await);
        }
        // Every node can serve discovery, so seeding from any of them works.
        *slots.lock().unwrap() = Some(cluster_slots_reply(&addrs));

        let client = MultiplexedClusterClient::connect(&addrs[0])
            .await
            .expect("fake cluster should connect");

        let mut sorted = addrs;
        sorted.sort();
        (client, sorted, log)
    }

    async fn healthy_cluster() -> (MultiplexedClusterClient, Vec<String>, ScanLog) {
        fake_cluster([ScanBehavior::TwoPages; 3]).await
    }

    /// The nodes in the order they were tagged, with consecutive duplicates
    /// collapsed, so a node visited twice would show up twice.
    fn visit_order(items: &[ClusterScanItem]) -> Vec<String> {
        items.iter().fold(Vec::new(), |mut acc, item| {
            if acc.last() != Some(&item.node) {
                acc.push(item.node.clone());
            }
            acc
        })
    }

    #[tokio::test]
    async fn a_cluster_scan_visits_every_master_and_tags_each_key() {
        let (client, sorted_addrs, _log) = healthy_cluster().await;

        let items: Vec<ClusterScanItem> = std::pin::pin!(ScanClusterStream::scan(&client, "*"))
            .map(|r| r.expect("scan should succeed"))
            .collect()
            .await;

        assert_eq!(items.len(), 9, "three keys from each of three nodes");
        assert_eq!(
            visit_order(&items),
            sorted_addrs,
            "every master visited exactly once, in sorted address order"
        );

        // Each fake node names its keys after itself, so a key tagged with the
        // wrong node is caught exactly rather than by counting.
        for item in &items {
            let key = String::from_utf8_lossy(&item.key);
            assert_eq!(
                key.rsplit_once(':').unwrap().0,
                item.node,
                "key {key} carries the wrong node tag"
            );
        }
    }

    #[tokio::test]
    async fn each_node_is_paged_until_its_cursor_returns_to_zero() {
        let (client, sorted_addrs, log) = healthy_cluster().await;

        let items: Vec<ClusterScanItem> =
            std::pin::pin!(ScanClusterStream::scan(&client, "user:*"))
                .map(|r| r.expect("scan should succeed"))
                .collect()
                .await;
        assert_eq!(items.len(), 9);

        let log = log.lock().unwrap();
        for addr in &sorted_addrs {
            let calls = log.get(addr).unwrap_or_else(|| panic!("no SCAN on {addr}"));
            assert_eq!(calls.len(), 2, "{addr} should have been paged twice");
            assert_eq!(calls[0][1], "0", "the first page starts at cursor 0");
            assert_eq!(
                calls[1][1], "1",
                "the second page continues from the returned cursor"
            );
            for call in calls {
                assert!(
                    call.contains(&"user:*".to_string()),
                    "the pattern is forwarded to every node"
                );
            }
        }
    }

    /// The behaviour this module exists to fix. `SCAN` carries no key, so a
    /// plain `execute` routes it to the default node and returns that node's
    /// keys only, with no indication the rest of the cluster was never asked.
    #[tokio::test]
    async fn a_plain_scan_reaches_only_one_node() {
        let (client, _sorted_addrs, log) = healthy_cluster().await;

        let result = client
            .execute(Scan::new().match_pattern("*"))
            .await
            .expect("scan should succeed");
        assert_eq!(result.results.len(), 2, "one page, from one node");

        let log = log.lock().unwrap();
        assert_eq!(log.len(), 1, "exactly one node saw the command");
    }

    #[tokio::test]
    async fn scan_with_count_forwards_the_hint_to_every_node() {
        let (client, sorted_addrs, log) = healthy_cluster().await;

        let items: Vec<ClusterScanItem> =
            std::pin::pin!(ScanClusterStream::scan_with_count(&client, "*", 32))
                .map(|r| r.expect("scan should succeed"))
                .collect()
                .await;
        assert_eq!(items.len(), 9);

        let log = log.lock().unwrap();
        for addr in &sorted_addrs {
            for call in log.get(addr).expect("node scanned") {
                let i = call
                    .iter()
                    .position(|a| a.eq_ignore_ascii_case("COUNT"))
                    .expect("COUNT is forwarded");
                assert_eq!(call[i + 1], "32");
            }
        }
    }

    /// A failing node ends the stream rather than being skipped: a scan that
    /// silently omitted one master's keys would be indistinguishable from a
    /// complete one.
    #[tokio::test]
    async fn an_error_from_one_node_ends_the_stream() {
        let (client, sorted_addrs, log) = fake_cluster([
            ScanBehavior::TwoPages,
            ScanBehavior::Fail,
            ScanBehavior::TwoPages,
        ])
        .await;

        let mut stream = std::pin::pin!(ScanClusterStream::scan(&client, "*"));
        let mut ok = 0usize;
        let mut err = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(_) => ok += 1,
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }

        let err = err.expect("the failing node should surface an error");
        assert!(
            err.to_string().contains("scan refused"),
            "unexpected error: {err}"
        );
        // Which node fails depends on where its ephemeral port sorts, so assert
        // the shape rather than a fixed count: every node before the failure
        // completed (three keys each), the failing one yielded nothing, and no
        // node after it was asked at all.
        assert!(
            ok.is_multiple_of(3) && ok < 9,
            "expected whole nodes before the failure, got {ok} keys"
        );
        let log = log.lock().unwrap();
        let scanned = visit_order_of_scanned(&log, &sorted_addrs);
        assert_eq!(
            scanned.len(),
            ok / 3 + 1,
            "the scan should stop at the failing node, having scanned {scanned:?}"
        );
    }

    /// The addresses that received at least one SCAN, in sorted-address order.
    fn visit_order_of_scanned(
        log: &HashMap<String, Vec<Vec<String>>>,
        sorted_addrs: &[String],
    ) -> Vec<String> {
        sorted_addrs
            .iter()
            .filter(|a| log.contains_key(*a))
            .cloned()
            .collect()
    }
}
