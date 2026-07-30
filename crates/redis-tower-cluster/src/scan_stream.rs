//! Cluster-wide SCAN iteration.
//!
//! `SCAN` iterates the keyspace of the single node it is sent to. It carries no
//! key, so slot routing has nothing to route on and
//! [`MultiplexedClusterClient::execute`] sends it to the default node --
//! returning roughly a third of a three-master cluster's keys with no
//! indication that anything was missed.
//!
//! [`ScanClusterStream`] runs a `SCAN` cursor loop against every master and
//! yields each key tagged with the node it came from. [`ClusterScan`] is the
//! configurable form, and is how you ask for more than one node at a time.
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
//! # Concurrency
//!
//! By default one master is paged at a time, so a scan of an `n`-master cluster
//! costs `n` times a single node's round trips. [`ClusterScan::concurrency`]
//! pages several masters at once instead:
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use futures::StreamExt;
//! use redis_tower_cluster::{ClusterScan, MultiplexedClusterClient};
//!
//! let client = MultiplexedClusterClient::connect("127.0.0.1:7000").await?;
//! let mut stream = std::pin::pin!(
//!     ClusterScan::new("user:*").count(500).concurrency(8).run(&client)
//! );
//! while let Some(item) = stream.next().await {
//!     println!("{:?}", item?.key);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The width is capped at [`MAX_SCAN_CONCURRENCY`]. It bounds how many masters
//! are being paged, not the total in-flight commands: each node still runs one
//! `SCAN` at a time, because the next cursor is only known once the previous
//! page returns.
//!
//! # Guarantees
//!
//! Redis's `SCAN` guarantee -- a key present for the whole iteration is
//! returned at least once, and a key may be returned more than once -- holds
//! per node, and so holds cluster-wide for a cluster whose slot assignment does
//! not change during the scan. It is a per-node guarantee, so it is unaffected
//! by how many nodes are scanned at once.
//!
//! Ordering is not: at concurrency 1 nodes are visited in sorted address order
//! and each node's keys arrive contiguously, while above 1 keys from different
//! nodes interleave in completion order. Nothing about `SCAN` guarantees an
//! order within a node either, so treat the sequence as unordered unless you
//! specifically want the sequential traversal.
//!
//! The node set is snapshotted once, when the stream is first polled. A slot
//! migrating between masters mid-scan can therefore be missed (if it moves from
//! a node not yet visited to one already visited) or seen twice. Re-checking
//! membership mid-scan is left for a follow-up; until then, treat a scan run
//! during a live resharding as approximate.

use std::pin::Pin;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use redis_tower_commands::Scan;
use redis_tower_core::RedisError;

use crate::multiplexed::MultiplexedClusterClient;

/// The largest number of masters a cluster scan will page at once.
///
/// [`ClusterScan::concurrency`] clamps to this. It matches the cluster client's
/// own connect fan-out bound: the ceiling exists so that a scan of a large
/// cluster cannot turn into an unbounded burst of concurrent work against every
/// master at once, not because any particular width is optimal.
pub const MAX_SCAN_CONCURRENCY: usize = 16;

/// A per-node scan stream, boxed so the fan-out can hold several at once.
type NodeScan = Pin<Box<dyn Stream<Item = Result<ClusterScanItem, RedisError>> + Send>>;

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

/// Shorthand constructors for cluster-wide SCAN iteration.
///
/// Each method returns an owned `impl Stream` -- the client is cheap to clone
/// and every call takes `&self`, so unlike
/// [`ScanStream`](redis_tower::ScanStream) the returned stream does not borrow
/// the client.
///
/// These are the one-node-at-a-time cases. Use [`ClusterScan`] to page several
/// masters at once.
pub struct ScanClusterStream;

impl ScanClusterStream {
    /// Iterate over all keys matching a pattern across every master node.
    ///
    /// Yields one [`ClusterScanItem`] per key. Nodes are visited sequentially,
    /// in sorted address order, each driven to its own cursor `"0"` before the
    /// next begins. Equivalent to `ClusterScan::new(pattern).run(client)`; see
    /// [`ClusterScan::concurrency`] to page several masters at once.
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
        ClusterScan::new(pattern).run(client)
    }

    /// Iterate over all keys matching a pattern across every master node, with
    /// a `COUNT` hint.
    ///
    /// The hint is passed to each per-node `SCAN`, so it bounds the work per
    /// round trip per node, not the total. Redis may return more or fewer.
    ///
    /// Equivalent to `ClusterScan::new(pattern).count(count).run(client)`.
    pub fn scan_with_count(
        client: &MultiplexedClusterClient,
        pattern: impl Into<String>,
        count: u64,
    ) -> impl Stream<Item = Result<ClusterScanItem, RedisError>> + 'static {
        ClusterScan::new(pattern).count(count).run(client)
    }
}

/// A cluster-wide scan, configured before it runs.
///
/// The general form behind [`ScanClusterStream::scan`]. Defaults to one master
/// at a time with no `COUNT` hint, which is what those shorthands do.
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use futures::StreamExt;
/// use redis_tower_cluster::{ClusterScan, MultiplexedClusterClient};
///
/// let client = MultiplexedClusterClient::connect("127.0.0.1:7000").await?;
/// let mut stream = std::pin::pin!(ClusterScan::new("user:*").concurrency(4).run(&client));
/// while let Some(item) = stream.next().await {
///     let item = item?;
///     println!("{} on {}", String::from_utf8_lossy(&item.key), item.node);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ClusterScan {
    pattern: String,
    count: Option<u64>,
    concurrency: usize,
}

impl ClusterScan {
    /// A scan of every master for keys matching `pattern`, one master at a time.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            count: None,
            concurrency: 1,
        }
    }

    /// Pass a `COUNT` hint to each per-node `SCAN`.
    ///
    /// Bounds the work per round trip per node, not the total. Redis may return
    /// more or fewer.
    pub fn count(mut self, count: u64) -> Self {
        self.count = Some(count);
        self
    }

    /// Page up to `nodes` masters at once.
    ///
    /// Clamped to at least 1 and at most [`MAX_SCAN_CONCURRENCY`], so `0` means
    /// sequential rather than "scan nothing".
    ///
    /// Above 1, keys from different nodes interleave and the sorted visit order
    /// of the sequential traversal no longer holds. Everything else is
    /// unchanged: each node is still paged to its own cursor `"0"`, one `SCAN`
    /// at a time, and an error from any node still ends the whole stream.
    pub fn concurrency(mut self, nodes: usize) -> Self {
        self.concurrency = nodes.clamp(1, MAX_SCAN_CONCURRENCY);
        self
    }

    /// Run the scan against `client`.
    ///
    /// The returned stream is owned rather than borrowing `client`, and does
    /// nothing until first polled -- which is when the node set is snapshotted.
    pub fn run(
        self,
        client: &MultiplexedClusterClient,
    ) -> impl Stream<Item = Result<ClusterScanItem, RedisError>> + 'static {
        scan_inner(client.clone(), self)
    }
}

/// Drive a [`ClusterScan`] over the client's masters.
///
/// An error from any node ends the whole stream: a partial cluster-wide scan
/// that reported success would be indistinguishable from a complete one, and
/// the caller cannot tell which keys are missing. At concurrency above 1 the
/// other in-flight node scans are dropped when that happens, so a node may have
/// been partly scanned and its keys already yielded.
fn scan_inner(
    client: MultiplexedClusterClient,
    scan: ClusterScan,
) -> impl Stream<Item = Result<ClusterScanItem, RedisError>> + 'static {
    let ClusterScan {
        pattern,
        count,
        concurrency,
    } = scan;
    async_stream::try_stream! {
        // Snapshotted once, on first poll. See the module docs on resharding.
        let nodes = client.master_service_addrs().await;

        if concurrency <= 1 {
            // Kept as an explicit loop rather than a width-1 fan-out: the
            // sorted visit order is a documented property of this path, and
            // this way it follows from the code instead of from a combinator's
            // internal polling order.
            for node in nodes {
                let mut per_node = scan_node(client.clone(), node, pattern.clone(), count);
                while let Some(item) = per_node.next().await {
                    yield item?;
                }
            }
        } else {
            let per_node = nodes
                .into_iter()
                .map(|node| scan_node(client.clone(), node, pattern.clone(), count));
            // Polls up to `concurrency` node scans at a time, pulling the next
            // node in only as an earlier one finishes.
            let mut merged = futures::stream::iter(per_node).flatten_unordered(concurrency);
            while let Some(item) = merged.next().await {
                yield item?;
            }
        }
    }
}

/// The `SCAN` cursor loop for a single master.
///
/// Pages that node until its own cursor comes back `"0"`, one command at a time:
/// the next cursor is only known once the previous page returns, so there is no
/// concurrency to be had within a node.
fn scan_node(
    client: MultiplexedClusterClient,
    node: String,
    pattern: String,
    count: Option<u64>,
) -> NodeScan {
    Box::pin(async_stream::try_stream! {
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Every SCAN command each fake node received, keyed by node address and
    /// recorded as the raw argument list.
    type ScanLog = Arc<Mutex<HashMap<String, Vec<Vec<String>>>>>;

    /// Counts `SCAN` commands in flight across all fake nodes at once, so a test
    /// can assert how many masters were being paged at the same moment.
    ///
    /// Serialized code can never push the peak above 1, which makes this a real
    /// check on the fan-out rather than a wall-clock comparison that would be
    /// flaky on a loaded machine.
    #[derive(Clone)]
    struct ScanProbe {
        in_flight: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        /// How long each node holds a `SCAN` before replying. Without a hold,
        /// whether two nodes' scans overlap is a scheduling coin flip, so tests
        /// that assert on the peak set one and the rest leave it zero.
        hold: Duration,
    }

    impl ScanProbe {
        fn new(hold: Duration) -> Self {
            Self {
                in_flight: Arc::new(AtomicUsize::new(0)),
                peak: Arc::new(AtomicUsize::new(0)),
                hold,
            }
        }

        /// Enter a `SCAN`, hold it, and leave. Called before the reply is written.
        async fn scanning(&self) {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            if !self.hold.is_zero() {
                tokio::time::sleep(self.hold).await;
            }
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
        }

        fn peak(&self) -> usize {
            self.peak.load(Ordering::SeqCst)
        }
    }

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
        probe: ScanProbe,
    ) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let node_addr = addr.clone();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let slots = slots.clone();
                let log = log.clone();
                let probe = probe.clone();
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
                                    probe.scanning().await;
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

    /// A connected client over three fake masters, plus everything a test needs
    /// to inspect what they saw.
    struct FakeCluster {
        client: MultiplexedClusterClient,
        /// The node addresses in sorted order, which is the order a sequential
        /// scan visits them in.
        sorted_addrs: Vec<String>,
        log: ScanLog,
        probe: ScanProbe,
    }

    async fn fake_cluster_holding(behaviors: [ScanBehavior; 3], hold: Duration) -> FakeCluster {
        let slots: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
        let log: ScanLog = Arc::new(Mutex::new(HashMap::new()));
        let probe = ScanProbe::new(hold);

        let mut addrs = Vec::new();
        for behavior in behaviors {
            addrs.push(spawn_node(behavior, slots.clone(), log.clone(), probe.clone()).await);
        }
        // Every node can serve discovery, so seeding from any of them works.
        *slots.lock().unwrap() = Some(cluster_slots_reply(&addrs));

        let client = MultiplexedClusterClient::connect(&addrs[0])
            .await
            .expect("fake cluster should connect");

        let mut sorted_addrs = addrs;
        sorted_addrs.sort();
        FakeCluster {
            client,
            sorted_addrs,
            log,
            probe,
        }
    }

    async fn fake_cluster(behaviors: [ScanBehavior; 3]) -> FakeCluster {
        fake_cluster_holding(behaviors, Duration::ZERO).await
    }

    async fn healthy_cluster() -> FakeCluster {
        fake_cluster([ScanBehavior::TwoPages; 3]).await
    }

    /// A healthy cluster whose nodes each hold a `SCAN` for `hold` before
    /// replying, so overlap between nodes is deterministic rather than a race.
    async fn healthy_cluster_holding(hold: Duration) -> FakeCluster {
        fake_cluster_holding([ScanBehavior::TwoPages; 3], hold).await
    }

    /// Long enough that two nodes paged at once reliably overlap, short enough
    /// that a sequential six-page traversal is still fast.
    const HOLD: Duration = Duration::from_millis(60);

    /// Collect a whole scan, failing the test on any error.
    async fn collect_ok(
        stream: impl Stream<Item = Result<ClusterScanItem, RedisError>>,
    ) -> Vec<ClusterScanItem> {
        std::pin::pin!(stream)
            .map(|r| r.expect("scan should succeed"))
            .collect()
            .await
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
        let fake = healthy_cluster().await;

        let items = collect_ok(ScanClusterStream::scan(&fake.client, "*")).await;

        assert_eq!(items.len(), 9, "three keys from each of three nodes");
        assert_eq!(
            visit_order(&items),
            fake.sorted_addrs,
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
        let fake = healthy_cluster().await;

        let items = collect_ok(ScanClusterStream::scan(&fake.client, "user:*")).await;
        assert_eq!(items.len(), 9);

        let log = fake.log.lock().unwrap();
        for addr in &fake.sorted_addrs {
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
        let fake = healthy_cluster().await;

        let result = fake
            .client
            .execute(Scan::new().match_pattern("*"))
            .await
            .expect("scan should succeed");
        assert_eq!(result.results.len(), 2, "one page, from one node");

        let log = fake.log.lock().unwrap();
        assert_eq!(log.len(), 1, "exactly one node saw the command");
    }

    #[tokio::test]
    async fn scan_with_count_forwards_the_hint_to_every_node() {
        let fake = healthy_cluster().await;

        let items = collect_ok(ScanClusterStream::scan_with_count(&fake.client, "*", 32)).await;
        assert_eq!(items.len(), 9);

        let log = fake.log.lock().unwrap();
        for addr in &fake.sorted_addrs {
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
        let fake = fake_cluster([
            ScanBehavior::TwoPages,
            ScanBehavior::Fail,
            ScanBehavior::TwoPages,
        ])
        .await;

        let mut stream = std::pin::pin!(ScanClusterStream::scan(&fake.client, "*"));
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
        let log = fake.log.lock().unwrap();
        let scanned = visit_order_of_scanned(&log, &fake.sorted_addrs);
        assert_eq!(
            scanned.len(),
            ok / 3 + 1,
            "the scan should stop at the failing node, having scanned {scanned:?}"
        );
    }

    /// The default path holds the sorted-visit-order guarantee, so it must not
    /// overlap nodes. Paired with the concurrent test below, this is what proves
    /// the fan-out is load-bearing rather than incidental.
    #[tokio::test]
    async fn a_sequential_scan_pages_one_master_at_a_time() {
        let fake = healthy_cluster_holding(HOLD).await;

        let items = collect_ok(ScanClusterStream::scan(&fake.client, "*")).await;

        assert_eq!(items.len(), 9);
        assert_eq!(
            fake.probe.peak(),
            1,
            "the default path must page one master at a time"
        );
    }

    #[tokio::test]
    async fn a_concurrent_scan_pages_several_masters_at_once() {
        let fake = healthy_cluster_holding(HOLD).await;

        let items = collect_ok(ClusterScan::new("*").concurrency(3).run(&fake.client)).await;

        assert_eq!(items.len(), 9, "every key still arrives");
        let peak = fake.probe.peak();
        assert!(
            peak > 1,
            "expected overlapping per-node scans, peak in-flight was {peak}"
        );
        assert!(
            peak <= 3,
            "fan-out exceeded the requested width: peak {peak} > 3"
        );
    }

    /// Everything except ordering is unchanged by the fan-out: every master is
    /// still reached, each is still paged to its own cursor `0`, each key still
    /// carries the node it came from, and `COUNT` still reaches every node.
    #[tokio::test]
    async fn a_concurrent_scan_covers_every_master_the_same_way() {
        let fake = healthy_cluster().await;

        let items = collect_ok(
            ClusterScan::new("user:*")
                .count(32)
                .concurrency(3)
                .run(&fake.client),
        )
        .await;
        assert_eq!(items.len(), 9);

        for item in &items {
            let key = String::from_utf8_lossy(&item.key);
            assert_eq!(
                key.rsplit_once(':').unwrap().0,
                item.node,
                "key {key} carries the wrong node tag"
            );
        }

        // Completion order is unspecified here, so compare the set of nodes
        // reached rather than the sequence.
        let mut reached: Vec<String> = items.iter().map(|i| i.node.clone()).collect();
        reached.sort();
        reached.dedup();
        assert_eq!(reached, fake.sorted_addrs, "every master reached");

        let log = fake.log.lock().unwrap();
        for addr in &fake.sorted_addrs {
            let calls = log.get(addr).unwrap_or_else(|| panic!("no SCAN on {addr}"));
            assert_eq!(calls.len(), 2, "{addr} should have been paged twice");
            assert_eq!(calls[0][1], "0", "the first page starts at cursor 0");
            assert_eq!(calls[1][1], "1", "the second page continues the cursor");
            for call in calls {
                assert!(call.contains(&"user:*".to_string()), "pattern forwarded");
                let i = call
                    .iter()
                    .position(|a| a.eq_ignore_ascii_case("COUNT"))
                    .expect("COUNT is forwarded");
                assert_eq!(call[i + 1], "32");
            }
        }
    }

    /// A failing node still ends the whole stream. The key counts of
    /// `an_error_from_one_node_ends_the_stream` do not carry over: other nodes
    /// are in flight when the error lands, so how much they yielded first is
    /// timing-dependent.
    #[tokio::test]
    async fn a_concurrent_scan_surfaces_an_error_from_any_node() {
        let fake = fake_cluster([
            ScanBehavior::TwoPages,
            ScanBehavior::Fail,
            ScanBehavior::TwoPages,
        ])
        .await;

        let mut stream = std::pin::pin!(ClusterScan::new("*").concurrency(3).run(&fake.client));
        let mut err = None;
        while let Some(item) = stream.next().await {
            if let Err(e) = item {
                err = Some(e);
                break;
            }
        }

        let err = err.expect("the failing node should surface an error");
        assert!(
            err.to_string().contains("scan refused"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn the_concurrency_width_is_clamped_to_the_documented_range() {
        assert_eq!(
            ClusterScan::new("*").concurrency,
            1,
            "sequential unless asked otherwise"
        );
        assert_eq!(
            ClusterScan::new("*").concurrency(0).concurrency,
            1,
            "0 means sequential, not scan nothing"
        );
        assert_eq!(ClusterScan::new("*").concurrency(4).concurrency, 4);
        assert_eq!(
            ClusterScan::new("*").concurrency(usize::MAX).concurrency,
            MAX_SCAN_CONCURRENCY
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
