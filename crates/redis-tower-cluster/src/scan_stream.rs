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
//! # Membership changes
//!
//! The set of masters is not snapshotted once. Each time the scan finishes the
//! masters it knows about it asks the client again, and scans any it has not
//! scanned yet, until a round turns up nothing new. So a master the client
//! learns about part-way through -- a node added by a reshard, or a replica
//! promoted at a new address -- is still scanned rather than missed entirely.
//! Conversely, a master the client drops part-way through is skipped: the
//! cluster no longer lists it as owning slots, so whoever owns them now is
//! either already scanned or picked up by a later round.
//!
//! Rounds are capped at [`MAX_MEMBERSHIP_ROUNDS`]. A scan that keeps finding
//! new masters past that fails, because a scan that stopped quietly could not
//! be told apart from one that covered the cluster.
//!
//! By itself this only sees membership the client has already learned, and a
//! `SCAN` never triggers that learning: it carries no key, so it is never
//! answered with a MOVED. [`ClusterScan::refresh_membership`] makes the scan ask
//! the cluster itself between rounds, which is what to use when a scan has to
//! stay correct across a live resharding.
//!
//! One gap remains either way. A slot that migrates from a master not yet
//! scanned to one already scanned is missed, and one that migrates the other way
//! is seen twice, because nothing about a per-node cursor tracks slots. Closing
//! that needs slot-level scan state, not membership checking.

use std::collections::BTreeSet;
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

/// The most membership re-check rounds a cluster scan will run before giving up.
///
/// A round runs only when the re-check turned up a master the scan has not
/// scanned yet, so a cluster whose membership is settled uses exactly one. The
/// ceiling exists so that a cluster reshaping itself faster than it can be
/// scanned ends the scan with an error instead of looping indefinitely.
pub const MAX_MEMBERSHIP_ROUNDS: usize = 8;

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
    refresh_membership: bool,
}

impl ClusterScan {
    /// A scan of every master for keys matching `pattern`, one master at a time.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            count: None,
            concurrency: 1,
            refresh_membership: false,
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

    /// Refresh the cluster topology before each membership re-check.
    ///
    /// Off by default. The scan always re-checks which masters the client holds
    /// between rounds, but on its own that only sees membership the client has
    /// already learned, and a `SCAN` never teaches it any: `SCAN` carries no key,
    /// so it is never answered with the MOVED that would drive a refresh. A
    /// scan-only workload can therefore run start to finish against a stale node
    /// set while the cluster reshards underneath it.
    ///
    /// Turning this on runs
    /// [`refresh_topology`](MultiplexedClusterClient::refresh_topology) between
    /// rounds, so the re-check sees the cluster's current slot map. The costs, in
    /// exchange:
    ///
    /// - one extra `CLUSTER SLOTS` round trip per round, including the final
    ///   round that confirms nothing new appeared;
    /// - the refresh's usual reconciliation of the client's own services, which
    ///   is to say a read-only scan can now prune a departed node and rebuild a
    ///   dead one;
    /// - a failed refresh ends the scan, because a scan asked to keep up with
    ///   membership cannot vouch for its coverage if it could not look.
    ///
    /// Refreshes only ever happen at a round boundary, when no `SCAN` is in
    /// flight, so a service rebuilt underneath the scan never interrupts a
    /// node's paging.
    pub fn refresh_membership(mut self, refresh: bool) -> Self {
        self.refresh_membership = refresh;
        self
    }

    /// Run the scan against `client`.
    ///
    /// The returned stream is owned rather than borrowing `client`, and does
    /// nothing until first polled -- which is when the first membership check
    /// happens.
    pub fn run(
        self,
        client: &MultiplexedClusterClient,
    ) -> impl Stream<Item = Result<ClusterScanItem, RedisError>> + 'static {
        scan_inner(client.clone(), self)
    }
}

/// Drive a [`ClusterScan`] over the client's masters, re-checking membership
/// between rounds.
///
/// Each round scans the masters the client holds that have not been scanned yet,
/// then asks again. A settled cluster spends one round on every master and a
/// second that finds nothing, so this is the same traversal as a single snapshot
/// would give; a cluster that gained a master mid-scan spends a further round on
/// it. See the module docs for what that does and does not cover.
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
        refresh_membership,
    } = scan;
    async_stream::try_stream! {
        let mut scanned: BTreeSet<String> = BTreeSet::new();
        let mut rounds = 0usize;

        loop {
            if refresh_membership {
                // Ask the cluster who owns what now, so the check below sees the
                // current slot map rather than only what other traffic on this
                // client happened to reveal. Safe here and nowhere else in this
                // loop: no SCAN is in flight at a round boundary, so a service
                // this rebuilds cannot interrupt a node mid-paging.
                client.refresh_topology().await?;
            }

            // Re-read rather than reuse a snapshot. A master the client has
            // learned about since the last round gets scanned; one it has
            // dropped never appears, so its slots are covered by whoever owns
            // them now.
            let pending: Vec<String> = client
                .master_service_addrs()
                .await
                .into_iter()
                .filter(|addr| !scanned.contains(addr))
                .collect();
            if pending.is_empty() {
                break;
            }

            rounds += 1;
            if rounds > MAX_MEMBERSHIP_ROUNDS {
                // Stopping quietly here would report a cluster-wide scan that
                // knowingly left masters unscanned.
                Err::<(), _>(RedisError::Redis(format!(
                    "cluster scan: still finding unscanned masters after \
                     {MAX_MEMBERSHIP_ROUNDS} membership rounds"
                )))?;
            }
            scanned.extend(pending.iter().cloned());

            if concurrency <= 1 {
                // Kept as an explicit loop rather than a width-1 fan-out: the
                // sorted visit order is a documented property of this path, and
                // this way it follows from the code instead of from a combinator's
                // internal polling order.
                for node in pending {
                    let mut per_node = scan_node(client.clone(), node, pattern.clone(), count);
                    while let Some(item) = per_node.next().await {
                        yield item?;
                    }
                }
            } else {
                let per_node = pending
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
}

/// The `SCAN` cursor loop for a single master.
///
/// Pages that node until its own cursor comes back `"0"`, one command at a time:
/// the next cursor is only known once the previous page returns, so there is no
/// concurrency to be had within a node.
///
/// A node the client stops holding a master service for part-way through ends
/// this stream without an error. That is a membership change rather than a scan
/// failure -- the cluster no longer lists the node as owning slots, so their
/// current owner is either scanned already or picked up by a later round -- and
/// failing here would turn an ordinary reshard into a failed scan. Checked after
/// the failure rather than before each page, so a node that is merely failing
/// still surfaces its error.
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
            let result = match client.execute_on_node(&node, cmd).await {
                Ok(result) => result,
                Err(e) => {
                    if client.holds_master(&node).await {
                        Err::<(), _>(e)?;
                    } else {
                        tracing::debug!(
                            node = %node,
                            error = %e,
                            "cluster scan: node is no longer a master this client holds, \
                             ending its scan"
                        );
                    }
                    break;
                }
            };
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

    /// What every fake node answers `CLUSTER SLOTS` with, shared by all of them,
    /// and mutable so a test can reshape the cluster mid-scan.
    #[derive(Default)]
    struct FakeTopology {
        /// The addresses currently published as masters. Empty before the
        /// cluster is wired up, which is answered with an error.
        masters: Vec<String>,
        /// Addresses to publish one at a time, one per `CLUSTER SLOTS` call, so
        /// a test can build a cluster whose membership a scan can never catch up
        /// with. Empty for a settled cluster, which is every other test.
        reveal_one_per_call: Vec<String>,
        /// How many times any node has been asked for the topology. Lets a test
        /// pin what a scan costs in `CLUSTER SLOTS` round trips.
        calls: usize,
    }

    impl FakeTopology {
        fn settled(masters: Vec<String>) -> Self {
            Self {
                masters,
                ..Self::default()
            }
        }

        /// The reply to serve now, revealing one more master first if this
        /// cluster is set up to grow.
        fn take_reply(&mut self) -> Option<Vec<u8>> {
            self.calls += 1;
            if let Some(next) = self.reveal_one_per_call.pop() {
                self.masters.push(next);
            }
            if self.masters.is_empty() {
                return None;
            }
            Some(cluster_slots_reply(&self.masters))
        }
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
        slots: Arc<Mutex<FakeTopology>>,
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
                                "CLUSTER" => match slots.lock().unwrap().take_reply() {
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
        topology: Arc<Mutex<FakeTopology>>,
    }

    impl FakeCluster {
        /// Spawn another master and publish it, so the next `CLUSTER SLOTS` sees
        /// a four-master cluster. The client only learns about it once something
        /// refreshes its topology.
        async fn add_master(&self) -> String {
            let addr = spawn_node(
                ScanBehavior::TwoPages,
                self.topology.clone(),
                self.log.clone(),
                self.probe.clone(),
            )
            .await;
            let mut topology = self.topology.lock().unwrap();
            topology.masters.push(addr.clone());
            addr
        }

        /// Unpublish a master, as a reshard that moved every one of its slots
        /// elsewhere would. The node keeps listening; the client only stops
        /// holding it once something refreshes its topology.
        fn remove_master(&self, addr: &str) {
            let mut topology = self.topology.lock().unwrap();
            topology.masters.retain(|a| a != addr);
        }

        /// Publish `count` further masters one at a time, one per `CLUSTER SLOTS`
        /// call, so a scan re-checking membership always finds another unscanned
        /// one.
        async fn grow_on_every_topology_call(&self, count: usize) {
            for _ in 0..count {
                let addr = spawn_node(
                    ScanBehavior::TwoPages,
                    self.topology.clone(),
                    self.log.clone(),
                    self.probe.clone(),
                )
                .await;
                self.topology.lock().unwrap().reveal_one_per_call.push(addr);
            }
        }

        fn topology_calls(&self) -> usize {
            self.topology.lock().unwrap().calls
        }
    }

    async fn fake_cluster_holding(behaviors: [ScanBehavior; 3], hold: Duration) -> FakeCluster {
        let topology: Arc<Mutex<FakeTopology>> = Arc::new(Mutex::new(FakeTopology::default()));
        let log: ScanLog = Arc::new(Mutex::new(HashMap::new()));
        let probe = ScanProbe::new(hold);

        let mut addrs = Vec::new();
        for behavior in behaviors {
            addrs.push(spawn_node(behavior, topology.clone(), log.clone(), probe.clone()).await);
        }
        // Every node can serve discovery, so seeding from any of them works.
        *topology.lock().unwrap() = FakeTopology::settled(addrs.clone());

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
            topology,
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

    /// The behaviour this slice adds. A master published part-way through a scan
    /// is scanned by a later round rather than missed entirely.
    ///
    /// The first key is what makes this exact: by the time it arrives, the first
    /// round has already fixed the list of masters it is going to scan, so a
    /// master added afterwards can only be reached by a re-check.
    #[tokio::test]
    async fn a_master_that_appears_mid_scan_is_still_scanned() {
        let fake = healthy_cluster().await;

        let mut stream = std::pin::pin!(
            ClusterScan::new("*")
                .refresh_membership(true)
                .run(&fake.client)
        );
        let first = stream
            .next()
            .await
            .expect("a first key")
            .expect("scan should succeed");

        let added = fake.add_master().await;

        let mut items = vec![first];
        while let Some(item) = stream.next().await {
            items.push(item.expect("scan should succeed"));
        }

        assert_eq!(items.len(), 12, "three keys from each of four masters");
        assert_eq!(
            visit_order(&items),
            {
                // The three original masters in sorted order, then the late
                // arrival last -- it is the only master its round has, wherever
                // its ephemeral port happens to sort.
                let mut expected = fake.sorted_addrs.clone();
                expected.push(added.clone());
                expected
            },
            "the late master is scanned, after the round that did not know about it"
        );
        assert_eq!(
            fake.log.lock().unwrap().get(&added).map(Vec::len),
            Some(2),
            "the late master is paged to its own cursor 0, like any other"
        );
    }

    /// The other half of a membership change. A master unpublished part-way
    /// through -- a reshard moved its slots elsewhere -- is skipped rather than
    /// failing the scan, even though the round that is running snapshotted it.
    ///
    /// A node that is present and merely failing still ends the stream; that is
    /// what `an_error_from_one_node_ends_the_stream` pins, and it is why the
    /// departed check is made after a failure rather than before each page.
    #[tokio::test]
    async fn a_master_that_leaves_mid_scan_is_skipped_rather_than_failing() {
        let fake = healthy_cluster().await;
        let departing = fake.sorted_addrs.last().unwrap().clone();

        let mut stream = std::pin::pin!(ScanClusterStream::scan(&fake.client, "*"));
        let first = stream
            .next()
            .await
            .expect("a first key")
            .expect("scan should succeed");

        // Unpublish the last master the sequential scan would reach, then let the
        // client reconcile -- which is what a background refresh triggered by
        // other traffic on this client would do.
        fake.remove_master(&departing);
        fake.client
            .refresh_topology()
            .await
            .expect("refresh should succeed");

        let mut items = vec![first];
        while let Some(item) = stream.next().await {
            items.push(item.expect("a departed master must not fail the scan"));
        }

        assert_eq!(
            items.len(),
            6,
            "three keys from each of the two that remain"
        );
        assert!(
            items.iter().all(|i| i.node != departing),
            "the departed master yielded nothing"
        );
        assert!(
            !fake.log.lock().unwrap().contains_key(&departing),
            "the departed master was never asked for a page"
        );
    }

    /// A cluster reshaping itself faster than it can be scanned ends the scan
    /// with an error. Stopping quietly would report a cluster-wide scan that
    /// knowingly left masters unscanned.
    #[tokio::test]
    async fn a_cluster_that_keeps_growing_ends_the_scan_with_an_error() {
        let fake = healthy_cluster().await;
        // One more master per topology call, for more calls than the scan is
        // allowed rounds, so every re-check finds another unscanned master.
        fake.grow_on_every_topology_call(MAX_MEMBERSHIP_ROUNDS + 2)
            .await;

        let mut stream = std::pin::pin!(
            ClusterScan::new("*")
                .refresh_membership(true)
                .run(&fake.client)
        );
        let mut err = None;
        while let Some(item) = stream.next().await {
            if let Err(e) = item {
                err = Some(e);
                break;
            }
        }

        let err = err.expect("a cluster that never settles should fail the scan");
        assert!(
            err.to_string().contains("membership rounds"),
            "unexpected error: {err}"
        );
    }

    /// Re-checking membership is free by default: it re-reads what the client
    /// already holds. Asking the cluster itself is what costs a round trip, and
    /// only happens when requested.
    ///
    /// Two clusters rather than two scans of one: a fake node counts SCAN pages
    /// per connection and the client holds one connection per node, so a second
    /// scan of the same fixture would find every node already at its last page.
    #[tokio::test]
    async fn only_a_refreshing_scan_asks_the_cluster_for_its_topology() {
        let quiet = healthy_cluster().await;
        let quiet_after_connect = quiet.topology_calls();
        let items = collect_ok(ScanClusterStream::scan(&quiet.client, "*")).await;
        assert_eq!(items.len(), 9);
        assert_eq!(
            quiet.topology_calls(),
            quiet_after_connect,
            "the default path must not add a CLUSTER SLOTS round trip"
        );

        let refreshing = healthy_cluster().await;
        let refreshing_after_connect = refreshing.topology_calls();
        let items = collect_ok(
            ClusterScan::new("*")
                .refresh_membership(true)
                .run(&refreshing.client),
        )
        .await;
        assert_eq!(items.len(), 9, "the same keys either way");
        assert_eq!(
            refreshing.topology_calls() - refreshing_after_connect,
            2,
            "one refresh before the round that scans, one before the round that \
             confirms nothing new appeared"
        );
    }

    /// A refresh the scan asked for and could not get ends the scan: it was asked
    /// to keep up with membership and cannot vouch for its coverage if it could
    /// not look.
    #[tokio::test]
    async fn a_refreshing_scan_fails_when_the_topology_is_unreachable() {
        let fake = healthy_cluster().await;
        // Every node answers CLUSTER SLOTS with an error once no master is
        // published, which is how a refresh finds no usable seed.
        fake.topology.lock().unwrap().masters.clear();

        let mut stream = std::pin::pin!(
            ClusterScan::new("*")
                .refresh_membership(true)
                .run(&fake.client)
        );
        let first = stream
            .next()
            .await
            .expect("the scan should report something");

        let err = first.expect_err("an unusable topology should fail the scan");
        assert!(
            err.to_string().contains("no topology"),
            "the failure should be the refresh's, not a scan's: {err}"
        );
        assert!(
            fake.log.lock().unwrap().is_empty(),
            "the scan should fail before paging any node"
        );
    }

    #[test]
    fn the_membership_round_cap_is_the_documented_one() {
        // Named in the module docs and in the error a scan fails with, so a
        // change here is a documentation change too.
        assert_eq!(MAX_MEMBERSHIP_ROUNDS, 8);
    }

    #[test]
    fn membership_refreshing_is_off_unless_asked_for() {
        assert!(!ClusterScan::new("*").refresh_membership);
        assert!(
            ClusterScan::new("*")
                .refresh_membership(true)
                .refresh_membership
        );
        assert!(
            !ClusterScan::new("*")
                .refresh_membership(true)
                .refresh_membership(false)
                .refresh_membership
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
