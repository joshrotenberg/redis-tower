//! Cluster topology discovery via `CLUSTER SLOTS`.
//!
//! [`ClusterTopology`] stores the slot ranges Redis reports and maintains an
//! O(1) slot-to-owner table. Clients patch that topology after `MOVED`
//! redirects and replace it after full discovery.
//!
//! # Example
//!
//! ```
//! use redis_tower_cluster::slot::slot_for_key;
//! use redis_tower_cluster::topology::{ClusterTopology, NodeAddr, SlotRange};
//!
//! let master = NodeAddr::new("redis-1.internal", 6379);
//! let topology = ClusterTopology::new(vec![SlotRange {
//!     start: 0,
//!     end: 16_383,
//!     master: master.clone(),
//!     replicas: vec![NodeAddr::new("redis-2.internal", 6379)],
//! }]);
//!
//! let slot = slot_for_key(b"user:42");
//! assert_eq!(topology.master_for_slot(slot), Some(&master));
//! ```

use redis_tower_core::{Frame, RedisConnection, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// Number of hash slots in a Redis cluster (fixed by the protocol).
const SLOT_COUNT: usize = 16_384;

/// Sentinel stored in the slot table for a slot that no range currently owns.
/// A range index can never reach this value: after single-slot splits there are
/// at most `SLOT_COUNT` ranges, so the largest valid index is `SLOT_COUNT - 1`.
const UNMAPPED: u16 = u16::MAX;

/// A slot range owned by a node.
#[derive(Debug, Clone)]
pub struct SlotRange {
    /// Start slot (inclusive).
    pub start: u16,
    /// End slot (inclusive).
    pub end: u16,
    /// Master node address.
    pub master: NodeAddr,
    /// Replica node addresses.
    pub replicas: Vec<NodeAddr>,
}

/// Address of a cluster node.
///
/// Defined in `redis-tower` (shared with `redis-tower-sentinel`'s replica
/// routing) and re-exported here under its original path.
pub use redis_tower::NodeAddr;

/// The full cluster topology: a list of slot ranges with their owners.
///
/// Alongside the ranges, the topology keeps a flat `slot -> range index` table
/// so slot lookups are O(1) rather than a linear scan of the ranges. The table
/// is the only routing cost that would otherwise grow with the number of shards
/// (a live scan is O(ranges); a scan over a 200-shard cluster with fragmented
/// slots is noticeably slower than one over three). It is rebuilt whenever the
/// set of ranges changes, so it always agrees with `slot_ranges`.
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    slot_ranges: Vec<SlotRange>,
    /// `slot -> index into slot_ranges`, or [`UNMAPPED`] for an unowned slot.
    /// Boxed so the 32 KiB table lives on the heap and `ClusterTopology` stays
    /// cheap to move.
    slot_owner: Box<[u16; SLOT_COUNT]>,
}

impl ClusterTopology {
    /// Build a topology from a set of slot ranges, computing the flat
    /// slot-to-owner lookup table.
    pub fn new(slot_ranges: Vec<SlotRange>) -> Self {
        let mut topology = ClusterTopology {
            slot_ranges,
            slot_owner: Box::new([UNMAPPED; SLOT_COUNT]),
        };
        topology.rebuild_slot_owner();
        topology
    }

    /// The slot ranges backing this topology.
    pub fn slot_ranges(&self) -> &[SlotRange] {
        &self.slot_ranges
    }

    /// Mutable access to the ranges for in-place node-address rewriting (e.g.
    /// remapping hosts behind a NAT).
    ///
    /// Callers must not change any range's `start`/`end` or add or remove
    /// ranges through this handle: only node addresses may be edited, so the
    /// flat slot table stays valid. Structural changes must go through
    /// [`reassign_slot`](Self::reassign_slot) or [`ClusterTopology::new`].
    pub(crate) fn slot_ranges_mut(&mut self) -> &mut [SlotRange] {
        &mut self.slot_ranges
    }

    /// Rebuild the flat slot-to-owner table from `slot_ranges`. O(SLOT_COUNT +
    /// total slots covered); called only when the ranges change, not per
    /// lookup.
    ///
    /// When ranges overlap (they should not in a healthy topology) the
    /// last-written owner wins. This matches the pre-table behaviour after a
    /// [`reassign_slot`] split, which keeps ranges disjoint, and the CLUSTER
    /// SLOTS contract, which reports disjoint ranges.
    fn rebuild_slot_owner(&mut self) {
        // Split the borrow so the range iterator and the table can be held at
        // once.
        let ClusterTopology {
            slot_ranges,
            slot_owner,
        } = self;
        slot_owner.fill(UNMAPPED);
        for (idx, range) in slot_ranges.iter().enumerate() {
            let owner = idx as u16;
            let start = range.start as usize;
            let end = (range.end as usize).min(SLOT_COUNT - 1);
            for slot in slot_owner[start..=end].iter_mut() {
                *slot = owner;
            }
        }
    }

    /// The range owning `slot`, via the flat table, or `None` when the slot is
    /// unowned or out of range.
    fn range_for_slot(&self, slot: u16) -> Option<&SlotRange> {
        let idx = *self.slot_owner.get(slot as usize)?;
        if idx == UNMAPPED {
            None
        } else {
            self.slot_ranges.get(idx as usize)
        }
    }

    /// Find the master node responsible for a given slot.
    pub fn master_for_slot(&self, slot: u16) -> Option<&NodeAddr> {
        self.range_for_slot(slot).map(|r| &r.master)
    }

    /// Get all unique master addresses, in first-seen order.
    ///
    /// Deduplicates globally (not just adjacent entries) so a master that owns
    /// several non-contiguous ranges -- which happens after
    /// [`reassign_slot`](Self::reassign_slot) splits a range on a single-slot
    /// MOVED -- is still reported once.
    pub fn master_addrs(&self) -> Vec<&NodeAddr> {
        let mut seen = std::collections::HashSet::new();
        self.slot_ranges
            .iter()
            .map(|r| &r.master)
            .filter(|a| seen.insert(*a))
            .collect()
    }

    /// Find replica nodes for a given slot.
    pub fn replicas_for_slot(&self, slot: u16) -> Option<&[NodeAddr]> {
        self.range_for_slot(slot).map(|r| r.replicas.as_slice())
    }

    /// Get all unique replica addresses.
    pub fn replica_addrs(&self) -> Vec<&NodeAddr> {
        let mut addrs: Vec<&NodeAddr> = self
            .slot_ranges
            .iter()
            .flat_map(|r| r.replicas.iter())
            .collect();
        addrs.sort_by_key(|a| a.addr_string());
        addrs.dedup_by(|a, b| a == b);
        addrs
    }

    /// Reassign a single slot to a new master after a MOVED redirect,
    /// splitting its containing range if necessary.
    ///
    /// A MOVED names exactly one slot. Reassigning the whole containing range
    /// (as a naive patch does) steals every other slot in that range and
    /// causes redirect ping-pong for the duration of a live resharding -- the
    /// client bounces the entire range between the old and new owner one
    /// command at a time. Instead, split the containing range into up to three
    /// pieces so only `slot` changes owner; the rest of the range keeps its
    /// current master and replicas.
    ///
    /// The moved slot starts with no known replicas -- a MOVED tells us the
    /// new master but not its replica set -- until the next full
    /// [`discover_topology`] refresh repopulates them. Reassigning a slot to
    /// the master that already owns it, or that is not currently mapped, is
    /// handled without splitting.
    pub fn reassign_slot(&mut self, slot: u16, master: NodeAddr) {
        let Some(idx) = self
            .slot_ranges
            .iter()
            .position(|r| slot >= r.start && slot <= r.end)
        else {
            // Slot isn't currently mapped: record it as a standalone range.
            self.slot_ranges.push(SlotRange {
                start: slot,
                end: slot,
                master,
                replicas: Vec::new(),
            });
            self.rebuild_slot_owner();
            return;
        };

        if self.slot_ranges[idx].master == master {
            // Already owned by this master; nothing to split.
            return;
        }

        let range = self.slot_ranges[idx].clone();
        let mut replacement = Vec::with_capacity(3);
        // Slots before the moved one keep the old owner.
        if slot > range.start {
            replacement.push(SlotRange {
                start: range.start,
                end: slot - 1,
                master: range.master.clone(),
                replicas: range.replicas.clone(),
            });
        }
        // The moved slot, now owned by the new master.
        replacement.push(SlotRange {
            start: slot,
            end: slot,
            master,
            replicas: Vec::new(),
        });
        // Slots after the moved one keep the old owner.
        if slot < range.end {
            replacement.push(SlotRange {
                start: slot + 1,
                end: range.end,
                master: range.master,
                replicas: range.replicas,
            });
        }
        self.slot_ranges.splice(idx..=idx, replacement);
        self.rebuild_slot_owner();
    }
}

/// Revisioned topology-change tracking used by cluster services that keep
/// state derived from slot ownership (notably client-side caches).
///
/// This stays crate-private because it describes an internal coordination
/// protocol, rather than adding a second public topology API alongside
/// [`ClusterTopology`]. A tracker is intended to live beside a client's
/// topology under the same lock. Record a change immediately after a MOVED
/// patch or when committing a freshly discovered topology.
#[allow(dead_code)]
pub(crate) mod changes {
    use std::collections::HashSet;
    use std::sync::Arc;

    use tokio::sync::watch;

    use super::{ClusterTopology, NodeAddr, SLOT_COUNT};

    /// A monotonically increasing generation for master slot ownership.
    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub(crate) struct TopologyRevision(u64);

    impl TopologyRevision {
        /// The revision assigned before any ownership change has been seen.
        pub(crate) const INITIAL: Self = Self(0);

        /// Expose the generation for diagnostics and generation comparisons.
        pub(crate) const fn get(self) -> u64 {
            self.0
        }

        fn next(self) -> Self {
            Self(
                self.0
                    .checked_add(1)
                    .expect("cluster topology revision overflowed"),
            )
        }
    }

    /// A topology captured together with the revision at which it was read.
    ///
    /// Keeping the revision with the routing snapshot prevents an ABA change
    /// (a slot moves A -> B -> A) from looking unchanged to a consumer that
    /// missed both updates.
    #[derive(Debug, Clone)]
    pub(crate) struct TopologySnapshot {
        revision: TopologyRevision,
        topology: ClusterTopology,
    }

    impl TopologySnapshot {
        pub(crate) fn revision(&self) -> TopologyRevision {
            self.revision
        }

        pub(crate) fn topology(&self) -> &ClusterTopology {
            &self.topology
        }

        pub(crate) fn into_topology(self) -> ClusterTopology {
            self.topology
        }
    }

    /// The owner transition for one hash slot.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct SlotOwnershipChange {
        pub(crate) slot: u16,
        pub(crate) old_owner: Option<NodeAddr>,
        pub(crate) new_owner: Option<NodeAddr>,
    }

    /// The master-routing difference between two topology snapshots.
    ///
    /// Replica-only changes are intentionally omitted: client-side caching is
    /// initially master-routed, and invalidation safety depends on master
    /// membership and slot ownership. Slot changes are sorted by slot;
    /// master lists are sorted by host and port, making the result stable for
    /// tests and diagnostics regardless of CLUSTER SLOTS range order.
    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    pub(crate) struct TopologyDiff {
        pub(crate) changed_slots: Vec<SlotOwnershipChange>,
        pub(crate) added_masters: Vec<NodeAddr>,
        pub(crate) removed_masters: Vec<NodeAddr>,
    }

    impl TopologyDiff {
        pub(crate) fn is_empty(&self) -> bool {
            self.changed_slots.is_empty()
                && self.added_masters.is_empty()
                && self.removed_masters.is_empty()
        }
    }

    /// Compute the master-routing difference between two topologies without
    /// mutating either one.
    pub(crate) fn diff(previous: &ClusterTopology, current: &ClusterTopology) -> TopologyDiff {
        let mut changed_slots = Vec::new();
        for slot in 0..SLOT_COUNT as u16 {
            let old_owner = previous.master_for_slot(slot);
            let new_owner = current.master_for_slot(slot);
            if old_owner != new_owner {
                changed_slots.push(SlotOwnershipChange {
                    slot,
                    old_owner: old_owner.cloned(),
                    new_owner: new_owner.cloned(),
                });
            }
        }

        let previous_masters: HashSet<NodeAddr> =
            previous.master_addrs().into_iter().cloned().collect();
        let current_masters: HashSet<NodeAddr> =
            current.master_addrs().into_iter().cloned().collect();

        let mut added_masters: Vec<NodeAddr> = current_masters
            .difference(&previous_masters)
            .cloned()
            .collect();
        let mut removed_masters: Vec<NodeAddr> = previous_masters
            .difference(&current_masters)
            .cloned()
            .collect();
        sort_nodes(&mut added_masters);
        sort_nodes(&mut removed_masters);

        TopologyDiff {
            changed_slots,
            added_masters,
            removed_masters,
        }
    }

    fn sort_nodes(nodes: &mut [NodeAddr]) {
        nodes.sort_by(|left, right| {
            left.host
                .cmp(&right.host)
                .then_with(|| left.port.cmp(&right.port))
        });
    }

    /// One committed master-routing transition.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) struct TopologyChange {
        pub(crate) previous_revision: TopologyRevision,
        pub(crate) revision: TopologyRevision,
        pub(crate) diff: TopologyDiff,
    }

    /// How a delivered change relates to the last revision a consumer applied.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum ChangeContinuity {
        /// This exact change was already applied.
        AlreadyApplied,
        /// This is the immediate next change and can be applied incrementally.
        Contiguous,
        /// One or more changes were missed; derived state must be rebuilt or
        /// conservatively cleared before accepting the latest topology.
        Gap,
    }

    impl TopologyChange {
        pub(crate) fn continuity_after(
            &self,
            observed_revision: TopologyRevision,
        ) -> ChangeContinuity {
            if observed_revision == self.revision {
                ChangeContinuity::AlreadyApplied
            } else if observed_revision == self.previous_revision {
                ChangeContinuity::Contiguous
            } else {
                ChangeContinuity::Gap
            }
        }
    }

    /// Revision source plus a latest-value notification channel.
    ///
    /// A Tokio watch channel deliberately coalesces rapid updates. Consumers
    /// detect that coalescing by comparing their last applied revision with
    /// [`TopologyChange::previous_revision`]; on a gap they must clear or
    /// rebuild state instead of applying only the latest slot list.
    pub(crate) struct TopologyChangeTracker {
        revision: TopologyRevision,
        changes: watch::Sender<Option<Arc<TopologyChange>>>,
    }

    impl Default for TopologyChangeTracker {
        fn default() -> Self {
            Self::new()
        }
    }

    impl TopologyChangeTracker {
        pub(crate) fn new() -> Self {
            let (changes, _) = watch::channel(None);
            Self {
                revision: TopologyRevision::INITIAL,
                changes,
            }
        }

        pub(crate) fn revision(&self) -> TopologyRevision {
            self.revision
        }

        pub(crate) fn snapshot(&self, topology: &ClusterTopology) -> TopologySnapshot {
            TopologySnapshot {
                revision: self.revision,
                topology: topology.clone(),
            }
        }

        pub(crate) fn subscribe(&self) -> watch::Receiver<Option<Arc<TopologyChange>>> {
            self.changes.subscribe()
        }

        /// Record a committed transition, advancing the revision only when
        /// master membership or slot ownership changed.
        pub(crate) fn record(
            &mut self,
            previous: &ClusterTopology,
            current: &ClusterTopology,
        ) -> Option<Arc<TopologyChange>> {
            let diff = diff(previous, current);
            if diff.is_empty() {
                return None;
            }

            let previous_revision = self.revision;
            self.revision = self.revision.next();
            let change = Arc::new(TopologyChange {
                previous_revision,
                revision: self.revision,
                diff,
            });
            self.changes.send_replace(Some(Arc::clone(&change)));
            Some(change)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::topology::SlotRange;

        fn node(host: &str, port: u16) -> NodeAddr {
            NodeAddr {
                host: host.to_string(),
                port,
            }
        }

        fn topology(ranges: &[(u16, u16, &str, u16)]) -> ClusterTopology {
            ClusterTopology::new(
                ranges
                    .iter()
                    .map(|&(start, end, host, port)| SlotRange {
                        start,
                        end,
                        master: node(host, port),
                        replicas: Vec::new(),
                    })
                    .collect(),
            )
        }

        #[test]
        fn diff_reports_each_changed_slot_and_master_membership() {
            let previous = topology(&[(0, 9, "node-a", 7000), (10, 19, "node-b", 7001)]);
            let current = topology(&[(0, 4, "node-a", 7000), (5, 19, "node-c", 7002)]);

            let change = diff(&previous, &current);

            assert_eq!(change.changed_slots.len(), 15);
            assert_eq!(
                change.changed_slots.first(),
                Some(&SlotOwnershipChange {
                    slot: 5,
                    old_owner: Some(node("node-a", 7000)),
                    new_owner: Some(node("node-c", 7002)),
                })
            );
            assert_eq!(
                change.changed_slots.last(),
                Some(&SlotOwnershipChange {
                    slot: 19,
                    old_owner: Some(node("node-b", 7001)),
                    new_owner: Some(node("node-c", 7002)),
                })
            );
            assert_eq!(change.added_masters, vec![node("node-c", 7002)]);
            assert_eq!(change.removed_masters, vec![node("node-b", 7001)]);
        }

        #[test]
        fn diff_reports_mapped_and_unmapped_owner_transitions() {
            let previous = topology(&[(0, 0, "node-a", 7000)]);
            let current = topology(&[(1, 1, "node-a", 7000)]);

            let change = diff(&previous, &current);

            assert_eq!(
                change.changed_slots,
                vec![
                    SlotOwnershipChange {
                        slot: 0,
                        old_owner: Some(node("node-a", 7000)),
                        new_owner: None,
                    },
                    SlotOwnershipChange {
                        slot: 1,
                        old_owner: None,
                        new_owner: Some(node("node-a", 7000)),
                    },
                ]
            );
            assert!(change.added_masters.is_empty());
            assert!(change.removed_masters.is_empty());
        }

        #[test]
        fn range_fragmentation_and_order_do_not_create_false_changes() {
            let previous = topology(&[(0, 9, "node-a", 7000), (10, 19, "node-b", 7001)]);
            let current = topology(&[
                (15, 19, "node-b", 7001),
                (0, 4, "node-a", 7000),
                (5, 9, "node-a", 7000),
                (10, 14, "node-b", 7001),
            ]);

            assert!(diff(&previous, &current).is_empty());
        }

        #[test]
        fn tracker_skips_replica_only_and_equivalent_changes() {
            let previous = ClusterTopology::new(vec![SlotRange {
                start: 0,
                end: 10,
                master: node("node-a", 7000),
                replicas: vec![node("replica-a", 7100)],
            }]);
            let current = ClusterTopology::new(vec![SlotRange {
                start: 0,
                end: 10,
                master: node("node-a", 7000),
                replicas: vec![node("replica-b", 7101)],
            }]);
            let mut tracker = TopologyChangeTracker::new();

            assert!(tracker.record(&previous, &current).is_none());
            assert_eq!(tracker.revision(), TopologyRevision::INITIAL);
            assert!(tracker.subscribe().borrow().is_none());
        }

        #[test]
        fn move_away_and_back_advances_revision_and_exposes_a_missed_change() {
            let original = topology(&[(0, 16_383, "node-a", 7000)]);
            let mut moved = original.clone();
            moved.reassign_slot(42, node("node-b", 7001));
            let mut returned = moved.clone();
            returned.reassign_slot(42, node("node-a", 7000));

            let mut tracker = TopologyChangeTracker::new();
            let original_snapshot = tracker.snapshot(&original);
            let receiver = tracker.subscribe();

            let first = tracker.record(&original, &moved).unwrap();
            assert_eq!(first.previous_revision.get(), 0);
            assert_eq!(first.revision.get(), 1);
            assert_eq!(
                first.continuity_after(original_snapshot.revision()),
                ChangeContinuity::Contiguous
            );

            let moved_snapshot = tracker.snapshot(&moved);
            let second = tracker.record(&moved, &returned).unwrap();
            assert_eq!(second.previous_revision.get(), 1);
            assert_eq!(second.revision.get(), 2);

            // Routing returned to its original owner, but the snapshot's
            // revision proves that an A -> B -> A transition occurred.
            let returned_snapshot = tracker.snapshot(&returned);
            assert_eq!(
                original_snapshot.topology().master_for_slot(42),
                returned_snapshot.topology().master_for_slot(42)
            );
            assert_ne!(original_snapshot.revision(), returned_snapshot.revision());

            // A watch receiver coalesces both writes to the latest one. A
            // consumer still at revision zero detects the gap and must clear;
            // one at revision one can apply the second change incrementally.
            let latest = receiver.borrow().clone().unwrap();
            assert_eq!(latest.revision, second.revision);
            assert_eq!(
                latest.continuity_after(original_snapshot.revision()),
                ChangeContinuity::Gap
            );
            assert_eq!(
                latest.continuity_after(moved_snapshot.revision()),
                ChangeContinuity::Contiguous
            );
            assert_eq!(
                latest.continuity_after(returned_snapshot.revision()),
                ChangeContinuity::AlreadyApplied
            );

            let slot_change = &latest.diff.changed_slots[0];
            assert_eq!(slot_change.slot, 42);
            assert_eq!(slot_change.old_owner, Some(node("node-b", 7001)));
            assert_eq!(slot_change.new_owner, Some(node("node-a", 7000)));
        }

        #[test]
        fn snapshot_can_return_its_topology() {
            let topology = topology(&[(0, 10, "node-a", 7000)]);
            let tracker = TopologyChangeTracker::new();
            let snapshot = tracker.snapshot(&topology);

            assert_eq!(snapshot.revision().get(), 0);
            assert_eq!(
                snapshot.into_topology().master_for_slot(5),
                Some(&node("node-a", 7000))
            );
        }
    }
}

/// Discover the cluster topology by sending CLUSTER SLOTS to a node.
pub async fn discover_topology(conn: &mut RedisConnection) -> Result<ClusterTopology, RedisError> {
    let frame = array(vec![bulk("CLUSTER"), bulk("SLOTS")]);
    conn.execute_pipeline(vec![frame]).await.and_then(|frames| {
        if frames.len() != 1 {
            return Err(RedisError::UnexpectedResponse {
                expected: "single CLUSTER SLOTS response",
                actual: format!("{} frames", frames.len()),
            });
        }
        parse_cluster_slots(&frames[0])
    })
}

/// Parse the response from CLUSTER SLOTS into a `ClusterTopology`.
///
/// The response is an array of slot ranges, each of which is:
/// `[start_slot, end_slot, [master_host, master_port, ...], [replica_host, replica_port, ...], ...]`
fn parse_cluster_slots(frame: &Frame) -> Result<ClusterTopology, RedisError> {
    let ranges = match frame {
        Frame::Array(Some(items)) => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "array of slot ranges",
                actual: format!("{other:?}"),
            });
        }
    };

    let mut slot_ranges = Vec::with_capacity(ranges.len());

    for range_frame in ranges {
        let range_items = match range_frame {
            Frame::Array(Some(items)) if items.len() >= 3 => items,
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "slot range array with >= 3 elements",
                    actual: format!("{other:?}"),
                });
            }
        };

        let start = extract_integer(&range_items[0])? as u16;
        let end = extract_integer(&range_items[1])? as u16;
        let master = parse_node_addr(&range_items[2])?;

        let mut replicas = Vec::new();
        for node_frame in &range_items[3..] {
            if let Ok(addr) = parse_node_addr(node_frame) {
                replicas.push(addr);
            }
        }

        slot_ranges.push(SlotRange {
            start,
            end,
            master,
            replicas,
        });
    }

    Ok(ClusterTopology::new(slot_ranges))
}

/// Parse a node address from a CLUSTER SLOTS node entry.
///
/// Each node entry is: `[host, port, node_id]` (host is bulk string, port is integer).
fn parse_node_addr(frame: &Frame) -> Result<NodeAddr, RedisError> {
    let items = match frame {
        Frame::Array(Some(items)) if items.len() >= 2 => items,
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "node array [host, port, ...]",
                actual: format!("{other:?}"),
            });
        }
    };

    let host = match &items[0] {
        Frame::BulkString(Some(b)) => String::from_utf8_lossy(b).into_owned(),
        other => {
            return Err(RedisError::UnexpectedResponse {
                expected: "bulk string host",
                actual: format!("{other:?}"),
            });
        }
    };

    let port = extract_integer(&items[1])? as u16;

    Ok(NodeAddr { host, port })
}

fn extract_integer(frame: &Frame) -> Result<i64, RedisError> {
    match frame {
        Frame::Integer(n) => Ok(*n),
        other => Err(RedisError::UnexpectedResponse {
            expected: "integer",
            actual: format!("{other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    /// Build a mock CLUSTER SLOTS response frame.
    type SlotDef<'a> = (u16, u16, (&'a str, u16), Vec<(&'a str, u16)>);

    fn mock_cluster_slots_response(ranges: Vec<SlotDef<'_>>) -> Frame {
        let mut slot_ranges = Vec::new();
        for (start, end, (master_host, master_port), replicas) in ranges {
            let mut range_items = vec![
                Frame::Integer(start as i64),
                Frame::Integer(end as i64),
                Frame::Array(Some(vec![
                    Frame::BulkString(Some(Bytes::from(master_host.to_string()))),
                    Frame::Integer(master_port as i64),
                    Frame::BulkString(Some(Bytes::from("master-node-id"))),
                ])),
            ];
            for (host, port) in replicas {
                range_items.push(Frame::Array(Some(vec![
                    Frame::BulkString(Some(Bytes::from(host.to_string()))),
                    Frame::Integer(port as i64),
                    Frame::BulkString(Some(Bytes::from("replica-node-id"))),
                ])));
            }
            slot_ranges.push(Frame::Array(Some(range_items)));
        }
        Frame::Array(Some(slot_ranges))
    }

    #[test]
    fn parse_three_master_topology() {
        let frame = mock_cluster_slots_response(vec![
            (0, 5460, ("127.0.0.1", 7000), vec![]),
            (5461, 10922, ("127.0.0.1", 7001), vec![]),
            (10923, 16383, ("127.0.0.1", 7002), vec![]),
        ]);
        let topo = parse_cluster_slots(&frame).unwrap();
        assert_eq!(topo.slot_ranges.len(), 3);
        assert_eq!(topo.master_addrs().len(), 3);

        // Verify slot ownership.
        assert_eq!(topo.master_for_slot(0).unwrap().port, 7000);
        assert_eq!(topo.master_for_slot(5460).unwrap().port, 7000);
        assert_eq!(topo.master_for_slot(5461).unwrap().port, 7001);
        assert_eq!(topo.master_for_slot(10922).unwrap().port, 7001);
        assert_eq!(topo.master_for_slot(10923).unwrap().port, 7002);
        assert_eq!(topo.master_for_slot(16383).unwrap().port, 7002);
    }

    #[test]
    fn parse_topology_with_replicas() {
        let frame = mock_cluster_slots_response(vec![
            (0, 5460, ("127.0.0.1", 7000), vec![("127.0.0.1", 7003)]),
            (5461, 10922, ("127.0.0.1", 7001), vec![("127.0.0.1", 7004)]),
            (10923, 16383, ("127.0.0.1", 7002), vec![("127.0.0.1", 7005)]),
        ]);
        let topo = parse_cluster_slots(&frame).unwrap();
        assert_eq!(topo.master_addrs().len(), 3);
        assert_eq!(topo.replica_addrs().len(), 3);

        let replicas_0 = topo.replicas_for_slot(0).unwrap();
        assert_eq!(replicas_0.len(), 1);
        assert_eq!(replicas_0[0].port, 7003);
    }

    #[test]
    fn master_for_slot_out_of_range() {
        let frame = mock_cluster_slots_response(vec![(0, 100, ("127.0.0.1", 7000), vec![])]);
        let topo = parse_cluster_slots(&frame).unwrap();
        assert!(topo.master_for_slot(101).is_none());
    }

    #[test]
    fn replicas_for_slot_no_replicas() {
        let frame = mock_cluster_slots_response(vec![(0, 16383, ("127.0.0.1", 7000), vec![])]);
        let topo = parse_cluster_slots(&frame).unwrap();
        let replicas = topo.replicas_for_slot(0).unwrap();
        assert!(replicas.is_empty());
    }

    #[test]
    fn parse_empty_topology() {
        let frame = Frame::Array(Some(vec![]));
        let topo = parse_cluster_slots(&frame).unwrap();
        assert!(topo.slot_ranges.is_empty());
        assert!(topo.master_for_slot(0).is_none());
    }

    #[test]
    fn parse_invalid_frame_type() {
        let frame = Frame::SimpleString(Bytes::from("OK"));
        let result = parse_cluster_slots(&frame);
        assert!(result.is_err());
    }

    #[test]
    fn parse_invalid_range_too_few_elements() {
        let frame = Frame::Array(Some(vec![Frame::Array(Some(vec![
            Frame::Integer(0),
            Frame::Integer(100),
            // Missing master node array.
        ]))]));
        let result = parse_cluster_slots(&frame);
        assert!(result.is_err());
    }

    #[test]
    fn multiple_replicas_per_slot() {
        let frame = mock_cluster_slots_response(vec![(
            0,
            16383,
            ("127.0.0.1", 7000),
            vec![("127.0.0.1", 7001), ("127.0.0.1", 7002)],
        )]);
        let topo = parse_cluster_slots(&frame).unwrap();
        let replicas = topo.replicas_for_slot(0).unwrap();
        assert_eq!(replicas.len(), 2);
    }

    // -- reassign_slot (single-slot MOVED patching) --

    fn node(port: u16) -> NodeAddr {
        NodeAddr {
            host: "127.0.0.1".to_string(),
            port,
        }
    }

    fn topo_with(ranges: &[(u16, u16, u16)]) -> ClusterTopology {
        ClusterTopology::new(
            ranges
                .iter()
                .map(|&(start, end, port)| SlotRange {
                    start,
                    end,
                    master: node(port),
                    replicas: vec![],
                })
                .collect(),
        )
    }

    #[test]
    fn reassign_slot_splits_containing_range_in_three() {
        let mut topo = topo_with(&[(0, 100, 7000)]);
        topo.reassign_slot(50, node(7009));
        // Only slot 50 moved; every other slot keeps the old owner.
        assert_eq!(topo.master_for_slot(50).unwrap().port, 7009);
        assert_eq!(topo.master_for_slot(49).unwrap().port, 7000);
        assert_eq!(topo.master_for_slot(51).unwrap().port, 7000);
        assert_eq!(topo.master_for_slot(0).unwrap().port, 7000);
        assert_eq!(topo.master_for_slot(100).unwrap().port, 7000);
        // Split into 0-49, 50-50, 51-100.
        assert_eq!(topo.slot_ranges.len(), 3);
    }

    #[test]
    fn reassign_slot_at_range_start_splits_in_two() {
        let mut topo = topo_with(&[(0, 100, 7000)]);
        topo.reassign_slot(0, node(7009));
        assert_eq!(topo.master_for_slot(0).unwrap().port, 7009);
        assert_eq!(topo.master_for_slot(1).unwrap().port, 7000);
        assert_eq!(topo.slot_ranges.len(), 2);
    }

    #[test]
    fn reassign_slot_at_range_end_splits_in_two() {
        let mut topo = topo_with(&[(0, 100, 7000)]);
        topo.reassign_slot(100, node(7009));
        assert_eq!(topo.master_for_slot(100).unwrap().port, 7009);
        assert_eq!(topo.master_for_slot(99).unwrap().port, 7000);
        assert_eq!(topo.slot_ranges.len(), 2);
    }

    #[test]
    fn reassign_single_slot_range_replaces_in_place() {
        let mut topo = topo_with(&[(50, 50, 7000)]);
        topo.reassign_slot(50, node(7009));
        assert_eq!(topo.master_for_slot(50).unwrap().port, 7009);
        assert_eq!(topo.slot_ranges.len(), 1);
    }

    #[test]
    fn reassign_unmapped_slot_adds_standalone_range() {
        let mut topo = topo_with(&[(0, 100, 7000)]);
        topo.reassign_slot(5000, node(7009));
        assert_eq!(topo.master_for_slot(5000).unwrap().port, 7009);
        assert_eq!(topo.master_for_slot(50).unwrap().port, 7000);
        assert_eq!(topo.slot_ranges.len(), 2);
    }

    #[test]
    fn reassign_slot_to_current_owner_is_noop() {
        let mut topo = topo_with(&[(0, 100, 7000)]);
        topo.reassign_slot(50, node(7000));
        assert_eq!(topo.slot_ranges.len(), 1);
        assert_eq!(topo.master_for_slot(50).unwrap().port, 7000);
    }

    #[test]
    fn reassign_slot_clears_moved_replicas_but_keeps_flank_replicas() {
        let mut topo = ClusterTopology::new(vec![SlotRange {
            start: 0,
            end: 100,
            master: node(7000),
            replicas: vec![node(7100)],
        }]);
        topo.reassign_slot(50, node(7009));
        // A MOVED gives the new master but not its replicas.
        assert_eq!(topo.replicas_for_slot(50).unwrap().len(), 0);
        // The flanks retain the original replica.
        assert_eq!(topo.replicas_for_slot(49).unwrap(), &[node(7100)][..]);
        assert_eq!(topo.replicas_for_slot(51).unwrap(), &[node(7100)][..]);
    }

    #[test]
    fn master_addrs_dedups_a_master_fragmented_by_a_split() {
        let mut topo = topo_with(&[(0, 100, 7000)]);
        topo.reassign_slot(50, node(7009));
        // slot_ranges now owns [7000, 7009, 7000]; 7000 must be reported once.
        assert_eq!(topo.master_addrs().len(), 2);
    }

    // -- flat slot-to-owner table (O(1) routing) --

    #[test]
    fn slot_table_routes_every_slot_across_a_full_cluster() {
        // Sixteen even ranges covering all 16384 slots, so lookups exercise the
        // whole table, not just the boundaries.
        let ranges: Vec<(u16, u16, u16)> = (0..16u16)
            .map(|i| {
                let start = i * 1024;
                (start, start + 1023, 7000 + i)
            })
            .collect();
        let topo = topo_with(&ranges);

        for slot in 0..16_384u16 {
            let expected_port = 7000 + (slot / 1024);
            assert_eq!(
                topo.master_for_slot(slot).unwrap().port,
                expected_port,
                "slot {slot} routed to the wrong master",
            );
        }
    }

    #[test]
    fn slot_table_reports_unowned_and_out_of_range_slots_as_none() {
        // A gap between two ranges leaves the middle slots unowned.
        let topo = topo_with(&[(0, 100, 7000), (200, 300, 7001)]);
        assert_eq!(topo.master_for_slot(100).unwrap().port, 7000);
        assert!(topo.master_for_slot(150).is_none());
        assert_eq!(topo.master_for_slot(200).unwrap().port, 7001);
        // Slots at or beyond SLOT_COUNT are never valid.
        assert!(topo.master_for_slot(16_384).is_none());
        assert!(topo.master_for_slot(u16::MAX).is_none());
    }

    #[test]
    fn slot_table_stays_consistent_after_reassign_splits() {
        let mut topo = topo_with(&[(0, 16_383, 7000)]);
        // A live resharding moves a scatter of individual slots to a new owner.
        for slot in [42u16, 4242, 8484, 12_726] {
            topo.reassign_slot(slot, node(7009));
        }
        for slot in [42u16, 4242, 8484, 12_726] {
            assert_eq!(topo.master_for_slot(slot).unwrap().port, 7009);
            assert_eq!(topo.master_for_slot(slot - 1).unwrap().port, 7000);
            assert_eq!(topo.master_for_slot(slot + 1).unwrap().port, 7000);
        }
        // The table agrees with a fresh linear scan of the ranges.
        for slot in 0..16_384u16 {
            let scanned = topo
                .slot_ranges()
                .iter()
                .find(|r| slot >= r.start && slot <= r.end)
                .map(|r| r.master.port);
            assert_eq!(topo.master_for_slot(slot).map(|a| a.port), scanned);
        }
    }
}
