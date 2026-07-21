//! Cluster topology discovery via CLUSTER SLOTS.

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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeAddr {
    pub host: String,
    pub port: u16,
}

impl NodeAddr {
    pub fn addr_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl std::fmt::Display for NodeAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

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
    fn node_addr_display() {
        let addr = NodeAddr {
            host: "127.0.0.1".to_string(),
            port: 7000,
        };
        assert_eq!(addr.to_string(), "127.0.0.1:7000");
        assert_eq!(addr.addr_string(), "127.0.0.1:7000");
    }

    #[test]
    fn node_addr_equality() {
        let a = NodeAddr {
            host: "127.0.0.1".to_string(),
            port: 7000,
        };
        let b = NodeAddr {
            host: "127.0.0.1".to_string(),
            port: 7000,
        };
        let c = NodeAddr {
            host: "127.0.0.1".to_string(),
            port: 7001,
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
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
