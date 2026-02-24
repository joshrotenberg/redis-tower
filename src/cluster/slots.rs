//! Redis cluster slot calculation and mapping
//!
//! Redis Cluster uses CRC16 to map keys to slots (0-16383).
//! Each slot is served by one master node.

use std::collections::HashMap;

/// Total number of slots in Redis Cluster
pub const SLOT_COUNT: u16 = 16384;

/// Calculate the slot for a given key
///
/// Uses CRC16 with the XMODEM polynomial, matching Redis's implementation.
///
/// # Hash Tags
///
/// If the key contains `{...}`, only the content between the first `{` and `}`
/// is hashed. This allows multiple keys to be stored in the same slot.
///
/// # Example
/// ```
/// use redis_tower::cluster::slot_for_key;
///
/// let slot1 = slot_for_key(b"user:123");
/// let slot2 = slot_for_key(b"{user}:123");
/// let slot3 = slot_for_key(b"{user}:456");
///
/// // slot2 and slot3 will be the same (both hash "user")
/// // slot1 will likely be different
/// assert_eq!(slot2, slot3);
/// ```
pub fn slot_for_key(key: &[u8]) -> u16 {
    let hash_key = extract_hash_tag(key);
    crc16_xmodem(hash_key) % SLOT_COUNT
}

/// Extract the hash tag from a key
///
/// If the key contains `{tag}`, returns just the tag content.
/// Otherwise, returns the full key.
fn extract_hash_tag(key: &[u8]) -> &[u8] {
    // Find first '{' and '}'
    if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
            let tag_start = start + 1;
            let tag_end = start + 1 + end;

            // Only use hash tag if it's not empty
            if tag_end > tag_start {
                return &key[tag_start..tag_end];
            }
        }
    }

    key
}

/// CRC16 using XMODEM polynomial (same as Redis)
///
/// Polynomial: x^16 + x^12 + x^5 + 1 (0x1021)
fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;

    for &byte in data {
        crc ^= (byte as u16) << 8;

        for _ in 0..8 {
            if (crc & 0x8000) != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }

    crc
}

/// Node assignment for a slot
#[derive(Debug, Clone)]
pub struct SlotAssignment {
    /// Master node address
    pub master: String,
    /// Replica node addresses (if any)
    pub replicas: Vec<String>,
}

/// Mapping of slots to node addresses
#[derive(Debug, Clone)]
pub struct SlotMap {
    /// Maps each slot (0-16383) to its master and replicas
    slots: Vec<Option<SlotAssignment>>,
}

impl SlotMap {
    /// Create a new empty slot map
    pub fn new() -> Self {
        Self {
            slots: vec![None; SLOT_COUNT as usize],
        }
    }

    /// Assign a range of slots to a node (master only, for backward compatibility)
    pub fn assign_slots(&mut self, start: u16, end: u16, node_addr: String) {
        for slot in start..=end {
            if (slot as usize) < self.slots.len() {
                self.slots[slot as usize] = Some(SlotAssignment {
                    master: node_addr.clone(),
                    replicas: Vec::new(),
                });
            }
        }
    }

    /// Assign a range of slots with master and replicas
    pub fn assign_slots_with_replicas(
        &mut self,
        start: u16,
        end: u16,
        master: String,
        replicas: Vec<String>,
    ) {
        for slot in start..=end {
            if (slot as usize) < self.slots.len() {
                self.slots[slot as usize] = Some(SlotAssignment {
                    master: master.clone(),
                    replicas: replicas.clone(),
                });
            }
        }
    }

    /// Get the master node address for a given slot
    pub fn get_node(&self, slot: u16) -> Option<&str> {
        self.slots
            .get(slot as usize)
            .and_then(|opt| opt.as_ref().map(|a| a.master.as_str()))
    }

    /// Get the slot assignment (master + replicas) for a given slot
    pub fn get_assignment(&self, slot: u16) -> Option<&SlotAssignment> {
        self.slots.get(slot as usize).and_then(|opt| opt.as_ref())
    }

    /// Set the node address for a single slot (master only)
    pub fn set_slot(&mut self, slot: u16, node_addr: String) {
        if (slot as usize) < self.slots.len() {
            self.slots[slot as usize] = Some(SlotAssignment {
                master: node_addr,
                replicas: Vec::new(),
            });
        }
    }

    /// Set the slot assignment with master and replicas
    pub fn set_slot_assignment(&mut self, slot: u16, assignment: SlotAssignment) {
        if (slot as usize) < self.slots.len() {
            self.slots[slot as usize] = Some(assignment);
        }
    }

    /// Get the master node address for a given key
    pub fn get_node_for_key(&self, key: &[u8]) -> Option<&str> {
        let slot = slot_for_key(key);
        self.get_node(slot)
    }

    /// Get the slot assignment for a given key
    pub fn get_assignment_for_key(&self, key: &[u8]) -> Option<&SlotAssignment> {
        let slot = slot_for_key(key);
        self.get_assignment(slot)
    }

    /// Check if all slots are assigned
    pub fn is_fully_assigned(&self) -> bool {
        self.slots.iter().all(|s| s.is_some())
    }

    /// Get statistics about slot assignments
    pub fn stats(&self) -> SlotMapStats {
        let mut node_slots: HashMap<String, u16> = HashMap::new();
        let mut unassigned = 0;

        for slot_opt in &self.slots {
            if let Some(assignment) = slot_opt {
                *node_slots.entry(assignment.master.clone()).or_insert(0) += 1;
            } else {
                unassigned += 1;
            }
        }

        SlotMapStats {
            total_slots: SLOT_COUNT,
            assigned_slots: SLOT_COUNT - unassigned,
            unassigned_slots: unassigned,
            nodes: node_slots.len() as u16,
            node_distribution: node_slots,
        }
    }
}

impl Default for SlotMap {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about slot assignment
#[derive(Debug, Clone)]
pub struct SlotMapStats {
    /// Total number of slots (always 16384)
    pub total_slots: u16,
    /// Number of assigned slots
    pub assigned_slots: u16,
    /// Number of unassigned slots
    pub unassigned_slots: u16,
    /// Number of nodes
    pub nodes: u16,
    /// Distribution of slots per node
    pub node_distribution: HashMap<String, u16>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16_known_values() {
        // Test vectors from Redis source code
        assert_eq!(crc16_xmodem(b"123456789"), 0x31C3);
    }

    #[test]
    fn test_slot_calculation() {
        // Slot should be in valid range
        let slot = slot_for_key(b"mykey");
        assert!(slot < SLOT_COUNT);
    }

    #[test]
    fn test_hash_tag_extraction() {
        // Keys with same hash tag should have same slot
        let slot1 = slot_for_key(b"{user}:123");
        let slot2 = slot_for_key(b"{user}:456");
        let slot3 = slot_for_key(b"{user}:789");

        assert_eq!(slot1, slot2);
        assert_eq!(slot2, slot3);

        // Keys without hash tag should differ
        let slot_a = slot_for_key(b"user:123");
        let slot_b = slot_for_key(b"user:456");

        // These will likely be different (not guaranteed, but very likely)
        // We can't assert inequality, but we can verify they're in valid range
        assert!(slot_a < SLOT_COUNT);
        assert!(slot_b < SLOT_COUNT);
    }

    #[test]
    fn test_empty_hash_tag() {
        // Empty hash tag should use full key
        let slot1 = slot_for_key(b"{}user:123");
        let slot2 = slot_for_key(b"user:123");

        // Should not be equal (different keys)
        // We verify they're both valid
        assert!(slot1 < SLOT_COUNT);
        assert!(slot2 < SLOT_COUNT);
    }

    #[test]
    fn test_extract_hash_tag() {
        assert_eq!(extract_hash_tag(b"key"), b"key");
        assert_eq!(extract_hash_tag(b"{tag}key"), b"tag");
        assert_eq!(extract_hash_tag(b"prefix{tag}suffix"), b"tag");
        assert_eq!(extract_hash_tag(b"{}key"), b"{}key"); // Empty tag, use full key
        assert_eq!(extract_hash_tag(b"{"), b"{"); // No closing brace
    }

    #[test]
    fn test_slot_map() {
        let mut map = SlotMap::new();

        // Assign some slots
        map.assign_slots(0, 5460, "127.0.0.1:7000".to_string());
        map.assign_slots(5461, 10922, "127.0.0.1:7001".to_string());
        map.assign_slots(10923, 16383, "127.0.0.1:7002".to_string());

        // Check assignments
        assert_eq!(map.get_node(0), Some("127.0.0.1:7000"));
        assert_eq!(map.get_node(5461), Some("127.0.0.1:7001"));
        assert_eq!(map.get_node(16383), Some("127.0.0.1:7002"));

        // Check stats
        let stats = map.stats();
        assert_eq!(stats.total_slots, 16384);
        assert_eq!(stats.assigned_slots, 16384);
        assert_eq!(stats.unassigned_slots, 0);
        assert_eq!(stats.nodes, 3);
        assert!(map.is_fully_assigned());
    }

    #[test]
    fn test_get_node_for_key() {
        let mut map = SlotMap::new();
        map.assign_slots(0, 16383, "127.0.0.1:7000".to_string());

        let node = map.get_node_for_key(b"mykey");
        assert_eq!(node, Some("127.0.0.1:7000"));
    }
}
