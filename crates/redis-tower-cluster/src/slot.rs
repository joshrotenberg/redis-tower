//! CRC16 slot calculation for Redis Cluster.

/// Total number of hash slots in a Redis Cluster.
pub const SLOT_COUNT: u16 = 16384;

/// CRC16 lookup table (XMODEM polynomial).
static CRC16_TABLE: [u16; 256] = {
    let mut table = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut j = 0;
        while j < 8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// Compute the CRC16 hash of a byte slice (XMODEM variant).
fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        let index = ((crc >> 8) ^ (byte as u16)) as usize;
        crc = (crc << 8) ^ CRC16_TABLE[index];
    }
    crc
}

/// Extract a hash tag from a key.
///
/// Redis Cluster uses the content between the first `{` and the next `}`
/// as the hash tag. If no valid tag is found, the entire key is used.
///
/// Examples:
/// - `{user:1}:name` -> `user:1`
/// - `foo{bar}baz` -> `bar`
/// - `foo{}bar` -> entire key (empty tag ignored)
/// - `foo` -> entire key
pub fn extract_hash_tag(key: &[u8]) -> &[u8] {
    if let Some(start) = key.iter().position(|&b| b == b'{') {
        if let Some(end) = key[start + 1..].iter().position(|&b| b == b'}') {
            if end > 0 {
                return &key[start + 1..start + 1 + end];
            }
        }
    }
    key
}

/// Calculate the slot number for a given key.
pub fn slot_for_key(key: &[u8]) -> u16 {
    let hash_input = extract_hash_tag(key);
    crc16(hash_input) % SLOT_COUNT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_tag_extraction() {
        assert_eq!(extract_hash_tag(b"{user:1}:name"), b"user:1");
        assert_eq!(extract_hash_tag(b"foo{bar}baz"), b"bar");
        assert_eq!(extract_hash_tag(b"foo{}bar"), b"foo{}bar");
        assert_eq!(extract_hash_tag(b"plain_key"), b"plain_key");
        assert_eq!(extract_hash_tag(b"{tag}"), b"tag");
        assert_eq!(extract_hash_tag(b"no_braces"), b"no_braces");
        assert_eq!(extract_hash_tag(b"{only_open"), b"{only_open");
    }

    #[test]
    fn slot_calculation() {
        // Known values from Redis documentation / redis-cli.
        assert_eq!(slot_for_key(b"foo"), 12182);
        assert_eq!(slot_for_key(b"bar"), 5061);
        assert_eq!(slot_for_key(b"hello"), 866);
    }

    #[test]
    fn hash_tag_routing() {
        // Keys with the same hash tag should map to the same slot.
        let slot1 = slot_for_key(b"{user:1}:name");
        let slot2 = slot_for_key(b"{user:1}:email");
        assert_eq!(slot1, slot2);
    }

    #[test]
    fn slot_range() {
        for key in [b"a".as_slice(), b"z", b"test", b"0", b"long_key_name"] {
            let slot = slot_for_key(key);
            assert!(
                slot < SLOT_COUNT,
                "slot {slot} out of range for key {key:?}"
            );
        }
    }
}
