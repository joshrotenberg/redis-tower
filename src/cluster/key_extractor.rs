//! KeyExtractor trait implementations for all commands
//!
//! This module implements the KeyExtractor trait for all Redis commands
//! that operate on keys, enabling the cluster client to route commands
//! to the correct node based on key hash slots.

use crate::cluster::client::KeyExtractor;
use crate::commands::*;

// ========== String Commands ==========

impl KeyExtractor for Get {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for Set {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for Del {
    fn extract_key(&self) -> Option<String> {
        // DEL can take multiple keys, but in cluster mode,
        // all keys must hash to the same slot. Return the first key.
        self.keys.first().cloned()
    }
}

impl KeyExtractor for Incr {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for Decr {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for IncrBy {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for DecrBy {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for IncrByFloat {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for Append {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for StrLen {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for GetRange {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for SetRange {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for GetEx {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for GetDel {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for MGet {
    fn extract_key(&self) -> Option<String> {
        // MGET can take multiple keys, return first for routing
        self.keys.first().cloned()
    }
}

impl KeyExtractor for Exists {
    fn extract_key(&self) -> Option<String> {
        // EXISTS can take multiple keys, return first for routing
        self.keys.first().cloned()
    }
}

impl KeyExtractor for Ttl {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for Expire {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for Mset {
    fn extract_key(&self) -> Option<String> {
        // MSET sets multiple keys, return first for routing
        self.pairs.first().map(|(k, _)| k.clone())
    }
}

impl KeyExtractor for Ping {
    fn extract_key(&self) -> Option<String> {
        // PING doesn't operate on a key, can be sent to any node
        None
    }
}

impl KeyExtractor for Echo {
    fn extract_key(&self) -> Option<String> {
        // ECHO doesn't operate on a key, can be sent to any node
        None
    }
}

// ========== Hash Commands ==========

impl KeyExtractor for hashes::HGet {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for hashes::HSet {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for hashes::HGetAll {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for hashes::HDel {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for hashes::HExists {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for hashes::HLen {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for hashes::HKeys {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for hashes::HVals {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for hashes::HMGet {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for hashes::HIncrBy {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for hashes::HIncrByFloat {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for hashes::HStrLen {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

// ========== List Commands ==========

impl KeyExtractor for lists::LPush {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for lists::RPush {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for lists::LPop {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for lists::RPop {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for lists::LRange {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for lists::LLen {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for lists::LIndex {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for lists::LSet {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for lists::LInsert {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for lists::LRem {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for lists::LTrim {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for lists::LPos {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for lists::BLPop {
    fn extract_key(&self) -> Option<String> {
        // BLPOP can take multiple keys, return first for routing
        self.keys.first().cloned()
    }
}

impl KeyExtractor for lists::BRPop {
    fn extract_key(&self) -> Option<String> {
        // BRPOP can take multiple keys, return first for routing
        self.keys.first().cloned()
    }
}

// ========== Set Commands ==========

impl KeyExtractor for sets::Sadd {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sets::Srem {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sets::Smembers {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sets::Sismember {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sets::Scard {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sets::Sinter {
    fn extract_key(&self) -> Option<String> {
        // SINTER operates on multiple keys, return first for routing
        self.keys.first().cloned()
    }
}

impl KeyExtractor for sets::Sunion {
    fn extract_key(&self) -> Option<String> {
        // SUNION operates on multiple keys, return first for routing
        self.keys.first().cloned()
    }
}

impl KeyExtractor for sets::Sdiff {
    fn extract_key(&self) -> Option<String> {
        // SDIFF operates on multiple keys, return first for routing
        self.keys.first().cloned()
    }
}

impl KeyExtractor for sets::Sscan {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sets::SPop {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sets::SRandMember {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sets::SMove {
    fn extract_key(&self) -> Option<String> {
        // SMOVE has source and destination, return source for routing
        Some(self.source.clone())
    }
}

impl KeyExtractor for sets::SInterStore {
    fn extract_key(&self) -> Option<String> {
        // SINTERSTORE writes to destination, route by destination
        Some(self.destination.clone())
    }
}

impl KeyExtractor for sets::SUnionStore {
    fn extract_key(&self) -> Option<String> {
        // SUNIONSTORE writes to destination, route by destination
        Some(self.destination.clone())
    }
}

impl KeyExtractor for sets::SDiffStore {
    fn extract_key(&self) -> Option<String> {
        // SDIFFSTORE writes to destination, route by destination
        Some(self.destination.clone())
    }
}

impl KeyExtractor for sets::SMIsMember {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sets::SInterCard {
    fn extract_key(&self) -> Option<String> {
        // SINTERCARD operates on multiple keys, return first for routing
        self.keys.first().cloned()
    }
}

// ========== Sorted Set Commands ==========

impl KeyExtractor for sorted_sets::Zadd {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sorted_sets::Zrem {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sorted_sets::Zcard {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sorted_sets::Zscore {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sorted_sets::Zrange {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sorted_sets::Zrevrange {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sorted_sets::Zrank {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sorted_sets::Zrevrank {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sorted_sets::Zincrby {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for sorted_sets::Zscan {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

// ========== Stream Commands ==========

impl KeyExtractor for streams::XAdd {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for streams::XRead {
    fn extract_key(&self) -> Option<String> {
        // XREAD can read from multiple streams, return first for routing
        self.streams.first().map(|(k, _)| k.clone())
    }
}

// XRange and XLen not yet implemented

// ========== Scan Commands ==========

impl KeyExtractor for scan::Scan {
    fn extract_key(&self) -> Option<String> {
        // SCAN iterates over the entire keyspace, no specific key
        None
    }
}

// Hscan is for hashes, Zscan is in sorted_sets module (already implemented above)

// ========== Transaction Commands ==========

impl KeyExtractor for crate::transaction::Watch {
    fn extract_key(&self) -> Option<String> {
        // WATCH can watch multiple keys, return first for routing
        self.keys.first().cloned()
    }
}

impl KeyExtractor for crate::transaction::Unwatch {
    fn extract_key(&self) -> Option<String> {
        // UNWATCH doesn't operate on a specific key
        None
    }
}

// Note: Multi, Exec, Discard are internal to Transaction and not standalone commands

// ========== Scripting Commands ==========

impl KeyExtractor for scripting::Eval {
    fn extract_key(&self) -> Option<String> {
        // EVAL can access multiple keys, return first for routing
        self.keys.first().cloned()
    }
}

impl KeyExtractor for scripting::EvalSha {
    fn extract_key(&self) -> Option<String> {
        // EVALSHA can access multiple keys, return first for routing
        self.keys.first().cloned()
    }
}

impl KeyExtractor for scripting::ScriptLoad {
    fn extract_key(&self) -> Option<String> {
        // SCRIPT LOAD doesn't operate on a key
        None
    }
}

impl KeyExtractor for scripting::ScriptExists {
    fn extract_key(&self) -> Option<String> {
        // SCRIPT EXISTS doesn't operate on a key
        None
    }
}

impl KeyExtractor for scripting::ScriptFlush {
    fn extract_key(&self) -> Option<String> {
        // SCRIPT FLUSH doesn't operate on a key
        None
    }
}

// ========== Pub/Sub Commands ==========

impl KeyExtractor for pubsub::Publish {
    fn extract_key(&self) -> Option<String> {
        // PUBLISH uses channel name, which can be used for routing
        Some(self.channel.clone())
    }
}

impl KeyExtractor for pubsub::PubsubNumsub {
    fn extract_key(&self) -> Option<String> {
        // PUBSUB NUMSUB can query multiple channels, return first
        self.channels.first().cloned()
    }
}

impl KeyExtractor for pubsub::PubsubNumpat {
    fn extract_key(&self) -> Option<String> {
        // PUBSUB NUMPAT doesn't operate on a specific key
        None
    }
}

// PubsubChannels not yet implemented

// ========== Cluster Commands ==========

impl KeyExtractor for crate::cluster::commands::ClusterSlots {
    fn extract_key(&self) -> Option<String> {
        // CLUSTER SLOTS is a cluster management command, no key
        None
    }
}

impl KeyExtractor for crate::cluster::commands::ClusterNodes {
    fn extract_key(&self) -> Option<String> {
        // CLUSTER NODES is a cluster management command, no key
        None
    }
}

impl KeyExtractor for crate::cluster::commands::ClusterInfo {
    fn extract_key(&self) -> Option<String> {
        // CLUSTER INFO is a cluster management command, no key
        None
    }
}

impl KeyExtractor for crate::cluster::commands::Asking {
    fn extract_key(&self) -> Option<String> {
        // ASKING is a cluster protocol command, no key
        None
    }
}

// ========== Streams Commands ==========

impl KeyExtractor for crate::commands::streams::XLen {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for crate::commands::streams::XDel {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for crate::commands::streams::XTrim {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for crate::commands::streams::XRange {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

impl KeyExtractor for crate::commands::streams::XRevRange {
    fn extract_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_key_extraction() {
        let cmd = Get::new("mykey");
        assert_eq!(cmd.extract_key(), Some("mykey".to_string()));

        let cmd = Set::new("mykey", b"value".to_vec());
        assert_eq!(cmd.extract_key(), Some("mykey".to_string()));
    }

    #[test]
    fn test_multi_key_extraction() {
        let cmd = Del::new(vec!["key1".to_string(), "key2".to_string()]);
        assert_eq!(cmd.extract_key(), Some("key1".to_string()));

        let cmd = MGet::new(vec!["key1".to_string(), "key2".to_string()]);
        assert_eq!(cmd.extract_key(), Some("key1".to_string()));
    }

    #[test]
    fn test_no_key_commands() {
        let cmd = Ping::new();
        assert_eq!(cmd.extract_key(), None);

        let cmd = Echo::new("hello");
        assert_eq!(cmd.extract_key(), None);
    }
}
