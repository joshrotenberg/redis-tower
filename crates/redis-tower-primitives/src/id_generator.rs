//! Persistent block-allocated numeric IDs.
//!
//! [`IdGenerator::allocate`] issues exactly one `INCRBY` and returns the newly
//! reserved range as an [`IdBlock`]. Iterating IDs inside the block performs no
//! Redis I/O and starts no task.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::MultiplexedClient;
//! use redis_tower_primitives::IdGenerator;
//!
//! let mut client = MultiplexedClient::connect("127.0.0.1:6379").await?;
//! let generator = IdGenerator::new("orders:next-id", 1_000)?;
//! let mut block = generator.allocate(&mut client).await?;
//!
//! let first_id = block.next().expect("a non-empty block");
//! # let _ = first_id;
//! # Ok(())
//! # }
//! ```
//!
//! # Persistence and failure mode
//!
//! The counter intentionally has no TTL: deleting, expiring, or restoring an
//! older value can reissue IDs. Redis failover can likewise roll back an
//! increment unless deployment durability prevents it. A lost allocation
//! response makes that block indeterminate; callers may allocate another block
//! and leave a gap, but must not guess or reuse the unknown range.
//!
//! # Cluster keys
//!
//! Allocation touches one Redis key and is cluster-safe without a hash tag.

use std::fmt;
use std::iter::FusedIterator;

use redis_tower::commands::IncrBy;
use redis_tower::{RedisError, RedisExecutor};

use crate::error::{ConfigurationError, require_key};

/// The one Redis command used to reserve every ID block.
pub const ID_ALLOCATION_COMMAND: &str = "INCRBY";

/// Configuration for one persistent Redis ID counter.
#[derive(Clone)]
pub struct IdGenerator {
    key: String,
    block_size: u64,
    increment: i64,
}

impl IdGenerator {
    /// Create an ID generator with an explicit positive allocation block size.
    pub fn new(key: impl Into<String>, block_size: u64) -> Result<Self, ConfigurationError> {
        let key = require_key(key, "key")?;
        if block_size == 0 {
            return Err(ConfigurationError::ZeroIdBlockSize);
        }
        let increment =
            i64::try_from(block_size).map_err(|_| ConfigurationError::IdBlockSizeTooLarge)?;
        Ok(Self {
            key,
            block_size,
            increment,
        })
    }

    /// Return the persistent Redis counter key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Return the number of IDs reserved by each Redis command.
    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    /// Reserve and return the next contiguous positive ID block.
    ///
    /// This performs exactly one `INCRBY`. A missing counter starts at zero,
    /// so the first block begins at ID 1. The counter key must be exclusively
    /// owned by generators using compatible positive increments.
    ///
    /// A lost response makes the allocated block unknown. Allocate another
    /// block and accept the gap rather than inferring IDs from local state.
    pub async fn allocate<E: RedisExecutor>(
        &self,
        executor: &mut E,
    ) -> Result<IdBlock, RedisError> {
        let end = executor
            .execute(IncrBy::new(self.key.as_str(), self.increment))
            .await?;
        if end < self.increment {
            return Err(RedisError::UnexpectedResponse {
                expected: "positive ID counter at least as large as the block size",
                actual: end.to_string(),
            });
        }
        let end = end as u64;
        let first = end - self.block_size + 1;
        Ok(IdBlock {
            first,
            next: first,
            last: end,
        })
    }
}

impl fmt::Debug for IdGenerator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdGenerator")
            .field("key", &self.key)
            .field("block_size", &self.block_size)
            .finish()
    }
}

/// One contiguous ID range reserved by a single Redis `INCRBY`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdBlock {
    first: u64,
    next: u64,
    last: u64,
}

impl IdBlock {
    /// Return the first ID originally reserved in this block.
    pub fn first_id(&self) -> u64 {
        self.first
    }

    /// Return the final ID reserved in this block.
    pub fn last_id(&self) -> u64 {
        self.last
    }

    /// Return the next unconsumed ID, or `None` when exhausted.
    pub fn next_id(&self) -> Option<u64> {
        (self.next <= self.last).then_some(self.next)
    }

    /// Return the number of IDs not yet yielded locally.
    pub fn remaining(&self) -> u64 {
        if self.next > self.last {
            0
        } else {
            self.last - self.next + 1
        }
    }
}

impl Iterator for IdBlock {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        let id = self.next_id()?;
        self.next += 1;
        Some(id)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match usize::try_from(self.remaining()) {
            Ok(remaining) => (remaining, Some(remaining)),
            Err(_) => (usize::MAX, None),
        }
    }
}

impl FusedIterator for IdBlock {}

#[cfg(test)]
mod tests {
    use redis_tower::{Command, Frame};

    use super::*;

    struct AllocationExecutor {
        calls: usize,
        reply: i64,
    }

    impl RedisExecutor for AllocationExecutor {
        async fn execute<Cmd: Command>(&mut self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
            self.calls += 1;
            assert_eq!(cmd.name(), ID_ALLOCATION_COMMAND);
            cmd.parse_response(Frame::Integer(self.reply))
        }
    }

    #[test]
    fn configuration_requires_key_and_positive_signed_block() {
        assert_eq!(
            IdGenerator::new("", 1).unwrap_err(),
            ConfigurationError::EmptyKey { parameter: "key" }
        );
        assert_eq!(
            IdGenerator::new("ids", 0).unwrap_err(),
            ConfigurationError::ZeroIdBlockSize
        );
        assert_eq!(
            IdGenerator::new("ids", i64::MAX as u64 + 1).unwrap_err(),
            ConfigurationError::IdBlockSizeTooLarge
        );
    }

    #[test]
    fn block_iterates_only_its_reserved_range() {
        let mut block = IdBlock {
            first: 7,
            next: 7,
            last: 9,
        };
        assert_eq!(block.first_id(), 7);
        assert_eq!(block.last_id(), 9);
        assert_eq!(block.remaining(), 3);
        assert_eq!(block.by_ref().collect::<Vec<_>>(), vec![7, 8, 9]);
        assert_eq!(block.next_id(), None);
        assert_eq!(block.remaining(), 0);
        assert_eq!(block.next(), None);
    }

    #[test]
    fn allocation_command_is_explicit() {
        assert_eq!(ID_ALLOCATION_COMMAND, "INCRBY");
    }

    #[tokio::test]
    async fn allocation_uses_one_command_and_rejects_non_positive_range() {
        let generator = IdGenerator::new("ids", 3).unwrap();
        let mut executor = AllocationExecutor { calls: 0, reply: 3 };
        assert_eq!(
            generator
                .allocate(&mut executor)
                .await
                .unwrap()
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(executor.calls, 1);

        let mut corrupt = AllocationExecutor {
            calls: 0,
            reply: -1,
        };
        assert!(generator.allocate(&mut corrupt).await.is_err());
        assert_eq!(corrupt.calls, 1);
    }
}
