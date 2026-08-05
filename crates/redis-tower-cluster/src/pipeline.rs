//! Cluster-aware pipelines and explicit cross-slot command helpers.
//!
//! [`ClusterPipeline`] preserves redis-tower's typed pipeline result API while
//! [`MultiplexedClusterClient`] groups its raw
//! frames by cluster node. The split helpers deliberately use explicit names:
//! splitting a Redis command across slots changes its atomicity contract and
//! must never happen invisibly inside ordinary command execution.

use std::collections::BTreeMap;

use bytes::Bytes;
use redis_tower::{Pipeline, PipelineResults};
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

use crate::MultiplexedClusterClient;
use crate::slot::slot_for_key;

/// A typed pipeline executed across a multiplexed Redis Cluster client.
///
/// Commands are pinned to their owning masters from one topology snapshot;
/// replica read preference is intentionally ignored. Commands targeting the
/// same master are sent in submission order in one node-local pipeline.
/// Different node pipelines run concurrently, so there is no total execution
/// order across hash slots, although results are always restored to the order
/// in which commands were pushed. The operation is not atomic across commands
/// or nodes.
///
/// A node-level transport error fails the whole call because Redis may have
/// applied an unknown prefix of that node's batch. Successful commands are
/// never replayed internally when a different entry returns MOVED or ASK.
/// Redirects are replayed only after the original node batches finish, so
/// during slot migration an earlier redirected command may execute after a
/// later same-slot command that succeeded on the original node.
/// Dropping or timing out the future after dispatch is likewise ambiguous:
/// some node batches may already have executed and no rollback is attempted.
pub struct ClusterPipeline {
    inner: Pipeline,
}

impl ClusterPipeline {
    /// Create an empty cluster pipeline.
    pub fn new() -> Self {
        Self {
            inner: Pipeline::new(),
        }
    }

    /// Append a typed command.
    ///
    /// A known command that itself names keys from multiple hash slots is
    /// rejected during pipeline preflight. Custom commands outside the
    /// maintained key-spec table retain the client's legacy first-argument
    /// routing behavior. Use [`MultiplexedClusterClient::mget_split`],
    /// [`MultiplexedClusterClient::mset_split`], or
    /// [`MultiplexedClusterClient::del_split`] when cross-slot behavior is
    /// intentional.
    pub fn push<Cmd: Command + 'static>(mut self, command: Cmd) -> Self {
        self.inner = self.inner.push(command);
        self
    }

    /// Execute the pipeline, consuming it and returning typed indexed results.
    pub async fn execute(
        self,
        client: &MultiplexedClusterClient,
    ) -> Result<PipelineResults, RedisError> {
        let mut executor = client.clone();
        self.inner.execute(&mut executor).await
    }

    /// Number of queued commands.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether no commands are queued.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl Default for ClusterPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiplexedClusterClient {
    /// Read keys from any number of hash slots.
    ///
    /// One MGET is sent per slot, slot groups run through a
    /// [`ClusterPipeline`], and values are restored to input order. Duplicate
    /// keys are preserved. Keys are copied as raw bytes, so embedded NULs and
    /// non-UTF-8 bytes are supported.
    ///
    /// This is not a consistent cross-slot snapshot: writes may occur between
    /// the independent node operations.
    pub async fn mget_split<I, K>(&self, keys: I) -> Result<Vec<Option<Bytes>>, RedisError>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        let keys = keys
            .into_iter()
            .map(|key| Bytes::copy_from_slice(key.as_ref()))
            .collect::<Vec<_>>();
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut by_slot: BTreeMap<u16, Vec<(usize, Bytes)>> = BTreeMap::new();
        for (index, key) in keys.iter().cloned().enumerate() {
            by_slot
                .entry(slot_for_key(&key))
                .or_default()
                .push((index, key));
        }

        let mut pipeline = ClusterPipeline::new();
        let mut input_indices = Vec::with_capacity(by_slot.len());
        for group in by_slot.into_values() {
            let (indices, keys): (Vec<_>, Vec<_>) = group.into_iter().unzip();
            input_indices.push(indices);
            pipeline = pipeline.push(BinaryMGet { keys });
        }

        let mut grouped = pipeline.execute(self).await?;
        let mut ordered = vec![None; keys.len()];
        for (result_index, indices) in input_indices.into_iter().enumerate() {
            let values = grouped.take::<Vec<Option<Bytes>>>(result_index)?;
            if values.len() != indices.len() {
                return Err(RedisError::UnexpectedResponse {
                    expected: "one MGET value per key",
                    actual: format!(
                        "received {} values for {} keys",
                        values.len(),
                        indices.len()
                    ),
                });
            }
            for (input_index, value) in indices.into_iter().zip(values) {
                ordered[input_index] = value;
            }
        }
        Ok(ordered)
    }

    /// Set binary key/value pairs that may span hash slots.
    ///
    /// One MSET is issued per slot. Each slot-local MSET is atomic, but the
    /// overall operation is **not atomic across slots**. If this method returns
    /// an error, some slot groups may already have been committed; no rollback
    /// is attempted. Use hash tags and ordinary `MSET` when all-or-nothing
    /// semantics are required.
    pub async fn mset_split<I, K, V>(&self, pairs: I) -> Result<(), RedisError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let mut by_slot: BTreeMap<u16, Vec<(Bytes, Bytes)>> = BTreeMap::new();
        for (key, value) in pairs {
            let key = Bytes::copy_from_slice(key.as_ref());
            let value = Bytes::copy_from_slice(value.as_ref());
            by_slot
                .entry(slot_for_key(&key))
                .or_default()
                .push((key, value));
        }
        if by_slot.is_empty() {
            return Ok(());
        }

        let group_count = by_slot.len();
        let mut pipeline = ClusterPipeline::new();
        for pairs in by_slot.into_values() {
            pipeline = pipeline.push(BinaryMSet { pairs });
        }
        let mut results = pipeline.execute(self).await?;
        for index in 0..group_count {
            results.take::<()>(index)?;
        }
        Ok(())
    }

    /// Delete binary keys that may span hash slots.
    ///
    /// One DEL is issued per slot and the deletion counts are summed. The
    /// operation is **not atomic across slots**. If an error is returned, some
    /// keys may already have been deleted and the final total is unknown; no
    /// rollback or replay is attempted.
    pub async fn del_split<I, K>(&self, keys: I) -> Result<i64, RedisError>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        let mut by_slot: BTreeMap<u16, Vec<Bytes>> = BTreeMap::new();
        for key in keys {
            let key = Bytes::copy_from_slice(key.as_ref());
            by_slot.entry(slot_for_key(&key)).or_default().push(key);
        }
        if by_slot.is_empty() {
            return Ok(0);
        }

        let group_count = by_slot.len();
        let mut pipeline = ClusterPipeline::new();
        for keys in by_slot.into_values() {
            pipeline = pipeline.push(BinaryDel { keys });
        }
        let mut results = pipeline.execute(self).await?;
        let mut deleted = 0i64;
        for index in 0..group_count {
            deleted = deleted.checked_add(results.take::<i64>(index)?).ok_or(
                RedisError::UnexpectedResponse {
                    expected: "DEL count representable as i64",
                    actual: "summed deletion count overflowed i64".to_string(),
                },
            )?;
        }
        Ok(deleted)
    }
}

struct BinaryMGet {
    keys: Vec<Bytes>,
}

impl Command for BinaryMGet {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        let mut args = Vec::with_capacity(self.keys.len() + 1);
        args.push(bulk("MGET"));
        args.extend(self.keys.iter().cloned().map(binary_bulk));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(values)) => values
                .into_iter()
                .map(|value| match value {
                    Frame::BulkString(value) => Ok(value),
                    Frame::Null => Ok(None),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string or null",
                        actual: format!("{other:?}"),
                    }),
                })
                .collect(),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "MGET"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

struct BinaryMSet {
    pairs: Vec<(Bytes, Bytes)>,
}

impl Command for BinaryMSet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = Vec::with_capacity(self.pairs.len() * 2 + 1);
        args.push(bulk("MSET"));
        for (key, value) in &self.pairs {
            args.push(binary_bulk(key.clone()));
            args.push(binary_bulk(value.clone()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(response) if response.as_ref() == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "MSET"
    }
}

struct BinaryDel {
    keys: Vec<Bytes>,
}

impl Command for BinaryDel {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = Vec::with_capacity(self.keys.len() + 1);
        args.push(bulk("DEL"));
        args.extend(self.keys.iter().cloned().map(binary_bulk));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(count) => Ok(count),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "DEL"
    }
}

fn binary_bulk(value: Bytes) -> Frame {
    Frame::BulkString(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cluster_pipeline_empty_and_len() {
        let pipeline = ClusterPipeline::new();
        assert!(pipeline.is_empty());
        assert_eq!(pipeline.len(), 0);

        let pipeline = pipeline.push(BinaryDel {
            keys: vec![Bytes::from_static(b"key")],
        });
        assert!(!pipeline.is_empty());
        assert_eq!(pipeline.len(), 1);
    }

    #[test]
    fn binary_split_commands_preserve_exact_bytes() {
        let key = Bytes::from_static(b"key:\0\xff");
        let value = Bytes::from_static(b"value:\0\x80\xff");

        assert_eq!(
            BinaryMGet {
                keys: vec![key.clone()]
            }
            .to_frame(),
            array(vec![bulk("MGET"), binary_bulk(key.clone())])
        );
        assert_eq!(
            BinaryMSet {
                pairs: vec![(key.clone(), value.clone())]
            }
            .to_frame(),
            array(vec![
                bulk("MSET"),
                binary_bulk(key.clone()),
                binary_bulk(value)
            ])
        );
        assert_eq!(
            BinaryDel {
                keys: vec![key.clone()]
            }
            .to_frame(),
            array(vec![bulk("DEL"), binary_bulk(key)])
        );
    }

    #[test]
    fn binary_mget_parses_missing_values_in_position() {
        let command = BinaryMGet {
            keys: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
        };
        let values = command
            .parse_response(Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from_static(b"value"))),
                Frame::Null,
            ])))
            .unwrap();
        assert_eq!(values, vec![Some(Bytes::from_static(b"value")), None]);
    }

    #[test]
    fn binary_write_commands_parse_expected_responses() {
        let mset = BinaryMSet {
            pairs: vec![(Bytes::from_static(b"a"), Bytes::from_static(b"1"))],
        };
        mset.parse_response(Frame::SimpleString(Bytes::from_static(b"OK")))
            .unwrap();

        let del = BinaryDel {
            keys: vec![Bytes::from_static(b"a")],
        };
        assert_eq!(del.parse_response(Frame::Integer(3)).unwrap(), 3);
    }
}
