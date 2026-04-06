use std::any::Any;

use redis_tower_core::{Command, Frame, RedisConnection, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

use crate::pipeline::{PipelineResults, ResponseParser};

/// A type-erased command entry for transactions.
struct TransactionEntry {
    frame: Frame,
    parser: ResponseParser,
}

/// Builds and executes a Redis transaction (MULTI/EXEC).
///
/// Supports optional WATCH keys for optimistic locking. If a watched key
/// is modified by another client before EXEC, the transaction is aborted
/// and [`TransactionResult::Aborted`] is returned.
///
/// # Example
///
/// ```ignore
/// use redis_tower::{Transaction, RedisConnection};
/// use redis_tower::commands::*;
///
/// let conn = RedisConnection::connect("127.0.0.1:6379").await?;
/// let result = Transaction::new()
///     .push(Set::new("x", "1"))
///     .push(Incr::new("x"))
///     .execute(&conn)
///     .await?;
///
/// match result {
///     TransactionResult::Committed(results) => {
///         let val: &i64 = results.get(1)?;
///         assert_eq!(*val, 2);
///     }
///     TransactionResult::Aborted => {
///         // WATCH key was modified
///     }
/// }
/// ```
pub struct Transaction {
    watch_keys: Vec<String>,
    entries: Vec<TransactionEntry>,
}

/// The outcome of a transaction execution.
pub enum TransactionResult {
    /// Transaction committed successfully. Results can be extracted by index.
    Committed(PipelineResults),
    /// Transaction was aborted because a WATCHed key was modified.
    Aborted,
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            watch_keys: Vec::new(),
            entries: Vec::new(),
        }
    }

    /// Watch keys for optimistic locking.
    ///
    /// If any watched key is modified by another client between the WATCH
    /// and the EXEC, the transaction will be aborted.
    pub fn watch(mut self, keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.watch_keys.extend(keys.into_iter().map(Into::into));
        self
    }

    /// Add a command to the transaction. Returns `self` for chaining.
    pub fn push<Cmd: Command + 'static>(mut self, cmd: Cmd) -> Self {
        let frame = cmd.to_frame();
        let parser = Box::new(
            move |response: Frame| -> Result<Box<dyn Any + Send>, RedisError> {
                let result = cmd.parse_response(response)?;
                Ok(Box::new(result))
            },
        );
        self.entries.push(TransactionEntry { frame, parser });
        self
    }

    /// Execute the transaction.
    ///
    /// Sends WATCH (if any), MULTI, all queued commands, and EXEC
    /// atomically under a single connection lock.
    pub async fn execute(
        self,
        conn: &mut RedisConnection,
    ) -> Result<TransactionResult, RedisError> {
        // Build WATCH frames.
        let watch_frames: Vec<Frame> = if self.watch_keys.is_empty() {
            Vec::new()
        } else {
            let mut args = vec![bulk("WATCH")];
            for key in &self.watch_keys {
                args.push(bulk(key.as_str()));
            }
            vec![array(args)]
        };

        let command_frames: Vec<Frame> = self.entries.iter().map(|e| e.frame.clone()).collect();

        let exec_result = conn
            .execute_transaction(watch_frames, command_frames)
            .await?;

        match exec_result {
            None => Ok(TransactionResult::Aborted),
            Some(responses) => {
                let mut results = Vec::with_capacity(responses.len());
                for (entry, response) in self.entries.into_iter().zip(responses) {
                    if let Frame::Error(ref e) = response {
                        results.push(Err(RedisError::Redis(
                            String::from_utf8_lossy(e).into_owned(),
                        )));
                    } else {
                        results.push((entry.parser)(response));
                    }
                }
                Ok(TransactionResult::Committed(PipelineResults::from_raw(
                    results,
                )))
            }
        }
    }

    /// Returns the number of queued commands.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if no commands are queued.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}
