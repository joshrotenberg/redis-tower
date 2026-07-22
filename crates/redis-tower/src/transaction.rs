//! Redis transactions (MULTI/EXEC) with optional WATCH support.
//!
//! [`Transaction`] builds a sequence of commands that are executed atomically
//! via MULTI/EXEC. For optimistic locking, use [`Transaction::watch`] to
//! observe keys before the transaction; if any watched key is modified by
//! another client, the transaction is aborted and
//! [`TransactionResult::Aborted`] is returned.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower::{Transaction, RedisConnection, commands::*};
//!
//! let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let result = Transaction::new()
//!     .push(Set::new("x", "1"))
//!     .push(Incr::new("x"))
//!     .execute(&mut conn)
//!     .await?;
//! # let _ = result;
//! # Ok(())
//! # }
//! ```

use std::any::Any;
use std::future::Future;
use std::sync::Arc;

use redis_tower_commands::Watch;
use redis_tower_core::{Command, Frame, RedisConnection, RedisError};
use redis_tower_protocol::helpers::{array, bulk};
use tokio::sync::Mutex;

use crate::client::RedisClient;
use crate::executor::RedisExecutor;
use crate::pipeline::{PipelineResults, ResponseParser};

/// A connection type that can execute a WATCH/MULTI/EXEC transaction.
///
/// Implemented by [`RedisConnection`]. The `Arc<Mutex<C>>` blanket impl
/// and the [`RedisClient`] impl allow shared clients to be passed directly
/// to [`Transaction::execute`].
///
/// ## Exclusive access
///
/// Transactions require exclusive access to a single connection for the
/// duration of WATCH/MULTI/EXEC. The `Arc<Mutex<C>>` blanket impl satisfies
/// this automatically: the mutex is locked for the entire transaction call,
/// preventing any other caller from interleaving commands.
pub trait TransactionExecutor {
    /// Execute a WATCH/MULTI/EXEC transaction.
    ///
    /// `watch_frames` is the serialized WATCH command (empty if no WATCH keys).
    /// `command_frames` are the commands to queue between MULTI and EXEC.
    ///
    /// Returns `Ok(Some(responses))` if the transaction committed,
    /// `Ok(None)` if it was aborted by a WATCH violation.
    fn execute_transaction(
        &mut self,
        watch_frames: Vec<Frame>,
        command_frames: Vec<Frame>,
    ) -> impl Future<Output = Result<Option<Vec<Frame>>, RedisError>> + Send;
}

impl TransactionExecutor for RedisConnection {
    fn execute_transaction(
        &mut self,
        watch_frames: Vec<Frame>,
        command_frames: Vec<Frame>,
    ) -> impl Future<Output = Result<Option<Vec<Frame>>, RedisError>> + Send {
        RedisConnection::execute_transaction(self, watch_frames, command_frames)
    }
}

impl<C: TransactionExecutor + Send> TransactionExecutor for Arc<Mutex<C>> {
    fn execute_transaction(
        &mut self,
        watch_frames: Vec<Frame>,
        command_frames: Vec<Frame>,
    ) -> impl Future<Output = Result<Option<Vec<Frame>>, RedisError>> + Send {
        let arc = Arc::clone(self);
        async move {
            arc.lock()
                .await
                .execute_transaction(watch_frames, command_frames)
                .await
        }
    }
}

impl TransactionExecutor for RedisClient {
    fn execute_transaction(
        &mut self,
        watch_frames: Vec<Frame>,
        command_frames: Vec<Frame>,
    ) -> impl Future<Output = Result<Option<Vec<Frame>>, RedisError>> + Send {
        let arc = Arc::clone(&self.inner);
        async move {
            arc.lock()
                .await
                .execute_transaction(watch_frames, command_frames)
                .await
        }
    }
}

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
/// Accepts any type that implements [`TransactionExecutor`], including
/// [`RedisConnection`], [`RedisClient`], and `Arc<Mutex<C>>` for any
/// `C: TransactionExecutor + Send`.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower::{Transaction, TransactionResult, RedisConnection};
/// use redis_tower::commands::*;
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
/// let result = Transaction::new()
///     .push(Set::new("x", "1"))
///     .push(Incr::new("x"))
///     .execute(&mut conn)
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
/// # Ok(())
/// # }
/// ```
pub struct Transaction {
    watch_keys: Vec<String>,
    entries: Vec<TransactionEntry>,
}

/// The outcome of a transaction execution.
#[must_use = "transaction result must be handled"]
pub enum TransactionResult {
    /// Transaction committed successfully. Results can be extracted by index.
    Committed(PipelineResults),
    /// Transaction was aborted because a WATCHed key was modified.
    Aborted,
}

impl TransactionResult {
    /// Returns `Some(results)` if the transaction committed, `None` if aborted.
    pub fn committed(self) -> Option<PipelineResults> {
        match self {
            TransactionResult::Committed(results) => Some(results),
            TransactionResult::Aborted => None,
        }
    }

    /// Returns `true` if the transaction was aborted due to a WATCH violation.
    pub fn is_aborted(&self) -> bool {
        matches!(self, TransactionResult::Aborted)
    }

    /// Returns `true` if the transaction committed successfully.
    pub fn is_committed(&self) -> bool {
        matches!(self, TransactionResult::Committed(_))
    }

    /// Unwraps the committed results, panicking if the transaction was aborted.
    ///
    /// # Panics
    ///
    /// Panics if the transaction was aborted. Use this only in tests or when
    /// the transaction cannot possibly be aborted (no WATCH keys).
    #[track_caller]
    pub fn unwrap(self) -> PipelineResults {
        match self {
            TransactionResult::Committed(results) => results,
            TransactionResult::Aborted => {
                panic!("called `TransactionResult::unwrap()` on an `Aborted` value")
            }
        }
    }
}

impl Transaction {
    /// Create a new empty transaction.
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
    /// Accepts any [`TransactionExecutor`]: [`RedisConnection`], [`RedisClient`],
    /// `Arc<Mutex<RedisConnection>>`, etc.
    ///
    /// Sends WATCH (if any), MULTI, all queued commands, and EXEC
    /// atomically under a single connection lock. For `Arc<Mutex<C>>`-based
    /// executors, the mutex is held for the entire WATCH/MULTI/EXEC sequence,
    /// preventing any interleaving with other callers.
    pub async fn execute<E: TransactionExecutor>(
        self,
        conn: &mut E,
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

/// Default number of WATCH/EXEC attempts used by [`transaction`] before it
/// gives up and returns [`RedisError::TransactionAborted`].
pub const DEFAULT_TRANSACTION_RETRIES: usize = 16;

/// Run a WATCH-protected transaction, retrying on optimistic-lock conflicts.
///
/// This is the ergonomic port of redis-rs's `transaction`/`transaction_async`
/// helpers. It implements the standard optimistic-locking loop:
///
/// 1. `WATCH` the given keys.
/// 2. Call `build`, which may read the watched keys on `conn` and returns the
///    [`Transaction`] body to commit (the commands to run between MULTI/EXEC).
/// 3. Run MULTI/EXEC. If a watched key was modified by another client, EXEC is
///    aborted and the whole sequence is retried from step 1.
///
/// The loop runs until EXEC commits or the retry cap is reached. On a commit it
/// returns the [`PipelineResults`]; if the cap is exhausted it returns
/// [`RedisError::TransactionAborted`].
///
/// Because the reads in `build` must happen *inside* the WATCH window, `conn`
/// must be an exclusively held connection for the duration of the call -- pass
/// a [`RedisConnection`] (`&mut`) or otherwise ensure no other task issues
/// commands on the same connection while the transaction is in flight. (This is
/// the same requirement redis-rs places on its `transaction` helper.) The
/// returned `Transaction` should carry the commands only; the WATCH is issued
/// by this helper, so do not also call [`Transaction::watch`] on it.
///
/// Uses [`DEFAULT_TRANSACTION_RETRIES`] as the cap. Use
/// [`transaction_with_retries`] to choose a different cap.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower::{transaction, Transaction, RedisConnection};
/// use redis_tower::commands::{Get, Set};
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// // Atomically double the value at `counter`, retrying if another client
/// // touches it between the read and the EXEC.
/// let results = transaction(&mut conn, ["counter"], async |c| {
///     let current: i64 = match c.execute(Get::new("counter")).await? {
///         Some(bytes) => String::from_utf8_lossy(&bytes).parse().unwrap_or(0),
///         None => 0,
///     };
///     Ok(Transaction::new().push(Set::new("counter", (current * 2).to_string())))
/// })
/// .await?;
/// # let _ = results;
/// # Ok(())
/// # }
/// ```
pub async fn transaction<C, F>(
    conn: &mut C,
    keys: impl IntoIterator<Item = impl Into<String>>,
    build: F,
) -> Result<PipelineResults, RedisError>
where
    C: RedisExecutor + TransactionExecutor + Send,
    F: AsyncFnMut(&mut C) -> Result<Transaction, RedisError>,
{
    transaction_with_retries(conn, keys, DEFAULT_TRANSACTION_RETRIES, build).await
}

/// Like [`transaction`], but with an explicit retry cap.
///
/// `max_retries` is the number of *additional* attempts made after the first,
/// so the closure is invoked at most `max_retries + 1` times. A cap of `0`
/// makes a single attempt and surfaces a WATCH abort immediately as
/// [`RedisError::TransactionAborted`].
///
/// See [`transaction`] for the full contract.
pub async fn transaction_with_retries<C, F>(
    conn: &mut C,
    keys: impl IntoIterator<Item = impl Into<String>>,
    max_retries: usize,
    mut build: F,
) -> Result<PipelineResults, RedisError>
where
    C: RedisExecutor + TransactionExecutor + Send,
    F: AsyncFnMut(&mut C) -> Result<Transaction, RedisError>,
{
    let keys: Vec<String> = keys.into_iter().map(Into::into).collect();
    let mut remaining = max_retries;

    loop {
        // WATCH the keys so the reads in `build` are covered by the optimistic
        // lock. Skip the round-trip entirely when there is nothing to watch.
        if !keys.is_empty() {
            conn.execute(Watch::keys(keys.clone())).await?;
        }

        // Build the atomic body. The closure may read the watched keys here.
        let txn = build(conn).await?;

        // Run MULTI/EXEC. The WATCH issued above is still active on the
        // connection, so the body carries no watch keys of its own.
        match txn.execute(&mut *conn).await? {
            TransactionResult::Committed(results) => return Ok(results),
            TransactionResult::Aborted => {
                // A watched key changed. EXEC already cleared the WATCH state,
                // so the next iteration re-watches from scratch.
                if remaining == 0 {
                    return Err(RedisError::TransactionAborted);
                }
                remaining -= 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn committed() -> TransactionResult {
        TransactionResult::Committed(PipelineResults::from_raw(Vec::new()))
    }

    #[test]
    fn committed_returns_some_for_committed() {
        assert!(committed().committed().is_some());
    }

    #[test]
    fn committed_returns_none_for_aborted() {
        assert!(TransactionResult::Aborted.committed().is_none());
    }

    #[test]
    fn is_aborted_reports_correctly() {
        assert!(TransactionResult::Aborted.is_aborted());
        assert!(!committed().is_aborted());
    }

    #[test]
    fn is_committed_reports_correctly() {
        assert!(committed().is_committed());
        assert!(!TransactionResult::Aborted.is_committed());
    }

    #[test]
    fn unwrap_succeeds_on_committed() {
        let results = committed().unwrap();
        assert!(results.is_empty());
    }

    #[test]
    #[should_panic(expected = "Aborted")]
    fn unwrap_panics_on_aborted() {
        let _ = TransactionResult::Aborted.unwrap();
    }

    /// A mock transaction executor for unit tests.
    struct MockTransactionConn {
        /// Simulates committed responses (one per command frame).
        responses: Option<Vec<Frame>>,
    }

    impl TransactionExecutor for MockTransactionConn {
        fn execute_transaction(
            &mut self,
            _watch_frames: Vec<Frame>,
            command_frames: Vec<Frame>,
        ) -> impl Future<Output = Result<Option<Vec<Frame>>, RedisError>> + Send {
            let responses = self.responses.take().map(|_| {
                // Return one SimpleString "OK" per queued command.
                command_frames
                    .iter()
                    .map(|_| Frame::SimpleString(bytes::Bytes::from("OK")))
                    .collect::<Vec<_>>()
            });
            async move { Ok(responses) }
        }
    }

    #[tokio::test]
    async fn transaction_execute_accepts_transaction_executor() {
        use redis_tower_commands::Set;

        let mut conn = MockTransactionConn {
            responses: Some(vec![]),
        };
        let result = Transaction::new()
            .push(Set::new("x", "1"))
            .execute(&mut conn)
            .await
            .unwrap();
        assert!(result.is_committed());
    }

    #[tokio::test]
    async fn transaction_execute_accepts_arc_mutex_executor() {
        use redis_tower_commands::Set;

        let conn = Arc::new(Mutex::new(MockTransactionConn {
            responses: Some(vec![]),
        }));
        let mut executor = Arc::clone(&conn);
        let result = Transaction::new()
            .push(Set::new("x", "1"))
            .execute(&mut executor)
            .await
            .unwrap();
        assert!(result.is_committed());
    }

    /// A mock that also implements [`RedisExecutor`] (for the WATCH/read path)
    /// and can be scripted to abort a fixed number of EXECs before committing,
    /// so the [`transaction`] retry loop can be exercised without a server.
    struct RetryMockConn {
        /// Number of EXECs to abort (WATCH violation) before the next commits.
        aborts_before_commit: usize,
        /// How many `execute_transaction` calls were made.
        exec_calls: usize,
        /// How many plain commands (i.e. WATCH) were issued via `execute`.
        watch_calls: usize,
    }

    impl RedisExecutor for RetryMockConn {
        fn execute<Cmd: Command>(
            &mut self,
            cmd: Cmd,
        ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
            self.watch_calls += 1;
            // Only WATCH is exercised in these tests; reply OK to it.
            let result = cmd.parse_response(Frame::SimpleString(bytes::Bytes::from("OK")));
            async move { result }
        }
    }

    impl TransactionExecutor for RetryMockConn {
        fn execute_transaction(
            &mut self,
            _watch_frames: Vec<Frame>,
            command_frames: Vec<Frame>,
        ) -> impl Future<Output = Result<Option<Vec<Frame>>, RedisError>> + Send {
            self.exec_calls += 1;
            let abort = self.aborts_before_commit > 0;
            if abort {
                self.aborts_before_commit -= 1;
            }
            let responses = if abort {
                // EXEC aborted by a WATCH violation.
                None
            } else {
                Some(
                    command_frames
                        .iter()
                        .map(|_| Frame::SimpleString(bytes::Bytes::from("OK")))
                        .collect::<Vec<_>>(),
                )
            };
            async move { Ok(responses) }
        }
    }

    #[tokio::test]
    async fn transaction_commits_on_first_attempt() {
        use redis_tower_commands::Set;

        let mut conn = RetryMockConn {
            aborts_before_commit: 0,
            exec_calls: 0,
            watch_calls: 0,
        };
        let results = transaction(&mut conn, ["counter"], async |_c| {
            Ok(Transaction::new().push(Set::new("counter", "1")))
        })
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(conn.exec_calls, 1, "should EXEC exactly once");
        assert_eq!(conn.watch_calls, 1, "should WATCH exactly once");
    }

    #[tokio::test]
    async fn transaction_retries_until_commit() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use redis_tower_commands::Set;

        let mut conn = RetryMockConn {
            aborts_before_commit: 2,
            exec_calls: 0,
            watch_calls: 0,
        };
        let build_calls = Arc::new(AtomicUsize::new(0));
        let bc = Arc::clone(&build_calls);

        let results = transaction(&mut conn, ["k"], async move |_c: &mut RetryMockConn| {
            bc.fetch_add(1, Ordering::SeqCst);
            Ok(Transaction::new().push(Set::new("k", "1")))
        })
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        // Two aborts then a commit -> three attempts, each re-WATCHing.
        assert_eq!(build_calls.load(Ordering::SeqCst), 3);
        assert_eq!(conn.exec_calls, 3);
        assert_eq!(conn.watch_calls, 3);
    }

    #[tokio::test]
    async fn transaction_gives_up_after_retry_cap() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use redis_tower_commands::Set;

        let mut conn = RetryMockConn {
            // Always aborts -- the cap, not a commit, must terminate the loop.
            aborts_before_commit: usize::MAX,
            exec_calls: 0,
            watch_calls: 0,
        };
        let build_calls = Arc::new(AtomicUsize::new(0));
        let bc = Arc::clone(&build_calls);

        let result =
            transaction_with_retries(&mut conn, ["k"], 2, async move |_c: &mut RetryMockConn| {
                bc.fetch_add(1, Ordering::SeqCst);
                Ok(Transaction::new().push(Set::new("k", "1")))
            })
            .await;

        assert!(matches!(result, Err(RedisError::TransactionAborted)));
        // First attempt + 2 retries = 3 invocations.
        assert_eq!(build_calls.load(Ordering::SeqCst), 3);
        assert_eq!(conn.exec_calls, 3);
    }

    #[tokio::test]
    async fn transaction_without_keys_skips_watch() {
        use redis_tower_commands::Set;

        let mut conn = RetryMockConn {
            aborts_before_commit: 0,
            exec_calls: 0,
            watch_calls: 0,
        };
        let empty: [&str; 0] = [];
        let results = transaction(&mut conn, empty, async |_c| {
            Ok(Transaction::new().push(Set::new("k", "1")))
        })
        .await
        .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(conn.exec_calls, 1);
        assert_eq!(conn.watch_calls, 0, "no keys means no WATCH round-trip");
    }

    #[tokio::test]
    async fn transaction_zero_retries_surfaces_abort_immediately() {
        use redis_tower_commands::Set;

        let mut conn = RetryMockConn {
            aborts_before_commit: usize::MAX,
            exec_calls: 0,
            watch_calls: 0,
        };
        let result = transaction_with_retries(&mut conn, ["k"], 0, async |_c| {
            Ok(Transaction::new().push(Set::new("k", "1")))
        })
        .await;

        assert!(matches!(result, Err(RedisError::TransactionAborted)));
        assert_eq!(conn.exec_calls, 1, "cap of 0 makes a single attempt");
    }
}
