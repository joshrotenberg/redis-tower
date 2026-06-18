//! Script helper with SHA1 caching and automatic NOSCRIPT fallback.
//!
//! [`Script`] wraps [`redis_tower_commands::Script`] -- which owns the SHA1
//! caching and command building -- and adds an executor-bound
//! [`execute`](Script::execute) method that tries EVALSHA first and
//! transparently falls back to EVAL when the server returns NOSCRIPT. Because it
//! is generic over [`RedisExecutor`], the fallback works against every client
//! type (connection, multiplexed, pooled, resilient, cluster, ...).
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::Script;
//!
//! let script = Script::new("return redis.call('GET', KEYS[1])");
//!
//! // Preferred: EVALSHA with automatic NOSCRIPT fallback to EVAL.
//! let result = script.execute(&mut conn, &["mykey"], &[]).await?;
//! ```

use redis_tower_commands::{Eval, EvalRo, EvalSha, EvalShaRo};
use redis_tower_core::{Frame, RedisError};

use crate::executor::RedisExecutor;

/// A Lua script with a pre-computed SHA1 digest and NOSCRIPT fallback.
///
/// `Script` is the recommended way to run Lua scripts against Redis. It caches
/// the SHA1 hash so repeated executions use EVALSHA (avoiding sending the full
/// script text over the wire). When the server has not yet seen the script,
/// [`execute`](Script::execute) transparently falls back to EVAL, which causes
/// the server to cache the script for subsequent EVALSHA calls.
///
/// The caching and command-building primitives live in
/// [`redis_tower_commands::Script`]; this type adds the executor-bound
/// execute-with-fallback convenience.
#[derive(Clone, Debug)]
pub struct Script {
    inner: redis_tower_commands::Script,
}

impl Script {
    /// Create a new `Script` from Lua source code.
    ///
    /// The SHA1 digest is computed immediately and cached for the lifetime of
    /// the value.
    pub fn new(source: impl Into<String>) -> Self {
        Self {
            inner: redis_tower_commands::Script::new(source),
        }
    }

    /// Get the SHA1 hex digest of the script.
    pub fn sha(&self) -> &str {
        self.inner.sha()
    }

    /// Get the Lua source code.
    pub fn source(&self) -> &str {
        self.inner.source()
    }

    /// Build an [`EvalSha`] command (preferred -- avoids sending the script text).
    pub fn evalsha(&self, keys: &[&str], args: &[&str]) -> EvalSha {
        self.inner.evalsha(keys, args)
    }

    /// Build an [`EvalShaRo`] command, the read-only variant of
    /// [`evalsha`](Script::evalsha).
    pub fn evalsha_ro(&self, keys: &[&str], args: &[&str]) -> EvalShaRo {
        self.inner.evalsha_ro(keys, args)
    }

    /// Build an [`Eval`] command (fallback when EVALSHA returns NOSCRIPT).
    pub fn eval(&self, keys: &[&str], args: &[&str]) -> Eval {
        self.inner.eval(keys, args)
    }

    /// Build an [`EvalRo`] command, the read-only variant of
    /// [`eval`](Script::eval).
    pub fn eval_ro(&self, keys: &[&str], args: &[&str]) -> EvalRo {
        self.inner.eval_ro(keys, args)
    }

    /// Execute the script, trying EVALSHA first and falling back to EVAL on
    /// NOSCRIPT.
    ///
    /// This is the recommended entry point for running scripts. On the first
    /// call Redis may not have the script cached, so it returns a NOSCRIPT
    /// error. This method catches that error and retries with the full EVAL
    /// command, which implicitly caches the script for subsequent EVALSHA
    /// calls. It is generic over [`RedisExecutor`], so it works with any client
    /// type.
    pub async fn execute<E: RedisExecutor>(
        &self,
        executor: &mut E,
        keys: &[&str],
        args: &[&str],
    ) -> Result<Frame, RedisError> {
        match executor.execute(self.evalsha(keys, args)).await {
            Err(e) if e.is_noscript() => executor.execute(self.eval(keys, args)).await,
            other => other,
        }
    }

    /// Execute the script read-only, trying EVALSHA_RO first and falling back to
    /// EVAL_RO on NOSCRIPT.
    ///
    /// The read-only counterpart of [`execute`](Script::execute): the server
    /// rejects any write commands the script attempts, and the call can be
    /// routed to replicas.
    pub async fn execute_ro<E: RedisExecutor>(
        &self,
        executor: &mut E,
        keys: &[&str],
        args: &[&str],
    ) -> Result<Frame, RedisError> {
        match executor.execute(self.evalsha_ro(keys, args)).await {
            Err(e) if e.is_noscript() => executor.execute(self.eval_ro(keys, args)).await,
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;

    #[test]
    fn sha_matches_known_digest() {
        // "return 1" has a well-known SHA1 digest.
        let script = Script::new("return 1");
        // Computed with: printf 'return 1' | sha1sum
        assert_eq!(script.sha(), "e0e1f9fabfc9d4800c877a703b823ac0578ff8db");
    }

    #[test]
    fn source_is_preserved() {
        let script = Script::new("return redis.call('GET', KEYS[1])");
        assert_eq!(script.source(), "return redis.call('GET', KEYS[1])");
    }

    #[test]
    fn evalsha_builds_correct_command() {
        let script = Script::new("return 1");
        let cmd = script.evalsha(&["key1", "key2"], &["arg1"]);
        assert_eq!(cmd.name(), "EVALSHA");

        // Verify the frame contains the SHA, not the source.
        let frame = cmd.to_frame();
        let frame_str = format!("{frame:?}");
        assert!(frame_str.contains(script.sha()));
        assert!(!frame_str.contains("return 1"));
    }

    #[test]
    fn eval_builds_correct_command() {
        let script = Script::new("return 1");
        let cmd = script.eval(&["key1"], &["arg1", "arg2"]);
        assert_eq!(cmd.name(), "EVAL");

        // Verify the frame contains the source, not the SHA.
        let frame = cmd.to_frame();
        let frame_str = format!("{frame:?}");
        assert!(frame_str.contains("return 1"));
    }

    #[test]
    fn ro_variants_build_ro_commands() {
        let script = Script::new("return 1");
        assert_eq!(script.evalsha_ro(&[], &[]).name(), "EVALSHA_RO");
        assert_eq!(script.eval_ro(&[], &[]).name(), "EVAL_RO");
    }

    #[test]
    fn empty_keys_and_args() {
        let script = Script::new("return 42");
        let cmd = script.evalsha(&[], &[]);
        let frame = cmd.to_frame();
        let frame_str = format!("{frame:?}");
        // numkeys should be "0"
        assert!(frame_str.contains("EVALSHA"));
    }
}
