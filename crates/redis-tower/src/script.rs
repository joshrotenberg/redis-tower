//! Script helper with SHA1 caching for Lua scripting.
//!
//! [`Script`] pre-computes the SHA1 digest of a Lua script at construction
//! time and provides convenience methods to build [`Eval`] and [`EvalSha`]
//! commands. The [`Script::execute`] method tries EVALSHA first and
//! transparently falls back to EVAL when the server returns NOSCRIPT.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::Script;
//!
//! let script = Script::new("return redis.call('GET', KEYS[1])");
//!
//! // Preferred: EVALSHA with automatic NOSCRIPT fallback.
//! let result = script.execute(&mut conn, &["mykey"], &[]).await?;
//! ```

use redis_tower_commands::{Eval, EvalSha};
use redis_tower_core::{Frame, RedisConnection, RedisError};
use sha1::{Digest, Sha1};

/// A Lua script with a pre-computed SHA1 digest.
///
/// `Script` is the recommended way to run Lua scripts against Redis. It caches
/// the SHA1 hash so that repeated executions can use EVALSHA (which avoids
/// sending the full script text over the wire). When the server has not yet
/// seen the script, [`execute`](Script::execute) transparently falls back to
/// EVAL.
pub struct Script {
    source: String,
    sha: String,
}

impl Script {
    /// Create a new `Script` from Lua source code.
    ///
    /// The SHA1 digest is computed immediately and cached for the lifetime of
    /// the value.
    pub fn new(source: impl Into<String>) -> Self {
        let source = source.into();
        let sha = hex::encode(Sha1::digest(source.as_bytes()));
        Self { source, sha }
    }

    /// Get the SHA1 hex digest of the script.
    pub fn sha(&self) -> &str {
        &self.sha
    }

    /// Get the Lua source code.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Build an [`EvalSha`] command (preferred -- avoids sending the script text).
    pub fn evalsha(&self, keys: &[&str], args: &[&str]) -> EvalSha {
        let mut cmd = EvalSha::new(&self.sha);
        for k in keys {
            cmd = cmd.key(*k);
        }
        for a in args {
            cmd = cmd.arg(*a);
        }
        cmd
    }

    /// Build an [`Eval`] command (fallback when EVALSHA returns NOSCRIPT).
    pub fn eval(&self, keys: &[&str], args: &[&str]) -> Eval {
        let mut cmd = Eval::new(&self.source);
        for k in keys {
            cmd = cmd.key(*k);
        }
        for a in args {
            cmd = cmd.arg(*a);
        }
        cmd
    }

    /// Execute the script, trying EVALSHA first and falling back to EVAL on
    /// NOSCRIPT.
    ///
    /// This is the recommended entry point for running scripts. On the first
    /// call Redis may not have the script cached, so it will return a NOSCRIPT
    /// error. This method catches that error and retries with the full EVAL
    /// command, which implicitly caches the script for subsequent EVALSHA
    /// calls.
    pub async fn execute(
        &self,
        conn: &mut RedisConnection,
        keys: &[&str],
        args: &[&str],
    ) -> Result<Frame, RedisError> {
        match conn.execute(self.evalsha(keys, args)).await {
            Err(RedisError::Redis(ref msg)) if msg.starts_with("NOSCRIPT") => {
                conn.execute(self.eval(keys, args)).await
            }
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
        // Computed with: echo -n "return 1" | sha1sum
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
        assert!(frame_str.contains(&script.sha));
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
    fn empty_keys_and_args() {
        let script = Script::new("return 42");
        let cmd = script.evalsha(&[], &[]);
        let frame = cmd.to_frame();
        let frame_str = format!("{frame:?}");
        // numkeys should be "0"
        assert!(frame_str.contains("EVALSHA"));
    }
}
