//! The [`RedisExecutor`] trait for executing Redis commands.
//!
//! All redis-tower client types implement this trait. Application code
//! can depend on `impl RedisExecutor` rather than concrete client types,
//! making it straightforward to substitute a mock in tests.

use std::future::Future;
use std::sync::Arc;

use redis_tower_core::{Command, RedisConnection, RedisError};
use tokio::sync::Mutex;

use crate::caching::CachedClient;
use crate::client::RedisClient;
use crate::multiplexed::MultiplexedClient;
use crate::resilient::ResilientRedisClient;

/// Trait for executing Redis commands, enabling test mocking.
///
/// All redis-tower client types implement this trait. Application code
/// can depend on `impl RedisExecutor` rather than concrete client types,
/// making it easy to substitute a mock in tests.
///
/// # Example
///
/// ```ignore
/// async fn increment(redis: &mut impl RedisExecutor, key: &str) -> Result<i64, RedisError> {
///     redis.execute(Incr::new(key)).await
/// }
/// ```
pub trait RedisExecutor {
    /// Execute a Redis command and return its typed response.
    fn execute<Cmd: Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send;
}

impl RedisExecutor for RedisConnection {
    fn execute<Cmd: Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
        RedisConnection::execute(self, cmd)
    }
}

impl RedisExecutor for RedisClient {
    fn execute<Cmd: Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
        RedisClient::execute(self, cmd)
    }
}

impl RedisExecutor for ResilientRedisClient {
    fn execute<Cmd: Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
        ResilientRedisClient::execute(self, cmd)
    }
}

impl RedisExecutor for CachedClient {
    fn execute<Cmd: Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
        CachedClient::execute(self, cmd)
    }
}

/// Blanket impl: any `Arc<Mutex<C>>` where `C: RedisExecutor + Send` is also
/// a `RedisExecutor`. This makes `RedisClient` (which wraps `Arc<Mutex<RedisConnection>>`)
/// and any user-defined Arc-wrapped executor automatically composable with
/// generic API wrappers like [`Json`](crate::json_api::Json).
impl<C: RedisExecutor + Send> RedisExecutor for Arc<Mutex<C>> {
    fn execute<Cmd: Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
        let arc = Arc::clone(self);
        async move { arc.lock().await.execute(cmd).await }
    }
}

/// Blanket impl: `&mut C` implements `RedisExecutor` when `C: RedisExecutor`.
///
/// This allows passing `&mut conn` or `&mut client` to APIs that accept
/// `impl RedisExecutor` by value, e.g. `Json::new(&mut conn)`.
impl<C: RedisExecutor + Send + ?Sized> RedisExecutor for &mut C {
    fn execute<Cmd: Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
        (**self).execute(cmd)
    }
}

/// `MultiplexedClient` implements `RedisExecutor`. Internally its `execute`
/// method takes `&self` (channel send -- no real mutation), but the trait's
/// `&mut self` contract is satisfied trivially.
impl RedisExecutor for MultiplexedClient {
    fn execute<Cmd: Command>(
        &mut self,
        cmd: Cmd,
    ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
        MultiplexedClient::execute(self, cmd)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Frame;
    use std::collections::VecDeque;

    /// A mock Redis executor that returns pre-configured frames.
    struct MockRedis {
        responses: VecDeque<Frame>,
    }

    impl MockRedis {
        fn new(responses: Vec<Frame>) -> Self {
            Self {
                responses: VecDeque::from(responses),
            }
        }
    }

    impl RedisExecutor for MockRedis {
        fn execute<Cmd: Command>(
            &mut self,
            cmd: Cmd,
        ) -> impl Future<Output = Result<Cmd::Response, RedisError>> + Send {
            let frame = self.responses.pop_front().unwrap_or(Frame::Null);
            async move { cmd.parse_response(frame) }
        }
    }

    #[tokio::test]
    async fn mock_executor_returns_response() {
        use bytes::Bytes;
        use redis_tower_commands::Get;

        let mut mock = MockRedis::new(vec![Frame::BulkString(Some(Bytes::from("hello")))]);
        let result: Option<Bytes> = mock.execute(Get::new("key")).await.unwrap();
        assert_eq!(result, Some(Bytes::from("hello")));
    }

    #[tokio::test]
    async fn arc_mutex_implements_executor() {
        use bytes::Bytes;
        use redis_tower_commands::Get;

        // Verify Arc<Mutex<MockRedis>> implements RedisExecutor at runtime too.
        let mock = Arc::new(Mutex::new(MockRedis::new(vec![Frame::BulkString(Some(
            Bytes::from("world"),
        ))])));
        let mut executor = Arc::clone(&mock);
        let result: Option<Bytes> = executor.execute(Get::new("key")).await.unwrap();
        assert_eq!(result, Some(Bytes::from("world")));
    }

    #[tokio::test]
    async fn mut_ref_implements_executor() {
        use bytes::Bytes;
        use redis_tower_commands::Get;

        // Verify &mut MockRedis implements RedisExecutor (used by Json::new(&mut conn)).
        let mut mock = MockRedis::new(vec![Frame::BulkString(Some(Bytes::from("ref")))]);
        let result: Option<Bytes> = mock.execute(Get::new("key")).await.unwrap();
        assert_eq!(result, Some(Bytes::from("ref")));
    }
}
