//! The [`RedisExecutor`] trait for executing Redis commands.
//!
//! All redis-tower client types implement this trait. Application code
//! can depend on `impl RedisExecutor` rather than concrete client types,
//! making it straightforward to substitute a mock in tests.

use std::future::Future;

use redis_tower_core::{Command, RedisConnection, RedisError};

use crate::caching::CachedClient;
use crate::client::RedisClient;
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
            let frame = self
                .responses
                .pop_front()
                .unwrap_or(Frame::Null);
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
}
