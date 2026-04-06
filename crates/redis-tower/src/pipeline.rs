//! Redis command pipelining.
//!
//! [`Pipeline`] batches multiple commands into a single network roundtrip,
//! reducing latency for bulk operations. Each command's typed response is
//! preserved and can be extracted from [`PipelineResults`] by index.
//!
//! # Example
//!
//! ```ignore
//! use redis_tower::{Pipeline, RedisConnection, commands::*};
//!
//! let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
//! let results = Pipeline::new()
//!     .push(Set::new("a", "1"))
//!     .push(Set::new("b", "2"))
//!     .push(Get::new("a"))
//!     .execute(&mut conn)
//!     .await?;
//!
//! let val: &Option<bytes::Bytes> = results.get(2)?;
//! ```

use std::any::Any;

use redis_tower_core::{Command, Frame, RedisConnection, RedisError};

/// Type-erased response parser: takes a Frame, returns a boxed Any result.
pub(crate) type ResponseParser =
    Box<dyn FnOnce(Frame) -> Result<Box<dyn Any + Send>, RedisError> + Send>;

/// A type-erased command entry for pipeline batching.
struct PipelineEntry {
    frame: Frame,
    parser: ResponseParser,
}

/// Batches multiple commands into a single network roundtrip.
///
/// Each command's response type is preserved and can be extracted from
/// the [`PipelineResults`] by index.
///
/// # Example
///
/// ```ignore
/// use redis_tower::{Pipeline, RedisConnection};
/// use redis_tower::commands::*;
///
/// let conn = RedisConnection::connect("127.0.0.1:6379").await?;
/// let results = Pipeline::new()
///     .push(Set::new("a", "1"))
///     .push(Set::new("b", "2"))
///     .push(Get::new("a"))
///     .execute(&conn)
///     .await?;
///
/// let val: Option<bytes::Bytes> = results.get(2)?;
/// ```
pub struct Pipeline {
    entries: Vec<PipelineEntry>,
}

impl Pipeline {
    /// Create a new empty pipeline.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Add a command to the pipeline. Returns `self` for chaining.
    pub fn push<Cmd: Command + 'static>(mut self, cmd: Cmd) -> Self {
        let frame = cmd.to_frame();
        let parser = Box::new(
            move |response: Frame| -> Result<Box<dyn Any + Send>, RedisError> {
                let result = cmd.parse_response(response)?;
                Ok(Box::new(result))
            },
        );
        self.entries.push(PipelineEntry { frame, parser });
        self
    }

    /// Execute all queued commands in a single roundtrip.
    pub async fn execute(self, conn: &mut RedisConnection) -> Result<PipelineResults, RedisError> {
        let frames: Vec<Frame> = self.entries.iter().map(|e| e.frame.clone()).collect();
        let responses = conn.execute_pipeline(frames).await?;

        let mut results = Vec::with_capacity(responses.len());
        for (entry, response) in self.entries.into_iter().zip(responses) {
            // Surface Redis errors per-command.
            if let Frame::Error(ref e) = response {
                results.push(Err(RedisError::Redis(
                    String::from_utf8_lossy(e).into_owned(),
                )));
            } else {
                results.push((entry.parser)(response));
            }
        }

        Ok(PipelineResults { results })
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

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Results from a pipeline execution.
///
/// Each result is accessed by index and downcast to the expected type.
pub struct PipelineResults {
    results: Vec<Result<Box<dyn Any + Send>, RedisError>>,
}

impl PipelineResults {
    /// Create from raw type-erased results (used by Transaction).
    pub(crate) fn from_raw(results: Vec<Result<Box<dyn Any + Send>, RedisError>>) -> Self {
        Self { results }
    }

    /// Get the result at `index`, downcasting to the expected response type.
    ///
    /// Returns `Err` if the command at that index produced a Redis error,
    /// or if the type `T` doesn't match the command's `Response` type.
    pub fn get<T: Send + 'static>(&self, index: usize) -> Result<&T, RedisError> {
        match self.results.get(index) {
            Some(Ok(boxed)) => boxed.downcast_ref::<T>().ok_or(RedisError::TypeMismatch {
                expected: std::any::type_name::<T>(),
            }),
            Some(Err(e)) => Err(RedisError::Redis(e.to_string())),
            None => Err(RedisError::TypeMismatch {
                expected: "valid index",
            }),
        }
    }

    /// Take the result at `index`, consuming it.
    pub fn take<T: Send + 'static>(&mut self, index: usize) -> Result<T, RedisError> {
        match self.results.get_mut(index) {
            Some(slot) => {
                // Replace with a placeholder error so it can't be taken twice.
                let result = std::mem::replace(
                    slot,
                    Err(RedisError::TypeMismatch {
                        expected: "already taken",
                    }),
                );
                match result {
                    Ok(boxed) => {
                        boxed
                            .downcast::<T>()
                            .map(|b| *b)
                            .map_err(|_| RedisError::TypeMismatch {
                                expected: std::any::type_name::<T>(),
                            })
                    }
                    Err(e) => Err(e),
                }
            }
            None => Err(RedisError::TypeMismatch {
                expected: "valid index",
            }),
        }
    }

    /// Returns the number of results.
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Returns true if there are no results.
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }
}
