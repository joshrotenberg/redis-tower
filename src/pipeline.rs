//! Type-safe Redis command pipelining
//!
//! Pipelining allows sending multiple commands to Redis in a single network roundtrip,
//! significantly improving throughput when executing many commands.
//!
//! Unlike redis-rs which uses a stringly-typed API, this implementation provides
//! full type safety through the Command trait.
//!
//! # Examples
//!
//! ```no_run
//! use redis_tower::Pipeline;
//! use redis_tower::commands::{Get, Set};
//! use redis_tower::client::RedisConnection;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let client = RedisConnection::connect("127.0.0.1:6379").await?;
//! let mut pipeline = Pipeline::new();
//!
//! pipeline
//!     .add(Set::new("key1", "value1"))
//!     .add(Set::new("key2", "value2"))
//!     .add(Get::new("key1"))
//!     .add(Get::new("key2"));
//!
//! let results = pipeline.execute(&client).await?;
//! # Ok(())
//! # }
//! ```

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;

/// A pipeline for batching multiple Redis commands
///
/// Pipelines send multiple commands to Redis in a single network roundtrip,
/// then read all responses back. This dramatically improves throughput compared
/// to executing commands one at a time.
///
/// # Type Safety
///
/// Unlike traditional Redis clients, this pipeline maintains type information
/// for each command. The response types are preserved and can be extracted
/// in a type-safe manner.
///
/// # Atomic Pipelines
///
/// Call `.atomic()` to wrap the pipeline in MULTI/EXEC, making all commands
/// execute atomically (all or nothing).
pub struct Pipeline {
    /// Commands to execute in the pipeline
    commands: Vec<Box<dyn PipelineCommand>>,
    /// Whether to wrap in MULTI/EXEC
    atomic: bool,
}

impl Pipeline {
    /// Create a new empty pipeline
    pub fn new() -> Self {
        Self {
            commands: Vec::new(),
            atomic: false,
        }
    }

    /// Create a new pipeline with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            commands: Vec::with_capacity(capacity),
            atomic: false,
        }
    }

    /// Add a command to the pipeline
    ///
    /// Commands are executed in the order they are added.
    pub fn add<Cmd>(&mut self, command: Cmd) -> &mut Self
    where
        Cmd: Command + Send + Sync + 'static,
    {
        self.commands.push(Box::new(TypedCommand { command }));
        self
    }

    /// Enable atomic mode (MULTI/EXEC)
    ///
    /// When atomic mode is enabled, all commands are wrapped in a transaction.
    /// Either all commands succeed or none do.
    pub fn atomic(&mut self) -> &mut Self {
        self.atomic = true;
        self
    }

    /// Check if pipeline is in atomic mode
    pub fn is_atomic(&self) -> bool {
        self.atomic
    }

    /// Get the number of commands in the pipeline
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Check if the pipeline is empty
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }

    /// Clear all commands from the pipeline
    pub fn clear(&mut self) {
        self.commands.clear();
    }

    /// Execute the pipeline against a connection
    ///
    /// Sends all commands in a single batch and reads all responses.
    /// Returns a PipelineResults that can be used to extract typed responses.
    pub async fn execute<C>(&self, connection: &C) -> Result<PipelineResults, RedisError>
    where
        C: PipelineExecutor,
    {
        connection.execute_pipeline(self).await
    }

    /// Get all command frames for encoding
    pub(crate) fn frames(&self) -> Vec<Frame> {
        let mut frames = Vec::new();

        if self.atomic {
            // Add MULTI command
            frames.push(Frame::Array(vec![Frame::BulkString(Some(
                bytes::Bytes::from("MULTI"),
            ))]));
        }

        // Add all command frames
        for cmd in &self.commands {
            frames.push(cmd.to_frame());
        }

        if self.atomic {
            // Add EXEC command
            frames.push(Frame::Array(vec![Frame::BulkString(Some(
                bytes::Bytes::from("EXEC"),
            ))]));
        }

        frames
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for executing pipelines
///
/// Implemented by RedisConnection and ClusterClient to support
/// pipeline execution.
pub trait PipelineExecutor {
    /// Execute a pipeline and return the raw results
    fn execute_pipeline(
        &self,
        pipeline: &Pipeline,
    ) -> impl std::future::Future<Output = Result<PipelineResults, RedisError>> + Send;
}

/// Results from executing a pipeline
///
/// Holds all response frames from the pipeline execution.
/// Individual responses can be extracted and parsed into their expected types.
pub struct PipelineResults {
    /// Raw response frames
    frames: Vec<Frame>,
    /// Current position for sequential extraction
    position: usize,
}

impl PipelineResults {
    /// Create new pipeline results from frames
    pub fn new(frames: Vec<Frame>) -> Self {
        Self {
            frames,
            position: 0,
        }
    }

    /// Get the number of results
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Check if results are empty
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Extract the next result as a specific type
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No more results available
    /// - Frame cannot be parsed into the requested type
    pub fn next_result<Cmd: Command>(&mut self) -> Result<Cmd::Response, RedisError> {
        if self.position >= self.frames.len() {
            return Err(RedisError::Protocol(
                "No more results in pipeline".to_string(),
            ));
        }

        let frame = self.frames[self.position].clone();
        self.position += 1;

        Cmd::parse_response(frame)
    }

    /// Get a specific result by index
    pub fn get<Cmd: Command>(&self, index: usize) -> Result<Cmd::Response, RedisError> {
        if index >= self.frames.len() {
            return Err(RedisError::Protocol(format!(
                "Index {} out of bounds (pipeline has {} results)",
                index,
                self.frames.len()
            )));
        }

        Cmd::parse_response(self.frames[index].clone())
    }

    /// Get all frames (for debugging)
    pub fn frames(&self) -> &[Frame] {
        &self.frames
    }
}

/// Internal trait for type-erased pipeline commands
trait PipelineCommand: Send + Sync {
    /// Convert command to frame
    fn to_frame(&self) -> Frame;
}

/// Wrapper for typed commands in the pipeline
struct TypedCommand<Cmd> {
    command: Cmd,
}

impl<Cmd: Command + Send + Sync> PipelineCommand for TypedCommand<Cmd> {
    fn to_frame(&self) -> Frame {
        self.command.to_frame()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_creation() {
        let pipeline = Pipeline::new();
        assert_eq!(pipeline.len(), 0);
        assert!(pipeline.is_empty());
        assert!(!pipeline.is_atomic());
    }

    #[test]
    fn test_pipeline_with_capacity() {
        let pipeline = Pipeline::with_capacity(10);
        assert_eq!(pipeline.len(), 0);
        assert_eq!(pipeline.commands.capacity(), 10);
    }

    #[test]
    fn test_pipeline_atomic() {
        let mut pipeline = Pipeline::new();
        assert!(!pipeline.is_atomic());

        pipeline.atomic();
        assert!(pipeline.is_atomic());
    }

    #[test]
    fn test_pipeline_clear() {
        let mut pipeline = Pipeline::new();
        // We'd need actual commands to test this properly
        pipeline.clear();
        assert_eq!(pipeline.len(), 0);
    }
}
