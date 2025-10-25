//! Metrics collection for Redis operations
//!
//! This module provides comprehensive metrics collection for Redis client operations,
//! including command execution, connection lifecycle, and error tracking.
//!
//! # Examples
//!
//! ```rust
//! use redis_tower::metrics::{MetricsConfig, MetricsCollector};
//!
//! // Create a metrics collector
//! let metrics = MetricsCollector::new();
//!
//! // Use with client configuration
//! let config = ClientConfig::builder()
//!     .metrics(MetricsConfig::enabled())
//!     .build();
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Configuration for metrics collection
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Enable command metrics (count, latency)
    pub collect_commands: bool,
    /// Enable connection metrics (pool stats, lifecycle)
    pub collect_connections: bool,
    /// Enable error metrics (by type)
    pub collect_errors: bool,
    /// Enable detailed per-command metrics
    pub collect_per_command: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            collect_commands: true,
            collect_connections: true,
            collect_errors: true,
            collect_per_command: false, // Disabled by default to avoid cardinality explosion
        }
    }
}

impl MetricsConfig {
    /// Enable all metrics collection
    pub fn all() -> Self {
        Self {
            collect_commands: true,
            collect_connections: true,
            collect_errors: true,
            collect_per_command: true,
        }
    }

    /// Disable all metrics collection
    pub fn none() -> Self {
        Self {
            collect_commands: false,
            collect_connections: false,
            collect_errors: false,
            collect_per_command: false,
        }
    }

    /// Create a new builder for custom configuration
    pub fn builder() -> MetricsConfigBuilder {
        MetricsConfigBuilder::default()
    }
}

/// Builder for MetricsConfig
#[derive(Debug, Default)]
pub struct MetricsConfigBuilder {
    collect_commands: Option<bool>,
    collect_connections: Option<bool>,
    collect_errors: Option<bool>,
    collect_per_command: Option<bool>,
}

impl MetricsConfigBuilder {
    /// Enable or disable command metrics
    pub fn collect_commands(mut self, enabled: bool) -> Self {
        self.collect_commands = Some(enabled);
        self
    }

    /// Enable or disable connection metrics
    pub fn collect_connections(mut self, enabled: bool) -> Self {
        self.collect_connections = Some(enabled);
        self
    }

    /// Enable or disable error metrics
    pub fn collect_errors(mut self, enabled: bool) -> Self {
        self.collect_errors = Some(enabled);
        self
    }

    /// Enable or disable per-command metrics
    pub fn collect_per_command(mut self, enabled: bool) -> Self {
        self.collect_per_command = Some(enabled);
        self
    }

    /// Build the configuration
    pub fn build(self) -> MetricsConfig {
        let default = MetricsConfig::default();
        MetricsConfig {
            collect_commands: self.collect_commands.unwrap_or(default.collect_commands),
            collect_connections: self
                .collect_connections
                .unwrap_or(default.collect_connections),
            collect_errors: self.collect_errors.unwrap_or(default.collect_errors),
            collect_per_command: self
                .collect_per_command
                .unwrap_or(default.collect_per_command),
        }
    }
}

/// Metrics collector for Redis operations
#[derive(Debug, Clone)]
pub struct MetricsCollector {
    config: MetricsConfig,
    command_metrics: Arc<CommandMetrics>,
    connection_metrics: Arc<ConnectionMetrics>,
    error_metrics: Arc<ErrorMetrics>,
}

impl MetricsCollector {
    /// Create a new metrics collector with default configuration
    pub fn new() -> Self {
        Self::with_config(MetricsConfig::default())
    }

    /// Create a new metrics collector with custom configuration
    pub fn with_config(config: MetricsConfig) -> Self {
        Self {
            config,
            command_metrics: Arc::new(CommandMetrics::default()),
            connection_metrics: Arc::new(ConnectionMetrics::default()),
            error_metrics: Arc::new(ErrorMetrics::default()),
        }
    }

    /// Record a command execution
    pub fn record_command(&self, duration: Duration) {
        if self.config.collect_commands {
            self.command_metrics.record(duration);
        }
    }

    /// Record a connection event
    pub fn record_connection_created(&self) {
        if self.config.collect_connections {
            self.connection_metrics
                .connections_created
                .fetch_add(1, Ordering::Relaxed);
            self.connection_metrics
                .connections_active
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a connection closure
    pub fn record_connection_closed(&self) {
        if self.config.collect_connections {
            self.connection_metrics
                .connections_closed
                .fetch_add(1, Ordering::Relaxed);
            self.connection_metrics
                .connections_active
                .fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Record a reconnection attempt
    pub fn record_reconnection(&self) {
        if self.config.collect_connections {
            self.connection_metrics
                .reconnections
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record an error
    pub fn record_error(&self, error_type: &str) {
        if self.config.collect_errors {
            match error_type {
                "connection" => self
                    .error_metrics
                    .connection_errors
                    .fetch_add(1, Ordering::Relaxed),
                "timeout" => self
                    .error_metrics
                    .timeout_errors
                    .fetch_add(1, Ordering::Relaxed),
                "parse" => self
                    .error_metrics
                    .parse_errors
                    .fetch_add(1, Ordering::Relaxed),
                "redis" => self
                    .error_metrics
                    .redis_errors
                    .fetch_add(1, Ordering::Relaxed),
                _ => self
                    .error_metrics
                    .other_errors
                    .fetch_add(1, Ordering::Relaxed),
            };
        }
    }

    /// Get a snapshot of current metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            commands: self.command_metrics.snapshot(),
            connections: self.connection_metrics.snapshot(),
            errors: self.error_metrics.snapshot(),
        }
    }

    /// Reset all metrics to zero
    pub fn reset(&self) {
        self.command_metrics.reset();
        self.connection_metrics.reset();
        self.error_metrics.reset();
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Command execution metrics
#[derive(Debug, Default)]
struct CommandMetrics {
    total_commands: AtomicU64,
    total_duration_micros: AtomicU64,
}

impl CommandMetrics {
    fn record(&self, duration: Duration) {
        self.total_commands.fetch_add(1, Ordering::Relaxed);
        self.total_duration_micros
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    fn snapshot(&self) -> CommandMetricsSnapshot {
        let total = self.total_commands.load(Ordering::Relaxed);
        let duration = self.total_duration_micros.load(Ordering::Relaxed);

        CommandMetricsSnapshot {
            total_commands: total,
            average_duration_micros: if total > 0 { duration / total } else { 0 },
        }
    }

    fn reset(&self) {
        self.total_commands.store(0, Ordering::Relaxed);
        self.total_duration_micros.store(0, Ordering::Relaxed);
    }
}

/// Connection lifecycle metrics
#[derive(Debug, Default)]
struct ConnectionMetrics {
    connections_created: AtomicU64,
    connections_closed: AtomicU64,
    connections_active: AtomicU64,
    reconnections: AtomicU64,
}

impl ConnectionMetrics {
    fn snapshot(&self) -> ConnectionMetricsSnapshot {
        ConnectionMetricsSnapshot {
            connections_created: self.connections_created.load(Ordering::Relaxed),
            connections_closed: self.connections_closed.load(Ordering::Relaxed),
            connections_active: self.connections_active.load(Ordering::Relaxed),
            reconnections: self.reconnections.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.connections_created.store(0, Ordering::Relaxed);
        self.connections_closed.store(0, Ordering::Relaxed);
        self.connections_active.store(0, Ordering::Relaxed);
        self.reconnections.store(0, Ordering::Relaxed);
    }
}

/// Error tracking metrics
#[derive(Debug, Default)]
struct ErrorMetrics {
    connection_errors: AtomicU64,
    timeout_errors: AtomicU64,
    parse_errors: AtomicU64,
    redis_errors: AtomicU64,
    other_errors: AtomicU64,
}

impl ErrorMetrics {
    fn snapshot(&self) -> ErrorMetricsSnapshot {
        ErrorMetricsSnapshot {
            connection_errors: self.connection_errors.load(Ordering::Relaxed),
            timeout_errors: self.timeout_errors.load(Ordering::Relaxed),
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
            redis_errors: self.redis_errors.load(Ordering::Relaxed),
            other_errors: self.other_errors.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.connection_errors.store(0, Ordering::Relaxed);
        self.timeout_errors.store(0, Ordering::Relaxed);
        self.parse_errors.store(0, Ordering::Relaxed);
        self.redis_errors.store(0, Ordering::Relaxed);
        self.other_errors.store(0, Ordering::Relaxed);
    }
}

/// Snapshot of all metrics at a point in time
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Command execution metrics
    pub commands: CommandMetricsSnapshot,
    /// Connection lifecycle metrics
    pub connections: ConnectionMetricsSnapshot,
    /// Error tracking metrics
    pub errors: ErrorMetricsSnapshot,
}

impl MetricsSnapshot {
    /// Get total error count across all types
    pub fn total_errors(&self) -> u64 {
        self.errors.connection_errors
            + self.errors.timeout_errors
            + self.errors.parse_errors
            + self.errors.redis_errors
            + self.errors.other_errors
    }
}

/// Snapshot of command metrics
#[derive(Debug, Clone)]
pub struct CommandMetricsSnapshot {
    /// Total number of commands executed
    pub total_commands: u64,
    /// Average command duration in microseconds
    pub average_duration_micros: u64,
}

impl CommandMetricsSnapshot {
    /// Get average duration as a Duration
    pub fn average_duration(&self) -> Duration {
        Duration::from_micros(self.average_duration_micros)
    }
}

/// Snapshot of connection metrics
#[derive(Debug, Clone)]
pub struct ConnectionMetricsSnapshot {
    /// Total connections created
    pub connections_created: u64,
    /// Total connections closed
    pub connections_closed: u64,
    /// Currently active connections
    pub connections_active: u64,
    /// Total reconnection attempts
    pub reconnections: u64,
}

/// Snapshot of error metrics
#[derive(Debug, Clone)]
pub struct ErrorMetricsSnapshot {
    /// Connection errors
    pub connection_errors: u64,
    /// Timeout errors
    pub timeout_errors: u64,
    /// Parse errors
    pub parse_errors: u64,
    /// Redis server errors
    pub redis_errors: u64,
    /// Other errors
    pub other_errors: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_config_default() {
        let config = MetricsConfig::default();
        assert!(config.collect_commands);
        assert!(config.collect_connections);
        assert!(config.collect_errors);
        assert!(!config.collect_per_command);
    }

    #[test]
    fn test_metrics_config_all() {
        let config = MetricsConfig::all();
        assert!(config.collect_commands);
        assert!(config.collect_connections);
        assert!(config.collect_errors);
        assert!(config.collect_per_command);
    }

    #[test]
    fn test_metrics_config_none() {
        let config = MetricsConfig::none();
        assert!(!config.collect_commands);
        assert!(!config.collect_connections);
        assert!(!config.collect_errors);
        assert!(!config.collect_per_command);
    }

    #[test]
    fn test_metrics_config_builder() {
        let config = MetricsConfig::builder()
            .collect_commands(true)
            .collect_connections(false)
            .collect_errors(true)
            .collect_per_command(false)
            .build();

        assert!(config.collect_commands);
        assert!(!config.collect_connections);
        assert!(config.collect_errors);
        assert!(!config.collect_per_command);
    }

    #[test]
    fn test_command_metrics() {
        let collector = MetricsCollector::new();

        collector.record_command(Duration::from_millis(10));
        collector.record_command(Duration::from_millis(20));
        collector.record_command(Duration::from_millis(30));

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.commands.total_commands, 3);
        assert_eq!(snapshot.commands.average_duration_micros, 20_000);
    }

    #[test]
    fn test_connection_metrics() {
        let collector = MetricsCollector::new();

        collector.record_connection_created();
        collector.record_connection_created();
        collector.record_connection_closed();
        collector.record_reconnection();

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.connections.connections_created, 2);
        assert_eq!(snapshot.connections.connections_closed, 1);
        assert_eq!(snapshot.connections.connections_active, 1);
        assert_eq!(snapshot.connections.reconnections, 1);
    }

    #[test]
    fn test_error_metrics() {
        let collector = MetricsCollector::new();

        collector.record_error("connection");
        collector.record_error("timeout");
        collector.record_error("parse");
        collector.record_error("redis");
        collector.record_error("other");

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.errors.connection_errors, 1);
        assert_eq!(snapshot.errors.timeout_errors, 1);
        assert_eq!(snapshot.errors.parse_errors, 1);
        assert_eq!(snapshot.errors.redis_errors, 1);
        assert_eq!(snapshot.errors.other_errors, 1);
        assert_eq!(snapshot.total_errors(), 5);
    }

    #[test]
    fn test_metrics_reset() {
        let collector = MetricsCollector::new();

        collector.record_command(Duration::from_millis(10));
        collector.record_connection_created();
        collector.record_error("connection");

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.commands.total_commands, 1);
        assert_eq!(snapshot.connections.connections_created, 1);
        assert_eq!(snapshot.errors.connection_errors, 1);

        collector.reset();

        let snapshot = collector.snapshot();
        assert_eq!(snapshot.commands.total_commands, 0);
        assert_eq!(snapshot.connections.connections_created, 0);
        assert_eq!(snapshot.errors.connection_errors, 0);
    }

    #[test]
    fn test_metrics_disabled() {
        let config = MetricsConfig::none();
        let collector = MetricsCollector::with_config(config);

        collector.record_command(Duration::from_millis(10));
        collector.record_connection_created();
        collector.record_error("connection");

        let snapshot = collector.snapshot();
        // Metrics should not be collected when disabled
        assert_eq!(snapshot.commands.total_commands, 0);
        assert_eq!(snapshot.connections.connections_created, 0);
        assert_eq!(snapshot.errors.connection_errors, 0);
    }
}
