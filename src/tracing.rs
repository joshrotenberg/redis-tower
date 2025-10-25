//! Tracing and observability utilities for Redis operations
//!
//! This module provides structured tracing for Redis commands, connections,
//! and network operations using `tracing` and `tokio-tracing`.

use tracing::Level;

/// Tracing configuration for Redis operations
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// Whether to trace individual commands
    pub trace_commands: bool,

    /// Whether to trace connection lifecycle events
    pub trace_connections: bool,

    /// Whether to trace network I/O operations
    pub trace_network: bool,

    /// Default tracing level for commands
    pub command_level: Level,

    /// Default tracing level for connections
    pub connection_level: Level,

    /// Default tracing level for network operations
    pub network_level: Level,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            trace_commands: true,
            trace_connections: true,
            trace_network: false, // Network tracing can be verbose
            command_level: Level::DEBUG,
            connection_level: Level::INFO,
            network_level: Level::TRACE,
        }
    }
}

impl TracingConfig {
    /// Create a new builder for tracing configuration
    pub fn builder() -> TracingConfigBuilder {
        TracingConfigBuilder::default()
    }

    /// Enable all tracing
    pub fn all() -> Self {
        Self {
            trace_commands: true,
            trace_connections: true,
            trace_network: true,
            ..Default::default()
        }
    }

    /// Disable all tracing
    pub fn none() -> Self {
        Self {
            trace_commands: false,
            trace_connections: false,
            trace_network: false,
            ..Default::default()
        }
    }
}

/// Builder for tracing configuration
#[derive(Debug, Default)]
pub struct TracingConfigBuilder {
    trace_commands: Option<bool>,
    trace_connections: Option<bool>,
    trace_network: Option<bool>,
    command_level: Option<Level>,
    connection_level: Option<Level>,
    network_level: Option<Level>,
}

impl TracingConfigBuilder {
    /// Enable or disable command tracing
    pub fn trace_commands(mut self, enabled: bool) -> Self {
        self.trace_commands = Some(enabled);
        self
    }

    /// Enable or disable connection tracing
    pub fn trace_connections(mut self, enabled: bool) -> Self {
        self.trace_connections = Some(enabled);
        self
    }

    /// Enable or disable network tracing
    pub fn trace_network(mut self, enabled: bool) -> Self {
        self.trace_network = Some(enabled);
        self
    }

    /// Set the tracing level for commands
    pub fn command_level(mut self, level: Level) -> Self {
        self.command_level = Some(level);
        self
    }

    /// Set the tracing level for connections
    pub fn connection_level(mut self, level: Level) -> Self {
        self.connection_level = Some(level);
        self
    }

    /// Set the tracing level for network operations
    pub fn network_level(mut self, level: Level) -> Self {
        self.network_level = Some(level);
        self
    }

    /// Build the tracing configuration
    pub fn build(self) -> TracingConfig {
        let default = TracingConfig::default();
        TracingConfig {
            trace_commands: self.trace_commands.unwrap_or(default.trace_commands),
            trace_connections: self.trace_connections.unwrap_or(default.trace_connections),
            trace_network: self.trace_network.unwrap_or(default.trace_network),
            command_level: self.command_level.unwrap_or(default.command_level),
            connection_level: self.connection_level.unwrap_or(default.connection_level),
            network_level: self.network_level.unwrap_or(default.network_level),
        }
    }
}

/// Helper macro for conditionally creating command spans
#[macro_export]
macro_rules! trace_command {
    ($config:expr, $command:expr, $body:expr) => {{
        if $config.trace_commands {
            let span = tracing::span!($config.command_level, "redis_command", command = $command,);
            let _enter = span.enter();
            $body
        } else {
            $body
        }
    }};
}

/// Helper macro for conditionally creating connection spans
#[macro_export]
macro_rules! trace_connection {
    ($config:expr, $event:expr, $addr:expr, $body:expr) => {{
        if $config.trace_connections {
            let span = tracing::span!(
                $config.connection_level,
                "redis_connection",
                event = $event,
                addr = $addr,
            );
            let _enter = span.enter();
            $body
        } else {
            $body
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TracingConfig::default();
        assert!(config.trace_commands);
        assert!(config.trace_connections);
        assert!(!config.trace_network);
        assert_eq!(config.command_level, Level::DEBUG);
        assert_eq!(config.connection_level, Level::INFO);
    }

    #[test]
    fn test_all_config() {
        let config = TracingConfig::all();
        assert!(config.trace_commands);
        assert!(config.trace_connections);
        assert!(config.trace_network);
    }

    #[test]
    fn test_none_config() {
        let config = TracingConfig::none();
        assert!(!config.trace_commands);
        assert!(!config.trace_connections);
        assert!(!config.trace_network);
    }

    #[test]
    fn test_builder() {
        let config = TracingConfig::builder()
            .trace_commands(true)
            .trace_network(true)
            .command_level(Level::INFO)
            .build();

        assert!(config.trace_commands);
        assert!(config.trace_network);
        assert_eq!(config.command_level, Level::INFO);
    }
}
