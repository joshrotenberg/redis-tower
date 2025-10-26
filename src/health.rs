//! Connection health checking and validation
//!
//! This module provides comprehensive health checking for Redis connections:
//! - Active health checks (PING-based validation)
//! - Passive health checks (error tracking)
//! - Configurable health check policies
//! - Integration with connection pooling and resilience layers
//!
//! # Examples
//!
//! ```rust,no_run
//! use redis_tower::health::{HealthCheckConfig, HealthChecker};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create health check configuration
//! let config = HealthCheckConfig::builder()
//!     .interval(Duration::from_secs(30))
//!     .timeout(Duration::from_secs(5))
//!     .failure_threshold(3)
//!     .success_threshold(2)
//!     .build();
//!
//! // Health checks are automatically integrated with ResilientConnection
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::client::RedisConnection;
use crate::commands::Ping;
use crate::types::RedisError;

/// Health status of a Redis connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// Connection is healthy
    Healthy,
    /// Connection is degraded (some failures but not yet unhealthy)
    Degraded,
    /// Connection is unhealthy
    Unhealthy,
    /// Health status is unknown (not yet checked)
    Unknown,
}

/// Configuration for connection health checking
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Interval between health checks (default: 30 seconds)
    interval: Duration,
    /// Timeout for each health check (default: 5 seconds)
    timeout: Duration,
    /// Number of consecutive failures before marking unhealthy (default: 3)
    failure_threshold: usize,
    /// Number of consecutive successes needed to mark healthy (default: 2)
    success_threshold: usize,
    /// Whether to perform health checks (default: true)
    enabled: bool,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(5),
            failure_threshold: 3,
            success_threshold: 2,
            enabled: true,
        }
    }
}

impl HealthCheckConfig {
    /// Create a new builder for health check configuration
    pub fn builder() -> HealthCheckConfigBuilder {
        HealthCheckConfigBuilder::default()
    }

    /// Disable health checks
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Get the health check interval
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Get the health check timeout
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Get the failure threshold
    pub fn failure_threshold(&self) -> usize {
        self.failure_threshold
    }

    /// Get the success threshold
    pub fn success_threshold(&self) -> usize {
        self.success_threshold
    }

    /// Check if health checks are enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Builder for HealthCheckConfig
#[derive(Debug, Default)]
pub struct HealthCheckConfigBuilder {
    interval: Option<Duration>,
    timeout: Option<Duration>,
    failure_threshold: Option<usize>,
    success_threshold: Option<usize>,
    enabled: Option<bool>,
}

impl HealthCheckConfigBuilder {
    /// Set the interval between health checks
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = Some(interval);
        self
    }

    /// Set the timeout for each health check
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set the number of consecutive failures before marking unhealthy
    pub fn failure_threshold(mut self, threshold: usize) -> Self {
        self.failure_threshold = Some(threshold);
        self
    }

    /// Set the number of consecutive successes needed to mark healthy
    pub fn success_threshold(mut self, threshold: usize) -> Self {
        self.success_threshold = Some(threshold);
        self
    }

    /// Enable or disable health checks
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = Some(enabled);
        self
    }

    /// Build the health check configuration
    pub fn build(self) -> HealthCheckConfig {
        let default = HealthCheckConfig::default();
        HealthCheckConfig {
            interval: self.interval.unwrap_or(default.interval),
            timeout: self.timeout.unwrap_or(default.timeout),
            failure_threshold: self.failure_threshold.unwrap_or(default.failure_threshold),
            success_threshold: self.success_threshold.unwrap_or(default.success_threshold),
            enabled: self.enabled.unwrap_or(default.enabled),
        }
    }
}

/// Health checker for Redis connections
///
/// Tracks connection health using both active (PING) and passive (error tracking) checks.
#[derive(Debug, Clone)]
pub struct HealthChecker {
    config: Arc<HealthCheckConfig>,
    state: Arc<HealthState>,
}

#[derive(Debug)]
struct HealthState {
    status: RwLock<HealthStatus>,
    consecutive_successes: AtomicUsize,
    consecutive_failures: AtomicUsize,
    last_check: RwLock<Option<Instant>>,
    total_checks: AtomicU64,
    total_successes: AtomicU64,
    total_failures: AtomicU64,
}

impl Default for HealthState {
    fn default() -> Self {
        Self {
            status: RwLock::new(HealthStatus::Unknown),
            consecutive_successes: AtomicUsize::new(0),
            consecutive_failures: AtomicUsize::new(0),
            last_check: RwLock::new(None),
            total_checks: AtomicU64::new(0),
            total_successes: AtomicU64::new(0),
            total_failures: AtomicU64::new(0),
        }
    }
}

impl HealthChecker {
    /// Create a new health checker with the given configuration
    pub fn new(config: HealthCheckConfig) -> Self {
        Self {
            config: Arc::new(config),
            state: Arc::new(HealthState::default()),
        }
    }

    /// Get the current health status
    pub async fn status(&self) -> HealthStatus {
        *self.state.status.read().await
    }

    /// Check if the connection should be health checked
    ///
    /// Returns true if:
    /// - Health checks are enabled
    /// - Sufficient time has passed since last check
    pub async fn should_check(&self) -> bool {
        if !self.config.enabled {
            return false;
        }

        let last_check = self.state.last_check.read().await;
        match *last_check {
            None => true, // Never checked
            Some(last) => last.elapsed() >= self.config.interval,
        }
    }

    /// Perform an active health check using PING
    ///
    /// Returns true if the connection is healthy, false otherwise.
    pub async fn check(&self, connection: &RedisConnection) -> bool {
        if !self.config.enabled {
            return true; // Always healthy if checks disabled
        }

        self.state.total_checks.fetch_add(1, Ordering::Relaxed);
        *self.state.last_check.write().await = Some(Instant::now());

        // Perform PING with timeout
        let ping_result =
            tokio::time::timeout(self.config.timeout, connection.execute(Ping::new())).await;

        let is_healthy = ping_result.is_ok() && ping_result.unwrap().is_ok();

        if is_healthy {
            self.record_success().await;
        } else {
            self.record_failure().await;
        }

        is_healthy
    }

    /// Record a successful health check
    async fn record_success(&self) {
        self.state.total_successes.fetch_add(1, Ordering::Relaxed);
        self.state.consecutive_failures.store(0, Ordering::Relaxed);
        let successes = self
            .state
            .consecutive_successes
            .fetch_add(1, Ordering::Relaxed)
            + 1;

        if successes >= self.config.success_threshold {
            *self.state.status.write().await = HealthStatus::Healthy;
        }
    }

    /// Record a failed health check
    async fn record_failure(&self) {
        self.state.total_failures.fetch_add(1, Ordering::Relaxed);
        self.state.consecutive_successes.store(0, Ordering::Relaxed);
        let failures = self
            .state
            .consecutive_failures
            .fetch_add(1, Ordering::Relaxed)
            + 1;

        let mut status = self.state.status.write().await;
        if failures >= self.config.failure_threshold {
            *status = HealthStatus::Unhealthy;
        } else if failures > 0 {
            *status = HealthStatus::Degraded;
        }
    }

    /// Record a passive failure (error during normal operation)
    ///
    /// This allows tracking connection health based on command failures,
    /// not just explicit health checks.
    pub async fn record_error(&self, _error: &RedisError) {
        self.record_failure().await;
    }

    /// Get health check statistics
    pub fn stats(&self) -> HealthStats {
        HealthStats {
            total_checks: self.state.total_checks.load(Ordering::Relaxed),
            total_successes: self.state.total_successes.load(Ordering::Relaxed),
            total_failures: self.state.total_failures.load(Ordering::Relaxed),
            consecutive_successes: self.state.consecutive_successes.load(Ordering::Relaxed),
            consecutive_failures: self.state.consecutive_failures.load(Ordering::Relaxed),
        }
    }

    /// Reset health check state
    pub async fn reset(&self) {
        *self.state.status.write().await = HealthStatus::Unknown;
        self.state.consecutive_successes.store(0, Ordering::Relaxed);
        self.state.consecutive_failures.store(0, Ordering::Relaxed);
        *self.state.last_check.write().await = None;
    }
}

/// Health check statistics
#[derive(Debug, Clone, Copy)]
pub struct HealthStats {
    /// Total number of health checks performed
    pub total_checks: u64,
    /// Total number of successful checks
    pub total_successes: u64,
    /// Total number of failed checks
    pub total_failures: u64,
    /// Current consecutive successes
    pub consecutive_successes: usize,
    /// Current consecutive failures
    pub consecutive_failures: usize,
}

impl HealthStats {
    /// Get the success rate as a percentage (0.0 - 100.0)
    pub fn success_rate(&self) -> f64 {
        if self.total_checks == 0 {
            0.0
        } else {
            (self.total_successes as f64 / self.total_checks as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_check_config_default() {
        let config = HealthCheckConfig::default();
        assert_eq!(config.interval(), Duration::from_secs(30));
        assert_eq!(config.timeout(), Duration::from_secs(5));
        assert_eq!(config.failure_threshold(), 3);
        assert_eq!(config.success_threshold(), 2);
        assert!(config.is_enabled());
    }

    #[test]
    fn test_health_check_config_builder() {
        let config = HealthCheckConfig::builder()
            .interval(Duration::from_secs(60))
            .timeout(Duration::from_secs(10))
            .failure_threshold(5)
            .success_threshold(3)
            .enabled(false)
            .build();

        assert_eq!(config.interval(), Duration::from_secs(60));
        assert_eq!(config.timeout(), Duration::from_secs(10));
        assert_eq!(config.failure_threshold(), 5);
        assert_eq!(config.success_threshold(), 3);
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_health_check_config_disabled() {
        let config = HealthCheckConfig::disabled();
        assert!(!config.is_enabled());
    }

    #[tokio::test]
    async fn test_health_checker_initial_status() {
        let checker = HealthChecker::new(HealthCheckConfig::default());
        assert_eq!(checker.status().await, HealthStatus::Unknown);
    }

    #[tokio::test]
    async fn test_health_checker_should_check() {
        let config = HealthCheckConfig::builder()
            .interval(Duration::from_millis(100))
            .build();
        let checker = HealthChecker::new(config);

        // Should check initially
        assert!(checker.should_check().await);

        // Mark as checked
        *checker.state.last_check.write().await = Some(Instant::now());

        // Should not check immediately
        assert!(!checker.should_check().await);

        // Wait for interval
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should check again
        assert!(checker.should_check().await);
    }

    #[tokio::test]
    async fn test_health_checker_disabled() {
        let config = HealthCheckConfig::disabled();
        let checker = HealthChecker::new(config);

        // Should never check when disabled
        assert!(!checker.should_check().await);
    }

    #[tokio::test]
    async fn test_health_stats_success_rate() {
        let stats = HealthStats {
            total_checks: 100,
            total_successes: 95,
            total_failures: 5,
            consecutive_successes: 10,
            consecutive_failures: 0,
        };

        assert_eq!(stats.success_rate(), 95.0);
    }

    #[tokio::test]
    async fn test_health_stats_success_rate_zero_checks() {
        let stats = HealthStats {
            total_checks: 0,
            total_successes: 0,
            total_failures: 0,
            consecutive_successes: 0,
            consecutive_failures: 0,
        };

        assert_eq!(stats.success_rate(), 0.0);
    }

    #[tokio::test]
    async fn test_health_checker_record_success() {
        let config = HealthCheckConfig::builder().success_threshold(2).build();
        let checker = HealthChecker::new(config);

        // Initial status
        assert_eq!(checker.status().await, HealthStatus::Unknown);

        // First success - not yet healthy
        checker.record_success().await;
        assert_ne!(checker.status().await, HealthStatus::Healthy);

        // Second success - now healthy
        checker.record_success().await;
        assert_eq!(checker.status().await, HealthStatus::Healthy);

        let stats = checker.stats();
        assert_eq!(stats.total_successes, 2);
        assert_eq!(stats.consecutive_successes, 2);
    }

    #[tokio::test]
    async fn test_health_checker_record_failure() {
        let config = HealthCheckConfig::builder().failure_threshold(3).build();
        let checker = HealthChecker::new(config);

        // First failure - degraded
        checker.record_failure().await;
        assert_eq!(checker.status().await, HealthStatus::Degraded);

        // Second failure - still degraded
        checker.record_failure().await;
        assert_eq!(checker.status().await, HealthStatus::Degraded);

        // Third failure - unhealthy
        checker.record_failure().await;
        assert_eq!(checker.status().await, HealthStatus::Unhealthy);

        let stats = checker.stats();
        assert_eq!(stats.total_failures, 3);
        assert_eq!(stats.consecutive_failures, 3);
    }

    #[tokio::test]
    async fn test_health_checker_reset() {
        let checker = HealthChecker::new(HealthCheckConfig::default());

        // Record some activity
        checker.record_success().await;
        checker.record_failure().await;

        // Reset
        checker.reset().await;

        // Should be back to unknown
        assert_eq!(checker.status().await, HealthStatus::Unknown);
        let stats = checker.stats();
        assert_eq!(stats.consecutive_successes, 0);
        assert_eq!(stats.consecutive_failures, 0);
    }
}
