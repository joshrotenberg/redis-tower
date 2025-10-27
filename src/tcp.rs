//! TCP socket configuration options
//!
//! This module provides fine-grained control over TCP socket behavior for
//! performance tuning and specific deployment requirements.

use std::time::Duration;

/// TCP socket configuration
///
/// Controls low-level TCP socket options for performance tuning.
///
/// # Example
/// ```rust
/// use redis_tower::tcp::TcpConfig;
/// use std::time::Duration;
///
/// let tcp_config = TcpConfig::new()
///     .with_nodelay(true)              // Disable Nagle's algorithm
///     .with_ttl(64)                     // Set IP TTL
///     .with_linger(Some(Duration::from_secs(30)));  // Linger on close
///
/// // On Linux, you can also set TCP user timeout
/// #[cfg(target_os = "linux")]
/// let tcp_config = tcp_config.with_user_timeout(Duration::from_secs(10));
/// ```
#[derive(Debug, Clone, Default)]
pub struct TcpConfig {
    /// Enable/disable TCP_NODELAY (Nagle's algorithm)
    ///
    /// When enabled (true), small packets are sent immediately without buffering.
    /// This reduces latency but may increase bandwidth usage.
    ///
    /// **Default**: None (uses system default, typically disabled for most systems)
    ///
    /// **Recommendation**: Enable (true) for Redis to reduce latency
    pub nodelay: Option<bool>,

    /// Set the linger duration for the socket
    ///
    /// Controls how long the socket waits to send remaining data before closing.
    /// - `Some(duration)`: Wait up to duration for data to be sent
    /// - `None`: Close immediately (RST), discarding unsent data
    ///
    /// **Default**: None (uses system default)
    pub linger: Option<Option<Duration>>,

    /// Set the IP Time-To-Live (TTL) value
    ///
    /// Controls the maximum number of network hops before packets are discarded.
    ///
    /// **Default**: None (uses system default, typically 64)
    pub ttl: Option<u32>,

    /// Set TCP user timeout (Linux only)
    ///
    /// Maximum time transmitted data may remain unacknowledged before the
    /// connection is forcibly closed. This helps detect broken connections faster.
    ///
    /// **Default**: None (uses system default)
    ///
    /// **Platform**: Linux only (ignored on other platforms)
    #[cfg(target_os = "linux")]
    pub user_timeout: Option<Duration>,
}

impl TcpConfig {
    /// Create a new TCP configuration with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable or disable TCP_NODELAY (Nagle's algorithm)
    ///
    /// # Arguments
    /// * `nodelay` - true to disable Nagle's algorithm (send immediately), false to enable buffering
    ///
    /// # Example
    /// ```rust
    /// use redis_tower::tcp::TcpConfig;
    ///
    /// let config = TcpConfig::new().with_nodelay(true);
    /// ```
    pub fn with_nodelay(mut self, nodelay: bool) -> Self {
        self.nodelay = Some(nodelay);
        self
    }

    /// Set the linger duration for the socket
    ///
    /// # Arguments
    /// * `linger` - Some(duration) to wait, None for immediate close
    ///
    /// # Example
    /// ```rust
    /// use redis_tower::tcp::TcpConfig;
    /// use std::time::Duration;
    ///
    /// // Wait up to 30 seconds for data to be sent before closing
    /// let config = TcpConfig::new().with_linger(Some(Duration::from_secs(30)));
    ///
    /// // Close immediately without waiting
    /// let config = TcpConfig::new().with_linger(None);
    /// ```
    pub fn with_linger(mut self, linger: Option<Duration>) -> Self {
        self.linger = Some(linger);
        self
    }

    /// Set the IP Time-To-Live (TTL) value
    ///
    /// # Arguments
    /// * `ttl` - Number of network hops (typically 64 or 255)
    ///
    /// # Example
    /// ```rust
    /// use redis_tower::tcp::TcpConfig;
    ///
    /// let config = TcpConfig::new().with_ttl(64);
    /// ```
    pub fn with_ttl(mut self, ttl: u32) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Set TCP user timeout (Linux only)
    ///
    /// # Arguments
    /// * `timeout` - Maximum time for unacknowledged data
    ///
    /// # Example
    /// ```rust
    /// use redis_tower::tcp::TcpConfig;
    /// use std::time::Duration;
    ///
    /// #[cfg(target_os = "linux")]
    /// let config = TcpConfig::new().with_user_timeout(Duration::from_secs(10));
    /// ```
    #[cfg(target_os = "linux")]
    pub fn with_user_timeout(mut self, timeout: Duration) -> Self {
        self.user_timeout = Some(timeout);
        self
    }

    /// Create a configuration optimized for low latency
    ///
    /// - Enables TCP_NODELAY (disables Nagle's algorithm)
    /// - Sets reasonable TTL (64)
    /// - On Linux, sets aggressive user timeout (5s)
    ///
    /// # Example
    /// ```rust
    /// use redis_tower::tcp::TcpConfig;
    ///
    /// let config = TcpConfig::low_latency();
    /// ```
    pub fn low_latency() -> Self {
        let config = Self::new().with_nodelay(true).with_ttl(64);

        #[cfg(target_os = "linux")]
        let config = config.with_user_timeout(Duration::from_secs(5));

        config
    }

    /// Check if any TCP options are configured
    pub fn is_configured(&self) -> bool {
        self.nodelay.is_some() || self.linger.is_some() || self.ttl.is_some() || {
            #[cfg(target_os = "linux")]
            {
                self.user_timeout.is_some()
            }
            #[cfg(not(target_os = "linux"))]
            {
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tcp_config_default() {
        let config = TcpConfig::default();
        assert!(config.nodelay.is_none());
        assert!(config.linger.is_none());
        assert!(config.ttl.is_none());
        assert!(!config.is_configured());
    }

    #[test]
    fn test_tcp_config_builder() {
        let config = TcpConfig::new()
            .with_nodelay(true)
            .with_ttl(128)
            .with_linger(Some(Duration::from_secs(30)));

        assert_eq!(config.nodelay, Some(true));
        assert_eq!(config.ttl, Some(128));
        assert_eq!(config.linger, Some(Some(Duration::from_secs(30))));
        assert!(config.is_configured());
    }

    #[test]
    fn test_tcp_config_low_latency() {
        let config = TcpConfig::low_latency();
        assert_eq!(config.nodelay, Some(true));
        assert_eq!(config.ttl, Some(64));
        assert!(config.is_configured());

        #[cfg(target_os = "linux")]
        assert_eq!(config.user_timeout, Some(Duration::from_secs(5)));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_tcp_config_user_timeout() {
        let config = TcpConfig::new().with_user_timeout(Duration::from_secs(10));
        assert_eq!(config.user_timeout, Some(Duration::from_secs(10)));
        assert!(config.is_configured());
    }

    #[test]
    fn test_tcp_config_linger_none() {
        let config = TcpConfig::new().with_linger(None);
        assert_eq!(config.linger, Some(None));
        assert!(config.is_configured());
    }
}
