//! URL parsing for Redis connection strings
//!
//! Supports both `redis://` and `rediss://` schemes.

use crate::tls::TlsConfig;
use crate::types::RedisError;

/// Connection type for Redis
#[derive(Debug, Clone)]
pub enum ConnectionType {
    /// TCP connection with host and port
    Tcp { host: String, port: u16 },
    /// Unix domain socket with path
    Unix { path: String },
}

/// Parsed Redis URL
#[derive(Debug, Clone)]
pub struct RedisUrl {
    /// Connection type (TCP or Unix socket)
    pub connection: ConnectionType,
    /// TLS configuration (automatic for rediss://, not applicable for unix://)
    pub tls: TlsConfig,
    /// Database number (from URL path)
    pub db: Option<u8>,
    /// Username for authentication
    pub username: Option<String>,
    /// Password for authentication
    pub password: Option<String>,
}

impl RedisUrl {
    /// Parse a Redis URL
    ///
    /// # Supported formats
    /// - `redis://localhost:6379`
    /// - `redis://localhost:6379/0`
    /// - `redis://user:pass@localhost:6379`
    /// - `rediss://localhost:6380` (TLS enabled automatically)
    /// - `unix:///tmp/redis.sock` (Unix domain socket)
    /// - `unix:///tmp/redis.sock?db=0` (Unix socket with database)
    /// - `localhost:6379` (defaults to redis://)
    ///
    /// # Example
    /// ```
    /// use redis_tower::url::RedisUrl;
    ///
    /// let url = RedisUrl::parse("rediss://localhost:6380").unwrap();
    /// assert!(url.tls.is_enabled());
    ///
    /// let unix_url = RedisUrl::parse("unix:///tmp/redis.sock").unwrap();
    /// ```
    pub fn parse(url: &str) -> Result<Self, RedisError> {
        // Handle URLs without scheme
        let url = if !url.contains("://") {
            format!("redis://{}", url)
        } else {
            url.to_string()
        };

        // Parse scheme
        let (scheme, rest) = url
            .split_once("://")
            .ok_or_else(|| RedisError::Connection("Invalid URL format".to_string()))?;

        // Handle Unix socket URLs separately
        if scheme == "unix" {
            return Self::parse_unix_url(rest);
        }

        let use_tls = match scheme {
            "redis" => false,
            "rediss" => true,
            _ => {
                return Err(RedisError::Connection(format!(
                    "Unsupported scheme: {}",
                    scheme
                )));
            }
        };

        // Parse authority and path
        let (authority, path) = if let Some((auth, p)) = rest.split_once('/') {
            (auth, Some(p))
        } else {
            (rest, None)
        };

        // Parse username and password
        let (credentials, host_port) = if let Some((creds, hp)) = authority.split_once('@') {
            (Some(creds), hp)
        } else {
            (None, authority)
        };

        let (username, password) = if let Some(creds) = credentials {
            if let Some((user, pass)) = creds.split_once(':') {
                let username = if user.is_empty() {
                    None
                } else {
                    Some(user.to_string())
                };
                (username, Some(pass.to_string()))
            } else {
                (None, Some(creds.to_string()))
            }
        } else {
            (None, None)
        };

        // Parse host and port
        let (host, port) = if let Some((h, p)) = host_port.split_once(':') {
            let port = p
                .parse::<u16>()
                .map_err(|_| RedisError::Connection(format!("Invalid port: {}", p)))?;
            (h.to_string(), port)
        } else {
            // Default port based on scheme
            let default_port = if use_tls { 6380 } else { 6379 };
            (host_port.to_string(), default_port)
        };

        // Parse database number from path
        let db =
            if let Some(path) = path {
                if !path.is_empty() {
                    Some(path.parse::<u8>().map_err(|_| {
                        RedisError::Connection(format!("Invalid database: {}", path))
                    })?)
                } else {
                    None
                }
            } else {
                None
            };

        // Create TLS config if using rediss://
        let tls = if use_tls {
            #[cfg(feature = "tls-rustls")]
            {
                TlsConfig::rustls().with_native_roots().build()?
            }
            #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
            {
                TlsConfig::native_tls().build()?
            }
            #[cfg(not(any(feature = "tls-rustls", feature = "tls-native-tls")))]
            {
                return Err(RedisError::Connection(
                    "TLS requested but no TLS feature enabled".to_string(),
                ));
            }
        } else {
            TlsConfig::None
        };

        Ok(Self {
            connection: ConnectionType::Tcp { host, port },
            tls,
            db,
            username,
            password,
        })
    }

    /// Parse a Unix socket URL
    ///
    /// Format: unix:///path/to/socket or unix:///path/to/socket?db=0
    fn parse_unix_url(rest: &str) -> Result<Self, RedisError> {
        // Split path and query string
        let (path, query) = if let Some((p, q)) = rest.split_once('?') {
            (p, Some(q))
        } else {
            (rest, None)
        };

        // Path must be absolute
        if !path.starts_with('/') {
            return Err(RedisError::Connection(
                "Unix socket path must be absolute".to_string(),
            ));
        }

        // Parse query parameters
        let mut db = None;
        let mut username = None;
        let mut password = None;

        if let Some(query) = query {
            for param in query.split('&') {
                if let Some((key, value)) = param.split_once('=') {
                    match key {
                        "db" => {
                            db = Some(value.parse::<u8>().map_err(|_| {
                                RedisError::Connection(format!("Invalid database: {}", value))
                            })?);
                        }
                        "user" => {
                            username = Some(value.to_string());
                        }
                        "password" => {
                            password = Some(value.to_string());
                        }
                        _ => {} // Ignore unknown parameters
                    }
                }
            }
        }

        Ok(Self {
            connection: ConnectionType::Unix {
                path: path.to_string(),
            },
            tls: TlsConfig::None, // TLS not applicable for Unix sockets
            db,
            username,
            password,
        })
    }

    /// Get the address string (host:port for TCP, path for Unix)
    pub fn addr(&self) -> String {
        match &self.connection {
            ConnectionType::Tcp { host, port } => format!("{}:{}", host, port),
            ConnectionType::Unix { path } => path.clone(),
        }
    }

    /// Check if this is a Unix socket connection
    pub fn is_unix(&self) -> bool {
        matches!(self.connection, ConnectionType::Unix { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_redis() {
        let url = RedisUrl::parse("redis://localhost:6379").unwrap();
        match &url.connection {
            ConnectionType::Tcp { host, port } => {
                assert_eq!(host, "localhost");
                assert_eq!(*port, 6379);
            }
            _ => panic!("Expected TCP connection"),
        }
        assert!(!url.tls.is_enabled());
        assert_eq!(url.db, None);
        assert_eq!(url.username, None);
        assert_eq!(url.password, None);
    }

    #[test]
    fn test_parse_redis_with_db() {
        let url = RedisUrl::parse("redis://localhost:6379/5").unwrap();
        match &url.connection {
            ConnectionType::Tcp { host, port } => {
                assert_eq!(host, "localhost");
                assert_eq!(*port, 6379);
            }
            _ => panic!("Expected TCP connection"),
        }
        assert_eq!(url.db, Some(5));
    }

    #[test]
    fn test_parse_redis_with_password() {
        let url = RedisUrl::parse("redis://:mypassword@localhost:6379").unwrap();
        match &url.connection {
            ConnectionType::Tcp { host, port } => {
                assert_eq!(host, "localhost");
                assert_eq!(*port, 6379);
            }
            _ => panic!("Expected TCP connection"),
        }
        assert_eq!(url.username, None);
        assert_eq!(url.password, Some("mypassword".to_string()));
    }

    #[test]
    fn test_parse_redis_with_username_password() {
        let url = RedisUrl::parse("redis://user:pass@localhost:6379").unwrap();
        match &url.connection {
            ConnectionType::Tcp { host, port } => {
                assert_eq!(host, "localhost");
                assert_eq!(*port, 6379);
            }
            _ => panic!("Expected TCP connection"),
        }
        assert_eq!(url.username, Some("user".to_string()));
        assert_eq!(url.password, Some("pass".to_string()));
    }

    #[test]
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    fn test_parse_rediss() {
        let url = RedisUrl::parse("rediss://localhost:6380").unwrap();
        match &url.connection {
            ConnectionType::Tcp { host, port } => {
                assert_eq!(host, "localhost");
                assert_eq!(*port, 6380);
            }
            _ => panic!("Expected TCP connection"),
        }
        assert!(url.tls.is_enabled());
    }

    #[test]
    fn test_parse_no_scheme() {
        let url = RedisUrl::parse("localhost:6379").unwrap();
        match &url.connection {
            ConnectionType::Tcp { host, port } => {
                assert_eq!(host, "localhost");
                assert_eq!(*port, 6379);
            }
            _ => panic!("Expected TCP connection"),
        }
        assert!(!url.tls.is_enabled());
    }

    #[test]
    fn test_parse_default_port() {
        let url = RedisUrl::parse("redis://localhost").unwrap();
        match &url.connection {
            ConnectionType::Tcp { host, port } => {
                assert_eq!(host, "localhost");
                assert_eq!(*port, 6379);
            }
            _ => panic!("Expected TCP connection"),
        }
    }

    #[test]
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    fn test_parse_rediss_default_port() {
        let url = RedisUrl::parse("rediss://localhost").unwrap();
        match &url.connection {
            ConnectionType::Tcp { host, port } => {
                assert_eq!(host, "localhost");
                assert_eq!(*port, 6380);
            }
            _ => panic!("Expected TCP connection"),
        }
    }

    #[test]
    fn test_parse_invalid_scheme() {
        let result = RedisUrl::parse("http://localhost:6379");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_port() {
        let result = RedisUrl::parse("redis://localhost:abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_db() {
        let result = RedisUrl::parse("redis://localhost:6379/abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_addr() {
        let url = RedisUrl::parse("redis://localhost:6379").unwrap();
        assert_eq!(url.addr(), "localhost:6379");
    }

    #[test]
    fn test_parse_unix_socket() {
        let url = RedisUrl::parse("unix:///tmp/redis.sock").unwrap();
        match &url.connection {
            ConnectionType::Unix { path } => {
                assert_eq!(path, "/tmp/redis.sock");
            }
            _ => panic!("Expected Unix connection"),
        }
        assert!(!url.tls.is_enabled());
        assert_eq!(url.db, None);
        assert!(url.is_unix());
    }

    #[test]
    fn test_parse_unix_socket_with_db() {
        let url = RedisUrl::parse("unix:///var/run/redis.sock?db=5").unwrap();
        match &url.connection {
            ConnectionType::Unix { path } => {
                assert_eq!(path, "/var/run/redis.sock");
            }
            _ => panic!("Expected Unix connection"),
        }
        assert_eq!(url.db, Some(5));
        assert!(url.is_unix());
    }

    #[test]
    fn test_parse_unix_socket_with_auth() {
        let url = RedisUrl::parse("unix:///tmp/redis.sock?user=admin&password=secret").unwrap();
        match &url.connection {
            ConnectionType::Unix { path } => {
                assert_eq!(path, "/tmp/redis.sock");
            }
            _ => panic!("Expected Unix connection"),
        }
        assert_eq!(url.username, Some("admin".to_string()));
        assert_eq!(url.password, Some("secret".to_string()));
    }

    #[test]
    fn test_parse_unix_socket_addr() {
        let url = RedisUrl::parse("unix:///tmp/redis.sock").unwrap();
        assert_eq!(url.addr(), "/tmp/redis.sock");
    }

    #[test]
    fn test_parse_unix_socket_invalid_relative_path() {
        let result = RedisUrl::parse("unix://tmp/redis.sock");
        assert!(result.is_err());
    }
}
