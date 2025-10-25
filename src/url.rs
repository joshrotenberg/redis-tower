//! URL parsing for Redis connection strings
//!
//! Supports both `redis://` and `rediss://` schemes.

use crate::tls::TlsConfig;
use crate::types::RedisError;

/// Parsed Redis URL
#[derive(Debug, Clone)]
pub struct RedisUrl {
    /// The host to connect to
    pub host: String,
    /// The port to connect to
    pub port: u16,
    /// TLS configuration (automatic for rediss://)
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
    /// - `localhost:6379` (defaults to redis://)
    ///
    /// # Example
    /// ```
    /// use redis_tower::url::RedisUrl;
    ///
    /// let url = RedisUrl::parse("rediss://localhost:6380").unwrap();
    /// assert!(url.tls.is_enabled());
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
            host,
            port,
            tls,
            db,
            username,
            password,
        })
    }

    /// Get the address string (host:port)
    pub fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_redis() {
        let url = RedisUrl::parse("redis://localhost:6379").unwrap();
        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, 6379);
        assert!(!url.tls.is_enabled());
        assert_eq!(url.db, None);
        assert_eq!(url.username, None);
        assert_eq!(url.password, None);
    }

    #[test]
    fn test_parse_redis_with_db() {
        let url = RedisUrl::parse("redis://localhost:6379/5").unwrap();
        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, 6379);
        assert_eq!(url.db, Some(5));
    }

    #[test]
    fn test_parse_redis_with_password() {
        let url = RedisUrl::parse("redis://:mypassword@localhost:6379").unwrap();
        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, 6379);
        assert_eq!(url.username, None);
        assert_eq!(url.password, Some("mypassword".to_string()));
    }

    #[test]
    fn test_parse_redis_with_username_password() {
        let url = RedisUrl::parse("redis://user:pass@localhost:6379").unwrap();
        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, 6379);
        assert_eq!(url.username, Some("user".to_string()));
        assert_eq!(url.password, Some("pass".to_string()));
    }

    #[test]
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    fn test_parse_rediss() {
        let url = RedisUrl::parse("rediss://localhost:6380").unwrap();
        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, 6380);
        assert!(url.tls.is_enabled());
    }

    #[test]
    fn test_parse_no_scheme() {
        let url = RedisUrl::parse("localhost:6379").unwrap();
        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, 6379);
        assert!(!url.tls.is_enabled());
    }

    #[test]
    fn test_parse_default_port() {
        let url = RedisUrl::parse("redis://localhost").unwrap();
        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, 6379);
    }

    #[test]
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    fn test_parse_rediss_default_port() {
        let url = RedisUrl::parse("rediss://localhost").unwrap();
        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, 6380);
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
}
