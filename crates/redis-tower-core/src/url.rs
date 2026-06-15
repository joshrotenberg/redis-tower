use crate::error::RedisError;

/// Parsed Redis connection URL.
///
/// Produced by [`parse_redis_url`]. Contains all fields needed to establish
/// and authenticate a Redis connection.
///
/// # Example
///
/// ```ignore
/// use redis_tower_core::parse_redis_url;
///
/// let url = parse_redis_url("redis://user:pass@myhost:6380/2")?;
/// assert_eq!(url.host, "myhost");
/// assert_eq!(url.port, 6380);
/// assert_eq!(url.database, Some(2));
/// ```
#[derive(Debug, Clone)]
pub struct RedisUrl {
    /// Host to connect to.
    pub host: String,

    /// Port number (default: 6379).
    pub port: u16,

    /// Username for AUTH (Redis 6+ ACL).
    pub username: Option<String>,

    /// Password for AUTH.
    pub password: Option<String>,

    /// Database number for SELECT.
    pub database: Option<u16>,

    /// Whether TLS is required (`rediss://`).
    pub tls: bool,

    /// Whether this is a Unix socket connection.
    pub unix: bool,

    /// Unix socket path (if `unix` is true).
    pub path: Option<String>,
}

impl Default for RedisUrl {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            username: None,
            password: None,
            database: None,
            tls: false,
            unix: false,
            path: None,
        }
    }
}

/// Parse a Redis URL into connection parameters.
///
/// Supported schemes:
/// - `redis://[user:pass@]host[:port][/db]`
/// - `rediss://[user:pass@]host[:port][/db]` (TLS)
/// - `valkey://` / `valkeys://` -- aliases for `redis://` / `rediss://`
/// - `unix:///path/to/socket[?db=N]`
pub fn parse_redis_url(url: &str) -> Result<RedisUrl, RedisError> {
    if url.starts_with("unix://") {
        let path = url.strip_prefix("unix://").unwrap();
        let (path, db) = if let Some((p, query)) = path.split_once('?') {
            let db = query
                .strip_prefix("db=")
                .and_then(|d| d.parse::<u16>().ok());
            (p, db)
        } else {
            (path, None)
        };

        return Ok(RedisUrl {
            unix: true,
            path: Some(path.to_string()),
            database: db,
            ..Default::default()
        });
    }

    // `valkey://` / `valkeys://` are accepted as aliases for `redis://` /
    // `rediss://` -- Valkey speaks the same protocol on the same schemes.
    let (tls, rest) = if let Some(rest) = url.strip_prefix("rediss://") {
        (true, rest)
    } else if let Some(rest) = url.strip_prefix("redis://") {
        (false, rest)
    } else if let Some(rest) = url.strip_prefix("valkeys://") {
        (true, rest)
    } else if let Some(rest) = url.strip_prefix("valkey://") {
        (false, rest)
    } else {
        return Err(RedisError::InvalidUrl(
            "expected redis://, rediss://, valkey://, valkeys://, or unix:// scheme".into(),
        ));
    };

    let (auth, host_part) = if let Some((auth, rest)) = rest.split_once('@') {
        (Some(auth), rest)
    } else {
        (None, rest)
    };

    let (username, password) = if let Some(auth) = auth {
        if let Some((user, pass)) = auth.split_once(':') {
            let user = if user.is_empty() {
                None
            } else {
                Some(user.to_string())
            };
            (user, Some(pass.to_string()))
        } else {
            (None, Some(auth.to_string()))
        }
    } else {
        (None, None)
    };

    let (host_port, db_str) = if let Some((hp, db)) = host_part.split_once('/') {
        (hp, Some(db))
    } else {
        (host_part, None)
    };

    let (host, port) = if let Some((h, p)) = host_port.rsplit_once(':') {
        let port = p
            .parse::<u16>()
            .map_err(|_| RedisError::InvalidUrl(format!("invalid port: {p}")))?;
        (h.to_string(), port)
    } else {
        (host_port.to_string(), 6379)
    };

    let database = db_str
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<u16>()
                .map_err(|_| RedisError::InvalidUrl(format!("invalid database: {s}")))
        })
        .transpose()?;

    Ok(RedisUrl {
        host,
        port,
        username,
        password,
        database,
        tls,
        unix: false,
        path: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple() {
        let url = parse_redis_url("redis://localhost").unwrap();
        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, 6379);
        assert!(!url.tls);
    }

    #[test]
    fn parse_with_port() {
        let url = parse_redis_url("redis://localhost:6380").unwrap();
        assert_eq!(url.port, 6380);
    }

    #[test]
    fn parse_with_auth() {
        let url = parse_redis_url("redis://user:pass@localhost/2").unwrap();
        assert_eq!(url.username.as_deref(), Some("user"));
        assert_eq!(url.password.as_deref(), Some("pass"));
        assert_eq!(url.database, Some(2));
    }

    #[test]
    fn parse_tls() {
        let url = parse_redis_url("rediss://host:6380").unwrap();
        assert!(url.tls);
    }

    #[test]
    fn parse_valkey_scheme() {
        // valkey:// is a plaintext alias for redis://
        let url = parse_redis_url("valkey://user:pass@localhost:6380/2").unwrap();
        assert_eq!(url.host, "localhost");
        assert_eq!(url.port, 6380);
        assert_eq!(url.database, Some(2));
        assert_eq!(url.username.as_deref(), Some("user"));
        assert!(!url.tls);
    }

    #[test]
    fn parse_valkeys_scheme_is_tls() {
        // valkeys:// is the TLS alias for rediss://
        let url = parse_redis_url("valkeys://host:6380").unwrap();
        assert!(url.tls);
        assert_eq!(url.host, "host");
        assert_eq!(url.port, 6380);
    }

    #[test]
    fn parse_password_only() {
        let url = parse_redis_url("redis://:secret@localhost").unwrap();
        assert!(url.username.is_none());
        assert_eq!(url.password.as_deref(), Some("secret"));
    }

    #[test]
    fn parse_unix() {
        let url = parse_redis_url("unix:///var/run/redis.sock?db=3").unwrap();
        assert!(url.unix);
        assert_eq!(url.path.as_deref(), Some("/var/run/redis.sock"));
        assert_eq!(url.database, Some(3));
    }

    #[test]
    fn parse_invalid_scheme() {
        assert!(parse_redis_url("http://localhost").is_err());
    }

    // -- Edge cases --

    #[test]
    fn parse_empty_url() {
        assert!(parse_redis_url("").is_err());
    }

    #[test]
    fn parse_host_only_no_port_defaults_6379() {
        let url = parse_redis_url("redis://myhost").unwrap();
        assert_eq!(url.host, "myhost");
        assert_eq!(url.port, 6379);
    }

    #[test]
    fn parse_password_with_special_characters() {
        let url = parse_redis_url("redis://:p%40ss:w0rd@localhost").unwrap();
        // The parser does not percent-decode, so it preserves the raw value.
        assert_eq!(url.password.as_deref(), Some("p%40ss:w0rd"));
        assert!(url.username.is_none());
    }

    #[test]
    fn parse_rediss_sets_tls_flag() {
        let url = parse_redis_url("rediss://secure.host:6380/1").unwrap();
        assert!(url.tls);
        assert_eq!(url.host, "secure.host");
        assert_eq!(url.port, 6380);
        assert_eq!(url.database, Some(1));
    }

    #[test]
    fn parse_unix_with_db_parameter() {
        let url = parse_redis_url("unix:///tmp/redis.sock?db=5").unwrap();
        assert!(url.unix);
        assert_eq!(url.path.as_deref(), Some("/tmp/redis.sock"));
        assert_eq!(url.database, Some(5));
    }

    #[test]
    fn parse_unix_without_db() {
        let url = parse_redis_url("unix:///tmp/redis.sock").unwrap();
        assert!(url.unix);
        assert_eq!(url.path.as_deref(), Some("/tmp/redis.sock"));
        assert_eq!(url.database, None);
    }

    #[test]
    fn parse_unix_with_invalid_db_ignored() {
        let url = parse_redis_url("unix:///tmp/redis.sock?db=notanumber").unwrap();
        assert!(url.unix);
        assert_eq!(url.database, None);
    }

    #[test]
    fn parse_with_database_zero() {
        let url = parse_redis_url("redis://localhost/0").unwrap();
        assert_eq!(url.database, Some(0));
    }

    #[test]
    fn parse_trailing_slash_no_database() {
        let url = parse_redis_url("redis://localhost/").unwrap();
        assert_eq!(url.database, None);
    }

    #[test]
    fn parse_invalid_port() {
        assert!(parse_redis_url("redis://localhost:notaport").is_err());
    }

    #[test]
    fn parse_invalid_database() {
        assert!(parse_redis_url("redis://localhost/notadb").is_err());
    }

    #[test]
    fn parse_with_username_and_password() {
        let url = parse_redis_url("redis://admin:secret@localhost:6379/3").unwrap();
        assert_eq!(url.username.as_deref(), Some("admin"));
        assert_eq!(url.password.as_deref(), Some("secret"));
        assert_eq!(url.database, Some(3));
    }

    #[test]
    fn parse_auth_token_without_colon() {
        // redis://token@host -- treated as password-only (no colon separator)
        let url = parse_redis_url("redis://mytoken@localhost").unwrap();
        assert!(url.username.is_none());
        assert_eq!(url.password.as_deref(), Some("mytoken"));
    }

    #[test]
    fn default_redis_url() {
        let url = RedisUrl::default();
        assert_eq!(url.host, "127.0.0.1");
        assert_eq!(url.port, 6379);
        assert!(!url.tls);
        assert!(!url.unix);
        assert!(url.username.is_none());
        assert!(url.password.is_none());
        assert!(url.database.is_none());
        assert!(url.path.is_none());
    }
}
