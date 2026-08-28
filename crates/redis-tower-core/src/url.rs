use crate::error::RedisError;

/// Parsed Redis connection URL.
///
/// Produced by [`parse_redis_url`]. Contains all fields needed to establish
/// and authenticate a Redis connection.
///
/// # Example
///
/// ```no_run
/// use redis_tower_core::parse_redis_url;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let url = parse_redis_url("redis://user:pass@myhost:6380/2")?;
/// assert_eq!(url.host, "myhost");
/// assert_eq!(url.port, 6380);
/// assert_eq!(url.database, Some(2));
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct RedisUrl {
    /// Host to connect to.
    pub host: String,

    /// Port number (default: 6379).
    pub port: u16,

    /// Username for AUTH (Redis 6+ ACL). Percent-decoded.
    pub username: Option<String>,

    /// Password for AUTH. Percent-decoded.
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
///
/// The username and password are percent-decoded (managed-service passwords
/// routinely contain URL-special characters such as `@`, `:`, and `/`, which
/// must be percent-encoded to appear in a URL). See [`percent_decode`] for
/// the exact decoding rules.
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
                Some(percent_decode(user)?)
            };
            (user, Some(percent_decode(pass)?))
        } else {
            (None, Some(percent_decode(auth)?))
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

/// Decode percent-encoded (`%XX`) sequences in a URL component.
///
/// A `%` followed by two hex digits is decoded to the corresponding byte. A
/// `%` **not** followed by two hex digits is preserved as-is (lenient
/// decoding, matching the behavior of the `percent-encoding` crate used by
/// most URL libraries), so a legacy password containing a literal `%` keeps
/// working.
///
/// # Errors
///
/// Returns [`RedisError::InvalidUrl`] if the decoded bytes are not valid
/// UTF-8.
pub fn percent_decode(input: &str) -> Result<String, RedisError> {
    if !input.contains('%') {
        return Ok(input.to_string());
    }
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_value(bytes[i + 1]), hex_value(bytes[i + 2]))
        {
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| {
        RedisError::InvalidUrl("percent-decoded URL component is not valid UTF-8".to_string())
    })
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
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
        let url = parse_redis_url("redis://:p%40ss%3Aw0rd@localhost").unwrap();
        assert_eq!(url.password.as_deref(), Some("p@ss:w0rd"));
        assert!(url.username.is_none());
    }

    #[test]
    fn parse_percent_encoded_username_and_password() {
        let url = parse_redis_url("redis://us%2Fer:pa%25ss@localhost").unwrap();
        assert_eq!(url.username.as_deref(), Some("us/er"));
        assert_eq!(url.password.as_deref(), Some("pa%ss"));
    }

    #[test]
    fn parse_bare_token_is_percent_decoded() {
        let url = parse_redis_url("redis://tok%2Ben@localhost").unwrap();
        assert!(url.username.is_none());
        assert_eq!(url.password.as_deref(), Some("tok+en"));
    }

    #[test]
    fn parse_literal_percent_without_hex_is_preserved() {
        // Lenient decoding: `%` not followed by two hex digits passes through
        // raw, so an unencoded legacy password containing `%` keeps working.
        let url = parse_redis_url("redis://:100%pass@localhost").unwrap();
        assert_eq!(url.password.as_deref(), Some("100%pass"));
    }

    #[test]
    fn parse_percent_decoded_invalid_utf8_is_rejected() {
        // %FF alone is not valid UTF-8 after decoding.
        assert!(parse_redis_url("redis://:%FF@localhost").is_err());
    }

    #[test]
    fn percent_decode_plain_string_unchanged() {
        assert_eq!(percent_decode("plain").unwrap(), "plain");
    }

    #[test]
    fn percent_decode_truncated_escape_preserved() {
        assert_eq!(percent_decode("abc%2").unwrap(), "abc%2");
        assert_eq!(percent_decode("abc%").unwrap(), "abc%");
    }

    #[test]
    fn percent_decode_multibyte_utf8() {
        // "café" with the é percent-encoded as UTF-8 (0xC3 0xA9).
        assert_eq!(percent_decode("caf%C3%A9").unwrap(), "café");
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
