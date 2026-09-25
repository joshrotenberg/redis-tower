use crate::ProtocolVersion;
use crate::error::RedisError;

#[derive(Debug)]
pub(crate) struct ParsedRedisUrl {
    pub(crate) url: RedisUrl,
    pub(crate) protocol: Option<ProtocolVersion>,
    /// Percent-decoded Unix path bytes. Keeping these separately from the
    /// public string representation lets the connector open every path the
    /// platform accepts without a lossy UTF-8 conversion.
    #[cfg(unix)]
    pub(crate) unix_path: Option<Vec<u8>>,
}

#[derive(Default)]
struct UrlQuery {
    database: Option<u16>,
    username: Option<String>,
    password: Option<String>,
    protocol: Option<ProtocolVersion>,
}

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
    ///
    /// Bracketed IPv6 URL literals retain their brackets so joining this
    /// field with [`port`](Self::port) produces an unambiguous socket address.
    /// Connection setup removes those brackets before passing the host to TLS.
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

    /// Percent-decoded Unix socket path (if `unix` is true).
    ///
    /// This string-only view cannot represent a non-UTF-8 Unix path. The
    /// connection methods still support such paths directly from a URL;
    /// [`parse_redis_url`] returns [`RedisError::InvalidUrl`] rather than
    /// replacing invalid bytes.
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

impl RedisUrl {
    pub(crate) fn tcp_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
    pub(crate) fn tls_server_name(&self) -> &str {
        let Some(ipv6) = self
            .host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
        else {
            return &self.host;
        };
        ipv6.split_once('%')
            .map_or(ipv6, |(address, _scope)| address)
    }
}

/// Parse a Redis URL into connection parameters.
///
/// Supported schemes:
/// - `redis://[user:pass@]host[:port][/db][?protocol=resp2|resp3]`
/// - `rediss://[user:pass@]host[:port][/db][?protocol=resp2|resp3]` (TLS)
/// - `valkey://` / `valkeys://` -- aliases for `redis://` / `rediss://`
/// - `{unix|redis+unix|valkey+unix}:///path/to/socket` with optional query
///   parameters `user`, `pass`, `db`, and `protocol`
///
/// Unix socket paths use URL path encoding, so `%20` is a space while `+`
/// remains a literal plus. Unix query values use form encoding: both `%20`
/// and `+` are spaces, and a literal plus is `%2B`. Query parameters may
/// appear in any order; when repeated, the final value wins. Unknown query
/// parameters are ignored for compatibility with other Redis clients.
/// `user` requires `pass`; `pass` without `user` uses Redis's legacy
/// password-only `AUTH` form.
///
/// `protocol` accepts `2`, `resp2`, `3`, or `resp3`. It is consumed by the
/// connection methods and is therefore not represented in [`RedisUrl`]. An
/// explicit [`ProtocolVersion`](crate::ProtocolVersion) in
/// [`ConnectionConfig`](crate::ConnectionConfig) takes precedence; otherwise
/// the URL value replaces automatic negotiation.
///
/// IPv6 literals use URL brackets, for example `rediss://[::1]:6380/0`.
/// [`RedisUrl::host`] retains the brackets for socket-address formatting;
/// connection setup removes them before TLS server-name validation.
///
/// The username and password are percent-decoded (managed-service passwords
/// routinely contain URL-special characters such as `@`, `:`, and `/`, which
/// must be percent-encoded to appear in a URL). See [`percent_decode`] for
/// the exact decoding rules.
pub fn parse_redis_url(url: &str) -> Result<RedisUrl, RedisError> {
    let parsed = parse_connection_url(url)?;
    if parsed.url.unix && parsed.url.path.is_none() {
        return Err(RedisError::InvalidUrl(
            "percent-decoded Unix socket path is not valid UTF-8; use RedisConnection::connect_url to connect without a lossy conversion"
                .to_string(),
        ));
    }
    Ok(parsed.url)
}

pub(crate) fn parse_connection_url(url: &str) -> Result<ParsedRedisUrl, RedisError> {
    let unix_rest = ["unix://", "redis+unix://", "valkey+unix://"]
        .into_iter()
        .find_map(|scheme| url.strip_prefix(scheme));
    if let Some(rest) = unix_rest {
        let (encoded_path, query) = split_query(rest);
        if encoded_path.is_empty() {
            return Err(RedisError::InvalidUrl(
                "unix URL missing socket path".to_string(),
            ));
        }
        let path_bytes = percent_decode_bytes(encoded_path);
        if path_bytes.is_empty() {
            return Err(RedisError::InvalidUrl(
                "unix URL missing socket path".to_string(),
            ));
        }
        let query = parse_query(query, true)?;
        if query.username.is_some() && query.password.is_none() {
            return Err(RedisError::InvalidUrl(
                "unix URL user parameter requires a pass parameter".to_string(),
            ));
        }
        let path = String::from_utf8(path_bytes.clone()).ok();

        return Ok(ParsedRedisUrl {
            url: RedisUrl {
                username: query.username,
                password: query.password,
                database: query.database,
                unix: true,
                path,
                ..Default::default()
            },
            protocol: query.protocol,
            #[cfg(unix)]
            unix_path: Some(path_bytes),
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
            "expected redis://, rediss://, valkey://, valkeys://, unix://, redis+unix://, or valkey+unix:// scheme".into(),
        ));
    };

    let (rest, query) = split_query(rest);
    let query = parse_query(query, false)?;

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

    let (host, port) = parse_host_port(host_port)?;

    let database = db_str
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<u16>()
                .map_err(|_| RedisError::InvalidUrl(format!("invalid database: {s}")))
        })
        .transpose()?;

    Ok(ParsedRedisUrl {
        url: RedisUrl {
            host,
            port,
            username,
            password,
            database,
            tls,
            unix: false,
            path: None,
        },
        protocol: query.protocol,
        #[cfg(unix)]
        unix_path: None,
    })
}

fn split_query(input: &str) -> (&str, Option<&str>) {
    input
        .split_once('?')
        .map_or((input, None), |(path, query)| (path, Some(query)))
}

fn parse_query(query: Option<&str>, unix: bool) -> Result<UrlQuery, RedisError> {
    let mut parsed = UrlQuery::default();
    for pair in query.into_iter().flat_map(|query| query.split('&')) {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = form_decode(key)?;
        let value = form_decode(value)?;
        match key.as_str() {
            "db" if unix => {
                parsed.database =
                    Some(value.parse::<u16>().map_err(|_| {
                        RedisError::InvalidUrl(format!("invalid database: {value}"))
                    })?);
            }
            "user" if unix => parsed.username = Some(value),
            "pass" if unix => parsed.password = Some(value),
            "protocol" => {
                parsed.protocol = Some(match value.as_str() {
                    "2" | "resp2" => ProtocolVersion::Resp2,
                    "3" | "resp3" => ProtocolVersion::Resp3,
                    _ => {
                        return Err(RedisError::InvalidUrl(format!(
                            "invalid protocol version: {value}"
                        )));
                    }
                });
            }
            _ => {}
        }
    }
    Ok(parsed)
}

fn form_decode(input: &str) -> Result<String, RedisError> {
    let replaced;
    let input = if input.contains('+') {
        replaced = input.replace('+', " ");
        &replaced
    } else {
        input
    };
    String::from_utf8(percent_decode_bytes(input)).map_err(|_| {
        RedisError::InvalidUrl("percent-decoded URL query is not valid UTF-8".to_string())
    })
}

fn parse_host_port(host_port: &str) -> Result<(String, u16), RedisError> {
    if let Some(ipv6) = host_port.strip_prefix('[') {
        let close = ipv6
            .find(']')
            .ok_or_else(|| RedisError::InvalidUrl("unterminated IPv6 address".to_string()))?;
        if close == 0 {
            return Err(RedisError::InvalidUrl("empty IPv6 address".to_string()));
        }
        format!("[{}]:0", &ipv6[..close])
            .parse::<std::net::SocketAddrV6>()
            .map_err(|_| {
                RedisError::InvalidUrl(format!("invalid IPv6 address: {}", &ipv6[..close]))
            })?;

        // Keep brackets on the parsed host: `host:port` remains a valid
        // socket address, while `RedisUrl::tls_server_name` supplies the
        // unbracketed literal required by TLS libraries.
        let bracketed_host = &host_port[..close + 2];
        let suffix = &ipv6[close + 1..];
        let port = match suffix.strip_prefix(':') {
            Some(port) => parse_port(port)?,
            None if suffix.is_empty() => 6379,
            None => {
                return Err(RedisError::InvalidUrl(
                    "unexpected characters after IPv6 address".to_string(),
                ));
            }
        };
        return Ok((bracketed_host.to_string(), port));
    }

    if host_port.matches(':').count() > 1 {
        return Err(RedisError::InvalidUrl(
            "IPv6 addresses in Redis URLs must be enclosed in brackets".to_string(),
        ));
    }

    if let Some((host, port)) = host_port.rsplit_once(':') {
        Ok((host.to_string(), parse_port(port)?))
    } else {
        Ok((host_port.to_string(), 6379))
    }
}

fn parse_port(port: &str) -> Result<u16, RedisError> {
    port.parse::<u16>()
        .map_err(|_| RedisError::InvalidUrl(format!("invalid port: {port}")))
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
    String::from_utf8(percent_decode_bytes(input)).map_err(|_| {
        RedisError::InvalidUrl("percent-decoded URL component is not valid UTF-8".to_string())
    })
}

fn percent_decode_bytes(input: &str) -> Vec<u8> {
    if !input.contains('%') {
        return input.as_bytes().to_vec();
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
    out
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
    fn parse_tls_ipv6_urls_separate_socket_and_server_names() {
        for (input, expected_port) in [
            ("rediss://[::1]/", 6379),
            ("rediss://[::1]:6380/", 6380),
            ("valkeys://[::1]/", 6379),
            ("valkeys://[::1]:6380/", 6380),
        ] {
            let url = parse_redis_url(input).unwrap();
            assert!(url.tls, "{input}");
            assert_eq!(url.host, "[::1]", "{input}");
            assert_eq!(url.port, expected_port, "{input}");
            assert_eq!(url.tcp_addr(), format!("[::1]:{expected_port}"), "{input}");
            #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
            assert_eq!(url.tls_server_name(), "::1", "{input}");
        }
    }

    #[test]
    fn tcp_targets_preserve_hostname_and_ipv4_behavior() {
        for (input, host, address) in [
            (
                "rediss://redis.example.com:6380",
                "redis.example.com",
                "redis.example.com:6380",
            ),
            ("rediss://127.0.0.1:6380", "127.0.0.1", "127.0.0.1:6380"),
            (
                "rediss://redis%2Eexample:6380",
                "redis%2Eexample",
                "redis%2Eexample:6380",
            ),
        ] {
            let url = parse_redis_url(input).unwrap();
            assert_eq!(url.host, host);
            assert_eq!(url.tcp_addr(), address);
            #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
            assert_eq!(url.tls_server_name(), host);
        }
    }

    #[test]
    fn malformed_ipv6_authorities_are_rejected() {
        for input in [
            "rediss://[::1",
            "rediss://[]",
            "rediss://[::1]extra",
            "rediss://[::1]:not-a-port",
            "rediss://[not-ipv6]",
            "rediss://::1",
        ] {
            assert!(parse_redis_url(input).is_err(), "accepted {input}");
        }
    }

    #[test]
    fn numeric_ipv6_scope_is_kept_for_tcp_and_removed_for_tls_identity() {
        let url = parse_redis_url("rediss://[fe80::1%3]:6380/").unwrap();
        assert_eq!(url.host, "[fe80::1%3]");
        assert_eq!(url.tcp_addr(), "[fe80::1%3]:6380");
        #[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
        assert_eq!(url.tls_server_name(), "fe80::1");
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
    fn parse_unix_aliases_decode_paths_and_setup_query() {
        for scheme in ["unix", "redis+unix", "valkey+unix"] {
            let input = format!(
                "{scheme}:///tmp/redis%20socket+name.sock?pass=%26%3F%3D+%2A%2B&db=2&user=%25agent%25&protocol=resp3"
            );
            let parsed = parse_connection_url(&input).unwrap();

            assert!(parsed.url.unix, "{scheme}");
            assert_eq!(
                parsed.url.path.as_deref(),
                Some("/tmp/redis socket+name.sock"),
                "{scheme}"
            );
            #[cfg(unix)]
            assert_eq!(
                parsed.unix_path.as_deref(),
                Some(&b"/tmp/redis socket+name.sock"[..]),
                "{scheme}"
            );
            assert_eq!(parsed.url.username.as_deref(), Some("%agent%"), "{scheme}");
            assert_eq!(parsed.url.password.as_deref(), Some("&?= *+"), "{scheme}");
            assert_eq!(parsed.url.database, Some(2), "{scheme}");
            assert_eq!(parsed.protocol, Some(ProtocolVersion::Resp3), "{scheme}");
        }
    }

    #[test]
    fn url_protocol_accepts_redis_rs_spellings_and_last_value_wins() {
        for (value, expected) in [
            ("2", ProtocolVersion::Resp2),
            ("resp2", ProtocolVersion::Resp2),
            ("3", ProtocolVersion::Resp3),
            ("resp3", ProtocolVersion::Resp3),
        ] {
            let parsed =
                parse_connection_url(&format!("redis://localhost/?protocol={value}")).unwrap();
            assert_eq!(parsed.protocol, Some(expected), "{value}");
        }

        let parsed =
            parse_connection_url("unix:///tmp/redis.sock?db=1&db=2&protocol=resp2&protocol=resp3")
                .unwrap();
        assert_eq!(parsed.url.database, Some(2));
        assert_eq!(parsed.protocol, Some(ProtocolVersion::Resp3));
    }

    #[test]
    fn unix_url_rejects_incomplete_or_invalid_setup() {
        for input in [
            "unix://",
            "redis+unix://?db=1",
            "unix:///tmp/redis.sock?db=notanumber",
            "unix:///tmp/redis.sock?user=agent",
            "unix:///tmp/redis.sock?protocol=4",
            "redis://localhost/?protocol=auto",
        ] {
            assert!(parse_redis_url(input).is_err(), "accepted {input}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn connection_parser_preserves_non_utf8_unix_path_bytes() {
        let parsed = parse_connection_url("unix:///tmp/redis-%FF.sock").unwrap();
        assert_eq!(
            parsed.unix_path.as_deref(),
            Some(&b"/tmp/redis-\xff.sock"[..])
        );
        assert!(parsed.url.path.is_none());
        assert!(parse_redis_url("unix:///tmp/redis-%FF.sock").is_err());
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
    fn parse_unix_with_invalid_db_is_rejected() {
        assert!(parse_redis_url("unix:///tmp/redis.sock?db=notanumber").is_err());
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
