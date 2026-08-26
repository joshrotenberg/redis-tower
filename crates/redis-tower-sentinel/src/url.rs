//! `redis+sentinel://` URL parsing.
//!
//! Shared by [`MultiplexedSentinelClient::connect_url`] and (through it)
//! `redis-tower-client`'s `UniversalClient::connect_url`.
//!
//! [`MultiplexedSentinelClient::connect_url`]: crate::MultiplexedSentinelClient::connect_url

use redis_tower::credentials::StaticCredentials;
use redis_tower_core::{RedisError, percent_decode};

/// Default port for sentinel nodes when a URL host omits one.
const DEFAULT_SENTINEL_PORT: u16 = 26379;

/// Parsed `redis+sentinel://` / `rediss+sentinel://` URL.
#[derive(Debug)]
pub(crate) struct SentinelUrl {
    /// Sentinel `host:port` addresses.
    pub(crate) sentinel_addrs: Vec<String>,
    /// Name of the monitored master.
    pub(crate) master_name: String,
    /// Credentials for the Redis data nodes (from the URL userinfo).
    pub(crate) node_credentials: Option<StaticCredentials>,
    /// Credentials for the sentinel nodes (from the query parameters).
    pub(crate) sentinel_credentials: Option<StaticCredentials>,
    /// Whether TLS is required (`rediss+sentinel://`), for both hops.
    pub(crate) tls: bool,
}

/// Parse a sentinel URL:
///
/// ```text
/// redis+sentinel://[user:pass@]host1[:port1],host2[:port2]/master-name
///     [?sentinel_username=U&sentinel_password=P]
/// rediss+sentinel://...    (TLS for both sentinel and node connections)
/// ```
///
/// The userinfo (`user:pass@`) authenticates the **data nodes**; sentinels
/// commonly run without auth or with separate credentials, which go in the
/// `sentinel_username` / `sentinel_password` query parameters. All credential
/// components and the master name are percent-decoded. Hosts without an
/// explicit port default to 26379.
pub(crate) fn parse_sentinel_url(url: &str) -> Result<SentinelUrl, RedisError> {
    let (tls, rest) = if let Some(rest) = url.strip_prefix("rediss+sentinel://") {
        (true, rest)
    } else if let Some(rest) = url.strip_prefix("redis+sentinel://") {
        (false, rest)
    } else {
        return Err(RedisError::InvalidUrl(
            "expected redis+sentinel:// or rediss+sentinel:// scheme".into(),
        ));
    };

    let (rest, query) = match rest.split_once('?') {
        Some((rest, query)) => (rest, Some(query)),
        None => (rest, None),
    };

    let (auth, rest) = match rest.split_once('@') {
        Some((auth, rest)) => (Some(auth), rest),
        None => (None, rest),
    };

    let node_credentials = auth.map(parse_userinfo).transpose()?.flatten();

    let (hosts, master_name) = rest.split_once('/').ok_or_else(|| {
        RedisError::InvalidUrl(
            "redis+sentinel URL requires a master name: redis+sentinel://h1,h2/master".into(),
        )
    })?;

    let sentinel_addrs: Vec<String> = hosts
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|addr| {
            if addr.contains(':') {
                addr.to_string()
            } else {
                format!("{addr}:{DEFAULT_SENTINEL_PORT}")
            }
        })
        .collect();
    if sentinel_addrs.is_empty() {
        return Err(RedisError::InvalidUrl(
            "redis+sentinel URL requires at least one sentinel host".into(),
        ));
    }

    let master_name = percent_decode(master_name)?;
    if master_name.is_empty() {
        return Err(RedisError::InvalidUrl(
            "redis+sentinel URL requires a master name after the '/'".into(),
        ));
    }

    let sentinel_credentials = parse_sentinel_query(query)?;

    Ok(SentinelUrl {
        sentinel_addrs,
        master_name,
        node_credentials,
        sentinel_credentials,
        tls,
    })
}

/// Parse a URL userinfo component into credentials, using the same rules as
/// the standalone and cluster URL parsers: `user:pass` is an ACL login,
/// `:pass` or a bare token is a legacy `requirepass` password.
fn parse_userinfo(auth: &str) -> Result<Option<StaticCredentials>, RedisError> {
    let credentials = if let Some((user, pass)) = auth.split_once(':') {
        let pass = percent_decode(pass)?;
        if user.is_empty() {
            Some(StaticCredentials::password(pass))
        } else {
            Some(StaticCredentials::new(percent_decode(user)?, pass))
        }
    } else if auth.is_empty() {
        None
    } else {
        Some(StaticCredentials::password(percent_decode(auth)?))
    };
    Ok(credentials)
}

/// Parse the query string into sentinel credentials. Only
/// `sentinel_username` and `sentinel_password` are recognized; any other key
/// is an error so a typo cannot silently drop authentication.
fn parse_sentinel_query(query: Option<&str>) -> Result<Option<StaticCredentials>, RedisError> {
    let Some(query) = query else {
        return Ok(None);
    };

    let mut username: Option<String> = None;
    let mut password: Option<String> = None;
    for pair in query.split('&').filter(|s| !s.is_empty()) {
        let (key, value) = pair.split_once('=').ok_or_else(|| {
            RedisError::InvalidUrl(format!("query parameter `{pair}` is missing a value"))
        })?;
        match key {
            "sentinel_username" => username = Some(percent_decode(value)?),
            "sentinel_password" => password = Some(percent_decode(value)?),
            other => {
                return Err(RedisError::InvalidUrl(format!(
                    "unsupported sentinel URL query parameter `{other}` \
                     (expected sentinel_username or sentinel_password)"
                )));
            }
        }
    }

    match (username, password) {
        (None, None) => Ok(None),
        (None, Some(pass)) => Ok(Some(StaticCredentials::password(pass))),
        (Some(user), Some(pass)) => Ok(Some(StaticCredentials::new(user, pass))),
        (Some(_), None) => Err(RedisError::InvalidUrl(
            "sentinel_username requires sentinel_password".into(),
        )),
    }
}

/// Build the default TLS config for a `rediss+sentinel://` URL: rustls when
/// available (validating against the system roots with a webpki-roots
/// fallback), otherwise native-tls. Mirrors the cluster crate's URL TLS
/// default; for a custom config, use the builder's `.tls()` /
/// `.sentinel_tls()` / `.node_tls()`.
#[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
pub(crate) fn default_url_tls() -> redis_tower_core::tls::TlsConfig {
    #[cfg(feature = "tls-rustls")]
    {
        redis_tower_core::tls::TlsConfig::default_rustls()
    }
    #[cfg(all(not(feature = "tls-rustls"), feature = "tls-native-tls"))]
    {
        redis_tower_core::tls::TlsConfig::default_native_tls()
    }
}

/// Error for a `rediss+sentinel://` URL when no TLS feature is enabled.
#[cfg(not(any(feature = "tls-rustls", feature = "tls-native-tls")))]
pub(crate) fn tls_feature_required() -> RedisError {
    RedisError::InvalidUrl(
        "rediss+sentinel:// requires the `tls-rustls` or `tls-native-tls` feature".to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower::credentials::CredentialProvider;

    async fn creds(provider: &StaticCredentials) -> (Option<String>, String) {
        let c = provider.get_credentials().await.unwrap();
        (
            c.username().map(str::to_string),
            c.password_value().to_string(),
        )
    }

    #[test]
    fn parse_minimal() {
        let parsed = parse_sentinel_url("redis+sentinel://127.0.0.1:26379/mymaster").unwrap();
        assert_eq!(parsed.sentinel_addrs, vec!["127.0.0.1:26379"]);
        assert_eq!(parsed.master_name, "mymaster");
        assert!(parsed.node_credentials.is_none());
        assert!(parsed.sentinel_credentials.is_none());
        assert!(!parsed.tls);
    }

    #[test]
    fn parse_multiple_hosts_with_default_port() {
        let parsed = parse_sentinel_url("redis+sentinel://s1,s2:26380,s3/mymaster").unwrap();
        assert_eq!(
            parsed.sentinel_addrs,
            vec!["s1:26379", "s2:26380", "s3:26379"]
        );
    }

    #[tokio::test]
    async fn parse_node_credentials_from_userinfo() {
        let parsed =
            parse_sentinel_url("redis+sentinel://alice:s%40cret@s1:26379/mymaster").unwrap();
        let (user, pass) = creds(parsed.node_credentials.as_ref().unwrap()).await;
        assert_eq!(user.as_deref(), Some("alice"));
        assert_eq!(pass, "s@cret");
        assert!(parsed.sentinel_credentials.is_none());
    }

    #[tokio::test]
    async fn parse_password_only_userinfo_is_legacy_auth() {
        let parsed = parse_sentinel_url("redis+sentinel://:hunter2@s1/mymaster").unwrap();
        let (user, pass) = creds(parsed.node_credentials.as_ref().unwrap()).await;
        assert!(user.is_none());
        assert_eq!(pass, "hunter2");
    }

    #[tokio::test]
    async fn parse_sentinel_credentials_from_query() {
        let parsed = parse_sentinel_url(
            "redis+sentinel://s1/mymaster?sentinel_username=admin&sentinel_password=p%3Ass",
        )
        .unwrap();
        assert!(parsed.node_credentials.is_none());
        let (user, pass) = creds(parsed.sentinel_credentials.as_ref().unwrap()).await;
        assert_eq!(user.as_deref(), Some("admin"));
        assert_eq!(pass, "p:ss");
    }

    #[tokio::test]
    async fn parse_sentinel_password_only_query() {
        let parsed =
            parse_sentinel_url("redis+sentinel://s1/mymaster?sentinel_password=sp").unwrap();
        let (user, pass) = creds(parsed.sentinel_credentials.as_ref().unwrap()).await;
        assert!(user.is_none());
        assert_eq!(pass, "sp");
    }

    #[test]
    fn parse_rediss_sets_tls() {
        let parsed = parse_sentinel_url("rediss+sentinel://s1:26379/mymaster").unwrap();
        assert!(parsed.tls);
    }

    #[test]
    fn parse_percent_encoded_master_name() {
        let parsed = parse_sentinel_url("redis+sentinel://s1/my%20master").unwrap();
        assert_eq!(parsed.master_name, "my master");
    }

    #[test]
    fn reject_missing_master_name() {
        assert!(parse_sentinel_url("redis+sentinel://s1:26379").is_err());
        assert!(parse_sentinel_url("redis+sentinel://s1:26379/").is_err());
    }

    #[test]
    fn reject_missing_hosts() {
        assert!(parse_sentinel_url("redis+sentinel:///mymaster").is_err());
    }

    #[test]
    fn reject_wrong_scheme() {
        assert!(parse_sentinel_url("redis://s1/mymaster").is_err());
    }

    #[test]
    fn reject_unknown_query_parameter() {
        // A typo'd credential key must error rather than silently dropping auth.
        let err =
            parse_sentinel_url("redis+sentinel://s1/mymaster?sentinal_password=oops").unwrap_err();
        assert!(matches!(err, RedisError::InvalidUrl(_)), "got {err:?}");
    }

    #[test]
    fn reject_sentinel_username_without_password() {
        assert!(
            parse_sentinel_url("redis+sentinel://s1/mymaster?sentinel_username=admin").is_err()
        );
    }
}
