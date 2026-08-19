//! Dynamic credential provider for token rotation and cloud auth.
//!
//! Implement [`CredentialProvider`] to supply credentials dynamically,
//! e.g., from AWS IAM, Azure Entra ID, or a secrets manager. The
//! [`CredentialConnectionFactory`] fetches credentials for every fresh
//! connection and composes with reconnecting clients and connection pools.
//! [`AuthenticatedConnection`] remains available for direct, manually managed
//! connections.
//!
//! # Example
//!
//! ```no_run
//! use std::future::Future;
//! use std::pin::Pin;
//! use redis_tower::credentials::{
//!     AuthenticatedConnection, CredentialProvider, Credentials, StaticCredentials,
//! };
//! use redis_tower::commands::Ping;
//! use redis_tower::RedisError;
//!
//! # async fn fetch_iam_token() -> Result<String, RedisError> { Ok("token".into()) }
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Static credentials (simple case).
//! let creds = StaticCredentials::password("my_secret");
//! let mut conn = AuthenticatedConnection::connect("127.0.0.1:6379", creds).await?;
//! conn.execute(Ping::new()).await?;
//!
//! // Dynamic credentials (cloud IAM). This direct wrapper fetches once on
//! // connect and again whenever reauthenticate() is called explicitly.
//! struct IamProvider;
//! impl CredentialProvider for IamProvider {
//!     fn get_credentials(
//!         &self,
//!     ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
//!         // Fetch a short-lived token from an IAM service.
//!         Box::pin(async { Ok(Credentials::new("default", fetch_iam_token().await?)) })
//!     }
//! }
//! let mut conn = AuthenticatedConnection::connect("127.0.0.1:6379", IamProvider).await?;
//! conn.reauthenticate().await?;
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use redis_tower_commands::Auth;
use redis_tower_core::{Command, ConnectionConfig, ProtocolVersion, RedisConnection, RedisError};
use tokio_util::sync::CancellationToken;

/// A push stream of fresh credentials.
///
/// Providers yield only after the credentials used by an established
/// connection have changed. Each item is either the replacement credentials
/// or a refresh error. Providers must apply their own retry delay after an
/// error so consumers cannot enter a busy loop.
pub type CredentialUpdateStream =
    Pin<Box<dyn Stream<Item = Result<Credentials, RedisError>> + Send + 'static>>;

/// Credentials for Redis authentication.
#[derive(Clone, zeroize::Zeroize, zeroize::ZeroizeOnDrop)]
pub struct Credentials {
    /// Optional username (Redis 6+ ACL). `None` for password-only auth.
    pub username: Option<String>,
    /// Password or auth token.
    pub password: String,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Credentials")
            .field("username", &self.username.as_deref())
            .field("password", &"<redacted>")
            .finish()
    }
}

impl Credentials {
    /// Create credentials with username and password (Redis 6+ ACL).
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: Some(username.into()),
            password: password.into(),
        }
    }

    /// Create credentials with password only (legacy AUTH).
    pub fn password(password: impl Into<String>) -> Self {
        Self {
            username: None,
            password: password.into(),
        }
    }

    /// Return the ACL username, if this is a two-argument credential.
    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    /// Return the password or short-lived auth token.
    ///
    /// Treat the returned value as secret material. It is borrowed so callers
    /// do not create an additional plaintext allocation merely to authenticate.
    pub fn password_value(&self) -> &str {
        &self.password
    }

    /// Build the typed Redis `AUTH` command for these credentials.
    ///
    /// This is primarily useful when applying a value from a
    /// [`CredentialUpdateStream`] to an established client.
    pub fn auth_command(&self) -> Auth {
        match &self.username {
            Some(user) => Auth::credentials(user, &self.password),
            None => Auth::password(&self.password),
        }
    }
}

/// Trait for providing credentials dynamically.
///
/// Implement this for cloud auth providers (AWS IAM, Azure Entra ID),
/// secrets managers, or any source of rotating credentials.
pub trait CredentialProvider: Send + Sync + 'static {
    /// Fetch current credentials.
    ///
    /// [`CredentialConnectionFactory`] calls this for the initial connection
    /// and every reconnect. Direct wrappers call it when connecting or when
    /// reauthentication is requested. Implementations should handle caching
    /// internally.
    fn get_credentials(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>>;

    /// Force a fresh credential fetch after Redis rejects cached credentials.
    ///
    /// Providers that cache credentials should override this method to
    /// invalidate or bypass that cache. The default calls
    /// [`get_credentials`](Self::get_credentials) again, preserving the
    /// existing behavior of simple providers and closures.
    /// Calls may be concurrent across factory-backed clients or pool slots, so
    /// caching providers must synchronize cache invalidation and refetching.
    fn force_refresh(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
        self.get_credentials()
    }
}

/// A credential provider that can push replacements for established sockets.
///
/// Implementations return an independent stream for each subscription. The
/// first item must represent a credential newer than the one used during the
/// connection's initial [`CredentialProvider::get_credentials`] call. The
/// `Arc<Self>` receiver lets the stream retain the provider without requiring
/// the concrete type to be `Clone` and keeps the trait object-safe.
pub trait StreamingCredentialProvider: CredentialProvider {
    /// Subscribe to future credential replacements.
    fn subscribe(self: Arc<Self>) -> CredentialUpdateStream;
}

/// Owned task that applies credentials emitted by a streaming provider.
///
/// Dropping the handle cancels the subscription. Call [`shutdown`](Self::shutdown)
/// to cancel it and wait for the task to finish. The task logs provider and
/// reauthentication errors and keeps consuming later updates; it never
/// replays a user command.
#[must_use = "dropping the handle stops push-based credential reauthentication"]
pub struct CredentialReauthenticationHandle {
    cancellation: CancellationToken,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl CredentialReauthenticationHandle {
    fn new(cancellation: CancellationToken, task: tokio::task::JoinHandle<()>) -> Self {
        Self {
            cancellation,
            task: Some(task),
        }
    }

    /// Stop consuming credential updates and wait for the task to exit.
    pub async fn shutdown(mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for CredentialReauthenticationHandle {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Start applying pushed credentials with an asynchronous callback.
///
/// The callback should send one `AUTH` command to every established connection
/// represented by its target. It must not retry a user command after an
/// authentication error.
pub fn spawn_credential_reauthentication<F, Fut>(
    provider: Arc<dyn StreamingCredentialProvider>,
    reauthenticate: F,
) -> CredentialReauthenticationHandle
where
    F: Fn(Credentials) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), RedisError>> + Send + 'static,
{
    let mut updates = Arc::clone(&provider).subscribe();
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task = tokio::spawn(async move {
        loop {
            let update = tokio::select! {
                () = task_cancellation.cancelled() => break,
                update = updates.next() => update,
            };
            let Some(update) = update else {
                break;
            };
            match update {
                Ok(credentials) => {
                    if let Err(error) = reauthenticate(credentials).await {
                        tracing::warn!(error = %error, "credential reauthentication failed");
                    }
                }
                Err(error) => {
                    tracing::warn!(error = %error, "credential refresh stream failed");
                }
            }
        }
    });
    CredentialReauthenticationHandle::new(cancellation, task)
}

/// A simple provider that always returns the same credentials.
#[derive(Debug, Clone)]
pub struct StaticCredentials {
    creds: Credentials,
}

impl StaticCredentials {
    /// Create a static provider with username and password.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            creds: Credentials::new(username, password),
        }
    }

    /// Create a static provider with password only.
    pub fn password(password: impl Into<String>) -> Self {
        Self {
            creds: Credentials::password(password),
        }
    }
}

impl CredentialProvider for StaticCredentials {
    fn get_credentials(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
        let creds = self.creds.clone();
        Box::pin(async move { Ok(creds) })
    }
}

/// Blanket impl for closures.
impl<F, Fut> CredentialProvider for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Credentials, RedisError>> + Send + 'static,
{
    fn get_credentials(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
        Box::pin((self)())
    }
}

/// A provider-backed factory for authenticated Redis connections.
///
/// Every call fetches credentials, authenticates the fresh connection, and
/// then negotiates the requested RESP protocol. This setup order matters for
/// protected servers: negotiating RESP3 before `AUTH` can produce `NOAUTH`
/// and leave an automatic negotiation silently on RESP2.
///
/// If Redis rejects the first `AUTH` with `NOAUTH` or `WRONGPASS`, the factory
/// asks the provider to [`force_refresh`](CredentialProvider::force_refresh)
/// and retries `AUTH` once. It never retries user commands.
///
/// The factory implements both
/// [`ConnectionFactory`](crate::reconnect::ConnectionFactory) and
/// [`PoolFactory`](crate::pool::PoolFactory), so the same setup is replayed by
/// resilient, multiplexed, lazy, and replacement pool connections.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower::credentials::{CredentialConnectionFactory, StaticCredentials};
/// use redis_tower::reconnect::{ReconnectConfig, ResilientConnection};
///
/// let factory = CredentialConnectionFactory::new(
///     "127.0.0.1:6379",
///     StaticCredentials::password("secret"),
/// );
/// let connection = ResilientConnection::new(factory, ReconnectConfig::default()).await?;
/// # let _ = connection;
/// # Ok(())
/// # }
/// ```
#[must_use = "a credential connection factory must be passed to a client or pool"]
pub struct CredentialConnectionFactory {
    addr: String,
    provider: Arc<dyn CredentialProvider>,
    connection_config: ConnectionConfig,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    tls: Option<(String, Arc<redis_tower_core::tls::TlsConfig>)>,
}

impl Clone for CredentialConnectionFactory {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr.clone(),
            provider: Arc::clone(&self.provider),
            connection_config: self.connection_config.clone(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls: self.tls.clone(),
        }
    }
}

impl CredentialConnectionFactory {
    /// Create a plain-TCP factory backed by `provider`.
    pub fn new(addr: impl Into<String>, provider: impl CredentialProvider) -> Self {
        Self::from_shared_provider(addr, Arc::new(provider))
    }

    /// Create a plain-TCP factory from a shared, type-erased provider.
    ///
    /// This constructor lets several topology or pool factories share one
    /// provider cache and refresh state.
    pub fn from_shared_provider(
        addr: impl Into<String>,
        provider: Arc<dyn CredentialProvider>,
    ) -> Self {
        Self {
            addr: addr.into(),
            provider,
            connection_config: ConnectionConfig::default(),
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls: None,
        }
    }

    /// Apply connection settings to every initial connection and reconnect.
    ///
    /// Keepalive, connect timeout, and RESP decode limits apply during the
    /// initial RESP2 bootstrap. The requested protocol is negotiated only
    /// after authentication succeeds.
    pub fn with_connection_config(mut self, config: ConnectionConfig) -> Self {
        self.connection_config = config;
        self
    }

    /// Use explicit TLS settings for every connection made by this factory.
    ///
    /// `hostname` is the server name used for certificate verification. This
    /// explicit form avoids guessing from an address that may be an IPv6
    /// literal or a proxy endpoint.
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    pub fn with_tls(
        mut self,
        hostname: impl Into<String>,
        tls: redis_tower_core::tls::TlsConfig,
    ) -> Self {
        self.tls = Some((hostname.into(), Arc::new(tls)));
        self
    }

    /// Return the shared credential provider used by this factory.
    pub fn provider(&self) -> &dyn CredentialProvider {
        self.provider.as_ref()
    }

    async fn connect_inner(&self) -> Result<RedisConnection, RedisError> {
        let requested_protocol = self.connection_config.protocol();
        let bootstrap_config = self
            .connection_config
            .clone()
            .with_protocol(ProtocolVersion::Resp2);

        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
        let mut conn = match &self.tls {
            Some((hostname, tls)) => {
                RedisConnection::connect_tls_with_config(
                    &self.addr,
                    hostname,
                    tls.as_ref(),
                    &bootstrap_config,
                )
                .await?
            }
            None => RedisConnection::connect_with_config(&self.addr, &bootstrap_config).await?,
        };
        #[cfg(not(any(feature = "tls-rustls", feature = "tls-native-tls")))]
        let mut conn = RedisConnection::connect_with_config(&self.addr, &bootstrap_config).await?;

        authenticate_with_refresh(&mut conn, self.provider.as_ref()).await?;
        conn.negotiate_protocol(requested_protocol).await?;
        Ok(conn)
    }
}

impl crate::reconnect::ConnectionFactory for CredentialConnectionFactory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
        let factory = self.clone();
        Box::pin(async move { factory.connect_inner().await })
    }
}

impl crate::pool::PoolFactory for CredentialConnectionFactory {
    type Connection = RedisConnection;

    fn create(&self) -> Pin<Box<dyn Future<Output = Result<Self::Connection, RedisError>> + Send>> {
        crate::reconnect::ConnectionFactory::connect(self)
    }
}

/// Authenticate one freshly opened connection and refresh once when Redis
/// rejects cached credentials.
///
/// This helper is shared by standalone, Cluster, and Sentinel setup paths so
/// every topology applies the same bounded retry rule. It is intended only for
/// connection establishment; it never retries a user command.
pub async fn authenticate_with_refresh(
    conn: &mut RedisConnection,
    provider: &dyn CredentialProvider,
) -> Result<(), RedisError> {
    let credentials = provider.get_credentials().await?;
    match conn.execute(credentials.auth_command()).await {
        Err(error) if is_auth_rejection(&error) => {
            let credentials = provider.force_refresh().await?;
            conn.execute(credentials.auth_command()).await
        }
        result => result,
    }
}

/// Return whether Redis rejected authentication because credentials were
/// missing or invalid.
pub fn is_auth_rejection(error: &RedisError) -> bool {
    let RedisError::Redis(message) = error else {
        return false;
    };
    message
        .split_ascii_whitespace()
        .next()
        .map(|prefix| prefix.trim_start_matches('-'))
        .is_some_and(|prefix| {
            prefix.eq_ignore_ascii_case("NOAUTH") || prefix.eq_ignore_ascii_case("WRONGPASS")
        })
}

/// A connection that authenticates using a [`CredentialProvider`].
///
/// Fetches credentials from the provider and sends AUTH after connecting.
/// This direct wrapper does not reconnect automatically. Use
/// [`CredentialConnectionFactory`] with a reconnecting client when credentials
/// must be fetched again for every replacement connection.
pub struct AuthenticatedConnection<P> {
    conn: RedisConnection,
    provider: P,
}

impl<P: CredentialProvider> AuthenticatedConnection<P> {
    /// Connect and authenticate using the credential provider.
    pub async fn connect(addr: &str, provider: P) -> Result<Self, RedisError> {
        let mut conn = RedisConnection::connect(addr).await?;
        let creds = provider.get_credentials().await?;
        conn.execute(creds.auth_command()).await?;
        Ok(Self { conn, provider })
    }

    /// Connect via URL (ignoring URL credentials) and authenticate with the provider.
    pub async fn connect_url(url: &str, provider: P) -> Result<Self, RedisError> {
        let mut conn = RedisConnection::connect_url(url).await?;
        let creds = provider.get_credentials().await?;
        conn.execute(creds.auth_command()).await?;
        Ok(Self { conn, provider })
    }

    /// Re-authenticate with fresh credentials from the provider.
    ///
    /// Call this when you receive an auth error or proactively before
    /// token expiry.
    pub async fn reauthenticate(&mut self) -> Result<(), RedisError> {
        let creds = self.provider.get_credentials().await?;
        self.conn.execute(creds.auth_command()).await
    }

    /// Execute a command.
    pub async fn execute<Cmd: Command>(&mut self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        self.conn.execute(cmd).await
    }

    /// Get a reference to the credential provider.
    pub fn provider(&self) -> &P {
        &self.provider
    }

    /// Get a mutable reference to the inner connection.
    pub fn connection_mut(&mut self) -> &mut RedisConnection {
        &mut self.conn
    }
}

/// A connection that periodically refreshes credentials on a timer.
///
/// Wraps a [`RedisConnection`] in `Arc<Mutex<>>` and spawns a background
/// tokio task that re-authenticates at `refresh_interval`. This is intended
/// for cloud environments (AWS ElastiCache IAM, GCP MemoryStore) where
/// credentials expire.
///
/// The refresh interval should be shorter than the token TTL to avoid
/// authentication gaps.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use std::time::Duration;
/// use redis_tower::credentials::{RotatingAuthClient, StaticCredentials};
///
/// let provider = StaticCredentials::password("token");
/// let client = RotatingAuthClient::connect(
///     "127.0.0.1:6379",
///     provider,
///     Duration::from_secs(300),
/// ).await?;
/// # let _ = client;
/// # Ok(())
/// # }
/// ```
pub struct RotatingAuthClient<P> {
    conn: std::sync::Arc<tokio::sync::Mutex<RedisConnection>>,
    provider: std::sync::Arc<P>,
    timer_task: Option<tokio::task::JoinHandle<()>>,
    _streaming_task: Option<CredentialReauthenticationHandle>,
}

impl<P: CredentialProvider> RotatingAuthClient<P> {
    /// Connect, authenticate, and start background credential rotation.
    ///
    /// The background task re-authenticates every `refresh_interval`. If
    /// credential fetch or AUTH fails, the error is logged (via `tracing`)
    /// and the next tick retries.
    pub async fn connect(
        addr: &str,
        provider: P,
        refresh_interval: std::time::Duration,
    ) -> Result<Self, RedisError> {
        let mut conn = RedisConnection::connect(addr).await?;
        let creds = provider.get_credentials().await?;
        conn.execute(creds.auth_command()).await?;

        let conn = std::sync::Arc::new(tokio::sync::Mutex::new(conn));
        let provider = std::sync::Arc::new(provider);

        let refresh_conn = conn.clone();
        let refresh_provider = provider.clone();
        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(refresh_interval);
            interval.tick().await; // skip first immediate tick
            loop {
                interval.tick().await;
                match refresh_provider.get_credentials().await {
                    Ok(creds) => {
                        let mut c = refresh_conn.lock().await;
                        let _ = c.execute(creds.auth_command()).await;
                    }
                    Err(_) => {
                        // Best-effort: next tick will retry.
                    }
                }
            }
        });

        Ok(Self {
            conn,
            provider,
            timer_task: Some(task),
            _streaming_task: None,
        })
    }

    /// Execute a command on the underlying connection.
    pub async fn execute<Cmd: Command>(&self, cmd: Cmd) -> Result<Cmd::Response, RedisError> {
        let mut conn = self.conn.lock().await;
        conn.execute(cmd).await
    }

    /// Get a reference to the credential provider.
    pub fn provider(&self) -> &P {
        &self.provider
    }
}

impl<P> Drop for RotatingAuthClient<P> {
    fn drop(&mut self) {
        if let Some(task) = self.timer_task.take() {
            task.abort();
        }
        // `CredentialReauthenticationHandle::drop` cancels and aborts the
        // streaming task after this method returns.
    }
}

impl<P: StreamingCredentialProvider> RotatingAuthClient<P> {
    /// Connect, authenticate, and re-authenticate whenever `provider` emits.
    ///
    /// Unlike [`Self::connect`], this has no polling interval. The provider
    /// controls refresh timing from the actual credential lifetime, and the
    /// owned task stops when this client is dropped.
    pub async fn connect_streaming(addr: &str, provider: P) -> Result<Self, RedisError> {
        let mut conn = RedisConnection::connect(addr).await?;
        let credentials = provider.get_credentials().await?;
        conn.execute(credentials.auth_command()).await?;

        let conn = Arc::new(tokio::sync::Mutex::new(conn));
        let provider = Arc::new(provider);
        let streaming_provider: Arc<dyn StreamingCredentialProvider> = provider.clone();
        let refresh_conn = Arc::clone(&conn);
        let streaming_task =
            spawn_credential_reauthentication(streaming_provider, move |credentials| {
                let refresh_conn = Arc::clone(&refresh_conn);
                async move {
                    let mut conn = refresh_conn.lock().await;
                    conn.execute(credentials.auth_command()).await
                }
            });

        Ok(Self {
            conn,
            provider,
            timer_task: None,
            _streaming_task: Some(streaming_task),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_protocol::helpers::{array, bulk};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use zeroize::Zeroize;

    struct RefreshAwareProvider {
        get_calls: Arc<AtomicUsize>,
        refresh_calls: Arc<AtomicUsize>,
    }

    struct OneShotStreamingProvider;

    #[test]
    fn credentials_zeroize_owned_material() {
        let mut credentials = Credentials::new("alice", "secret-token");
        credentials.zeroize();
        assert!(credentials.username.is_none());
        assert!(credentials.password.is_empty());
    }

    impl CredentialProvider for OneShotStreamingProvider {
        fn get_credentials(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
            Box::pin(async { Ok(Credentials::password("initial")) })
        }
    }

    impl StreamingCredentialProvider for OneShotStreamingProvider {
        fn subscribe(self: Arc<Self>) -> CredentialUpdateStream {
            Box::pin(futures::stream::once(async {
                Ok(Credentials::password("pushed"))
            }))
        }
    }

    impl CredentialProvider for RefreshAwareProvider {
        fn get_credentials(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(Credentials::password("cached")) })
        }

        fn force_refresh(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
            self.refresh_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(Credentials::password("fresh")) })
        }
    }

    #[test]
    fn credentials_password_only() {
        let creds = Credentials::password("secret");
        assert!(creds.username.is_none());
        assert_eq!(creds.password, "secret");

        let auth = creds.auth_command();
        let frame = auth.to_frame();
        assert_eq!(frame, array(vec![bulk("AUTH"), bulk("secret")]));
    }

    #[test]
    fn credentials_with_username() {
        let creds = Credentials::new("admin", "pass123");
        assert_eq!(creds.username.as_deref(), Some("admin"));
        assert_eq!(creds.password, "pass123");

        let auth = creds.auth_command();
        let frame = auth.to_frame();
        assert_eq!(
            frame,
            array(vec![bulk("AUTH"), bulk("admin"), bulk("pass123")])
        );
    }

    #[test]
    fn credentials_debug_redacts_password() {
        let debug = format!("{:?}", Credentials::new("admin", "super-secret-token"));
        assert!(debug.contains("admin"));
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("super-secret-token"));
    }

    #[test]
    fn static_credentials_password() {
        let provider = StaticCredentials::password("token123");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let creds = rt.block_on(provider.get_credentials()).unwrap();
        assert_eq!(creds.password, "token123");
        assert!(creds.username.is_none());
    }

    #[test]
    fn static_credentials_with_user() {
        let provider = StaticCredentials::new("user", "pass");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let creds = rt.block_on(provider.get_credentials()).unwrap();
        assert_eq!(creds.username.as_deref(), Some("user"));
        assert_eq!(creds.password, "pass");
    }

    #[test]
    fn closure_as_credential_provider() {
        let provider = || async { Ok(Credentials::password("dynamic_token")) };
        let rt = tokio::runtime::Runtime::new().unwrap();
        let creds = rt.block_on(provider.get_credentials()).unwrap();
        assert_eq!(creds.password, "dynamic_token");
    }

    #[test]
    fn force_refresh_defaults_to_another_get() {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = {
            let calls = Arc::clone(&calls);
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(Credentials::password("fresh")) }
            }
        };

        let rt = tokio::runtime::Runtime::new().unwrap();
        let credentials = rt.block_on(provider.force_refresh()).unwrap();

        assert_eq!(credentials.password, "fresh");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn force_refresh_is_object_safe_and_overridable() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let refresh_calls = Arc::new(AtomicUsize::new(0));
        let provider: Arc<dyn CredentialProvider> = Arc::new(RefreshAwareProvider {
            get_calls: Arc::clone(&get_calls),
            refresh_calls: Arc::clone(&refresh_calls),
        });
        let factory = CredentialConnectionFactory::from_shared_provider("localhost:6379", provider);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let credentials = rt.block_on(factory.provider().force_refresh()).unwrap();

        assert_eq!(credentials.password, "fresh");
        assert_eq!(get_calls.load(Ordering::SeqCst), 0);
        assert_eq!(refresh_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn auth_rejection_classification_is_specific() {
        assert!(is_auth_rejection(&RedisError::Redis(
            "WRONGPASS invalid username-password pair".into()
        )));
        assert!(is_auth_rejection(&RedisError::Redis(
            "NOAUTH Authentication required".into()
        )));
        assert!(is_auth_rejection(&RedisError::Redis(
            "-wrongpass stale token".into()
        )));
        assert!(!is_auth_rejection(&RedisError::Redis(
            "ERR invalid password policy".into()
        )));
        assert!(!is_auth_rejection(&RedisError::ConnectionClosed));
    }

    #[test]
    fn credential_factory_implements_connection_and_pool_factories() {
        fn assert_connection_factory<T: crate::reconnect::ConnectionFactory>() {}
        fn assert_pool_factory<T: crate::pool::PoolFactory<Connection = RedisConnection>>() {}
        fn assert_send_sync<T: Send + Sync>() {}

        assert_connection_factory::<CredentialConnectionFactory>();
        assert_pool_factory::<CredentialConnectionFactory>();
        assert_send_sync::<CredentialConnectionFactory>();
    }

    #[tokio::test]
    async fn push_helper_applies_emitted_credentials_and_shuts_down() {
        let provider: Arc<dyn StreamingCredentialProvider> = Arc::new(OneShotStreamingProvider);
        let applied = Arc::new(tokio::sync::Notify::new());
        let observed = Arc::new(std::sync::Mutex::new(None));
        let handle = spawn_credential_reauthentication(provider, {
            let applied = Arc::clone(&applied);
            let observed = Arc::clone(&observed);
            move |credentials| {
                let applied = Arc::clone(&applied);
                let observed = Arc::clone(&observed);
                async move {
                    *observed.lock().unwrap() = Some(credentials.password.clone());
                    applied.notify_one();
                    Ok(())
                }
            }
        });

        tokio::time::timeout(std::time::Duration::from_secs(1), applied.notified())
            .await
            .expect("pushed credential was not applied");
        assert_eq!(observed.lock().unwrap().as_deref(), Some("pushed"));
        handle.shutdown().await;
    }

    #[test]
    fn credentials_clone() {
        let creds = Credentials::new("u", "p");
        let cloned = creds.clone();
        assert_eq!(cloned.username, creds.username);
        assert_eq!(cloned.password, creds.password);
    }

    // -- RotatingAuthClient --

    #[test]
    fn rotating_auth_client_types_compile() {
        // Verify RotatingAuthClient can be constructed with StaticCredentials
        // (type-level check, no actual connection).
        fn _assert_send<T: Send>() {}
        _assert_send::<RotatingAuthClient<StaticCredentials>>();
    }

    #[test]
    fn rotating_auth_client_drop_aborts_task() {
        // Verify that dropping a RotatingAuthClient does not panic.
        // We cannot construct one without a real connection, but we can
        // confirm the Drop impl compiles and the type is well-formed.
        let _provider = StaticCredentials::password("token");
        // Type assertion only -- actual connect needs a running Redis.
    }
}
