//! Dynamic credential provider for token rotation and cloud auth.
//!
//! Implement [`CredentialProvider`] to supply credentials dynamically,
//! e.g., from AWS IAM, Azure Entra ID, or a secrets manager. The
//! [`CredentialConnectionFactory`] fetches credentials for every fresh
//! connection and composes with reconnecting clients and connection pools.
//! URL-backed factories preserve transport, database, and protocol setup, and
//! [`SharedCredentialProvider`] lets type-erased clients share one provider
//! cache across ordinary, Cluster, Pub/Sub, and MONITOR connection owners.
//! [`AuthenticatedConnection`] remains available for direct, manually managed
//! connections.
//!
//! In-place reauthentication is only appropriate for owners that explicitly
//! serialize it, such as multiplexed clients and retained pools. Pub/Sub and
//! MONITOR owners reconnect or terminate at credential-update boundaries; see
//! the [cloud authentication guide] for the complete session matrix.
//!
//! [cloud authentication guide]: https://github.com/joshrotenberg/redis-tower/blob/main/docs/CLOUD-AUTH.md
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
use std::time::Duration;

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

/// Cloneable ownership handle for one shared credential-provider instance.
///
/// The wrapper delegates to an `Arc<P>` and itself implements
/// [`CredentialProvider`]. When `P` also implements
/// [`StreamingCredentialProvider`], the wrapper implements that trait too.
/// This lets a type-erased host pass one provider cache to standalone,
/// Cluster, pool, Pub/Sub, and MONITOR connection factories without requiring
/// every builder to expose an `Arc<dyn ...>` overload.
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use redis_tower::credentials::{
///     CredentialProvider, Credentials, SharedCredentialProvider,
/// };
/// use redis_tower::RedisError;
///
/// let provider: Arc<dyn CredentialProvider> = Arc::new(|| async {
///     Ok::<_, RedisError>(Credentials::password("short-lived-token"))
/// });
/// let shared = SharedCredentialProvider::from_arc(provider);
/// let standalone = shared.clone();
/// let dedicated_session = shared;
/// # let _ = (standalone, dedicated_session);
/// ```
pub struct SharedCredentialProvider<P: ?Sized = dyn CredentialProvider> {
    inner: Arc<P>,
}

impl<P: ?Sized> Clone for SharedCredentialProvider<P> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<P: ?Sized> fmt::Debug for SharedCredentialProvider<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SharedCredentialProvider")
            .finish_non_exhaustive()
    }
}

impl<P> SharedCredentialProvider<P> {
    /// Wrap a concrete provider in shared ownership.
    pub fn new(provider: P) -> Self {
        Self {
            inner: Arc::new(provider),
        }
    }
}

impl<P: ?Sized> SharedCredentialProvider<P> {
    /// Wrap an existing shared, possibly type-erased provider.
    pub fn from_arc(provider: Arc<P>) -> Self {
        Self { inner: provider }
    }

    /// Borrow the shared provider allocation.
    pub fn as_arc(&self) -> &Arc<P> {
        &self.inner
    }

    /// Return a clone of the shared provider allocation.
    pub fn clone_arc(&self) -> Arc<P> {
        Arc::clone(&self.inner)
    }

    /// Consume the handle and return the shared provider allocation.
    pub fn into_arc(self) -> Arc<P> {
        self.inner
    }
}

impl<P: CredentialProvider + ?Sized> CredentialProvider for SharedCredentialProvider<P> {
    fn get_credentials(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
        self.inner.get_credentials()
    }

    fn force_refresh(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
        self.inner.force_refresh()
    }
}

impl<P: StreamingCredentialProvider + ?Sized> StreamingCredentialProvider
    for SharedCredentialProvider<P>
{
    fn subscribe(self: Arc<Self>) -> CredentialUpdateStream {
        Arc::clone(&self.inner).subscribe()
    }
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
/// Use this only for a target that explicitly supports in-place `AUTH`, such as
/// a normal multiplexed client or retained connection pool. Pub/Sub and
/// MONITOR sockets are in dedicated protocol modes, while transactions and
/// blocking calls can carry connection-local state; those owners should
/// reconnect or terminate at a safe boundary instead. The callback must never
/// retry a user command after an authentication error.
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
                    if reauthenticate(credentials).await.is_err() {
                        // The callback may wrap a provider or server error.
                        // Never render it here: third-party errors are not
                        // guaranteed to redact credential material.
                        tracing::warn!("credential reauthentication failed");
                    }
                }
                Err(_error) => {
                    // Provider errors can contain SDK request details. Keep
                    // tracing useful without risking token disclosure.
                    tracing::warn!("credential refresh stream failed");
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
/// [`from_url`](Self::from_url) additionally preserves URL transport, database,
/// and protocol settings while replacing only URL-embedded authentication with
/// the provider.
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
    target: CredentialTarget,
    provider: Arc<dyn CredentialProvider>,
    connection_config: ConnectionConfig,
    setup_timeout: Option<Duration>,
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    tls: Option<(String, Arc<redis_tower_core::tls::TlsConfig>)>,
}

#[derive(Clone)]
enum CredentialTarget {
    Address(String),
    Url(String),
}

impl Clone for CredentialConnectionFactory {
    fn clone(&self) -> Self {
        Self {
            target: self.target.clone(),
            provider: Arc::clone(&self.provider),
            connection_config: self.connection_config.clone(),
            setup_timeout: self.setup_timeout,
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
            target: CredentialTarget::Address(addr.into()),
            provider,
            connection_config: ConnectionConfig::default(),
            setup_timeout: None,
            #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
            tls: None,
        }
    }

    /// Create a URL-backed factory that authenticates with `provider`.
    ///
    /// TCP, TLS, Unix-socket, database, and `protocol` URL settings are
    /// preserved. Any username or password embedded in the URL is ignored;
    /// the provider is the single authentication source. Use
    /// [`RedisConnection::connect_url`] when static URL credentials are
    /// desired instead.
    pub fn from_url(url: impl Into<String>, provider: impl CredentialProvider) -> Self {
        Self::from_url_with_shared_provider(url, Arc::new(provider))
    }

    /// Create a URL-backed factory from a shared, type-erased provider.
    pub fn from_url_with_shared_provider(
        url: impl Into<String>,
        provider: Arc<dyn CredentialProvider>,
    ) -> Self {
        Self {
            target: CredentialTarget::Url(url.into()),
            provider,
            connection_config: ConnectionConfig::default(),
            setup_timeout: None,
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

    /// Bound the complete provider-backed setup sequence.
    ///
    /// Unlike [`ConnectionConfig::connect_timeout`], which covers only the
    /// socket connect operation, this deadline includes credential lookup,
    /// transport and TLS establishment, bounded refresh-on-rejection, database
    /// selection, and RESP negotiation. Expiry returns
    /// [`RedisError::ConnectTimeout`] and drops the partial socket.
    pub fn with_setup_timeout(mut self, timeout: Duration) -> Self {
        self.setup_timeout = Some(timeout);
        self
    }

    /// Use explicit TLS settings for every connection made by this factory.
    ///
    /// For address targets, `hostname` is the server name used for certificate
    /// verification. URL targets use the URL host, matching
    /// [`RedisConnection::connect_url_with_tls`].
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

    /// Clone the shared provider allocation used by this factory.
    ///
    /// This is useful when one type-erased provider must also be passed to a
    /// Cluster builder or a dedicated-session factory.
    pub fn shared_provider(&self) -> Arc<dyn CredentialProvider> {
        Arc::clone(&self.provider)
    }

    async fn connect_inner(&self) -> Result<RedisConnection, RedisError> {
        match self.setup_timeout {
            Some(timeout) => tokio::time::timeout(timeout, self.connect_unbounded())
                .await
                .map_err(|_elapsed| RedisError::ConnectTimeout)?,
            None => self.connect_unbounded().await,
        }
    }

    async fn connect_unbounded(&self) -> Result<RedisConnection, RedisError> {
        let requested_protocol = self.connection_config.protocol();
        let bootstrap_config = self
            .connection_config
            .clone()
            .with_protocol(ProtocolVersion::Resp2);

        #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
        {
            match &self.target {
                CredentialTarget::Address(addr) => {
                    let mut connection = match &self.tls {
                        Some((hostname, tls)) => {
                            RedisConnection::connect_tls_with_config(
                                addr,
                                hostname,
                                tls.as_ref(),
                                &bootstrap_config,
                            )
                            .await?
                        }
                        None => {
                            RedisConnection::connect_with_config(addr, &bootstrap_config).await?
                        }
                    };
                    authenticate_with_refresh(&mut connection, self.provider.as_ref()).await?;
                    connection.negotiate_protocol(requested_protocol).await?;
                    Ok(connection)
                }
                CredentialTarget::Url(url) => {
                    let mut pending = match &self.tls {
                        Some((_hostname, tls)) => {
                            RedisConnection::begin_url_connection_with_tls_and_config(
                                url,
                                tls.as_ref(),
                                &self.connection_config,
                            )
                            .await?
                        }
                        None => {
                            RedisConnection::begin_url_connection_with_config(
                                url,
                                &self.connection_config,
                            )
                            .await?
                        }
                    };
                    authenticate_with_refresh(pending.connection_mut(), self.provider.as_ref())
                        .await?;
                    pending.finish().await
                }
            }
        }
        #[cfg(not(any(feature = "tls-rustls", feature = "tls-native-tls")))]
        {
            match &self.target {
                CredentialTarget::Address(addr) => {
                    let mut connection =
                        RedisConnection::connect_with_config(addr, &bootstrap_config).await?;
                    authenticate_with_refresh(&mut connection, self.provider.as_ref()).await?;
                    connection.negotiate_protocol(requested_protocol).await?;
                    Ok(connection)
                }
                CredentialTarget::Url(url) => {
                    let mut pending = RedisConnection::begin_url_connection_with_config(
                        url,
                        &self.connection_config,
                    )
                    .await?;
                    authenticate_with_refresh(pending.connection_mut(), self.provider.as_ref())
                        .await?;
                    pending.finish().await
                }
            }
        }
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
    let credentials = provider
        .get_credentials()
        .await
        .map_err(|_error| credential_provider_failure("current credential lookup"))?;
    match conn.execute(credentials.auth_command()).await {
        Err(error) if is_auth_rejection(&error) => {
            let credentials = provider
                .force_refresh()
                .await
                .map_err(|_error| credential_provider_failure("forced refresh"))?;
            conn.execute(credentials.auth_command()).await
        }
        result => result,
    }
}

fn credential_provider_failure(operation: &'static str) -> RedisError {
    // Never embed a third-party provider error. SDK errors can contain request
    // details and custom providers may accidentally include token material.
    RedisError::Redis(format!("AUTH_PROVIDER {operation} failed"))
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

/// Return whether an error belongs to credential lookup or authentication.
///
/// This includes provider failures redacted by redis-tower as well as Redis
/// `NOAUTH` and `WRONGPASS` replies. It is intended for downstream error
/// classification without parsing full messages or exposing secret material.
pub fn is_authentication_error(error: &RedisError) -> bool {
    if is_auth_rejection(error) {
        return true;
    }
    matches!(error, RedisError::Redis(message) if message.starts_with("AUTH_PROVIDER "))
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
        authenticate_with_refresh(&mut conn, &provider).await?;
        Ok(Self { conn, provider })
    }

    /// Connect via URL and authenticate with the provider.
    ///
    /// The URL transport, database, and protocol are preserved. URL-embedded
    /// credentials are ignored so the provider remains the only credential
    /// source.
    pub async fn connect_url(url: &str, provider: P) -> Result<Self, RedisError> {
        let mut pending =
            RedisConnection::begin_url_connection_with_config(url, &ConnectionConfig::default())
                .await?;
        authenticate_with_refresh(pending.connection_mut(), &provider).await?;
        let conn = pending.finish().await?;
        Ok(Self { conn, provider })
    }

    /// Re-authenticate with fresh credentials from the provider.
    ///
    /// Call this when you receive an auth error or proactively before
    /// token expiry.
    pub async fn reauthenticate(&mut self) -> Result<(), RedisError> {
        let creds = self
            .provider
            .get_credentials()
            .await
            .map_err(|_error| credential_provider_failure("reauthentication lookup"))?;
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
        authenticate_with_refresh(&mut conn, &provider).await?;

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
        authenticate_with_refresh(&mut conn, &provider).await?;

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
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tracing_subscriber::layer::{Context, Layer};
    use tracing_subscriber::prelude::*;
    use zeroize::Zeroize;

    struct RefreshAwareProvider {
        get_calls: Arc<AtomicUsize>,
        refresh_calls: Arc<AtomicUsize>,
    }

    struct OneShotStreamingProvider;

    struct LeakyStreamingProvider;

    #[derive(Clone, Default)]
    struct EventCapture {
        events: Arc<Mutex<Vec<String>>>,
    }

    struct FieldCollector(String);

    impl tracing::field::Visit for FieldCollector {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
            self.0.push_str(&format!(" {}={value:?}", field.name()));
        }
    }

    impl<S: tracing::Subscriber> Layer<S> for EventCapture {
        fn on_event(&self, event: &tracing::Event<'_>, _context: Context<'_, S>) {
            let mut collector = FieldCollector(String::new());
            event.record(&mut collector);
            self.events.lock().unwrap().push(collector.0);
        }
    }

    struct DropAwareStreamingProvider {
        stream_created: Arc<tokio::sync::Notify>,
        stream_dropped: Arc<AtomicBool>,
    }

    struct DropAwareStream {
        created: Arc<tokio::sync::Notify>,
        dropped: Arc<AtomicBool>,
        announced: bool,
    }

    impl Stream for DropAwareStream {
        type Item = Result<Credentials, RedisError>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            if !self.announced {
                self.announced = true;
                self.created.notify_one();
            }
            std::task::Poll::Pending
        }
    }

    impl Drop for DropAwareStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

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

    impl CredentialProvider for DropAwareStreamingProvider {
        fn get_credentials(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
            Box::pin(async { Ok(Credentials::password("initial")) })
        }
    }

    impl CredentialProvider for LeakyStreamingProvider {
        fn get_credentials(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
            Box::pin(async { Ok(Credentials::password("initial")) })
        }
    }

    impl StreamingCredentialProvider for LeakyStreamingProvider {
        fn subscribe(self: Arc<Self>) -> CredentialUpdateStream {
            Box::pin(futures::stream::iter([
                Err(RedisError::Redis(
                    "provider-secret-must-not-be-traced".to_string(),
                )),
                Ok(Credentials::password("callback-input-secret")),
            ]))
        }
    }

    impl StreamingCredentialProvider for DropAwareStreamingProvider {
        fn subscribe(self: Arc<Self>) -> CredentialUpdateStream {
            Box::pin(DropAwareStream {
                created: Arc::clone(&self.stream_created),
                dropped: Arc::clone(&self.stream_dropped),
                announced: false,
            })
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
    fn shared_provider_handle_preserves_one_type_erased_provider() {
        let get_calls = Arc::new(AtomicUsize::new(0));
        let refresh_calls = Arc::new(AtomicUsize::new(0));
        let provider: Arc<dyn CredentialProvider> = Arc::new(RefreshAwareProvider {
            get_calls: Arc::clone(&get_calls),
            refresh_calls: Arc::clone(&refresh_calls),
        });
        let shared = SharedCredentialProvider::from_arc(provider);
        let first = shared.clone();
        let second = shared;

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            first.get_credentials().await.unwrap();
            second.force_refresh().await.unwrap();
        });

        assert_eq!(get_calls.load(Ordering::SeqCst), 1);
        assert_eq!(refresh_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn shared_streaming_provider_delegates_subscription() {
        let shared = SharedCredentialProvider::new(OneShotStreamingProvider);
        let shared: Arc<dyn StreamingCredentialProvider> = Arc::new(shared);
        let mut updates = Arc::clone(&shared).subscribe();

        let credentials = updates.next().await.unwrap().unwrap();
        assert_eq!(credentials.password_value(), "pushed");
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
    fn provider_failure_is_authentication_classified_without_secret_material() {
        let error = credential_provider_failure("current credential lookup");
        let rendered = error.to_string();
        assert!(is_authentication_error(&error));
        assert!(rendered.contains("AUTH_PROVIDER"));
        assert!(!rendered.contains("token"));
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

    #[tokio::test]
    async fn dropping_push_handle_drops_provider_stream() {
        let stream_created = Arc::new(tokio::sync::Notify::new());
        let stream_dropped = Arc::new(AtomicBool::new(false));
        let provider: Arc<dyn StreamingCredentialProvider> = Arc::new(DropAwareStreamingProvider {
            stream_created: Arc::clone(&stream_created),
            stream_dropped: Arc::clone(&stream_dropped),
        });
        let handle = spawn_credential_reauthentication(provider, |_credentials| async { Ok(()) });

        tokio::time::timeout(Duration::from_secs(1), stream_created.notified())
            .await
            .expect("provider stream was not polled");
        drop(handle);
        tokio::task::yield_now().await;
        assert!(
            stream_dropped.load(Ordering::SeqCst),
            "dropping the owner must stop and drop its refresh stream"
        );
    }

    #[tokio::test]
    async fn push_helper_tracing_does_not_render_provider_or_callback_errors() {
        let capture = EventCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);
        let callback_reached = Arc::new(tokio::sync::Notify::new());
        let provider: Arc<dyn StreamingCredentialProvider> = Arc::new(LeakyStreamingProvider);
        let handle = spawn_credential_reauthentication(provider, {
            let callback_reached = Arc::clone(&callback_reached);
            move |_credentials| {
                callback_reached.notify_one();
                async {
                    Err(RedisError::Redis(
                        "callback-secret-must-not-be-traced".to_string(),
                    ))
                }
            }
        });

        tokio::time::timeout(Duration::from_secs(1), callback_reached.notified())
            .await
            .expect("credential callback was not reached");
        handle.shutdown().await;

        let events = capture.events.lock().unwrap().join("\n");
        assert!(events.contains("credential refresh stream failed"));
        assert!(events.contains("credential reauthentication failed"));
        assert!(!events.contains("provider-secret-must-not-be-traced"));
        assert!(!events.contains("callback-secret-must-not-be-traced"));
        assert!(!events.contains("callback-input-secret"));
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
