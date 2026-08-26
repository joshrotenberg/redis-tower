#![deny(missing_docs)]
//! Microsoft Entra ID authentication for Azure Managed Redis and Azure Cache
//! for Redis.
//!
//! [`EntraIdProvider`] uses any Azure SDK [`TokenCredential`], with a
//! convenience constructor for [`ManagedIdentityCredential`].
//! It caches each access token, refreshes at 75% of its observed lifetime, and
//! exposes that replacement through redis-tower's push credential stream.
//!
//! # Example
//!
//! ```no_run
//! use redis_tower_auth_azure::EntraIdProvider;
//!
//! # fn provider() -> Result<EntraIdProvider, azure_core::Error> {
//! EntraIdProvider::managed_identity(
//!     "00000000-0000-0000-0000-000000000000",
//!     None,
//! )
//! # }
//! ```
//!
//! The object ID becomes the Redis ACL username. Pass the provider to a
//! credential-aware connection factory and configure TLS separately. See the
//! [cloud authentication guide](https://github.com/joshrotenberg/redis-tower/blob/main/docs/CLOUD-AUTH.md)
//! for complete topology examples.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use azure_core::credentials::TokenCredential;
use azure_identity::{ManagedIdentityCredential, ManagedIdentityCredentialOptions};
use redis_tower::RedisError;
use redis_tower::credentials::{
    CredentialProvider, CredentialUpdateStream, Credentials, StreamingCredentialProvider,
};
use tokio::sync::Mutex;

/// Microsoft Entra scope accepted by Azure Managed Redis and Azure Cache for
/// Redis.
pub const AZURE_REDIS_SCOPE: &str = "https://redis.azure.com/.default";

/// Numerator of the proactive refresh point within the token lifetime.
pub const REFRESH_FRACTION_NUMERATOR: u32 = 3;

/// Denominator of the proactive refresh point within the token lifetime.
pub const REFRESH_FRACTION_DENOMINATOR: u32 = 4;

const REFRESH_RETRY_DELAY: Duration = Duration::from_secs(5);

struct CachedToken {
    credentials: Credentials,
    generated_at: Instant,
    refresh_at: Instant,
}

struct Inner {
    object_id: String,
    scope: String,
    credential: Arc<dyn TokenCredential>,
    cached: Mutex<Option<CachedToken>>,
}

/// Entra ID credential provider with proactive token refresh.
///
/// `object_id` is the object ID of the managed identity or service principal
/// configured as the Redis user. It becomes the ACL username; the Entra access
/// token becomes the Redis password.
#[derive(Clone)]
pub struct EntraIdProvider {
    inner: Arc<Inner>,
}

impl fmt::Debug for EntraIdProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EntraIdProvider")
            .field("object_id", &self.inner.object_id)
            .field("scope", &self.inner.scope)
            .finish_non_exhaustive()
    }
}

impl EntraIdProvider {
    /// Create a provider from any Azure SDK token credential.
    ///
    /// This supports managed identity, workload identity, service principals,
    /// and custom `TokenCredential` implementations while retaining the Redis
    /// object-ID username required by Azure.
    pub fn new(
        object_id: impl Into<String>,
        credential: Arc<dyn TokenCredential>,
    ) -> Result<Self, RedisError> {
        Self::with_scope(object_id, AZURE_REDIS_SCOPE, credential)
    }

    /// Create a provider with an explicit Entra resource scope.
    ///
    /// Most callers should use [`Self::new`]. The custom scope exists for
    /// sovereign-cloud or compatibility deployments whose configured audience
    /// differs from Azure's standard Redis resource.
    pub fn with_scope(
        object_id: impl Into<String>,
        scope: impl Into<String>,
        credential: Arc<dyn TokenCredential>,
    ) -> Result<Self, RedisError> {
        let object_id = object_id.into();
        let scope = scope.into();
        if object_id.trim().is_empty() {
            return Err(RedisError::Redis(
                "Entra Redis object ID must not be empty".to_string(),
            ));
        }
        if scope.trim().is_empty() {
            return Err(RedisError::Redis(
                "Entra Redis token scope must not be empty".to_string(),
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                object_id,
                scope,
                credential,
                cached: Mutex::new(None),
            }),
        })
    }

    /// Create a provider using Azure's managed-identity credential.
    ///
    /// `options` selects a user-assigned identity when needed. Pass `None` for
    /// the system-assigned identity of the current Azure resource.
    pub fn managed_identity(
        object_id: impl Into<String>,
        options: Option<ManagedIdentityCredentialOptions>,
    ) -> azure_core::Result<Self> {
        let credential: Arc<dyn TokenCredential> = ManagedIdentityCredential::new(options)?;
        // The Azure constructor cannot produce an invalid object ID. Preserve
        // its error type for ergonomic `?` use and validate before constructing
        // the managed credential in normal callers through `new` when needed.
        let object_id = object_id.into();
        if object_id.trim().is_empty() {
            return Err(azure_core::Error::with_message(
                azure_core::error::ErrorKind::Credential,
                "Entra Redis object ID must not be empty".to_string(),
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                object_id,
                scope: AZURE_REDIS_SCOPE.to_string(),
                credential,
                cached: Mutex::new(None),
            }),
        })
    }

    /// Return the Redis username (managed-identity or service-principal object
    /// ID).
    pub fn object_id(&self) -> &str {
        &self.inner.object_id
    }

    /// Return the Entra resource scope requested from the Azure credential.
    pub fn scope(&self) -> &str {
        &self.inner.scope
    }

    async fn credentials(&self, force: bool) -> Result<Credentials, RedisError> {
        let requested_at = Instant::now();
        let mut cached = self.inner.cached.lock().await;
        if let Some(token) = cached.as_ref()
            && Instant::now() < token.refresh_at
            && (!force || token.generated_at >= requested_at)
        {
            // Coalesce force-refresh calls that began before the current token
            // was generated without masking a later rejection of that token.
            return Ok(token.credentials.clone());
        }

        let scope = self.inner.scope.as_str();
        let token = self
            .inner
            .credential
            .get_token(&[scope], None)
            .await
            .map_err(|error| provider_error("Entra token acquisition", error))?;
        let lifetime = token_lifetime(token.expires_on.unix_timestamp())?;
        let refresh_after = refresh_after(lifetime);
        let credentials = Credentials::new(&self.inner.object_id, token.token.secret());
        let generated_at = Instant::now();
        *cached = Some(CachedToken {
            credentials: credentials.clone(),
            generated_at,
            refresh_at: generated_at + refresh_after,
        });
        Ok(credentials)
    }

    async fn delay_until_refresh(&self) -> Duration {
        self.inner
            .cached
            .lock()
            .await
            .as_ref()
            .map(|token| token.refresh_at.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::ZERO)
    }
}

impl CredentialProvider for EntraIdProvider {
    fn get_credentials(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
        let provider = self.clone();
        Box::pin(async move { provider.credentials(false).await })
    }

    fn force_refresh(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
        let provider = self.clone();
        Box::pin(async move { provider.credentials(true).await })
    }
}

impl StreamingCredentialProvider for EntraIdProvider {
    fn subscribe(self: Arc<Self>) -> CredentialUpdateStream {
        Box::pin(async_stream::stream! {
            loop {
                let delay = self.delay_until_refresh().await;
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                // Pull through the shared cache after the deadline. Multiple
                // topology listeners therefore coalesce onto one token fetch.
                match self.credentials(false).await {
                    Ok(credentials) => yield Ok(credentials),
                    Err(error) => {
                        yield Err(error);
                        tokio::time::sleep(REFRESH_RETRY_DELAY).await;
                    }
                }
            }
        })
    }
}

fn token_lifetime(expires_at_unix_seconds: i64) -> Result<Duration, RedisError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| provider_error("system clock", error))?
        .as_secs();
    let expires = u64::try_from(expires_at_unix_seconds).map_err(|_| {
        RedisError::Redis("Entra token has an invalid pre-epoch expiry".to_string())
    })?;
    let seconds = expires.checked_sub(now).ok_or_else(|| {
        RedisError::Redis("Entra credential returned an expired access token".to_string())
    })?;
    if seconds == 0 {
        return Err(RedisError::Redis(
            "Entra credential returned an expired access token".to_string(),
        ));
    }
    Ok(Duration::from_secs(seconds))
}

fn refresh_after(lifetime: Duration) -> Duration {
    lifetime
        .checked_mul(REFRESH_FRACTION_NUMERATOR)
        .and_then(|duration| duration.checked_div(REFRESH_FRACTION_DENOMINATOR))
        .unwrap_or(lifetime)
}

fn provider_error(context: &str, error: impl fmt::Display) -> RedisError {
    RedisError::Redis(format!("{context} failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use azure_core::credentials::{AccessToken, TokenRequestOptions};
    use azure_core::time::OffsetDateTime;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct FakeCredential {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl TokenCredential for FakeCredential {
        async fn get_token(
            &self,
            _scopes: &[&str],
            _options: Option<TokenRequestOptions<'_>>,
        ) -> azure_core::Result<AccessToken> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(AccessToken::new(
                format!("token-{call}"),
                OffsetDateTime::now_utc() + azure_core::time::Duration::hours(1),
            ))
        }
    }

    fn provider() -> (EntraIdProvider, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let provider = EntraIdProvider::new(
            "00000000-0000-0000-0000-000000000001",
            Arc::new(FakeCredential {
                calls: Arc::clone(&calls),
            }),
        )
        .unwrap();
        (provider, calls)
    }

    #[tokio::test]
    async fn caches_and_force_refreshes_tokens() {
        let (provider, _) = provider();
        let first = provider.get_credentials().await.unwrap();
        let cached = provider.get_credentials().await.unwrap();
        let refreshed = provider.force_refresh().await.unwrap();
        assert_eq!(first.password, "token-1");
        assert_eq!(cached.password, "token-1");
        assert_eq!(refreshed.password, "token-2");
    }

    #[tokio::test]
    async fn concurrent_force_refreshes_are_coalesced() {
        let (provider, calls) = provider();
        provider.get_credentials().await.unwrap();

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let provider = provider.clone();
            tasks.push(tokio::spawn(async move {
                provider.force_refresh().await.unwrap()
            }));
        }
        for task in tasks {
            assert_eq!(task.await.unwrap().password, "token-2");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn refreshes_at_three_quarters_of_lifetime() {
        assert_eq!(
            refresh_after(Duration::from_secs(3600)),
            Duration::from_secs(2700)
        );
    }

    #[test]
    fn debug_never_contains_access_token() {
        let debug = format!("{:?}", provider().0);
        assert!(debug.contains("00000000-0000-0000-0000-000000000001"));
        assert!(!debug.contains("token-"));
    }
}
