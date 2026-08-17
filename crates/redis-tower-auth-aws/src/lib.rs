#![deny(missing_docs)]
//! AWS IAM authentication for Amazon ElastiCache.
//!
//! [`ElastiCacheIamProvider`] implements both [`CredentialProvider`] and
//! [`StreamingCredentialProvider`].
//! It generates the documented SigV4 `elasticache:Connect` presigned request,
//! caches it for reconnect fan-out, and emits a fresh credential before the
//! 15-minute token expires.
//!
//! ElastiCache IAM requires in-transit encryption. This crate produces
//! credentials only; callers must configure TLS on their standalone, Cluster,
//! or Sentinel client.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sigv4::http_request::{
    SignableBody, SignableRequest, SignatureLocation, SigningParams, SigningSettings, sign,
};
use redis_tower::RedisError;
use redis_tower::credentials::{
    CredentialProvider, CredentialUpdateStream, Credentials, StreamingCredentialProvider,
};
use tokio::sync::Mutex;
use url::Url;

/// Maximum lifetime of an ElastiCache IAM authentication token.
pub const IAM_TOKEN_TTL: Duration = Duration::from_secs(15 * 60);

/// Point at which the provider replaces a cached token and emits an update.
///
/// This is 75% of the documented 15-minute lifetime, leaving 225 seconds for
/// temporary credential-source or network failures before token expiry.
pub const IAM_TOKEN_REFRESH_AFTER: Duration = Duration::from_secs(675);

const REFRESH_RETRY_DELAY: Duration = Duration::from_secs(5);
const SERVICE_NAME: &str = "elasticache";

/// ElastiCache resource shape used by IAM authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ElastiCacheResourceType {
    /// A provisioned replication group.
    ReplicationGroup,
    /// An ElastiCache Serverless cache.
    ServerlessCache,
}

struct CachedToken {
    credentials: Credentials,
    generated_at: Instant,
    refresh_at: Instant,
}

struct Inner {
    user_id: String,
    cache_name: String,
    region: String,
    resource_type: ElastiCacheResourceType,
    aws_credentials: SharedCredentialsProvider,
    cached: Mutex<Option<CachedToken>>,
}

/// SigV4-backed credential provider for ElastiCache IAM authentication.
///
/// The ElastiCache user name and user ID must be identical. `cache_name` is
/// the replication-group or serverless-cache name used as the presigned
/// request host; it is normalized to lowercase as required by ElastiCache.
#[derive(Clone)]
pub struct ElastiCacheIamProvider {
    inner: Arc<Inner>,
}

impl fmt::Debug for ElastiCacheIamProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ElastiCacheIamProvider")
            .field("user_id", &self.inner.user_id)
            .field("cache_name", &self.inner.cache_name)
            .field("region", &self.inner.region)
            .field("resource_type", &self.inner.resource_type)
            .finish_non_exhaustive()
    }
}

impl ElastiCacheIamProvider {
    /// Create a provider from any AWS SDK credentials provider.
    ///
    /// Use [`ElastiCacheResourceType::ServerlessCache`] for serverless caches;
    /// its signed request includes `ResourceType=ServerlessCache`.
    pub fn new(
        user_id: impl Into<String>,
        cache_name: impl Into<String>,
        region: impl Into<String>,
        resource_type: ElastiCacheResourceType,
        aws_credentials: impl ProvideCredentials + 'static,
    ) -> Result<Self, RedisError> {
        let user_id = user_id.into();
        let cache_name = cache_name.into().to_ascii_lowercase();
        let region = region.into();
        validate_component("user ID", &user_id)?;
        validate_component("cache name", &cache_name)?;
        validate_component("AWS region", &region)?;
        if cache_name.contains(['/', ':', '?', '#']) {
            return Err(RedisError::Redis(
                "ElastiCache cache name must not contain URI delimiters".to_string(),
            ));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                user_id,
                cache_name,
                region,
                resource_type,
                aws_credentials: SharedCredentialsProvider::new(aws_credentials),
                cached: Mutex::new(None),
            }),
        })
    }

    /// Return the ElastiCache IAM user ID used as the Redis ACL username.
    pub fn user_id(&self) -> &str {
        &self.inner.user_id
    }

    /// Return the normalized cache name used in the SigV4 request.
    pub fn cache_name(&self) -> &str {
        &self.inner.cache_name
    }

    /// Return the AWS signing region.
    pub fn region(&self) -> &str {
        &self.inner.region
    }

    async fn credentials(&self, force: bool) -> Result<Credentials, RedisError> {
        let requested_at = Instant::now();
        let mut cached = self.inner.cached.lock().await;
        if let Some(token) = cached.as_ref()
            && Instant::now() < token.refresh_at
            && (!force || token.generated_at >= requested_at)
        {
            // A force-refresh caller reuses a token generated after that call
            // began. Concurrent AUTH rejections therefore single-flight while
            // a later rejection of the replacement still forces another fetch.
            return Ok(token.credentials.clone());
        }

        let credentials = self.sign_at(SystemTime::now()).await?;
        let generated_at = Instant::now();
        *cached = Some(CachedToken {
            credentials: credentials.clone(),
            generated_at,
            refresh_at: generated_at + IAM_TOKEN_REFRESH_AFTER,
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

    async fn sign_at(&self, now: SystemTime) -> Result<Credentials, RedisError> {
        let sdk_credentials = self
            .inner
            .aws_credentials
            .provide_credentials()
            .await
            .map_err(|error| provider_error("AWS credential resolution", error))?;
        let identity = sdk_credentials.into();

        let mut url = Url::parse(&format!("http://{}/", self.inner.cache_name))
            .map_err(|error| provider_error("ElastiCache IAM request URL", error))?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("Action", "connect");
            query.append_pair("User", &self.inner.user_id);
            if self.inner.resource_type == ElastiCacheResourceType::ServerlessCache {
                query.append_pair("ResourceType", "ServerlessCache");
            }
        }

        let mut settings = SigningSettings::default();
        settings.signature_location = SignatureLocation::QueryParams;
        settings.expires_in = Some(IAM_TOKEN_TTL);
        let params: SigningParams<'_> = aws_sigv4::sign::v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.inner.region)
            .name(SERVICE_NAME)
            .time(now)
            .settings(settings)
            .build()
            .map_err(|error| provider_error("ElastiCache IAM signing parameters", error))?
            .into();

        let signable = SignableRequest::new(
            "GET",
            url.as_str(),
            std::iter::empty(),
            SignableBody::Bytes(&[]),
        )
        .map_err(|error| provider_error("ElastiCache IAM signable request", error))?;
        let (instructions, _) = sign(signable, &params)
            .map_err(|error| provider_error("ElastiCache IAM signing", error))?
            .into_parts();
        let mut request = http::Request::builder()
            .method("GET")
            .uri(url.as_str())
            .body(())
            .map_err(|error| provider_error("ElastiCache IAM HTTP request", error))?;
        instructions.apply_to_request_http1x(&mut request);
        let token = request
            .uri()
            .to_string()
            .strip_prefix("http://")
            .ok_or_else(|| {
                RedisError::Redis(
                    "ElastiCache IAM signer returned an unexpected URI scheme".to_string(),
                )
            })?
            .to_string();
        Ok(Credentials::new(&self.inner.user_id, token))
    }
}

impl CredentialProvider for ElastiCacheIamProvider {
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

impl StreamingCredentialProvider for ElastiCacheIamProvider {
    fn subscribe(self: Arc<Self>) -> CredentialUpdateStream {
        Box::pin(async_stream::stream! {
            loop {
                let delay = self.delay_until_refresh().await;
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                // Pull through the shared cache after the deadline. When
                // several subscribers wake together, the mutex lets the first
                // refresh and the rest reuse that same new token.
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

fn validate_component(name: &str, value: &str) -> Result<(), RedisError> {
    if value.trim().is_empty() {
        Err(RedisError::Redis(format!(
            "ElastiCache IAM {name} must not be empty"
        )))
    } else {
        Ok(())
    }
}

fn provider_error(context: &str, error: impl fmt::Display) -> RedisError {
    RedisError::Redis(format!("{context} failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_credential_types::Credentials as AwsCredentials;

    fn provider(resource_type: ElastiCacheResourceType) -> ElastiCacheIamProvider {
        ElastiCacheIamProvider::new(
            "redis-user",
            "Example-Cache",
            "us-east-1",
            resource_type,
            AwsCredentials::new(
                "AKIDEXAMPLE",
                "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
                None,
                None,
                "unit-test",
            ),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn signs_documented_replication_group_shape_for_fifteen_minutes() {
        let provider = provider(ElastiCacheResourceType::ReplicationGroup);
        let credentials = provider
            .sign_at(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000))
            .await
            .unwrap();
        assert_eq!(credentials.username.as_deref(), Some("redis-user"));
        assert!(
            credentials
                .password
                .starts_with("example-cache/?Action=connect&User=redis-user")
        );
        assert!(credentials.password.contains("X-Amz-Expires=900"));
        assert!(!credentials.password.contains("ResourceType"));
    }

    #[tokio::test]
    async fn serverless_signature_includes_resource_type() {
        let provider = provider(ElastiCacheResourceType::ServerlessCache);
        let credentials = provider
            .sign_at(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000))
            .await
            .unwrap();
        assert!(
            credentials
                .password
                .contains("ResourceType=ServerlessCache")
        );
    }

    #[test]
    fn debug_omits_aws_credentials() {
        let debug = format!("{:?}", provider(ElastiCacheResourceType::ReplicationGroup));
        assert!(debug.contains("redis-user"));
        assert!(!debug.contains("AKIDEXAMPLE"));
        assert!(!debug.contains("EXAMPLEKEY"));
    }
}
