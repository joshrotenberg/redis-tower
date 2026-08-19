# Cloud and rotating credentials

redis-tower separates two credential-rotation moments:

1. Every fresh socket asks a `CredentialProvider` for current credentials. A
   `CredentialConnectionFactory`, Cluster builder, or Sentinel builder carries
   that provider through initial connect, reconnect, pool growth, topology
   discovery, and failover.
2. A `StreamingCredentialProvider` emits replacements for sockets that remain
   open. An owned reauthentication handle sends `AUTH` to every current data
   socket and stops when the handle is dropped or shut down.

Connection setup always opens in RESP2, authenticates, and only then negotiates
the requested protocol. If Redis rejects setup credentials with `NOAUTH` or
`WRONGPASS`, the provider is force-refreshed and `AUTH` is attempted once more.
This bounded retry applies only to connection setup. redis-tower never replays a
user command after an authentication error.

## AWS ElastiCache IAM

`redis-tower-auth-aws` implements the ElastiCache IAM SigV4 flow for provisioned
replication groups and serverless caches. It creates a presigned
`elasticache:Connect` request with the documented 15-minute lifetime, caches it
for reconnect fan-out, and emits a replacement after 75% of that lifetime.

ElastiCache IAM requires TLS. The provider creates credentials; the client must
still be configured with the appropriate TLS hostname and trust roots.

```rust,ignore
use std::sync::Arc;
use aws_config::BehaviorVersion;
use redis_tower::{
    AutoPipelineConfig, ConnectionConfig, CredentialConnectionFactory,
    MultiplexedClient, ProtocolVersion, StreamingCredentialProvider,
};
use redis_tower::auto_pipeline::AutoPipelineReconnectConfig;
use redis_tower_auth_aws::{
    ElastiCacheIamProvider, ElastiCacheResourceType,
};

let aws = aws_config::load_defaults(BehaviorVersion::latest()).await;
let provider = ElastiCacheIamProvider::new(
    "redis-app",
    "production-cache",
    "us-west-2",
    ElastiCacheResourceType::ReplicationGroup,
    aws.credentials_provider().expect("AWS credentials").clone(),
)?;

let factory = CredentialConnectionFactory::new(
    "production-cache.example.cache.amazonaws.com:6379",
    provider.clone(),
)
.with_connection_config(
    ConnectionConfig::new().with_protocol(ProtocolVersion::Resp3),
)
.with_tls("production-cache.example.cache.amazonaws.com", tls_config);

let client = MultiplexedClient::from_factory(
    factory,
    AutoPipelineConfig::default(),
    AutoPipelineReconnectConfig::default(),
).await?;
let updates: Arc<dyn StreamingCredentialProvider> = Arc::new(provider);
let auth_handle = client.spawn_credential_reauthentication(updates);
```

Use `ElastiCacheResourceType::ServerlessCache` for serverless ElastiCache. The
signed request then includes `ResourceType=ServerlessCache`. The IAM user name
and user ID must match, and the IAM policy must permit both the cache resource
and user resource.

## Microsoft Entra ID

`redis-tower-auth-azure` requests the standard Azure Redis scope,
`https://redis.azure.com/.default`. The managed identity or service-principal
object ID becomes the Redis username and its access token becomes the password.
The provider caches a token and emits its replacement at approximately 75% of
the observed lifetime.

```rust,ignore
use std::sync::Arc;
use redis_tower::{
    AutoPipelineConfig, CredentialConnectionFactory, MultiplexedClient,
    StreamingCredentialProvider,
};
use redis_tower::auto_pipeline::AutoPipelineReconnectConfig;
use redis_tower_auth_azure::EntraIdProvider;

let provider = EntraIdProvider::managed_identity(
    "00000000-0000-0000-0000-000000000000",
    None,
)?;
let factory = CredentialConnectionFactory::new(
    "my-cache.westus.redis.azure.net:6380",
    provider.clone(),
)
.with_tls("my-cache.westus.redis.azure.net", tls_config);

let client = MultiplexedClient::from_factory(
    factory,
    AutoPipelineConfig::default(),
    AutoPipelineReconnectConfig::default(),
).await?;
let updates: Arc<dyn StreamingCredentialProvider> = Arc::new(provider);
let auth_handle = client.spawn_credential_reauthentication(updates);
```

`EntraIdProvider::new` accepts any Azure SDK `TokenCredential`, so workload
identity and service-principal credentials use the same cache and push stream.
`with_scope` is available for sovereign-cloud or compatibility deployments.

## Pools, Cluster, and Sentinel

Use one cloneable provider instance for both socket creation and streaming
updates. Provider clones share their token cache.

```rust,ignore
// Pool: the factory covers new/replacement slots; the handle covers live slots.
let factory = CredentialConnectionFactory::new(address, provider.clone());
let pool = ConnectionPool::connect_with_factory(pool_config, factory).await?;
let auth_handle = pool.spawn_credential_reauthentication(Arc::new(provider.clone()));

// Cluster: every node setup/reconnect fetches credentials.
let cluster = MultiplexedClusterClient::builder(seed)
    .credentials(provider.clone())
    .connect()
    .await?;
let auth_handle = cluster.spawn_credential_reauthentication(Arc::new(provider.clone()));

// Sentinel discovery credentials are independent from Redis data credentials.
// Only data sockets persist, so the update handle targets node credentials.
let sentinel = SentinelClient::builder(sentinels, service_name)
    .sentinel_credentials(sentinel_provider)
    .node_credentials(provider.clone())
    .connect()
    .await?;
let auth_handle = sentinel.spawn_credential_reauthentication(Arc::new(provider));
```

Direct `ClusterClient`, `SentinelClient`, `MultiplexedClusterClient`, and
`MultiplexedSentinelClient` expose the same owned update mechanism. A failed
Cluster node reauthentication removes that socket from routing so its normal
provider-backed reconnect path can rebuild it. Sentinel master failures force
rediscovery; failed replica sockets are removed.

## Secret handling and failure behavior

`Credentials` redacts passwords and tokens from `Debug` output and zeroizes its
owned username/password buffers on drop. The AWS and Azure providers retain
their cached Redis credentials in this type, so replacing or dropping a cached
token zeroizes that allocation. Avoid cloning or logging token strings in
application provider implementations.

Credential update streams are deliberately best-effort. Provider and `AUTH`
errors are logged and later emissions are still consumed. Keep the owned handle
alive for as long as the client should receive proactive updates, and call
`shutdown().await` during graceful shutdown. A provider should delay after an
emitted error; this prevents a broken credential source from creating a busy
loop.
