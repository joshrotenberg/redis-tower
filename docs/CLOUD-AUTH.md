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

Use `CredentialConnectionFactory::from_url` when the deployment URL also owns
transport, database, or protocol configuration. The provider replaces only the
URL's username/password; TCP versus TLS versus Unix socket, `db`, and
`protocol` remain part of every initial or replacement connection. Bound the
whole sequence—not just the socket dial—with `with_setup_timeout`:

```rust,ignore
let factory = CredentialConnectionFactory::from_url(
    "rediss://cache.example.net:6380/2?protocol=resp3",
    provider.clone(),
)
.with_connection_config(connection_config)
.with_setup_timeout(Duration::from_secs(5));
```

`ConnectionConfig::connect_timeout` covers the TCP or Unix connect operation.
The factory setup timeout additionally covers provider lookup, TLS, the one
allowed forced refresh, `SELECT`, and `HELLO`. A timeout drops the partial
socket and returns `RedisError::ConnectTimeout`.

## Shared provider ownership

Concrete AWS and Entra providers are cheap clones backed by one internal cache.
Type-erased hosts can use `SharedCredentialProvider` to retain the same property
after converting a provider to `Arc<dyn CredentialProvider>` or
`Arc<dyn StreamingCredentialProvider>`:

```rust,ignore
use std::sync::Arc;
use redis_tower::{
    CredentialConnectionFactory, CredentialProvider, SharedCredentialProvider,
};

let erased: Arc<dyn CredentialProvider> = build_provider();
let shared = SharedCredentialProvider::from_arc(erased);

let ordinary = CredentialConnectionFactory::from_url(redis_url, shared.clone());
let dedicated = CredentialConnectionFactory::from_url(redis_url, shared.clone());
let cluster = MultiplexedClusterClient::builder(cluster_seed)
    .credentials(shared)
    .connect()
    .await?;
```

Every clone delegates to the same allocation, so concurrent ordinary,
dedicated, and Cluster connection attempts share the provider's cache and
single-flight behavior.

## Established sockets and dedicated modes

Credential lookup during connection setup and credential changes after setup
are different contracts:

| Owner | On provider update | Rejected replacement `AUTH` |
|---|---|---|
| Standalone `MultiplexedClient` | Use `spawn_credential_reauthentication` to serialize `AUTH` through its worker | The worker stays installed with its previous Redis identity. The task logs a redacted warning and consumes later updates. For fail-closed rotation, stop admission, shut down the client, and rebuild it through the provider factory before resuming traffic. |
| Retained `ConnectionPool` | Use `spawn_credential_reauthentication` to visit every active slot | Failed slots stay installed with their previous identity while the remaining slots are attempted. For fail-closed rotation, call `close`, drop all clones, and rebuild the pool through the provider factory. |
| Direct or multiplexed Cluster client | Use the owner's `spawn_credential_reauthentication` handle | The failed node socket/worker is removed from routing and a later command reconnects through the provider. |
| Direct `SentinelClient` | Use `spawn_credential_reauthentication` for current data sockets; discovery sockets are short-lived | A failed master is marked for rediscovery before the next command, and failed replicas are removed. |
| `MultiplexedSentinelClient` | Use `spawn_credential_reauthentication` for current data workers | Failed workers stay routed with their previous identity. Stop routing, drop every client clone/update handle, and rebuild the client before resuming traffic when rotation must fail closed. |
| Fresh blocking or transaction connection | Finish/cancel the operation, then reconnect through the provider factory | Inserting `AUTH` into connection-local operation state is not a generic safe boundary. An ambiguous operation is never replayed. |
| Pub/Sub | Reconnect through the provider factory; confirmed subscriptions are replayed | Subscription mode is stateful, and an arbitrary callback must not inject `AUTH`. Messages during the gap are lost. |
| MONITOR | Terminate and create a new `MonitorStream` through the provider factory | MONITOR owns a one-way event stream, has no resume cursor, and loses events during the gap. |

The generic update task is deliberately best-effort: it does not expose an
error channel, renders neither provider nor callback errors, and continues
after a failure. Where an owner exposes `reauthenticate_all`, call it directly
when the result is needed synchronously. The table above is the authoritative
retirement policy; do not assume that every owner evicts a socket after a
rejected update.

`PubSubConnection::connect_with`, `BinaryPubSubConnection::connect_with`, and
`MonitorStream::connect_with` open their dedicated sockets through any
`ConnectionFactory`, including `CredentialConnectionFactory`. A streaming
provider owner can select between its update stream and the session stream:

```rust,ignore
use std::sync::Arc;
use redis_tower::{BinaryPubSubConnection, StreamingCredentialProvider};
use tokio_stream::StreamExt;

let mut pubsub = BinaryPubSubConnection::connect_with(&factory).await?;
pubsub.subscribe_bytes(&[b"events"]).await?;
let mut credential_updates = Arc::clone(&streaming_provider).subscribe();

loop {
    tokio::select! {
        update = credential_updates.next() => {
            update.ok_or("credential stream ended")??;
            // The provider cache is current before it emits. Reconnect rather
            // than sending AUTH in subscription mode.
            pubsub.reconnect_with(&factory).await?;
        }
        message = pubsub.next() => {
            let Some(message) = message else { break };
            handle_message(message?);
        }
    }
}
```

Dropping a `CredentialReauthenticationHandle` cancels and aborts its provider
stream; `shutdown().await` performs the same cancellation and waits for task
exit. Dedicated-session select loops are owned directly by the application and
stop when that owner is dropped.

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

The downstream
[`redis-database-mcp-rs#92`](https://github.com/redis-developer/redis-database-mcp-rs/issues/92)
contract keeps Azure SDK dependencies behind the server's optional Entra
feature. Construct one `EntraIdProvider` in the server layer, wrap or clone it
into every `DirectRedis*` connection factory, and keep static URL
authentication as the no-feature/default path. Its acceptance test injects a
unique token into a failing provider, invokes a real MCP tool, and proves the
serialized tool result or JSON-RPC error, returned metadata, and captured
tracing/log output omit the token while retaining `is_authentication_error`
classification. MCP policy and that tool-boundary evidence stay downstream.

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
`MultiplexedSentinelClient` expose the same owned update mechanism, but their
failure retirement differs. Both Cluster owners remove a failed node from
routing so its normal provider-backed reconnect path can rebuild it. Direct
`SentinelClient` forces rediscovery after a master failure and removes failed
replicas. `MultiplexedSentinelClient` retains a worker that rejects `AUTH` with
its previous identity; stop routing and rebuild that owner to fail closed.

## Secret handling and failure behavior

`Credentials` redacts passwords and tokens from `Debug` output and zeroizes its
owned username/password buffers on drop. The AWS and Azure providers retain
their cached Redis credentials in this type, so replacing or dropping a cached
token zeroizes that allocation. Avoid cloning or logging token strings in
application provider implementations.

Credential update streams are deliberately best-effort. Provider and `AUTH`
errors are logged without rendering third-party error text, and later emissions
are still consumed. Provider failures returned from setup are redacted to an
`AUTH_PROVIDER ... failed` message and are recognized by
`is_authentication_error`; Redis `NOAUTH` and `WRONGPASS` retain their server
classification. Keep the owned handle alive for as long as the client should
receive proactive updates, and call `shutdown().await` during graceful
shutdown. A provider should delay after an emitted error; this prevents a
broken credential source from creating a busy loop.

## Manual Azure Managed Redis verification

The normal suite uses deterministic fake providers; no cloud credential is
required. Before shipping a downstream Entra integration, run this manual
check against a disposable Azure Managed Redis database:

1. Create a Redis data-access-policy assignment for the managed identity or
   service principal and record its object ID. Start with the least-privilege
   command/key policy required by the application.
2. Configure `rediss://<host>:10000/0?protocol=resp3` without URL credentials.
   Build `EntraIdProvider` from the host's Azure `TokenCredential`, then build a
   URL-backed `CredentialConnectionFactory` with a finite setup timeout.
3. Exercise an ordinary multiplexed command, one new dedicated connection, and
   Cluster node discovery if the target exposes Cluster mode. Confirm no token
   appears in logs, errors, traces, or MCP tool output.
4. Leave the process running through one proactive refresh. Verify ordinary
   sockets reauthenticate, a Pub/Sub owner reconnects and restores confirmed
   subscriptions, and a MONITOR owner terminates/reopens with its documented
   observation gap.
5. Revoke or misconfigure the assignment and confirm the application reports a
   redacted authentication-category failure without retrying an ambiguous
   write, transaction, or blocking operation.
