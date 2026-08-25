# redis-tower-auth-aws

AWS ElastiCache IAM credentials for `redis-tower`.

The provider signs the documented `elasticache:Connect` request with SigV4,
uses the resulting 15-minute presigned URL as the Redis password, and exposes a
push stream that renews established connections before expiry.

ElastiCache IAM requires TLS. Configure the corresponding `redis-tower` client
or topology builder with a TLS backend and pass the same provider to both its
connection factory and push-reauthentication handle.

```rust,ignore
let provider = ElastiCacheIamProvider::new(
    "redis-app",
    "production-cache",
    "us-west-2",
    ElastiCacheResourceType::ReplicationGroup,
    aws_credentials,
)?;
let factory = CredentialConnectionFactory::new(address, provider.clone())
    .with_tls(hostname, tls_config);
let client = MultiplexedClient::from_factory(
    factory,
    AutoPipelineConfig::default(),
    AutoPipelineReconnectConfig::default(),
).await?;
let auth_handle = client.spawn_credential_reauthentication(Arc::new(provider));
```

See the workspace [cloud-auth guide](https://github.com/joshrotenberg/redis-tower/blob/main/docs/CLOUD-AUTH.md) for Cluster,
Sentinel, and pool setup plus security and failure behavior.
