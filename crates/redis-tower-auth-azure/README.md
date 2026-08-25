# redis-tower-auth-azure

Microsoft Entra ID managed-identity credentials for `redis-tower`.

The provider requests the Azure Redis scope, uses the managed identity or
service-principal object ID as the Redis username, caches the access token, and
pushes a replacement at approximately 75% of its observed lifetime.

```rust,ignore
let provider = EntraIdProvider::managed_identity(object_id, None)?;
let factory = CredentialConnectionFactory::new(address, provider.clone())
    .with_tls(hostname, tls_config);
let client = MultiplexedClient::from_factory(
    factory,
    AutoPipelineConfig::default(),
    AutoPipelineReconnectConfig::default(),
).await?;
let auth_handle = client.spawn_credential_reauthentication(Arc::new(provider));
```

`EntraIdProvider::new` also accepts any Azure SDK `TokenCredential` for
workload identity or service-principal authentication. See the workspace
[cloud-auth guide](https://github.com/joshrotenberg/redis-tower/blob/main/docs/CLOUD-AUTH.md) for topology setup and failure
behavior.
