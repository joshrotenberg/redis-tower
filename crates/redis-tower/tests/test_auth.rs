//! Live authentication integration suite (issue #484).
//!
//! Unlike the rest of the standalone integration tests in this crate -- which
//! boot a single no-auth server on 6399 via `common::redis_addr()` -- these
//! tests need a server that enforces `requirepass`. They spin up their own
//! dedicated password-protected `redis-server` (port 6398) behind a `OnceCell`,
//! the same shared-handle pattern `common/mod.rs` uses, so the process is
//! started once and reused across every test in this file.
//!
//! Coverage:
//! - password auth success / wrong-password failure / no-credentials failure
//! - ACL user auth (`ACL SETUSER` + `connect_url` as that user)
//! - a credentialed `ConnectionPool`
//! - re-auth replay through `UrlConnectionFactory` (the classic production bug:
//!   a reconnect silently drops the session's AUTH and every later command
//!   fails with NOAUTH)
//! - provider-backed protected RESP3 negotiation, multiplexed reconnect, and
//!   retained-factory pool replacement
//!
//! These are NOT `#[ignore]`: they boot their own server and require nothing
//! but `redis-server` on `PATH`.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use redis_server_wrapper::RedisServer;
use redis_tower::auto_pipeline::{AutoPipelineConfig, AutoPipelineReconnectConfig};
use redis_tower::commands::*;
use redis_tower::credentials::{CredentialConnectionFactory, CredentialProvider, Credentials};
use redis_tower::pool::{ConnectionPool, PoolConfig};
use redis_tower::reconnect::{ConnectionFactory, ReconnectConfig, UrlConnectionFactory};
use redis_tower::{
    ConnectionConfig, Frame, MultiplexedClient, ProtocolVersion, RedisConnection,
    ResilientConnection,
};
use redis_tower_core::RedisError;
use tokio::sync::OnceCell;

/// Password the dedicated auth server requires (`requirepass`).
const PASSWORD: &str = "s3cr3t";
/// Port for the dedicated auth server. Distinct from `common`'s 6399.
const AUTH_PORT: u16 = 6398;

static AUTH_REDIS: OnceCell<redis_server_wrapper::RedisServerHandle> = OnceCell::const_new();
static AUTH_ADDR: OnceCell<String> = OnceCell::const_new();

/// Test provider whose current password can be rotated without rebuilding the
/// factory. A separate refresh password lets a test prove the factory's one
/// explicit WRONGPASS refresh attempt rather than merely counting normal
/// credential reads.
#[derive(Clone)]
struct TestCredentialProvider {
    current_password: Arc<Mutex<String>>,
    refresh_password: Option<String>,
    get_calls: Arc<AtomicUsize>,
    refresh_calls: Arc<AtomicUsize>,
}

impl TestCredentialProvider {
    fn password(password: impl Into<String>) -> Self {
        Self {
            current_password: Arc::new(Mutex::new(password.into())),
            refresh_password: None,
            get_calls: Arc::new(AtomicUsize::new(0)),
            refresh_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn with_refresh(
        current_password: impl Into<String>,
        refresh_password: impl Into<String>,
    ) -> Self {
        Self {
            refresh_password: Some(refresh_password.into()),
            ..Self::password(current_password)
        }
    }

    fn set_password(&self, password: impl Into<String>) {
        *self.current_password.lock().unwrap() = password.into();
    }

    fn get_calls(&self) -> usize {
        self.get_calls.load(Ordering::SeqCst)
    }

    fn refresh_calls(&self) -> usize {
        self.refresh_calls.load(Ordering::SeqCst)
    }

    fn credentials(&self) -> Credentials {
        Credentials::new("default", self.current_password.lock().unwrap().clone())
    }
}

impl CredentialProvider for TestCredentialProvider {
    fn get_credentials(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
        self.get_calls.fetch_add(1, Ordering::SeqCst);
        let credentials = self.credentials();
        Box::pin(async move { Ok(credentials) })
    }

    fn force_refresh(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<Credentials, RedisError>> + Send>> {
        self.refresh_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(password) = &self.refresh_password {
            self.set_password(password.clone());
        }
        let credentials = self.credentials();
        Box::pin(async move { Ok(credentials) })
    }
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port for an auth integration server");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn hello_protocol(frame: &Frame) -> Option<i64> {
    match frame {
        Frame::Map(entries) => entries
            .iter()
            .find(|(key, _)| key.as_str() == Some("proto"))
            .and_then(|(_, value)| value.as_integer()),
        Frame::Array(Some(items)) => items
            .chunks_exact(2)
            .find(|pair| pair[0].as_str() == Some("proto"))
            .and_then(|pair| pair[1].as_integer()),
        _ => None,
    }
}

/// Address (`host:port`) of the shared password-protected server, started once.
async fn auth_addr() -> &'static str {
    AUTH_ADDR
        .get_or_init(|| async {
            let handle = RedisServer::new()
                .port(AUTH_PORT)
                .password(PASSWORD)
                .start()
                .await
                .expect("failed to start password-protected Redis server");
            let addr = handle.addr();
            AUTH_REDIS.set(handle).ok();
            addr
        })
        .await
}

/// A `redis://default:<pass>@host:port` URL for the shared auth server.
async fn auth_url() -> String {
    format!("redis://default:{PASSWORD}@{}", auth_addr().await)
}

/// `connect_url` with the right password connects and round-trips SET/GET.
#[tokio::test]
async fn password_auth_success() {
    let url = auth_url().await;
    let mut conn = RedisConnection::connect_url(&url)
        .await
        .expect("connect_url with correct password should succeed");

    let k = "test:auth:password_success";
    conn.execute(Set::new(k, "hello")).await.unwrap();
    let val: Option<Bytes> = conn.execute(Get::new(k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("hello")));
    conn.execute(Del::new(k)).await.unwrap();
}

/// A wrong password must surface a real auth error -- not a panic, not a
/// silently-usable connection. `connect_url` sends AUTH during connect, so the
/// server's `-WRONGPASS` reply turns into `Err(RedisError::Redis(_))` right at
/// connect time. Security-critical: the test asserts the failure is observed.
#[tokio::test]
async fn password_auth_wrong_password_fails() {
    let addr = auth_addr().await;
    let url = format!("redis://default:WRONG@{addr}");

    // `RedisConnection` is not `Debug`, so use a let-else rather than
    // `expect_err` to assert the connect failed without a usable connection.
    let Err(err) = RedisConnection::connect_url(&url).await else {
        panic!("connect_url with a wrong password must return Err, never a usable connection");
    };
    // The server rejects AUTH with -WRONGPASS, delivered as RedisError::Redis.
    match err {
        RedisError::Redis(msg) => {
            let upper = msg.to_uppercase();
            assert!(
                upper.contains("WRONGPASS") || upper.contains("AUTH") || upper.contains("PASSWORD"),
                "expected an auth-related server error, got: {msg}"
            );
        }
        other => panic!("expected RedisError::Redis (server auth rejection), got: {other:?}"),
    }
}

/// No credentials at all against a `requirepass` server. `connect_url` with no
/// password skips AUTH, and `CLIENT SETINFO` errors during connect are ignored,
/// so the connect itself may succeed -- but the first real command comes back
/// `-NOAUTH`. Assert the command-issuing path observes a genuine error.
#[tokio::test]
async fn no_credentials_against_protected_server_fails() {
    let addr = auth_addr().await;
    let url = format!("redis://{addr}");

    // Connect may succeed (no AUTH is sent); the command must not.
    let mut conn = match RedisConnection::connect_url(&url).await {
        Ok(conn) => conn,
        Err(e) => {
            // Some servers reject pre-auth; that is also an acceptable failure.
            match e {
                RedisError::Redis(_)
                | RedisError::Connection { .. }
                | RedisError::ConnectionClosed => {
                    return;
                }
                other => panic!("unexpected connect error against protected server: {other:?}"),
            }
        }
    };

    let err = conn
        .execute(Set::new("test:auth:nocreds", "x"))
        .await
        .expect_err("a command on an unauthenticated connection to a protected server must fail");
    match err {
        RedisError::Redis(msg) => {
            let upper = msg.to_uppercase();
            assert!(
                upper.contains("NOAUTH") || upper.contains("AUTH"),
                "expected a NOAUTH server error, got: {msg}"
            );
        }
        other => panic!("expected RedisError::Redis (NOAUTH), got: {other:?}"),
    }
}

/// Create an ACL user with a default-authed connection, then authenticate AS
/// that user via `connect_url` and confirm `ACL WHOAMI` reports the new user.
#[tokio::test]
async fn acl_user_auth() {
    let addr = auth_addr().await;

    // Admin connection (default user, password auth) creates the ACL user.
    let admin_url = auth_url().await;
    let mut admin = RedisConnection::connect_url(&admin_url)
        .await
        .expect("admin connect should succeed");
    admin
        .execute(
            AclSetUser::new("alice")
                .rule("on")
                .rule(">alicepw")
                .rule("~*")
                .rule("+@all"),
        )
        .await
        .expect("ACL SETUSER alice should succeed");

    // Connect as alice and verify identity.
    let alice_url = format!("redis://alice:alicepw@{addr}");
    let mut alice = RedisConnection::connect_url(&alice_url)
        .await
        .expect("connect as alice should succeed");
    let who: String = alice
        .execute(AclWhoAmI::new())
        .await
        .expect("ACL WHOAMI should succeed");
    assert_eq!(
        who, "alice",
        "ACL WHOAMI should report the authenticated user"
    );

    // alice has +@all ~*, so a normal command works too.
    alice
        .execute(Set::new("test:auth:alice", "v"))
        .await
        .unwrap();
    let val: Option<Bytes> = alice.execute(Get::new("test:auth:alice")).await.unwrap();
    assert_eq!(val, Some(Bytes::from("v")));
    alice.execute(Del::new("test:auth:alice")).await.unwrap();

    admin.execute(AclDelUser::new("alice")).await.unwrap();
}

/// A `ConnectionPool` whose connections each authenticate via a credentialed
/// `connect_url`. Every pooled connection runs AUTH on creation; a command
/// through the pool must succeed.
#[tokio::test]
async fn pool_with_credentials() {
    let url = auth_url().await;
    let pool = ConnectionPool::connect(3, || {
        let url = url.clone();
        async move { RedisConnection::connect_url(&url).await }
    })
    .await
    .expect("credentialed pool should build (each connection authenticates)");

    assert_eq!(pool.size(), 3);

    let k = "test:auth:pool";
    pool.execute(Set::new(k, "pooled")).await.unwrap();
    let val: Option<Bytes> = pool.execute(Get::new(k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("pooled")));
    pool.execute(Del::new(k)).await.unwrap();
}

/// The classic production reconnect bug: after a reconnect the new socket must
/// replay AUTH, or every later command fails with NOAUTH. `UrlConnectionFactory`
/// calls `connect_url` on every `connect()`, so each connection it produces is
/// already authenticated. We assert that twice -- two independent connections
/// from the same factory -- which is exactly what the reconnect path relies on
/// (a reconnect is just another `factory.connect()`).
#[tokio::test]
async fn reconnect_replays_auth() {
    let url = auth_url().await;
    let factory = UrlConnectionFactory::new(url);

    // First connection from the factory: already authenticated, no manual AUTH.
    let mut first = factory
        .connect()
        .await
        .expect("factory's first connection should authenticate via the URL");
    first
        .execute(Set::new("test:auth:reconnect", "v1"))
        .await
        .expect("command on a factory-authed connection should succeed without manual AUTH");

    // Simulate what a reconnect does: ask the factory for a fresh connection.
    // It must also be authenticated -- this is the property that makes
    // reconnect-with-auth correct.
    let mut second = factory
        .connect()
        .await
        .expect("factory's second (reconnect-equivalent) connection should also authenticate");
    let val: Option<Bytes> = second
        .execute(Get::new("test:auth:reconnect"))
        .await
        .expect("command on the reconnected connection should succeed without manual AUTH");
    assert_eq!(val, Some(Bytes::from("v1")));

    second
        .execute(Del::new("test:auth:reconnect"))
        .await
        .unwrap();
}

/// The multiplexed client's credentialed `connect_url` path authenticates the
/// same way the basic connection does.
#[tokio::test]
async fn multiplexed_password_auth() {
    let url = auth_url().await;
    let client = MultiplexedClient::connect_url(&url)
        .await
        .expect("MultiplexedClient::connect_url with credentials should succeed");

    let k = "test:auth:mux";
    client.execute(Set::new(k, "mux")).await.unwrap();
    let val: Option<Bytes> = client.execute(Get::new(k)).await.unwrap();
    assert_eq!(val, Some(Bytes::from("mux")));
    client.execute(Del::new(k)).await.unwrap();
}

/// A protected RESP3 connection has to authenticate while the socket is still
/// in its RESP2 bootstrap state, then negotiate RESP3. This also proves that a
/// stale provider result gets exactly one explicit refresh on WRONGPASS.
#[tokio::test]
async fn provider_factory_refreshes_then_negotiates_protected_resp3() {
    let port = free_port();
    let correct_password = "provider-resp3-current";
    let _server = RedisServer::new()
        .port(port)
        .password(correct_password)
        .start()
        .await
        .expect("start protected RESP3 test server");
    let addr = format!("127.0.0.1:{port}");

    let provider = TestCredentialProvider::with_refresh("provider-resp3-stale", correct_password);
    let factory = CredentialConnectionFactory::new(&addr, provider.clone())
        .with_connection_config(ConnectionConfig::default().with_protocol(ProtocolVersion::Resp3));
    let mut conn = ConnectionFactory::connect(&factory)
        .await
        .expect("one provider refresh should authenticate before RESP3 negotiation");

    let hello = conn
        .execute(Hello::new())
        .await
        .expect("authenticated RESP3 connection should accept HELLO");
    assert!(
        matches!(hello, Frame::Map(_)),
        "forced RESP3 HELLO should use a map response, got {hello:?}"
    );
    assert_eq!(hello_protocol(&hello), Some(3));
    assert_eq!(provider.get_calls(), 1, "initial connect should fetch once");
    assert_eq!(
        provider.refresh_calls(),
        1,
        "WRONGPASS should trigger exactly one explicit refresh"
    );
}

/// `ResilientConnection` reconnects only when its Tower readiness state is
/// driven after a transport error. The retained provider-backed factory must
/// fetch the rotated password before readiness becomes successful again.
#[tokio::test]
async fn resilient_connection_reconnect_fetches_rotated_provider_credentials() {
    if std::env::var_os("REDIS_EXTERNAL_SERVICE").is_some() {
        return;
    }

    let port = free_port();
    let first_password = "provider-resilient-first";
    let second_password = "provider-resilient-second";
    let server = RedisServer::new()
        .port(port)
        .password(first_password)
        .start()
        .await
        .expect("start initial protected resilient server");
    let addr = format!("127.0.0.1:{port}");

    let provider = TestCredentialProvider::password(first_password);
    let factory = CredentialConnectionFactory::new(&addr, provider.clone());
    let mut conn = ResilientConnection::new(
        factory,
        ReconnectConfig::default()
            .base_delay(Duration::from_millis(20))
            .max_delay(Duration::from_millis(100))
            .connect_timeout(Duration::from_secs(1))
            .jitter(false),
    )
    .await
    .expect("initial provider-backed resilient connection should authenticate");
    assert_eq!(conn.execute(Ping::new()).await.unwrap(), "PONG");
    assert_eq!(provider.get_calls(), 1);

    server.stop();
    let first_error = conn
        .execute(Ping::new())
        .await
        .expect_err("the original socket should report the stopped server");
    assert!(
        first_error.is_retryable(),
        "server stop should surface a retryable transport error, got {first_error:?}"
    );
    // A peer reset can first surface from the RESP codec as Protocol(Io),
    // while the now-drained socket reports ConnectionClosed on the next call.
    // Drive that documented one-request detection window before readiness.
    if !first_error.is_connection_error() {
        let closed = conn
            .execute(Ping::new())
            .await
            .expect_err("the reset socket should be closed after its first I/O error");
        assert!(
            closed.is_connection_error(),
            "the follow-up request should mark the socket replaceable, got {closed:?}"
        );
    }

    provider.set_password(second_password);
    let _restarted = RedisServer::new()
        .port(port)
        .password(second_password)
        .start()
        .await
        .expect("restart protected resilient server with rotated password");

    tokio::time::timeout(
        Duration::from_secs(5),
        std::future::poll_fn(|cx| {
            <ResilientConnection as tower::Service<Ping>>::poll_ready(&mut conn, cx)
        }),
    )
    .await
    .expect("resilient readiness should recover before the test deadline")
    .expect("resilient readiness should reconnect successfully");
    assert_eq!(conn.execute(Ping::new()).await.unwrap(), "PONG");
    assert_eq!(
        provider.get_calls(),
        2,
        "initial and replacement connections should each fetch credentials"
    );
    assert_eq!(provider.refresh_calls(), 0);
}

/// The auto-pipeline owns its socket, so reconnect authentication must come
/// from its retained provider-backed factory. Restarting Redis with a new
/// password proves that the worker fetches the rotated credential rather than
/// replaying a token captured when the client was constructed.
#[tokio::test]
async fn multiplexed_reconnect_fetches_rotated_provider_credentials() {
    if std::env::var_os("REDIS_EXTERNAL_SERVICE").is_some() {
        return;
    }

    let port = free_port();
    let first_password = "provider-mux-first";
    let second_password = "provider-mux-second";
    let server = RedisServer::new()
        .port(port)
        .password(first_password)
        .start()
        .await
        .expect("start initial protected multiplexed server");
    let addr = format!("127.0.0.1:{port}");

    let provider = TestCredentialProvider::password(first_password);
    let factory = CredentialConnectionFactory::new(&addr, provider.clone());
    let client = MultiplexedClient::from_factory(
        factory,
        AutoPipelineConfig {
            response_timeout: Some(Duration::from_secs(1)),
            ..AutoPipelineConfig::default()
        },
        AutoPipelineReconnectConfig::new(
            ReconnectConfig::default()
                .base_delay(Duration::from_millis(20))
                .max_delay(Duration::from_millis(100))
                .connect_timeout(Duration::from_secs(1))
                .jitter(false),
        ),
    )
    .await
    .expect("initial provider-backed multiplexed connection should authenticate");

    let pong: String = client.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
    assert_eq!(provider.get_calls(), 1);

    server.stop();
    provider.set_password(second_password);
    let _restarted = RedisServer::new()
        .port(port)
        .password(second_password)
        .start()
        .await
        .expect("restart protected multiplexed server with rotated password");

    // The first request detects the dead original socket. Later requests are
    // accepted after the worker installs a freshly authenticated connection.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    tokio::time::timeout_at(deadline, async {
        loop {
            match client.execute(Ping::new()).await {
                Ok(pong) if pong == "PONG" && provider.get_calls() >= 2 => break,
                Ok(other) => panic!("unexpected PING response after reconnect: {other}"),
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
    })
    .await
    .expect("multiplexed client did not reconnect with rotated credentials within 5s");

    assert_eq!(
        provider.refresh_calls(),
        0,
        "normal reconnect should fetch current credentials without forced refresh"
    );
    client.shutdown().await;
}

/// A factory-backed pool retains its factory specifically so a dead slot can
/// be replaced after a failed lazy health PING. The replacement must fetch the
/// provider's current password; retaining only the first authenticated socket
/// (or its original token) would fail this command.
#[tokio::test]
async fn pool_replacement_fetches_rotated_provider_credentials() {
    if std::env::var_os("REDIS_EXTERNAL_SERVICE").is_some() {
        return;
    }

    let port = free_port();
    let first_password = "provider-pool-first";
    let second_password = "provider-pool-second";
    let server = RedisServer::new()
        .port(port)
        .password(first_password)
        .start()
        .await
        .expect("start initial protected pool server");
    let addr = format!("127.0.0.1:{port}");

    let provider = TestCredentialProvider::password(first_password);
    let factory = CredentialConnectionFactory::new(&addr, provider.clone());
    let pool = ConnectionPool::connect_with_factory(
        PoolConfig::default()
            .size(1)
            .health_check_interval(Duration::from_millis(1)),
        factory,
    )
    .await
    .expect("build provider-backed pool");

    let pong: String = pool.execute(Ping::new()).await.unwrap();
    assert_eq!(pong, "PONG");
    assert_eq!(provider.get_calls(), 1);

    server.stop();
    provider.set_password(second_password);
    let _restarted = RedisServer::new()
        .port(port)
        .password(second_password)
        .start()
        .await
        .expect("restart protected pool server with rotated password");

    let pong: String = pool
        .execute(Ping::new())
        .await
        .expect("failed health PING should install an authenticated replacement");
    assert_eq!(pong, "PONG");
    assert_eq!(
        provider.get_calls(),
        2,
        "initial slot and its replacement should each fetch credentials"
    );
    assert_eq!(provider.refresh_calls(), 0);
}
