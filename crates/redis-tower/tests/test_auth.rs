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
//!
//! These are NOT `#[ignore]`: they boot their own server and require nothing
//! but `redis-server` on `PATH`.

use bytes::Bytes;
use redis_server_wrapper::RedisServer;
use redis_tower::RedisConnection;
use redis_tower::commands::*;
use redis_tower::pool::ConnectionPool;
use redis_tower::reconnect::{ConnectionFactory, UrlConnectionFactory};
use redis_tower_core::RedisError;
use tokio::sync::OnceCell;

/// Password the dedicated auth server requires (`requirepass`).
const PASSWORD: &str = "s3cr3t";
/// Port for the dedicated auth server. Distinct from `common`'s 6399.
const AUTH_PORT: u16 = 6398;

static AUTH_REDIS: OnceCell<redis_server_wrapper::RedisServerHandle> = OnceCell::const_new();
static AUTH_ADDR: OnceCell<String> = OnceCell::const_new();

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
                RedisError::Redis(_) | RedisError::Connection(_) | RedisError::ConnectionClosed => {
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
    use redis_tower::MultiplexedClient;

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
