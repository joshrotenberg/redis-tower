//! Standalone integration tests that self-provision their infrastructure.
//!
//! The TLS tests start a TLS-enabled, password-protected `redis-server` with a
//! generated private CA, the same mechanism the cluster suite uses in CI. The
//! client trusts that CA and keeps hostname verification enabled; positive
//! tests therefore prove more than encrypted bytes reached an insecure test
//! endpoint.
//!
//! TLS coverage requires either the `tls-rustls` or `tls-native-tls` feature.
//! The standalone integration job runs `--all-features`, exercising both:
//!
//! ```bash
//! cargo test -p redis-tower --test test_infrastructure --features tls-rustls
//! cargo test -p redis-tower --test test_infrastructure --features tls-native-tls
//! ```
//!
//! Cluster and sentinel integration tests live in the dedicated
//! `redis-tower-cluster` and `redis-tower-sentinel` crates.

// ---------------------------------------------------------------------------
// TLS tests (#153, #473)
//
// Each TLS test owns an independently allocated port, certificate directory,
// and server handle. Test startup is serialized because the process wrapper
// has shared launch resources, but no fixed listener is reclaimed. The server
// is reachable only over its TLS port. Local environments without a TLS-capable
// redis-server report an explicit skip; CI sets REDIS_TEST_REQUIRE_TLS=1 so the
// same condition is a hard failure there.
// ---------------------------------------------------------------------------

#[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
mod tls {
    use bytes::Bytes;
    use redis_server_wrapper::{RedisServer, RedisServerHandle};
    use redis_tower::RedisConnection;
    use redis_tower::commands::*;
    use tokio::sync::Mutex;

    const TLS_PASSWORD: &str = "p@ss:w/rd%";
    const TLS_PASSWORD_ENCODED: &str = "p%40ss%3Aw%2Frd%25";
    static TLS_FIXTURE_LOCK: Mutex<()> = Mutex::const_new(());

    struct TestRunDir(std::path::PathBuf);

    impl TestRunDir {
        fn create() -> Result<Self, String> {
            let run_id = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|error| format!("system clock is before the Unix epoch: {error}"))?
                .as_nanos();
            let path = std::env::temp_dir()
                .join(format!("redis-tower-tls-{}-{run_id}", std::process::id()));
            std::fs::create_dir_all(&path)
                .map_err(|error| format!("failed to create TLS test directory: {error}"))?;
            Ok(Self(path))
        }
    }

    impl Drop for TestRunDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct TlsFixture {
        _server: RedisServerHandle,
        ca_pem: Vec<u8>,
        port: u16,
        // Declared after the server so Redis releases certificate files before
        // their unique directory is removed.
        _run_dir: TestRunDir,
    }

    fn tls_is_required() -> bool {
        std::env::var_os("REDIS_TEST_REQUIRE_TLS").is_some()
    }

    fn free_port() -> Result<u16, String> {
        std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .map(|address| address.port())
            .map_err(|error| format!("failed to allocate a TLS test port: {error}"))
    }

    /// Start a TLS-enabled redis-server with generated certs. Missing local
    /// infrastructure is an explicit skip, but required CI legs fail closed.
    async fn start_tls_server() -> Option<TlsFixture> {
        let result = async {
            let run_dir = TestRunDir::create()?;
            let certs_dir = run_dir.0.join("certs");
            let certs = redis_server_wrapper::tls::generate_test_certs(&certs_dir)
                .map_err(|error| format!("failed to generate TLS certificates: {error}"))?;
            let ca_pem = std::fs::read(&certs.ca_cert_file)
                .map_err(|error| format!("failed to read generated CA: {error}"))?;
            let port = free_port()?;

            // `port 0` disables the plaintext listener so the server is
            // reachable only over TLS, proving the client speaks TLS.
            let server = RedisServer::new()
                .port(0)
                .tls_port(port)
                .tls_cert_file(&certs.cert_file)
                .tls_key_file(&certs.key_file)
                .tls_ca_cert_file(&certs.ca_cert_file)
                .tls_auth_clients(false)
                .password(TLS_PASSWORD)
                .start()
                .await
                .map_err(|error| format!("failed to start TLS redis-server: {error}"))?;
            Ok::<_, String>(TlsFixture {
                _server: server,
                ca_pem,
                port,
                _run_dir: run_dir,
            })
        }
        .await;

        match result {
            Ok(fixture) => Some(fixture),
            Err(reason) if tls_is_required() => {
                panic!("required TLS test infrastructure is unavailable: {reason}")
            }
            Err(reason) => {
                eprintln!(
                    "skipping TLS tests because local infrastructure is unavailable: {reason}; \
                     set REDIS_TEST_REQUIRE_TLS=1 to fail instead"
                );
                None
            }
        }
    }

    fn tls_addr(fixture: &TlsFixture) -> String {
        format!("127.0.0.1:{}", fixture.port)
    }

    fn tls_url(fixture: &TlsFixture, db: u8) -> String {
        format!(
            "rediss://:{TLS_PASSWORD_ENCODED}@127.0.0.1:{}/{db}?protocol=resp3",
            fixture.port
        )
    }

    #[cfg(feature = "tls-rustls")]
    fn rustls_config(fixture: &TlsFixture) -> redis_tower_core::tls::TlsConfig {
        redis_tower_core::tls::TlsConfig::default_rustls().with_root_ca_pem(fixture.ca_pem.clone())
    }

    #[cfg(feature = "tls-native-tls")]
    fn native_tls_config(fixture: &TlsFixture) -> redis_tower_core::tls::TlsConfig {
        redis_tower_core::tls::TlsConfig::default_native_tls()
            .with_root_ca_pem(fixture.ca_pem.clone())
    }

    // -- rustls backend --

    #[cfg(feature = "tls-rustls")]
    #[tokio::test]
    async fn tls_rustls_verified_auth_url_roundtrip() {
        let _fixture_guard = TLS_FIXTURE_LOCK.lock().await;
        let Some(fixture) = start_tls_server().await else {
            return;
        };
        let mut conn =
            RedisConnection::connect_url_with_tls(&tls_url(&fixture, 1), &rustls_config(&fixture))
                .await
                .expect(
                    "rustls should trust the generated CA, verify the IP SAN, and authenticate",
                );
        assert!(conn.is_resp3());
        let pong = conn.execute(Ping::new()).await.unwrap();
        assert_eq!(pong, "PONG");
        let key = "tls_test:rustls:key";
        conn.execute(Set::new(key, "value")).await.unwrap();
        let val = conn.execute(Get::new(key)).await.unwrap();
        assert_eq!(val, Some(Bytes::from("value")));
        conn.execute(Del::new(key)).await.unwrap();
    }

    #[cfg(feature = "tls-rustls")]
    #[tokio::test]
    async fn tls_rustls_rejects_wrong_hostname_and_untrusted_ca() {
        let _fixture_guard = TLS_FIXTURE_LOCK.lock().await;
        let Some(fixture) = start_tls_server().await else {
            return;
        };
        if RedisConnection::connect_tls(
            &tls_addr(&fixture),
            "wrong.example",
            &rustls_config(&fixture),
        )
        .await
        .is_ok()
        {
            panic!("a hostname absent from the certificate SAN must fail");
        }
        if RedisConnection::connect_tls(
            &tls_addr(&fixture),
            "127.0.0.1",
            &redis_tower_core::tls::TlsConfig::default_rustls(),
        )
        .await
        .is_ok()
        {
            panic!("an untrusted private CA must fail");
        }
    }

    // -- native-tls backend --

    #[cfg(feature = "tls-native-tls")]
    #[tokio::test]
    async fn tls_native_tls_verified_auth_url_roundtrip() {
        let _fixture_guard = TLS_FIXTURE_LOCK.lock().await;
        let Some(fixture) = start_tls_server().await else {
            return;
        };
        let mut conn = RedisConnection::connect_url_with_tls(
            &tls_url(&fixture, 1),
            &native_tls_config(&fixture),
        )
        .await
        .expect("native-tls should trust the generated CA, verify the IP SAN, and authenticate");
        assert!(conn.is_resp3());
        let pong = conn.execute(Ping::new()).await.unwrap();
        assert_eq!(pong, "PONG");
        let key = "tls_test:native:key";
        conn.execute(Set::new(key, "value")).await.unwrap();
        let val = conn.execute(Get::new(key)).await.unwrap();
        assert_eq!(val, Some(Bytes::from("value")));
        conn.execute(Del::new(key)).await.unwrap();
    }

    #[cfg(feature = "tls-native-tls")]
    #[tokio::test]
    async fn tls_native_tls_rejects_wrong_hostname_and_untrusted_ca() {
        let _fixture_guard = TLS_FIXTURE_LOCK.lock().await;
        let Some(fixture) = start_tls_server().await else {
            return;
        };
        if RedisConnection::connect_tls(
            &tls_addr(&fixture),
            "wrong.example",
            &native_tls_config(&fixture),
        )
        .await
        .is_ok()
        {
            panic!("a hostname absent from the certificate SAN must fail");
        }
        if RedisConnection::connect_tls(
            &tls_addr(&fixture),
            "127.0.0.1",
            &redis_tower_core::tls::TlsConfig::default_native_tls(),
        )
        .await
        .is_ok()
        {
            panic!("an untrusted private CA must fail");
        }
    }
}
