//! Live exact-payload regressions against the adapters used by the binaries.
#![cfg(unix)]
#![cfg(any(
    feature = "client-redis-tower",
    feature = "client-redis-rs",
    feature = "client-fred"
))]
use resource_bench::{ProbeConnection, ProbeProfile};

async fn assert_payload_contract<C: ProbeConnection>(url: &str, profile: ProbeProfile) {
    let mut connection = C::connect_with_profile(url, profile).await.unwrap();
    connection.set_fixture("abcd").await.unwrap();
    connection.get_fixture(b"abcd").await.unwrap();
    assert!(
        connection.get_fixture(b"wxyz").await.is_err(),
        "same-length corrupt reply passed"
    );
    assert!(connection.get_fixture(b"x").await.is_err());
}

#[tokio::test]
async fn enabled_adapters_validate_exact_returned_payload() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let server = redis_server_wrapper::RedisServer::new()
        .port(port)
        .save(false)
        .start()
        .await
        .unwrap();
    let url = format!("redis://{}/?protocol=resp2", server.addr());
    #[cfg(feature = "client-redis-tower")]
    {
        assert_payload_contract::<resource_bench::adapters::TowerConnection>(
            &url,
            ProbeProfile::Baseline,
        )
        .await;
        assert_payload_contract::<resource_bench::adapters::TowerConnection>(
            &url.replacen("redis://", "valkey://", 1),
            ProbeProfile::Baseline,
        )
        .await;
        assert_payload_contract::<resource_bench::adapters::TowerMuxConnection>(
            &url,
            ProbeProfile::MatchedMuxResp2,
        )
        .await;
    }
    #[cfg(feature = "client-redis-rs")]
    assert_payload_contract::<resource_bench::adapters::RedisRsConnection>(
        &url,
        ProbeProfile::MatchedMuxResp2,
    )
    .await;
    #[cfg(feature = "client-fred")]
    assert_payload_contract::<resource_bench::adapters::FredConnection>(
        &url,
        ProbeProfile::Baseline,
    )
    .await;
}
