mod common;

use common::{conn, redis_addr};
use redis_tower::commands::*;
use redis_tower::{MultiplexedClient, RedisConnection};

// ---------------------------------------------------------------------------
// Error path integration tests (issue #342)
// ---------------------------------------------------------------------------

/// Connecting to a port with nothing listening should return an error without
/// panicking. Port 19999 is unlikely to have anything listening on it.
#[tokio::test]
async fn connection_refused() {
    let result = RedisConnection::connect("127.0.0.1:19999").await;
    assert!(result.is_err(), "expected connection error, got Ok");
}

/// Setting a string key and then performing a list operation on it should
/// return a WRONGTYPE error from the server.
#[tokio::test]
async fn wrongtype_error_on_live_server() {
    let mut c = conn().await;
    let key = "test:errors:wrongtype";
    c.execute(Del::new(key)).await.unwrap();
    c.execute(Set::new(key, "not_a_list")).await.unwrap();

    let result = c.execute(LPush::new(key, "item")).await;
    assert!(result.is_err(), "expected WRONGTYPE error, got Ok");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("WRONGTYPE"),
        "expected WRONGTYPE error, got: {err}"
    );

    c.execute(Del::new(key)).await.unwrap();
}

/// Same WRONGTYPE scenario exercised through the MultiplexedClient path.
#[tokio::test]
async fn wrongtype_error_via_multiplexed_client() {
    let addr = redis_addr().await;
    let client = MultiplexedClient::connect(addr)
        .await
        .expect("failed to connect multiplexed client");

    let key = "test:errors:wrongtype:mux";
    client.execute(Del::new(key)).await.unwrap();
    client.execute(Set::new(key, "not_a_list")).await.unwrap();

    let result = client.execute(LPush::new(key, "item")).await;
    assert!(result.is_err(), "expected WRONGTYPE error, got Ok");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("WRONGTYPE"),
        "expected WRONGTYPE error via MultiplexedClient, got: {err}"
    );

    client.execute(Del::new(key)).await.unwrap();
}

/// Attempting an INCR on a non-integer string returns a server error.
#[tokio::test]
async fn not_integer_error() {
    let mut c = conn().await;
    let key = "test:errors:not_integer";
    c.execute(Del::new(key)).await.unwrap();
    c.execute(Set::new(key, "not_a_number")).await.unwrap();

    let result = c.execute(Incr::new(key)).await;
    assert!(result.is_err(), "expected error on INCR of non-integer");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("not an integer"),
        "expected 'not an integer' error, got: {err}"
    );

    c.execute(Del::new(key)).await.unwrap();
}
