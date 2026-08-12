//! Deterministic RESP3 maintenance-notification coverage (issue #498).
//!
//! These tests use a codec-backed scripted server rather than Redis itself:
//! open-source Redis does not emit Smart Client Handoff notifications, and a
//! live Cloud/Software maintenance window cannot be made deterministic in CI.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use redis_tower::auto_pipeline::{AutoPipelineConfig, AutoPipelineReconnectConfig};
use redis_tower::commands::{Echo, Ping};
use redis_tower::reconnect::{ConnectionFactory, ReconnectConfig};
use redis_tower::{
    ConnectionEvent, ConnectionEventBus, ConnectionEventStream, Frame, MaintenanceNotificationKind,
    MultiplexedClient, ProtocolVersion, RedisConnection, RedisError, RedisStream, RespCodec,
};
use redis_tower_protocol::helpers::bulk;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_util::codec::Framed;

const IO_TIMEOUT: Duration = Duration::from_secs(3);
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

type ServerConnection = Framed<RedisStream, RespCodec>;

async fn bounded<T>(operation: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(TEST_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("timed out after {TEST_TIMEOUT:?} while {operation}"))
}

async fn short_bounded<T>(operation: &str, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(IO_TIMEOUT, future)
        .await
        .unwrap_or_else(|_| panic!("timed out after {IO_TIMEOUT:?} while {operation}"))
}

#[derive(Clone)]
struct ScriptedResp3Factory {
    addr: SocketAddr,
    calls: Arc<AtomicUsize>,
    fail_from_call: Option<usize>,
}

impl ScriptedResp3Factory {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            calls: Arc::new(AtomicUsize::new(0)),
            fail_from_call: None,
        }
    }

    /// Fail factory calls beginning with this zero-based call index.
    fn failing_from(mut self, call: usize) -> Self {
        self.fail_from_call = Some(call);
        self
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl ConnectionFactory for ScriptedResp3Factory {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, RedisError>> + Send>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        let addr = self.addr;
        let fail = self
            .fail_from_call
            .is_some_and(|first_failure| call >= first_failure);
        Box::pin(async move {
            if fail {
                return Err(RedisError::ConnectionClosed);
            }
            let stream = TcpStream::connect(addr)
                .await
                .map_err(|_| RedisError::ConnectionClosed)?;
            let mut connection = RedisConnection::from_stream(RedisStream::Tcp(stream));
            connection
                .negotiate_protocol(ProtocolVersion::Resp3)
                .await?;
            Ok(connection)
        })
    }
}

fn command_words(frame: &Frame) -> Vec<String> {
    let Frame::Array(Some(items)) = frame else {
        panic!("expected a command array, got {frame:?}");
    };
    items
        .iter()
        .map(|item| match item {
            Frame::BulkString(Some(bytes)) | Frame::SimpleString(bytes) => {
                String::from_utf8_lossy(bytes).into_owned()
            }
            other => panic!("expected a command string, got {other:?}"),
        })
        .collect()
}

fn assert_command(frame: &Frame, expected: &[&str]) {
    let expected = expected
        .iter()
        .map(|word| (*word).to_owned())
        .collect::<Vec<_>>();
    assert_eq!(
        command_words(frame),
        expected,
        "scripted server received the wrong command"
    );
}

async fn receive(connection: &mut ServerConnection, operation: &str) -> Frame {
    short_bounded(operation, connection.next())
        .await
        .unwrap_or_else(|| panic!("client closed while {operation}"))
        .unwrap_or_else(|error| panic!("client sent an invalid frame while {operation}: {error}"))
}

async fn send(connection: &mut ServerConnection, frame: Frame, operation: &str) {
    short_bounded(operation, connection.send(frame))
        .await
        .unwrap_or_else(|error| panic!("failed while {operation}: {error}"));
}

async fn accept_resp3(listener: &TcpListener) -> ServerConnection {
    let (stream, _) = short_bounded("accepting a scripted RESP3 connection", listener.accept())
        .await
        .expect("failed to accept scripted RESP3 connection");
    let mut connection = Framed::new(RedisStream::Tcp(stream), RespCodec::new());
    let hello = receive(&mut connection, "reading HELLO 3").await;
    assert_command(&hello, &["HELLO", "3"]);
    send(
        &mut connection,
        Frame::Map(vec![
            (bulk("server"), bulk("redis")),
            (bulk("proto"), Frame::Integer(3)),
        ]),
        "replying to HELLO 3",
    )
    .await;
    connection
}

async fn expect_registration(connection: &mut ServerConnection, response: Frame) {
    let registration = receive(connection, "reading CLIENT MAINT_NOTIFICATIONS").await;
    assert_command(
        &registration,
        &[
            "CLIENT",
            "MAINT_NOTIFICATIONS",
            "ON",
            "moving-endpoint-type",
            "none",
        ],
    );
    send(
        connection,
        response,
        "replying to CLIENT MAINT_NOTIFICATIONS",
    )
    .await;
}

async fn accept_registered(listener: &TcpListener) -> ServerConnection {
    let mut connection = accept_resp3(listener).await;
    expect_registration(
        &mut connection,
        Frame::SimpleString(Bytes::from_static(b"OK")),
    )
    .await;
    connection
}

fn moving(sequence: i64, ttl_seconds: i64) -> Frame {
    Frame::Push(vec![
        bulk("MOVING"),
        Frame::Integer(sequence),
        Frame::Integer(ttl_seconds),
        Frame::Null,
    ])
}

fn moving_strings(sequence: &str, ttl_seconds: &str) -> Frame {
    Frame::Push(vec![
        Frame::SimpleString(Bytes::from_static(b"MOVING")),
        bulk(sequence),
        Frame::SimpleString(Bytes::copy_from_slice(ttl_seconds.as_bytes())),
        Frame::Null,
    ])
}

fn migrating_strings(sequence: &str, ttl_seconds: &str) -> Frame {
    Frame::Push(vec![
        bulk("MIGRATING"),
        Frame::SimpleString(Bytes::copy_from_slice(sequence.as_bytes())),
        bulk(ttl_seconds),
    ])
}

fn pipeline_config() -> AutoPipelineConfig {
    AutoPipelineConfig {
        max_batch_size: 1,
        queue_capacity: 8,
        ..AutoPipelineConfig::default()
    }
}

fn reconnect_config(max_retries: usize) -> AutoPipelineReconnectConfig {
    AutoPipelineReconnectConfig::new(
        ReconnectConfig::default()
            .max_retries(max_retries)
            .base_delay(Duration::ZERO)
            .max_delay(Duration::ZERO)
            .jitter(false)
            .connect_timeout(IO_TIMEOUT),
    )
}

async fn next_event_matching(
    events: &mut ConnectionEventStream,
    description: &str,
    predicate: impl Fn(&ConnectionEvent) -> bool,
) -> ConnectionEvent {
    bounded(description, async {
        loop {
            let event = events
                .recv()
                .await
                .unwrap_or_else(|error| panic!("connection event stream failed: {error}"));
            if predicate(&event) {
                return event;
            }
        }
    })
    .await
}

async fn next_maintenance_event(events: &mut ConnectionEventStream) -> ConnectionEvent {
    next_event_matching(events, "waiting for a maintenance event", |event| {
        matches!(event, ConnectionEvent::MaintenanceNotification { .. })
    })
    .await
}

async fn assert_no_maintenance_event(events: &mut ConnectionEventStream, duration: Duration) {
    let result = tokio::time::timeout(duration, async {
        loop {
            let event = events
                .recv()
                .await
                .unwrap_or_else(|error| panic!("connection event stream failed: {error}"));
            if matches!(event, ConnectionEvent::MaintenanceNotification { .. }) {
                return event;
            }
        }
    })
    .await;
    assert!(
        result.is_err(),
        "unexpected duplicate or malformed maintenance event: {result:?}"
    );
}

async fn shutdown(client: MultiplexedClient) {
    bounded("shutting down maintenance client", client.shutdown()).await;
}

#[tokio::test]
async fn idle_moving_reconnects_at_half_ttl_deduplicates_and_registers_again() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let factory = ScriptedResp3Factory::new(listener.local_addr().unwrap());
    let observed_factory = factory.clone();
    let (early_result_tx, early_result_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let mut first = accept_registered(&listener).await;
        send(&mut first, moving(41, 2), "sending idle MOVING").await;

        // The portable endpoint-none contract schedules handoff at ttl/2, not
        // immediately and not at the full TTL.
        let early = tokio::time::timeout(Duration::from_millis(300), listener.accept()).await;
        early_result_tx
            .send(early.is_err())
            .expect("test dropped early-handoff observation");

        let mut second = accept_registered(&listener).await;
        drop(first);

        // The same sequence can be replayed on the replacement connection.
        // It must not schedule a second handoff.
        send(
            &mut second,
            moving_strings("41", "0"),
            "sending duplicate MOVING",
        )
        .await;
        let ping = receive(&mut second, "reading PING after maintenance handoff").await;
        assert_command(&ping, &["PING"]);
        send(
            &mut second,
            Frame::SimpleString(Bytes::from_static(b"PONG")),
            "replying to PING after maintenance handoff",
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(300), listener.accept())
                .await
                .is_err(),
            "duplicate MOVING sequence scheduled a second reconnect"
        );
        let _ = short_bounded("waiting for final maintenance socket close", second.next()).await;
    });

    let events = ConnectionEventBus::new(32);
    let mut event_stream = events.subscribe();
    let (client, handle) = bounded(
        "building factory-backed maintenance client",
        MultiplexedClient::from_factory_with_maintenance_and_events(
            factory,
            pipeline_config(),
            reconnect_config(1),
            events,
        ),
    )
    .await
    .expect("failed to build factory-backed maintenance client");

    assert!(
        bounded("checking handoff did not run before ttl/2", early_result_rx)
            .await
            .expect("server dropped early-handoff observation")
    );
    assert_eq!(
        next_maintenance_event(&mut event_stream).await,
        ConnectionEvent::MaintenanceNotification {
            kind: MaintenanceNotificationKind::Moving,
            sequence: 41,
            ttl: Duration::from_secs(2),
        }
    );
    assert_eq!(
        bounded(
            "executing PING after maintenance handoff",
            client.execute(Ping::new()),
        )
        .await
        .expect("PING after maintenance handoff failed"),
        "PONG"
    );
    assert_no_maintenance_event(&mut event_stream, Duration::from_millis(200)).await;
    assert_eq!(
        observed_factory.calls(),
        2,
        "initial connection and one maintenance replacement were expected"
    );

    bounded("stopping maintenance listener", handle.shutdown()).await;
    shutdown(client).await;
    bounded("joining idle MOVING server", server)
        .await
        .expect("idle MOVING server panicked");
}

#[tokio::test]
async fn interleaved_moving_finishes_active_batch_once_and_gates_queued_work() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let factory = ScriptedResp3Factory::new(listener.local_addr().unwrap());
    let (moving_sent_tx, moving_sent_rx) = oneshot::channel();
    let (release_active_tx, release_active_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let mut first = accept_registered(&listener).await;
        let active = receive(&mut first, "reading active ECHO").await;
        assert_command(&active, &["ECHO", "active"]);
        send(
            &mut first,
            moving(52, 0),
            "interleaving MOVING before active response",
        )
        .await;
        moving_sent_tx
            .send(())
            .expect("test dropped MOVING notification");

        assert!(
            tokio::time::timeout(Duration::from_millis(100), first.next())
                .await
                .is_err(),
            "queued work reached the old socket before the active batch completed"
        );
        short_bounded("waiting to release active batch", release_active_rx)
            .await
            .expect("test dropped active-batch release");
        send(
            &mut first,
            Frame::BulkString(Some(Bytes::from_static(b"active"))),
            "completing active ECHO",
        )
        .await;

        let mut second = accept_registered(&listener).await;
        drop(first);
        let queued = receive(&mut second, "reading queued ECHO on replacement").await;
        assert_command(&queued, &["ECHO", "queued"]);
        send(
            &mut second,
            Frame::BulkString(Some(Bytes::from_static(b"queued"))),
            "completing queued ECHO",
        )
        .await;
        let _ = short_bounded("waiting for interleaved server socket close", second.next()).await;
    });

    let events = ConnectionEventBus::new(32);
    let mut event_stream = events.subscribe();
    let (client, handle) = bounded(
        "building interleaved maintenance client",
        MultiplexedClient::from_factory_with_maintenance_and_events(
            factory,
            pipeline_config(),
            reconnect_config(1),
            events,
        ),
    )
    .await
    .expect("failed to build interleaved maintenance client");

    let active_client = client.clone();
    let active = tokio::spawn(async move { active_client.execute(Echo::new("active")).await });
    bounded("waiting for interleaved MOVING", moving_sent_rx)
        .await
        .expect("server dropped interleaved MOVING signal");
    let queued_client = client.clone();
    let queued = tokio::spawn(async move { queued_client.execute(Echo::new("queued")).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !active.is_finished(),
        "active batch completed before its reply"
    );
    assert!(
        !queued.is_finished(),
        "queued work completed before maintenance reconnect"
    );
    release_active_tx
        .send(())
        .expect("server dropped active-batch release");

    assert_eq!(
        bounded("joining active maintenance request", active)
            .await
            .expect("active request task panicked")
            .expect("active request failed"),
        Bytes::from_static(b"active")
    );
    // The worker is the sole socket reader, so it can publish the interleaved
    // push only after it has consumed the active response and restored RESP
    // alignment. It must observe the push before admitting the queued batch.
    assert!(matches!(
        next_maintenance_event(&mut event_stream).await,
        ConnectionEvent::MaintenanceNotification {
            kind: MaintenanceNotificationKind::Moving,
            sequence: 52,
            ttl,
        } if ttl.is_zero()
    ));
    assert_eq!(
        bounded("joining queued maintenance request", queued)
            .await
            .expect("queued request task panicked")
            .expect("queued request failed"),
        Bytes::from_static(b"queued")
    );

    bounded(
        "stopping interleaved maintenance listener",
        handle.shutdown(),
    )
    .await;
    shutdown(client).await;
    bounded("joining interleaved MOVING server", server)
        .await
        .expect("interleaved MOVING server panicked");
}

#[tokio::test]
async fn migrating_duplicates_and_malformed_pushes_do_not_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let factory = ScriptedResp3Factory::new(listener.local_addr().unwrap());
    let observed_factory = factory.clone();

    let server = tokio::spawn(async move {
        let mut connection = accept_registered(&listener).await;
        let ping = receive(&mut connection, "reading parser-coverage PING").await;
        assert_command(&ping, &["PING"]);
        for (description, push) in [
            ("valid MIGRATING", migrating_strings("73", "9")),
            (
                "duplicate MIGRATING",
                Frame::Push(vec![
                    bulk("MIGRATING"),
                    Frame::Integer(73),
                    Frame::Integer(9),
                ]),
            ),
            (
                "invalid sequence",
                Frame::Push(vec![bulk("MOVING"), bulk("NaN"), bulk("0"), Frame::Null]),
            ),
            (
                "missing endpoint",
                Frame::Push(vec![bulk("MOVING"), Frame::Integer(74), Frame::Integer(0)]),
            ),
            (
                "non-null endpoint forbidden by endpoint-type none",
                Frame::Push(vec![
                    bulk("MOVING"),
                    Frame::Integer(75),
                    Frame::Integer(0),
                    bulk("replacement.invalid:6379"),
                ]),
            ),
            (
                "unknown maintenance push",
                Frame::Push(vec![bulk("MAINT_UNKNOWN"), Frame::Integer(76)]),
            ),
        ] {
            send(&mut connection, push, &format!("sending {description}")).await;
        }
        send(
            &mut connection,
            Frame::SimpleString(Bytes::from_static(b"PONG")),
            "replying after parser-coverage pushes",
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(400), listener.accept())
                .await
                .is_err(),
            "MIGRATING or malformed push unexpectedly triggered reconnect"
        );
        let _ = short_bounded("waiting for parser server socket close", connection.next()).await;
    });

    let events = ConnectionEventBus::new(32);
    let mut event_stream = events.subscribe();
    let (client, handle) = bounded(
        "building parser-coverage maintenance client",
        MultiplexedClient::from_factory_with_maintenance_and_events(
            factory,
            pipeline_config(),
            reconnect_config(1),
            events,
        ),
    )
    .await
    .expect("failed to build parser-coverage maintenance client");

    assert_eq!(
        bounded(
            "executing parser-coverage PING",
            client.execute(Ping::new())
        )
        .await
        .expect("parser-coverage PING failed"),
        "PONG"
    );
    assert_eq!(
        next_maintenance_event(&mut event_stream).await,
        ConnectionEvent::MaintenanceNotification {
            kind: MaintenanceNotificationKind::Migrating,
            sequence: 73,
            ttl: Duration::from_secs(9),
        }
    );
    assert_no_maintenance_event(&mut event_stream, Duration::from_millis(300)).await;
    assert_eq!(observed_factory.calls(), 1);

    bounded("stopping parser maintenance listener", handle.shutdown()).await;
    shutdown(client).await;
    bounded("joining parser-coverage server", server)
        .await
        .expect("parser-coverage server panicked");
}

async fn listener_stop_prevents_handoff(use_explicit_shutdown: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let factory = ScriptedResp3Factory::new(listener.local_addr().unwrap());
    let observed_factory = factory.clone();

    let server = tokio::spawn(async move {
        let mut connection = accept_registered(&listener).await;
        let ping = receive(&mut connection, "reading PING after listener stop").await;
        assert_command(&ping, &["PING"]);
        send(
            &mut connection,
            moving(84, 0),
            "interleaving MOVING after listener stop",
        )
        .await;
        send(
            &mut connection,
            Frame::SimpleString(Bytes::from_static(b"PONG")),
            "replying to PING after listener stop",
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(400), listener.accept())
                .await
                .is_err(),
            "stopped maintenance listener still triggered reconnect"
        );
        let _ = short_bounded(
            "waiting for stopped-listener socket close",
            connection.next(),
        )
        .await;
    });

    let events = ConnectionEventBus::new(16);
    let mut event_stream = events.subscribe();
    let (client, handle) = bounded(
        "building listener-lifecycle client",
        MultiplexedClient::from_factory_with_maintenance_and_events(
            factory,
            pipeline_config(),
            reconnect_config(1),
            events,
        ),
    )
    .await
    .expect("failed to build listener-lifecycle client");
    if use_explicit_shutdown {
        bounded(
            "explicitly shutting down maintenance listener",
            handle.shutdown(),
        )
        .await;
    } else {
        drop(handle);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(
        bounded(
            "executing PING after maintenance listener stop",
            client.execute(Ping::new()),
        )
        .await
        .expect("PING after listener stop failed"),
        "PONG"
    );
    assert_no_maintenance_event(&mut event_stream, Duration::from_millis(300)).await;
    assert_eq!(observed_factory.calls(), 1);
    shutdown(client).await;
    bounded("joining listener-lifecycle server", server)
        .await
        .expect("listener-lifecycle server panicked");
}

#[tokio::test]
async fn dropping_listener_stops_maintenance_handling() {
    listener_stop_prevents_handoff(false).await;
}

#[tokio::test]
async fn shutting_down_listener_stops_maintenance_handling() {
    listener_stop_prevents_handoff(true).await;
}

#[tokio::test]
async fn listener_shutdown_cancels_pending_handoff_before_half_ttl() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let factory = ScriptedResp3Factory::new(listener.local_addr().unwrap());
    let observed_factory = factory.clone();
    let (no_reconnect_tx, no_reconnect_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let mut connection = accept_registered(&listener).await;
        send(
            &mut connection,
            moving(85, 2),
            "sending future MOVING before listener shutdown",
        )
        .await;

        let ping = receive(&mut connection, "reading PING after cancelling handoff").await;
        assert_command(&ping, &["PING"]);
        send(
            &mut connection,
            Frame::SimpleString(Bytes::from_static(b"PONG")),
            "replying to PING after cancelling handoff",
        )
        .await;

        // Wait beyond the original half-TTL boundary. A mere scheduling delay
        // cannot make this pass: the same socket must remain installed.
        let no_reconnect = tokio::time::timeout(Duration::from_millis(1_200), listener.accept())
            .await
            .is_err();
        no_reconnect_tx
            .send(no_reconnect)
            .expect("test dropped no-reconnect observation");
        let _ = short_bounded(
            "waiting for cancelled-handoff socket close",
            connection.next(),
        )
        .await;
    });

    let events = ConnectionEventBus::new(16);
    let mut event_stream = events.subscribe();
    let (client, handle) = bounded(
        "building pending-handoff cancellation client",
        MultiplexedClient::from_factory_with_maintenance_and_events(
            factory,
            pipeline_config(),
            reconnect_config(1),
            events,
        ),
    )
    .await
    .expect("failed to build pending-handoff cancellation client");
    assert_eq!(
        next_maintenance_event(&mut event_stream).await,
        ConnectionEvent::MaintenanceNotification {
            kind: MaintenanceNotificationKind::Moving,
            sequence: 85,
            ttl: Duration::from_secs(2),
        }
    );

    bounded(
        "shutting down listener before the half-TTL boundary",
        handle.shutdown(),
    )
    .await;
    assert_eq!(
        bounded(
            "executing PING after cancelling pending handoff",
            client.execute(Ping::new()),
        )
        .await
        .expect("PING after cancelling pending handoff failed"),
        "PONG"
    );
    assert!(
        bounded(
            "checking cancelled handoff stayed cancelled",
            no_reconnect_rx
        )
        .await
        .expect("server dropped no-reconnect observation")
    );
    assert_eq!(
        observed_factory.calls(),
        1,
        "cancelling the pending handoff must retain the original connection"
    );

    shutdown(client).await;
    bounded("joining pending-handoff cancellation server", server)
        .await
        .expect("pending-handoff cancellation server panicked");
}

#[tokio::test]
async fn final_client_shutdown_completes_while_handoff_is_pending() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let factory = ScriptedResp3Factory::new(listener.local_addr().unwrap());
    let observed_factory = factory.clone();

    let server = tokio::spawn(async move {
        let mut connection = accept_registered(&listener).await;
        send(
            &mut connection,
            moving(86, 60),
            "sending long-TTL MOVING before final client shutdown",
        )
        .await;
        let closed = short_bounded(
            "waiting for pending-handoff client socket close",
            connection.next(),
        )
        .await;
        assert!(
            closed.is_none(),
            "client sent data instead of closing during shutdown: {closed:?}"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(400), listener.accept())
                .await
                .is_err(),
            "final client shutdown initiated a maintenance reconnect"
        );
    });

    let events = ConnectionEventBus::new(16);
    let mut event_stream = events.subscribe();
    let (client, handle) = bounded(
        "building pending-handoff shutdown client",
        MultiplexedClient::from_factory_with_maintenance_and_events(
            factory,
            pipeline_config(),
            reconnect_config(1),
            events,
        ),
    )
    .await
    .expect("failed to build pending-handoff shutdown client");
    assert_eq!(
        next_maintenance_event(&mut event_stream).await,
        ConnectionEvent::MaintenanceNotification {
            kind: MaintenanceNotificationKind::Moving,
            sequence: 86,
            ttl: Duration::from_secs(60),
        }
    );

    // This previously looped forever by repeatedly re-entering the pending
    // handoff branch after observing final-client shutdown.
    shutdown(client).await;
    bounded(
        "stopping listener after final client shutdown",
        handle.shutdown(),
    )
    .await;
    assert_eq!(
        observed_factory.calls(),
        1,
        "final client shutdown must not start a replacement connection"
    );
    bounded("joining pending-handoff shutdown server", server)
        .await
        .expect("pending-handoff shutdown server panicked");
}

#[tokio::test]
async fn listener_shutdown_during_batch_wins_over_moving_before_eof() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let factory = ScriptedResp3Factory::new(listener.local_addr().unwrap());
    let observed_factory = factory.clone();
    let (active_seen_tx, active_seen_rx) = oneshot::channel();
    let (release_eof_tx, release_eof_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let mut first = accept_registered(&listener).await;
        let active = receive(&mut first, "reading active ECHO before listener disable").await;
        assert_command(&active, &["ECHO", "active-before-disable"]);
        active_seen_tx
            .send(())
            .expect("test dropped active-batch observation");
        short_bounded("waiting to send MOVING and EOF", release_eof_rx)
            .await
            .expect("test dropped MOVING/EOF release");
        send(
            &mut first,
            moving(87, 0),
            "sending MOVING after listener disable",
        )
        .await;
        drop(first);

        // Maintenance has been disabled, so an ordinary factory reconnect
        // negotiates RESP3 but must not register CLIENT MAINT_NOTIFICATIONS.
        let mut second = accept_resp3(&listener).await;
        let queued = receive(&mut second, "reading queued ECHO after ordinary reconnect").await;
        assert_command(&queued, &["ECHO", "queued-after-disable"]);
        send(
            &mut second,
            Frame::BulkString(Some(Bytes::from_static(b"queued-after-disable"))),
            "replying to queued ECHO after ordinary reconnect",
        )
        .await;
        let ping = receive(
            &mut second,
            "reading follow-up PING after ordinary reconnect",
        )
        .await;
        assert_command(&ping, &["PING"]);
        send(
            &mut second,
            Frame::SimpleString(Bytes::from_static(b"PONG")),
            "replying to follow-up PING after ordinary reconnect",
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(300), listener.accept())
                .await
                .is_err(),
            "disabled maintenance push scheduled an extra replacement"
        );
        let _ = short_bounded(
            "waiting for ordinary replacement socket close",
            second.next(),
        )
        .await;
    });

    let events = ConnectionEventBus::new(32);
    let mut event_stream = events.subscribe();
    let (client, handle) = bounded(
        "building in-flight listener-disable client",
        MultiplexedClient::from_factory_with_maintenance_and_events(
            factory,
            pipeline_config(),
            reconnect_config(1),
            events,
        ),
    )
    .await
    .expect("failed to build in-flight listener-disable client");

    let active_client = client.clone();
    let active = tokio::spawn(async move {
        active_client
            .execute(Echo::new("active-before-disable"))
            .await
    });
    bounded(
        "waiting for active batch before listener disable",
        active_seen_rx,
    )
    .await
    .expect("server dropped active-batch observation");

    let queued_client = client.clone();
    let queued = tokio::spawn(async move {
        queued_client
            .execute(Echo::new("queued-after-disable"))
            .await
    });

    // Poll shutdown once before permitting the server push. Its control
    // message is then queued while the sole worker remains blocked on the
    // active response, making the control-before-push ordering deterministic.
    let handle_shutdown = handle.shutdown();
    tokio::pin!(handle_shutdown);
    tokio::select! {
        biased;
        () = &mut handle_shutdown => {
            panic!("listener shutdown acknowledged before the active batch ended")
        }
        () = tokio::time::sleep(Duration::from_millis(50)) => {}
    }
    release_eof_tx
        .send(())
        .expect("server dropped MOVING/EOF release");

    let active_result = bounded("joining active request after MOVING/EOF", active)
        .await
        .expect("active request task panicked");
    assert!(
        matches!(active_result, Err(RedisError::ConnectionClosed)),
        "ambiguous active request had unexpected result: {active_result:?}"
    );
    bounded(
        "waiting for in-flight listener shutdown acknowledgement",
        &mut handle_shutdown,
    )
    .await;
    assert_eq!(
        bounded("joining queued work after ordinary reconnect", queued)
            .await
            .expect("queued request task panicked")
            .expect("queued request failed after ordinary reconnect"),
        Bytes::from_static(b"queued-after-disable")
    );
    assert_eq!(
        bounded(
            "executing follow-up PING after ordinary reconnect",
            client.execute(Ping::new()),
        )
        .await
        .expect("follow-up PING failed after ordinary reconnect"),
        "PONG"
    );
    assert_no_maintenance_event(&mut event_stream, Duration::from_millis(200)).await;
    assert_eq!(
        observed_factory.calls(),
        2,
        "one initial connection and one ordinary replacement were expected"
    );

    shutdown(client).await;
    bounded("joining in-flight listener-disable server", server)
        .await
        .expect("in-flight listener-disable server panicked");
}

#[tokio::test]
async fn unsupported_registration_fails_closed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let factory = ScriptedResp3Factory::new(listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut connection = accept_resp3(&listener).await;
        expect_registration(
            &mut connection,
            Frame::Error(Bytes::from_static(b"ERR unknown subcommand")),
        )
        .await;
        let _ = short_bounded(
            "waiting for rejected registration socket close",
            connection.next(),
        )
        .await;
    });

    let events = ConnectionEventBus::new(8);
    let mut event_stream = events.subscribe();
    let error = match bounded(
        "waiting for required registration rejection",
        MultiplexedClient::from_factory_with_maintenance_and_events(
            factory,
            pipeline_config(),
            reconnect_config(0),
            events,
        ),
    )
    .await
    {
        Ok(_) => panic!("required maintenance constructor unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(
        error.to_string().to_ascii_lowercase().contains("maint"),
        "unexpected registration error: {error}"
    );
    let connect_failed = next_event_matching(
        &mut event_stream,
        "waiting for maintenance setup ConnectFailed event",
        |event| matches!(event, ConnectionEvent::ConnectFailed { .. }),
    )
    .await;
    let ConnectionEvent::ConnectFailed { error: event_error } = connect_failed else {
        unreachable!("event predicate returned a non-ConnectFailed event");
    };
    assert_eq!(event_error.as_ref(), error.to_string());
    assert!(
        event_error.contains("CLIENT MAINT_NOTIFICATIONS"),
        "setup failure event lacked maintenance context: {event_error}"
    );

    bounded("joining rejected-registration server", server)
        .await
        .expect("rejected-registration server panicked");
}

#[tokio::test]
async fn moving_decoded_before_batch_eof_reconnects_once_and_deduplicates_replay() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let factory = ScriptedResp3Factory::new(listener.local_addr().unwrap());
    let observed_factory = factory.clone();
    let (replacement_ready_tx, replacement_ready_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        let mut first = accept_registered(&listener).await;
        let active = receive(&mut first, "reading active ECHO before EOF").await;
        assert_command(&active, &["ECHO", "ambiguous"]);
        send(
            &mut first,
            moving(106, 0),
            "sending MOVING immediately before EOF",
        )
        .await;
        // No command response follows. The active request is ambiguous and
        // must fail, but the already-decoded notification must still be
        // recorded before the ordinary transport reconnect starts.
        drop(first);

        let mut second = accept_registered(&listener).await;
        send(
            &mut second,
            moving_strings("106", "0"),
            "replaying MOVING sequence on replacement",
        )
        .await;
        replacement_ready_tx
            .send(())
            .expect("test dropped replacement-ready signal");

        assert!(
            tokio::time::timeout(Duration::from_millis(400), listener.accept())
                .await
                .is_err(),
            "replayed MOVING scheduled a second replacement"
        );
        let ping = receive(&mut second, "reading PING after EOF replacement").await;
        assert_command(&ping, &["PING"]);
        send(
            &mut second,
            Frame::SimpleString(Bytes::from_static(b"PONG")),
            "replying to PING after EOF replacement",
        )
        .await;
        let _ = short_bounded("waiting for EOF-regression socket close", second.next()).await;
    });

    let events = ConnectionEventBus::new(32);
    let mut event_stream = events.subscribe();
    let (client, handle) = bounded(
        "building batch-EOF maintenance client",
        MultiplexedClient::from_factory_with_maintenance_and_events(
            factory,
            pipeline_config(),
            reconnect_config(1),
            events,
        ),
    )
    .await
    .expect("failed to build batch-EOF maintenance client");

    let active = bounded(
        "waiting for ambiguous active request to fail",
        client.execute(Echo::new("ambiguous")),
    )
    .await;
    assert!(
        matches!(active, Err(RedisError::ConnectionClosed)),
        "active request received an unexpected result after EOF: {active:?}"
    );
    bounded("waiting for replacement registration", replacement_ready_rx)
        .await
        .expect("server dropped replacement-ready signal");
    assert_eq!(
        next_maintenance_event(&mut event_stream).await,
        ConnectionEvent::MaintenanceNotification {
            kind: MaintenanceNotificationKind::Moving,
            sequence: 106,
            ttl: Duration::ZERO,
        }
    );
    assert_eq!(
        bounded(
            "executing PING after batch-EOF replacement",
            client.execute(Ping::new()),
        )
        .await
        .expect("PING after batch-EOF replacement failed"),
        "PONG"
    );
    assert_no_maintenance_event(&mut event_stream, Duration::from_millis(200)).await;
    assert_eq!(
        observed_factory.calls(),
        2,
        "one initial connection and one EOF replacement were expected"
    );

    bounded("stopping batch-EOF maintenance listener", handle.shutdown()).await;
    shutdown(client).await;
    bounded("joining batch-EOF server", server)
        .await
        .expect("batch-EOF server panicked");
}

#[tokio::test]
async fn moving_reconnect_exhaustion_fails_queued_work() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let factory = ScriptedResp3Factory::new(listener.local_addr().unwrap()).failing_from(1);
    let observed_factory = factory.clone();
    let server = tokio::spawn(async move {
        let mut connection = accept_registered(&listener).await;
        send(
            &mut connection,
            moving(95, 0),
            "sending MOVING before reconnect exhaustion",
        )
        .await;
        let retired_socket =
            tokio::time::timeout(Duration::from_millis(400), connection.next()).await;
        assert!(
            retired_socket.as_ref().is_err() || matches!(retired_socket, Ok(None)),
            "queued work was sent ambiguously on the retired connection: {retired_socket:?}"
        );
    });

    let events = ConnectionEventBus::new(32);
    let mut event_stream = events.subscribe();
    let (client, handle) = bounded(
        "building exhaustion maintenance client",
        MultiplexedClient::from_factory_with_maintenance_and_events(
            factory,
            pipeline_config(),
            reconnect_config(1),
            events,
        ),
    )
    .await
    .expect("failed to build exhaustion maintenance client");
    assert!(matches!(
        next_maintenance_event(&mut event_stream).await,
        ConnectionEvent::MaintenanceNotification {
            kind: MaintenanceNotificationKind::Moving,
            sequence: 95,
            ttl,
        } if ttl.is_zero()
    ));

    let result = bounded(
        "waiting for queued work to fail after reconnect exhaustion",
        client.execute(Ping::new()),
    )
    .await;
    assert!(
        matches!(result, Err(RedisError::ConnectionClosed)),
        "unexpected queued-work result after maintenance exhaustion: {result:?}"
    );
    let exhausted = next_event_matching(
        &mut event_stream,
        "waiting for maintenance reconnect exhaustion event",
        |event| matches!(event, ConnectionEvent::ReconnectExhausted { .. }),
    )
    .await;
    assert_eq!(
        exhausted,
        ConnectionEvent::ReconnectExhausted { attempts: 2 }
    );
    assert_eq!(
        observed_factory.calls(),
        3,
        "one initial connect plus two replacement attempts expected"
    );

    bounded("stopping exhausted maintenance listener", handle.shutdown()).await;
    shutdown(client).await;
    bounded("joining reconnect-exhaustion server", server)
        .await
        .expect("reconnect-exhaustion server panicked");
}
