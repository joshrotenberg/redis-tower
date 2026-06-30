//! Integration coverage for the exotic-tail command sweep (issue #513):
//! container `HELP` subcommands, `LOLWUT`, `CLIENT GETREDIR`, and
//! `SCRIPT DEBUG` against a live server.

mod common;

use common::conn;
use redis_tower::commands::*;

#[tokio::test]
async fn help_subcommands_return_lines() {
    let mut c = conn().await;

    macro_rules! assert_help {
        ($cmd:expr, $name:literal) => {{
            let lines = c
                .execute($cmd)
                .await
                .unwrap_or_else(|e| panic!("{} HELP failed: {e:?}", $name));
            assert!(!lines.is_empty(), "{} HELP returned no lines", $name);
        }};
    }

    // CLUSTER HELP is exercised in the cluster test suite: a standalone server
    // rejects all CLUSTER subcommands with "cluster support disabled".
    assert_help!(AclHelp::new(), "ACL");
    assert_help!(ClientHelp::new(), "CLIENT");
    assert_help!(CommandHelp::new(), "COMMAND");
    assert_help!(ConfigHelp::new(), "CONFIG");
    // DEBUG HELP is omitted: the DEBUG command is disabled unless the server is
    // started with `enable-debug-command`.
    assert_help!(FunctionHelp::new(), "FUNCTION");
    assert_help!(LatencyHelp::new(), "LATENCY");
    assert_help!(MemoryHelp::new(), "MEMORY");
    assert_help!(ModuleHelp::new(), "MODULE");
    assert_help!(ObjectHelp::new(), "OBJECT");
    assert_help!(PubSubHelp::new(), "PUBSUB");
    assert_help!(ScriptHelp::new(), "SCRIPT");
    assert_help!(SlowlogHelp::new(), "SLOWLOG");
    assert_help!(XGroupHelp::new(), "XGROUP");
    assert_help!(XInfoHelp::new(), "XINFO");
}

#[tokio::test]
async fn cover_lolwut() {
    let mut c = conn().await;
    let art = c.execute(Lolwut::new()).await.unwrap();
    assert!(!art.is_empty(), "LOLWUT returned empty output");
}

#[tokio::test]
async fn cover_client_getredir() {
    let mut c = conn().await;
    // No CLIENT TRACKING redirection has been configured on this connection,
    // so the server reports -1 (tracking disabled).
    let redir = c.execute(ClientGetRedir::new()).await.unwrap();
    assert_eq!(redir, -1);
}

#[tokio::test]
async fn cover_script_debug() {
    let mut c = conn().await;
    // Toggle the EVAL debugger off; the server replies with a simple OK.
    c.execute(ScriptDebug::no()).await.unwrap();
}
