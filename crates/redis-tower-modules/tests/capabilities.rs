//! Fail-closed capability preflight for assertion-executing module CI.
//!
//! This target is ignored for ordinary local test runs. Workflows that promise
//! module evidence invoke it explicitly before the behavioral suites so a
//! reachable but incompatible server cannot turn missing assertions into a
//! green job.

use redis_tower::Frame;
use redis_tower::commands::{Info, RawCommand};
use redis_tower_core::RedisConnection;

const DEFAULT_REQUIRED_COMMANDS: &str = "JSON.SET,FT.CREATE,TS.CREATE,BF.ADD,VADD";

fn parse_version(value: &str) -> (u32, u32, u32) {
    let numeric = value.split_once('-').map_or(value, |(version, _)| version);
    let mut parts = numeric.split('.');
    let major = parts
        .next()
        .and_then(|part| part.parse().ok())
        .expect("redis_version major must be an integer");
    let minor = parts
        .next()
        .and_then(|part| part.parse().ok())
        .expect("redis_version minor must be an integer");
    let patch = parts
        .next()
        .and_then(|part| part.parse().ok())
        .expect("redis_version patch must be an integer");
    (major, minor, patch)
}

#[tokio::test]
#[ignore = "requires the module-enabled CI service"]
async fn required_server_version_and_commands_are_present() {
    let url = std::env::var("REDIS_URL")
        .expect("REDIS_URL is required for the module capability preflight");
    let mut connection = RedisConnection::connect_url(&url)
        .await
        .expect("connect to the required module-enabled Redis service");

    let info = connection
        .execute(Info::new().section("server"))
        .await
        .expect("INFO server must execute during capability discovery");
    let version_text = info
        .lines()
        .find_map(|line| line.trim_end().strip_prefix("redis_version:"))
        .expect("INFO server omitted redis_version");
    let version = parse_version(version_text);
    let minimum_text =
        std::env::var("REDIS_MODULE_MIN_VERSION").unwrap_or_else(|_| "8.0.0".to_owned());
    let minimum = parse_version(&minimum_text);
    assert!(
        version >= minimum,
        "the module gate requires Redis {minimum_text} or newer, found {version_text}"
    );

    let required_commands = std::env::var("REDIS_MODULE_COMMANDS")
        .unwrap_or_else(|_| DEFAULT_REQUIRED_COMMANDS.to_owned())
        .split(',')
        .map(str::trim)
        .filter(|command| !command.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert!(
        !required_commands.is_empty(),
        "REDIS_MODULE_COMMANDS must name at least one required command"
    );
    let mut command = RawCommand::new("COMMAND").arg("INFO");
    for required in &required_commands {
        command = command.arg(required);
    }
    let response = connection
        .execute(command)
        .await
        .expect("COMMAND INFO capability discovery must succeed");
    let entries = match response {
        Frame::Array(Some(entries)) => entries,
        other => panic!("COMMAND INFO returned an unexpected frame: {other:?}"),
    };
    assert_eq!(
        entries.len(),
        required_commands.len(),
        "COMMAND INFO did not preserve one result per requested capability"
    );
    for (name, entry) in required_commands.iter().zip(entries) {
        assert!(
            !matches!(entry, Frame::Null | Frame::Array(None)),
            "required module command {name} is unavailable"
        );
    }

    eprintln!(
        "module capability evidence: redis_version={version_text}; commands={}",
        required_commands.join(",")
    );
}

#[test]
fn version_parser_accepts_release_and_prerelease_forms() {
    assert_eq!(parse_version("8.0.6"), (8, 0, 6));
    assert_eq!(parse_version("8.4.0-rc1"), (8, 4, 0));
}
