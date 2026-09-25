//! Consumer-derived semantic comparisons with redis-rs.
//!
//! This suite deliberately uses independent key namespaces for mutations. It
//! compares public results rather than wire frames, with normalization limited
//! to Redis collections whose order or RESP2/RESP3 shape is not contractual.

mod common;

use std::fmt;

use bytes::Bytes;
use common::redis_addr;
use redis_tower::commands::{
    BitOp, BitOperation, Echo, Eval, GeoAdd, HSet, PfAdd, Publish, RPush, RawCommand, Rename, SAdd,
    Set, SetOutcome, SetPreviousValue, SetStatus, XAdd, ZAdd,
};
use redis_tower::{
    Command, Frame, Pipeline, ProtocolVersion, RedisConnection, RedisError, Transaction,
    TransactionResult,
};

const CORPUS_SEED: u64 = 0x5245_4449_535f_4d43;
const REDIS_RS_VERSION: &str = "1.7.0";
const REDIS_TOWER_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Copy, Debug)]
enum Protocol {
    Resp2,
    Resp3,
}

impl Protocol {
    const ALL: [Self; 2] = [Self::Resp2, Self::Resp3];

    fn tower(self) -> ProtocolVersion {
        match self {
            Self::Resp2 => ProtocolVersion::Resp2,
            Self::Resp3 => ProtocolVersion::Resp3,
        }
    }

    fn query(self) -> &'static str {
        match self {
            Self::Resp2 => "resp2",
            Self::Resp3 => "resp3",
        }
    }
}

impl fmt::Display for Protocol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.query())
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum SemanticValue {
    Null,
    Bytes(Vec<u8>),
    Integer(i64),
    Double(String),
    Boolean(bool),
    Array(Vec<Self>),
    Map(Vec<(Self, Self)>),
    Error(String),
    Unsupported(String),
}

fn float_text(value: f64) -> String {
    if value.is_nan() {
        "nan".to_owned()
    } else if value == f64::INFINITY {
        "inf".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-inf".to_owned()
    } else {
        value.to_string()
    }
}

fn error_code(message: &[u8]) -> String {
    String::from_utf8_lossy(message)
        .split_ascii_whitespace()
        .next()
        .unwrap_or("ERR")
        .to_ascii_uppercase()
}

fn displayed_error_code(rendered: &str) -> String {
    rendered
        .split_ascii_whitespace()
        .map(|word| word.trim_matches(|character: char| !character.is_ascii_alphanumeric()))
        .find(|word| {
            word.len() > 1
                && word.chars().all(|character| {
                    !character.is_ascii_alphabetic() || character.is_ascii_uppercase()
                })
        })
        .unwrap_or("CLIENT")
        .to_owned()
}

fn tower_value(frame: Frame) -> SemanticValue {
    match frame {
        Frame::SimpleString(value) => SemanticValue::Bytes(value.to_vec()),
        Frame::Error(value) | Frame::BlobError(value) => SemanticValue::Error(error_code(&value)),
        Frame::Integer(value) => SemanticValue::Integer(value),
        Frame::BulkString(Some(value)) => SemanticValue::Bytes(value.to_vec()),
        Frame::BulkString(None) | Frame::Array(None) | Frame::Null => SemanticValue::Null,
        Frame::Double(value) => SemanticValue::Double(float_text(value)),
        Frame::SpecialFloat(value) => {
            SemanticValue::Double(String::from_utf8_lossy(&value).to_ascii_lowercase())
        }
        Frame::Boolean(value) => SemanticValue::Boolean(value),
        Frame::BigNumber(value) => SemanticValue::Bytes(value.to_vec()),
        Frame::VerbatimString(_format, value) => SemanticValue::Bytes(value.to_vec()),
        Frame::Array(Some(values))
        | Frame::Set(values)
        | Frame::Push(values)
        | Frame::StreamedArray(values)
        | Frame::StreamedSet(values)
        | Frame::StreamedPush(values) => {
            SemanticValue::Array(values.into_iter().map(tower_value).collect())
        }
        Frame::Map(values) | Frame::StreamedMap(values) => SemanticValue::Map(
            values
                .into_iter()
                .map(|(key, value)| (tower_value(key), tower_value(value)))
                .collect(),
        ),
        Frame::Attribute(values) | Frame::StreamedAttribute(values) => {
            SemanticValue::Unsupported(format!("attribute:{values:?}"))
        }
        Frame::StreamedString(values) => SemanticValue::Bytes(
            values
                .into_iter()
                .flat_map(|value| value.to_vec())
                .collect(),
        ),
        Frame::StreamedStringChunk(value) => SemanticValue::Bytes(value.to_vec()),
        other => SemanticValue::Unsupported(format!("{other:?}")),
    }
}

fn redis_rs_value(value: redis::Value) -> SemanticValue {
    match value {
        redis::Value::Nil => SemanticValue::Null,
        redis::Value::Int(value) => SemanticValue::Integer(value),
        redis::Value::BulkString(value) => SemanticValue::Bytes(value),
        redis::Value::Array(values) | redis::Value::Set(values) => {
            SemanticValue::Array(values.into_iter().map(redis_rs_value).collect())
        }
        redis::Value::SimpleString(value) => SemanticValue::Bytes(value.into_bytes()),
        redis::Value::Okay => SemanticValue::Bytes(b"OK".to_vec()),
        redis::Value::Map(values) => SemanticValue::Map(
            values
                .into_iter()
                .map(|(key, value)| (redis_rs_value(key), redis_rs_value(value)))
                .collect(),
        ),
        redis::Value::Attribute { data, .. } => redis_rs_value(*data),
        redis::Value::Double(value) => SemanticValue::Double(float_text(value)),
        redis::Value::Boolean(value) => SemanticValue::Boolean(value),
        redis::Value::VerbatimString { text, .. } => SemanticValue::Bytes(text.into_bytes()),
        redis::Value::BigNumber(value) => SemanticValue::Bytes(value.to_string().into_bytes()),
        redis::Value::Push { data, .. } => {
            SemanticValue::Array(data.into_iter().map(redis_rs_value).collect())
        }
        redis::Value::ServerError(error) => SemanticValue::Error(error.code().to_owned()),
        other => SemanticValue::Unsupported(format!("{other:?}")),
    }
}

fn unordered(value: SemanticValue) -> SemanticValue {
    match value {
        SemanticValue::Array(mut values) => {
            values.sort();
            SemanticValue::Array(values)
        }
        other => other,
    }
}

fn pair_entries(value: SemanticValue) -> SemanticValue {
    match value {
        SemanticValue::Map(values) => SemanticValue::Map(values),
        SemanticValue::Array(values) => {
            let mut result = Vec::new();
            if values
                .iter()
                .all(|value| matches!(value, SemanticValue::Array(pair) if pair.len() == 2))
            {
                for value in values {
                    let SemanticValue::Array(mut pair) = value else {
                        unreachable!("pair shape checked")
                    };
                    let second = pair.pop().expect("two elements");
                    let first = pair.pop().expect("two elements");
                    result.push((first, second));
                }
            } else {
                assert!(
                    values.len().is_multiple_of(2),
                    "pair normalization requires an even response"
                );
                let mut values = values.into_iter();
                while let Some(first) = values.next() {
                    result.push((first, values.next().expect("even length checked")));
                }
            }
            SemanticValue::Map(result)
        }
        other => other,
    }
}

fn pairs(value: SemanticValue) -> SemanticValue {
    match pair_entries(value) {
        SemanticValue::Map(mut values) => {
            values.sort();
            SemanticValue::Map(values)
        }
        other => other,
    }
}

fn diagnostic(
    case: &str,
    step: &str,
    side: &str,
    protocol: Protocol,
    server_version: &str,
) -> String {
    format!(
        "case={case} step={step} side={side} protocol={protocol} server={server_version} \
         redis-tower={REDIS_TOWER_VERSION} redis-rs={REDIS_RS_VERSION} seed={CORPUS_SEED:#x}"
    )
}

#[derive(Clone, Debug)]
struct ObservedError {
    code: String,
    server: bool,
    diagnostic: String,
}

fn tower_error(error: RedisError, diagnostic: String) -> ObservedError {
    let rendered = error.to_string();
    let prefix = error.server_error_prefix();
    let code = match prefix {
        Some(prefix)
            if prefix
                .chars()
                .all(|character| !character.is_ascii_lowercase()) =>
        {
            prefix.to_owned()
        }
        _ => displayed_error_code(&rendered),
    };
    ObservedError {
        server: error.server_error_prefix().is_some(),
        code,
        diagnostic,
    }
}

fn redis_rs_error(error: redis::RedisError, diagnostic: String) -> ObservedError {
    ObservedError {
        code: error.code().unwrap_or("CLIENT").to_owned(),
        server: error.code().is_some(),
        diagnostic,
    }
}

struct TowerAdapter {
    connection: RedisConnection,
    case: &'static str,
    protocol: Protocol,
    server_version: String,
}

impl TowerAdapter {
    async fn connect(addr: &str, case: &'static str, protocol: Protocol) -> Self {
        let context = diagnostic(case, "CONNECT", "redis-tower", protocol, "unavailable");
        Self {
            connection: RedisConnection::connect_with_protocol(addr, protocol.tower())
                .await
                .unwrap_or_else(|_| panic!("{context} error=connection-failed")),
            case,
            protocol,
            server_version: "unavailable".to_owned(),
        }
    }

    fn set_server_version(&mut self, version: &str) {
        self.server_version = version.to_owned();
    }

    fn diagnostic(&self, command: &str) -> String {
        diagnostic(
            self.case,
            command,
            "redis-tower",
            self.protocol,
            &self.server_version,
        )
    }

    async fn typed<C>(&mut self, step: &str, command: C) -> Result<C::Response, ObservedError>
    where
        C: Command,
    {
        let diagnostic = self.diagnostic(step);
        self.connection
            .execute(command)
            .await
            .map_err(|error| tower_error(error, diagnostic))
    }

    async fn raw(&mut self, command: &str, args: &[&[u8]]) -> Result<SemanticValue, ObservedError> {
        let diagnostic = self.diagnostic(command);
        let command = args
            .iter()
            .fold(RawCommand::new(command), |command, arg| command.arg(arg));
        self.connection
            .execute(command)
            .await
            .map(tower_value)
            .map_err(|error| tower_error(error, diagnostic))
    }

    async fn u64(&mut self, command: &str, args: &[&[u8]]) -> Result<u64, ObservedError> {
        let diagnostic = self.diagnostic(command);
        let command = args
            .iter()
            .fold(RawCommand::new(command), |command, arg| command.arg(arg))
            .query::<u64>();
        self.connection
            .execute(command)
            .await
            .map_err(|error| tower_error(error, diagnostic))
    }

    async fn string(&mut self, command: &str, args: &[&[u8]]) -> Result<String, ObservedError> {
        let diagnostic = self.diagnostic(command);
        let command = args
            .iter()
            .fold(RawCommand::new(command), |command, arg| command.arg(arg))
            .query::<String>();
        self.connection
            .execute(command)
            .await
            .map_err(|error| tower_error(error, diagnostic))
    }
}

struct RedisRsAdapter {
    connection: redis::aio::MultiplexedConnection,
    case: &'static str,
    protocol: Protocol,
    server_version: String,
}

impl RedisRsAdapter {
    async fn connect(addr: &str, case: &'static str, protocol: Protocol) -> Self {
        let url = format!("redis://{addr}/?protocol={}", protocol.query());
        let context = diagnostic(case, "CONNECT", "redis-rs", protocol, "unavailable");
        let client = redis::Client::open(url)
            .unwrap_or_else(|_| panic!("{context} error=invalid-connection-url"));
        Self {
            connection: client
                .get_multiplexed_async_connection()
                .await
                .unwrap_or_else(|_| panic!("{context} error=connection-failed")),
            case,
            protocol,
            server_version: "unavailable".to_owned(),
        }
    }

    fn set_server_version(&mut self, version: &str) {
        self.server_version = version.to_owned();
    }

    fn diagnostic(&self, command: &str) -> String {
        diagnostic(
            self.case,
            command,
            "redis-rs",
            self.protocol,
            &self.server_version,
        )
    }

    async fn raw(&mut self, command: &str, args: &[&[u8]]) -> Result<SemanticValue, ObservedError> {
        let diagnostic = self.diagnostic(command);
        let mut command = redis::cmd(command);
        for arg in args {
            command.arg(arg);
        }
        command
            .query_async::<redis::Value>(&mut self.connection)
            .await
            .map(redis_rs_value)
            .map_err(|error| redis_rs_error(error, diagnostic))
    }

    async fn u64(&mut self, command: &str, args: &[&[u8]]) -> Result<u64, ObservedError> {
        let diagnostic = self.diagnostic(command);
        let mut command = redis::cmd(command);
        for arg in args {
            command.arg(arg);
        }
        command
            .query_async::<u64>(&mut self.connection)
            .await
            .map_err(|error| redis_rs_error(error, diagnostic))
    }

    async fn string(&mut self, command: &str, args: &[&[u8]]) -> Result<String, ObservedError> {
        let diagnostic = self.diagnostic(command);
        let mut command = redis::cmd(command);
        for arg in args {
            command.arg(arg);
        }
        command
            .query_async::<String>(&mut self.connection)
            .await
            .map_err(|error| redis_rs_error(error, diagnostic))
    }
}

struct Pair {
    tower: TowerAdapter,
    redis_rs: RedisRsAdapter,
    case: &'static str,
    protocol: Protocol,
    server_version: String,
}

impl Pair {
    async fn connect(case: &'static str, protocol: Protocol) -> Self {
        let addr = redis_addr().await;
        let mut tower = TowerAdapter::connect(addr, case, protocol).await;
        let mut redis_rs = RedisRsAdapter::connect(addr, case, protocol).await;
        let info = tower
            .raw("INFO", &[b"server"])
            .await
            .expect("INFO server should succeed");
        let SemanticValue::Bytes(info) = info else {
            panic!("INFO server returned an unexpected shape")
        };
        let info = String::from_utf8_lossy(&info);
        let server_version = info
            .lines()
            .find_map(|line| line.strip_prefix("redis_version:"))
            .unwrap_or("unknown")
            .trim()
            .to_owned();
        tower.set_server_version(&server_version);
        redis_rs.set_server_version(&server_version);
        let pair = Self {
            tower,
            redis_rs,
            case,
            protocol,
            server_version,
        };
        eprintln!("differential leg: {}", pair.diagnostic("START", "both"));
        pair
    }

    fn key(&self, side: &str, suffix: &str) -> Vec<u8> {
        format!(
            "redis_tower:diff:{CORPUS_SEED:016x}:{}:{}:{side}:{suffix}",
            self.protocol, self.case
        )
        .into_bytes()
    }

    fn binary_key(&self, side: &str, suffix: &str) -> Vec<u8> {
        let mut key = self.key(side, suffix);
        key.extend_from_slice(&[b':', 0xff, 0, 0x80]);
        key
    }

    fn same(&self, step: &str, tower: SemanticValue, redis_rs: SemanticValue) {
        assert_eq!(
            tower, redis_rs,
            "case={} step={step} protocol={} server={} redis-tower={} redis-rs={} seed={CORPUS_SEED:#x}",
            self.case, self.protocol, self.server_version, REDIS_TOWER_VERSION, REDIS_RS_VERSION
        );
    }

    fn same_error(&self, step: &str, tower: ObservedError, redis_rs: ObservedError) {
        assert_eq!(
            (tower.code, tower.server),
            (redis_rs.code, redis_rs.server),
            "case={} step={step} protocol={} server={} redis-tower={} redis-rs={} seed={CORPUS_SEED:#x}",
            self.case,
            self.protocol,
            self.server_version,
            REDIS_TOWER_VERSION,
            REDIS_RS_VERSION
        );
    }

    fn diagnostic(&self, step: &str, side: &str) -> String {
        diagnostic(self.case, step, side, self.protocol, &self.server_version)
    }

    async fn reset(&mut self, tower_key: &[u8], redis_key: &[u8]) {
        self.tower
            .raw("DEL", &[tower_key])
            .await
            .expect("redis-tower namespace reset");
        self.redis_rs
            .raw("DEL", &[redis_key])
            .await
            .expect("redis-rs namespace reset");
    }
}

fn panic_message(error: tokio::task::JoinError) -> String {
    let payload = error.into_panic();
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        "non-string panic".to_owned()
    }
}

#[tokio::test]
async fn differential_connection_diagnostics_do_not_expose_credentials() {
    const USER: &str = "review-user";
    const PASSWORD: &str = "REVIEW_SECRET_TOKEN";
    const SENSITIVE_TARGET: &str = "review-user:REVIEW_SECRET_TOKEN@127.0.0.1:not-a-port";

    let tower = tokio::spawn(async {
        TowerAdapter::connect(SENSITIVE_TARGET, "credential-redaction", Protocol::Resp2).await
    })
    .await;
    let tower_message = match tower {
        Err(error) if error.is_panic() => panic_message(error),
        _ => panic!("synthetic redis-tower connection unexpectedly succeeded"),
    };

    let redis_rs = tokio::spawn(async {
        RedisRsAdapter::connect(SENSITIVE_TARGET, "credential-redaction", Protocol::Resp2).await
    })
    .await;
    let redis_rs_message = match redis_rs {
        Err(error) if error.is_panic() => panic_message(error),
        _ => panic!("synthetic redis-rs connection unexpectedly succeeded"),
    };

    for (side, message) in [
        ("redis-tower", tower_message),
        ("redis-rs", redis_rs_message),
    ] {
        assert!(message.contains("case=credential-redaction"));
        assert!(message.contains(&format!("side={side}")));
        assert!(message.contains("error="));
        assert!(!message.contains(USER), "username leaked through {side}");
        assert!(
            !message.contains(PASSWORD),
            "password leaked through {side}"
        );
        assert!(
            !message.contains(SENSITIVE_TARGET),
            "connection target leaked through {side}"
        );
    }
}

#[tokio::test]
async fn diff_mcp_scalar_nil_binary_and_numeric_boundaries() {
    for protocol in Protocol::ALL {
        let mut pair = Pair::connect("scalar-binary", protocol).await;
        let tower_missing = pair.key("tower", "missing");
        let redis_missing = pair.key("redis-rs", "missing");
        pair.reset(&tower_missing, &redis_missing).await;
        let tower = pair.tower.raw("GET", &[&tower_missing]).await.unwrap();
        let redis_rs = pair.redis_rs.raw("GET", &[&redis_missing]).await.unwrap();
        pair.same("null", tower, redis_rs);

        let tower_key = pair.binary_key("tower", "payload");
        let redis_key = pair.binary_key("redis-rs", "payload");
        pair.reset(&tower_key, &redis_key).await;
        let payload = [0, 0xff, 0x80, b'a', 0];
        let tower = pair
            .tower
            .raw("SET", &[&tower_key, &payload])
            .await
            .unwrap();
        let redis_rs = pair
            .redis_rs
            .raw("SET", &[&redis_key, &payload])
            .await
            .unwrap();
        pair.same("binary-set", tower, redis_rs);
        let tower = pair.tower.raw("GET", &[&tower_key]).await.unwrap();
        let redis_rs = pair.redis_rs.raw("GET", &[&redis_key]).await.unwrap();
        pair.same("binary-get", tower, redis_rs);

        let tower_empty = pair.key("tower", "empty");
        let redis_empty = pair.key("redis-rs", "empty");
        pair.reset(&tower_empty, &redis_empty).await;
        pair.tower.raw("SET", &[&tower_empty, b""]).await.unwrap();
        pair.redis_rs
            .raw("SET", &[&redis_empty, b""])
            .await
            .unwrap();
        let tower = pair
            .tower
            .raw("MGET", &[&tower_key, &tower_missing, &tower_empty])
            .await
            .unwrap();
        let redis_rs = pair
            .redis_rs
            .raw("MGET", &[&redis_key, &redis_missing, &redis_empty])
            .await
            .unwrap();
        pair.same("null-versus-empty", tower, redis_rs);

        let tower = pair.tower.raw("ECHO", &[&payload]).await.unwrap();
        let redis_rs = pair.redis_rs.raw("ECHO", &[&payload]).await.unwrap();
        pair.same("binary-echo", tower, redis_rs);

        let tower_number = pair.key("tower", "u64-max");
        let redis_number = pair.key("redis-rs", "u64-max");
        pair.reset(&tower_number, &redis_number).await;
        let max = u64::MAX.to_string();
        pair.tower
            .raw("SET", &[&tower_number, max.as_bytes()])
            .await
            .unwrap();
        pair.redis_rs
            .raw("SET", &[&redis_number, max.as_bytes()])
            .await
            .unwrap();
        assert_eq!(
            pair.tower.u64("GET", &[&tower_number]).await.unwrap(),
            pair.redis_rs.u64("GET", &[&redis_number]).await.unwrap(),
            "u64 boundary diverged for {} on {}",
            pair.protocol,
            pair.server_version
        );

        pair.tower
            .raw("SET", &[&tower_number, b"-1"])
            .await
            .unwrap();
        pair.redis_rs
            .raw("SET", &[&redis_number, b"-1"])
            .await
            .unwrap();
        let tower_range = pair.tower.u64("GET", &[&tower_number]).await.unwrap_err();
        let redis_range = pair
            .redis_rs
            .u64("GET", &[&redis_number])
            .await
            .unwrap_err();
        assert_eq!(
            tower_range.diagnostic,
            pair.diagnostic("GET", "redis-tower")
        );
        assert_eq!(redis_range.diagnostic, pair.diagnostic("GET", "redis-rs"));

        let tower_utf8 = pair.tower.string("ECHO", &[&payload]).await.unwrap_err();
        let redis_utf8 = pair.redis_rs.string("ECHO", &[&payload]).await.unwrap_err();
        assert_eq!(
            tower_utf8.diagnostic,
            pair.diagnostic("ECHO", "redis-tower")
        );
        assert_eq!(redis_utf8.diagnostic, pair.diagnostic("ECHO", "redis-rs"));
    }
}

#[tokio::test]
async fn diff_typed_remaining_families_preserve_opaque_bytes() {
    for protocol in Protocol::ALL {
        let mut pair = Pair::connect("typed-remaining-binary", protocol).await;
        let binary = [0xff, 0, b'\r', b'\n', b'*', b'3'];

        let tower_source = pair.binary_key("tower", "rename-source");
        let redis_source = pair.binary_key("redis-rs", "rename-source");
        let tower_destination = pair.binary_key("tower", "rename-destination");
        let redis_destination = pair.binary_key("redis-rs", "rename-destination");
        pair.reset(&tower_source, &redis_source).await;
        pair.reset(&tower_destination, &redis_destination).await;
        pair.tower
            .raw("SET", &[&tower_source, &binary])
            .await
            .unwrap();
        pair.redis_rs
            .raw("SET", &[&redis_source, &binary])
            .await
            .unwrap();
        pair.tower
            .typed(
                "typed RENAME",
                Rename::new(&tower_source, &tower_destination),
            )
            .await
            .unwrap();
        pair.redis_rs
            .raw("RENAME", &[&redis_source, &redis_destination])
            .await
            .unwrap();
        let tower = pair.tower.raw("GET", &[&tower_destination]).await.unwrap();
        let redis_rs = pair
            .redis_rs
            .raw("GET", &[&redis_destination])
            .await
            .unwrap();
        pair.same("typed-rename", tower, redis_rs);

        let tower_script_key = pair.binary_key("tower", "script");
        let redis_script_key = pair.binary_key("redis-rs", "script");
        let script = b"return {string.sub(KEYS[1], -3), ARGV[1]}";
        let tower = pair
            .tower
            .typed(
                "typed EVAL",
                Eval::new(script).key(&tower_script_key).arg(binary),
            )
            .await
            .map(tower_value)
            .unwrap();
        let redis_rs = pair
            .redis_rs
            .raw("EVAL", &[script, b"1", &redis_script_key, &binary])
            .await
            .unwrap();
        pair.same("typed-eval", tower, redis_rs);

        let tower = pair
            .tower
            .typed("typed ECHO", Echo::new(binary))
            .await
            .map(|value| SemanticValue::Bytes(value.to_vec()))
            .unwrap();
        let redis_rs = pair.redis_rs.raw("ECHO", &[&binary]).await.unwrap();
        pair.same("typed-echo", tower, redis_rs);

        let tower_channel = pair.binary_key("tower", "channel");
        let redis_channel = pair.binary_key("redis-rs", "channel");
        let tower = pair
            .tower
            .typed("typed PUBLISH", Publish::new(&tower_channel, binary))
            .await
            .map(SemanticValue::Integer)
            .unwrap();
        let redis_rs = pair
            .redis_rs
            .raw("PUBLISH", &[&redis_channel, &binary])
            .await
            .unwrap();
        pair.same("typed-publish", tower, redis_rs);

        let tower_geo = pair.binary_key("tower", "geo");
        let redis_geo = pair.binary_key("redis-rs", "geo");
        pair.reset(&tower_geo, &redis_geo).await;
        let tower = pair
            .tower
            .typed(
                "typed GEOADD",
                GeoAdd::new(&tower_geo).member(-122.4194, 37.7749, binary),
            )
            .await
            .map(SemanticValue::Integer)
            .unwrap();
        let redis_rs = pair
            .redis_rs
            .raw("GEOADD", &[&redis_geo, b"-122.4194", b"37.7749", &binary])
            .await
            .unwrap();
        pair.same("typed-geoadd", tower, redis_rs);

        let tower_hll = pair.binary_key("tower", "hll");
        let redis_hll = pair.binary_key("redis-rs", "hll");
        pair.reset(&tower_hll, &redis_hll).await;
        pair.tower
            .typed("typed PFADD", PfAdd::new(&tower_hll, binary))
            .await
            .unwrap();
        pair.redis_rs
            .raw("PFADD", &[&redis_hll, &binary])
            .await
            .unwrap();
        let tower = pair.tower.raw("PFCOUNT", &[&tower_hll]).await.unwrap();
        let redis_rs = pair.redis_rs.raw("PFCOUNT", &[&redis_hll]).await.unwrap();
        pair.same("typed-hll", tower, redis_rs);

        let tower_bitmap = pair.binary_key("tower", "bitmap");
        let redis_bitmap = pair.binary_key("redis-rs", "bitmap");
        pair.reset(&tower_bitmap, &redis_bitmap).await;
        pair.tower
            .raw("SET", &[&tower_bitmap, &binary])
            .await
            .unwrap();
        pair.redis_rs
            .raw("SET", &[&redis_bitmap, &binary])
            .await
            .unwrap();
        let tower = pair
            .tower
            .typed(
                "typed BITOP",
                BitOp::new(BitOperation::Not, &tower_destination, [&tower_bitmap]),
            )
            .await
            .map(SemanticValue::Integer)
            .unwrap();
        let redis_rs = pair
            .redis_rs
            .raw("BITOP", &[b"NOT", &redis_destination, &redis_bitmap])
            .await
            .unwrap();
        pair.same("typed-bitop", tower, redis_rs);
    }
}

#[tokio::test]
async fn diff_mcp_typed_set_builder_options() {
    for protocol in Protocol::ALL {
        let mut pair = Pair::connect("typed-set-options", protocol).await;
        let tower_key = pair.key("tower", "set-option");
        let redis_key = pair.key("redis-rs", "set-option");
        pair.reset(&tower_key, &redis_key).await;

        let existing = b"\0\xffexisting".as_slice();
        pair.tower
            .raw("SET", &[&tower_key, existing])
            .await
            .unwrap();
        pair.redis_rs
            .raw("SET", &[&redis_key, existing])
            .await
            .unwrap();

        let tower_nx = pair
            .tower
            .typed(
                "SET NX GET rejected",
                Set::new(&tower_key, b"replacement".as_slice())
                    .nx()
                    .get()
                    .with_outcome(),
            )
            .await
            .unwrap();
        let redis_context = pair.diagnostic("SET NX GET rejected", "redis-rs");
        let redis_nx = redis::cmd("SET")
            .arg(&redis_key)
            .arg(b"replacement")
            .arg("NX")
            .arg("GET")
            .query_async::<Option<Vec<u8>>>(&mut pair.redis_rs.connection)
            .await
            .unwrap_or_else(|_| panic!("{redis_context} error=command-failed"));
        assert_eq!(
            tower_nx,
            SetOutcome {
                status: SetStatus::NotApplied,
                previous: redis_nx
                    .map(|value| SetPreviousValue::Value(Bytes::from(value)))
                    .unwrap_or(SetPreviousValue::Missing),
            },
            "{}",
            pair.diagnostic("SET NX GET rejected result", "both")
        );
        let tower = pair.tower.raw("GET", &[&tower_key]).await.unwrap();
        let redis_rs = pair.redis_rs.raw("GET", &[&redis_key]).await.unwrap();
        pair.same("SET NX rejected stored value", tower, redis_rs);

        pair.reset(&tower_key, &redis_key).await;
        let tower_nx = pair
            .tower
            .typed(
                "SET NX GET applied",
                Set::new(&tower_key, b"replacement".as_slice())
                    .nx()
                    .get()
                    .with_outcome(),
            )
            .await
            .unwrap();
        let redis_context = pair.diagnostic("SET NX GET applied", "redis-rs");
        let redis_nx = redis::cmd("SET")
            .arg(&redis_key)
            .arg(b"replacement")
            .arg("NX")
            .arg("GET")
            .query_async::<Option<Vec<u8>>>(&mut pair.redis_rs.connection)
            .await
            .unwrap_or_else(|_| panic!("{redis_context} error=command-failed"));
        assert_eq!(
            tower_nx,
            SetOutcome {
                status: SetStatus::Applied,
                previous: redis_nx
                    .map(|value| SetPreviousValue::Value(Bytes::from(value)))
                    .unwrap_or(SetPreviousValue::Missing),
            },
            "{}",
            pair.diagnostic("SET NX GET applied result", "both")
        );
        let tower = pair.tower.raw("GET", &[&tower_key]).await.unwrap();
        let redis_rs = pair.redis_rs.raw("GET", &[&redis_key]).await.unwrap();
        pair.same("SET NX applied stored value", tower, redis_rs);

        pair.reset(&tower_key, &redis_key).await;
        let tower_xx = pair
            .tower
            .typed(
                "SET XX GET rejected",
                Set::new(&tower_key, b"replacement".as_slice())
                    .xx()
                    .get()
                    .with_outcome(),
            )
            .await
            .unwrap();
        let redis_context = pair.diagnostic("SET XX GET rejected", "redis-rs");
        let redis_xx = redis::cmd("SET")
            .arg(&redis_key)
            .arg(b"replacement")
            .arg("XX")
            .arg("GET")
            .query_async::<Option<Vec<u8>>>(&mut pair.redis_rs.connection)
            .await
            .unwrap_or_else(|_| panic!("{redis_context} error=command-failed"));
        assert_eq!(
            tower_xx,
            SetOutcome {
                status: SetStatus::NotApplied,
                previous: redis_xx
                    .map(|value| SetPreviousValue::Value(Bytes::from(value)))
                    .unwrap_or(SetPreviousValue::Missing),
            },
            "{}",
            pair.diagnostic("SET XX GET rejected result", "both")
        );
        let tower = pair.tower.raw("GET", &[&tower_key]).await.unwrap();
        let redis_rs = pair.redis_rs.raw("GET", &[&redis_key]).await.unwrap();
        pair.same("SET XX rejected stored value", tower, redis_rs);

        pair.tower.raw("SET", &[&tower_key, b""]).await.unwrap();
        pair.redis_rs.raw("SET", &[&redis_key, b""]).await.unwrap();
        let tower_xx = pair
            .tower
            .typed(
                "SET XX GET applied",
                Set::new(&tower_key, b"replacement".as_slice())
                    .xx()
                    .get()
                    .with_outcome(),
            )
            .await
            .unwrap();
        let redis_context = pair.diagnostic("SET XX GET applied", "redis-rs");
        let redis_xx = redis::cmd("SET")
            .arg(&redis_key)
            .arg(b"replacement")
            .arg("XX")
            .arg("GET")
            .query_async::<Option<Vec<u8>>>(&mut pair.redis_rs.connection)
            .await
            .unwrap_or_else(|_| panic!("{redis_context} error=command-failed"));
        assert_eq!(
            tower_xx,
            SetOutcome {
                status: SetStatus::Applied,
                previous: redis_xx
                    .map(|value| SetPreviousValue::Value(Bytes::from(value)))
                    .unwrap_or(SetPreviousValue::Missing),
            },
            "{}",
            pair.diagnostic("SET XX GET applied result", "both")
        );
        let tower = pair.tower.raw("GET", &[&tower_key]).await.unwrap();
        let redis_rs = pair.redis_rs.raw("GET", &[&redis_key]).await.unwrap();
        pair.same("SET XX applied stored value", tower, redis_rs);
    }
}

#[tokio::test]
async fn diff_mcp_hash_collection_and_stream_shapes() {
    for protocol in Protocol::ALL {
        let mut pair = Pair::connect("collections-streams", protocol).await;
        let tower_hash = pair.binary_key("tower", "hash");
        let redis_hash = pair.binary_key("redis-rs", "hash");
        pair.reset(&tower_hash, &redis_hash).await;
        let binary = [0xff, 0, 0x80];
        pair.tower
            .typed(
                "typed HSET",
                HSet::new(&tower_hash, b"field-b", binary).field(b"field-a", b"one"),
            )
            .await
            .unwrap();
        pair.redis_rs
            .raw(
                "HSET",
                &[&redis_hash, b"field-b", &binary, b"field-a", b"one"],
            )
            .await
            .unwrap();
        let tower = pairs(pair.tower.raw("HGETALL", &[&tower_hash]).await.unwrap());
        let redis_rs = pairs(pair.redis_rs.raw("HGETALL", &[&redis_hash]).await.unwrap());
        pair.same("hash-map", tower, redis_rs);

        let tower_list = pair.binary_key("tower", "list");
        let redis_list = pair.binary_key("redis-rs", "list");
        pair.reset(&tower_list, &redis_list).await;
        pair.tower
            .typed(
                "typed RPUSH",
                RPush::elements(
                    &tower_list,
                    [b"first".as_slice(), &binary, b"third".as_slice()],
                ),
            )
            .await
            .unwrap();
        pair.redis_rs
            .raw("RPUSH", &[&redis_list, b"first", &binary, b"third"])
            .await
            .unwrap();
        let tower = pair
            .tower
            .raw("LRANGE", &[&tower_list, b"0", b"-1"])
            .await
            .unwrap();
        let redis_rs = pair
            .redis_rs
            .raw("LRANGE", &[&redis_list, b"0", b"-1"])
            .await
            .unwrap();
        pair.same("ordered-list", tower, redis_rs);

        let tower_set = pair.binary_key("tower", "set");
        let redis_set = pair.binary_key("redis-rs", "set");
        pair.reset(&tower_set, &redis_set).await;
        pair.tower
            .typed(
                "typed SADD",
                SAdd::members(
                    &tower_set,
                    [b"beta".as_slice(), b"alpha".as_slice(), &binary],
                ),
            )
            .await
            .unwrap();
        pair.redis_rs
            .raw("SADD", &[&redis_set, b"beta", b"alpha", &binary])
            .await
            .unwrap();
        let tower = unordered(pair.tower.raw("SMEMBERS", &[&tower_set]).await.unwrap());
        let redis_rs = unordered(pair.redis_rs.raw("SMEMBERS", &[&redis_set]).await.unwrap());
        pair.same("unordered-set", tower, redis_rs);

        let tower_zset = pair.binary_key("tower", "zset");
        let redis_zset = pair.binary_key("redis-rs", "zset");
        pair.reset(&tower_zset, &redis_zset).await;
        pair.tower
            .typed(
                "typed ZADD",
                ZAdd::new(&tower_zset)
                    .member(1.0, b"beta")
                    .member(2.5, b"alpha"),
            )
            .await
            .unwrap();
        pair.redis_rs
            .raw("ZADD", &[&redis_zset, b"1", b"beta", b"2.5", b"alpha"])
            .await
            .unwrap();
        let tower = pair_entries(
            pair.tower
                .raw("ZRANGE", &[&tower_zset, b"0", b"-1", b"WITHSCORES"])
                .await
                .unwrap(),
        );
        let redis_rs = pair_entries(
            pair.redis_rs
                .raw("ZRANGE", &[&redis_zset, b"0", b"-1", b"WITHSCORES"])
                .await
                .unwrap(),
        );
        pair.same("zset-pairs", tower, redis_rs);

        let tower_stream = pair.binary_key("tower", "stream");
        let redis_stream = pair.binary_key("redis-rs", "stream");
        pair.reset(&tower_stream, &redis_stream).await;
        pair.tower
            .typed(
                "typed XADD",
                XAdd::new(&tower_stream).id("1-0").field(b"field", binary),
            )
            .await
            .unwrap();
        pair.redis_rs
            .raw("XADD", &[&redis_stream, b"1-0", b"field", &binary])
            .await
            .unwrap();
        let tower = pair
            .tower
            .raw("XRANGE", &[&tower_stream, b"-", b"+"])
            .await
            .unwrap();
        let redis_rs = pair
            .redis_rs
            .raw("XRANGE", &[&redis_stream, b"-", b"+"])
            .await
            .unwrap();
        pair.same("nested-stream", tower, redis_rs);
    }
}

#[tokio::test]
async fn diff_mcp_errors_raw_and_administrative_replies() {
    for protocol in Protocol::ALL {
        let mut pair = Pair::connect("errors-admin", protocol).await;
        let tower_wrongtype = pair.key("tower", "wrongtype");
        let redis_wrongtype = pair.key("redis-rs", "wrongtype");
        pair.reset(&tower_wrongtype, &redis_wrongtype).await;
        pair.tower
            .raw("RPUSH", &[&tower_wrongtype, b"value"])
            .await
            .unwrap();
        pair.redis_rs
            .raw("RPUSH", &[&redis_wrongtype, b"value"])
            .await
            .unwrap();
        let tower = pair
            .tower
            .raw("GET", &[&tower_wrongtype])
            .await
            .unwrap_err();
        let redis_rs = pair
            .redis_rs
            .raw("GET", &[&redis_wrongtype])
            .await
            .unwrap_err();
        pair.same_error("wrongtype", tower, redis_rs);

        const SENSITIVE_TYPED_ARGUMENT: &[u8] = b"DIFFERENTIAL_TYPED_SECRET";
        let typed_error = pair
            .tower
            .typed(
                "typed HSET wrongtype",
                HSet::new(
                    &tower_wrongtype,
                    SENSITIVE_TYPED_ARGUMENT,
                    SENSITIVE_TYPED_ARGUMENT,
                ),
            )
            .await
            .unwrap_err();
        let expected_diagnostic = pair.diagnostic("typed HSET wrongtype", "redis-tower");
        assert_eq!(typed_error.diagnostic, expected_diagnostic);
        for required in [
            format!("case={}", pair.case),
            "step=typed HSET wrongtype".to_owned(),
            "side=redis-tower".to_owned(),
            format!("protocol={}", pair.protocol),
            format!("server={}", pair.server_version),
            format!("redis-tower={REDIS_TOWER_VERSION}"),
            format!("redis-rs={REDIS_RS_VERSION}"),
            format!("seed={CORPUS_SEED:#x}"),
        ] {
            assert!(
                typed_error.diagnostic.contains(&required),
                "typed diagnostic omitted {required}"
            );
        }
        assert!(
            !typed_error
                .diagnostic
                .as_bytes()
                .windows(SENSITIVE_TYPED_ARGUMENT.len())
                .any(|window| window == SENSITIVE_TYPED_ARGUMENT),
            "typed diagnostic exposed a sensitive command argument"
        );
        let redis_rs = pair
            .redis_rs
            .raw(
                "HSET",
                &[
                    &redis_wrongtype,
                    SENSITIVE_TYPED_ARGUMENT,
                    SENSITIVE_TYPED_ARGUMENT,
                ],
            )
            .await
            .unwrap_err();
        pair.same_error("typed-wrongtype", typed_error, redis_rs);

        let tower_counter = pair.key("tower", "overflow");
        let redis_counter = pair.key("redis-rs", "overflow");
        pair.reset(&tower_counter, &redis_counter).await;
        let max = i64::MAX.to_string();
        pair.tower
            .raw("SET", &[&tower_counter, max.as_bytes()])
            .await
            .unwrap();
        pair.redis_rs
            .raw("SET", &[&redis_counter, max.as_bytes()])
            .await
            .unwrap();
        let tower = pair.tower.raw("INCR", &[&tower_counter]).await.unwrap_err();
        let redis_rs = pair
            .redis_rs
            .raw("INCR", &[&redis_counter])
            .await
            .unwrap_err();
        pair.same_error("integer-overflow", tower, redis_rs);

        let script = b"return {false, {}, 'OK', 42, ARGV[1]}";
        let binary = [0xff, 0, 0x80];
        let tower = pair
            .tower
            .raw("EVAL", &[script, b"0", &binary])
            .await
            .unwrap();
        let redis_rs = pair
            .redis_rs
            .raw("EVAL", &[script, b"0", &binary])
            .await
            .unwrap();
        pair.same("raw-nested-module-shape", tower, redis_rs);

        let tower = pair.tower.raw("COMMAND", &[b"INFO", b"GET"]).await.unwrap();
        let redis_rs = pair
            .redis_rs
            .raw("COMMAND", &[b"INFO", b"GET"])
            .await
            .unwrap();
        pair.same("administrative-command-info", tower, redis_rs);
    }
}

#[tokio::test]
async fn diff_mcp_blocking_command_uses_dedicated_sessions() {
    for protocol in Protocol::ALL {
        let mut pair = Pair::connect("blocking-dedicated", protocol).await;
        let tower_source = pair.key("tower", "source");
        let redis_source = pair.key("redis-rs", "source");
        let tower_destination = pair.key("tower", "destination");
        let redis_destination = pair.key("redis-rs", "destination");
        pair.reset(&tower_source, &redis_source).await;
        pair.reset(&tower_destination, &redis_destination).await;

        let binary = [0xff, 0, 0x80];
        pair.tower
            .raw("RPUSH", &[&tower_source, &binary])
            .await
            .unwrap();
        pair.redis_rs
            .raw("RPUSH", &[&redis_source, &binary])
            .await
            .unwrap();

        // Each adapter owns this connection exclusively. BLMOVE would stall a
        // shared multiplexed worker if the source were empty, which is why the
        // lifecycle contract requires a dedicated session for blocking work.
        let tower = pair
            .tower
            .raw(
                "BLMOVE",
                &[&tower_source, &tower_destination, b"LEFT", b"RIGHT", b"1"],
            )
            .await
            .unwrap();
        let redis_rs = pair
            .redis_rs
            .raw(
                "BLMOVE",
                &[&redis_source, &redis_destination, b"LEFT", b"RIGHT", b"1"],
            )
            .await
            .unwrap();
        pair.same("blocking-binary-move", tower, redis_rs);
    }
}

#[tokio::test]
async fn diff_mcp_pipeline_and_transaction_outcomes() {
    for protocol in Protocol::ALL {
        let mut pair = Pair::connect("pipeline-transaction", protocol).await;
        let tower_key = pair.key("tower", "pipeline");
        let redis_key = pair.key("redis-rs", "pipeline");
        pair.reset(&tower_key, &redis_key).await;

        let tower_results = Pipeline::new()
            .push(RawCommand::new("SET").arg(&tower_key).arg("value"))
            .push(
                RawCommand::new("HSET")
                    .arg(&tower_key)
                    .arg("field")
                    .arg("x"),
            )
            .push(RawCommand::new("GET").arg(&tower_key))
            .execute(&mut pair.tower.connection)
            .await
            .unwrap();
        let tower = [
            tower_results.get::<Frame>(0).cloned().map(tower_value),
            tower_results.get::<Frame>(1).cloned().map(tower_value),
            tower_results.get::<Frame>(2).cloned().map(tower_value),
        ];
        assert!(
            tower[1].is_err(),
            "tower pipeline did not preserve WRONGTYPE"
        );

        let mut redis_pipeline = redis::pipe();
        redis_pipeline
            .cmd("SET")
            .arg(&redis_key)
            .arg("value")
            .cmd("HSET")
            .arg(&redis_key)
            .arg("field")
            .arg("x")
            .cmd("GET")
            .arg(&redis_key)
            .ignore_errors();
        let redis_results: Vec<redis::RedisResult<redis::Value>> = redis_pipeline
            .query_async(&mut pair.redis_rs.connection)
            .await
            .unwrap();
        let redis_rs: Vec<Result<SemanticValue, RedisError>> = redis_results
            .into_iter()
            .map(|result| {
                result
                    .map(redis_rs_value)
                    .map_err(|error| RedisError::Redis(error.code().unwrap_or("CLIENT").to_owned()))
            })
            .collect();
        assert_eq!(tower.len(), redis_rs.len());
        for index in [0, 2] {
            pair.same(
                "pipeline-alignment",
                tower[index].as_ref().unwrap().clone(),
                redis_rs[index].as_ref().unwrap().clone(),
            );
        }
        assert_eq!(
            displayed_error_code(&tower[1].as_ref().unwrap_err().to_string()),
            displayed_error_code(&redis_rs[1].as_ref().unwrap_err().to_string())
        );
        let tower_ping = pair.tower.raw("PING", &[]).await.unwrap();
        let redis_ping = pair.redis_rs.raw("PING", &[]).await.unwrap();
        pair.same("post-pipeline-alignment", tower_ping, redis_ping);

        let tower_tx_key = pair.key("tower", "transaction");
        let redis_tx_key = pair.key("redis-rs", "transaction");
        pair.reset(&tower_tx_key, &redis_tx_key).await;
        let tower_transaction = Transaction::new()
            .push(RawCommand::new("SET").arg(&tower_tx_key).arg("40"))
            .push(RawCommand::new("INCR").arg(&tower_tx_key))
            .push(RawCommand::new("GET").arg(&tower_tx_key))
            .execute(&mut pair.tower.connection)
            .await
            .unwrap();
        let TransactionResult::Committed(tower_transaction) = tower_transaction else {
            panic!("unwatched transaction unexpectedly aborted")
        };
        let tower = vec![
            tower_value(tower_transaction.get::<Frame>(0).unwrap().clone()),
            tower_value(tower_transaction.get::<Frame>(1).unwrap().clone()),
            tower_value(tower_transaction.get::<Frame>(2).unwrap().clone()),
        ];

        let mut redis_transaction = redis::pipe();
        redis_transaction
            .atomic()
            .cmd("SET")
            .arg(&redis_tx_key)
            .arg("40")
            .cmd("INCR")
            .arg(&redis_tx_key)
            .cmd("GET")
            .arg(&redis_tx_key)
            .ignore_errors();
        let redis_results: Vec<redis::RedisResult<redis::Value>> = redis_transaction
            .query_async(&mut pair.redis_rs.connection)
            .await
            .unwrap();
        let redis_rs: Vec<_> = redis_results
            .into_iter()
            .map(|result| redis_rs_value(result.unwrap()))
            .collect();
        assert_eq!(tower, redis_rs, "transaction results diverged");

        let tower_watch = pair.key("tower", "watch-abort");
        let redis_watch = pair.key("redis-rs", "watch-abort");
        pair.reset(&tower_watch, &redis_watch).await;
        pair.tower
            .raw("SET", &[&tower_watch, b"before"])
            .await
            .unwrap();
        pair.redis_rs
            .raw("SET", &[&redis_watch, b"before"])
            .await
            .unwrap();
        pair.tower.raw("WATCH", &[&tower_watch]).await.unwrap();
        pair.redis_rs.raw("WATCH", &[&redis_watch]).await.unwrap();

        let addr = redis_addr().await;
        let mut tower_mutator = TowerAdapter::connect(addr, pair.case, protocol).await;
        let mut redis_mutator = RedisRsAdapter::connect(addr, pair.case, protocol).await;
        tower_mutator.set_server_version(&pair.server_version);
        redis_mutator.set_server_version(&pair.server_version);
        tower_mutator
            .raw("SET", &[&tower_watch, b"changed"])
            .await
            .unwrap();
        redis_mutator
            .raw("SET", &[&redis_watch, b"changed"])
            .await
            .unwrap();

        let tower_aborted = pair
            .tower
            .connection
            .execute_transaction(
                Vec::new(),
                vec![
                    RawCommand::new("SET")
                        .arg(&tower_watch)
                        .arg("committed")
                        .to_frame(),
                ],
            )
            .await
            .unwrap()
            .is_none();
        let mut redis_abort = redis::pipe();
        redis_abort
            .atomic()
            .cmd("SET")
            .arg(&redis_watch)
            .arg("committed");
        let redis_aborted = matches!(
            redis_abort
                .query_async::<redis::Value>(&mut pair.redis_rs.connection)
                .await
                .unwrap(),
            redis::Value::Nil
        );
        assert!(
            tower_aborted && redis_aborted,
            "WATCH abort semantics diverged"
        );
    }
}

fn mismatch(left: SemanticValue, right: SemanticValue) -> Result<(), String> {
    if left == right {
        Ok(())
    } else {
        Err(format!("semantic mismatch: {left:?} != {right:?}"))
    }
}

fn require_single_execution(observed: usize) -> Result<(), String> {
    if observed == 1 {
        Ok(())
    } else {
        Err(format!("non-idempotent command executed {observed} times"))
    }
}

#[tokio::test]
async fn differential_negative_controls_detect_wrong_conversion_option_and_replay() {
    assert!(
        mismatch(
            SemanticValue::Bytes(vec![0xff]),
            SemanticValue::Bytes(String::from_utf8_lossy(&[0xff]).into_owned().into_bytes())
        )
        .is_err(),
        "lossy UTF-8 conversion was not detected"
    );
    assert!(
        require_single_execution(2).is_err(),
        "accidental replay was not detected"
    );
    let ranked = SemanticValue::Array(vec![
        SemanticValue::Array(vec![
            SemanticValue::Bytes(b"beta".to_vec()),
            SemanticValue::Bytes(b"1".to_vec()),
        ]),
        SemanticValue::Array(vec![
            SemanticValue::Bytes(b"alpha".to_vec()),
            SemanticValue::Bytes(b"2.5".to_vec()),
        ]),
    ]);
    let mut reversed = ranked.clone();
    let SemanticValue::Array(ref mut entries) = reversed else {
        unreachable!("ranked fixture is an array")
    };
    entries.reverse();
    assert!(
        mismatch(pair_entries(ranked), pair_entries(reversed)).is_err(),
        "a changed sorted-set rank order was not detected"
    );

    let mut pair = Pair::connect("negative-option-control", Protocol::Resp3).await;
    let tower_key = pair.key("tower", "set-option");
    let redis_key = pair.key("redis-rs", "set-option");
    let tower_typed_key =
        String::from_utf8(tower_key.clone()).expect("generated test key is UTF-8");
    pair.reset(&tower_key, &redis_key).await;
    pair.tower
        .raw("SET", &[&tower_key, b"existing"])
        .await
        .unwrap();
    pair.redis_rs
        .raw("SET", &[&redis_key, b"existing"])
        .await
        .unwrap();
    pair.tower
        .typed(
            "SET NX GET negative control",
            Set::new(tower_typed_key, "replacement")
                .nx()
                .get()
                .with_outcome(),
        )
        .await
        .unwrap();
    let redis_context = pair.diagnostic("SET GET without NX negative control", "redis-rs");
    redis::cmd("SET")
        .arg(&redis_key)
        .arg("replacement")
        .arg("GET")
        .query_async::<Option<Vec<u8>>>(&mut pair.redis_rs.connection)
        .await
        .unwrap_or_else(|_| panic!("{redis_context} error=command-failed"));
    let tower = pair.tower.raw("GET", &[&tower_key]).await.unwrap();
    let redis_rs = pair.redis_rs.raw("GET", &[&redis_key]).await.unwrap();
    assert!(
        mismatch(tower, redis_rs).is_err(),
        "{}: a changed typed SET option was not detected",
        pair.diagnostic("SET option negative control", "both")
    );
}
