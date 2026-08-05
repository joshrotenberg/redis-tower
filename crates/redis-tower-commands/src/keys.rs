use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// DEL key [key ...]
///
/// Removes the specified keys. Returns the number of keys removed.
#[derive(Clone)]
pub struct Del {
    keys: Vec<String>,
}

impl Del {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for Del {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("DEL")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "DEL"
    }
}

/// EXISTS key [key ...]
///
/// Returns the number of specified keys that exist.
#[derive(Clone)]
pub struct Exists {
    keys: Vec<String>,
}

impl Exists {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for Exists {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("EXISTS")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "EXISTS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// Condition flag for the EXPIRE family of commands (Redis 7.0+).
#[derive(Debug, Clone, Copy)]
pub enum ExpireCondition {
    /// Set expiry only when the key has no existing expiry.
    Nx,
    /// Set expiry only when the key already has an expiry.
    Xx,
    /// Set expiry only when the new TTL is greater than the current one.
    Gt,
    /// Set expiry only when the new TTL is less than the current one.
    Lt,
}

impl ExpireCondition {
    fn as_str(&self) -> &str {
        match self {
            ExpireCondition::Nx => "NX",
            ExpireCondition::Xx => "XX",
            ExpireCondition::Gt => "GT",
            ExpireCondition::Lt => "LT",
        }
    }
}

/// EXPIRE key seconds \[NX | XX | GT | LT\]
///
/// Sets a timeout on `key`. Returns `true` if the timeout was set.
#[derive(Clone)]
pub struct Expire {
    key: String,
    seconds: u64,
    condition: Option<ExpireCondition>,
}

impl Expire {
    pub fn new(key: impl Into<String>, seconds: u64) -> Self {
        Self {
            key: key.into(),
            seconds,
            condition: None,
        }
    }

    /// Set the condition flag (NX, XX, GT, or LT).
    pub fn condition(mut self, condition: ExpireCondition) -> Self {
        self.condition = Some(condition);
        self
    }
}

impl Command for Expire {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("EXPIRE"),
            bulk(self.key.as_str()),
            bulk(self.seconds.to_string()),
        ];
        if let Some(condition) = self.condition {
            args.push(bulk(condition.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n == 1),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "EXPIRE"
    }
}

/// TTL key
///
/// Returns the remaining time to live of a key in seconds.
/// Returns -2 if the key does not exist, -1 if no expiry is set.
#[derive(Clone)]
pub struct Ttl {
    key: String,
}

impl Ttl {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Ttl {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("TTL"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "TTL"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// RENAME key newkey
///
/// Renames `key` to `newkey`. Errors if `key` does not exist.
#[derive(Clone)]
pub struct Rename {
    key: String,
    new_key: String,
}

impl Rename {
    pub fn new(key: impl Into<String>, new_key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            new_key: new_key.into(),
        }
    }
}

impl Command for Rename {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("RENAME"),
            bulk(self.key.as_str()),
            bulk(self.new_key.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "RENAME"
    }
}

/// TYPE key
///
/// Returns the type of the value stored at `key` as a string
/// (e.g., "string", "list", "set", "zset", "hash", "none").
#[derive(Clone)]
pub struct Type {
    key: String,
}

impl Type {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Type {
    type Response = String;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("TYPE"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "simple string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "TYPE"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// UNLINK key [key ...]
///
/// Removes the specified keys without blocking the server.
/// Returns the number of keys removed.
#[derive(Clone)]
pub struct Unlink {
    keys: Vec<String>,
}

impl Unlink {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for Unlink {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("UNLINK")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "UNLINK"
    }
}

/// PERSIST key
///
/// Removes the existing timeout on `key`. Returns `true` if the timeout was removed.
#[derive(Clone)]
pub struct Persist {
    key: String,
}

impl Persist {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Persist {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("PERSIST"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n == 1),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "PERSIST"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// PEXPIRE key milliseconds \[NX | XX | GT | LT\]
///
/// Sets a timeout on `key` in milliseconds. Returns `true` if the timeout was set.
#[derive(Clone)]
pub struct PExpire {
    key: String,
    milliseconds: u64,
    condition: Option<ExpireCondition>,
}

impl PExpire {
    pub fn new(key: impl Into<String>, milliseconds: u64) -> Self {
        Self {
            key: key.into(),
            milliseconds,
            condition: None,
        }
    }

    /// Set the condition flag (NX, XX, GT, or LT).
    pub fn condition(mut self, condition: ExpireCondition) -> Self {
        self.condition = Some(condition);
        self
    }
}

impl Command for PExpire {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("PEXPIRE"),
            bulk(self.key.as_str()),
            bulk(self.milliseconds.to_string()),
        ];
        if let Some(condition) = self.condition {
            args.push(bulk(condition.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n == 1),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "PEXPIRE"
    }
}

/// PEXPIREAT key ms-timestamp \[NX | XX | GT | LT\]
///
/// Sets an expiry on `key` as an absolute Unix timestamp in milliseconds.
/// Returns `true` if the timeout was set.
#[derive(Clone)]
pub struct PExpireAt {
    key: String,
    ms_timestamp: i64,
    condition: Option<ExpireCondition>,
}

impl PExpireAt {
    pub fn new(key: impl Into<String>, ms_timestamp: i64) -> Self {
        Self {
            key: key.into(),
            ms_timestamp,
            condition: None,
        }
    }

    /// Set the condition flag (NX, XX, GT, or LT).
    pub fn condition(mut self, condition: ExpireCondition) -> Self {
        self.condition = Some(condition);
        self
    }
}

impl Command for PExpireAt {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("PEXPIREAT"),
            bulk(self.key.as_str()),
            bulk(self.ms_timestamp.to_string()),
        ];
        if let Some(condition) = self.condition {
            args.push(bulk(condition.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n == 1),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "PEXPIREAT"
    }
}

/// COPY source destination \[REPLACE\]
///
/// Copies the value stored at `source` to `destination`.
/// Returns `true` if the key was copied.
#[derive(Clone)]
pub struct Copy {
    source: String,
    destination: String,
    replace: bool,
}

impl Copy {
    pub fn new(source: impl Into<String>, destination: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            destination: destination.into(),
            replace: false,
        }
    }

    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }
}

impl Command for Copy {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("COPY"),
            bulk(self.source.as_str()),
            bulk(self.destination.as_str()),
        ];
        if self.replace {
            args.push(bulk("REPLACE"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n == 1),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "COPY"
    }
}

/// KEYS pattern
///
/// Returns all keys matching `pattern`.
#[derive(Clone)]
pub struct Keys {
    pattern: String,
}

impl Keys {
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
        }
    }
}

impl Command for Keys {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("KEYS"), bulk(self.pattern.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(Some(data)) => Ok(data),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string",
                        actual: format!("{other:?}"),
                    }),
                })
                .collect(),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "KEYS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// RANDOMKEY
///
/// Returns a random key from the keyspace, or `None` if the database is empty.
#[derive(Clone)]
pub struct RandomKey;

impl RandomKey {
    pub fn new() -> Self {
        Self
    }
}

impl Default for RandomKey {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for RandomKey {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("RANDOMKEY")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(data) => Ok(data),
            Frame::Null => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "RANDOMKEY"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// TOUCH key [key ...]
///
/// Alters the last access time of the specified keys.
/// Returns the number of keys that were touched.
#[derive(Clone)]
pub struct Touch {
    keys: Vec<String>,
}

impl Touch {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for Touch {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TOUCH")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "TOUCH"
    }
}

/// EXPIRETIME key
///
/// Returns the absolute Unix timestamp (in seconds) at which the key will expire.
/// Returns -1 if the key exists but has no expiry, -2 if the key does not exist.
#[derive(Clone)]
pub struct ExpireTime {
    key: String,
}

impl ExpireTime {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for ExpireTime {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("EXPIRETIME"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "EXPIRETIME"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// PEXPIRETIME key
///
/// Returns the absolute Unix timestamp (in milliseconds) at which the key will expire.
/// Returns -1 if the key exists but has no expiry, -2 if the key does not exist.
#[derive(Clone)]
pub struct PExpireTime {
    key: String,
}

impl PExpireTime {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for PExpireTime {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("PEXPIRETIME"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "PEXPIRETIME"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// DUMP key
///
/// Returns a serialized version of the value stored at the specified key.
/// Returns `None` if the key does not exist.
#[derive(Clone)]
pub struct Dump {
    key: String,
}

impl Dump {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Dump {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("DUMP"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(data) => Ok(data),
            Frame::Null => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "DUMP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// RESTORE key ttl serialized-value \[REPLACE\] \[ABSTTL\] \[IDLETIME seconds\] \[FREQ frequency\]
///
/// Deserializes a previously-dumped value and associates it with a key.
/// The `ttl_ms` argument sets the time-to-live in milliseconds (0 for no expiry).
#[derive(Clone)]
pub struct Restore {
    key: String,
    ttl_ms: u64,
    serialized_value: Bytes,
    replace: bool,
    absttl: bool,
    idletime: Option<u64>,
    freq: Option<u64>,
}

impl Restore {
    pub fn new(key: impl Into<String>, ttl_ms: u64, serialized_value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            ttl_ms,
            serialized_value: serialized_value.into(),
            replace: false,
            absttl: false,
            idletime: None,
            freq: None,
        }
    }

    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }

    pub fn absttl(mut self) -> Self {
        self.absttl = true;
        self
    }

    pub fn idletime(mut self, seconds: u64) -> Self {
        self.idletime = Some(seconds);
        self
    }

    pub fn freq(mut self, frequency: u64) -> Self {
        self.freq = Some(frequency);
        self
    }
}

impl Command for Restore {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("RESTORE"),
            bulk(self.key.as_str()),
            bulk(self.ttl_ms.to_string()),
            bulk(&self.serialized_value),
        ];
        if self.replace {
            args.push(bulk("REPLACE"));
        }
        if self.absttl {
            args.push(bulk("ABSTTL"));
        }
        if let Some(idle) = self.idletime {
            args.push(bulk("IDLETIME"));
            args.push(bulk(idle.to_string()));
        }
        if let Some(f) = self.freq {
            args.push(bulk("FREQ"));
            args.push(bulk(f.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "RESTORE"
    }
}

/// Authentication used by [`Migrate`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum MigrateAuth {
    Password(Bytes),
    UsernamePassword(Bytes, Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MigrateKeySelection {
    Single(Bytes),
    Multiple(Vec<Bytes>),
}

/// Result returned by [`Migrate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrateResult {
    /// The selected keys were transferred successfully.
    Ok,
    /// None of the selected keys existed on the source server.
    NoKey,
}

/// MIGRATE host port key destination-db timeout \[COPY\] \[REPLACE\]
/// \[AUTH password | AUTH2 username password\] \[KEYS key \[key ...\]\]
///
/// Atomically transfers one or more keys to another Redis server. Use
/// [`keys`](Self::keys) for the multi-key form; it emits the required empty
/// key argument followed by `KEYS` and the selected keys.
#[derive(Debug, Clone)]
pub struct Migrate {
    host: Bytes,
    port: u16,
    key_selection: MigrateKeySelection,
    destination_db: u64,
    timeout_ms: u64,
    copy: bool,
    replace: bool,
    auth: Option<MigrateAuth>,
}

impl Migrate {
    /// Construct the single-key form of `MIGRATE`.
    ///
    /// Redis reserves the positional empty key as the marker for its multi-key
    /// syntax. An actual empty key is therefore encoded as `KEYS ""` so it
    /// retains its normal Redis key semantics.
    pub fn new(
        host: impl AsRef<[u8]>,
        port: u16,
        key: impl AsRef<[u8]>,
        destination_db: u64,
        timeout_ms: u64,
    ) -> Self {
        let key = Bytes::copy_from_slice(key.as_ref());
        let key_selection = if key.is_empty() {
            MigrateKeySelection::Multiple(vec![key])
        } else {
            MigrateKeySelection::Single(key)
        };
        Self {
            host: Bytes::copy_from_slice(host.as_ref()),
            port,
            key_selection,
            destination_db,
            timeout_ms,
            copy: false,
            replace: false,
            auth: None,
        }
    }

    /// Construct the multi-key form of `MIGRATE` with one required key.
    ///
    /// Append further keys with [`key`](Self::key).
    pub fn keys(
        host: impl AsRef<[u8]>,
        port: u16,
        first_key: impl AsRef<[u8]>,
        destination_db: u64,
        timeout_ms: u64,
    ) -> Self {
        Self {
            host: Bytes::copy_from_slice(host.as_ref()),
            port,
            key_selection: MigrateKeySelection::Multiple(vec![Bytes::copy_from_slice(
                first_key.as_ref(),
            )]),
            destination_db,
            timeout_ms,
            copy: false,
            replace: false,
            auth: None,
        }
    }

    /// Append another key to the multi-key `KEYS` form.
    ///
    /// When called on a single-key request, the builder transparently switches
    /// to the equivalent `KEYS` form and retains the original key first.
    pub fn key(mut self, key: impl AsRef<[u8]>) -> Self {
        let key = Bytes::copy_from_slice(key.as_ref());
        self.key_selection = match self.key_selection {
            MigrateKeySelection::Single(first) => MigrateKeySelection::Multiple(vec![first, key]),
            MigrateKeySelection::Multiple(mut keys) => {
                keys.push(key);
                MigrateKeySelection::Multiple(keys)
            }
        };
        self
    }

    /// Leave the source keys in place after transferring them.
    pub fn copy(mut self) -> Self {
        self.copy = true;
        self
    }

    /// Replace destination keys that already exist.
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }

    /// Authenticate to the destination with a password.
    pub fn auth(mut self, password: impl AsRef<[u8]>) -> Self {
        self.auth = Some(MigrateAuth::Password(Bytes::copy_from_slice(
            password.as_ref(),
        )));
        self
    }

    /// Authenticate to the destination with a username and password.
    pub fn auth2(mut self, username: impl AsRef<[u8]>, password: impl AsRef<[u8]>) -> Self {
        self.auth = Some(MigrateAuth::UsernamePassword(
            Bytes::copy_from_slice(username.as_ref()),
            Bytes::copy_from_slice(password.as_ref()),
        ));
        self
    }
}

impl Command for Migrate {
    type Response = MigrateResult;

    fn to_frame(&self) -> Frame {
        let positional_key = match &self.key_selection {
            MigrateKeySelection::Single(key) => bulk(key),
            MigrateKeySelection::Multiple(_) => bulk(""),
        };
        let mut args = vec![
            bulk("MIGRATE"),
            bulk(&self.host),
            bulk(self.port.to_string()),
            positional_key,
            bulk(self.destination_db.to_string()),
            bulk(self.timeout_ms.to_string()),
        ];
        if self.copy {
            args.push(bulk("COPY"));
        }
        if self.replace {
            args.push(bulk("REPLACE"));
        }
        match &self.auth {
            Some(MigrateAuth::Password(password)) => {
                args.push(bulk("AUTH"));
                args.push(bulk(password));
            }
            Some(MigrateAuth::UsernamePassword(username, password)) => {
                args.push(bulk("AUTH2"));
                args.push(bulk(username));
                args.push(bulk(password));
            }
            None => {}
        }
        if let MigrateKeySelection::Multiple(keys) = &self.key_selection {
            args.push(bulk("KEYS"));
            args.extend(keys.iter().map(bulk));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(value) if value.eq_ignore_ascii_case(b"OK") => {
                Ok(MigrateResult::Ok)
            }
            Frame::SimpleString(value) if value.eq_ignore_ascii_case(b"NOKEY") => {
                Ok(MigrateResult::NoKey)
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK or NOKEY",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "MIGRATE"
    }
}

/// Sort order for SORT and SORT_RO commands.
#[derive(Clone)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// SORT key \[BY pattern\] \[GET pattern ...\] \[LIMIT offset count\] \[ASC|DESC\] \[ALPHA\] \[STORE destination\]
///
/// Sorts the elements in a list, set, or sorted set. When STORE is used, the
/// response is an integer (number of elements stored); otherwise it is an array
/// of bulk strings. The response type is `Frame` to accommodate both cases.
#[derive(Clone)]
pub struct Sort {
    key: String,
    by: Option<String>,
    get: Vec<String>,
    limit: Option<(i64, i64)>,
    order: Option<SortOrder>,
    alpha: bool,
    store: Option<String>,
}

impl Sort {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            by: None,
            get: Vec::new(),
            limit: None,
            order: None,
            alpha: false,
            store: None,
        }
    }

    pub fn by(mut self, pattern: impl Into<String>) -> Self {
        self.by = Some(pattern.into());
        self
    }

    pub fn get(mut self, pattern: impl Into<String>) -> Self {
        self.get.push(pattern.into());
        self
    }

    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.limit = Some((offset, count));
        self
    }

    pub fn order(mut self, order: SortOrder) -> Self {
        self.order = Some(order);
        self
    }

    pub fn alpha(mut self) -> Self {
        self.alpha = true;
        self
    }

    pub fn store(mut self, destination: impl Into<String>) -> Self {
        self.store = Some(destination.into());
        self
    }
}

impl Command for Sort {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SORT"), bulk(self.key.as_str())];
        if let Some(ref pattern) = self.by {
            args.push(bulk("BY"));
            args.push(bulk(pattern.as_str()));
        }
        for pattern in &self.get {
            args.push(bulk("GET"));
            args.push(bulk(pattern.as_str()));
        }
        if let Some((offset, count)) = self.limit {
            args.push(bulk("LIMIT"));
            args.push(bulk(offset.to_string()));
            args.push(bulk(count.to_string()));
        }
        if let Some(ref order) = self.order {
            match order {
                SortOrder::Asc => args.push(bulk("ASC")),
                SortOrder::Desc => args.push(bulk("DESC")),
            }
        }
        if self.alpha {
            args.push(bulk("ALPHA"));
        }
        if let Some(ref dest) = self.store {
            args.push(bulk("STORE"));
            args.push(bulk(dest.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "SORT"
    }
}

/// SORT_RO key \[BY pattern\] \[GET pattern ...\] \[LIMIT offset count\] \[ASC|DESC\] \[ALPHA\]
///
/// Read-only variant of SORT. Returns the sorted elements without the STORE
/// option. Each element is returned as an `Option<Bytes>` (nil for missing
/// GET references).
#[derive(Clone)]
pub struct SortRo {
    key: String,
    by: Option<String>,
    get: Vec<String>,
    limit: Option<(i64, i64)>,
    order: Option<SortOrder>,
    alpha: bool,
}

impl SortRo {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            by: None,
            get: Vec::new(),
            limit: None,
            order: None,
            alpha: false,
        }
    }

    pub fn by(mut self, pattern: impl Into<String>) -> Self {
        self.by = Some(pattern.into());
        self
    }

    pub fn get(mut self, pattern: impl Into<String>) -> Self {
        self.get.push(pattern.into());
        self
    }

    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.limit = Some((offset, count));
        self
    }

    pub fn order(mut self, order: SortOrder) -> Self {
        self.order = Some(order);
        self
    }

    pub fn alpha(mut self) -> Self {
        self.alpha = true;
        self
    }
}

impl Command for SortRo {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("SORT_RO"), bulk(self.key.as_str())];
        if let Some(ref pattern) = self.by {
            args.push(bulk("BY"));
            args.push(bulk(pattern.as_str()));
        }
        for pattern in &self.get {
            args.push(bulk("GET"));
            args.push(bulk(pattern.as_str()));
        }
        if let Some((offset, count)) = self.limit {
            args.push(bulk("LIMIT"));
            args.push(bulk(offset.to_string()));
            args.push(bulk(count.to_string()));
        }
        if let Some(ref order) = self.order {
            match order {
                SortOrder::Asc => args.push(bulk("ASC")),
                SortOrder::Desc => args.push(bulk("DESC")),
            }
        }
        if self.alpha {
            args.push(bulk("ALPHA"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(data) => Ok(data),
                    Frame::Null => Ok(None),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string or null",
                        actual: format!("{other:?}"),
                    }),
                })
                .collect(),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SORT_RO"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// OBJECT ENCODING key
///
/// Returns the internal encoding of the Redis object stored at the key.
#[derive(Clone)]
pub struct ObjectEncoding {
    key: String,
}

impl ObjectEncoding {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for ObjectEncoding {
    type Response = String;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("OBJECT"),
            bulk("ENCODING"),
            bulk(self.key.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(s)) => Ok(String::from_utf8_lossy(&s).into_owned()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "OBJECT ENCODING"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// OBJECT FREQ key
///
/// Returns the logarithmic access frequency counter of a key (requires
/// maxmemory-policy to be set to an LFU policy).
#[derive(Clone)]
pub struct ObjectFreq {
    key: String,
}

impl ObjectFreq {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for ObjectFreq {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("OBJECT"), bulk("FREQ"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "OBJECT FREQ"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// OBJECT HELP
///
/// Returns helpful text about the OBJECT subcommands.
#[derive(Clone)]
pub struct ObjectHelp;

impl ObjectHelp {
    pub fn new() -> Self {
        Self
    }
}

impl Default for ObjectHelp {
    fn default() -> Self {
        Self::new()
    }
}

impl Command for ObjectHelp {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("OBJECT"), bulk("HELP")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        crate::help::parse_help_lines(frame)
    }

    fn name(&self) -> &str {
        "OBJECT HELP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// OBJECT IDLETIME key
///
/// Returns the number of seconds since the object stored at the key is idle
/// (not accessed by read or write operations).
#[derive(Clone)]
pub struct ObjectIdleTime {
    key: String,
}

impl ObjectIdleTime {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for ObjectIdleTime {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("OBJECT"),
            bulk("IDLETIME"),
            bulk(self.key.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "OBJECT IDLETIME"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// OBJECT REFCOUNT key
///
/// Returns the number of references of the object stored at the key.
#[derive(Clone)]
pub struct ObjectRefCount {
    key: String,
}

impl ObjectRefCount {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for ObjectRefCount {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("OBJECT"),
            bulk("REFCOUNT"),
            bulk(self.key.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "OBJECT REFCOUNT"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// EXPIREAT key unix-time-seconds \[NX | XX | GT | LT\]
///
/// Sets an expiry on `key` as an absolute Unix timestamp in seconds.
/// Returns `true` if the timeout was set.
#[derive(Clone)]
pub struct ExpireAt {
    key: String,
    timestamp: i64,
    condition: Option<ExpireCondition>,
}

impl ExpireAt {
    pub fn new(key: impl Into<String>, timestamp: i64) -> Self {
        Self {
            key: key.into(),
            timestamp,
            condition: None,
        }
    }

    /// Set the condition flag (NX, XX, GT, or LT).
    pub fn condition(mut self, condition: ExpireCondition) -> Self {
        self.condition = Some(condition);
        self
    }
}

impl Command for ExpireAt {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("EXPIREAT"),
            bulk(self.key.as_str()),
            bulk(self.timestamp.to_string()),
        ];
        if let Some(condition) = self.condition {
            args.push(bulk(condition.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n == 1),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "EXPIREAT"
    }
}

/// PTTL key
///
/// Returns the remaining time to live of a key in milliseconds.
/// Returns -2 if the key does not exist, -1 if no expiry is set.
#[derive(Clone)]
pub struct Pttl {
    key: String,
}

impl Pttl {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Pttl {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("PTTL"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "PTTL"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// RENAMENX key newkey
///
/// Renames `key` to `newkey`, only if `newkey` does not yet exist.
/// Returns `true` if the key was renamed.
#[derive(Clone)]
pub struct RenameNx {
    key: String,
    new_key: String,
}

impl RenameNx {
    pub fn new(key: impl Into<String>, new_key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            new_key: new_key.into(),
        }
    }
}

impl Command for RenameNx {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("RENAMENX"),
            bulk(self.key.as_str()),
            bulk(self.new_key.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n == 1),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "RENAMENX"
    }
}

/// MOVE key db
///
/// Moves `key` from the currently selected database to the specified
/// destination database. Returns `true` if the key was moved.
#[derive(Clone)]
pub struct Move {
    key: String,
    db: u16,
}

impl Move {
    pub fn new(key: impl Into<String>, db: u16) -> Self {
        Self {
            key: key.into(),
            db,
        }
    }
}

impl Command for Move {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("MOVE"),
            bulk(self.key.as_str()),
            bulk(self.db.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n == 1),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "MOVE"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use redis_tower_protocol::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

    // -- Del --

    #[test]
    fn del_single_to_frame() {
        let cmd = Del::new("mykey");
        assert_eq!(cmd.to_frame(), array(vec![bulk("DEL"), bulk("mykey")]));
    }

    #[test]
    fn del_multiple_to_frame() {
        let cmd = Del::keys(vec!["a", "b", "c"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("DEL"), bulk("a"), bulk("b"), bulk("c")])
        );
    }

    #[test]
    fn del_parse_integer() {
        let cmd = Del::new("mykey");
        assert_eq!(cmd.parse_response(Frame::Integer(1)).unwrap(), 1);
    }

    #[test]
    fn del_parse_error_on_string() {
        let cmd = Del::new("mykey");
        assert!(
            cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
                .is_err()
        );
    }

    // -- Exists --

    #[test]
    fn exists_to_frame() {
        let cmd = Exists::new("k");
        assert_eq!(cmd.to_frame(), array(vec![bulk("EXISTS"), bulk("k")]));
    }

    #[test]
    fn exists_multiple_to_frame() {
        let cmd = Exists::keys(vec!["a", "b"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("EXISTS"), bulk("a"), bulk("b")])
        );
    }

    #[test]
    fn exists_parse_integer() {
        let cmd = Exists::new("k");
        assert_eq!(cmd.parse_response(Frame::Integer(2)).unwrap(), 2);
    }

    // -- Expire --

    #[test]
    fn expire_to_frame() {
        let cmd = Expire::new("k", 60);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("EXPIRE"), bulk("k"), bulk("60")])
        );
    }

    #[test]
    fn expire_parse_true() {
        let cmd = Expire::new("k", 60);
        assert!(cmd.parse_response(Frame::Integer(1)).unwrap());
    }

    #[test]
    fn expire_parse_false() {
        let cmd = Expire::new("k", 60);
        assert!(!cmd.parse_response(Frame::Integer(0)).unwrap());
    }

    #[test]
    fn expire_parse_boolean() {
        let cmd = Expire::new("k", 60);
        assert!(cmd.parse_response(Frame::Boolean(true)).unwrap());
    }

    // -- Ttl --

    #[test]
    fn ttl_to_frame() {
        let cmd = Ttl::new("k");
        assert_eq!(cmd.to_frame(), array(vec![bulk("TTL"), bulk("k")]));
    }

    #[test]
    fn ttl_parse_integer() {
        let cmd = Ttl::new("k");
        assert_eq!(cmd.parse_response(Frame::Integer(-2)).unwrap(), -2);
    }

    // -- Type --

    #[test]
    fn type_to_frame() {
        let cmd = Type::new("k");
        assert_eq!(cmd.to_frame(), array(vec![bulk("TYPE"), bulk("k")]));
    }

    #[test]
    fn type_parse_simple_string() {
        let cmd = Type::new("k");
        let frame = Frame::SimpleString(Bytes::from("string"));
        assert_eq!(cmd.parse_response(frame).unwrap(), "string");
    }

    #[test]
    fn type_parse_error_on_integer() {
        let cmd = Type::new("k");
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    // -- Rename --

    #[test]
    fn rename_to_frame() {
        let cmd = Rename::new("old", "new");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("RENAME"), bulk("old"), bulk("new")])
        );
    }

    #[test]
    fn rename_parse_ok() {
        let cmd = Rename::new("old", "new");
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
    }

    // -- Copy --

    #[test]
    fn copy_to_frame() {
        let cmd = Copy::new("src", "dst");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("COPY"), bulk("src"), bulk("dst")])
        );
    }

    #[test]
    fn copy_replace_to_frame() {
        let cmd = Copy::new("src", "dst").replace();
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("COPY"),
                bulk("src"),
                bulk("dst"),
                bulk("REPLACE")
            ])
        );
    }

    #[test]
    fn copy_parse_true() {
        let cmd = Copy::new("src", "dst");
        assert!(cmd.parse_response(Frame::Integer(1)).unwrap());
    }

    // -- Keys --

    #[test]
    fn keys_to_frame() {
        let cmd = Keys::new("user:*");
        assert_eq!(cmd.to_frame(), array(vec![bulk("KEYS"), bulk("user:*")]));
    }

    #[test]
    fn keys_parse_array() {
        let cmd = Keys::new("*");
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("k1"))),
            Frame::BulkString(Some(Bytes::from("k2"))),
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![Bytes::from("k1"), Bytes::from("k2")]);
    }

    // -- Sort --

    #[test]
    fn sort_with_options_to_frame() {
        let cmd = Sort::new("mylist")
            .by("weight_*")
            .limit(0, 10)
            .order(SortOrder::Desc)
            .alpha();
        match cmd.to_frame() {
            Frame::Array(Some(args)) => {
                assert_eq!(args[0], bulk("SORT"));
                assert_eq!(args[1], bulk("mylist"));
                assert_eq!(args[2], bulk("BY"));
                assert_eq!(args[3], bulk("weight_*"));
                assert!(args.contains(&bulk("DESC")));
                assert!(args.contains(&bulk("ALPHA")));
            }
            _ => panic!("expected array"),
        }
    }

    // -- ObjectEncoding --

    #[test]
    fn object_encoding_to_frame() {
        let cmd = ObjectEncoding::new("mykey");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("OBJECT"), bulk("ENCODING"), bulk("mykey")])
        );
    }

    #[test]
    fn object_encoding_parse_response() {
        let cmd = ObjectEncoding::new("mykey");
        let frame = Frame::BulkString(Some(Bytes::from("ziplist")));
        assert_eq!(cmd.parse_response(frame).unwrap(), "ziplist");
    }

    // -- Persist --

    #[test]
    fn persist_to_frame() {
        let cmd = Persist::new("k");
        assert_eq!(cmd.to_frame(), array(vec![bulk("PERSIST"), bulk("k")]));
    }

    // -- RandomKey --

    #[test]
    fn randomkey_to_frame() {
        let cmd = RandomKey::new();
        assert_eq!(cmd.to_frame(), array(vec![bulk("RANDOMKEY")]));
    }

    #[test]
    fn randomkey_parse_null() {
        let cmd = RandomKey::new();
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    // -- Expire with condition --

    #[test]
    fn expire_with_condition_to_frame() {
        let cmd = Expire::new("k", 60).condition(ExpireCondition::Nx);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("EXPIRE"), bulk("k"), bulk("60"), bulk("NX")])
        );
    }

    #[test]
    fn pexpire_with_condition_to_frame() {
        let cmd = PExpire::new("k", 1000).condition(ExpireCondition::Gt);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("PEXPIRE"), bulk("k"), bulk("1000"), bulk("GT")])
        );
    }

    #[test]
    fn pexpireat_with_condition_to_frame() {
        let cmd = PExpireAt::new("k", 99999).condition(ExpireCondition::Lt);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("PEXPIREAT"),
                bulk("k"),
                bulk("99999"),
                bulk("LT")
            ])
        );
    }

    // -- ExpireAt --

    #[test]
    fn expireat_to_frame() {
        let cmd = ExpireAt::new("k", 1700000000);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("EXPIREAT"), bulk("k"), bulk("1700000000")])
        );
    }

    #[test]
    fn expireat_with_condition_to_frame() {
        let cmd = ExpireAt::new("k", 1700000000).condition(ExpireCondition::Xx);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("EXPIREAT"),
                bulk("k"),
                bulk("1700000000"),
                bulk("XX")
            ])
        );
    }

    #[test]
    fn expireat_parse_true() {
        let cmd = ExpireAt::new("k", 1700000000);
        assert!(cmd.parse_response(Frame::Integer(1)).unwrap());
    }

    // -- Pttl --

    #[test]
    fn pttl_to_frame() {
        let cmd = Pttl::new("k");
        assert_eq!(cmd.to_frame(), array(vec![bulk("PTTL"), bulk("k")]));
    }

    #[test]
    fn pttl_parse_integer() {
        let cmd = Pttl::new("k");
        assert_eq!(cmd.parse_response(Frame::Integer(1500)).unwrap(), 1500);
    }

    // -- RenameNx --

    #[test]
    fn renamenx_to_frame() {
        let cmd = RenameNx::new("old", "new");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("RENAMENX"), bulk("old"), bulk("new")])
        );
    }

    #[test]
    fn renamenx_parse_true() {
        let cmd = RenameNx::new("old", "new");
        assert!(cmd.parse_response(Frame::Integer(1)).unwrap());
    }

    #[test]
    fn renamenx_parse_false() {
        let cmd = RenameNx::new("old", "new");
        assert!(!cmd.parse_response(Frame::Integer(0)).unwrap());
    }

    // -- Move --

    #[test]
    fn move_to_frame() {
        let cmd = Move::new("k", 1);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("MOVE"), bulk("k"), bulk("1")])
        );
    }

    #[test]
    fn move_parse_true() {
        let cmd = Move::new("k", 1);
        assert!(cmd.parse_response(Frame::Integer(1)).unwrap());
    }

    // -- Migrate --

    #[test]
    fn migrate_single_key_with_options_is_binary_safe() {
        let cmd = Migrate::new(b"127.0.0.1", 6380, b"source\0key", 2, 5_000)
            .copy()
            .replace()
            .auth(b"secret\0password");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("MIGRATE"),
                bulk("127.0.0.1"),
                bulk("6380"),
                bulk(b"source\0key"),
                bulk("2"),
                bulk("5000"),
                bulk("COPY"),
                bulk("REPLACE"),
                bulk("AUTH"),
                bulk(b"secret\0password"),
            ])
        );
        assert_eq!(cmd.name(), "MIGRATE");
        assert!(!cmd.idempotent());
    }

    #[test]
    fn migrate_multiple_keys_and_auth2() {
        let cmd = Migrate::keys("redis.internal", 6379, b"key\0one", 0, 750)
            .key(b"key\0two")
            .auth2(b"user\0name", b"pass\0word");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("MIGRATE"),
                bulk("redis.internal"),
                bulk("6379"),
                bulk(""),
                bulk("0"),
                bulk("750"),
                bulk("AUTH2"),
                bulk(b"user\0name"),
                bulk(b"pass\0word"),
                bulk("KEYS"),
                bulk(b"key\0one"),
                bulk(b"key\0two"),
            ])
        );
    }

    #[test]
    fn migrate_empty_key_uses_the_explicit_keys_form() {
        let cmd = Migrate::new("127.0.0.1", 6380, b"", 0, 5_000);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("MIGRATE"),
                bulk("127.0.0.1"),
                bulk("6380"),
                bulk(""),
                bulk("0"),
                bulk("5000"),
                bulk("KEYS"),
                bulk(""),
            ])
        );
    }

    #[test]
    fn migrate_parses_ok_and_nokey() {
        let cmd = Migrate::new("localhost", 6379, "key", 0, 1_000);
        assert_eq!(
            cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
                .unwrap(),
            MigrateResult::Ok
        );
        assert_eq!(
            cmd.parse_response(Frame::SimpleString(Bytes::from("NOKEY")))
                .unwrap(),
            MigrateResult::NoKey
        );
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }
}
