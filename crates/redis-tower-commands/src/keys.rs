use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// DEL key [key ...]
///
/// Removes the specified keys. Returns the number of keys removed.
///
/// See: <https://redis.io/commands/del>
pub struct Del {
    keys: Vec<String>,
}

impl Del {
    /// Creates a new [`Del`] command.
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
///
/// See: <https://redis.io/commands/exists>
pub struct Exists {
    keys: Vec<String>,
}

impl Exists {
    /// Creates a new [`Exists`] command.
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
}

/// EXPIRE key seconds
///
/// Sets a timeout on `key`. Returns `true` if the timeout was set.
///
/// See: <https://redis.io/commands/expire>
pub struct Expire {
    key: String,
    seconds: u64,
}

impl Expire {
    /// Creates a new [`Expire`] command.
    pub fn new(key: impl Into<String>, seconds: u64) -> Self {
        Self {
            key: key.into(),
            seconds,
        }
    }
}

impl Command for Expire {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("EXPIRE"),
            bulk(self.key.as_str()),
            bulk(self.seconds.to_string()),
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
        "EXPIRE"
    }
}

/// TTL key
///
/// Returns the remaining time to live of a key in seconds.
/// Returns -2 if the key does not exist, -1 if no expiry is set.
///
/// See: <https://redis.io/commands/ttl>
pub struct Ttl {
    key: String,
}

impl Ttl {
    /// Creates a new [`Ttl`] command.
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
}

/// RENAME key newkey
///
/// Renames `key` to `newkey`. Errors if `key` does not exist.
///
/// See: <https://redis.io/commands/rename>
pub struct Rename {
    key: String,
    new_key: String,
}

impl Rename {
    /// Creates a new [`Rename`] command.
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
///
/// See: <https://redis.io/commands/type>
pub struct Type {
    key: String,
}

impl Type {
    /// Creates a new [`Type`] command.
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
}

/// UNLINK key [key ...]
///
/// Removes the specified keys without blocking the server.
/// Returns the number of keys removed.
///
/// See: <https://redis.io/commands/unlink>
pub struct Unlink {
    keys: Vec<String>,
}

impl Unlink {
    /// Creates a new [`Unlink`] command.
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
///
/// See: <https://redis.io/commands/persist>
pub struct Persist {
    key: String,
}

impl Persist {
    /// Creates a new [`Persist`] command.
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
}

/// PEXPIRE key milliseconds
///
/// Sets a timeout on `key` in milliseconds. Returns `true` if the timeout was set.
///
/// See: <https://redis.io/commands/pexpire>
pub struct PExpire {
    key: String,
    milliseconds: u64,
}

impl PExpire {
    /// Creates a new [`PExpire`] command.
    pub fn new(key: impl Into<String>, milliseconds: u64) -> Self {
        Self {
            key: key.into(),
            milliseconds,
        }
    }
}

impl Command for PExpire {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("PEXPIRE"),
            bulk(self.key.as_str()),
            bulk(self.milliseconds.to_string()),
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
        "PEXPIRE"
    }
}

/// PEXPIREAT key ms-timestamp
///
/// Sets an expiry on `key` as an absolute Unix timestamp in milliseconds.
/// Returns `true` if the timeout was set.
///
/// See: <https://redis.io/commands/pexpireat>
pub struct PExpireAt {
    key: String,
    ms_timestamp: i64,
}

impl PExpireAt {
    /// Creates a new [`PExpireAt`] command.
    pub fn new(key: impl Into<String>, ms_timestamp: i64) -> Self {
        Self {
            key: key.into(),
            ms_timestamp,
        }
    }
}

impl Command for PExpireAt {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("PEXPIREAT"),
            bulk(self.key.as_str()),
            bulk(self.ms_timestamp.to_string()),
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
        "PEXPIREAT"
    }
}

/// COPY source destination \[REPLACE\]
///
/// Copies the value stored at `source` to `destination`.
/// Returns `true` if the key was copied.
///
/// See: <https://redis.io/commands/copy>
pub struct Copy {
    source: String,
    destination: String,
    replace: bool,
}

impl Copy {
    /// Creates a new [`Copy`] command.
    pub fn new(source: impl Into<String>, destination: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            destination: destination.into(),
            replace: false,
        }
    }

    #[must_use]
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
///
/// See: <https://redis.io/commands/keys>
pub struct Keys {
    pattern: String,
}

impl Keys {
    /// Creates a new [`Keys`] command.
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
}

/// RANDOMKEY
///
/// Returns a random key from the keyspace, or `None` if the database is empty.
///
/// See: <https://redis.io/commands/randomkey>
pub struct RandomKey;

impl RandomKey {
    /// Creates a new [`RandomKey`] command.
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
}

/// TOUCH key [key ...]
///
/// Alters the last access time of the specified keys.
/// Returns the number of keys that were touched.
///
/// See: <https://redis.io/commands/touch>
pub struct Touch {
    keys: Vec<String>,
}

impl Touch {
    /// Creates a new [`Touch`] command.
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
///
/// See: <https://redis.io/commands/expiretime>
pub struct ExpireTime {
    key: String,
}

impl ExpireTime {
    /// Creates a new [`ExpireTime`] command.
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
}

/// PEXPIRETIME key
///
/// Returns the absolute Unix timestamp (in milliseconds) at which the key will expire.
/// Returns -1 if the key exists but has no expiry, -2 if the key does not exist.
///
/// See: <https://redis.io/commands/pexpiretime>
pub struct PExpireTime {
    key: String,
}

impl PExpireTime {
    /// Creates a new [`PExpireTime`] command.
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
}

/// DUMP key
///
/// Returns a serialized version of the value stored at the specified key.
/// Returns `None` if the key does not exist.
///
/// See: <https://redis.io/commands/dump>
pub struct Dump {
    key: String,
}

impl Dump {
    /// Creates a new [`Dump`] command.
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
}

/// RESTORE key ttl serialized-value \[REPLACE\] \[ABSTTL\] \[IDLETIME seconds\] \[FREQ frequency\]
///
/// Deserializes a previously-dumped value and associates it with a key.
/// The `ttl_ms` argument sets the time-to-live in milliseconds (0 for no expiry).
///
/// See: <https://redis.io/commands/restore>
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
    /// Creates a new [`Restore`] command. `ttl_ms` is the TTL in milliseconds (0 = no expiry).
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

    #[must_use]
    pub fn replace(mut self) -> Self {
        self.replace = true;
        self
    }

    #[must_use]
    pub fn absttl(mut self) -> Self {
        self.absttl = true;
        self
    }

    #[must_use]
    pub fn idletime(mut self, seconds: u64) -> Self {
        self.idletime = Some(seconds);
        self
    }

    #[must_use]
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

/// Sort order for SORT and SORT_RO commands.
pub enum SortOrder {
    Asc,
    Desc,
}

/// SORT key \[BY pattern\] \[GET pattern ...\] \[LIMIT offset count\] \[ASC|DESC\] \[ALPHA\] \[STORE destination\]
///
/// Sorts the elements in a list, set, or sorted set. When STORE is used, the
/// response is an integer (number of elements stored); otherwise it is an array
/// of bulk strings. The response type is `Frame` to accommodate both cases.
///
/// See: <https://redis.io/commands/sort>
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
    /// Creates a new [`Sort`] command.
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

    #[must_use]
    pub fn by(mut self, pattern: impl Into<String>) -> Self {
        self.by = Some(pattern.into());
        self
    }

    #[must_use]
    pub fn get(mut self, pattern: impl Into<String>) -> Self {
        self.get.push(pattern.into());
        self
    }

    #[must_use]
    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.limit = Some((offset, count));
        self
    }

    #[must_use]
    pub fn order(mut self, order: SortOrder) -> Self {
        self.order = Some(order);
        self
    }

    #[must_use]
    pub fn alpha(mut self) -> Self {
        self.alpha = true;
        self
    }

    #[must_use]
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
///
/// See: <https://redis.io/commands/sort_ro>
pub struct SortRo {
    key: String,
    by: Option<String>,
    get: Vec<String>,
    limit: Option<(i64, i64)>,
    order: Option<SortOrder>,
    alpha: bool,
}

impl SortRo {
    /// Creates a new [`SortRo`] command.
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

    #[must_use]
    pub fn by(mut self, pattern: impl Into<String>) -> Self {
        self.by = Some(pattern.into());
        self
    }

    #[must_use]
    pub fn get(mut self, pattern: impl Into<String>) -> Self {
        self.get.push(pattern.into());
        self
    }

    #[must_use]
    pub fn limit(mut self, offset: i64, count: i64) -> Self {
        self.limit = Some((offset, count));
        self
    }

    #[must_use]
    pub fn order(mut self, order: SortOrder) -> Self {
        self.order = Some(order);
        self
    }

    #[must_use]
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
}

/// OBJECT ENCODING key
///
/// Returns the internal encoding of the Redis object stored at the key.
///
/// See: <https://redis.io/commands/object-encoding>
pub struct ObjectEncoding {
    key: String,
}

impl ObjectEncoding {
    /// Creates a new [`ObjectEncoding`] command.
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
}

/// OBJECT FREQ key
///
/// Returns the logarithmic access frequency counter of a key (requires
/// maxmemory-policy to be set to an LFU policy).
///
/// See: <https://redis.io/commands/object-freq>
pub struct ObjectFreq {
    key: String,
}

impl ObjectFreq {
    /// Creates a new [`ObjectFreq`] command.
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
}

/// OBJECT HELP
///
/// Returns helpful text about the OBJECT subcommands.
///
/// See: <https://redis.io/commands/object-help>
pub struct ObjectHelp;

impl ObjectHelp {
    /// Creates a new [`ObjectHelp`] command.
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
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(Some(data)) => Ok(data),
                    // Redis may return OBJECT HELP lines as SimpleString frames.
                    Frame::SimpleString(data) => Ok(data),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string or simple string",
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
        "OBJECT HELP"
    }
}

/// OBJECT IDLETIME key
///
/// Returns the number of seconds since the object stored at the key is idle
/// (not accessed by read or write operations).
///
/// See: <https://redis.io/commands/object-idletime>
pub struct ObjectIdleTime {
    key: String,
}

impl ObjectIdleTime {
    /// Creates a new [`ObjectIdleTime`] command.
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
}

/// OBJECT REFCOUNT key
///
/// Returns the number of references of the object stored at the key.
///
/// See: <https://redis.io/commands/object-refcount>
pub struct ObjectRefCount {
    key: String,
}

impl ObjectRefCount {
    /// Creates a new [`ObjectRefCount`] command.
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
}
