use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// DEL key [key ...]
///
/// Removes the specified keys. Returns the number of keys removed.
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
}

/// EXPIRE key seconds
///
/// Sets a timeout on `key`. Returns `true` if the timeout was set.
pub struct Expire {
    key: String,
    seconds: u64,
}

impl Expire {
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
}

/// RENAME key newkey
///
/// Renames `key` to `newkey`. Errors if `key` does not exist.
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
}

/// UNLINK key [key ...]
///
/// Removes the specified keys without blocking the server.
/// Returns the number of keys removed.
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
}

/// PEXPIRE key milliseconds
///
/// Sets a timeout on `key` in milliseconds. Returns `true` if the timeout was set.
pub struct PExpire {
    key: String,
    milliseconds: u64,
}

impl PExpire {
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
pub struct PExpireAt {
    key: String,
    ms_timestamp: i64,
}

impl PExpireAt {
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
}

/// RANDOMKEY
///
/// Returns a random key from the keyspace, or `None` if the database is empty.
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
}

/// TOUCH key [key ...]
///
/// Alters the last access time of the specified keys.
/// Returns the number of keys that were touched.
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
}

/// PEXPIRETIME key
///
/// Returns the absolute Unix timestamp (in milliseconds) at which the key will expire.
/// Returns -1 if the key exists but has no expiry, -2 if the key does not exist.
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
}

/// DUMP key
///
/// Returns a serialized version of the value stored at the specified key.
/// Returns `None` if the key does not exist.
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
}

/// RESTORE key ttl serialized-value \[REPLACE\] \[ABSTTL\] \[IDLETIME seconds\] \[FREQ frequency\]
///
/// Deserializes a previously-dumped value and associates it with a key.
/// The `ttl_ms` argument sets the time-to-live in milliseconds (0 for no expiry).
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
}

/// OBJECT ENCODING key
///
/// Returns the internal encoding of the Redis object stored at the key.
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
}

/// OBJECT FREQ key
///
/// Returns the logarithmic access frequency counter of a key (requires
/// maxmemory-policy to be set to an LFU policy).
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
        array(vec![
            bulk("OBJECT"),
            bulk("FREQ"),
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
        "OBJECT FREQ"
    }
}

/// OBJECT HELP
///
/// Returns helpful text about the OBJECT subcommands.
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
        "OBJECT HELP"
    }
}

/// OBJECT IDLETIME key
///
/// Returns the number of seconds since the object stored at the key is idle
/// (not accessed by read or write operations).
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
}

/// OBJECT REFCOUNT key
///
/// Returns the number of references of the object stored at the key.
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
}
