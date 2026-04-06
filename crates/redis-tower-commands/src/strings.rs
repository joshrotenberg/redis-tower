use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// GET key
///
/// Returns the value of `key`, or `None` if the key does not exist.
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Get {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("GET"), bulk(self.key.as_str())])
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
        "GET"
    }
}

/// SET key value \[EX seconds\] \[PX milliseconds\] \[NX|XX\] \[GET\]
///
/// Sets `key` to hold `value`. Returns `Ok` on success, or the old value
/// if `GET` is specified.
pub struct Set {
    key: String,
    value: String,
    ex: Option<u64>,
    px: Option<u64>,
    condition: Option<SetCondition>,
    get: bool,
}

/// Condition for SET (NX or XX).
pub enum SetCondition {
    /// Only set if the key does not exist.
    Nx,
    /// Only set if the key already exists.
    Xx,
}

impl Set {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            ex: None,
            px: None,
            condition: None,
            get: false,
        }
    }

    /// Set expiration in seconds.
    pub fn ex(mut self, seconds: u64) -> Self {
        self.ex = Some(seconds);
        self.px = None;
        self
    }

    /// Set expiration in milliseconds.
    pub fn px(mut self, milliseconds: u64) -> Self {
        self.px = Some(milliseconds);
        self.ex = None;
        self
    }

    /// Only set if the key does not exist.
    pub fn nx(mut self) -> Self {
        self.condition = Some(SetCondition::Nx);
        self
    }

    /// Only set if the key already exists.
    pub fn xx(mut self) -> Self {
        self.condition = Some(SetCondition::Xx);
        self
    }

    /// Return the old value stored at `key`.
    pub fn get(mut self) -> Self {
        self.get = true;
        self
    }
}

impl Command for Set {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("SET"),
            bulk(self.key.as_str()),
            bulk(self.value.as_str()),
        ];

        if let Some(ex) = self.ex {
            args.push(bulk("EX"));
            args.push(bulk(ex.to_string()));
        }
        if let Some(px) = self.px {
            args.push(bulk("PX"));
            args.push(bulk(px.to_string()));
        }
        match &self.condition {
            Some(SetCondition::Nx) => args.push(bulk("NX")),
            Some(SetCondition::Xx) => args.push(bulk("XX")),
            None => {}
        }
        if self.get {
            args.push(bulk("GET"));
        }

        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(None),
            Frame::BulkString(data) => Ok(data),
            Frame::Null => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK, bulk string, or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SET"
    }
}

/// INCR key
///
/// Increments the integer value of `key` by one.
pub struct Incr {
    key: String,
}

impl Incr {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Incr {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("INCR"), bulk(self.key.as_str())])
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
        "INCR"
    }
}

/// MGET key [key ...]
///
/// Returns the values of all specified keys.
pub struct MGet {
    keys: Vec<String>,
}

impl MGet {
    pub fn new(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for MGet {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("MGET")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
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
        "MGET"
    }
}

/// APPEND key value
///
/// Appends `value` to the end of the string at `key`. Returns the length
/// of the string after the append.
pub struct Append {
    key: String,
    value: String,
}

impl Append {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl Command for Append {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("APPEND"),
            bulk(self.key.as_str()),
            bulk(self.value.as_str()),
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
        "APPEND"
    }
}

/// MSET key value \[key value ...\]
///
/// Sets multiple keys to their respective values atomically.
pub struct MSet {
    pairs: Vec<(String, String)>,
}

impl MSet {
    pub fn new(pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self {
        Self {
            pairs: pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }
}

impl Command for MSet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("MSET")];
        for (k, v) in &self.pairs {
            args.push(bulk(k.as_str()));
            args.push(bulk(v.as_str()));
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
        "MSET"
    }
}

/// GETEX key \[EX seconds | PX milliseconds | EXAT timestamp | PXAT timestamp | PERSIST\]
///
/// Gets the value of `key` and optionally sets its expiration.
/// Returns `None` if the key does not exist.
pub struct GetEx {
    key: String,
    ex: Option<u64>,
    px: Option<u64>,
    exat: Option<u64>,
    pxat: Option<u64>,
    persist: bool,
}

impl GetEx {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            ex: None,
            px: None,
            exat: None,
            pxat: None,
            persist: false,
        }
    }

    /// Set expiration in seconds.
    pub fn ex(mut self, seconds: u64) -> Self {
        self.ex = Some(seconds);
        self.px = None;
        self.exat = None;
        self.pxat = None;
        self.persist = false;
        self
    }

    /// Set expiration in milliseconds.
    pub fn px(mut self, milliseconds: u64) -> Self {
        self.px = Some(milliseconds);
        self.ex = None;
        self.exat = None;
        self.pxat = None;
        self.persist = false;
        self
    }

    /// Set expiration as a Unix timestamp in seconds.
    pub fn exat(mut self, timestamp: u64) -> Self {
        self.exat = Some(timestamp);
        self.ex = None;
        self.px = None;
        self.pxat = None;
        self.persist = false;
        self
    }

    /// Set expiration as a Unix timestamp in milliseconds.
    pub fn pxat(mut self, timestamp: u64) -> Self {
        self.pxat = Some(timestamp);
        self.ex = None;
        self.px = None;
        self.exat = None;
        self.persist = false;
        self
    }

    /// Remove the existing expiration on the key.
    pub fn persist(mut self) -> Self {
        self.persist = true;
        self.ex = None;
        self.px = None;
        self.exat = None;
        self.pxat = None;
        self
    }
}

impl Command for GetEx {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("GETEX"), bulk(self.key.as_str())];

        if let Some(ex) = self.ex {
            args.push(bulk("EX"));
            args.push(bulk(ex.to_string()));
        }
        if let Some(px) = self.px {
            args.push(bulk("PX"));
            args.push(bulk(px.to_string()));
        }
        if let Some(exat) = self.exat {
            args.push(bulk("EXAT"));
            args.push(bulk(exat.to_string()));
        }
        if let Some(pxat) = self.pxat {
            args.push(bulk("PXAT"));
            args.push(bulk(pxat.to_string()));
        }
        if self.persist {
            args.push(bulk("PERSIST"));
        }

        array(args)
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
        "GETEX"
    }
}

/// GETDEL key
///
/// Gets the value of `key` and deletes it. Returns `None` if the key does
/// not exist.
pub struct GetDel {
    key: String,
}

impl GetDel {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for GetDel {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("GETDEL"), bulk(self.key.as_str())])
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
        "GETDEL"
    }
}

/// SETEX key seconds value
///
/// Sets `key` to hold `value` with an expiration of `seconds`.
pub struct SetEx {
    key: String,
    seconds: u64,
    value: String,
}

impl SetEx {
    pub fn new(key: impl Into<String>, seconds: u64, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            seconds,
            value: value.into(),
        }
    }
}

impl Command for SetEx {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("SETEX"),
            bulk(self.key.as_str()),
            bulk(self.seconds.to_string()),
            bulk(self.value.as_str()),
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
        "SETEX"
    }
}

/// PSETEX key milliseconds value
///
/// Sets `key` to hold `value` with an expiration of `milliseconds`.
pub struct PSetEx {
    key: String,
    milliseconds: u64,
    value: String,
}

impl PSetEx {
    pub fn new(key: impl Into<String>, milliseconds: u64, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            milliseconds,
            value: value.into(),
        }
    }
}

impl Command for PSetEx {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("PSETEX"),
            bulk(self.key.as_str()),
            bulk(self.milliseconds.to_string()),
            bulk(self.value.as_str()),
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
        "PSETEX"
    }
}

/// SETNX key value
///
/// Sets `key` to hold `value` if `key` does not exist. Returns `true` if
/// the key was set, `false` if the key already existed.
pub struct SetNx {
    key: String,
    value: String,
}

impl SetNx {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl Command for SetNx {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("SETNX"),
            bulk(self.key.as_str()),
            bulk(self.value.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),
            Frame::Integer(0) => Ok(false),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer 0 or 1",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "SETNX"
    }
}

/// INCRBYFLOAT key increment
///
/// Increments the floating-point value of `key` by `increment`. Returns the
/// new value.
pub struct IncrByFloat {
    key: String,
    increment: f64,
}

impl IncrByFloat {
    pub fn new(key: impl Into<String>, increment: f64) -> Self {
        Self {
            key: key.into(),
            increment,
        }
    }
}

impl Command for IncrByFloat {
    type Response = f64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("INCRBYFLOAT"),
            bulk(self.key.as_str()),
            bulk(self.increment.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                let s = std::str::from_utf8(&data).map_err(|_| RedisError::UnexpectedResponse {
                    expected: "valid UTF-8 bulk string",
                    actual: format!("{data:?}"),
                })?;
                s.parse::<f64>()
                    .map_err(|_| RedisError::UnexpectedResponse {
                        expected: "float string",
                        actual: s.to_string(),
                    })
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "INCRBYFLOAT"
    }
}

/// DECR key
///
/// Decrements the integer value of `key` by one.
pub struct Decr {
    key: String,
}

impl Decr {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Decr {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("DECR"), bulk(self.key.as_str())])
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
        "DECR"
    }
}

/// DECRBY key decrement
///
/// Decrements the integer value of `key` by `decrement`.
pub struct DecrBy {
    key: String,
    decrement: i64,
}

impl DecrBy {
    pub fn new(key: impl Into<String>, decrement: i64) -> Self {
        Self {
            key: key.into(),
            decrement,
        }
    }
}

impl Command for DecrBy {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("DECRBY"),
            bulk(self.key.as_str()),
            bulk(self.decrement.to_string()),
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
        "DECRBY"
    }
}

/// GETRANGE key start end
///
/// Returns the substring of the string value stored at `key`, determined
/// by the offsets `start` and `end` (both inclusive).
pub struct GetRange {
    key: String,
    start: i64,
    end: i64,
}

impl GetRange {
    pub fn new(key: impl Into<String>, start: i64, end: i64) -> Self {
        Self {
            key: key.into(),
            start,
            end,
        }
    }
}

impl Command for GetRange {
    type Response = Bytes;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("GETRANGE"),
            bulk(self.key.as_str()),
            bulk(self.start.to_string()),
            bulk(self.end.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(data),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "GETRANGE"
    }
}

/// SETRANGE key offset value
///
/// Overwrites part of the string stored at `key`, starting at the
/// specified byte `offset`. Returns the length of the string after the
/// modification.
pub struct SetRange {
    key: String,
    offset: i64,
    value: String,
}

impl SetRange {
    pub fn new(key: impl Into<String>, offset: i64, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            offset,
            value: value.into(),
        }
    }
}

impl Command for SetRange {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("SETRANGE"),
            bulk(self.key.as_str()),
            bulk(self.offset.to_string()),
            bulk(self.value.as_str()),
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
        "SETRANGE"
    }
}

/// STRLEN key
///
/// Returns the length of the string value stored at `key`, or 0 if the
/// key does not exist.
pub struct StrLen {
    key: String,
}

impl StrLen {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for StrLen {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("STRLEN"), bulk(self.key.as_str())])
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
        "STRLEN"
    }
}

/// INCRBY key increment
///
/// Increments the integer value of `key` by `increment`. Returns the new
/// value after the increment.
pub struct IncrBy {
    key: String,
    increment: i64,
}

impl IncrBy {
    pub fn new(key: impl Into<String>, increment: i64) -> Self {
        Self {
            key: key.into(),
            increment,
        }
    }
}

impl Command for IncrBy {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("INCRBY"),
            bulk(self.key.as_str()),
            bulk(self.increment.to_string()),
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
        "INCRBY"
    }
}

/// MSETNX key value \[key value ...\]
///
/// Sets the given keys to their respective values, but only if none of the
/// keys already exist. Returns `true` if all keys were set, `false` if no
/// key was set (at least one already existed).
pub struct MSetNx {
    pairs: Vec<(String, String)>,
}

impl MSetNx {
    pub fn new(pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self {
        Self {
            pairs: pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }
}

impl Command for MSetNx {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("MSETNX")];
        for (k, v) in &self.pairs {
            args.push(bulk(k.as_str()));
            args.push(bulk(v.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),
            Frame::Integer(0) => Ok(false),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer 0 or 1",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "MSETNX"
    }
}

/// Mode selector for the LCS command.
pub enum LcsMode {
    /// Return the longest common substring as bytes.
    String,
    /// Return only the length of the longest common substring.
    Len,
    /// Return match indices. Optionally filter by minimum match length and
    /// include match lengths.
    Idx {
        min_match_len: Option<u64>,
        with_match_len: bool,
    },
}

/// LCS key1 key2 \[LEN\] \[IDX\] \[MINMATCHLEN len\] \[WITHMATCHLEN\]
///
/// Returns the longest common substring between the values stored at two
/// keys. The response type depends on the selected mode: a bulk string for
/// the default mode, an integer for LEN mode, or a raw Frame for IDX mode
/// (which returns a complex nested structure).
pub struct Lcs {
    key1: String,
    key2: String,
    mode: LcsMode,
}

impl Lcs {
    /// Create a new LCS command in default (string) mode.
    pub fn new(key1: impl Into<String>, key2: impl Into<String>) -> Self {
        Self {
            key1: key1.into(),
            key2: key2.into(),
            mode: LcsMode::String,
        }
    }

    /// Switch to LEN mode -- returns only the length.
    pub fn len(mut self) -> Self {
        self.mode = LcsMode::Len;
        self
    }

    /// Switch to IDX mode -- returns match positions.
    pub fn idx(mut self) -> Self {
        self.mode = LcsMode::Idx {
            min_match_len: None,
            with_match_len: false,
        };
        self
    }

    /// Set the MINMATCHLEN option (only meaningful in IDX mode).
    pub fn min_match_len(mut self, len: u64) -> Self {
        match &mut self.mode {
            LcsMode::Idx { min_match_len, .. } => *min_match_len = Some(len),
            _ => {
                self.mode = LcsMode::Idx {
                    min_match_len: Some(len),
                    with_match_len: false,
                };
            }
        }
        self
    }

    /// Enable WITHMATCHLEN (only meaningful in IDX mode).
    pub fn with_match_len(mut self) -> Self {
        match &mut self.mode {
            LcsMode::Idx { with_match_len, .. } => *with_match_len = true,
            _ => {
                self.mode = LcsMode::Idx {
                    min_match_len: None,
                    with_match_len: true,
                };
            }
        }
        self
    }
}

impl Command for Lcs {
    /// The response is a raw `Frame` because the structure varies by mode:
    /// bulk string in default mode, integer in LEN mode, and a nested
    /// array/map in IDX mode.
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("LCS"),
            bulk(self.key1.as_str()),
            bulk(self.key2.as_str()),
        ];

        match &self.mode {
            LcsMode::String => {}
            LcsMode::Len => {
                args.push(bulk("LEN"));
            }
            LcsMode::Idx {
                min_match_len,
                with_match_len,
            } => {
                args.push(bulk("IDX"));
                if let Some(len) = min_match_len {
                    args.push(bulk("MINMATCHLEN"));
                    args.push(bulk(len.to_string()));
                }
                if *with_match_len {
                    args.push(bulk("WITHMATCHLEN"));
                }
            }
        }

        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "LCS"
    }
}

/// GETSET key value
///
/// Atomically sets `key` to `value` and returns the old value stored at
/// `key`. Returns `None` if the key did not exist previously.
///
/// Note: GETSET is deprecated in favor of `SET key value GET`, but remains
/// widely used.
pub struct GetSet {
    key: String,
    value: String,
}

impl GetSet {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

impl Command for GetSet {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("GETSET"),
            bulk(self.key.as_str()),
            bulk(self.value.as_str()),
        ])
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
        "GETSET"
    }
}
