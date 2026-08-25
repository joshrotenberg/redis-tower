use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// GET key
///
/// Returns the value of `key`, or `None` if the key does not exist.
#[derive(Clone)]
pub struct Get {
    key: String,
}

impl Get {
    /// Create a new [`Get`] command.
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// SET key value \[EX seconds\] \[PX milliseconds\] \[NX|XX\] \[GET\]
///
/// Sets `key` to hold `value`. Returns `Ok` on success, or the old value
/// if `GET` is specified.
#[derive(Clone)]
pub struct Set {
    key: String,
    value: String,
    ex: Option<u64>,
    px: Option<u64>,
    condition: Option<SetCondition>,
    get: bool,
}

/// Condition for SET (NX or XX).
#[derive(Clone)]
pub enum SetCondition {
    /// Only set if the key does not exist.
    Nx,
    /// Only set if the key already exists.
    Xx,
}

impl Set {
    /// Create a new [`Set`] command.
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
#[derive(Clone)]
pub struct Incr {
    key: String,
}

impl Incr {
    /// Create a new [`Incr`] command.
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

/// A numeric bound for [`IncrEx`].
///
/// The bound type must match the command's increment mode: use an integer
/// bound with the default or `BYINT` mode, and a floating-point bound with
/// `BYFLOAT`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IncrExBound {
    /// A signed 64-bit integer bound.
    Integer(i64),
    /// A floating-point bound.
    Float(f64),
}

impl From<i32> for IncrExBound {
    fn from(value: i32) -> Self {
        Self::Integer(i64::from(value))
    }
}

impl From<i64> for IncrExBound {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<f32> for IncrExBound {
    fn from(value: f32) -> Self {
        Self::Float(f64::from(value))
    }
}

impl From<f64> for IncrExBound {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl IncrExBound {
    fn as_string(self) -> String {
        match self {
            Self::Integer(value) => value.to_string(),
            Self::Float(value) => value.to_string(),
        }
    }
}

/// The result of an [`IncrEx`] operation.
///
/// Redis returns integer elements for the default and `BYINT` modes. In
/// `BYFLOAT` mode, RESP2 returns bulk strings and RESP3 returns doubles; both
/// protocol forms are normalized into [`Self::Float`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IncrExResult {
    /// Result from the default or `BYINT` mode.
    Integer {
        /// The value stored at the key after the operation.
        value: i64,
        /// The increment that Redis actually applied.
        actual_increment: i64,
    },
    /// Result from `BYFLOAT` mode.
    Float {
        /// The value stored at the key after the operation.
        value: f64,
        /// The increment that Redis actually applied.
        actual_increment: f64,
    },
}

#[derive(Clone, Copy)]
enum IncrExIncrement {
    Default,
    Integer(i64),
    Float(f64),
}

/// INCREX key \[BYFLOAT increment | BYINT increment\]
/// \[LBOUND lowerbound\] \[UBOUND upperbound\] \[SATURATE\]
/// \[EX seconds | PX milliseconds | EXAT timestamp | PXAT timestamp | PERSIST\]
/// \[ENX\]
///
/// Atomically increments the numeric value stored at `key`, optionally
/// constraining the result and changing its expiration (Redis 8.8+). The
/// response contains both the resulting value and the increment Redis actually
/// applied. If a bound would be exceeded without `SATURATE`, Redis leaves the
/// value unchanged and reports an actual increment of zero.
///
/// If no increment is configured, Redis uses integer mode with an increment of
/// one. `ENX` requires `EX`, `PX`, `EXAT`, or `PXAT`; Redis rejects it without
/// one of those expiration options.
#[derive(Clone)]
pub struct IncrEx {
    key: String,
    increment: IncrExIncrement,
    lower_bound: Option<IncrExBound>,
    upper_bound: Option<IncrExBound>,
    saturate: bool,
    ex: Option<u64>,
    px: Option<u64>,
    exat: Option<u64>,
    pxat: Option<u64>,
    persist: bool,
    enx: bool,
}

impl IncrEx {
    /// Create an `INCREX` command in the default integer-by-one mode.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            increment: IncrExIncrement::Default,
            lower_bound: None,
            upper_bound: None,
            saturate: false,
            ex: None,
            px: None,
            exat: None,
            pxat: None,
            persist: false,
            enx: false,
        }
    }

    /// Increment by a signed 64-bit integer.
    pub fn by_int(mut self, increment: i64) -> Self {
        self.increment = IncrExIncrement::Integer(increment);
        self
    }

    /// Increment by a floating-point value.
    pub fn by_float(mut self, increment: f64) -> Self {
        self.increment = IncrExIncrement::Float(increment);
        self
    }

    /// Set the inclusive lower bound.
    ///
    /// The bound's numeric type must match the selected increment mode.
    pub fn lower_bound(mut self, bound: impl Into<IncrExBound>) -> Self {
        self.lower_bound = Some(bound.into());
        self
    }

    /// Set the inclusive upper bound.
    ///
    /// The bound's numeric type must match the selected increment mode.
    pub fn upper_bound(mut self, bound: impl Into<IncrExBound>) -> Self {
        self.upper_bound = Some(bound.into());
        self
    }

    /// Cap or floor an out-of-bounds result instead of skipping the operation.
    pub fn saturate(mut self) -> Self {
        self.saturate = true;
        self
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

    /// Remove the key's existing expiration.
    ///
    /// `PERSIST` is incompatible with `ENX`, so this also clears an `ENX`
    /// option configured earlier in the builder chain.
    pub fn persist(mut self) -> Self {
        self.persist = true;
        self.ex = None;
        self.px = None;
        self.exat = None;
        self.pxat = None;
        self.enx = false;
        self
    }

    /// Only set the configured expiration when the key has no existing TTL.
    ///
    /// This must be combined with `EX`, `PX`, `EXAT`, or `PXAT`.
    pub fn enx(mut self) -> Self {
        self.enx = true;
        self
    }
}

impl Command for IncrEx {
    type Response = IncrExResult;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("INCREX"), bulk(self.key.as_str())];

        match self.increment {
            IncrExIncrement::Default => {}
            IncrExIncrement::Integer(increment) => {
                args.push(bulk("BYINT"));
                args.push(bulk(increment.to_string()));
            }
            IncrExIncrement::Float(increment) => {
                args.push(bulk("BYFLOAT"));
                args.push(bulk(increment.to_string()));
            }
        }
        if let Some(bound) = self.lower_bound {
            args.push(bulk("LBOUND"));
            args.push(bulk(bound.as_string()));
        }
        if let Some(bound) = self.upper_bound {
            args.push(bulk("UBOUND"));
            args.push(bulk(bound.as_string()));
        }
        if self.saturate {
            args.push(bulk("SATURATE"));
        }
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
        if self.enx {
            args.push(bulk("ENX"));
        }

        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        let frames = match frame {
            Frame::Array(Some(frames)) if frames.len() == 2 => frames,
            Frame::Array(Some(frames)) => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "two-element array",
                    actual: format!("array with {} elements", frames.len()),
                });
            }
            other => {
                return Err(RedisError::UnexpectedResponse {
                    expected: "two-element array",
                    actual: format!("{other:?}"),
                });
            }
        };

        match self.increment {
            IncrExIncrement::Default | IncrExIncrement::Integer(_) => {
                let mut values = frames.into_iter();
                let value = parse_increx_integer(values.next().expect("length checked"))?;
                let actual_increment =
                    parse_increx_integer(values.next().expect("length checked"))?;
                Ok(IncrExResult::Integer {
                    value,
                    actual_increment,
                })
            }
            IncrExIncrement::Float(_) => {
                let mut values = frames.into_iter();
                let value = parse_increx_float(values.next().expect("length checked"))?;
                let actual_increment = parse_increx_float(values.next().expect("length checked"))?;
                Ok(IncrExResult::Float {
                    value,
                    actual_increment,
                })
            }
        }
    }

    fn name(&self) -> &str {
        "INCREX"
    }
}

fn parse_increx_integer(frame: Frame) -> Result<i64, RedisError> {
    match frame {
        Frame::Integer(value) => Ok(value),
        other => Err(RedisError::UnexpectedResponse {
            expected: "integer",
            actual: format!("{other:?}"),
        }),
    }
}

fn parse_increx_float(frame: Frame) -> Result<f64, RedisError> {
    match frame {
        Frame::Double(value) => Ok(value),
        Frame::BulkString(Some(data)) => {
            let value = std::str::from_utf8(&data).map_err(|_| RedisError::UnexpectedResponse {
                expected: "valid UTF-8 float bulk string",
                actual: format!("{data:?}"),
            })?;
            value
                .parse::<f64>()
                .map_err(|_| RedisError::UnexpectedResponse {
                    expected: "float bulk string",
                    actual: value.to_string(),
                })
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "double or float bulk string",
            actual: format!("{other:?}"),
        }),
    }
}

/// MGET key [key ...]
///
/// Returns the values of all specified keys.
#[derive(Clone)]
pub struct MGet {
    keys: Vec<String>,
}

impl MGet {
    /// Create a new [`MGet`] command.
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// APPEND key value
///
/// Appends `value` to the end of the string at `key`. Returns the length
/// of the string after the append.
#[derive(Clone)]
pub struct Append {
    key: String,
    value: String,
}

impl Append {
    /// Create a new [`Append`] command.
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
#[derive(Clone)]
pub struct MSet {
    pairs: Vec<(String, String)>,
}

impl MSet {
    /// Create a new [`MSet`] command.
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

#[derive(Clone, Copy)]
enum MSetExCondition {
    Nx,
    Xx,
}

/// MSETEX numkeys key value \[key value ...\] \[NX | XX\]
/// \[EX seconds | PX milliseconds | EXAT timestamp | PXAT timestamp | KEEPTTL\]
///
/// Atomically sets multiple string keys with an optional shared expiration
/// (Redis 8.4+). Returns `true` when all keys were set and `false` when a
/// configured `NX` or `XX` condition prevented the operation.
#[derive(Clone)]
pub struct MSetEx {
    pairs: Vec<(String, String)>,
    condition: Option<MSetExCondition>,
    ex: Option<u64>,
    px: Option<u64>,
    exat: Option<u64>,
    pxat: Option<u64>,
    keep_ttl: bool,
}

impl MSetEx {
    /// Create an `MSETEX` command from key/value pairs.
    pub fn new(pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>) -> Self {
        Self {
            pairs: pairs
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            condition: None,
            ex: None,
            px: None,
            exat: None,
            pxat: None,
            keep_ttl: false,
        }
    }

    /// Set only if none of the specified keys exist.
    pub fn nx(mut self) -> Self {
        self.condition = Some(MSetExCondition::Nx);
        self
    }

    /// Set only if all of the specified keys already exist.
    pub fn xx(mut self) -> Self {
        self.condition = Some(MSetExCondition::Xx);
        self
    }

    /// Set expiration in seconds.
    pub fn ex(mut self, seconds: u64) -> Self {
        self.ex = Some(seconds);
        self.px = None;
        self.exat = None;
        self.pxat = None;
        self.keep_ttl = false;
        self
    }

    /// Set expiration in milliseconds.
    pub fn px(mut self, milliseconds: u64) -> Self {
        self.px = Some(milliseconds);
        self.ex = None;
        self.exat = None;
        self.pxat = None;
        self.keep_ttl = false;
        self
    }

    /// Set expiration as a Unix timestamp in seconds.
    pub fn exat(mut self, timestamp: u64) -> Self {
        self.exat = Some(timestamp);
        self.ex = None;
        self.px = None;
        self.pxat = None;
        self.keep_ttl = false;
        self
    }

    /// Set expiration as a Unix timestamp in milliseconds.
    pub fn pxat(mut self, timestamp: u64) -> Self {
        self.pxat = Some(timestamp);
        self.ex = None;
        self.px = None;
        self.exat = None;
        self.keep_ttl = false;
        self
    }

    /// Preserve the existing expiration of each key.
    pub fn keep_ttl(mut self) -> Self {
        self.keep_ttl = true;
        self.ex = None;
        self.px = None;
        self.exat = None;
        self.pxat = None;
        self
    }
}

impl Command for MSetEx {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("MSETEX"), bulk(self.pairs.len().to_string())];
        for (key, value) in &self.pairs {
            args.push(bulk(key.as_str()));
            args.push(bulk(value.as_str()));
        }
        match self.condition {
            Some(MSetExCondition::Nx) => args.push(bulk("NX")),
            Some(MSetExCondition::Xx) => args.push(bulk("XX")),
            None => {}
        }
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
        if self.keep_ttl {
            args.push(bulk("KEEPTTL"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_zero_or_one(frame)
    }

    fn name(&self) -> &str {
        "MSETEX"
    }
}

/// GETEX key \[EX seconds | PX milliseconds | EXAT timestamp | PXAT timestamp | PERSIST\]
///
/// Gets the value of `key` and optionally sets its expiration.
/// Returns `None` if the key does not exist.
#[derive(Clone)]
pub struct GetEx {
    key: String,
    ex: Option<u64>,
    px: Option<u64>,
    exat: Option<u64>,
    pxat: Option<u64>,
    persist: bool,
}

impl GetEx {
    /// Create a new [`GetEx`] command.
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
#[derive(Clone)]
pub struct GetDel {
    key: String,
}

impl GetDel {
    /// Create a new [`GetDel`] command.
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

#[derive(Clone)]
enum DelExCondition {
    Eq(String),
    Ne(String),
    DigestEq(String),
    DigestNe(String),
}

/// DELEX key \[IFEQ value | IFNE value | IFDEQ digest | IFDNE digest\]
///
/// Conditionally deletes `key` by comparing either its value or its
/// hexadecimal [`Digest`] result (Redis 8.4+). With no condition, this deletes
/// the key regardless of its type. Returns `true` when the key was deleted.
#[derive(Clone)]
pub struct DelEx {
    key: String,
    condition: Option<DelExCondition>,
}

impl DelEx {
    /// Create an unconditional `DELEX` command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            condition: None,
        }
    }

    /// Delete only when the current string value equals `value`.
    pub fn if_eq(mut self, value: impl Into<String>) -> Self {
        self.condition = Some(DelExCondition::Eq(value.into()));
        self
    }

    /// Delete only when the current string value does not equal `value`.
    pub fn if_ne(mut self, value: impl Into<String>) -> Self {
        self.condition = Some(DelExCondition::Ne(value.into()));
        self
    }

    /// Delete only when the current string digest equals `digest`.
    pub fn if_digest_eq(mut self, digest: impl Into<String>) -> Self {
        self.condition = Some(DelExCondition::DigestEq(digest.into()));
        self
    }

    /// Delete only when the current string digest does not equal `digest`.
    pub fn if_digest_ne(mut self, digest: impl Into<String>) -> Self {
        self.condition = Some(DelExCondition::DigestNe(digest.into()));
        self
    }
}

impl Command for DelEx {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("DELEX"), bulk(self.key.as_str())];
        match &self.condition {
            Some(DelExCondition::Eq(value)) => {
                args.push(bulk("IFEQ"));
                args.push(bulk(value.as_str()));
            }
            Some(DelExCondition::Ne(value)) => {
                args.push(bulk("IFNE"));
                args.push(bulk(value.as_str()));
            }
            Some(DelExCondition::DigestEq(digest)) => {
                args.push(bulk("IFDEQ"));
                args.push(bulk(digest.as_str()));
            }
            Some(DelExCondition::DigestNe(digest)) => {
                args.push(bulk("IFDNE"));
                args.push(bulk(digest.as_str()));
            }
            None => {}
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_zero_or_one(frame)
    }

    fn name(&self) -> &str {
        "DELEX"
    }
}

/// DIGEST key
///
/// Returns the XXH3 hash digest of a string value as hexadecimal bytes (Redis
/// 8.4+), or `None` when `key` does not exist.
#[derive(Clone)]
pub struct Digest {
    key: String,
}

impl Digest {
    /// Create a `DIGEST` command.
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Digest {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("DIGEST"), bulk(self.key.as_str())])
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
        "DIGEST"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

fn parse_zero_or_one(frame: Frame) -> Result<bool, RedisError> {
    match frame {
        Frame::Integer(0) => Ok(false),
        Frame::Integer(1) => Ok(true),
        other => Err(RedisError::UnexpectedResponse {
            expected: "integer 0 or 1",
            actual: format!("{other:?}"),
        }),
    }
}

/// SETEX key seconds value
///
/// Sets `key` to hold `value` with an expiration of `seconds`.
#[derive(Clone)]
pub struct SetEx {
    key: String,
    seconds: u64,
    value: String,
}

impl SetEx {
    /// Create a new [`SetEx`] command.
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
#[derive(Clone)]
pub struct PSetEx {
    key: String,
    milliseconds: u64,
    value: String,
}

impl PSetEx {
    /// Create a new [`PSetEx`] command.
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
#[derive(Clone)]
pub struct SetNx {
    key: String,
    value: String,
}

impl SetNx {
    /// Create a new [`SetNx`] command.
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
#[derive(Clone)]
pub struct IncrByFloat {
    key: String,
    increment: f64,
}

impl IncrByFloat {
    /// Create a new [`IncrByFloat`] command.
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
#[derive(Clone)]
pub struct Decr {
    key: String,
}

impl Decr {
    /// Create a new [`Decr`] command.
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
#[derive(Clone)]
pub struct DecrBy {
    key: String,
    decrement: i64,
}

impl DecrBy {
    /// Create a new [`DecrBy`] command.
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
#[derive(Clone)]
pub struct GetRange {
    key: String,
    start: i64,
    end: i64,
}

impl GetRange {
    /// Create a new [`GetRange`] command.
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// SETRANGE key offset value
///
/// Overwrites part of the string stored at `key`, starting at the
/// specified byte `offset`. Returns the length of the string after the
/// modification.
#[derive(Clone)]
pub struct SetRange {
    key: String,
    offset: i64,
    value: String,
}

impl SetRange {
    /// Create a new [`SetRange`] command.
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
#[derive(Clone)]
pub struct StrLen {
    key: String,
}

impl StrLen {
    /// Create a new [`StrLen`] command.
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// INCRBY key increment
///
/// Increments the integer value of `key` by `increment`. Returns the new
/// value after the increment.
#[derive(Clone)]
pub struct IncrBy {
    key: String,
    increment: i64,
}

impl IncrBy {
    /// Create a new [`IncrBy`] command.
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
#[derive(Clone)]
pub struct MSetNx {
    pairs: Vec<(String, String)>,
}

impl MSetNx {
    /// Create a new [`MSetNx`] command.
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
#[derive(Clone)]
pub enum LcsMode {
    /// Return the longest common substring as bytes.
    String,
    /// Return only the length of the longest common substring.
    Len,
    /// Return match indices. Optionally filter by minimum match length and
    /// include match lengths.
    Idx {
        /// Optional minimum length for reported matches.
        min_match_len: Option<u64>,
        /// Whether each reported match should include its length.
        with_match_len: bool,
    },
}

/// LCS key1 key2 \[LEN\] \[IDX\] \[MINMATCHLEN len\] \[WITHMATCHLEN\]
///
/// Returns the longest common substring between the values stored at two
/// keys. The response type depends on the selected mode: a bulk string for
/// the default mode, an integer for LEN mode, or a raw Frame for IDX mode
/// (which returns a complex nested structure).
#[derive(Clone)]
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// GETSET key value
///
/// Atomically sets `key` to `value` and returns the old value stored at
/// `key`. Returns `None` if the key did not exist previously.
///
/// Note: GETSET is deprecated in favor of `SET key value GET`, but remains
/// widely used.
#[derive(Clone)]
pub struct GetSet {
    key: String,
    value: String,
}

impl GetSet {
    /// Create a new [`GetSet`] command.
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

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use redis_tower_protocol::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

    // -- Get --

    #[test]
    fn get_to_frame() {
        let cmd = Get::new("mykey");
        let frame = cmd.to_frame();
        assert_eq!(frame, array(vec![bulk("GET"), bulk("mykey")]));
    }

    #[test]
    fn get_parse_bulk_string() {
        let cmd = Get::new("mykey");
        let frame = Frame::BulkString(Some(Bytes::from("hello")));
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, Some(Bytes::from("hello")));
    }

    #[test]
    fn get_parse_null() {
        let cmd = Get::new("mykey");
        let result = cmd.parse_response(Frame::Null).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn get_parse_error_on_integer() {
        let cmd = Get::new("mykey");
        assert!(cmd.parse_response(Frame::Integer(42)).is_err());
    }

    // -- Set --

    #[test]
    fn set_basic_to_frame() {
        let cmd = Set::new("k", "v");
        let frame = cmd.to_frame();
        assert_eq!(frame, array(vec![bulk("SET"), bulk("k"), bulk("v")]));
    }

    #[test]
    fn set_with_ex_nx_to_frame() {
        let cmd = Set::new("k", "v").ex(60).nx();
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            array(vec![
                bulk("SET"),
                bulk("k"),
                bulk("v"),
                bulk("EX"),
                bulk("60"),
                bulk("NX"),
            ])
        );
    }

    #[test]
    fn set_with_px_xx_get_to_frame() {
        let cmd = Set::new("k", "v").px(5000).xx().get();
        let frame = cmd.to_frame();
        assert_eq!(
            frame,
            array(vec![
                bulk("SET"),
                bulk("k"),
                bulk("v"),
                bulk("PX"),
                bulk("5000"),
                bulk("XX"),
                bulk("GET"),
            ])
        );
    }

    #[test]
    fn set_parse_ok() {
        let cmd = Set::new("k", "v");
        let frame = Frame::SimpleString(Bytes::from("OK"));
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn set_parse_bulk_with_get() {
        let cmd = Set::new("k", "v").get();
        let frame = Frame::BulkString(Some(Bytes::from("old")));
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, Some(Bytes::from("old")));
    }

    #[test]
    fn set_parse_null_nx_failure() {
        let cmd = Set::new("k", "v").nx();
        let result = cmd.parse_response(Frame::Null).unwrap();
        assert_eq!(result, None);
    }

    // -- Incr --

    #[test]
    fn incr_to_frame() {
        let cmd = Incr::new("counter");
        assert_eq!(cmd.to_frame(), array(vec![bulk("INCR"), bulk("counter")]));
    }

    #[test]
    fn incr_parse_integer() {
        let cmd = Incr::new("counter");
        assert_eq!(cmd.parse_response(Frame::Integer(5)).unwrap(), 5);
    }

    #[test]
    fn incr_parse_error_on_string() {
        let cmd = Incr::new("counter");
        assert!(
            cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
                .is_err()
        );
    }

    // -- IncrBy --

    #[test]
    fn incrby_to_frame() {
        let cmd = IncrBy::new("counter", 10);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("INCRBY"), bulk("counter"), bulk("10")])
        );
    }

    // -- IncrByFloat --

    #[test]
    fn incrbyfloat_to_frame() {
        let cmd = IncrByFloat::new("key", 1.5);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("INCRBYFLOAT"), bulk("key"), bulk("1.5")])
        );
    }

    #[test]
    fn incrbyfloat_parse_response() {
        let cmd = IncrByFloat::new("key", 1.5);
        let frame = Frame::BulkString(Some(Bytes::from("11.5")));
        let result = cmd.parse_response(frame).unwrap();
        assert!((result - 11.5).abs() < f64::EPSILON);
    }

    // -- MGet --

    #[test]
    fn mget_to_frame() {
        let cmd = MGet::new(vec!["a", "b", "c"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("MGET"), bulk("a"), bulk("b"), bulk("c")])
        );
    }

    #[test]
    fn mget_parse_mixed_results() {
        let cmd = MGet::new(vec!["a", "b"]);
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("val_a"))),
            Frame::Null,
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![Some(Bytes::from("val_a")), None]);
    }

    #[test]
    fn mget_parse_error_on_integer() {
        let cmd = MGet::new(vec!["a"]);
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    // -- MSet --

    #[test]
    fn mset_to_frame() {
        let cmd = MSet::new(vec![("a", "1"), ("b", "2")]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("MSET"),
                bulk("a"),
                bulk("1"),
                bulk("b"),
                bulk("2")
            ])
        );
    }

    #[test]
    fn mset_parse_ok() {
        let cmd = MSet::new(vec![("a", "1")]);
        let frame = Frame::SimpleString(Bytes::from("OK"));
        cmd.parse_response(frame).unwrap();
    }

    // -- SetNx --

    #[test]
    fn setnx_parse_true() {
        let cmd = SetNx::new("k", "v");
        assert!(cmd.parse_response(Frame::Integer(1)).unwrap());
    }

    #[test]
    fn setnx_parse_false() {
        let cmd = SetNx::new("k", "v");
        assert!(!cmd.parse_response(Frame::Integer(0)).unwrap());
    }

    // -- GetEx --

    #[test]
    fn getex_with_persist_to_frame() {
        let cmd = GetEx::new("mykey").persist();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("GETEX"), bulk("mykey"), bulk("PERSIST")])
        );
    }

    #[test]
    fn getex_with_exat_to_frame() {
        let cmd = GetEx::new("mykey").exat(1000);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("GETEX"),
                bulk("mykey"),
                bulk("EXAT"),
                bulk("1000")
            ])
        );
    }

    // -- Append --

    #[test]
    fn append_to_frame() {
        let cmd = Append::new("mykey", "world");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("APPEND"), bulk("mykey"), bulk("world")])
        );
    }

    #[test]
    fn append_parse_integer() {
        let cmd = Append::new("mykey", "world");
        assert_eq!(cmd.parse_response(Frame::Integer(10)).unwrap(), 10);
    }

    // -- MSetNx --

    #[test]
    fn msetnx_to_frame() {
        let cmd = MSetNx::new(vec![("a", "1"), ("b", "2")]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("MSETNX"),
                bulk("a"),
                bulk("1"),
                bulk("b"),
                bulk("2")
            ])
        );
    }

    #[test]
    fn msetnx_parse_true() {
        let cmd = MSetNx::new(vec![("a", "1")]);
        assert!(cmd.parse_response(Frame::Integer(1)).unwrap());
    }

    // -- Lcs --

    #[test]
    fn lcs_len_to_frame() {
        let cmd = Lcs::new("k1", "k2").len();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("LCS"), bulk("k1"), bulk("k2"), bulk("LEN")])
        );
    }

    // -- DelEx --

    #[test]
    fn delex_unconditional_to_frame() {
        let cmd = DelEx::new("key");
        assert_eq!(cmd.to_frame(), array(vec![bulk("DELEX"), bulk("key")]));
    }

    #[test]
    fn delex_value_conditions_to_frame() {
        assert_eq!(
            DelEx::new("key").if_eq("value").to_frame(),
            array(vec![
                bulk("DELEX"),
                bulk("key"),
                bulk("IFEQ"),
                bulk("value"),
            ])
        );
        assert_eq!(
            DelEx::new("key").if_ne("value").to_frame(),
            array(vec![
                bulk("DELEX"),
                bulk("key"),
                bulk("IFNE"),
                bulk("value"),
            ])
        );
    }

    #[test]
    fn delex_digest_conditions_to_frame() {
        assert_eq!(
            DelEx::new("key").if_digest_eq("abc123").to_frame(),
            array(vec![
                bulk("DELEX"),
                bulk("key"),
                bulk("IFDEQ"),
                bulk("abc123"),
            ])
        );
        assert_eq!(
            DelEx::new("key").if_digest_ne("abc123").to_frame(),
            array(vec![
                bulk("DELEX"),
                bulk("key"),
                bulk("IFDNE"),
                bulk("abc123"),
            ])
        );
    }

    #[test]
    fn delex_last_condition_wins() {
        let cmd = DelEx::new("key")
            .if_eq("old")
            .if_ne("new")
            .if_digest_eq("first")
            .if_digest_ne("last");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("DELEX"),
                bulk("key"),
                bulk("IFDNE"),
                bulk("last"),
            ])
        );
    }

    #[test]
    fn delex_parse_zero_or_one() {
        let cmd = DelEx::new("key");
        assert!(cmd.parse_response(Frame::Integer(1)).unwrap());
        assert!(!cmd.parse_response(Frame::Integer(0)).unwrap());
    }

    #[test]
    fn delex_rejects_invalid_response() {
        let cmd = DelEx::new("key");
        assert!(cmd.parse_response(Frame::Integer(2)).is_err());
        assert!(
            cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
                .is_err()
        );
    }

    // -- Digest --

    #[test]
    fn digest_to_frame() {
        let cmd = Digest::new("key");
        assert_eq!(cmd.to_frame(), array(vec![bulk("DIGEST"), bulk("key")]));
    }

    #[test]
    fn digest_parse_value_and_nulls() {
        let cmd = Digest::new("key");
        let digest = Bytes::from("b6acb9d84a38ff74");
        assert_eq!(
            cmd.parse_response(Frame::BulkString(Some(digest.clone())))
                .unwrap(),
            Some(digest)
        );
        assert_eq!(cmd.parse_response(Frame::BulkString(None)).unwrap(), None);
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    #[test]
    fn digest_rejects_non_bulk_response() {
        let cmd = Digest::new("key");
        assert!(cmd.parse_response(Frame::Integer(1)).is_err());
    }

    // -- MSetEx --

    #[test]
    fn msetex_to_frame_and_key_positions() {
        let cmd = MSetEx::new(vec![("key1", "value1"), ("key2", "value2")]);
        let expected = array(vec![
            bulk("MSETEX"),
            bulk("2"),
            bulk("key1"),
            bulk("value1"),
            bulk("key2"),
            bulk("value2"),
        ]);
        let frame = cmd.to_frame();
        assert_eq!(frame, expected);

        let Frame::Array(Some(args)) = frame else {
            panic!("MSETEX request must be an array");
        };
        assert_eq!(args[1], bulk("2"), "argv[1] is the key count");
        assert_eq!(args[2], bulk("key1"), "the first key is at argv[2]");
        assert_eq!(args[4], bulk("key2"), "keys repeat every two arguments");
    }

    #[test]
    fn msetex_nx_ex_to_frame() {
        let cmd = MSetEx::new(vec![("key", "value")]).nx().ex(60);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("MSETEX"),
                bulk("1"),
                bulk("key"),
                bulk("value"),
                bulk("NX"),
                bulk("EX"),
                bulk("60"),
            ])
        );
    }

    #[test]
    fn msetex_xx_px_to_frame() {
        let cmd = MSetEx::new(vec![("key", "value")]).xx().px(1500);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("MSETEX"),
                bulk("1"),
                bulk("key"),
                bulk("value"),
                bulk("XX"),
                bulk("PX"),
                bulk("1500"),
            ])
        );
    }

    #[test]
    fn msetex_absolute_expirations_to_frame() {
        let base = || MSetEx::new(vec![("key", "value")]);
        assert_eq!(
            base().exat(1_700_000_000).to_frame(),
            array(vec![
                bulk("MSETEX"),
                bulk("1"),
                bulk("key"),
                bulk("value"),
                bulk("EXAT"),
                bulk("1700000000"),
            ])
        );
        assert_eq!(
            base().pxat(1_700_000_000_000).to_frame(),
            array(vec![
                bulk("MSETEX"),
                bulk("1"),
                bulk("key"),
                bulk("value"),
                bulk("PXAT"),
                bulk("1700000000000"),
            ])
        );
    }

    #[test]
    fn msetex_latest_condition_and_expiration_win() {
        let cmd = MSetEx::new(vec![("key", "value")])
            .nx()
            .xx()
            .ex(1)
            .px(2)
            .exat(3)
            .pxat(4)
            .keep_ttl();
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("MSETEX"),
                bulk("1"),
                bulk("key"),
                bulk("value"),
                bulk("XX"),
                bulk("KEEPTTL"),
            ])
        );
    }

    #[test]
    fn msetex_parse_zero_or_one() {
        let cmd = MSetEx::new(vec![("key", "value")]);
        assert!(cmd.parse_response(Frame::Integer(1)).unwrap());
        assert!(!cmd.parse_response(Frame::Integer(0)).unwrap());
    }

    #[test]
    fn msetex_rejects_invalid_response() {
        let cmd = MSetEx::new(vec![("key", "value")]);
        assert!(cmd.parse_response(Frame::Integer(2)).is_err());
        assert!(
            cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
                .is_err()
        );
    }

    // -- IncrEx --

    #[test]
    fn increx_default_to_frame() {
        let cmd = IncrEx::new("counter");
        assert_eq!(cmd.to_frame(), array(vec![bulk("INCREX"), bulk("counter")]));
    }

    #[test]
    fn increx_integer_and_float_increments_to_frame() {
        assert_eq!(
            IncrEx::new("counter").by_int(-5).to_frame(),
            array(vec![
                bulk("INCREX"),
                bulk("counter"),
                bulk("BYINT"),
                bulk("-5"),
            ])
        );
        assert_eq!(
            IncrEx::new("counter").by_float(0.25).to_frame(),
            array(vec![
                bulk("INCREX"),
                bulk("counter"),
                bulk("BYFLOAT"),
                bulk("0.25"),
            ])
        );
    }

    #[test]
    fn increx_bounds_saturate_and_enx_to_frame() {
        let cmd = IncrEx::new("counter")
            .by_int(5)
            .lower_bound(-10)
            .upper_bound(100_i64)
            .saturate()
            .ex(60)
            .enx();
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("INCREX"),
                bulk("counter"),
                bulk("BYINT"),
                bulk("5"),
                bulk("LBOUND"),
                bulk("-10"),
                bulk("UBOUND"),
                bulk("100"),
                bulk("SATURATE"),
                bulk("EX"),
                bulk("60"),
                bulk("ENX"),
            ])
        );
    }

    #[test]
    fn increx_float_bounds_to_frame() {
        let cmd = IncrEx::new("counter")
            .by_float(0.5)
            .lower_bound(-1.25_f32)
            .upper_bound(9.75);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("INCREX"),
                bulk("counter"),
                bulk("BYFLOAT"),
                bulk("0.5"),
                bulk("LBOUND"),
                bulk("-1.25"),
                bulk("UBOUND"),
                bulk("9.75"),
            ])
        );
    }

    #[test]
    fn increx_expiration_variants_to_frame() {
        let base = || IncrEx::new("counter");
        assert_eq!(
            base().px(1500).to_frame(),
            array(vec![
                bulk("INCREX"),
                bulk("counter"),
                bulk("PX"),
                bulk("1500"),
            ])
        );
        assert_eq!(
            base().exat(1_700_000_000).to_frame(),
            array(vec![
                bulk("INCREX"),
                bulk("counter"),
                bulk("EXAT"),
                bulk("1700000000"),
            ])
        );
        assert_eq!(
            base().pxat(1_700_000_000_000).to_frame(),
            array(vec![
                bulk("INCREX"),
                bulk("counter"),
                bulk("PXAT"),
                bulk("1700000000000"),
            ])
        );
    }

    #[test]
    fn increx_persist_clears_expiration_and_enx() {
        let cmd = IncrEx::new("counter").ex(60).enx().px(100).persist();
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("INCREX"), bulk("counter"), bulk("PERSIST")])
        );
    }

    #[test]
    fn increx_parse_integer_result() {
        let cmd = IncrEx::new("counter").by_int(5);
        let result = cmd
            .parse_response(array(vec![Frame::Integer(15), Frame::Integer(5)]))
            .unwrap();
        assert_eq!(
            result,
            IncrExResult::Integer {
                value: 15,
                actual_increment: 5,
            }
        );
    }

    #[test]
    fn increx_parse_resp2_float_result() {
        let cmd = IncrEx::new("counter").by_float(0.25);
        let result = cmd
            .parse_response(array(vec![
                Frame::BulkString(Some(Bytes::from("1.75"))),
                Frame::BulkString(Some(Bytes::from("0.25"))),
            ]))
            .unwrap();
        assert_eq!(
            result,
            IncrExResult::Float {
                value: 1.75,
                actual_increment: 0.25,
            }
        );
    }

    #[test]
    fn increx_parse_resp3_float_result() {
        let cmd = IncrEx::new("counter").by_float(0.25);
        let result = cmd
            .parse_response(array(vec![Frame::Double(1.75), Frame::Double(0.25)]))
            .unwrap();
        assert_eq!(
            result,
            IncrExResult::Float {
                value: 1.75,
                actual_increment: 0.25,
            }
        );
    }

    #[test]
    fn increx_rejects_malformed_results() {
        let integer = IncrEx::new("counter");
        assert!(
            integer
                .parse_response(array(vec![Frame::Integer(1)]))
                .is_err()
        );
        assert!(
            integer
                .parse_response(array(vec![Frame::Integer(1), Frame::Double(1.0)]))
                .is_err()
        );
        assert!(integer.parse_response(Frame::Array(None)).is_err());

        let float = IncrEx::new("counter").by_float(0.5);
        assert!(
            float
                .parse_response(array(vec![
                    Frame::BulkString(Some(Bytes::from("not-a-float"))),
                    Frame::BulkString(Some(Bytes::from("0.5"))),
                ]))
                .is_err()
        );
        assert!(
            float
                .parse_response(array(vec![
                    Frame::BulkString(Some(Bytes::from_static(&[0xff]))),
                    Frame::Double(0.5),
                ]))
                .is_err()
        );
    }

    #[test]
    fn new_string_command_metadata() {
        let delex = DelEx::new("delex-key");
        assert_eq!(delex.name(), "DELEX");
        assert!(!delex.idempotent());

        let msetex = MSetEx::new(vec![("msetex-key", "value")]);
        assert_eq!(msetex.name(), "MSETEX");
        assert!(!msetex.idempotent());

        let digest = Digest::new("digest-key");
        assert_eq!(digest.name(), "DIGEST");
        assert!(digest.idempotent());

        let increx = IncrEx::new("increx-key");
        assert_eq!(increx.name(), "INCREX");
        assert!(!increx.idempotent());
    }
}
