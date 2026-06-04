use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// LPUSH key element \[element ...\]
///
/// Prepends one or more elements to the head of the list stored at `key`.
/// Returns the length of the list after the push operation.
pub struct LPush {
    key: String,
    elements: Vec<String>,
}

impl LPush {
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            elements: vec![element.into()],
        }
    }

    pub fn elements(
        key: impl Into<String>,
        elements: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            elements: elements.into_iter().map(Into::into).collect(),
        }
    }

    /// Add another element to push.
    pub fn element(mut self, element: impl Into<String>) -> Self {
        self.elements.push(element.into());
        self
    }
}

impl Command for LPush {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("LPUSH"), bulk(self.key.as_str())];
        for element in &self.elements {
            args.push(bulk(element.as_str()));
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
        "LPUSH"
    }
}

/// RPUSH key element \[element ...\]
///
/// Appends one or more elements to the tail of the list stored at `key`.
/// Returns the length of the list after the push operation.
pub struct RPush {
    key: String,
    elements: Vec<String>,
}

impl RPush {
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            elements: vec![element.into()],
        }
    }

    pub fn elements(
        key: impl Into<String>,
        elements: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            elements: elements.into_iter().map(Into::into).collect(),
        }
    }

    /// Add another element to push.
    pub fn element(mut self, element: impl Into<String>) -> Self {
        self.elements.push(element.into());
        self
    }
}

impl Command for RPush {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("RPUSH"), bulk(self.key.as_str())];
        for element in &self.elements {
            args.push(bulk(element.as_str()));
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
        "RPUSH"
    }
}

/// LPOP key
///
/// Removes and returns the first element of the list stored at `key`.
/// Returns `None` if the key does not exist.
pub struct LPop {
    key: String,
}

impl LPop {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for LPop {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("LPOP"), bulk(self.key.as_str())])
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
        "LPOP"
    }
}

/// RPOP key
///
/// Removes and returns the last element of the list stored at `key`.
/// Returns `None` if the key does not exist.
pub struct RPop {
    key: String,
}

impl RPop {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for RPop {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("RPOP"), bulk(self.key.as_str())])
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
        "RPOP"
    }
}

/// LRANGE key start stop
///
/// Returns the specified elements of the list stored at `key`. The offsets
/// `start` and `stop` are zero-based indices, with negative values counting
/// from the end of the list.
pub struct LRange {
    key: String,
    start: i64,
    stop: i64,
}

impl LRange {
    pub fn new(key: impl Into<String>, start: i64, stop: i64) -> Self {
        Self {
            key: key.into(),
            start,
            stop,
        }
    }
}

impl Command for LRange {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("LRANGE"),
            bulk(self.key.as_str()),
            bulk(self.start.to_string()),
            bulk(self.stop.to_string()),
        ])
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
            Frame::Array(None) => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "LRANGE"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// LLEN key
///
/// Returns the length of the list stored at `key`. If the key does not
/// exist, it is interpreted as an empty list and 0 is returned.
pub struct LLen {
    key: String,
}

impl LLen {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for LLen {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("LLEN"), bulk(self.key.as_str())])
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
        "LLEN"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// LINDEX key index
///
/// Returns the element at `index` in the list stored at `key`. The index
/// is zero-based, with negative values counting from the end of the list.
/// Returns `None` if the index is out of range.
pub struct LIndex {
    key: String,
    index: i64,
}

impl LIndex {
    pub fn new(key: impl Into<String>, index: i64) -> Self {
        Self {
            key: key.into(),
            index,
        }
    }
}

impl Command for LIndex {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("LINDEX"),
            bulk(self.key.as_str()),
            bulk(self.index.to_string()),
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
        "LINDEX"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// LSET key index element
///
/// Sets the list element at `index` to `element`. An error is returned for
/// out-of-range indices.
pub struct LSet {
    key: String,
    index: i64,
    element: String,
}

impl LSet {
    pub fn new(key: impl Into<String>, index: i64, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            index,
            element: element.into(),
        }
    }
}

impl Command for LSet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("LSET"),
            bulk(self.key.as_str()),
            bulk(self.index.to_string()),
            bulk(self.element.as_str()),
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
        "LSET"
    }
}

/// Direction for LMOVE source/destination.
pub enum ListDirection {
    Left,
    Right,
}

impl ListDirection {
    fn as_str(&self) -> &str {
        match self {
            ListDirection::Left => "LEFT",
            ListDirection::Right => "RIGHT",
        }
    }
}

/// LMOVE source destination LEFT|RIGHT LEFT|RIGHT
///
/// Atomically pops an element from `source` and pushes it to `destination`.
/// Returns the element moved.
pub struct LMove {
    source: String,
    destination: String,
    wherefrom: ListDirection,
    whereto: ListDirection,
}

impl LMove {
    pub fn new(
        source: impl Into<String>,
        destination: impl Into<String>,
        wherefrom: ListDirection,
        whereto: ListDirection,
    ) -> Self {
        Self {
            source: source.into(),
            destination: destination.into(),
            wherefrom,
            whereto,
        }
    }
}

impl Command for LMove {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("LMOVE"),
            bulk(self.source.as_str()),
            bulk(self.destination.as_str()),
            bulk(self.wherefrom.as_str()),
            bulk(self.whereto.as_str()),
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
        "LMOVE"
    }
}

/// LPUSHX key element
///
/// Prepends an element to the head of the list stored at `key`, only if `key`
/// already exists and holds a list. Returns the length of the list after the
/// push operation, or 0 if the key does not exist.
pub struct LPushX {
    key: String,
    element: String,
}

impl LPushX {
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
        }
    }
}

impl Command for LPushX {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("LPUSHX"),
            bulk(self.key.as_str()),
            bulk(self.element.as_str()),
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
        "LPUSHX"
    }
}

/// RPUSHX key element
///
/// Appends an element to the tail of the list stored at `key`, only if `key`
/// already exists and holds a list. Returns the length of the list after the
/// push operation, or 0 if the key does not exist.
pub struct RPushX {
    key: String,
    element: String,
}

impl RPushX {
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
        }
    }
}

impl Command for RPushX {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("RPUSHX"),
            bulk(self.key.as_str()),
            bulk(self.element.as_str()),
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
        "RPUSHX"
    }
}

/// Position relative to a pivot element for `LINSERT`.
pub enum ListPosition {
    Before,
    After,
}

impl ListPosition {
    fn as_str(&self) -> &str {
        match self {
            ListPosition::Before => "BEFORE",
            ListPosition::After => "AFTER",
        }
    }
}

/// LINSERT key BEFORE|AFTER pivot element
///
/// Inserts `element` in the list stored at `key` either before or after the
/// reference value `pivot`. Returns the length of the list after the insert
/// operation, or -1 when the pivot value was not found.
pub struct LInsert {
    key: String,
    position: ListPosition,
    pivot: String,
    element: String,
}

impl LInsert {
    pub fn new(
        key: impl Into<String>,
        position: ListPosition,
        pivot: impl Into<String>,
        element: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            position,
            pivot: pivot.into(),
            element: element.into(),
        }
    }
}

impl Command for LInsert {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("LINSERT"),
            bulk(self.key.as_str()),
            bulk(self.position.as_str()),
            bulk(self.pivot.as_str()),
            bulk(self.element.as_str()),
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
        "LINSERT"
    }
}

/// LREM key count element
///
/// Removes the first `count` occurrences of `element` from the list stored
/// at `key`. If `count` is positive, elements are removed from head to tail;
/// if negative, from tail to head; if zero, all occurrences are removed.
/// Returns the number of removed elements.
pub struct LRem {
    key: String,
    count: i64,
    element: String,
}

impl LRem {
    pub fn new(key: impl Into<String>, count: i64, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count,
            element: element.into(),
        }
    }
}

impl Command for LRem {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("LREM"),
            bulk(self.key.as_str()),
            bulk(self.count.to_string()),
            bulk(self.element.as_str()),
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
        "LREM"
    }
}

/// LTRIM key start stop
///
/// Trims an existing list so that it will contain only the specified range of
/// elements. Both `start` and `stop` are zero-based indices, with negative
/// values counting from the end of the list.
pub struct LTrim {
    key: String,
    start: i64,
    stop: i64,
}

impl LTrim {
    pub fn new(key: impl Into<String>, start: i64, stop: i64) -> Self {
        Self {
            key: key.into(),
            start,
            stop,
        }
    }
}

impl Command for LTrim {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("LTRIM"),
            bulk(self.key.as_str()),
            bulk(self.start.to_string()),
            bulk(self.stop.to_string()),
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
        "LTRIM"
    }
}

/// LPOS key element \[RANK rank\] \[COUNT count\] \[MAXLEN maxlen\]
///
/// Returns the index of matching elements inside a list. By default returns
/// the position of the first match. Use the builder methods to set optional
/// `RANK`, `COUNT`, and `MAXLEN` sub-commands.
pub struct LPos {
    key: String,
    element: String,
    rank: Option<i64>,
    count: Option<u64>,
    maxlen: Option<u64>,
}

impl LPos {
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
            rank: None,
            count: None,
            maxlen: None,
        }
    }

    /// Set the RANK option. A positive rank skips that many matches from the
    /// head; a negative rank searches from the tail.
    pub fn rank(mut self, rank: i64) -> Self {
        self.rank = Some(rank);
        self
    }

    /// Set the COUNT option. Limits the number of returned matches (0 means
    /// return all matches).
    pub fn count(mut self, count: u64) -> Self {
        self.count = Some(count);
        self
    }

    /// Set the MAXLEN option. Limits the scan to the first `maxlen` entries.
    pub fn maxlen(mut self, maxlen: u64) -> Self {
        self.maxlen = Some(maxlen);
        self
    }
}

impl Command for LPos {
    type Response = Option<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("LPOS"),
            bulk(self.key.as_str()),
            bulk(self.element.as_str()),
        ];
        if let Some(rank) = self.rank {
            args.push(bulk("RANK"));
            args.push(bulk(rank.to_string()));
        }
        if let Some(count) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(count.to_string()));
        }
        if let Some(maxlen) = self.maxlen {
            args.push(bulk("MAXLEN"));
            args.push(bulk(maxlen.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(Some(n)),
            Frame::Null => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "LPOS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// LMPOP numkeys key \[key ...\] LEFT|RIGHT \[COUNT count\]
///
/// Pops one or more elements from the first non-empty list among the
/// specified keys. Returns the key name and the popped elements as
/// `Some((key, elements))`, or `None` if all lists are empty.
pub struct LMPop {
    keys: Vec<String>,
    direction: ListDirection,
    count: Option<u64>,
}

impl LMPop {
    pub fn new(
        keys: impl IntoIterator<Item = impl Into<String>>,
        direction: ListDirection,
    ) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
            direction,
            count: None,
        }
    }

    /// Set the COUNT option to pop multiple elements.
    pub fn count(mut self, count: u64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for LMPop {
    type Response = Option<(Bytes, Vec<Bytes>)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("LMPOP"), bulk(self.keys.len().to_string())];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        args.push(bulk(self.direction.as_str()));
        if let Some(count) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(count.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Null => Ok(None),
            Frame::Array(None) => Ok(None),
            Frame::Array(Some(frames)) if frames.len() == 2 => {
                let mut iter = frames.into_iter();
                let key = match iter.next().unwrap() {
                    Frame::BulkString(Some(data)) => data,
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "bulk string (key name)",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                let elements = match iter.next().unwrap() {
                    Frame::Array(Some(elems)) => elems
                        .into_iter()
                        .map(|f| match f {
                            Frame::BulkString(Some(data)) => Ok(data),
                            other => Err(RedisError::UnexpectedResponse {
                                expected: "bulk string",
                                actual: format!("{other:?}"),
                            }),
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "array of bulk strings",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                Ok(Some((key, elements)))
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "null or two-element array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "LMPOP"
    }
}

/// LPOP key count
///
/// Removes and returns up to `count` elements from the head of the list stored
/// at `key`. Returns an empty vector if the key does not exist.
pub struct LPopCount {
    key: String,
    count: u64,
}

impl LPopCount {
    pub fn new(key: impl Into<String>, count: u64) -> Self {
        Self {
            key: key.into(),
            count,
        }
    }
}

impl Command for LPopCount {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("LPOP"),
            bulk(self.key.as_str()),
            bulk(self.count.to_string()),
        ])
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
            Frame::Array(None) | Frame::Null => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "LPOP"
    }
}

/// RPOP key count
///
/// Removes and returns up to `count` elements from the tail of the list stored
/// at `key`. Returns an empty vector if the key does not exist.
pub struct RPopCount {
    key: String,
    count: u64,
}

impl RPopCount {
    pub fn new(key: impl Into<String>, count: u64) -> Self {
        Self {
            key: key.into(),
            count,
        }
    }
}

impl Command for RPopCount {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("RPOP"),
            bulk(self.key.as_str()),
            bulk(self.count.to_string()),
        ])
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
            Frame::Array(None) | Frame::Null => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "RPOP"
    }
}

/// BLMPOP timeout numkeys key \[key ...\] LEFT|RIGHT \[COUNT count\]
///
/// Blocking variant of LMPOP. Pops one or more elements from the first
/// non-empty list among the specified keys, blocking up to `timeout` seconds
/// (0 to block indefinitely). Returns the key name and the popped elements as
/// `Some((key, elements))`, or `None` on timeout.
pub struct BlMPop {
    timeout: f64,
    keys: Vec<String>,
    direction: ListDirection,
    count: Option<u64>,
}

impl BlMPop {
    pub fn new(
        timeout: f64,
        keys: impl IntoIterator<Item = impl Into<String>>,
        direction: ListDirection,
    ) -> Self {
        Self {
            timeout,
            keys: keys.into_iter().map(Into::into).collect(),
            direction,
            count: None,
        }
    }

    /// Set the COUNT option to pop multiple elements.
    pub fn count(mut self, count: u64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for BlMPop {
    type Response = Option<(Bytes, Vec<Bytes>)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("BLMPOP"),
            bulk(self.timeout.to_string()),
            bulk(self.keys.len().to_string()),
        ];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
        }
        args.push(bulk(self.direction.as_str()));
        if let Some(count) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(count.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Null => Ok(None),
            Frame::Array(None) => Ok(None),
            Frame::Array(Some(frames)) if frames.len() == 2 => {
                let mut iter = frames.into_iter();
                let key = match iter.next().unwrap() {
                    Frame::BulkString(Some(data)) => data,
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "bulk string (key name)",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                let elements = match iter.next().unwrap() {
                    Frame::Array(Some(elems)) => elems
                        .into_iter()
                        .map(|f| match f {
                            Frame::BulkString(Some(data)) => Ok(data),
                            other => Err(RedisError::UnexpectedResponse {
                                expected: "bulk string",
                                actual: format!("{other:?}"),
                            }),
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    other => {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "array of bulk strings",
                            actual: format!("{other:?}"),
                        });
                    }
                };
                Ok(Some((key, elements)))
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "null or two-element array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "BLMPOP"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use redis_tower_protocol::Frame;
    use redis_tower_protocol::helpers::{array, bulk};

    // -- LPush --

    #[test]
    fn lpush_single_to_frame() {
        let cmd = LPush::new("mylist", "a");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("LPUSH"), bulk("mylist"), bulk("a")])
        );
    }

    #[test]
    fn lpush_multiple_to_frame() {
        let cmd = LPush::elements("mylist", vec!["a", "b", "c"]);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("LPUSH"),
                bulk("mylist"),
                bulk("a"),
                bulk("b"),
                bulk("c"),
            ])
        );
    }

    #[test]
    fn lpush_parse_integer() {
        let cmd = LPush::new("mylist", "a");
        assert_eq!(cmd.parse_response(Frame::Integer(3)).unwrap(), 3);
    }

    #[test]
    fn lpush_parse_error_on_string() {
        let cmd = LPush::new("mylist", "a");
        assert!(
            cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
                .is_err()
        );
    }

    // -- RPush --

    #[test]
    fn rpush_to_frame() {
        let cmd = RPush::new("mylist", "x");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("RPUSH"), bulk("mylist"), bulk("x")])
        );
    }

    // -- LPop --

    #[test]
    fn lpop_to_frame() {
        let cmd = LPop::new("mylist");
        assert_eq!(cmd.to_frame(), array(vec![bulk("LPOP"), bulk("mylist")]));
    }

    #[test]
    fn lpop_parse_value() {
        let cmd = LPop::new("mylist");
        let frame = Frame::BulkString(Some(Bytes::from("first")));
        assert_eq!(
            cmd.parse_response(frame).unwrap(),
            Some(Bytes::from("first"))
        );
    }

    #[test]
    fn lpop_parse_null() {
        let cmd = LPop::new("mylist");
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    // -- RPop --

    #[test]
    fn rpop_to_frame() {
        let cmd = RPop::new("mylist");
        assert_eq!(cmd.to_frame(), array(vec![bulk("RPOP"), bulk("mylist")]));
    }

    // -- LRange --

    #[test]
    fn lrange_to_frame() {
        let cmd = LRange::new("mylist", 0, -1);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("LRANGE"), bulk("mylist"), bulk("0"), bulk("-1")])
        );
    }

    #[test]
    fn lrange_parse_array() {
        let cmd = LRange::new("mylist", 0, -1);
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::BulkString(Some(Bytes::from("b"))),
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    #[test]
    fn lrange_parse_empty() {
        let cmd = LRange::new("mylist", 0, -1);
        let frame = Frame::Array(None);
        let result = cmd.parse_response(frame).unwrap();
        assert!(result.is_empty());
    }

    // -- LLen --

    #[test]
    fn llen_to_frame() {
        let cmd = LLen::new("mylist");
        assert_eq!(cmd.to_frame(), array(vec![bulk("LLEN"), bulk("mylist")]));
    }

    // -- LIndex --

    #[test]
    fn lindex_to_frame() {
        let cmd = LIndex::new("mylist", 0);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("LINDEX"), bulk("mylist"), bulk("0")])
        );
    }

    #[test]
    fn lindex_parse_null() {
        let cmd = LIndex::new("mylist", 99);
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    // -- LSet --

    #[test]
    fn lset_to_frame() {
        let cmd = LSet::new("mylist", 0, "newval");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("LSET"),
                bulk("mylist"),
                bulk("0"),
                bulk("newval")
            ])
        );
    }

    #[test]
    fn lset_parse_ok() {
        let cmd = LSet::new("mylist", 0, "v");
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
    }

    // -- LMove --

    #[test]
    fn lmove_to_frame() {
        let cmd = LMove::new("src", "dst", ListDirection::Left, ListDirection::Right);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("LMOVE"),
                bulk("src"),
                bulk("dst"),
                bulk("LEFT"),
                bulk("RIGHT"),
            ])
        );
    }

    // -- LMPop --

    #[test]
    fn lmpop_to_frame() {
        let cmd = LMPop::new(vec!["k1", "k2"], ListDirection::Left).count(3);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("LMPOP"),
                bulk("2"),
                bulk("k1"),
                bulk("k2"),
                bulk("LEFT"),
                bulk("COUNT"),
                bulk("3"),
            ])
        );
    }

    #[test]
    fn lmpop_parse_null() {
        let cmd = LMPop::new(vec!["k1"], ListDirection::Left);
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    #[test]
    fn lmpop_parse_result() {
        let cmd = LMPop::new(vec!["k1"], ListDirection::Left);
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("k1"))),
            array(vec![Frame::BulkString(Some(Bytes::from("elem")))]),
        ]);
        let result = cmd.parse_response(frame).unwrap().unwrap();
        assert_eq!(result.0, Bytes::from("k1"));
        assert_eq!(result.1, vec![Bytes::from("elem")]);
    }

    // -- LPos --

    #[test]
    fn lpos_to_frame() {
        let cmd = LPos::new("mylist", "val");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("LPOS"), bulk("mylist"), bulk("val")])
        );
    }

    #[test]
    fn lpos_with_options_to_frame() {
        let cmd = LPos::new("mylist", "val").rank(2).count(0).maxlen(100);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("LPOS"),
                bulk("mylist"),
                bulk("val"),
                bulk("RANK"),
                bulk("2"),
                bulk("COUNT"),
                bulk("0"),
                bulk("MAXLEN"),
                bulk("100"),
            ])
        );
    }

    #[test]
    fn lpos_parse_integer() {
        let cmd = LPos::new("mylist", "val");
        assert_eq!(cmd.parse_response(Frame::Integer(3)).unwrap(), Some(3));
    }

    #[test]
    fn lpos_parse_null() {
        let cmd = LPos::new("mylist", "val");
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    // -- LInsert --

    #[test]
    fn linsert_to_frame() {
        let cmd = LInsert::new("mylist", ListPosition::Before, "pivot", "elem");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("LINSERT"),
                bulk("mylist"),
                bulk("BEFORE"),
                bulk("pivot"),
                bulk("elem"),
            ])
        );
    }

    // -- LTrim --

    #[test]
    fn ltrim_to_frame() {
        let cmd = LTrim::new("mylist", 1, -1);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("LTRIM"), bulk("mylist"), bulk("1"), bulk("-1")])
        );
    }

    #[test]
    fn ltrim_parse_ok() {
        let cmd = LTrim::new("mylist", 0, 99);
        cmd.parse_response(Frame::SimpleString(Bytes::from("OK")))
            .unwrap();
    }

    // -- LPopCount --

    #[test]
    fn lpop_count_to_frame() {
        let cmd = LPopCount::new("mylist", 2);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("LPOP"), bulk("mylist"), bulk("2")])
        );
    }

    #[test]
    fn lpop_count_parse_array() {
        let cmd = LPopCount::new("mylist", 2);
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::BulkString(Some(Bytes::from("b"))),
        ]);
        let result = cmd.parse_response(frame).unwrap();
        assert_eq!(result, vec![Bytes::from("a"), Bytes::from("b")]);
    }

    #[test]
    fn lpop_count_parse_null() {
        let cmd = LPopCount::new("mylist", 2);
        assert!(cmd.parse_response(Frame::Null).unwrap().is_empty());
    }

    // -- RPopCount --

    #[test]
    fn rpop_count_to_frame() {
        let cmd = RPopCount::new("mylist", 3);
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("RPOP"), bulk("mylist"), bulk("3")])
        );
    }

    // -- BlMPop --

    #[test]
    fn blmpop_to_frame() {
        let cmd = BlMPop::new(1.5, vec!["k1", "k2"], ListDirection::Left).count(3);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("BLMPOP"),
                bulk("1.5"),
                bulk("2"),
                bulk("k1"),
                bulk("k2"),
                bulk("LEFT"),
                bulk("COUNT"),
                bulk("3"),
            ])
        );
    }

    #[test]
    fn blmpop_parse_null() {
        let cmd = BlMPop::new(1.0, vec!["k1"], ListDirection::Right);
        assert_eq!(cmd.parse_response(Frame::Null).unwrap(), None);
    }

    #[test]
    fn blmpop_parse_result() {
        let cmd = BlMPop::new(1.0, vec!["k1"], ListDirection::Left);
        let frame = array(vec![
            Frame::BulkString(Some(Bytes::from("k1"))),
            array(vec![Frame::BulkString(Some(Bytes::from("elem")))]),
        ]);
        let result = cmd.parse_response(frame).unwrap().unwrap();
        assert_eq!(result.0, Bytes::from("k1"));
        assert_eq!(result.1, vec![Bytes::from("elem")]);
    }
}
