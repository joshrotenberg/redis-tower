//! Redis 8.8 array commands.
//!
//! This native command family is available in Redis 8.8. Array indexes are
//! unsigned: ordinary commands accept values through `u64::MAX - 1`;
//! [`ArSeek`] uniquely accepts `u64::MAX` to place the insert cursor in its
//! terminal state.

use std::collections::HashMap;

use bytes::Bytes;
use redis_tower_core::{Command, Frame, FromFrame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

fn owned_bytes(value: impl AsRef<[u8]>) -> Bytes {
    Bytes::copy_from_slice(value.as_ref())
}

fn parse_u64(frame: Frame) -> Result<u64, RedisError> {
    u64::from_frame(frame)
}

fn parse_i64(frame: Frame) -> Result<i64, RedisError> {
    i64::from_frame(frame)
}

fn parse_bool(frame: Frame) -> Result<bool, RedisError> {
    match frame {
        Frame::Integer(0) => Ok(false),
        Frame::Integer(1) => Ok(true),
        Frame::Boolean(value) => Ok(value),
        other => Err(RedisError::UnexpectedResponse {
            expected: "integer 0 or 1, or boolean",
            actual: format!("{other:?}"),
        }),
    }
}

fn parse_optional_bytes(frame: Frame) -> Result<Option<Bytes>, RedisError> {
    Option::<Bytes>::from_frame(frame)
}

fn parse_optional_values(frame: Frame) -> Result<Vec<Option<Bytes>>, RedisError> {
    Vec::<Option<Bytes>>::from_frame(frame)
}

/// An existing value and its unsigned array index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayEntry {
    /// The zero-based array index.
    pub index: u64,
    /// The binary-safe value stored at `index`.
    pub value: Bytes,
}

impl FromFrame for ArrayEntry {
    fn from_frame(frame: Frame) -> Result<Self, RedisError> {
        let (index, value) = <(u64, Bytes)>::from_frame(frame)?;
        Ok(Self { index, value })
    }
}

/// A range accepted by [`ArGrep`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArGrepBound {
    /// A concrete zero-based array index.
    Index(u64),
    /// The logical first index, encoded as `-`.
    Start,
    /// The logical last index, encoded as `+`.
    End,
}

impl From<u64> for ArGrepBound {
    fn from(value: u64) -> Self {
        Self::Index(value)
    }
}

impl ArGrepBound {
    fn append_to(&self, args: &mut Vec<Frame>) {
        match self {
            Self::Index(index) => args.push(bulk(index.to_string())),
            Self::Start => args.push(bulk("-")),
            Self::End => args.push(bulk("+")),
        }
    }
}

/// A textual predicate accepted by [`ArGrep`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArGrepPredicate {
    /// Match values that equal the supplied bytes exactly.
    Exact(Bytes),
    /// Match values containing the supplied bytes as a substring.
    Match(Bytes),
    /// Match values using Redis glob syntax.
    Glob(Bytes),
    /// Match values using a Redis/TRE regular expression.
    Regex(Bytes),
}

impl ArGrepPredicate {
    /// Construct an `EXACT` predicate.
    pub fn exact(value: impl AsRef<[u8]>) -> Self {
        Self::Exact(owned_bytes(value))
    }

    /// Construct a `MATCH` substring predicate.
    pub fn matches(value: impl AsRef<[u8]>) -> Self {
        Self::Match(owned_bytes(value))
    }

    /// Construct a `GLOB` predicate.
    pub fn glob(pattern: impl AsRef<[u8]>) -> Self {
        Self::Glob(owned_bytes(pattern))
    }

    /// Construct an `RE` regular-expression predicate.
    ///
    /// Redis 8.8 requires a non-empty pattern of at most 2048 bytes and does
    /// not support backreferences. Redis validates those constraints.
    pub fn regex(pattern: impl AsRef<[u8]>) -> Self {
        Self::Regex(owned_bytes(pattern))
    }

    fn append_to(&self, args: &mut Vec<Frame>) {
        let (token, value) = match self {
            Self::Exact(value) => ("EXACT", value),
            Self::Match(value) => ("MATCH", value),
            Self::Glob(value) => ("GLOB", value),
            Self::Regex(value) => ("RE", value),
        };
        args.push(bulk(token));
        args.push(bulk(value));
    }
}

/// How multiple [`ArGrepPredicate`] values are combined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArGrepCombinator {
    /// Require every predicate to match.
    And,
    /// Require at least one predicate to match (the Redis default).
    Or,
}

/// The response returned by [`ArGrep`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArGrepResult {
    /// Matching indexes when `WITHVALUES` is not enabled.
    Indices(Vec<u64>),
    /// Matching index-value pairs when `WITHVALUES` is enabled.
    Entries(Vec<ArrayEntry>),
}

/// An aggregate operation accepted by [`ArOp`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArOpOperation {
    /// Sum numeric values and return bulk numeric text.
    Sum,
    /// Return the minimum numeric value as bulk numeric text.
    Min,
    /// Return the maximum numeric value as bulk numeric text.
    Max,
    /// Apply signed 64-bit bitwise AND.
    And,
    /// Apply signed 64-bit bitwise OR.
    Or,
    /// Apply signed 64-bit bitwise XOR.
    Xor,
    /// Count values equal to the supplied binary-safe value.
    Match(Bytes),
    /// Count populated slots in the range.
    Used,
}

impl ArOpOperation {
    /// Construct a binary-safe `MATCH value` operation.
    pub fn matches(value: impl AsRef<[u8]>) -> Self {
        Self::Match(owned_bytes(value))
    }

    fn append_to(&self, args: &mut Vec<Frame>) {
        match self {
            Self::Sum => args.push(bulk("SUM")),
            Self::Min => args.push(bulk("MIN")),
            Self::Max => args.push(bulk("MAX")),
            Self::And => args.push(bulk("AND")),
            Self::Or => args.push(bulk("OR")),
            Self::Xor => args.push(bulk("XOR")),
            Self::Match(value) => {
                args.push(bulk("MATCH"));
                args.push(bulk(value));
            }
            Self::Used => args.push(bulk("USED")),
        }
    }
}

/// The operation-dependent response returned by [`ArOp`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArOpResult {
    /// `SUM`, `MIN`, or `MAX`: binary-safe numeric text, or `None` when no
    /// numeric element was present.
    Value(Option<Bytes>),
    /// `AND`, `OR`, `XOR`, `MATCH`, or `USED`: an integer, or `None` when a
    /// bitwise operation had no usable value.
    Integer(Option<i64>),
}

/// Extra statistics returned by `ARINFO ... FULL`.
#[derive(Debug, Clone, PartialEq)]
pub struct ArInfoFull {
    /// Number of dense slices.
    pub dense_slices: u64,
    /// Number of sparse slices.
    pub sparse_slices: u64,
    /// Average allocated dense-window size.
    pub average_dense_size: f64,
    /// Average dense-window fill ratio.
    pub average_dense_fill: f64,
    /// Average sparse-slice capacity.
    pub average_sparse_size: f64,
}

/// Metadata returned by [`ArInfo`].
#[derive(Debug, Clone, PartialEq)]
pub struct ArInfoResponse {
    /// Number of populated slots.
    pub count: u64,
    /// Logical length (`maximum index + 1`).
    pub len: u64,
    /// Next index used by `ARINSERT`/`ARRING` as reported by Redis.
    pub next_insert_index: u64,
    /// Number of allocated slices.
    pub slices: u64,
    /// Allocated top-level directory size.
    pub directory_size: u64,
    /// Number of populated super-directory entries.
    pub super_directory_entries: u64,
    /// Number of logical positions in each slice.
    pub slice_size: u64,
    /// Statistics requested with `FULL`.
    pub full: Option<ArInfoFull>,
}

fn info_pairs(frame: Frame) -> Result<Vec<(Frame, Frame)>, RedisError> {
    match frame {
        Frame::Map(pairs) | Frame::StreamedMap(pairs) | Frame::Attribute(pairs) => Ok(pairs),
        Frame::Array(Some(items)) => {
            if items.len() % 2 != 0 {
                return Err(RedisError::UnexpectedResponse {
                    expected: "ARINFO map or even-length flat array",
                    actual: format!("array of {} elements", items.len()),
                });
            }
            let mut items = items.into_iter();
            let mut pairs = Vec::with_capacity(items.len() / 2);
            while let (Some(key), Some(value)) = (items.next(), items.next()) {
                pairs.push((key, value));
            }
            Ok(pairs)
        }
        other => Err(RedisError::UnexpectedResponse {
            expected: "ARINFO map or even-length flat array",
            actual: format!("{other:?}"),
        }),
    }
}

fn parse_info_map(frame: Frame, require_full: bool) -> Result<ArInfoResponse, RedisError> {
    let mut fields = HashMap::<Bytes, Frame>::new();
    for (key, value) in info_pairs(frame)? {
        fields.insert(Bytes::from_frame(key)?, value);
    }

    fn take<T: FromFrame>(
        fields: &mut HashMap<Bytes, Frame>,
        name: &'static [u8],
    ) -> Result<T, RedisError> {
        let value = fields
            .remove(name)
            .ok_or_else(|| RedisError::UnexpectedResponse {
                expected: "complete ARINFO field set",
                actual: format!("missing field {}", String::from_utf8_lossy(name)),
            })?;
        T::from_frame(value)
    }

    let count = take(&mut fields, b"count")?;
    let len = take(&mut fields, b"len")?;
    let next_insert_index = take(&mut fields, b"next-insert-index")?;
    let slices = take(&mut fields, b"slices")?;
    let directory_size = take(&mut fields, b"directory-size")?;
    let super_directory_entries = take(&mut fields, b"super-dir-entries")?;
    let slice_size = take(&mut fields, b"slice-size")?;

    let has_full = require_full
        || fields.contains_key(&Bytes::from_static(b"dense-slices"))
        || fields.contains_key(&Bytes::from_static(b"sparse-slices"));
    let full = if has_full {
        Some(ArInfoFull {
            dense_slices: take(&mut fields, b"dense-slices")?,
            sparse_slices: take(&mut fields, b"sparse-slices")?,
            average_dense_size: take(&mut fields, b"avg-dense-size")?,
            average_dense_fill: take(&mut fields, b"avg-dense-fill")?,
            average_sparse_size: take(&mut fields, b"avg-sparse-size")?,
        })
    } else {
        None
    };

    Ok(ArInfoResponse {
        count,
        len,
        next_insert_index,
        slices,
        directory_size,
        super_directory_entries,
        slice_size,
        full,
    })
}

/// ARCOUNT key
///
/// Returns the number of populated slots, or zero when `key` is absent.
#[derive(Debug, Clone)]
pub struct ArCount {
    key: Bytes,
}

impl ArCount {
    /// Construct an `ARCOUNT` command.
    pub fn new(key: impl AsRef<[u8]>) -> Self {
        Self {
            key: owned_bytes(key),
        }
    }
}

impl Command for ArCount {
    type Response = u64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("ARCOUNT"), bulk(&self.key)])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_u64(frame)
    }

    fn name(&self) -> &str {
        "ARCOUNT"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ARDEL key index \[index ...\]
///
/// Deletes one or more indexes and returns the number of populated slots that
/// were removed. Requiring the first index in [`ArDel::new`] prevents an
/// invalid empty variadic list.
#[derive(Debug, Clone)]
pub struct ArDel {
    key: Bytes,
    indices: Vec<u64>,
}

impl ArDel {
    /// Construct an `ARDEL` command with its first required index.
    pub fn new(key: impl AsRef<[u8]>, index: u64) -> Self {
        Self {
            key: owned_bytes(key),
            indices: vec![index],
        }
    }

    /// Add another index to delete.
    pub fn index(mut self, index: u64) -> Self {
        self.indices.push(index);
        self
    }
}

impl Command for ArDel {
    type Response = u64;

    fn to_frame(&self) -> Frame {
        let mut args = Vec::with_capacity(2 + self.indices.len());
        args.push(bulk("ARDEL"));
        args.push(bulk(&self.key));
        args.extend(self.indices.iter().map(|index| bulk(index.to_string())));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_u64(frame)
    }

    fn name(&self) -> &str {
        "ARDEL"
    }
}

/// ARDELRANGE key start end \[start end ...\]
///
/// Deletes all populated elements covered by one or more inclusive ranges and
/// returns the number deleted. Redis accepts each range in either direction.
#[derive(Debug, Clone)]
pub struct ArDelRange {
    key: Bytes,
    ranges: Vec<(u64, u64)>,
}

impl ArDelRange {
    /// Construct an `ARDELRANGE` command with its first required range.
    pub fn new(key: impl AsRef<[u8]>, start: u64, end: u64) -> Self {
        Self {
            key: owned_bytes(key),
            ranges: vec![(start, end)],
        }
    }

    /// Add another inclusive range.
    pub fn range(mut self, start: u64, end: u64) -> Self {
        self.ranges.push((start, end));
        self
    }
}

impl Command for ArDelRange {
    type Response = u64;

    fn to_frame(&self) -> Frame {
        let mut args = Vec::with_capacity(2 + self.ranges.len() * 2);
        args.push(bulk("ARDELRANGE"));
        args.push(bulk(&self.key));
        for (start, end) in &self.ranges {
            args.push(bulk(start.to_string()));
            args.push(bulk(end.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_u64(frame)
    }

    fn name(&self) -> &str {
        "ARDELRANGE"
    }
}

/// ARGET key index
///
/// Returns the binary-safe value at `index`, or `None` for a hole or absent
/// key.
#[derive(Debug, Clone)]
pub struct ArGet {
    key: Bytes,
    index: u64,
}

impl ArGet {
    /// Construct an `ARGET` command.
    pub fn new(key: impl AsRef<[u8]>, index: u64) -> Self {
        Self {
            key: owned_bytes(key),
            index,
        }
    }
}

impl Command for ArGet {
    type Response = Option<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ARGET"),
            bulk(&self.key),
            bulk(self.index.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_optional_bytes(frame)
    }

    fn name(&self) -> &str {
        "ARGET"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ARGETRANGE key start end
///
/// Returns every position in the inclusive range, preserving holes as
/// `None`. If `start > end`, values are returned in descending index order.
#[derive(Debug, Clone)]
pub struct ArGetRange {
    key: Bytes,
    start: u64,
    end: u64,
}

impl ArGetRange {
    /// Construct an `ARGETRANGE` command.
    pub fn new(key: impl AsRef<[u8]>, start: u64, end: u64) -> Self {
        Self {
            key: owned_bytes(key),
            start,
            end,
        }
    }
}

impl Command for ArGetRange {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ARGETRANGE"),
            bulk(&self.key),
            bulk(self.start.to_string()),
            bulk(self.end.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_optional_values(frame)
    }

    fn name(&self) -> &str {
        "ARGETRANGE"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ARGREP key start end predicate \[predicate ...\] \[options ...\]
///
/// Searches populated array elements with one or more textual predicates.
/// Redis defaults to `OR`, an unlimited result count, and case-sensitive
/// matching. [`ArGrep::with_values`] changes the response variant from
/// [`ArGrepResult::Indices`] to [`ArGrepResult::Entries`].
#[derive(Debug, Clone)]
pub struct ArGrep {
    key: Bytes,
    start: ArGrepBound,
    end: ArGrepBound,
    predicates: Vec<ArGrepPredicate>,
    combinator: Option<ArGrepCombinator>,
    limit: Option<u64>,
    with_values: bool,
    nocase: bool,
}

impl ArGrep {
    /// Construct an `ARGREP` command with its first required predicate.
    pub fn new(
        key: impl AsRef<[u8]>,
        start: impl Into<ArGrepBound>,
        end: impl Into<ArGrepBound>,
        predicate: ArGrepPredicate,
    ) -> Self {
        Self {
            key: owned_bytes(key),
            start: start.into(),
            end: end.into(),
            predicates: vec![predicate],
            combinator: None,
            limit: None,
            with_values: false,
            nocase: false,
        }
    }

    /// Add another predicate. Redis 8.8 accepts at most 250 predicates.
    pub fn predicate(mut self, predicate: ArGrepPredicate) -> Self {
        self.predicates.push(predicate);
        self
    }

    /// Combine all predicates with logical AND.
    pub fn and(mut self) -> Self {
        self.combinator = Some(ArGrepCombinator::And);
        self
    }

    /// Combine all predicates with logical OR.
    pub fn or(mut self) -> Self {
        self.combinator = Some(ArGrepCombinator::Or);
        self
    }

    /// Stop after `limit` matches. Redis requires a positive value.
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Return nested index-value entries instead of indexes alone.
    pub fn with_values(mut self) -> Self {
        self.with_values = true;
        self
    }

    /// Enable ASCII case-insensitive matching for all predicates.
    pub fn nocase(mut self) -> Self {
        self.nocase = true;
        self
    }
}

impl Command for ArGrep {
    type Response = ArGrepResult;

    fn to_frame(&self) -> Frame {
        let mut args = Vec::with_capacity(4 + self.predicates.len() * 2 + 4);
        args.push(bulk("ARGREP"));
        args.push(bulk(&self.key));
        self.start.append_to(&mut args);
        self.end.append_to(&mut args);
        for predicate in &self.predicates {
            predicate.append_to(&mut args);
        }
        match self.combinator {
            Some(ArGrepCombinator::And) => args.push(bulk("AND")),
            Some(ArGrepCombinator::Or) => args.push(bulk("OR")),
            None => {}
        }
        if let Some(limit) = self.limit {
            args.push(bulk("LIMIT"));
            args.push(bulk(limit.to_string()));
        }
        if self.with_values {
            args.push(bulk("WITHVALUES"));
        }
        if self.nocase {
            args.push(bulk("NOCASE"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        if self.with_values {
            Vec::<ArrayEntry>::from_frame(frame).map(ArGrepResult::Entries)
        } else {
            Vec::<u64>::from_frame(frame).map(ArGrepResult::Indices)
        }
    }

    fn name(&self) -> &str {
        "ARGREP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ARINFO key \[FULL\]
///
/// Returns typed array metadata. RESP2's alternating key-value array and
/// RESP3's map reply are normalized into [`ArInfoResponse`].
#[derive(Debug, Clone)]
pub struct ArInfo {
    key: Bytes,
    full: bool,
}

impl ArInfo {
    /// Construct a base `ARINFO` command.
    pub fn new(key: impl AsRef<[u8]>) -> Self {
        Self {
            key: owned_bytes(key),
            full: false,
        }
    }

    /// Request the five additional per-encoding slice statistics.
    pub fn full(mut self) -> Self {
        self.full = true;
        self
    }
}

impl Command for ArInfo {
    type Response = ArInfoResponse;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("ARINFO"), bulk(&self.key)];
        if self.full {
            args.push(bulk("FULL"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_info_map(frame, self.full)
    }

    fn name(&self) -> &str {
        "ARINFO"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ARINSERT key value \[value ...\]
///
/// Inserts values at consecutive cursor positions and returns the last index
/// written. Requiring the first value in [`ArInsert::new`] prevents an invalid
/// empty variadic list.
#[derive(Debug, Clone)]
pub struct ArInsert {
    key: Bytes,
    values: Vec<Bytes>,
}

impl ArInsert {
    /// Construct an `ARINSERT` command with its first required value.
    pub fn new(key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Self {
        Self {
            key: owned_bytes(key),
            values: vec![owned_bytes(value)],
        }
    }

    /// Add another value to the same consecutive insertion batch.
    pub fn value(mut self, value: impl AsRef<[u8]>) -> Self {
        self.values.push(owned_bytes(value));
        self
    }
}

impl Command for ArInsert {
    type Response = u64;

    fn to_frame(&self) -> Frame {
        let mut args = Vec::with_capacity(2 + self.values.len());
        args.push(bulk("ARINSERT"));
        args.push(bulk(&self.key));
        args.extend(self.values.iter().map(bulk));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_u64(frame)
    }

    fn name(&self) -> &str {
        "ARINSERT"
    }
}

/// ARLASTITEMS key count \[REV\]
///
/// Returns up to `count` cursor-relative positions. The default order is
/// chronological; [`ArLastItems::rev`] requests newest-first order. Holes are
/// preserved as `None`.
#[derive(Debug, Clone)]
pub struct ArLastItems {
    key: Bytes,
    count: u64,
    rev: bool,
}

impl ArLastItems {
    /// Construct an `ARLASTITEMS` command.
    pub fn new(key: impl AsRef<[u8]>, count: u64) -> Self {
        Self {
            key: owned_bytes(key),
            count,
            rev: false,
        }
    }

    /// Return positions in reverse chronological order (newest first).
    pub fn rev(mut self) -> Self {
        self.rev = true;
        self
    }
}

impl Command for ArLastItems {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("ARLASTITEMS"),
            bulk(&self.key),
            bulk(self.count.to_string()),
        ];
        if self.rev {
            args.push(bulk("REV"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_optional_values(frame)
    }

    fn name(&self) -> &str {
        "ARLASTITEMS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ARLEN key
///
/// Returns `maximum populated index + 1`, or zero when `key` is absent.
#[derive(Debug, Clone)]
pub struct ArLen {
    key: Bytes,
}

impl ArLen {
    /// Construct an `ARLEN` command.
    pub fn new(key: impl AsRef<[u8]>) -> Self {
        Self {
            key: owned_bytes(key),
        }
    }
}

impl Command for ArLen {
    type Response = u64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("ARLEN"), bulk(&self.key)])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_u64(frame)
    }

    fn name(&self) -> &str {
        "ARLEN"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ARMGET key index \[index ...\]
///
/// Returns one binary-safe optional value for every requested index.
#[derive(Debug, Clone)]
pub struct ArMGet {
    key: Bytes,
    indices: Vec<u64>,
}

impl ArMGet {
    /// Construct an `ARMGET` command with its first required index.
    pub fn new(key: impl AsRef<[u8]>, index: u64) -> Self {
        Self {
            key: owned_bytes(key),
            indices: vec![index],
        }
    }

    /// Add another index to fetch.
    pub fn index(mut self, index: u64) -> Self {
        self.indices.push(index);
        self
    }
}

impl Command for ArMGet {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        let mut args = Vec::with_capacity(2 + self.indices.len());
        args.push(bulk("ARMGET"));
        args.push(bulk(&self.key));
        args.extend(self.indices.iter().map(|index| bulk(index.to_string())));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_optional_values(frame)
    }

    fn name(&self) -> &str {
        "ARMGET"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ARMSET key index value \[index value ...\]
///
/// Sets scattered index-value pairs and returns the number of previously empty
/// slots filled.
#[derive(Debug, Clone)]
pub struct ArMSet {
    key: Bytes,
    entries: Vec<(u64, Bytes)>,
}

impl ArMSet {
    /// Construct an `ARMSET` command with its first required pair.
    pub fn new(key: impl AsRef<[u8]>, index: u64, value: impl AsRef<[u8]>) -> Self {
        Self {
            key: owned_bytes(key),
            entries: vec![(index, owned_bytes(value))],
        }
    }

    /// Add another scattered index-value pair.
    pub fn pair(mut self, index: u64, value: impl AsRef<[u8]>) -> Self {
        self.entries.push((index, owned_bytes(value)));
        self
    }
}

impl Command for ArMSet {
    type Response = u64;

    fn to_frame(&self) -> Frame {
        let mut args = Vec::with_capacity(2 + self.entries.len() * 2);
        args.push(bulk("ARMSET"));
        args.push(bulk(&self.key));
        for (index, value) in &self.entries {
            args.push(bulk(index.to_string()));
            args.push(bulk(value));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_u64(frame)
    }

    fn name(&self) -> &str {
        "ARMSET"
    }
}

/// ARNEXT key
///
/// Returns the next index `ARINSERT` or `ARRING` would use. A missing key or a
/// never-used cursor returns `Some(0)`; `None` represents an exhausted cursor.
#[derive(Debug, Clone)]
pub struct ArNext {
    key: Bytes,
}

impl ArNext {
    /// Construct an `ARNEXT` command.
    pub fn new(key: impl AsRef<[u8]>) -> Self {
        Self {
            key: owned_bytes(key),
        }
    }
}

impl Command for ArNext {
    type Response = Option<u64>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("ARNEXT"), bulk(&self.key)])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Option::<u64>::from_frame(frame)
    }

    fn name(&self) -> &str {
        "ARNEXT"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// AROP key start end operation
///
/// Aggregates populated elements in an inclusive range. The operation enum
/// makes the optional `MATCH value` argument structurally valid, while
/// [`ArOpResult`] preserves Redis's operation-dependent reply type.
#[derive(Debug, Clone)]
pub struct ArOp {
    key: Bytes,
    start: u64,
    end: u64,
    operation: ArOpOperation,
}

impl ArOp {
    /// Construct an `AROP` command.
    pub fn new(key: impl AsRef<[u8]>, start: u64, end: u64, operation: ArOpOperation) -> Self {
        Self {
            key: owned_bytes(key),
            start,
            end,
            operation,
        }
    }
}

impl Command for ArOp {
    type Response = ArOpResult;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("AROP"),
            bulk(&self.key),
            bulk(self.start.to_string()),
            bulk(self.end.to_string()),
        ];
        self.operation.append_to(&mut args);
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match &self.operation {
            ArOpOperation::Sum | ArOpOperation::Min | ArOpOperation::Max => {
                parse_optional_bytes(frame).map(ArOpResult::Value)
            }
            ArOpOperation::And | ArOpOperation::Or | ArOpOperation::Xor => {
                Option::<i64>::from_frame(frame).map(ArOpResult::Integer)
            }
            ArOpOperation::Match(_) | ArOpOperation::Used => {
                parse_i64(frame).map(|value| ArOpResult::Integer(Some(value)))
            }
        }
    }

    fn name(&self) -> &str {
        "AROP"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ARRING key size value \[value ...\]
///
/// Inserts values into a ring window and returns the last index written.
/// Redis requires `size` to be positive. Requiring the first value in
/// [`ArRing::new`] prevents an invalid empty variadic list.
#[derive(Debug, Clone)]
pub struct ArRing {
    key: Bytes,
    size: u64,
    values: Vec<Bytes>,
}

impl ArRing {
    /// Construct an `ARRING` command with its first required value.
    pub fn new(key: impl AsRef<[u8]>, size: u64, value: impl AsRef<[u8]>) -> Self {
        Self {
            key: owned_bytes(key),
            size,
            values: vec![owned_bytes(value)],
        }
    }

    /// Add another value to the same ring insertion batch.
    pub fn value(mut self, value: impl AsRef<[u8]>) -> Self {
        self.values.push(owned_bytes(value));
        self
    }
}

impl Command for ArRing {
    type Response = u64;

    fn to_frame(&self) -> Frame {
        let mut args = Vec::with_capacity(3 + self.values.len());
        args.push(bulk("ARRING"));
        args.push(bulk(&self.key));
        args.push(bulk(self.size.to_string()));
        args.extend(self.values.iter().map(bulk));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_u64(frame)
    }

    fn name(&self) -> &str {
        "ARRING"
    }
}

/// ARSCAN key start end \[LIMIT limit\]
///
/// Returns populated elements as nested index-value entries, skipping holes.
/// If `start > end`, entries are returned in descending index order.
#[derive(Debug, Clone)]
pub struct ArScan {
    key: Bytes,
    start: u64,
    end: u64,
    limit: Option<u64>,
}

impl ArScan {
    /// Construct an `ARSCAN` command.
    pub fn new(key: impl AsRef<[u8]>, start: u64, end: u64) -> Self {
        Self {
            key: owned_bytes(key),
            start,
            end,
            limit: None,
        }
    }

    /// Limit the number of returned entries. Redis requires a positive value.
    pub fn limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }
}

impl Command for ArScan {
    type Response = Vec<ArrayEntry>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("ARSCAN"),
            bulk(&self.key),
            bulk(self.start.to_string()),
            bulk(self.end.to_string()),
        ];
        if let Some(limit) = self.limit {
            args.push(bulk("LIMIT"));
            args.push(bulk(limit.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Vec::<ArrayEntry>::from_frame(frame)
    }

    fn name(&self) -> &str {
        "ARSCAN"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// ARSEEK key index
///
/// Sets the next index used by `ARINSERT` and `ARRING`. Returns `true` when the
/// cursor was updated and `false` when `key` does not exist. Unlike other array
/// commands, Redis permits `u64::MAX` here to create a terminal cursor.
#[derive(Debug, Clone)]
pub struct ArSeek {
    key: Bytes,
    index: u64,
}

impl ArSeek {
    /// Construct an `ARSEEK` command.
    pub fn new(key: impl AsRef<[u8]>, index: u64) -> Self {
        Self {
            key: owned_bytes(key),
            index,
        }
    }
}

impl Command for ArSeek {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("ARSEEK"),
            bulk(&self.key),
            bulk(self.index.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_bool(frame)
    }

    fn name(&self) -> &str {
        "ARSEEK"
    }
}

/// ARSET key index value \[value ...\]
///
/// Sets contiguous values beginning at `index` and returns the number of
/// previously empty slots filled.
#[derive(Debug, Clone)]
pub struct ArSet {
    key: Bytes,
    index: u64,
    values: Vec<Bytes>,
}

impl ArSet {
    /// Construct an `ARSET` command with its first required value.
    pub fn new(key: impl AsRef<[u8]>, index: u64, value: impl AsRef<[u8]>) -> Self {
        Self {
            key: owned_bytes(key),
            index,
            values: vec![owned_bytes(value)],
        }
    }

    /// Add another contiguous value.
    pub fn value(mut self, value: impl AsRef<[u8]>) -> Self {
        self.values.push(owned_bytes(value));
        self
    }
}

impl Command for ArSet {
    type Response = u64;

    fn to_frame(&self) -> Frame {
        let mut args = Vec::with_capacity(3 + self.values.len());
        args.push(bulk("ARSET"));
        args.push(bulk(&self.key));
        args.push(bulk(self.index.to_string()));
        args.extend(self.values.iter().map(bulk));
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_u64(frame)
    }

    fn name(&self) -> &str {
        "ARSET"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX_ARRAY_INDEX: u64 = u64::MAX - 1;
    const MAX_ARRAY_INDEX_TEXT: &[u8] = b"18446744073709551614";

    fn command_args<C: Command>(command: &C) -> Vec<Bytes> {
        match command.to_frame() {
            Frame::Array(Some(items)) => items
                .into_iter()
                .map(Bytes::from_frame)
                .collect::<Result<_, _>>()
                .expect("command arguments should be bulk strings"),
            other => panic!("expected command array, got {other:?}"),
        }
    }

    fn assert_args<C: Command>(command: &C, expected: &[&[u8]]) {
        assert_eq!(
            command_args(command),
            expected
                .iter()
                .map(|value| Bytes::copy_from_slice(value))
                .collect::<Vec<_>>()
        );
    }

    fn base_info_pairs() -> Vec<(Frame, Frame)> {
        vec![
            (bulk("count"), Frame::Integer(3)),
            (bulk("len"), Frame::Integer(11)),
            (bulk("next-insert-index"), Frame::Integer(4)),
            (bulk("slices"), Frame::Integer(2)),
            (bulk("directory-size"), Frame::Integer(8)),
            (bulk("super-dir-entries"), Frame::Integer(0)),
            (bulk("slice-size"), Frame::Integer(64)),
        ]
    }

    fn flatten_pairs(pairs: Vec<(Frame, Frame)>) -> Frame {
        array(
            pairs
                .into_iter()
                .flat_map(|(key, value)| [key, value])
                .collect(),
        )
    }

    #[test]
    fn arcount_and_arlen_frames_and_unsigned_responses() {
        let count = ArCount::new("array");
        let len = ArLen::new("array");
        assert_args(&count, &[b"ARCOUNT", b"array"]);
        assert_args(&len, &[b"ARLEN", b"array"]);
        assert_eq!(count.name(), "ARCOUNT");
        assert_eq!(len.name(), "ARLEN");

        assert_eq!(count.parse_response(Frame::Integer(7)).unwrap(), 7);
        assert_eq!(
            count
                .parse_response(Frame::BulkString(Some(Bytes::from_static(
                    MAX_ARRAY_INDEX_TEXT,
                ))))
                .unwrap(),
            MAX_ARRAY_INDEX
        );
        assert_eq!(
            len.parse_response(Frame::BigNumber(Bytes::from_static(MAX_ARRAY_INDEX_TEXT,)))
                .unwrap(),
            MAX_ARRAY_INDEX
        );
        assert!(count.parse_response(Frame::Integer(-1)).is_err());
    }

    #[test]
    fn ardel_builds_nonempty_index_list() {
        let command = ArDel::new("array", 1).index(MAX_ARRAY_INDEX);
        assert_args(&command, &[b"ARDEL", b"array", b"1", MAX_ARRAY_INDEX_TEXT]);
        assert_eq!(command.name(), "ARDEL");
        assert_eq!(command.parse_response(Frame::Integer(2)).unwrap(), 2);
        assert!(!command.idempotent());
    }

    #[test]
    fn ardelrange_builds_complete_range_pairs() {
        let command = ArDelRange::new("array", 9, 3).range(20, 25);
        assert_args(
            &command,
            &[b"ARDELRANGE", b"array", b"9", b"3", b"20", b"25"],
        );
        assert_eq!(command.name(), "ARDELRANGE");
        assert_eq!(
            command
                .parse_response(Frame::BulkString(Some(Bytes::from_static(b"4"))))
                .unwrap(),
            4
        );
    }

    #[test]
    fn arget_is_binary_safe_and_normalizes_resp2_and_resp3_nulls() {
        let key = String::from("array");
        let command = ArGet::new(&key, 4);
        assert_args(&command, &[b"ARGET", b"array", b"4"]);
        let value = Bytes::from_static(b"\0binary\xff");
        assert_eq!(
            command
                .parse_response(Frame::BulkString(Some(value.clone())))
                .unwrap(),
            Some(value)
        );
        assert_eq!(
            command.parse_response(Frame::BulkString(None)).unwrap(),
            None
        );
        assert_eq!(command.parse_response(Frame::Null).unwrap(), None);
        assert!(command.idempotent());
    }

    #[test]
    fn argetrange_preserves_holes() {
        let command = ArGetRange::new("array", 5, 2);
        assert_args(&command, &[b"ARGETRANGE", b"array", b"5", b"2"]);
        let response = array(vec![
            bulk("five"),
            Frame::BulkString(None),
            Frame::Null,
            bulk("two"),
        ]);
        assert_eq!(
            command.parse_response(response).unwrap(),
            vec![
                Some(Bytes::from_static(b"five")),
                None,
                None,
                Some(Bytes::from_static(b"two")),
            ]
        );
        assert!(command.idempotent());
    }

    #[test]
    fn argrep_builds_all_predicates_bounds_and_options() {
        let command = ArGrep::new(
            "array",
            ArGrepBound::Start,
            ArGrepBound::End,
            ArGrepPredicate::exact("one"),
        )
        .predicate(ArGrepPredicate::matches("on"))
        .predicate(ArGrepPredicate::glob("o*"))
        .predicate(ArGrepPredicate::regex("^o"))
        .and()
        .limit(2)
        .nocase();
        assert_args(
            &command,
            &[
                b"ARGREP", b"array", b"-", b"+", b"EXACT", b"one", b"MATCH", b"on", b"GLOB", b"o*",
                b"RE", b"^o", b"AND", b"LIMIT", b"2", b"NOCASE",
            ],
        );
        assert_eq!(command.name(), "ARGREP");
        assert_eq!(
            command
                .parse_response(array(vec![
                    Frame::Integer(1),
                    Frame::BigNumber(Bytes::from_static(MAX_ARRAY_INDEX_TEXT)),
                ]))
                .unwrap(),
            ArGrepResult::Indices(vec![1, MAX_ARRAY_INDEX])
        );
        assert!(command.idempotent());
    }

    #[test]
    fn argrep_with_values_parses_nested_binary_entries() {
        let binary = Bytes::from_static(b"\0value\xff");
        let command = ArGrep::new(
            "array",
            0,
            MAX_ARRAY_INDEX,
            ArGrepPredicate::matches(binary.clone()),
        )
        .or()
        .with_values();
        assert_args(
            &command,
            &[
                b"ARGREP",
                b"array",
                b"0",
                MAX_ARRAY_INDEX_TEXT,
                b"MATCH",
                b"\0value\xff",
                b"OR",
                b"WITHVALUES",
            ],
        );
        let response = array(vec![array(vec![
            Frame::BulkString(Some(Bytes::from_static(MAX_ARRAY_INDEX_TEXT))),
            Frame::BulkString(Some(binary.clone())),
        ])]);
        assert_eq!(
            command.parse_response(response).unwrap(),
            ArGrepResult::Entries(vec![ArrayEntry {
                index: MAX_ARRAY_INDEX,
                value: binary,
            }])
        );
        assert!(
            command
                .parse_response(array(vec![Frame::Integer(1), bulk("value")]))
                .is_err()
        );
    }

    #[test]
    fn arinfo_parses_resp2_base_fields() {
        let command = ArInfo::new("array");
        assert_args(&command, &[b"ARINFO", b"array"]);
        let info = command
            .parse_response(flatten_pairs(base_info_pairs()))
            .unwrap();
        assert_eq!(
            info,
            ArInfoResponse {
                count: 3,
                len: 11,
                next_insert_index: 4,
                slices: 2,
                directory_size: 8,
                super_directory_entries: 0,
                slice_size: 64,
                full: None,
            }
        );
        assert!(command.idempotent());
    }

    #[test]
    fn arinfo_parses_resp3_full_fields_and_resp2_doubles() {
        let command = ArInfo::new("array").full();
        assert_args(&command, &[b"ARINFO", b"array", b"FULL"]);
        let mut pairs = base_info_pairs();
        pairs.extend([
            (bulk("dense-slices"), Frame::Integer(1)),
            (bulk("sparse-slices"), Frame::Integer(1)),
            (bulk("avg-dense-size"), Frame::Double(32.0)),
            (
                bulk("avg-dense-fill"),
                Frame::BulkString(Some(Bytes::from_static(b"0.75"))),
            ),
            (bulk("avg-sparse-size"), Frame::Double(8.5)),
        ]);
        let info = command.parse_response(Frame::Map(pairs)).unwrap();
        assert_eq!(
            info.full,
            Some(ArInfoFull {
                dense_slices: 1,
                sparse_slices: 1,
                average_dense_size: 32.0,
                average_dense_fill: 0.75,
                average_sparse_size: 8.5,
            })
        );

        let mut resp2_pairs = base_info_pairs();
        resp2_pairs.extend([
            (bulk("dense-slices"), Frame::Integer(0)),
            (bulk("sparse-slices"), Frame::Integer(2)),
            (bulk("avg-dense-size"), bulk("0")),
            (bulk("avg-dense-fill"), bulk("0")),
            (bulk("avg-sparse-size"), bulk("4.25")),
        ]);
        let resp2 = command.parse_response(flatten_pairs(resp2_pairs)).unwrap();
        assert_eq!(resp2.full.unwrap().average_sparse_size, 4.25);
    }

    #[test]
    fn arinfo_rejects_malformed_or_incomplete_maps() {
        let command = ArInfo::new("array");
        assert!(command.parse_response(array(vec![bulk("count")])).is_err());
        assert!(
            command
                .parse_response(Frame::Map(vec![(bulk("count"), Frame::Integer(1))]))
                .is_err()
        );
        assert!(
            ArInfo::new("array")
                .full()
                .parse_response(Frame::Map(base_info_pairs()))
                .is_err()
        );
    }

    #[test]
    fn arinsert_builds_binary_batch_and_parses_high_index() {
        let command = ArInsert::new("array", b"first".as_slice()).value([0, 255]);
        assert_args(&command, &[b"ARINSERT", b"array", b"first", b"\0\xff"]);
        assert_eq!(command.name(), "ARINSERT");
        assert_eq!(
            command
                .parse_response(Frame::BigNumber(Bytes::from_static(MAX_ARRAY_INDEX_TEXT,)))
                .unwrap(),
            MAX_ARRAY_INDEX
        );
    }

    #[test]
    fn arlastitems_builds_rev_and_preserves_holes() {
        let command = ArLastItems::new("array", 3).rev();
        assert_args(&command, &[b"ARLASTITEMS", b"array", b"3", b"REV"]);
        assert_eq!(
            command
                .parse_response(array(vec![bulk("new"), Frame::Null, bulk("old")]))
                .unwrap(),
            vec![
                Some(Bytes::from_static(b"new")),
                None,
                Some(Bytes::from_static(b"old")),
            ]
        );
        assert!(command.idempotent());
    }

    #[test]
    fn armget_builds_indices_and_preserves_nulls() {
        let command = ArMGet::new("array", 0).index(8);
        assert_args(&command, &[b"ARMGET", b"array", b"0", b"8"]);
        assert_eq!(
            command
                .parse_response(array(vec![bulk("zero"), Frame::BulkString(None)]))
                .unwrap(),
            vec![Some(Bytes::from_static(b"zero")), None]
        );
        assert!(command.idempotent());
    }

    #[test]
    fn armset_builds_binary_pairs() {
        let dynamic_key = String::from("array");
        let command = ArMSet::new(&dynamic_key, 0, "zero").pair(8, [0, 255]);
        assert_args(
            &command,
            &[b"ARMSET", b"array", b"0", b"zero", b"8", b"\0\xff"],
        );
        assert_eq!(command.name(), "ARMSET");
        assert_eq!(command.parse_response(Frame::Integer(2)).unwrap(), 2);
    }

    #[test]
    fn arnext_distinguishes_zero_terminal_null_and_high_index() {
        let command = ArNext::new("array");
        assert_args(&command, &[b"ARNEXT", b"array"]);
        assert_eq!(command.parse_response(Frame::Integer(0)).unwrap(), Some(0));
        assert_eq!(
            command.parse_response(Frame::BulkString(None)).unwrap(),
            None
        );
        assert_eq!(command.parse_response(Frame::Null).unwrap(), None);
        assert_eq!(
            command
                .parse_response(Frame::BigNumber(Bytes::from_static(MAX_ARRAY_INDEX_TEXT,)))
                .unwrap(),
            Some(MAX_ARRAY_INDEX)
        );
        assert!(command.idempotent());
    }

    #[test]
    fn arop_builds_every_operation_variant() {
        let operations = [
            (ArOpOperation::Sum, b"SUM".as_slice()),
            (ArOpOperation::Min, b"MIN".as_slice()),
            (ArOpOperation::Max, b"MAX".as_slice()),
            (ArOpOperation::And, b"AND".as_slice()),
            (ArOpOperation::Or, b"OR".as_slice()),
            (ArOpOperation::Xor, b"XOR".as_slice()),
            (ArOpOperation::Used, b"USED".as_slice()),
        ];
        for (operation, token) in operations {
            let command = ArOp::new("array", 1, 9, operation);
            assert_args(&command, &[b"AROP", b"array", b"1", b"9", token]);
            assert_eq!(command.name(), "AROP");
        }
        let match_command = ArOp::new(
            "array",
            1,
            9,
            ArOpOperation::matches(Bytes::from_static(b"\0\xff")),
        );
        assert_args(
            &match_command,
            &[b"AROP", b"array", b"1", b"9", b"MATCH", b"\0\xff"],
        );
    }

    #[test]
    fn arop_parses_value_integer_and_null_branches() {
        let sum = ArOp::new("array", 0, 9, ArOpOperation::Sum);
        assert_eq!(
            sum.parse_response(bulk("12.5")).unwrap(),
            ArOpResult::Value(Some(Bytes::from_static(b"12.5")))
        );
        assert_eq!(
            sum.parse_response(Frame::Null).unwrap(),
            ArOpResult::Value(None)
        );

        let and = ArOp::new("array", 0, 9, ArOpOperation::And);
        assert_eq!(
            and.parse_response(Frame::Integer(-4)).unwrap(),
            ArOpResult::Integer(Some(-4))
        );
        assert_eq!(
            and.parse_response(Frame::BulkString(None)).unwrap(),
            ArOpResult::Integer(None)
        );

        let used = ArOp::new("array", 0, 9, ArOpOperation::Used);
        assert_eq!(
            used.parse_response(Frame::Integer(3)).unwrap(),
            ArOpResult::Integer(Some(3))
        );
        assert!(used.parse_response(Frame::Null).is_err());
        assert!(used.idempotent());
    }

    #[test]
    fn arring_builds_nonempty_batch() {
        let command = ArRing::new("ring", 3, "one").value("two");
        assert_args(&command, &[b"ARRING", b"ring", b"3", b"one", b"two"]);
        assert_eq!(command.name(), "ARRING");
        assert_eq!(command.parse_response(Frame::Integer(1)).unwrap(), 1);
        assert!(!command.idempotent());
    }

    #[test]
    fn arscan_builds_limit_and_parses_nested_entries() {
        let command = ArScan::new("array", 9, 0).limit(2);
        assert_args(&command, &[b"ARSCAN", b"array", b"9", b"0", b"LIMIT", b"2"]);
        let binary = Bytes::from_static(b"\0\xff");
        let response = array(vec![
            array(vec![Frame::Integer(9), bulk("nine")]),
            array(vec![
                Frame::BigNumber(Bytes::from_static(MAX_ARRAY_INDEX_TEXT)),
                Frame::BulkString(Some(binary.clone())),
            ]),
        ]);
        assert_eq!(
            command.parse_response(response).unwrap(),
            vec![
                ArrayEntry {
                    index: 9,
                    value: Bytes::from_static(b"nine"),
                },
                ArrayEntry {
                    index: MAX_ARRAY_INDEX,
                    value: binary,
                },
            ]
        );
        assert!(
            command
                .parse_response(array(vec![Frame::Integer(1), bulk("flat")]))
                .is_err()
        );
        assert!(command.idempotent());
    }

    #[test]
    fn arseek_accepts_terminal_index_and_boolean_protocol_forms() {
        let command = ArSeek::new("array", u64::MAX);
        assert_args(&command, &[b"ARSEEK", b"array", b"18446744073709551615"]);
        assert_eq!(command.name(), "ARSEEK");
        assert!(command.parse_response(Frame::Integer(1)).unwrap());
        assert!(!command.parse_response(Frame::Boolean(false)).unwrap());
        assert!(command.parse_response(Frame::Integer(2)).is_err());
    }

    #[test]
    fn arset_builds_contiguous_binary_values() {
        let command = ArSet::new("array", 4, "four").value([0, 255]);
        assert_args(&command, &[b"ARSET", b"array", b"4", b"four", b"\0\xff"]);
        assert_eq!(command.name(), "ARSET");
        assert_eq!(command.parse_response(Frame::Integer(2)).unwrap(), 2);
        assert!(!command.idempotent());
    }

    #[test]
    fn exactly_the_read_commands_are_idempotent() {
        assert!(ArCount::new("k").idempotent());
        assert!(ArGet::new("k", 0).idempotent());
        assert!(ArGetRange::new("k", 0, 1).idempotent());
        assert!(ArGrep::new("k", 0, 1, ArGrepPredicate::exact("v")).idempotent());
        assert!(ArInfo::new("k").idempotent());
        assert!(ArLastItems::new("k", 1).idempotent());
        assert!(ArLen::new("k").idempotent());
        assert!(ArMGet::new("k", 0).idempotent());
        assert!(ArNext::new("k").idempotent());
        assert!(ArOp::new("k", 0, 1, ArOpOperation::Used).idempotent());
        assert!(ArScan::new("k", 0, 1).idempotent());

        assert!(!ArDel::new("k", 0).idempotent());
        assert!(!ArDelRange::new("k", 0, 1).idempotent());
        assert!(!ArInsert::new("k", "v").idempotent());
        assert!(!ArMSet::new("k", 0, "v").idempotent());
        assert!(!ArRing::new("k", 1, "v").idempotent());
        assert!(!ArSeek::new("k", 0).idempotent());
        assert!(!ArSet::new("k", 0, "v").idempotent());
    }
}
