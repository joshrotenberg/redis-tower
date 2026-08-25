use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// Quantization type for VADD.
#[derive(Clone)]
pub enum VQuantization {
    /// 8-bit quantization.
    Q8,
    /// 16-bit brain float quantization.
    Bf16,
    /// No quantization.
    NoQuant,
}

/// VADD key (FP32 vector | VALUES num val ...) element \[REDUCE dim\] \[CAS\]
/// \[M cap\] \[EF build\] \[SETATTR json\] \[QUANT Q8|BF16|NOQUANT\]
///
/// Adds an element with its vector to the vector set at `key`. Returns `true`
/// if the element was added, `false` if it already existed (and was updated).
#[derive(Clone)]
pub struct VAdd {
    key: String,
    vector: Vec<f32>,
    element: String,
    reduce: Option<u64>,
    cas: bool,
    m: Option<u64>,
    ef: Option<u64>,
    setattr: Option<String>,
    quant: Option<VQuantization>,
}

impl VAdd {
    /// Create a new [`VAdd`] command.
    pub fn new(
        key: impl Into<String>,
        vector: impl Into<Vec<f32>>,
        element: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            vector: vector.into(),
            element: element.into(),
            reduce: None,
            cas: false,
            m: None,
            ef: None,
            setattr: None,
            quant: None,
        }
    }

    /// Reduce the vector to `dim` dimensions.
    pub fn reduce(mut self, dim: u64) -> Self {
        self.reduce = Some(dim);
        self
    }

    /// Enable check-and-set semantics.
    pub fn cas(mut self) -> Self {
        self.cas = true;
        self
    }

    /// Set the maximum number of links per node.
    pub fn m(mut self, cap: u64) -> Self {
        self.m = Some(cap);
        self
    }

    /// Set the EF construction parameter.
    pub fn ef(mut self, build: u64) -> Self {
        self.ef = Some(build);
        self
    }

    /// Set a JSON attribute on the element.
    pub fn setattr(mut self, json: impl Into<String>) -> Self {
        self.setattr = Some(json.into());
        self
    }

    /// Set the quantization type.
    pub fn quant(mut self, q: VQuantization) -> Self {
        self.quant = Some(q);
        self
    }
}

impl Command for VAdd {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("VADD"),
            bulk(self.key.as_str()),
            bulk("VALUES"),
            bulk(self.vector.len().to_string()),
        ];
        for v in &self.vector {
            args.push(bulk(v.to_string()));
        }
        args.push(bulk(self.element.as_str()));

        if let Some(dim) = self.reduce {
            args.push(bulk("REDUCE"));
            args.push(bulk(dim.to_string()));
        }
        if self.cas {
            args.push(bulk("CAS"));
        }
        if let Some(cap) = self.m {
            args.push(bulk("M"));
            args.push(bulk(cap.to_string()));
        }
        if let Some(build) = self.ef {
            args.push(bulk("EF"));
            args.push(bulk(build.to_string()));
        }
        if let Some(ref json) = self.setattr {
            args.push(bulk("SETATTR"));
            args.push(bulk(json.as_str()));
        }
        match &self.quant {
            Some(VQuantization::Q8) => args.push(bulk("Q8")),
            Some(VQuantization::Bf16) => args.push(bulk("BF16")),
            Some(VQuantization::NoQuant) => args.push(bulk("NOQUANT")),
            None => {}
        }

        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),
            Frame::Integer(0) => Ok(false),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer 0 or 1",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "VADD"
    }
}

/// VREM key element
///
/// Removes an element from the vector set at `key`. Returns `true` if the
/// element was removed, `false` if it did not exist.
#[derive(Clone)]
pub struct VRem {
    key: String,
    element: String,
}

impl VRem {
    /// Create a new [`VRem`] command.
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
        }
    }
}

impl Command for VRem {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("VREM"),
            bulk(self.key.as_str()),
            bulk(self.element.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),
            Frame::Integer(0) => Ok(false),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer 0 or 1",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "VREM"
    }
}

/// VCARD key
///
/// Returns the number of elements in the vector set at `key`.
#[derive(Clone)]
pub struct VCard {
    key: String,
}

impl VCard {
    /// Create a new [`VCard`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for VCard {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("VCARD"), bulk(self.key.as_str())])
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
        "VCARD"
    }
}

/// VDIM key
///
/// Returns the dimensionality of the vectors in the vector set at `key`.
#[derive(Clone)]
pub struct VDim {
    key: String,
}

impl VDim {
    /// Create a new [`VDim`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for VDim {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("VDIM"), bulk(self.key.as_str())])
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
        "VDIM"
    }
}

/// VISMEMBER key element
///
/// Checks whether `element` exists in the vector set at `key`. Returns `true`
/// when it exists and `false` when either the element or key does not exist.
///
/// Redis returns an integer under RESP2 and a boolean under RESP3; both response
/// shapes are normalized to [`bool`].
#[derive(Clone)]
pub struct VIsMember {
    key: String,
    element: String,
}

impl VIsMember {
    /// Creates a membership check for `element` in the vector set at `key`.
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
        }
    }
}

impl Command for VIsMember {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("VISMEMBER"),
            bulk(self.key.as_str()),
            bulk(self.element.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),
            Frame::Integer(0) => Ok(false),
            Frame::Boolean(value) => Ok(value),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer 0 or 1, or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "VISMEMBER"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// VEMB key element \[RAW\]
///
/// Returns the vector embedding for `element` in the vector set at `key`.
/// Without RAW, returns an array of doubles. With RAW, returns the raw FP32
/// binary blob.
#[derive(Clone)]
pub struct VEmb {
    key: String,
    element: String,
    raw: bool,
}

impl VEmb {
    /// Create a new [`VEmb`] command.
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
            raw: false,
        }
    }

    /// Request the raw FP32 binary blob instead of parsed doubles.
    pub fn raw(mut self) -> Self {
        self.raw = true;
        self
    }
}

impl Command for VEmb {
    type Response = Vec<f64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("VEMB"),
            bulk(self.key.as_str()),
            bulk(self.element.as_str()),
        ];
        if self.raw {
            args.push(bulk("RAW"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(Some(data)) => {
                        let s = String::from_utf8_lossy(&data);
                        s.parse::<f64>()
                            .map_err(|_| RedisError::UnexpectedResponse {
                                expected: "float string",
                                actual: format!("{s}"),
                            })
                    }
                    Frame::Double(d) => Ok(d),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string or double",
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
        "VEMB"
    }
}

/// VSIM key (ELE element | VALUES num val ... | FP32 blob) \[COUNT n\]
/// \[EF n\] \[FILTER expr\] \[FILTER-EF n\] \[WITHSCORES\] \[NOTHREAD\] \[TRUTH\]
///
/// Finds the most similar elements to the given vector or element in the
/// vector set. Returns element names, or (element, score) pairs when
/// WITHSCORES is specified.
#[derive(Clone)]
pub struct VSim {
    key: String,
    target: VSimTarget,
    count: Option<u64>,
    ef: Option<u64>,
    filter: Option<String>,
    filter_ef: Option<u64>,
    withscores: bool,
    nothread: bool,
    truth: bool,
}

/// Target for VSIM: search by existing element name or by vector values.
#[derive(Clone)]
pub enum VSimTarget {
    /// Search by existing element name.
    Element(String),
    /// Search by vector values.
    Values(Vec<f32>),
}

impl VSim {
    /// Search by existing element name.
    pub fn by_element(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            target: VSimTarget::Element(element.into()),
            count: None,
            ef: None,
            filter: None,
            filter_ef: None,
            withscores: false,
            nothread: false,
            truth: false,
        }
    }

    /// Search by vector values.
    pub fn by_values(key: impl Into<String>, vector: impl Into<Vec<f32>>) -> Self {
        Self {
            key: key.into(),
            target: VSimTarget::Values(vector.into()),
            count: None,
            ef: None,
            filter: None,
            filter_ef: None,
            withscores: false,
            nothread: false,
            truth: false,
        }
    }

    /// Limit the number of results.
    pub fn count(mut self, n: u64) -> Self {
        self.count = Some(n);
        self
    }

    /// Set the EF search parameter.
    pub fn ef(mut self, n: u64) -> Self {
        self.ef = Some(n);
        self
    }

    /// Filter results by attribute expression.
    pub fn filter(mut self, expr: impl Into<String>) -> Self {
        self.filter = Some(expr.into());
        self
    }

    /// Set the EF parameter for filtered search.
    pub fn filter_ef(mut self, n: u64) -> Self {
        self.filter_ef = Some(n);
        self
    }

    /// Include similarity scores in the response.
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }

    /// Disable multi-threading for this query.
    pub fn nothread(mut self) -> Self {
        self.nothread = true;
        self
    }

    /// Use brute-force (exact) search instead of approximate.
    pub fn truth(mut self) -> Self {
        self.truth = true;
        self
    }
}

fn parse_vector_result_name(frame: Frame) -> Result<Bytes, RedisError> {
    match frame {
        Frame::BulkString(Some(data)) | Frame::SimpleString(data) => Ok(data),
        other => Err(RedisError::UnexpectedResponse {
            expected: "bulk or simple string",
            actual: format!("{other:?}"),
        }),
    }
}

fn parse_vector_result_score(frame: Frame) -> Result<f64, RedisError> {
    match frame {
        Frame::BulkString(Some(data)) | Frame::SimpleString(data) => {
            let score = String::from_utf8_lossy(&data);
            score
                .parse::<f64>()
                .map_err(|_| RedisError::UnexpectedResponse {
                    expected: "float string",
                    actual: score.into_owned(),
                })
        }
        Frame::Double(score) => Ok(score),
        Frame::Integer(score) => Ok(score as f64),
        other => Err(RedisError::UnexpectedResponse {
            expected: "string, double, or integer score",
            actual: format!("{other:?}"),
        }),
    }
}

fn parse_vector_results(
    frame: Frame,
    withscores: bool,
) -> Result<Vec<(Bytes, Option<f64>)>, RedisError> {
    match frame {
        Frame::Array(Some(frames)) if withscores => {
            if frames.len() % 2 != 0 {
                return Err(RedisError::UnexpectedResponse {
                    expected: "even number of elements for WITHSCORES",
                    actual: format!("got {} elements", frames.len()),
                });
            }
            let mut result = Vec::with_capacity(frames.len() / 2);
            let mut frames = frames.into_iter();
            while let (Some(name), Some(score)) = (frames.next(), frames.next()) {
                result.push((
                    parse_vector_result_name(name)?,
                    Some(parse_vector_result_score(score)?),
                ));
            }
            Ok(result)
        }
        Frame::Array(Some(frames)) => frames
            .into_iter()
            .map(|name| Ok((parse_vector_result_name(name)?, None)))
            .collect(),
        Frame::Map(entries) | Frame::StreamedMap(entries) if withscores => entries
            .into_iter()
            .map(|(name, score)| {
                Ok((
                    parse_vector_result_name(name)?,
                    Some(parse_vector_result_score(score)?),
                ))
            })
            .collect(),
        other => Err(RedisError::UnexpectedResponse {
            expected: if withscores { "array or map" } else { "array" },
            actual: format!("{other:?}"),
        }),
    }
}

impl Command for VSim {
    type Response = Vec<(Bytes, Option<f64>)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("VSIM"), bulk(self.key.as_str())];

        match &self.target {
            VSimTarget::Element(elem) => {
                args.push(bulk("ELE"));
                args.push(bulk(elem.as_str()));
            }
            VSimTarget::Values(vector) => {
                args.push(bulk("VALUES"));
                args.push(bulk(vector.len().to_string()));
                for v in vector {
                    args.push(bulk(v.to_string()));
                }
            }
        }

        if let Some(n) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(n.to_string()));
        }
        if let Some(n) = self.ef {
            args.push(bulk("EF"));
            args.push(bulk(n.to_string()));
        }
        if let Some(ref expr) = self.filter {
            args.push(bulk("FILTER"));
            args.push(bulk(expr.as_str()));
        }
        if let Some(n) = self.filter_ef {
            args.push(bulk("FILTER-EF"));
            args.push(bulk(n.to_string()));
        }
        if self.withscores {
            args.push(bulk("WITHSCORES"));
        }
        if self.nothread {
            args.push(bulk("NOTHREAD"));
        }
        if self.truth {
            args.push(bulk("TRUTH"));
        }

        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_vector_results(frame, self.withscores)
    }

    fn name(&self) -> &str {
        "VSIM"
    }
}

/// VRANDMEMBER key \[COUNT n\]
///
/// Returns one or more random elements from the vector set at `key`.
#[derive(Clone)]
pub struct VRandMember {
    key: String,
    count: Option<i64>,
}

impl VRandMember {
    /// Create a new [`VRandMember`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    /// Return `n` random elements. Negative values allow duplicates.
    pub fn count(mut self, n: i64) -> Self {
        self.count = Some(n);
        self
    }
}

impl Command for VRandMember {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("VRANDMEMBER"), bulk(self.key.as_str())];
        if let Some(n) = self.count {
            args.push(bulk(n.to_string()));
        }
        array(args)
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
            // Without COUNT, Redis returns a single bulk string.
            Frame::BulkString(Some(data)) => Ok(vec![data]),
            Frame::BulkString(None) | Frame::Null => Ok(vec![]),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or bulk string",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "VRANDMEMBER"
    }
}

/// VRANGE key start end \[count\]
///
/// Returns elements from the vector set at `key` in byte-wise lexicographical
/// order. The `start` and `end` arguments use Redis range syntax:
///
/// - `[element` includes the boundary.
/// - `(element` excludes the boundary.
/// - `-` selects the minimum start.
/// - `+` selects the maximum end.
///
/// Use [`VRange::count`] to bound the number of returned elements. Unlike range
/// commands whose limit is introduced by a `COUNT` token, `VRANGE` takes its
/// optional count as a bare positional argument.
#[derive(Clone)]
pub struct VRange {
    key: String,
    start: String,
    end: String,
    count: Option<i64>,
}

impl VRange {
    /// Creates a lexicographical range query for the vector set at `key`.
    pub fn new(key: impl Into<String>, start: impl Into<String>, end: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            start: start.into(),
            end: end.into(),
            count: None,
        }
    }

    /// Limits the number of returned elements.
    ///
    /// A negative count returns every matching element and zero returns an empty
    /// array.
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for VRange {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("VRANGE"),
            bulk(self.key.as_str()),
            bulk(self.start.as_str()),
            bulk(self.end.as_str()),
        ];
        if let Some(count) = self.count {
            args.push(bulk(count.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|frame| match frame {
                    Frame::BulkString(Some(element)) => Ok(element),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string element",
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
        "VRANGE"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// VGETATTR key element
///
/// Returns the JSON attribute string for `element` in the vector set at `key`,
/// or `None` if no attribute is set.
#[derive(Clone)]
pub struct VGetAttr {
    key: String,
    element: String,
}

impl VGetAttr {
    /// Create a new [`VGetAttr`] command.
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
        }
    }
}

impl Command for VGetAttr {
    type Response = Option<String>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("VGETATTR"),
            bulk(self.key.as_str()),
            bulk(self.element.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                let s = String::from_utf8(data.to_vec()).map_err(|_| {
                    RedisError::UnexpectedResponse {
                        expected: "valid UTF-8 string",
                        actual: "invalid UTF-8".to_string(),
                    }
                })?;
                Ok(Some(s))
            }
            Frame::BulkString(None) | Frame::Null => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "VGETATTR"
    }
}

/// VSETATTR key element json
///
/// Sets a JSON attribute on `element` in the vector set at `key`. Returns
/// `true` on success.
#[derive(Clone)]
pub struct VSetAttr {
    key: String,
    element: String,
    json: String,
}

impl VSetAttr {
    /// Create a new [`VSetAttr`] command.
    pub fn new(
        key: impl Into<String>,
        element: impl Into<String>,
        json: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
            json: json.into(),
        }
    }
}

impl Command for VSetAttr {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("VSETATTR"),
            bulk(self.key.as_str()),
            bulk(self.element.as_str()),
            bulk(self.json.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),
            Frame::Integer(0) => Ok(false),
            Frame::Boolean(b) => Ok(b),
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(true),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer 0 or 1, or OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "VSETATTR"
    }
}

/// Delete the attribute from `element` in the vector set at `key`.
///
/// Redis has no `VDELATTR` command; an attribute is cleared by setting it to
/// the empty string, so this builder sends `VSETATTR key element ""`. The
/// ergonomic `VDelAttr` name is kept. Returns `true` if the element exists
/// (its attribute is cleared), `false` if the element is not in the set.
#[derive(Clone)]
pub struct VDelAttr {
    key: String,
    element: String,
}

impl VDelAttr {
    /// Create a new [`VDelAttr`] command.
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
        }
    }
}

impl Command for VDelAttr {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        // No VDELATTR command exists in Redis; clear the attribute by setting
        // it to the empty string via VSETATTR.
        array(vec![
            bulk("VSETATTR"),
            bulk(self.key.as_str()),
            bulk(self.element.as_str()),
            bulk(""),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),
            Frame::Integer(0) => Ok(false),
            Frame::Boolean(b) => Ok(b),
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(true),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer 0 or 1, or OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        // VSETATTR is what actually goes on the wire.
        "VSETATTR"
    }
}

/// VINFO key
///
/// Returns information about the vector set at `key` as a flat array of
/// alternating field names and values.
#[derive(Clone)]
pub struct VInfo {
    key: String,
}

impl VInfo {
    /// Create a new [`VInfo`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for VInfo {
    type Response = Vec<Frame>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("VINFO"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => Ok(frames),
            // RESP3 returns a Map -- flatten key-value pairs into a flat array.
            Frame::Map(pairs) => {
                let mut frames = Vec::with_capacity(pairs.len() * 2);
                for (k, v) in pairs {
                    frames.push(k);
                    frames.push(v);
                }
                Ok(frames)
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "array or map",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "VINFO"
    }
}

/// VLINKS key element \[WITHSCORES\]
///
/// Returns the neighbor links of `element` in the vector set at `key`.
/// With WITHSCORES, returns (element, score) pairs.
#[derive(Clone)]
pub struct VLinks {
    key: String,
    element: String,
    withscores: bool,
}

impl VLinks {
    /// Create a new [`VLinks`] command.
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
            withscores: false,
        }
    }

    /// Include similarity scores in the response.
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }
}

impl Command for VLinks {
    type Response = Vec<(Bytes, Option<f64>)>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("VLINKS"),
            bulk(self.key.as_str()),
            bulk(self.element.as_str()),
        ];
        if self.withscores {
            args.push(bulk("WITHSCORES"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_vector_results(frame, self.withscores)
    }

    fn name(&self) -> &str {
        "VLINKS"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vismember_serializes_key_and_element() {
        let command = VIsMember::new("vectors", "member");

        assert_eq!(
            command.to_frame(),
            array(vec![bulk("VISMEMBER"), bulk("vectors"), bulk("member")])
        );
        assert_eq!(command.name(), "VISMEMBER");
        assert!(command.idempotent());
    }

    #[test]
    fn vismember_parses_resp2_integer_reply() {
        let command = VIsMember::new("vectors", "member");

        assert!(command.parse_response(Frame::Integer(1)).unwrap());
        assert!(!command.parse_response(Frame::Integer(0)).unwrap());
    }

    #[test]
    fn vismember_parses_resp3_boolean_reply() {
        let command = VIsMember::new("vectors", "member");

        assert!(command.parse_response(Frame::Boolean(true)).unwrap());
        assert!(!command.parse_response(Frame::Boolean(false)).unwrap());
    }

    #[test]
    fn vismember_rejects_invalid_reply() {
        let command = VIsMember::new("vectors", "member");

        assert!(command.parse_response(Frame::Integer(2)).is_err());
        assert!(
            command
                .parse_response(Frame::BulkString(Some(Bytes::from_static(b"1"))))
                .is_err()
        );
    }

    #[test]
    fn vector_score_replies_accept_resp2_arrays_and_resp3_maps() {
        let resp2 = Frame::Array(Some(vec![
            bulk("a"),
            bulk("1.0"),
            Frame::SimpleString(Bytes::from_static(b"b")),
            Frame::Double(0.5),
        ]));
        let resp3 = Frame::Map(vec![
            (bulk("a"), Frame::Double(1.0)),
            (
                Frame::SimpleString(Bytes::from_static(b"b")),
                Frame::Integer(1),
            ),
        ]);
        let expected = vec![
            (Bytes::from_static(b"a"), Some(1.0)),
            (Bytes::from_static(b"b"), Some(0.5)),
        ];

        assert_eq!(
            VSim::by_element("vectors", "a")
                .withscores()
                .parse_response(resp2)
                .unwrap(),
            expected
        );
        assert_eq!(
            VSim::by_element("vectors", "a")
                .withscores()
                .parse_response(resp3.clone())
                .unwrap(),
            vec![
                (Bytes::from_static(b"a"), Some(1.0)),
                (Bytes::from_static(b"b"), Some(1.0)),
            ]
        );
        assert_eq!(
            VLinks::new("vectors", "a")
                .withscores()
                .parse_response(resp3)
                .unwrap(),
            vec![
                (Bytes::from_static(b"a"), Some(1.0)),
                (Bytes::from_static(b"b"), Some(1.0)),
            ]
        );
    }

    #[test]
    fn vrange_serializes_without_count() {
        let command = VRange::new("vectors", "[apple", "(pear");

        assert_eq!(
            command.to_frame(),
            array(vec![
                bulk("VRANGE"),
                bulk("vectors"),
                bulk("[apple"),
                bulk("(pear")
            ])
        );
        assert_eq!(command.name(), "VRANGE");
        assert!(command.idempotent());
    }

    #[test]
    fn vrange_serializes_positional_count() {
        for count in [10, 0, -1] {
            let command = VRange::new("vectors", "-", "+").count(count);

            assert_eq!(
                command.to_frame(),
                array(vec![
                    bulk("VRANGE"),
                    bulk("vectors"),
                    bulk("-"),
                    bulk("+"),
                    bulk(count.to_string())
                ])
            );
        }
    }

    #[test]
    fn vrange_parses_ordered_binary_safe_elements() {
        let command = VRange::new("vectors", "-", "+");
        let binary = Bytes::from_static(b"\0binary\xff");

        let result = command
            .parse_response(Frame::Array(Some(vec![
                Frame::BulkString(Some(Bytes::from_static(b"apple"))),
                Frame::BulkString(Some(binary.clone())),
                Frame::BulkString(Some(Bytes::from_static(b"pear"))),
            ])))
            .unwrap();

        assert_eq!(
            result,
            vec![
                Bytes::from_static(b"apple"),
                binary,
                Bytes::from_static(b"pear")
            ]
        );
    }

    #[test]
    fn vrange_parses_empty_array() {
        let command = VRange::new("vectors", "-", "+");

        assert_eq!(
            command
                .parse_response(Frame::Array(Some(Vec::new())))
                .unwrap(),
            Vec::<Bytes>::new()
        );
    }

    #[test]
    fn vrange_rejects_invalid_reply_shapes() {
        let command = VRange::new("vectors", "-", "+");

        assert!(
            command
                .parse_response(Frame::BulkString(Some(Bytes::from_static(b"member"))))
                .is_err()
        );
        assert!(command.parse_response(Frame::Array(None)).is_err());
        assert!(
            command
                .parse_response(Frame::Array(Some(vec![Frame::BulkString(None)])))
                .is_err()
        );
        assert!(
            command
                .parse_response(Frame::Array(Some(vec![Frame::Integer(1)])))
                .is_err()
        );
    }
}
