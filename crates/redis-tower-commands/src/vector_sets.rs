use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// Quantization type for VADD.
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
pub struct VRem {
    key: String,
    element: String,
}

impl VRem {
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
pub struct VCard {
    key: String,
}

impl VCard {
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
pub struct VDim {
    key: String,
}

impl VDim {
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

/// VEMB key element \[RAW\]
///
/// Returns the vector embedding for `element` in the vector set at `key`.
/// Without RAW, returns an array of doubles. With RAW, returns the raw FP32
/// binary blob.
pub struct VEmb {
    key: String,
    element: String,
    raw: bool,
}

impl VEmb {
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
        match frame {
            Frame::Array(Some(frames)) => {
                if self.withscores {
                    // Pairs: [element, score, element, score, ...]
                    if frames.len() % 2 != 0 {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "even number of elements for WITHSCORES",
                            actual: format!("got {} elements", frames.len()),
                        });
                    }
                    let mut result = Vec::with_capacity(frames.len() / 2);
                    let mut iter = frames.into_iter();
                    while let Some(name_frame) = iter.next() {
                        let score_frame = iter.next().unwrap();
                        let name = match name_frame {
                            Frame::BulkString(Some(data)) => data,
                            other => {
                                return Err(RedisError::UnexpectedResponse {
                                    expected: "bulk string",
                                    actual: format!("{other:?}"),
                                });
                            }
                        };
                        let score = match score_frame {
                            Frame::BulkString(Some(data)) => {
                                let s = String::from_utf8_lossy(&data);
                                s.parse::<f64>()
                                    .map_err(|_| RedisError::UnexpectedResponse {
                                        expected: "float string",
                                        actual: format!("{s}"),
                                    })?
                            }
                            Frame::Double(d) => d,
                            other => {
                                return Err(RedisError::UnexpectedResponse {
                                    expected: "bulk string or double",
                                    actual: format!("{other:?}"),
                                });
                            }
                        };
                        result.push((name, Some(score)));
                    }
                    Ok(result)
                } else {
                    frames
                        .into_iter()
                        .map(|f| match f {
                            Frame::BulkString(Some(data)) => Ok((data, None)),
                            other => Err(RedisError::UnexpectedResponse {
                                expected: "bulk string",
                                actual: format!("{other:?}"),
                            }),
                        })
                        .collect()
                }
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "VSIM"
    }
}

/// VRANDMEMBER key \[COUNT n\]
///
/// Returns one or more random elements from the vector set at `key`.
pub struct VRandMember {
    key: String,
    count: Option<i64>,
}

impl VRandMember {
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

/// VGETATTR key element
///
/// Returns the JSON attribute string for `element` in the vector set at `key`,
/// or `None` if no attribute is set.
pub struct VGetAttr {
    key: String,
    element: String,
}

impl VGetAttr {
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
pub struct VSetAttr {
    key: String,
    element: String,
    json: String,
}

impl VSetAttr {
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

/// VDELATTR key element
///
/// Deletes the JSON attribute from `element` in the vector set at `key`.
/// Returns `true` if the attribute was removed, `false` if no attribute existed.
pub struct VDelAttr {
    key: String,
    element: String,
}

impl VDelAttr {
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
        array(vec![
            bulk("VDELATTR"),
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
        "VDELATTR"
    }
}

/// VINFO key
///
/// Returns information about the vector set at `key` as a flat array of
/// alternating field names and values.
pub struct VInfo {
    key: String,
}

impl VInfo {
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
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
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
pub struct VLinks {
    key: String,
    element: String,
    withscores: bool,
}

impl VLinks {
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
        match frame {
            Frame::Array(Some(frames)) => {
                if self.withscores {
                    if frames.len() % 2 != 0 {
                        return Err(RedisError::UnexpectedResponse {
                            expected: "even number of elements for WITHSCORES",
                            actual: format!("got {} elements", frames.len()),
                        });
                    }
                    let mut result = Vec::with_capacity(frames.len() / 2);
                    let mut iter = frames.into_iter();
                    while let Some(name_frame) = iter.next() {
                        let score_frame = iter.next().unwrap();
                        let name = match name_frame {
                            Frame::BulkString(Some(data)) => data,
                            other => {
                                return Err(RedisError::UnexpectedResponse {
                                    expected: "bulk string",
                                    actual: format!("{other:?}"),
                                });
                            }
                        };
                        let score = match score_frame {
                            Frame::BulkString(Some(data)) => {
                                let s = String::from_utf8_lossy(&data);
                                s.parse::<f64>()
                                    .map_err(|_| RedisError::UnexpectedResponse {
                                        expected: "float string",
                                        actual: format!("{s}"),
                                    })?
                            }
                            Frame::Double(d) => d,
                            other => {
                                return Err(RedisError::UnexpectedResponse {
                                    expected: "bulk string or double",
                                    actual: format!("{other:?}"),
                                });
                            }
                        };
                        result.push((name, Some(score)));
                    }
                    Ok(result)
                } else {
                    frames
                        .into_iter()
                        .map(|f| match f {
                            Frame::BulkString(Some(data)) => Ok((data, None)),
                            other => Err(RedisError::UnexpectedResponse {
                                expected: "bulk string",
                                actual: format!("{other:?}"),
                            }),
                        })
                        .collect()
                }
            }
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "VLINKS"
    }
}
