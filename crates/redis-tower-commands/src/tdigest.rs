use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_f64_from_frame(f: Frame) -> Result<f64, RedisError> {
    match f {
        Frame::Double(v) => Ok(v),
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
            expected: "double or bulk string",
            actual: format!("{other:?}"),
        }),
    }
}

fn parse_f64_array(frame: Frame) -> Result<Vec<f64>, RedisError> {
    match frame {
        Frame::Array(Some(frames)) => frames.into_iter().map(parse_f64_from_frame).collect(),
        other => Err(RedisError::UnexpectedResponse {
            expected: "array",
            actual: format!("{other:?}"),
        }),
    }
}

fn parse_i64_array(frame: Frame) -> Result<Vec<i64>, RedisError> {
    match frame {
        Frame::Array(Some(frames)) => frames
            .into_iter()
            .map(|f| match f {
                Frame::Integer(n) => Ok(n),
                other => Err(RedisError::UnexpectedResponse {
                    expected: "integer",
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

// ===========================================================================
// T-Digest commands
// ===========================================================================

/// TDIGEST.CREATE key \[COMPRESSION compression\]
///
/// Creates an empty T-Digest sketch at `key`.
pub struct TdigestCreate {
    key: String,
    compression: Option<i64>,
}

impl TdigestCreate {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            compression: None,
        }
    }

    /// Set the compression parameter.
    pub fn compression(mut self, compression: i64) -> Self {
        self.compression = Some(compression);
        self
    }
}

impl Command for TdigestCreate {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TDIGEST.CREATE"), bulk(self.key.as_str())];
        if let Some(c) = self.compression {
            args.push(bulk("COMPRESSION"));
            args.push(bulk(c.to_string()));
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
        "TDIGEST.CREATE"
    }
}

/// TDIGEST.ADD key value \[value ...\]
///
/// Adds one or more values to the T-Digest sketch at `key`.
pub struct TdigestAdd {
    key: String,
    values: Vec<f64>,
}

impl TdigestAdd {
    pub fn new(key: impl Into<String>, values: impl IntoIterator<Item = f64>) -> Self {
        Self {
            key: key.into(),
            values: values.into_iter().collect(),
        }
    }
}

impl Command for TdigestAdd {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TDIGEST.ADD"), bulk(self.key.as_str())];
        for v in &self.values {
            args.push(bulk(v.to_string()));
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
        "TDIGEST.ADD"
    }
}

/// TDIGEST.MERGE destination numkeys source \[source ...\]
/// \[COMPRESSION compression\] \[OVERRIDE\]
///
/// Merges one or more T-Digest sketches into a destination key.
pub struct TdigestMerge {
    destination: String,
    sources: Vec<String>,
    compression: Option<i64>,
    override_flag: bool,
}

impl TdigestMerge {
    pub fn new(
        destination: impl Into<String>,
        sources: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            destination: destination.into(),
            sources: sources.into_iter().map(Into::into).collect(),
            compression: None,
            override_flag: false,
        }
    }

    /// Set the compression parameter for the merged result.
    pub fn compression(mut self, compression: i64) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Override the destination if it already exists.
    pub fn override_dest(mut self) -> Self {
        self.override_flag = true;
        self
    }
}

impl Command for TdigestMerge {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("TDIGEST.MERGE"),
            bulk(self.destination.as_str()),
            bulk(self.sources.len().to_string()),
        ];
        for src in &self.sources {
            args.push(bulk(src.as_str()));
        }
        if let Some(c) = self.compression {
            args.push(bulk("COMPRESSION"));
            args.push(bulk(c.to_string()));
        }
        if self.override_flag {
            args.push(bulk("OVERRIDE"));
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
        "TDIGEST.MERGE"
    }
}

/// TDIGEST.CDF key value \[value ...\]
///
/// Returns the cumulative distribution function value for each given value.
pub struct TdigestCdf {
    key: String,
    values: Vec<f64>,
}

impl TdigestCdf {
    pub fn new(key: impl Into<String>, values: impl IntoIterator<Item = f64>) -> Self {
        Self {
            key: key.into(),
            values: values.into_iter().collect(),
        }
    }
}

impl Command for TdigestCdf {
    type Response = Vec<f64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TDIGEST.CDF"), bulk(self.key.as_str())];
        for v in &self.values {
            args.push(bulk(v.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_f64_array(frame)
    }

    fn name(&self) -> &str {
        "TDIGEST.CDF"
    }
}

/// TDIGEST.QUANTILE key quantile \[quantile ...\]
///
/// Returns the estimated value at each given quantile.
pub struct TdigestQuantile {
    key: String,
    quantiles: Vec<f64>,
}

impl TdigestQuantile {
    pub fn new(key: impl Into<String>, quantiles: impl IntoIterator<Item = f64>) -> Self {
        Self {
            key: key.into(),
            quantiles: quantiles.into_iter().collect(),
        }
    }
}

impl Command for TdigestQuantile {
    type Response = Vec<f64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TDIGEST.QUANTILE"), bulk(self.key.as_str())];
        for q in &self.quantiles {
            args.push(bulk(q.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_f64_array(frame)
    }

    fn name(&self) -> &str {
        "TDIGEST.QUANTILE"
    }
}

/// TDIGEST.MIN key
///
/// Returns the minimum value observed by the T-Digest.
pub struct TdigestMin {
    key: String,
}

impl TdigestMin {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for TdigestMin {
    type Response = f64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("TDIGEST.MIN"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_f64_from_frame(frame)
    }

    fn name(&self) -> &str {
        "TDIGEST.MIN"
    }
}

/// TDIGEST.MAX key
///
/// Returns the maximum value observed by the T-Digest.
pub struct TdigestMax {
    key: String,
}

impl TdigestMax {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for TdigestMax {
    type Response = f64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("TDIGEST.MAX"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_f64_from_frame(frame)
    }

    fn name(&self) -> &str {
        "TDIGEST.MAX"
    }
}

/// TDIGEST.INFO key
///
/// Returns information about the T-Digest at `key` as a raw Frame.
pub struct TdigestInfo {
    key: String,
}

impl TdigestInfo {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for TdigestInfo {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("TDIGEST.INFO"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "TDIGEST.INFO"
    }
}

/// TDIGEST.RESET key
///
/// Resets the T-Digest sketch at `key`, discarding all observed values.
pub struct TdigestReset {
    key: String,
}

impl TdigestReset {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for TdigestReset {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("TDIGEST.RESET"), bulk(self.key.as_str())])
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
        "TDIGEST.RESET"
    }
}

/// TDIGEST.TRIMMED_MEAN key low_quantile high_quantile
///
/// Returns the trimmed mean between the given quantile bounds.
pub struct TdigestTrimmedMean {
    key: String,
    low_quantile: f64,
    high_quantile: f64,
}

impl TdigestTrimmedMean {
    pub fn new(key: impl Into<String>, low_quantile: f64, high_quantile: f64) -> Self {
        Self {
            key: key.into(),
            low_quantile,
            high_quantile,
        }
    }
}

impl Command for TdigestTrimmedMean {
    type Response = f64;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("TDIGEST.TRIMMED_MEAN"),
            bulk(self.key.as_str()),
            bulk(self.low_quantile.to_string()),
            bulk(self.high_quantile.to_string()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_f64_from_frame(frame)
    }

    fn name(&self) -> &str {
        "TDIGEST.TRIMMED_MEAN"
    }
}

/// TDIGEST.RANK key value \[value ...\]
///
/// Returns the estimated rank of each given value.
pub struct TdigestRank {
    key: String,
    values: Vec<f64>,
}

impl TdigestRank {
    pub fn new(key: impl Into<String>, values: impl IntoIterator<Item = f64>) -> Self {
        Self {
            key: key.into(),
            values: values.into_iter().collect(),
        }
    }
}

impl Command for TdigestRank {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TDIGEST.RANK"), bulk(self.key.as_str())];
        for v in &self.values {
            args.push(bulk(v.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_i64_array(frame)
    }

    fn name(&self) -> &str {
        "TDIGEST.RANK"
    }
}

/// TDIGEST.REVRANK key value \[value ...\]
///
/// Returns the estimated reverse rank of each given value.
pub struct TdigestRevRank {
    key: String,
    values: Vec<f64>,
}

impl TdigestRevRank {
    pub fn new(key: impl Into<String>, values: impl IntoIterator<Item = f64>) -> Self {
        Self {
            key: key.into(),
            values: values.into_iter().collect(),
        }
    }
}

impl Command for TdigestRevRank {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TDIGEST.REVRANK"), bulk(self.key.as_str())];
        for v in &self.values {
            args.push(bulk(v.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_i64_array(frame)
    }

    fn name(&self) -> &str {
        "TDIGEST.REVRANK"
    }
}

/// TDIGEST.BYRANK key rank \[rank ...\]
///
/// Returns the estimated value at each given rank.
pub struct TdigestByRank {
    key: String,
    ranks: Vec<i64>,
}

impl TdigestByRank {
    pub fn new(key: impl Into<String>, ranks: impl IntoIterator<Item = i64>) -> Self {
        Self {
            key: key.into(),
            ranks: ranks.into_iter().collect(),
        }
    }
}

impl Command for TdigestByRank {
    type Response = Vec<f64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TDIGEST.BYRANK"), bulk(self.key.as_str())];
        for r in &self.ranks {
            args.push(bulk(r.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_f64_array(frame)
    }

    fn name(&self) -> &str {
        "TDIGEST.BYRANK"
    }
}

/// TDIGEST.BYREVRANK key rank \[rank ...\]
///
/// Returns the estimated value at each given reverse rank.
pub struct TdigestByRevRank {
    key: String,
    ranks: Vec<i64>,
}

impl TdigestByRevRank {
    pub fn new(key: impl Into<String>, ranks: impl IntoIterator<Item = i64>) -> Self {
        Self {
            key: key.into(),
            ranks: ranks.into_iter().collect(),
        }
    }
}

impl Command for TdigestByRevRank {
    type Response = Vec<f64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TDIGEST.BYREVRANK"), bulk(self.key.as_str())];
        for r in &self.ranks {
            args.push(bulk(r.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_f64_array(frame)
    }

    fn name(&self) -> &str {
        "TDIGEST.BYREVRANK"
    }
}
