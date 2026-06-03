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
///
/// See: <https://redis.io/docs/latest/commands/tdigest.create/>
pub struct TdigestCreate {
    key: String,
    compression: Option<i64>,
}

impl TdigestCreate {
    /// Creates a new [`TdigestCreate`] command.
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            compression: None,
        }
    }

    /// Set the compression parameter.
    #[must_use]
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
///
/// See: <https://redis.io/docs/latest/commands/tdigest.add/>
pub struct TdigestAdd {
    key: String,
    values: Vec<f64>,
}

impl TdigestAdd {
    /// Creates a new [`TdigestAdd`] command.
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
///
/// See: <https://redis.io/docs/latest/commands/tdigest.merge/>
pub struct TdigestMerge {
    destination: String,
    sources: Vec<String>,
    compression: Option<i64>,
    override_flag: bool,
}

impl TdigestMerge {
    /// Creates a new [`TdigestMerge`] command.
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
    #[must_use]
    pub fn compression(mut self, compression: i64) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Override the destination if it already exists.
    #[must_use]
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
///
/// See: <https://redis.io/docs/latest/commands/tdigest.cdf/>
pub struct TdigestCdf {
    key: String,
    values: Vec<f64>,
}

impl TdigestCdf {
    /// Creates a new [`TdigestCdf`] command.
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
///
/// See: <https://redis.io/docs/latest/commands/tdigest.quantile/>
pub struct TdigestQuantile {
    key: String,
    quantiles: Vec<f64>,
}

impl TdigestQuantile {
    /// Creates a new [`TdigestQuantile`] command.
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
///
/// See: <https://redis.io/docs/latest/commands/tdigest.min/>
pub struct TdigestMin {
    key: String,
}

impl TdigestMin {
    /// Creates a new [`TdigestMin`] command.
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
///
/// See: <https://redis.io/docs/latest/commands/tdigest.max/>
pub struct TdigestMax {
    key: String,
}

impl TdigestMax {
    /// Creates a new [`TdigestMax`] command.
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
///
/// See: <https://redis.io/docs/latest/commands/tdigest.info/>
pub struct TdigestInfo {
    key: String,
}

impl TdigestInfo {
    /// Creates a new [`TdigestInfo`] command.
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
///
/// See: <https://redis.io/docs/latest/commands/tdigest.reset/>
pub struct TdigestReset {
    key: String,
}

impl TdigestReset {
    /// Creates a new [`TdigestReset`] command.
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
///
/// See: <https://redis.io/docs/latest/commands/tdigest.trimmed_mean/>
pub struct TdigestTrimmedMean {
    key: String,
    low_quantile: f64,
    high_quantile: f64,
}

impl TdigestTrimmedMean {
    /// Creates a new [`TdigestTrimmedMean`] command.
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
///
/// See: <https://redis.io/docs/latest/commands/tdigest.rank/>
pub struct TdigestRank {
    key: String,
    values: Vec<f64>,
}

impl TdigestRank {
    /// Creates a new [`TdigestRank`] command.
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
///
/// See: <https://redis.io/docs/latest/commands/tdigest.revrank/>
pub struct TdigestRevRank {
    key: String,
    values: Vec<f64>,
}

impl TdigestRevRank {
    /// Creates a new [`TdigestRevRank`] command.
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
///
/// See: <https://redis.io/docs/latest/commands/tdigest.byrank/>
pub struct TdigestByRank {
    key: String,
    ranks: Vec<i64>,
}

impl TdigestByRank {
    /// Creates a new [`TdigestByRank`] command.
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
///
/// See: <https://redis.io/docs/latest/commands/tdigest.byrevrank/>
pub struct TdigestByRevRank {
    key: String,
    ranks: Vec<i64>,
}

impl TdigestByRevRank {
    /// Creates a new [`TdigestByRevRank`] command.
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
