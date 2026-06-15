use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

fn parse_bool_array(frame: Frame) -> Result<Vec<bool>, RedisError> {
    match frame {
        Frame::Array(Some(frames)) => frames
            .into_iter()
            .map(|f| match f {
                Frame::Integer(n) => Ok(n != 0),
                Frame::Boolean(b) => Ok(b),
                other => Err(RedisError::UnexpectedResponse {
                    expected: "integer or boolean",
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

fn parse_optional_bytes_array(frame: Frame) -> Result<Vec<Option<Bytes>>, RedisError> {
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

// ===========================================================================
// Count-Min Sketch commands
// ===========================================================================

/// CMS.INITBYDIM key width depth
///
/// Initializes a Count-Min Sketch with the given width and depth.
#[derive(Clone)]
pub struct CmsInitByDim {
    key: String,
    width: i64,
    depth: i64,
}

impl CmsInitByDim {
    pub fn new(key: impl Into<String>, width: i64, depth: i64) -> Self {
        Self {
            key: key.into(),
            width,
            depth,
        }
    }
}

impl Command for CmsInitByDim {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CMS.INITBYDIM"),
            bulk(self.key.as_str()),
            bulk(self.width.to_string()),
            bulk(self.depth.to_string()),
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
        "CMS.INITBYDIM"
    }
}

/// CMS.INITBYPROB key error probability
///
/// Initializes a Count-Min Sketch with the given error rate and probability.
#[derive(Clone)]
pub struct CmsInitByProb {
    key: String,
    error: f64,
    probability: f64,
}

impl CmsInitByProb {
    pub fn new(key: impl Into<String>, error: f64, probability: f64) -> Self {
        Self {
            key: key.into(),
            error,
            probability,
        }
    }
}

impl Command for CmsInitByProb {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("CMS.INITBYPROB"),
            bulk(self.key.as_str()),
            bulk(self.error.to_string()),
            bulk(self.probability.to_string()),
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
        "CMS.INITBYPROB"
    }
}

/// CMS.INCRBY key item increment \[item increment ...\]
///
/// Increments one or more items in the Count-Min Sketch. Returns the
/// estimated count for each item after incrementing.
#[derive(Clone)]
pub struct CmsIncrBy {
    key: String,
    items: Vec<(String, i64)>,
}

impl CmsIncrBy {
    pub fn new(
        key: impl Into<String>,
        items: impl IntoIterator<Item = (impl Into<String>, i64)>,
    ) -> Self {
        Self {
            key: key.into(),
            items: items.into_iter().map(|(s, n)| (s.into(), n)).collect(),
        }
    }
}

impl Command for CmsIncrBy {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("CMS.INCRBY"), bulk(self.key.as_str())];
        for (item, incr) in &self.items {
            args.push(bulk(item.as_str()));
            args.push(bulk(incr.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_i64_array(frame)
    }

    fn name(&self) -> &str {
        "CMS.INCRBY"
    }
}

/// CMS.QUERY key item \[item ...\]
///
/// Returns the estimated count for one or more items in the Count-Min Sketch.
#[derive(Clone)]
pub struct CmsQuery {
    key: String,
    items: Vec<String>,
}

impl CmsQuery {
    pub fn new(key: impl Into<String>, items: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            items: items.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for CmsQuery {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("CMS.QUERY"), bulk(self.key.as_str())];
        for item in &self.items {
            args.push(bulk(item.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_i64_array(frame)
    }

    fn name(&self) -> &str {
        "CMS.QUERY"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// CMS.MERGE destination numkeys source \[source ...\] \[WEIGHTS weight ...\]
///
/// Merges several Count-Min Sketches into a destination sketch.
#[derive(Clone)]
pub struct CmsMerge {
    destination: String,
    sources: Vec<String>,
    weights: Vec<i64>,
}

impl CmsMerge {
    pub fn new(
        destination: impl Into<String>,
        sources: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            destination: destination.into(),
            sources: sources.into_iter().map(Into::into).collect(),
            weights: Vec::new(),
        }
    }

    /// Set weights for the source sketches.
    pub fn weights(mut self, weights: impl IntoIterator<Item = i64>) -> Self {
        self.weights = weights.into_iter().collect();
        self
    }
}

impl Command for CmsMerge {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("CMS.MERGE"),
            bulk(self.destination.as_str()),
            bulk(self.sources.len().to_string()),
        ];
        for src in &self.sources {
            args.push(bulk(src.as_str()));
        }
        if !self.weights.is_empty() {
            args.push(bulk("WEIGHTS"));
            for w in &self.weights {
                args.push(bulk(w.to_string()));
            }
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
        "CMS.MERGE"
    }
}

/// CMS.INFO key
///
/// Returns information about the Count-Min Sketch at `key` as a raw Frame.
#[derive(Clone)]
pub struct CmsInfo {
    key: String,
}

impl CmsInfo {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for CmsInfo {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("CMS.INFO"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "CMS.INFO"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

// ===========================================================================
// Top-K commands
// ===========================================================================

/// TOPK.RESERVE key topk \[width depth decay\]
///
/// Initializes a Top-K data structure with the given parameters.
#[derive(Clone)]
pub struct TopkReserve {
    key: String,
    topk: i64,
    width: Option<i64>,
    depth: Option<i64>,
    decay: Option<f64>,
}

impl TopkReserve {
    pub fn new(key: impl Into<String>, topk: i64) -> Self {
        Self {
            key: key.into(),
            topk,
            width: None,
            depth: None,
            decay: None,
        }
    }

    /// Set the width, depth, and decay parameters.
    pub fn params(mut self, width: i64, depth: i64, decay: f64) -> Self {
        self.width = Some(width);
        self.depth = Some(depth);
        self.decay = Some(decay);
        self
    }
}

impl Command for TopkReserve {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("TOPK.RESERVE"),
            bulk(self.key.as_str()),
            bulk(self.topk.to_string()),
        ];
        if let (Some(w), Some(d), Some(decay)) = (self.width, self.depth, self.decay) {
            args.push(bulk(w.to_string()));
            args.push(bulk(d.to_string()));
            args.push(bulk(decay.to_string()));
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
        "TOPK.RESERVE"
    }
}

/// TOPK.ADD key item \[item ...\]
///
/// Adds one or more items to the Top-K. Returns a vector of evicted items
/// (or None for items that did not cause an eviction).
#[derive(Clone)]
pub struct TopkAdd {
    key: String,
    items: Vec<String>,
}

impl TopkAdd {
    pub fn new(key: impl Into<String>, items: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            items: items.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for TopkAdd {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TOPK.ADD"), bulk(self.key.as_str())];
        for item in &self.items {
            args.push(bulk(item.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_optional_bytes_array(frame)
    }

    fn name(&self) -> &str {
        "TOPK.ADD"
    }
}

/// TOPK.INCRBY key item increment \[item increment ...\]
///
/// Increments the score of one or more items in the Top-K. Returns a vector
/// of evicted items (or None for items that did not cause an eviction).
#[derive(Clone)]
pub struct TopkIncrBy {
    key: String,
    items: Vec<(String, i64)>,
}

impl TopkIncrBy {
    pub fn new(
        key: impl Into<String>,
        items: impl IntoIterator<Item = (impl Into<String>, i64)>,
    ) -> Self {
        Self {
            key: key.into(),
            items: items.into_iter().map(|(s, n)| (s.into(), n)).collect(),
        }
    }
}

impl Command for TopkIncrBy {
    type Response = Vec<Option<Bytes>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TOPK.INCRBY"), bulk(self.key.as_str())];
        for (item, incr) in &self.items {
            args.push(bulk(item.as_str()));
            args.push(bulk(incr.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_optional_bytes_array(frame)
    }

    fn name(&self) -> &str {
        "TOPK.INCRBY"
    }
}

/// TOPK.QUERY key item \[item ...\]
///
/// Checks whether one or more items are in the Top-K.
#[derive(Clone)]
pub struct TopkQuery {
    key: String,
    items: Vec<String>,
}

impl TopkQuery {
    pub fn new(key: impl Into<String>, items: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            items: items.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for TopkQuery {
    type Response = Vec<bool>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TOPK.QUERY"), bulk(self.key.as_str())];
        for item in &self.items {
            args.push(bulk(item.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_bool_array(frame)
    }

    fn name(&self) -> &str {
        "TOPK.QUERY"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// TOPK.COUNT key item \[item ...\]
///
/// Returns the approximate count for one or more items in the Top-K.
#[derive(Clone)]
pub struct TopkCount {
    key: String,
    items: Vec<String>,
}

impl TopkCount {
    pub fn new(key: impl Into<String>, items: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            items: items.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for TopkCount {
    type Response = Vec<i64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TOPK.COUNT"), bulk(self.key.as_str())];
        for item in &self.items {
            args.push(bulk(item.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_i64_array(frame)
    }

    fn name(&self) -> &str {
        "TOPK.COUNT"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// TOPK.LIST key \[WITHCOUNT\]
///
/// Returns the list of top-k items. When called with WITHCOUNT, the response
/// includes counts interleaved with items, returned as a raw Frame.
#[derive(Clone)]
pub struct TopkList {
    key: String,
    withcount: bool,
}

impl TopkList {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            withcount: false,
        }
    }

    /// Include counts in the response.
    pub fn withcount(mut self) -> Self {
        self.withcount = true;
        self
    }
}

impl Command for TopkList {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("TOPK.LIST"), bulk(self.key.as_str())];
        if self.withcount {
            args.push(bulk("WITHCOUNT"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "TOPK.LIST"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// TOPK.INFO key
///
/// Returns information about the Top-K at `key` as a raw Frame.
#[derive(Clone)]
pub struct TopkInfo {
    key: String,
}

impl TopkInfo {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for TopkInfo {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("TOPK.INFO"), bulk(self.key.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "TOPK.INFO"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;

    #[test]
    fn idempotency_flags() {
        // Read-only commands are safe to retry.
        assert!(CmsQuery::new("k", ["i"]).idempotent());
        // Mutating commands keep the default (false).
        assert!(!CmsIncrBy::new("k", [("i", 1)]).idempotent());
    }
}
