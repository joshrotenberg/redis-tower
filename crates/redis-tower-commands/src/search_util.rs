use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// FT.SUGADD key string score \[INCR\] \[PAYLOAD payload\]
///
/// Adds a suggestion string to an auto-complete dictionary. Returns the
/// current size of the dictionary.
#[derive(Clone)]
pub struct FtSugAdd {
    key: String,
    string: String,
    score: f64,
    incr: bool,
    payload: Option<String>,
}

impl FtSugAdd {
    pub fn new(key: impl Into<String>, string: impl Into<String>, score: f64) -> Self {
        Self {
            key: key.into(),
            string: string.into(),
            score,
            incr: false,
            payload: None,
        }
    }

    /// Increment the existing score instead of replacing it.
    pub fn incr(mut self) -> Self {
        self.incr = true;
        self
    }

    /// Set an opaque payload to store with the suggestion.
    pub fn payload(mut self, payload: impl Into<String>) -> Self {
        self.payload = Some(payload.into());
        self
    }
}

impl Command for FtSugAdd {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("FT.SUGADD"),
            bulk(self.key.as_str()),
            bulk(self.string.as_str()),
            bulk(self.score.to_string()),
        ];
        if self.incr {
            args.push(bulk("INCR"));
        }
        if let Some(payload) = &self.payload {
            args.push(bulk("PAYLOAD"));
            args.push(bulk(payload.as_str()));
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
        "FT.SUGADD"
    }
}

/// FT.SUGGET key prefix \[FUZZY\] \[WITHSCORES\] \[WITHPAYLOADS\] \[MAX num\]
///
/// Gets completion suggestions for a prefix from an auto-complete dictionary.
/// The response structure varies based on options, so it returns a raw `Frame`.
#[derive(Clone)]
pub struct FtSugGet {
    key: String,
    prefix: String,
    fuzzy: bool,
    withscores: bool,
    withpayloads: bool,
    max: Option<u64>,
}

impl FtSugGet {
    pub fn new(key: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            prefix: prefix.into(),
            fuzzy: false,
            withscores: false,
            withpayloads: false,
            max: None,
        }
    }

    /// Enable fuzzy matching.
    pub fn fuzzy(mut self) -> Self {
        self.fuzzy = true;
        self
    }

    /// Include scores in the results.
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }

    /// Include payloads in the results.
    pub fn withpayloads(mut self) -> Self {
        self.withpayloads = true;
        self
    }

    /// Limit the number of results.
    pub fn max(mut self, num: u64) -> Self {
        self.max = Some(num);
        self
    }
}

impl Command for FtSugGet {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("FT.SUGGET"),
            bulk(self.key.as_str()),
            bulk(self.prefix.as_str()),
        ];
        if self.fuzzy {
            args.push(bulk("FUZZY"));
        }
        if self.withscores {
            args.push(bulk("WITHSCORES"));
        }
        if self.withpayloads {
            args.push(bulk("WITHPAYLOADS"));
        }
        if let Some(max) = self.max {
            args.push(bulk("MAX"));
            args.push(bulk(max.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "FT.SUGGET"
    }
}

/// FT.SUGDEL key string
///
/// Deletes a string from an auto-complete dictionary. Returns `true` if the
/// string was found and deleted.
#[derive(Clone)]
pub struct FtSugDel {
    key: String,
    string: String,
}

impl FtSugDel {
    pub fn new(key: impl Into<String>, string: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            string: string.into(),
        }
    }
}

impl Command for FtSugDel {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("FT.SUGDEL"),
            bulk(self.key.as_str()),
            bulk(self.string.as_str()),
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
        "FT.SUGDEL"
    }
}

/// FT.SUGLEN key
///
/// Returns the number of entries in an auto-complete dictionary.
#[derive(Clone)]
pub struct FtSugLen {
    key: String,
}

impl FtSugLen {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for FtSugLen {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("FT.SUGLEN"), bulk(self.key.as_str())])
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
        "FT.SUGLEN"
    }
}

/// FT.SYNUPDATE index group_id term \[term ...\]
///
/// Updates a synonym group with additional terms.
#[derive(Clone)]
pub struct FtSynUpdate {
    index: String,
    group_id: String,
    terms: Vec<String>,
}

impl FtSynUpdate {
    pub fn new(
        index: impl Into<String>,
        group_id: impl Into<String>,
        terms: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            index: index.into(),
            group_id: group_id.into(),
            terms: terms.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for FtSynUpdate {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("FT.SYNUPDATE"),
            bulk(self.index.as_str()),
            bulk(self.group_id.as_str()),
        ];
        for term in &self.terms {
            args.push(bulk(term.as_str()));
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
        "FT.SYNUPDATE"
    }
}

/// FT.SYNDUMP index
///
/// Dumps the contents of a synonym group. Returns a raw `Frame` containing
/// alternating term and group ID pairs.
#[derive(Clone)]
pub struct FtSynDump {
    index: String,
}

impl FtSynDump {
    pub fn new(index: impl Into<String>) -> Self {
        Self {
            index: index.into(),
        }
    }
}

impl Command for FtSynDump {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("FT.SYNDUMP"), bulk(self.index.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "FT.SYNDUMP"
    }
}

/// FT.DICTADD dict term \[term ...\]
///
/// Adds terms to a dictionary. Returns the number of new terms added.
#[derive(Clone)]
pub struct FtDictAdd {
    dict: String,
    terms: Vec<String>,
}

impl FtDictAdd {
    pub fn new(
        dict: impl Into<String>,
        terms: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            dict: dict.into(),
            terms: terms.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for FtDictAdd {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("FT.DICTADD"), bulk(self.dict.as_str())];
        for term in &self.terms {
            args.push(bulk(term.as_str()));
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
        "FT.DICTADD"
    }
}

/// FT.DICTDEL dict term \[term ...\]
///
/// Deletes terms from a dictionary. Returns the number of terms deleted.
#[derive(Clone)]
pub struct FtDictDel {
    dict: String,
    terms: Vec<String>,
}

impl FtDictDel {
    pub fn new(
        dict: impl Into<String>,
        terms: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            dict: dict.into(),
            terms: terms.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for FtDictDel {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("FT.DICTDEL"), bulk(self.dict.as_str())];
        for term in &self.terms {
            args.push(bulk(term.as_str()));
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
        "FT.DICTDEL"
    }
}

/// FT.DICTDUMP dict
///
/// Returns all terms in a dictionary.
#[derive(Clone)]
pub struct FtDictDump {
    dict: String,
}

impl FtDictDump {
    pub fn new(dict: impl Into<String>) -> Self {
        Self { dict: dict.into() }
    }
}

impl Command for FtDictDump {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("FT.DICTDUMP"), bulk(self.dict.as_str())])
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
        "FT.DICTDUMP"
    }
}

/// Term inclusion mode for FT.SPELLCHECK.
#[derive(Clone)]
pub enum SpellCheckTerms {
    /// Include terms from the specified dictionary.
    Include(String),
    /// Exclude terms from the specified dictionary.
    Exclude(String),
}

/// FT.SPELLCHECK index query \[DISTANCE dist\] \[TERMS INCLUDE|EXCLUDE dict\]
///
/// Performs spelling correction on a query. Returns suggestions for
/// misspelled terms as a raw `Frame`.
#[derive(Clone)]
pub struct FtSpellCheck {
    index: String,
    query: String,
    distance: Option<u64>,
    terms: Option<SpellCheckTerms>,
}

impl FtSpellCheck {
    pub fn new(index: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            query: query.into(),
            distance: None,
            terms: None,
        }
    }

    /// Set the maximum Levenshtein distance for spelling suggestions (1-4).
    pub fn distance(mut self, dist: u64) -> Self {
        self.distance = Some(dist);
        self
    }

    /// Include terms from the given dictionary in spell checking.
    pub fn include_terms(mut self, dict: impl Into<String>) -> Self {
        self.terms = Some(SpellCheckTerms::Include(dict.into()));
        self
    }

    /// Exclude terms from the given dictionary in spell checking.
    pub fn exclude_terms(mut self, dict: impl Into<String>) -> Self {
        self.terms = Some(SpellCheckTerms::Exclude(dict.into()));
        self
    }
}

impl Command for FtSpellCheck {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("FT.SPELLCHECK"),
            bulk(self.index.as_str()),
            bulk(self.query.as_str()),
        ];
        if let Some(dist) = self.distance {
            args.push(bulk("DISTANCE"));
            args.push(bulk(dist.to_string()));
        }
        match &self.terms {
            Some(SpellCheckTerms::Include(dict)) => {
                args.push(bulk("TERMS"));
                args.push(bulk("INCLUDE"));
                args.push(bulk(dict.as_str()));
            }
            Some(SpellCheckTerms::Exclude(dict)) => {
                args.push(bulk("TERMS"));
                args.push(bulk("EXCLUDE"));
                args.push(bulk(dict.as_str()));
            }
            None => {}
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "FT.SPELLCHECK"
    }
}

/// FT.CONFIG SET option value
///
/// Sets a RediSearch configuration option.
#[derive(Clone)]
pub struct FtConfigSet {
    option: String,
    value: String,
}

impl FtConfigSet {
    pub fn new(option: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            option: option.into(),
            value: value.into(),
        }
    }
}

impl Command for FtConfigSet {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("FT.CONFIG"),
            bulk("SET"),
            bulk(self.option.as_str()),
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
        "FT.CONFIG SET"
    }
}

/// FT.CONFIG GET option
///
/// Gets the value of a RediSearch configuration option. Returns a raw `Frame`.
#[derive(Clone)]
pub struct FtConfigGet {
    option: String,
}

impl FtConfigGet {
    pub fn new(option: impl Into<String>) -> Self {
        Self {
            option: option.into(),
        }
    }
}

impl Command for FtConfigGet {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("FT.CONFIG"),
            bulk("GET"),
            bulk(self.option.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "FT.CONFIG GET"
    }
}
