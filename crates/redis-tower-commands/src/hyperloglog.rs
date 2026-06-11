use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// PFADD key element [element ...]
///
/// Adds the specified elements to the HyperLogLog at `key`.
/// Returns `true` if the internal representation was altered (i.e., the
/// estimated cardinality changed), `false` otherwise.
#[derive(Clone)]
pub struct PfAdd {
    key: String,
    elements: Vec<String>,
}

impl PfAdd {
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
}

impl Command for PfAdd {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("PFADD"), bulk(self.key.as_str())];
        for element in &self.elements {
            args.push(bulk(element.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n == 1),
            Frame::Boolean(b) => Ok(b),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer or boolean",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "PFADD"
    }
}

/// PFCOUNT key [key ...]
///
/// Returns the approximate cardinality of the set(s) observed by the
/// HyperLogLog at the specified key(s). When called with multiple keys,
/// returns the approximate cardinality of the union.
#[derive(Clone)]
pub struct PfCount {
    keys: Vec<String>,
}

impl PfCount {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }

    pub fn keys(keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for PfCount {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("PFCOUNT")];
        for key in &self.keys {
            args.push(bulk(key.as_str()));
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
        "PFCOUNT"
    }
}

/// PFMERGE destkey sourcekey [sourcekey ...]
///
/// Merges one or more HyperLogLog values into a single key.
/// The destination key will hold the union of the source keys.
#[derive(Clone)]
pub struct PfMerge {
    destkey: String,
    sourcekeys: Vec<String>,
}

impl PfMerge {
    pub fn new(
        destkey: impl Into<String>,
        sourcekeys: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            destkey: destkey.into(),
            sourcekeys: sourcekeys.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for PfMerge {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("PFMERGE"), bulk(self.destkey.as_str())];
        for key in &self.sourcekeys {
            args.push(bulk(key.as_str()));
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
        "PFMERGE"
    }
}
