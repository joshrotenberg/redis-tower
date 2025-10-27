//! Redis Vector Sets module (Redis 8.0+)
//!
//! Vector sets are a new data type for vector similarity search,
//! optimized for fast approximate nearest neighbor (ANN) queries.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// VADD command - Add element to vector set
///
/// Adds one or more elements with their associated vectors to a vector set.
/// Available since Redis 8.0.0 (beta).
#[derive(Debug, Clone)]
pub struct Vadd {
    key: String,
    elements: Vec<(String, Vec<f32>)>,
}

impl Vadd {
    pub fn new(key: impl Into<String>, elements: Vec<(impl Into<String>, Vec<f32>)>) -> Self {
        Self {
            key: key.into(),
            elements: elements.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }
}

impl Command for Vadd {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("VADD"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ];

        for (element, vector) in &self.elements {
            frames.push(Frame::BulkString(Some(Bytes::from(element.clone()))));
            // Encode vector as space-separated floats
            let vector_str = vector
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(" ");
            frames.push(Frame::BulkString(Some(Bytes::from(vector_str))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// VCARD command - Get cardinality of vector set
///
/// Returns the number of elements in a vector set.
/// Available since Redis 8.0.0 (beta).
#[derive(Debug, Clone)]
pub struct Vcard {
    key: String,
}

impl Vcard {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Vcard {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("VCARD"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// VDIM command - Get dimension of vectors in set
///
/// Returns the dimension of vectors stored in the vector set.
/// Available since Redis 8.0.0 (beta).
#[derive(Debug, Clone)]
pub struct Vdim {
    key: String,
}

impl Vdim {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Vdim {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("VDIM"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// VEMB command - Get embedding vector for element
///
/// Returns the approximate vector associated with an element.
/// Available since Redis 8.0.0 (beta).
#[derive(Debug, Clone)]
pub struct Vemb {
    key: String,
    element: String,
}

impl Vemb {
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
        }
    }
}

impl Command for Vemb {
    type Response = Option<Vec<f32>>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("VEMB"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(Bytes::from(self.element.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(&data);
                let vec: Result<Vec<f32>, _> = s.split_whitespace().map(|s| s.parse()).collect();
                Ok(Some(vec.map_err(|_| RedisError::UnexpectedResponse)?))
            }
            Frame::Null | Frame::BulkString(None) => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// VGETATTR command - Get attributes of element
///
/// Returns the attributes associated with an element.
/// Available since Redis 8.0.0 (beta).
#[derive(Debug, Clone)]
pub struct Vgetattr {
    key: String,
    element: String,
}

impl Vgetattr {
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
        }
    }
}

impl Command for Vgetattr {
    type Response = Option<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("VGETATTR"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(Bytes::from(self.element.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(Some(String::from_utf8_lossy(&data).to_string())),
            Frame::Null | Frame::BulkString(None) => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// VINFO command - Get metadata about vector set
///
/// Returns metadata and internal details about the vector set.
/// Available since Redis 8.0.0 (beta).
#[derive(Debug, Clone)]
pub struct Vinfo {
    key: String,
}

impl Vinfo {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }
}

impl Command for Vinfo {
    type Response = String;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("VINFO"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(String::from_utf8_lossy(&data).to_string()),
            Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).to_string()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// VISMEMBER command - Check if element exists in vector set
///
/// Returns 1 if the element exists, 0 otherwise.
/// Available since Redis 8.0.0 (beta).
#[derive(Debug, Clone)]
pub struct Vismember {
    key: String,
    element: String,
}

impl Vismember {
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
        }
    }
}

impl Command for Vismember {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("VISMEMBER"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(Bytes::from(self.element.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n != 0),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// VLINKS command - Get linked elements
///
/// Returns elements that are linked to the given element.
/// Available since Redis 8.0.0 (beta).
#[derive(Debug, Clone)]
pub struct Vlinks {
    key: String,
    element: String,
}

impl Vlinks {
    pub fn new(key: impl Into<String>, element: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
        }
    }
}

impl Command for Vlinks {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("VLINKS"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(Bytes::from(self.element.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    if let Frame::BulkString(Some(data)) = item {
                        result.push(String::from_utf8_lossy(&data).to_string());
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// VRANDMEMBER command - Get random element from vector set
///
/// Returns one or more random elements from the vector set.
/// Available since Redis 8.0.0 (beta).
#[derive(Debug, Clone)]
pub struct Vrandmember {
    key: String,
    count: Option<i64>,
}

impl Vrandmember {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            count: None,
        }
    }

    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for Vrandmember {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("VRANDMEMBER"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ];

        if let Some(count) = self.count {
            frames.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                for item in items {
                    if let Frame::BulkString(Some(data)) = item {
                        result.push(String::from_utf8_lossy(&data).to_string());
                    }
                }
                Ok(result)
            }
            Frame::BulkString(Some(data)) => Ok(vec![String::from_utf8_lossy(&data).to_string()]),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// VREM command - Remove elements from vector set
///
/// Removes one or more elements from the vector set.
/// Available since Redis 8.0.0 (beta).
#[derive(Debug, Clone)]
pub struct Vrem {
    key: String,
    elements: Vec<String>,
}

impl Vrem {
    pub fn new(key: impl Into<String>, elements: Vec<impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            elements: elements.into_iter().map(|e| e.into()).collect(),
        }
    }
}

impl Command for Vrem {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("VREM"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
        ];

        for element in &self.elements {
            frames.push(Frame::BulkString(Some(Bytes::from(element.clone()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// VSETATTR command - Set attributes for element
///
/// Sets attributes for an element in the vector set.
/// Available since Redis 8.0.0 (beta).
#[derive(Debug, Clone)]
pub struct Vsetattr {
    key: String,
    element: String,
    attributes: String,
}

impl Vsetattr {
    pub fn new(
        key: impl Into<String>,
        element: impl Into<String>,
        attributes: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
            attributes: attributes.into(),
        }
    }
}

impl Command for Vsetattr {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("VSETATTR"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(Bytes::from(self.element.clone()))),
            Frame::BulkString(Some(Bytes::from(self.attributes.clone()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Integer(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// VSIM command - Find similar vectors
///
/// Performs vector similarity search to find k-nearest neighbors.
/// Available since Redis 8.0.0 (beta).
#[derive(Debug, Clone)]
pub struct Vsim {
    key: String,
    vector: Vec<f32>,
    k: i64,
}

impl Vsim {
    pub fn new(key: impl Into<String>, vector: Vec<f32>, k: i64) -> Self {
        Self {
            key: key.into(),
            vector,
            k,
        }
    }
}

impl Command for Vsim {
    type Response = Vec<(String, f64)>;

    fn to_frame(&self) -> Frame {
        let vector_str = self
            .vector
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(" ");

        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("VSIM"))),
            Frame::BulkString(Some(Bytes::from(self.key.clone()))),
            Frame::BulkString(Some(Bytes::from(vector_str))),
            Frame::BulkString(Some(Bytes::from(self.k.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut result = Vec::new();
                let mut i = 0;
                while i < items.len() {
                    if i + 1 < items.len() {
                        let element = match &items[i] {
                            Frame::BulkString(Some(data)) => {
                                String::from_utf8_lossy(data).to_string()
                            }
                            _ => continue,
                        };
                        let score = match &items[i + 1] {
                            Frame::BulkString(Some(data)) => {
                                String::from_utf8_lossy(data).parse().unwrap_or(0.0)
                            }
                            _ => 0.0,
                        };
                        result.push((element, score));
                        i += 2;
                    } else {
                        break;
                    }
                }
                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}
