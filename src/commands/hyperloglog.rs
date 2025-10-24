//! HyperLogLog commands for cardinality estimation
//!
//! HyperLogLog is a probabilistic data structure used to estimate the cardinality
//! (number of unique elements) of a set with excellent space efficiency.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// PFADD command - Add elements to a HyperLogLog
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::PfAdd;
///
/// // Add single element
/// let cmd = PfAdd::new("unique_visitors", vec!["user1"]);
///
/// // Add multiple elements
/// let cmd = PfAdd::new("unique_visitors", vec!["user1", "user2", "user3"]);
/// ```
#[derive(Debug, Clone)]
pub struct PfAdd {
    key: String,
    elements: Vec<String>,
}

impl PfAdd {
    /// Create a new PFADD command
    pub fn new(key: impl Into<String>, elements: Vec<impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            elements: elements.into_iter().map(|e| e.into()).collect(),
        }
    }
}

impl Command for PfAdd {
    type Response = bool;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("PFADD"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for element in &self.elements {
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                element.as_bytes(),
            ))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(1) => Ok(true),  // HLL was modified
            Frame::Integer(0) => Ok(false), // HLL was not modified
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// PFCOUNT command - Get the approximate cardinality of a HyperLogLog
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::PfCount;
///
/// // Count single HLL
/// let cmd = PfCount::new(vec!["unique_visitors"]);
///
/// // Count union of multiple HLLs
/// let cmd = PfCount::new(vec!["visitors_page1", "visitors_page2"]);
/// ```
#[derive(Debug, Clone)]
pub struct PfCount {
    keys: Vec<String>,
}

impl PfCount {
    /// Create a new PFCOUNT command
    ///
    /// If multiple keys are provided, returns the approximated cardinality
    /// of the union of all the HyperLogLogs.
    pub fn new(keys: Vec<impl Into<String>>) -> Self {
        Self {
            keys: keys.into_iter().map(|k| k.into()).collect(),
        }
    }

    /// Create a PFCOUNT command for a single key
    pub fn single(key: impl Into<String>) -> Self {
        Self {
            keys: vec![key.into()],
        }
    }
}

impl Command for PfCount {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![Frame::BulkString(Some(Bytes::from("PFCOUNT")))];

        for key in &self.keys {
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(count) => Ok(count),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// PFMERGE command - Merge multiple HyperLogLogs into one
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::PfMerge;
///
/// // Merge multiple HLLs into a destination
/// let cmd = PfMerge::new("total_visitors", vec!["page1", "page2", "page3"]);
/// ```
#[derive(Debug, Clone)]
pub struct PfMerge {
    dest_key: String,
    source_keys: Vec<String>,
}

impl PfMerge {
    /// Create a new PFMERGE command
    ///
    /// Merges N different HyperLogLogs into a single one.
    pub fn new(dest_key: impl Into<String>, source_keys: Vec<impl Into<String>>) -> Self {
        Self {
            dest_key: dest_key.into(),
            source_keys: source_keys.into_iter().map(|k| k.into()).collect(),
        }
    }
}

impl Command for PfMerge {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("PFMERGE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.dest_key.as_bytes()))),
        ];

        for key in &self.source_keys {
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                key.as_bytes(),
            ))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pfadd_frame() {
        let cmd = PfAdd::new("hll", vec!["a", "b", "c"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 5); // PFADD + key + 3 elements
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("PFADD")
                ));
                assert!(matches!(
                    &args[1],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("hll")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_pfadd_response_modified() {
        let response = PfAdd::parse_response(Frame::Integer(1)).unwrap();
        assert!(response);
    }

    #[test]
    fn test_pfadd_response_not_modified() {
        let response = PfAdd::parse_response(Frame::Integer(0)).unwrap();
        assert!(!response);
    }

    #[test]
    fn test_pfcount_single_key() {
        let cmd = PfCount::single("hll");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 2); // PFCOUNT + key
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("PFCOUNT")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_pfcount_multiple_keys() {
        let cmd = PfCount::new(vec!["hll1", "hll2", "hll3"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 4); // PFCOUNT + 3 keys
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_pfcount_response() {
        let response = PfCount::parse_response(Frame::Integer(42)).unwrap();
        assert_eq!(response, 42);
    }

    #[test]
    fn test_pfmerge_frame() {
        let cmd = PfMerge::new("dest", vec!["src1", "src2"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 4); // PFMERGE + dest + 2 sources
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("PFMERGE")
                ));
                assert!(matches!(
                    &args[1],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("dest")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_pfmerge_response() {
        PfMerge::parse_response(Frame::SimpleString(Bytes::from("OK"))).unwrap();
    }
}
