//! Convenience constructors for building RESP3 command frames.

use bytes::Bytes;

use crate::Frame;

/// Create a bulk string frame from anything that can be represented as bytes.
pub fn bulk(data: impl AsRef<[u8]>) -> Frame {
    Frame::BulkString(Some(Bytes::copy_from_slice(data.as_ref())))
}

/// Create an array frame from a vec of frames.
pub fn array(frames: Vec<Frame>) -> Frame {
    Frame::Array(Some(frames))
}

/// Create a null bulk string frame.
pub fn null_bulk() -> Frame {
    Frame::BulkString(None)
}
