/// Protocol-level errors for RESP parsing and serialization.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// RESP parse error from resp-rs.
    #[error("parse error: {0}")]
    Parse(#[from] resp_rs::ParseError),

    /// I/O error from the underlying transport.
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// A single frame exceeded the codec's configured maximum size.
    #[error("frame of {size} bytes exceeds the configured maximum of {max} bytes")]
    FrameTooLarge {
        /// Bytes buffered for the unfinished frame when the limit was hit.
        size: usize,
        /// The configured maximum, in bytes.
        max: usize,
    },

    /// A frame nested deeper than the codec's configured maximum depth.
    #[error("frame nesting exceeds the configured maximum depth of {max}")]
    NestingTooDeep {
        /// The configured maximum nesting depth.
        max: usize,
    },
}
