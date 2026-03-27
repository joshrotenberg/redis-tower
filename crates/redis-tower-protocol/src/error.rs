/// Protocol-level errors for RESP parsing and serialization.
#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    /// RESP parse error from resp-rs.
    #[error("parse error: {0}")]
    Parse(#[from] resp_rs::ParseError),

    /// I/O error from the underlying transport.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
