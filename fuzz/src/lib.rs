//! Shared input model and semantic oracle for the RESP fuzz targets.

use bytes::BytesMut;
use redis_tower_protocol::{ParseError, ProtocolError, RespCodec, RespLimits, frame_to_bytes};
use std::io::ErrorKind;
use tokio_util::codec::Decoder;

const MAX_PLAN_BYTES: usize = 16;

/// One fuzz input split into decoder limits, a fragmentation plan, and wire bytes.
#[derive(Debug, Clone, Copy)]
pub struct FuzzCase<'a> {
    /// Limits applied by both decoder runs.
    pub limits: RespLimits,
    /// Bytes mapped to non-zero network chunk sizes and repeated as necessary.
    pub plan: &'a [u8],
    /// Bytes presented to the RESP decoder.
    pub wire: &'a [u8],
}

/// The stable error category used when comparing whole and fragmented decoding.
///
/// `FrameTooLarge` deliberately omits the reported lower-bound size: an
/// incremental decoder can prove that the limit is exceeded before it knows
/// the complete frame extent. The disposition must still be identical.
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorDisposition {
    /// A parser rejection with its exact parser variant.
    Parse(ParseError),
    /// A deliberately unsupported RESP encoding.
    Unsupported,
    /// Another I/O error kind and message.
    Io(ErrorKind, String),
    /// The configured per-frame wire-size limit was exceeded.
    FrameTooLarge,
    /// The configured aggregate nesting limit was exceeded.
    NestingTooDeep,
}

/// Why a decoder stopped after consuming every complete leading frame.
#[derive(Debug, Clone, PartialEq)]
pub enum TerminalDisposition {
    /// More input is required, including the ordinary empty-buffer state.
    Incomplete,
    /// The first undecoded frame was rejected.
    Error(ErrorDisposition),
}

/// Observable result of draining a byte sequence through [`RespCodec`].
#[derive(Debug, Clone, PartialEq)]
pub struct DecodeOutcome {
    /// Canonical encodings of complete frames, in reply order.
    ///
    /// Canonical bytes avoid `f64::NAN` equality traps while preserving frame
    /// variants and payload bytes for the whole-versus-fragmented comparison.
    pub frames: Vec<Vec<u8>>,
    /// Bytes left at the first incomplete or rejected frame.
    pub remaining: Vec<u8>,
    /// The reason decoding stopped.
    pub terminal: TerminalDisposition,
}

/// Decode the compact fuzz-input envelope.
///
/// Inputs of four bytes or more use `[depth, size_hi, size_lo, plan_len, ...]`.
/// At most 16 following bytes form the fragmentation plan; the rest is RESP
/// wire data. Short inputs remain useful as raw wire data under conservative
/// default limits.
pub fn split_case(data: &[u8]) -> FuzzCase<'_> {
    let [depth, size_hi, size_lo, plan_len, rest @ ..] = data else {
        return FuzzCase {
            limits: RespLimits {
                max_frame_size: 4 * 1024,
                max_depth: 32,
            },
            plan: &[],
            wire: data,
        };
    };
    let plan_len = usize::from(*plan_len).min(MAX_PLAN_BYTES).min(rest.len());
    let (plan, wire) = rest.split_at(plan_len);
    FuzzCase {
        limits: RespLimits {
            max_frame_size: usize::from(u16::from_be_bytes([*size_hi, *size_lo])),
            max_depth: usize::from(*depth % 65),
        },
        plan,
        wire,
    }
}

/// Drain the complete frames available when the entire wire is buffered.
pub fn decode_whole(case: FuzzCase<'_>) -> DecodeOutcome {
    let mut codec = RespCodec::with_limits(case.limits);
    let mut buffer = BytesMut::from(case.wire);
    let mut frames = Vec::new();
    let terminal = drain_available(&mut codec, &mut buffer, &mut frames)
        .unwrap_or(TerminalDisposition::Incomplete);
    DecodeOutcome {
        frames,
        remaining: buffer.to_vec(),
        terminal,
    }
}

/// Drain the same wire according to its repeating fragmentation plan.
///
/// If a prefix is rejected, all not-yet-delivered bytes are appended to the
/// retained buffer without another decode attempt. This makes the observable
/// remainder directly comparable with whole-buffer decoding while modelling
/// the connection-closing behavior after a protocol error.
pub fn decode_fragmented(case: FuzzCase<'_>) -> DecodeOutcome {
    let mut codec = RespCodec::with_limits(case.limits);
    let mut buffer = BytesMut::new();
    let mut frames = Vec::new();
    let mut offset = 0;
    let mut plan_index = 0;
    let mut terminal = None;

    while offset < case.wire.len() {
        let chunk_size = case
            .plan
            .get(plan_index % case.plan.len().max(1))
            .copied()
            .map_or(1, |value| usize::from(value % 64) + 1);
        plan_index += 1;
        let end = offset.saturating_add(chunk_size).min(case.wire.len());
        buffer.extend_from_slice(&case.wire[offset..end]);
        offset = end;
        if let Some(disposition) = drain_available(&mut codec, &mut buffer, &mut frames) {
            terminal = Some(disposition);
            buffer.extend_from_slice(&case.wire[offset..]);
            break;
        }
    }

    DecodeOutcome {
        frames,
        remaining: buffer.to_vec(),
        terminal: terminal.unwrap_or(TerminalDisposition::Incomplete),
    }
}

fn drain_available(
    codec: &mut RespCodec,
    buffer: &mut BytesMut,
    frames: &mut Vec<Vec<u8>>,
) -> Option<TerminalDisposition> {
    loop {
        let before = buffer.len();
        match codec.decode(buffer) {
            Ok(Some(frame)) => {
                assert!(
                    buffer.len() < before,
                    "a successfully decoded frame must consume input"
                );
                frames.push(frame_to_bytes(&frame).to_vec());
            }
            Ok(None) => return None,
            Err(error) => {
                return Some(TerminalDisposition::Error(classify_error(error)));
            }
        }
    }
}

fn classify_error(error: ProtocolError) -> ErrorDisposition {
    match error {
        ProtocolError::Parse(error) => ErrorDisposition::Parse(error),
        ProtocolError::Io(error) if error.kind() == ErrorKind::Unsupported => {
            ErrorDisposition::Unsupported
        }
        ProtocolError::Io(error) => ErrorDisposition::Io(error.kind(), error.to_string()),
        ProtocolError::FrameTooLarge { .. } => ErrorDisposition::FrameTooLarge,
        ProtocolError::NestingTooDeep { .. } => ErrorDisposition::NestingTooDeep,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn case<'a>(plan: &'a [u8], wire: &'a [u8]) -> FuzzCase<'a> {
        FuzzCase {
            limits: RespLimits {
                max_frame_size: 1024,
                max_depth: 8,
            },
            plan,
            wire,
        }
    }

    #[test]
    fn pipelines_match_under_varied_fragmentation() {
        let wire = b"+OK\r\n%1\r\n+k\r\n*2\r\n:1\r\n$3\r\na\0b\r\n,nan\r\n";
        let whole = decode_whole(case(&[], wire));
        for plan in [&[0][..], &[1, 7, 2, 63][..], &[255, 3][..]] {
            assert_eq!(decode_fragmented(case(plan, wire)), whole);
        }
        assert_eq!(whole.frames.len(), 3);
        assert!(whole.remaining.is_empty());
    }

    #[test]
    fn early_rejection_retains_undelivered_pipeline_bytes() {
        let wire = b"+OK\r\n*?\r\n:1\r\n.\r\n+LATER\r\n";
        let whole = decode_whole(case(&[], wire));
        assert_eq!(decode_fragmented(case(&[0], wire)), whole);
        assert_eq!(whole.frames, vec![b"+OK\r\n".to_vec()]);
        assert_eq!(
            whole.terminal,
            TerminalDisposition::Error(ErrorDisposition::Unsupported)
        );
        assert_eq!(whole.remaining, b"*?\r\n:1\r\n.\r\n+LATER\r\n");
    }

    #[test]
    fn size_error_lower_bounds_share_one_disposition() {
        let wire = b"$16\r\nabcdefghijklmnop\r\n";
        let constrained = FuzzCase {
            limits: RespLimits {
                max_frame_size: 8,
                max_depth: 8,
            },
            plan: &[0],
            wire,
        };
        assert_eq!(decode_fragmented(constrained), decode_whole(constrained));
    }

    #[test]
    fn envelope_caps_the_plan_and_keeps_the_rest_as_wire() {
        let mut data = vec![64, 0x12, 0x34, 200];
        data.extend(0_u8..20);
        data.extend_from_slice(b"+OK\r\n");
        let parsed = split_case(&data);
        assert_eq!(parsed.limits.max_depth, 64);
        assert_eq!(parsed.limits.max_frame_size, 0x1234);
        assert_eq!(parsed.plan, &data[4..20]);
        assert_eq!(parsed.wire, &data[20..]);
    }
}
