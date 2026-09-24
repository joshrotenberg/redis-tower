//! Frame traversal before the recursive, allocating RESP parser.
//!
//! The only allocation is a stack proportional to aggregate nesting. Counts
//! declared by the peer never determine an allocation here.

use crate::{ProtocolError, RespLimits};
use resp_rs::ParseError;

// Match resp-rs 0.1.8's private parser ceilings. Keep these checks before the
// parser so an invalid header does not wait indefinitely for its payload.
const MAX_COLLECTION_SIZE: usize = 10_000_000;
const MAX_BLOB_SIZE: usize = 512 * 1024 * 1024;
// Both an empty simple string and a RESP3 null occupy three wire bytes.
const MIN_ELEMENT_SIZE: usize = 3;
// Ordinary Redis replies nest only a few levels. Keep those counts inline so
// scanning an array or map does not allocate; adversarial/deep input spills.
const INLINE_NESTING: usize = 8;

struct PendingStack {
    inline: [usize; INLINE_NESTING],
    inline_len: usize,
    spill: Vec<usize>,
}

impl PendingStack {
    fn new() -> Self {
        Self {
            inline: [0; INLINE_NESTING],
            inline_len: 0,
            spill: Vec::new(),
        }
    }

    fn len(&self) -> usize {
        self.inline_len + self.spill.len()
    }

    fn push(&mut self, value: usize) {
        if self.inline_len < INLINE_NESTING {
            self.inline[self.inline_len] = value;
            self.inline_len += 1;
        } else {
            self.spill.push(value);
        }
    }

    fn last(&self) -> Option<&usize> {
        self.spill
            .last()
            .or_else(|| self.inline[..self.inline_len].last())
    }

    fn last_mut(&mut self) -> Option<&mut usize> {
        if let Some(last) = self.spill.last_mut() {
            Some(last)
        } else {
            self.inline[..self.inline_len].last_mut()
        }
    }

    fn pop(&mut self) -> Option<usize> {
        if let Some(value) = self.spill.pop() {
            Some(value)
        } else if self.inline_len > 0 {
            self.inline_len -= 1;
            Some(self.inline[self.inline_len])
        } else {
            None
        }
    }
}

/// Determine the complete first frame's extent without materializing frames.
///
/// Byte limits include headers and terminators. Every aggregate, including an
/// empty one, adds a nesting level; a RESP2 null array is a nil leaf. Attributes
/// and streaming tokens are unsupported because they are not complete replies
/// in the client's current response model. Payload bytes remain opaque.
pub(crate) fn frame_len(buf: &[u8], limits: RespLimits) -> Result<Option<usize>, ProtocolError> {
    if buf.is_empty() {
        return Ok(None);
    }

    let mut cursor = 0;
    // Outstanding children for each open aggregate. A zero ancestor remains
    // open while its last child is itself an unfinished aggregate.
    let mut open = PendingStack::new();
    // Initially the root is owed. Thereafter this is the sum of open counts.
    let mut pending = 1usize;

    loop {
        let Some(&tag) = buf.get(cursor) else {
            check_size(cursor, pending, limits)?;
            return Ok(None);
        };
        pending -= 1;
        if let Some(remaining) = open.last_mut() {
            *remaining -= 1;
        }

        match tag {
            b'|' => return Err(unsupported("RESP3 attributed replies are not supported")),
            b';' | b'.' => return Err(unsupported("RESP3 streaming replies are not supported")),
            b'+' | b'-' | b':' | b'#' | b'(' | b',' => {
                let Some(end) = line_end(buf, cursor + 1, pending, limits)? else {
                    return Ok(None);
                };
                let contents = &buf[cursor + 1..end];
                match tag {
                    b':' => validate_integer(contents)?,
                    b'#' if contents != b"t" && contents != b"f" => {
                        return Err(ParseError::InvalidBoolean.into());
                    }
                    b',' => {
                        let text =
                            std::str::from_utf8(contents).map_err(|_| ParseError::Utf8Error)?;
                        text.parse::<f64>().map_err(|_| ParseError::InvalidFormat)?;
                    }
                    _ => {}
                }
                cursor = end + 2;
            }
            b'_' => {
                let end = checked_add(cursor, 3)?;
                check_size(end, pending, limits)?;
                if buf.get(cursor + 1).is_some_and(|byte| *byte != b'\r')
                    || buf.get(cursor + 2).is_some_and(|byte| *byte != b'\n')
                {
                    return Err(ParseError::InvalidFormat.into());
                }
                if end > buf.len() {
                    return Ok(None);
                }
                cursor = end;
            }
            b'$' | b'!' | b'=' => {
                let Some(end) = line_end(buf, cursor + 1, pending, limits)? else {
                    return Ok(None);
                };
                let header = &buf[cursor + 1..end];
                if header == b"?" {
                    return Err(unsupported("RESP3 streaming replies are not supported"));
                }
                let payload_start = end + 2;
                if tag == b'$' && header == b"-1" {
                    cursor = payload_start;
                } else {
                    let length = parse_length(header)?;
                    if length > MAX_BLOB_SIZE {
                        return Err(ParseError::BadLength.into());
                    }
                    let payload_end = checked_add(payload_start, length)?;
                    let frame_end = checked_add(payload_end, 2)?;
                    check_size(frame_end, pending, limits)?;
                    if buf.get(payload_end).is_some_and(|byte| *byte != b'\r')
                        || buf.get(payload_end + 1).is_some_and(|byte| *byte != b'\n')
                    {
                        return Err(ParseError::InvalidFormat.into());
                    }
                    if frame_end > buf.len() {
                        return Ok(None);
                    }
                    // A verbatim payload begins with a three-byte format tag
                    // and colon. Match the dependency's first-colon rule.
                    if tag == b'='
                        && buf[payload_start..payload_end]
                            .iter()
                            .position(|byte| *byte == b':')
                            != Some(3)
                    {
                        return Err(ParseError::InvalidFormat.into());
                    }
                    cursor = frame_end;
                }
            }
            b'*' | b'~' | b'>' | b'%' => {
                let Some(end) = line_end(buf, cursor + 1, pending, limits)? else {
                    return Ok(None);
                };
                let header = &buf[cursor + 1..end];
                if header == b"?" {
                    return Err(unsupported("RESP3 streaming replies are not supported"));
                }
                cursor = end + 2;
                if !(tag == b'*' && header == b"-1") {
                    let count = parse_length(header)?;
                    if count > MAX_COLLECTION_SIZE {
                        return Err(ParseError::BadLength.into());
                    }
                    if open.len() >= limits.max_depth {
                        return Err(ProtocolError::NestingTooDeep {
                            max: limits.max_depth,
                        });
                    }
                    let children = if tag == b'%' {
                        count.checked_mul(2).ok_or(ParseError::BadLength)?
                    } else {
                        count
                    };
                    pending = checked_add(pending, children)?;
                    check_size(cursor, pending, limits)?;
                    if children > 0 {
                        open.push(children);
                    }
                }
            }
            _ => return Err(ParseError::InvalidTag(tag).into()),
        }

        check_size(cursor, pending, limits)?;
        if pending == 0 {
            return Ok(Some(cursor));
        }
        while open.last() == Some(&0) {
            open.pop();
        }
    }
}

fn unsupported(message: &'static str) -> ProtocolError {
    std::io::Error::new(std::io::ErrorKind::Unsupported, message).into()
}

fn checked_add(left: usize, right: usize) -> Result<usize, ProtocolError> {
    left.checked_add(right)
        .ok_or_else(|| ParseError::BadLength.into())
}

/// Check the smallest possible completed root, including all unvisited siblings.
fn check_size(bytes: usize, pending: usize, limits: RespLimits) -> Result<(), ProtocolError> {
    let children = pending
        .checked_mul(MIN_ELEMENT_SIZE)
        .ok_or(ParseError::BadLength)?;
    let size = checked_add(bytes, children)?;
    if size > limits.max_frame_size {
        return Err(ProtocolError::FrameTooLarge {
            size,
            max: limits.max_frame_size,
        });
    }
    Ok(())
}

/// Find a header's CRLF, stopping as soon as the first frame cannot fit.
fn line_end(
    buf: &[u8],
    from: usize,
    pending: usize,
    limits: RespLimits,
) -> Result<Option<usize>, ProtocolError> {
    let mut cursor = from;
    while cursor + 1 < buf.len() {
        check_size(cursor + 2, pending, limits)?;
        if buf[cursor] == b'\r' && buf[cursor + 1] == b'\n' {
            return Ok(Some(cursor));
        }
        cursor += 1;
    }
    let terminator_bytes = if buf.last() == Some(&b'\r') { 1 } else { 2 };
    check_size(checked_add(buf.len(), terminator_bytes)?, pending, limits)?;
    Ok(None)
}

fn parse_length(header: &[u8]) -> Result<usize, ProtocolError> {
    if header.is_empty() {
        return Err(ParseError::BadLength.into());
    }
    let mut length = 0usize;
    for &digit in header {
        if !digit.is_ascii_digit() {
            return Err(ParseError::BadLength.into());
        }
        length = length
            .checked_mul(10)
            .and_then(|value| value.checked_add(usize::from(digit - b'0')))
            .ok_or(ParseError::BadLength)?;
    }
    Ok(length)
}

fn validate_integer(contents: &[u8]) -> Result<(), ProtocolError> {
    let (negative, digits) = match contents.strip_prefix(b"-") {
        Some(digits) => (true, digits),
        None => (false, contents),
    };
    if digits.is_empty() {
        return Err(ParseError::InvalidFormat.into());
    }
    let mut value = 0i64;
    for (index, &digit) in digits.iter().enumerate() {
        if !digit.is_ascii_digit() {
            return Err(ParseError::InvalidFormat.into());
        }
        let digit = i64::from(digit - b'0');
        // Match resp-rs's special case and error ordering at i64::MIN: the
        // extra negative digit is accepted only at the end of the token.
        if negative && value == i64::MAX / 10 && digit == 8 && index == digits.len() - 1 {
            return Ok(());
        }
        if value > i64::MAX / 10 || (value == i64::MAX / 10 && digit > i64::MAX % 10) {
            return Err(ParseError::Overflow.into());
        }
        value = value * 10 + digit;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(size: usize, depth: usize) -> RespLimits {
        RespLimits {
            max_frame_size: size,
            max_depth: depth,
        }
    }

    #[test]
    fn completed_extent_and_pending_ancestor_lower_bound_are_exact() {
        let wire = b"*2\r\n*1\r\n_\r\n_\r\n";
        assert_eq!(frame_len(wire, limits(14, 2)).unwrap(), Some(14));
        // The nested array still owes its child and its parent owes a sibling.
        assert!(matches!(
            frame_len(b"*2\r\n*1\r\n", limits(13, 2)),
            Err(ProtocolError::FrameTooLarge { size: 14, max: 13 })
        ));
        assert_eq!(frame_len(b"*2\r\n*1\r\n", limits(14, 2)).unwrap(), None);
    }

    #[test]
    fn incomplete_lines_account_for_the_missing_terminator() {
        assert_eq!(frame_len(b"+OK", limits(5, 0)).unwrap(), None);
        assert_eq!(frame_len(b"+OK\r", limits(5, 0)).unwrap(), None);
        for wire in [b"+OK".as_slice(), b"+OK\r"] {
            assert!(matches!(
                frame_len(wire, limits(4, 0)),
                Err(ProtocolError::FrameTooLarge { size: 5, max: 4 })
            ));
        }
    }

    #[test]
    fn declared_counts_never_require_visiting_missing_children() {
        for header in [b"*10000000\r\n".as_slice(), b"%10000000\r\n"] {
            assert_eq!(frame_len(header, RespLimits::default()).unwrap(), None);
        }
        for header in [b"*4096\r\n".as_slice(), b"%4096\r\n"] {
            assert!(matches!(
                frame_len(header, limits(64, 2)),
                Err(ProtocolError::FrameTooLarge { .. })
            ));
        }
    }

    #[test]
    fn every_finite_frame_prefix_is_incomplete_until_the_boundary() {
        for wire in [
            b"+OK\r\n".as_slice(),
            b"$3\r\n\0\xffx\r\n",
            b"%1\r\n+k\r\n*2\r\n:1\r\n_\r\n",
            b"=7\r\ntxt:abc\r\n",
        ] {
            for end in 0..wire.len() {
                assert_eq!(
                    frame_len(&wire[..end], RespLimits::default()).unwrap(),
                    None,
                    "prefix {end} of {wire:?}"
                );
            }
            assert_eq!(
                frame_len(wire, limits(wire.len(), 4)).unwrap(),
                Some(wire.len())
            );
        }
    }

    #[test]
    fn empty_containers_have_depth_but_null_arrays_do_not() {
        for wire in [b"*0\r\n".as_slice(), b"%0\r\n", b"~0\r\n", b">0\r\n"] {
            assert!(matches!(
                frame_len(wire, limits(32, 0)),
                Err(ProtocolError::NestingTooDeep { max: 0 })
            ));
            assert_eq!(frame_len(wire, limits(32, 1)).unwrap(), Some(4));
        }
        assert_eq!(frame_len(b"*-1\r\n", limits(32, 0)).unwrap(), Some(5));
        assert_eq!(frame_len(b"*1\r\n*-1\r\n", limits(32, 1)).unwrap(), Some(9));
        assert!(matches!(
            frame_len(b"*1\r\n*0\r\n", limits(32, 1)),
            Err(ProtocolError::NestingTooDeep { max: 1 })
        ));
    }

    #[test]
    fn unsupported_tokens_are_only_inspected_at_element_boundaries() {
        for token in [
            b"$?\r\n".as_slice(),
            b"!?\r\n",
            b"=?\r\n",
            b"*?\r\n",
            b"~?\r\n",
            b">?\r\n",
            b"%?\r\n",
            b"|0\r\n",
            b"|?\r\n",
            b";0\r\n",
            b".\r\n",
        ] {
            assert!(matches!(
                frame_len(token, RespLimits::default()),
                Err(ProtocolError::Io(error)) if error.kind() == std::io::ErrorKind::Unsupported
            ));
            let mut pipeline = b"+OK\r\n".to_vec();
            pipeline.extend_from_slice(token);
            assert_eq!(frame_len(&pipeline, limits(5, 0)).unwrap(), Some(5));
            let mut payload = format!("${}\r\n", token.len()).into_bytes();
            payload.extend_from_slice(token);
            payload.extend_from_slice(b"\r\n");
            assert_eq!(
                frame_len(&payload, limits(payload.len(), 0)).unwrap(),
                Some(payload.len())
            );
        }
    }

    #[test]
    fn malformed_lengths_and_payload_terminators_fail_before_parsing() {
        for wire in [
            b"*10000001\r\n".as_slice(),
            b"%10000001\r\n",
            b"$536870913\r\n",
            b"!536870913\r\n",
            b"=536870913\r\n",
            b"$\r\n",
            b"*+1\r\n",
            b"%-1\r\n",
            b"!-1\r\n",
            b"=-1\r\n",
            b"$9999999999999999999999999999\r\n",
        ] {
            assert!(
                matches!(
                    frame_len(wire, RespLimits::default()),
                    Err(ProtocolError::Parse(ParseError::BadLength))
                ),
                "wire: {wire:?}"
            );
        }
        for wire in [b"$3\r\nabcXY".as_slice(), b"=3\r\nabc\r\n", b"_x\r\n"] {
            assert!(matches!(
                frame_len(wire, RespLimits::default()),
                Err(ProtocolError::Parse(ParseError::InvalidFormat))
            ));
        }
    }

    #[test]
    fn scalar_error_categories_match_the_dependency() {
        for wire in [
            b":-9223372036854775808\r\n".as_slice(),
            b":9223372036854775807\r\n",
            b":-9223372036854775809\r\n",
            b":-9223372036854775808x\r\n",
            b":9223372036854775808\r\n",
            b":+1\r\n",
            b":-\r\n",
            b"#true\r\n",
            b",wat\r\n",
            b",\xff\r\n",
        ] {
            let actual = frame_len(wire, RespLimits::default());
            let expected = resp_rs::resp3::parse_frame(bytes::Bytes::copy_from_slice(wire));
            match (actual, expected) {
                (Ok(Some(length)), Ok((_, remainder))) => {
                    assert_eq!(length, wire.len());
                    assert!(remainder.is_empty());
                }
                (Err(ProtocolError::Parse(actual)), Err(expected)) => assert_eq!(actual, expected),
                (actual, expected) => {
                    panic!("different outcomes for {wire:?}: {actual:?}, {expected:?}")
                }
            }
        }
    }
}
