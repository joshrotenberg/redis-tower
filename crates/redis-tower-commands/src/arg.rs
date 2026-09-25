//! Binary-safe arguments shared by typed Redis commands.

use std::borrow::Cow;
use std::fmt;

use bytes::Bytes;

/// An owned, binary-safe Redis command argument.
///
/// Redis keys, values, fields, and members are byte strings, not UTF-8 text.
/// `CommandArg` lets typed command builders accept those bytes without giving
/// up the convenient string call sites used by most applications.
///
/// Owned [`String`], [`Vec<u8>`], and [`Bytes`] inputs reuse their allocation
/// (or shared storage). Borrowed strings, byte slices, and arrays are copied
/// once because command values must be owned and may outlive the caller.
/// Cloning a `CommandArg` is cheap because [`Bytes`] uses shared storage.
///
/// # Examples
///
/// ```
/// use bytes::Bytes;
/// use redis_tower_commands::CommandArg;
///
/// let text = CommandArg::from("users:42");
/// let invalid_utf8 = CommandArg::from(&b"key:\xff"[..]);
/// let shared = CommandArg::from(Bytes::from_static(b"payload"));
///
/// assert_eq!(text.as_bytes(), b"users:42");
/// assert_eq!(invalid_utf8.as_bytes(), b"key:\xff");
/// assert_eq!(shared.as_bytes(), b"payload");
/// ```
#[derive(Clone, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CommandArg(Bytes);

impl CommandArg {
    /// Construct an argument by copying a borrowed byte slice.
    pub fn copy_from_slice(value: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(value))
    }

    /// Construct an argument backed by static bytes without copying.
    pub const fn from_static(value: &'static [u8]) -> Self {
        Self(Bytes::from_static(value))
    }

    /// Borrow the exact bytes that will be sent to Redis.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consume the argument and return its shared byte storage.
    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    /// Return the argument length in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Return `true` when the argument contains no bytes.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for CommandArg {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("CommandArg").field(&self.0).finish()
    }
}

impl AsRef<[u8]> for CommandArg {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Bytes> for CommandArg {
    fn from(value: Bytes) -> Self {
        Self(value)
    }
}

impl From<&Bytes> for CommandArg {
    fn from(value: &Bytes) -> Self {
        Self(value.clone())
    }
}

impl From<&CommandArg> for CommandArg {
    fn from(value: &CommandArg) -> Self {
        value.clone()
    }
}

impl From<CommandArg> for Bytes {
    fn from(value: CommandArg) -> Self {
        value.into_bytes()
    }
}

impl From<Vec<u8>> for CommandArg {
    fn from(value: Vec<u8>) -> Self {
        Self(Bytes::from(value))
    }
}

impl From<&Vec<u8>> for CommandArg {
    fn from(value: &Vec<u8>) -> Self {
        Self::copy_from_slice(value)
    }
}

impl From<String> for CommandArg {
    fn from(value: String) -> Self {
        Self(Bytes::from(value))
    }
}

impl From<Box<str>> for CommandArg {
    fn from(value: Box<str>) -> Self {
        Self::from(String::from(value))
    }
}

impl From<char> for CommandArg {
    fn from(value: char) -> Self {
        let mut encoded = [0; 4];
        Self::from(value.encode_utf8(&mut encoded))
    }
}

impl From<&str> for CommandArg {
    fn from(value: &str) -> Self {
        Self::copy_from_slice(value.as_bytes())
    }
}

impl From<&mut str> for CommandArg {
    fn from(value: &mut str) -> Self {
        Self::from(&*value)
    }
}

impl From<&String> for CommandArg {
    fn from(value: &String) -> Self {
        Self::from(value.as_str())
    }
}

impl From<&[u8]> for CommandArg {
    fn from(value: &[u8]) -> Self {
        Self::copy_from_slice(value)
    }
}

impl<const N: usize> From<[u8; N]> for CommandArg {
    fn from(value: [u8; N]) -> Self {
        Self::copy_from_slice(&value)
    }
}

impl<const N: usize> From<&[u8; N]> for CommandArg {
    fn from(value: &[u8; N]) -> Self {
        Self::copy_from_slice(value)
    }
}

impl<'a> From<Cow<'a, str>> for CommandArg {
    fn from(value: Cow<'a, str>) -> Self {
        match value {
            Cow::Borrowed(value) => Self::from(value),
            Cow::Owned(value) => Self::from(value),
        }
    }
}

impl<'a> From<Cow<'a, [u8]>> for CommandArg {
    fn from(value: Cow<'a, [u8]>) -> Self {
        match value {
            Cow::Borrowed(value) => Self::from(value),
            Cow::Owned(value) => Self::from(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_every_byte_from_supported_inputs() {
        let expected = b"\xff\x00\r\n";
        assert_eq!(CommandArg::from(expected).as_bytes(), expected);
        assert_eq!(CommandArg::from(expected.as_slice()).as_bytes(), expected);
        assert_eq!(CommandArg::from(expected.to_vec()).as_bytes(), expected);
        assert_eq!(
            CommandArg::from(Bytes::copy_from_slice(expected)).as_bytes(),
            expected
        );
    }

    #[test]
    fn accepts_text_and_empty_values() {
        assert_eq!(CommandArg::from("hello").as_bytes(), b"hello");
        assert!(CommandArg::from(String::new()).is_empty());
        assert!(CommandArg::from(Vec::<u8>::new()).is_empty());
    }
}
