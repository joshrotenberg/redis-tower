use std::marker::PhantomData;

use redis_tower_core::{Command, Frame, FromFrame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// Execute an arbitrary Redis command by name.
///
/// Use this for commands not covered by typed structs, custom modules,
/// or commands added in newer Redis versions.
///
/// # Example
///
/// ```ignore
/// let result = conn.execute(RawCommand::new("CUSTOM.CMD").arg("key").arg("val")).await?;
/// ```
#[derive(Clone)]
pub struct RawCommand {
    name_str: String,
    args: Vec<Vec<u8>>,
}

impl RawCommand {
    /// Create a new raw command with the given command name.
    pub fn new(name: impl Into<String>) -> Self {
        let name_str = name.into();
        Self {
            name_str,
            args: Vec::new(),
        }
    }

    /// Append an argument to this command.
    pub fn arg(mut self, arg: impl AsRef<[u8]>) -> Self {
        self.args.push(arg.as_ref().to_vec());
        self
    }

    /// Decode this command's reply into a typed value `T` instead of a raw [`Frame`].
    ///
    /// Returns a [`TypedRawCommand`] whose `Response` is `T`, so `execute`
    /// yields a decoded value. `T` can be any type implementing
    /// [`FromFrame`](redis_tower_core::FromFrame): scalars, `String`, `Bytes`,
    /// `Option<T>`, `Vec<T>`, tuples, and `HashMap<K, V>`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let count: i64 = conn.execute(RawCommand::new("SCARD").arg("s").query()).await?;
    /// let members: Vec<String> =
    ///     conn.execute(RawCommand::new("SMEMBERS").arg("s").query()).await?;
    /// ```
    pub fn query<T: FromFrame>(self) -> TypedRawCommand<T> {
        TypedRawCommand {
            inner: self,
            _marker: PhantomData,
        }
    }
}

/// A [`RawCommand`] that decodes its reply into a typed value `T`.
///
/// Created by [`RawCommand::query`]. Sends the exact same wire bytes as the
/// underlying `RawCommand`, but [`parse_response`](Command::parse_response)
/// runs `T::from_frame` so `execute` returns a `T` rather than a `Frame`.
pub struct TypedRawCommand<T> {
    inner: RawCommand,
    // `fn() -> T` keeps `TypedRawCommand` `Send`/`Sync` regardless of `T`.
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for TypedRawCommand<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: FromFrame + Send + 'static> Command for TypedRawCommand<T> {
    type Response = T;

    fn to_frame(&self) -> Frame {
        self.inner.to_frame()
    }

    fn parse_response(&self, frame: Frame) -> Result<T, RedisError> {
        T::from_frame(frame)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}

impl Command for RawCommand {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut frames = Vec::with_capacity(1 + self.args.len());
        frames.push(bulk(self.name_str.as_str()));
        for arg in &self.args {
            frames.push(bulk(arg.as_slice()));
        }
        array(frames)
    }

    fn parse_response(&self, frame: Frame) -> Result<Frame, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        &self.name_str
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn to_frame_single_command() {
        let cmd = RawCommand::new("PING");
        let frame = cmd.to_frame();
        let expected = Frame::Array(Some(vec![Frame::BulkString(Some(Bytes::from("PING")))]));
        assert_eq!(frame, expected);
    }

    #[test]
    fn to_frame_with_args() {
        let cmd = RawCommand::new("SET").arg("key").arg("value");
        let frame = cmd.to_frame();
        let expected = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("SET"))),
            Frame::BulkString(Some(Bytes::from("key"))),
            Frame::BulkString(Some(Bytes::from("value"))),
        ]));
        assert_eq!(frame, expected);
    }

    #[test]
    fn parse_response_passes_through() {
        let cmd = RawCommand::new("PING");
        let frame = Frame::SimpleString(Bytes::from("PONG"));
        let result = cmd.parse_response(frame.clone()).unwrap();
        assert_eq!(result, frame);
    }

    #[test]
    fn name_returns_command_name() {
        let cmd = RawCommand::new("CUSTOM.CMD");
        assert_eq!(cmd.name(), "CUSTOM.CMD");
    }

    #[test]
    fn multiple_args_binary() {
        let cmd = RawCommand::new("SET")
            .arg(b"bin\x00key".as_slice())
            .arg(b"\xff\xfe".as_slice());
        let frame = cmd.to_frame();
        match frame {
            Frame::Array(Some(frames)) => assert_eq!(frames.len(), 3),
            other => panic!("expected array, got {other:?}"),
        }
    }

    // -- TypedRawCommand (query) --

    #[test]
    fn query_sends_same_wire_bytes() {
        let raw = RawCommand::new("INCR").arg("ctr");
        let expected = raw.to_frame();
        let typed = RawCommand::new("INCR").arg("ctr").query::<i64>();
        assert_eq!(typed.to_frame(), expected);
        assert_eq!(typed.name(), "INCR");
    }

    #[test]
    fn query_decodes_integer() {
        let typed = RawCommand::new("INCR").arg("ctr").query::<i64>();
        let n = typed.parse_response(Frame::Integer(7)).unwrap();
        assert_eq!(n, 7);
    }

    #[test]
    fn query_decodes_vec_of_strings() {
        let typed = RawCommand::new("SMEMBERS").arg("s").query::<Vec<String>>();
        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Some(Bytes::from("a"))),
            Frame::BulkString(Some(Bytes::from("b"))),
        ]));
        let v = typed.parse_response(frame).unwrap();
        assert_eq!(v, vec!["a", "b"]);
    }

    #[test]
    fn query_decodes_option_none_on_nil() {
        let typed = RawCommand::new("GET")
            .arg("missing")
            .query::<Option<String>>();
        let v = typed.parse_response(Frame::BulkString(None)).unwrap();
        assert_eq!(v, None);
    }

    #[test]
    fn query_surfaces_error_frame() {
        let typed = RawCommand::new("GET").arg("k").query::<String>();
        let r = typed.parse_response(Frame::Error(Bytes::from("WRONGTYPE nope")));
        assert!(r.is_err());
    }

    #[test]
    fn query_to_frame_is_identity() {
        let typed = RawCommand::new("PING").query::<Frame>();
        let echoed = typed
            .parse_response(Frame::SimpleString(Bytes::from("PONG")))
            .unwrap();
        assert_eq!(echoed, Frame::SimpleString(Bytes::from("PONG")));
    }

    #[test]
    fn typed_raw_command_is_clone_and_send() {
        fn assert_send<T: Send>() {}
        assert_send::<TypedRawCommand<String>>();
        let typed = RawCommand::new("GET").arg("k").query::<String>();
        let _clone = typed.clone();
    }
}
