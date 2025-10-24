//! Redis connection management commands

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// AUTH command - Authenticate to the server with a password
#[derive(Debug, Clone)]
pub struct Auth {
    password: String,
}

impl Auth {
    /// Create a new AUTH command
    pub fn new(password: impl Into<String>) -> Self {
        Self {
            password: password.into(),
        }
    }
}

impl Command for Auth {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("AUTH"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.password.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Expected OK response".to_string())),
        }
    }
}

/// AUTH command with username (ACL authentication)
#[derive(Debug, Clone)]
pub struct AuthAcl {
    username: String,
    password: String,
}

impl AuthAcl {
    /// Create a new AUTH command with username
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

impl Command for AuthAcl {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("AUTH"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.username.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.password.as_bytes()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Expected OK response".to_string())),
        }
    }
}

/// READONLY command - Enable read-only mode for replica connections
#[derive(Debug, Clone, Copy)]
pub struct ReadOnly;

impl Command for ReadOnly {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("READONLY")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Expected OK response".to_string())),
        }
    }
}

/// READWRITE command - Disable read-only mode for replica connections
#[derive(Debug, Clone, Copy)]
pub struct ReadWrite;

impl Command for ReadWrite {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("READWRITE")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Expected OK response".to_string())),
        }
    }
}

/// SELECT command - Change the selected database
#[derive(Debug, Clone)]
pub struct Select {
    db: u32,
}

impl Select {
    /// Create a new SELECT command
    pub fn new(db: u32) -> Self {
        Self { db }
    }
}

impl Command for Select {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("SELECT"))),
            Frame::BulkString(Some(Bytes::from(self.db.to_string()))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Expected OK response".to_string())),
        }
    }
}

/// QUIT command - Close the connection
#[derive(Debug, Clone, Copy, Default)]
pub struct Quit;

impl Quit {
    /// Create a new QUIT command
    pub fn new() -> Self {
        Self
    }
}

impl Command for Quit {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![Frame::BulkString(Some(Bytes::from("QUIT")))])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if s == "OK" => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::Protocol("Expected OK response".to_string())),
        }
    }
}

/// CLIENT GETNAME command - Get the current connection name
///
/// Returns the name of the current connection as set by CLIENT SETNAME,
/// or None if no name is set.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientGetName;
///
/// let cmd = ClientGetName;
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ClientGetName;

impl Command for ClientGetName {
    type Response = Option<String>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("GETNAME"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => Ok(Some(String::from_utf8_lossy(&data).into_owned())),
            Frame::BulkString(None) | Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// CLIENT SETNAME command - Set the current connection name
///
/// Assigns a name to the current connection. The name can be displayed
/// in CLIENT LIST output and is useful for debugging and monitoring.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ClientSetName;
///
/// let cmd = ClientSetName::new("my-app-connection");
/// ```
#[derive(Debug, Clone)]
pub struct ClientSetName {
    name: String,
}

impl ClientSetName {
    /// Create a new CLIENT SETNAME command
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Command for ClientSetName {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("CLIENT"))),
            Frame::BulkString(Some(Bytes::from("SETNAME"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.name.as_bytes()))),
        ])
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
    fn test_auth_frame() {
        let cmd = Auth::new("mypassword");
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_auth_acl_frame() {
        let cmd = AuthAcl::new("default", "mypassword");
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 3);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_readonly_frame() {
        let cmd = ReadOnly;
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 1);
        } else {
            panic!("Expected array frame");
        }
    }

    #[test]
    fn test_select_frame() {
        let cmd = Select::new(1);
        let frame = cmd.to_frame();

        if let Frame::Array(elements) = frame {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("Expected array frame");
        }
    }
}
