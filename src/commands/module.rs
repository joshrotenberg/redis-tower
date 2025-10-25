//! Redis module management commands
//!
//! Commands for loading, unloading, and inspecting Redis modules.
//!
//! Available since Redis 4.0.0

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// Information about a loaded Redis module
#[derive(Debug, Clone)]
pub struct ModuleInfo {
    /// Module name
    pub name: String,
    /// Module version
    pub version: i64,
}

/// MODULE LIST - List all loaded modules
///
/// Returns information about all loaded modules.
///
/// Available since Redis 4.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ModuleList;
///
/// let cmd = ModuleList;
/// // Returns array of module info
/// ```
#[derive(Debug, Clone, Copy)]
pub struct ModuleList;

impl Command for ModuleList {
    type Response = Vec<ModuleInfo>;

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("MODULE"))),
            Frame::BulkString(Some(Bytes::from("LIST"))),
        ])
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(modules) => {
                let mut result = Vec::new();

                for module in modules {
                    match module {
                        Frame::Array(fields) => {
                            let mut name = String::new();
                            let mut version = 0i64;

                            // Parse key-value pairs
                            let mut i = 0;
                            while i + 1 < fields.len() {
                                let key = match &fields[i] {
                                    Frame::BulkString(Some(data)) | Frame::SimpleString(data) => {
                                        String::from_utf8_lossy(data).to_string()
                                    }
                                    _ => {
                                        i += 2;
                                        continue;
                                    }
                                };

                                match key.as_str() {
                                    "name" => {
                                        if let Frame::BulkString(Some(data))
                                        | Frame::SimpleString(data) = &fields[i + 1]
                                        {
                                            name = String::from_utf8_lossy(data).to_string();
                                        }
                                    }
                                    "ver" => {
                                        if let Frame::Integer(v) = &fields[i + 1] {
                                            version = *v;
                                        }
                                    }
                                    _ => {}
                                }

                                i += 2;
                            }

                            result.push(ModuleInfo { name, version });
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }

                Ok(result)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// MODULE LOAD - Load a module
///
/// Loads a module from a dynamic library at runtime.
///
/// Available since Redis 4.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ModuleLoad;
///
/// // Load module without arguments
/// let cmd = ModuleLoad::new("/path/to/module.so");
///
/// // Load module with arguments
/// let cmd = ModuleLoad::new("/path/to/module.so")
///     .with_args(vec!["arg1", "arg2"]);
/// ```
#[derive(Debug, Clone)]
pub struct ModuleLoad {
    path: String,
    args: Vec<String>,
}

impl ModuleLoad {
    /// Create a new MODULE LOAD command
    ///
    /// # Arguments
    /// * `path` - Path to the module's dynamic library
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            args: Vec::new(),
        }
    }

    /// Add arguments to pass to the module on load
    pub fn with_args(mut self, args: Vec<impl Into<String>>) -> Self {
        self.args = args.into_iter().map(|a| a.into()).collect();
        self
    }
}

impl Command for ModuleLoad {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("MODULE"))),
            Frame::BulkString(Some(Bytes::from("LOAD"))),
            Frame::BulkString(Some(Bytes::from(self.path.clone()))),
        ];

        for arg in &self.args {
            frames.push(Frame::BulkString(Some(Bytes::from(arg.clone()))));
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// MODULE LOADEX - Load a module with extended parameters
///
/// Loads a module with extended parameters for configuration and policy.
///
/// Available since Redis 7.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ModuleLoadEx;
///
/// // Load module with configuration
/// let cmd = ModuleLoadEx::new("/path/to/module.so")
///     .config("setting", "value")
///     .with_args(vec!["arg1", "arg2"]);
/// ```
#[derive(Debug, Clone)]
pub struct ModuleLoadEx {
    path: String,
    configs: Vec<(String, String)>,
    args: Vec<String>,
}

impl ModuleLoadEx {
    /// Create a new MODULE LOADEX command
    ///
    /// # Arguments
    /// * `path` - Path to the module's dynamic library
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            configs: Vec::new(),
            args: Vec::new(),
        }
    }

    /// Add a configuration parameter
    pub fn config(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.push((name.into(), value.into()));
        self
    }

    /// Add arguments to pass to the module on load
    pub fn with_args(mut self, args: Vec<impl Into<String>>) -> Self {
        self.args = args.into_iter().map(|a| a.into()).collect();
        self
    }
}

impl Command for ModuleLoadEx {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut frames = vec![
            Frame::BulkString(Some(Bytes::from("MODULE"))),
            Frame::BulkString(Some(Bytes::from("LOADEX"))),
            Frame::BulkString(Some(Bytes::from(self.path.clone()))),
        ];

        // Add CONFIG parameters
        for (name, value) in &self.configs {
            frames.push(Frame::BulkString(Some(Bytes::from("CONFIG"))));
            frames.push(Frame::BulkString(Some(Bytes::from(name.clone()))));
            frames.push(Frame::BulkString(Some(Bytes::from(value.clone()))));
        }

        // Add ARGS
        if !self.args.is_empty() {
            frames.push(Frame::BulkString(Some(Bytes::from("ARGS"))));
            for arg in &self.args {
                frames.push(Frame::BulkString(Some(Bytes::from(arg.clone()))));
            }
        }

        Frame::Array(frames)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(_) => Ok(()),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// MODULE UNLOAD - Unload a module
///
/// Unloads a module by name.
///
/// Available since Redis 4.0.0
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::ModuleUnload;
///
/// let cmd = ModuleUnload::new("mymodule");
/// ```
#[derive(Debug, Clone)]
pub struct ModuleUnload {
    name: String,
}

impl ModuleUnload {
    /// Create a new MODULE UNLOAD command
    ///
    /// # Arguments
    /// * `name` - Name of the module to unload
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl Command for ModuleUnload {
    type Response = ();

    fn to_frame(&self) -> Frame {
        Frame::Array(vec![
            Frame::BulkString(Some(Bytes::from("MODULE"))),
            Frame::BulkString(Some(Bytes::from("UNLOAD"))),
            Frame::BulkString(Some(Bytes::from(self.name.clone()))),
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
    fn test_module_list_frame() {
        let cmd = ModuleList;
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 2);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MODULE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("LIST"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_module_list_response() {
        // Simulate MODULE LIST response
        let frame = Frame::Array(vec![
            Frame::Array(vec![
                Frame::BulkString(Some(Bytes::from("name"))),
                Frame::BulkString(Some(Bytes::from("ReJSON"))),
                Frame::BulkString(Some(Bytes::from("ver"))),
                Frame::Integer(20000),
            ]),
            Frame::Array(vec![
                Frame::BulkString(Some(Bytes::from("name"))),
                Frame::BulkString(Some(Bytes::from("RediSearch"))),
                Frame::BulkString(Some(Bytes::from("ver"))),
                Frame::Integer(20600),
            ]),
        ]);

        let result = ModuleList::parse_response(frame).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "ReJSON");
        assert_eq!(result[0].version, 20000);
        assert_eq!(result[1].name, "RediSearch");
        assert_eq!(result[1].version, 20600);
    }

    #[test]
    fn test_module_load_frame() {
        let cmd = ModuleLoad::new("/path/to/module.so");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MODULE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("LOAD"))));
                assert_eq!(
                    parts[2],
                    Frame::BulkString(Some(Bytes::from("/path/to/module.so")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_module_load_with_args_frame() {
        let cmd = ModuleLoad::new("/path/to/module.so").with_args(vec!["arg1", "arg2"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 5);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MODULE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("LOAD"))));
                assert_eq!(
                    parts[2],
                    Frame::BulkString(Some(Bytes::from("/path/to/module.so")))
                );
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("arg1"))));
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("arg2"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_module_loadex_frame() {
        let cmd = ModuleLoadEx::new("/path/to/module.so");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MODULE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("LOADEX"))));
                assert_eq!(
                    parts[2],
                    Frame::BulkString(Some(Bytes::from("/path/to/module.so")))
                );
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_module_loadex_with_config_frame() {
        let cmd = ModuleLoadEx::new("/path/to/module.so").config("setting", "value");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 6);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MODULE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("LOADEX"))));
                assert_eq!(
                    parts[2],
                    Frame::BulkString(Some(Bytes::from("/path/to/module.so")))
                );
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("CONFIG"))));
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("setting"))));
                assert_eq!(parts[5], Frame::BulkString(Some(Bytes::from("value"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_module_loadex_with_args_frame() {
        let cmd = ModuleLoadEx::new("/path/to/module.so").with_args(vec!["arg1", "arg2"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 6);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MODULE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("LOADEX"))));
                assert_eq!(
                    parts[2],
                    Frame::BulkString(Some(Bytes::from("/path/to/module.so")))
                );
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("ARGS"))));
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("arg1"))));
                assert_eq!(parts[5], Frame::BulkString(Some(Bytes::from("arg2"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_module_loadex_with_config_and_args_frame() {
        let cmd = ModuleLoadEx::new("/path/to/module.so")
            .config("setting", "value")
            .with_args(vec!["arg1"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 8);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MODULE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("LOADEX"))));
                assert_eq!(
                    parts[2],
                    Frame::BulkString(Some(Bytes::from("/path/to/module.so")))
                );
                assert_eq!(parts[3], Frame::BulkString(Some(Bytes::from("CONFIG"))));
                assert_eq!(parts[4], Frame::BulkString(Some(Bytes::from("setting"))));
                assert_eq!(parts[5], Frame::BulkString(Some(Bytes::from("value"))));
                assert_eq!(parts[6], Frame::BulkString(Some(Bytes::from("ARGS"))));
                assert_eq!(parts[7], Frame::BulkString(Some(Bytes::from("arg1"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_module_unload_frame() {
        let cmd = ModuleUnload::new("mymodule");
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(parts) => {
                assert_eq!(parts.len(), 3);
                assert_eq!(parts[0], Frame::BulkString(Some(Bytes::from("MODULE"))));
                assert_eq!(parts[1], Frame::BulkString(Some(Bytes::from("UNLOAD"))));
                assert_eq!(parts[2], Frame::BulkString(Some(Bytes::from("mymodule"))));
            }
            _ => panic!("Expected Array frame"),
        }
    }
}
