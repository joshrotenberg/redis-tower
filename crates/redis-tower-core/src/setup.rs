//! Declarative, replayable settings for every physical Redis connection.

use crate::Frame;
use bytes::Bytes;
use redis_tower_protocol::helpers::{array, bulk};

/// Safe connection-local settings replayed before a connection is exposed.
///
/// Attach this policy to [`crate::ConnectionConfig::with_setup`]. Built-in
/// reconnect factories and Cluster clients retain it for every fresh socket.
/// Setup runs after authentication, database selection (URL connections), and
/// protocol negotiation; Cluster replica `READONLY` follows it. Commands run
/// sequentially, not as a batch, and every reply must be `+OK`. Any failure
/// closes the socket and returns a redacted [`crate::RedisError::Connection`].
///
/// Only replay-safe, connection-local commands are offered. Arbitrary hooks
/// could change authentication, database, reply mode, transaction state, or
/// Pub/Sub/tracking ownership behind the client's back. Use the dedicated
/// session APIs for those operations instead. Applying a one-time command on
/// a borrowed connection does **not** change its logical client's policy.
///
/// Debug output redacts the client name. Setup errors omit server reply text
/// and command arguments, since even error replies may echo sensitive data.
/// Names are visible to Redis administrators via `CLIENT LIST`; do not put
/// secrets in them.
///
/// ```
/// use redis_tower_core::{ConnectionConfig, ConnectionSetup};
/// let setup = ConnectionSetup::new().with_client_name("redis-mcp");
/// let config = ConnectionConfig::new().with_setup(setup);
/// assert!(!config.setup().is_empty());
/// assert!(!format!("{config:?}").contains("redis-mcp"));
/// ```
#[derive(Clone, Default)]
pub struct ConnectionSetup {
    client_name: Option<Bytes>,
    no_evict: Option<bool>,
    no_touch: Option<bool>,
}

impl ConnectionSetup {
    /// Create an empty policy (no additional Redis commands).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the logical client's name on every physical connection.
    ///
    /// Redis rejects names containing spaces or control characters. An empty
    /// name clears an existing name. This command requires Redis 2.6.9+ and
    /// the ACL permission `+client|setname`. Repeated calls replace the name.
    #[must_use]
    pub fn with_client_name(mut self, name: impl AsRef<[u8]>) -> Self {
        self.client_name = Some(Bytes::copy_from_slice(name.as_ref()));
        self
    }

    /// Configure `CLIENT NO-EVICT ON|OFF` on every physical connection.
    ///
    /// Requires Redis 7.0+ and `+client|no-evict`. Older servers or ACL denial
    /// fail setup; there is no silent downgrade. Repeated calls replace the flag.
    #[must_use]
    pub fn with_client_no_evict(mut self, enabled: bool) -> Self {
        self.no_evict = Some(enabled);
        self
    }

    /// Configure `CLIENT NO-TOUCH ON|OFF` on every physical connection.
    ///
    /// Requires Redis 7.2+ and `+client|no-touch`. Older servers or ACL denial
    /// fail setup; there is no silent downgrade. Repeated calls replace the flag.
    #[must_use]
    pub fn with_client_no_touch(mut self, enabled: bool) -> Self {
        self.no_touch = Some(enabled);
        self
    }

    /// Return whether this policy sends no additional setup commands.
    pub fn is_empty(&self) -> bool {
        self.client_name.is_none() && self.no_evict.is_none() && self.no_touch.is_none()
    }

    pub(crate) fn commands(&self) -> Vec<(&'static str, Frame)> {
        let mut commands = Vec::new();
        if let Some(name) = &self.client_name {
            commands.push((
                "CLIENT SETNAME",
                array(vec![bulk("CLIENT"), bulk("SETNAME"), bulk(name)]),
            ));
        }
        for (command, enabled) in [("NO-EVICT", self.no_evict), ("NO-TOUCH", self.no_touch)] {
            if let Some(enabled) = enabled {
                commands.push((
                    command,
                    array(vec![
                        bulk("CLIENT"),
                        bulk(command),
                        bulk(if enabled { "ON" } else { "OFF" }),
                    ]),
                ));
            }
        }
        commands
    }
}

impl std::fmt::Debug for ConnectionSetup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionSetup")
            .field(
                "client_name",
                &self.client_name.as_ref().map(|_| "[REDACTED]"),
            )
            .field("no_evict", &self.no_evict)
            .field("no_touch", &self.no_touch)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConnectionConfig, RedisConnection, RedisError, RedisStream, RespCodec};
    use futures::{SinkExt, StreamExt};
    use tokio_util::codec::Framed;

    async fn pair() -> (RedisConnection, Framed<RedisStream, RespCodec>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (client, server) = tokio::join!(
            tokio::net::TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        (
            RedisConnection::from_stream(RedisStream::Tcp(client.unwrap())),
            Framed::new(RedisStream::Tcp(server.unwrap().0), RespCodec::new()),
        )
    }

    #[test]
    fn empty_policy_and_name_replacement_are_redacted() {
        assert!(ConnectionSetup::new().is_empty());
        let config = ConnectionConfig::new()
            .with_setup(
                ConnectionSetup::new()
                    .with_client_name("old-secret")
                    .with_client_no_evict(true),
            )
            .with_client_name("new-secret");
        let debug = format!("{config:?}");
        assert!(!debug.contains("old-secret"));
        assert!(!debug.contains("new-secret"));
        let commands = config.setup().commands();
        assert_eq!(commands.len(), 2);
        assert_eq!(
            commands[0].1,
            array(vec![bulk("CLIENT"), bulk("SETNAME"), bulk("new-secret")])
        );
    }

    #[tokio::test]
    async fn setup_is_sequential_and_preserves_alignment() {
        let (mut connection, mut server) = pair().await;
        let setup = ConnectionSetup::new()
            .with_client_name("logical-client")
            .with_client_no_evict(true)
            .with_client_no_touch(false);
        let commands = setup.commands();
        let task = tokio::spawn(async move {
            for (_, expected) in commands {
                assert_eq!(server.next().await.unwrap().unwrap(), expected);
                assert!(
                    tokio::time::timeout(std::time::Duration::from_millis(10), server.next())
                        .await
                        .is_err(),
                    "setup was pipelined before checking the prior reply"
                );
                server.send(Frame::SimpleString("OK".into())).await.unwrap();
            }
            assert_eq!(
                server.next().await.unwrap().unwrap(),
                array(vec![bulk("PING")])
            );
            server
                .send(Frame::SimpleString("PONG".into()))
                .await
                .unwrap();
        });
        connection.apply_setup(&setup).await.unwrap();
        assert_eq!(
            connection
                .execute_pipeline(vec![array(vec![bulk("PING")])])
                .await
                .unwrap(),
            vec![Frame::SimpleString("PONG".into())]
        );
        task.await.unwrap();
    }

    #[tokio::test]
    async fn setup_errors_close_socket_stop_later_steps_and_redact_replies() {
        for response in [
            Frame::Error("ERR echoed-private-name".into()),
            Frame::BlobError("private-blob-error".into()),
            Frame::BulkString(Some("private-unexpected".into())),
        ] {
            let (mut connection, mut server) = pair().await;
            let task = tokio::spawn(async move {
                server.next().await.unwrap().unwrap();
                server.send(response).await.unwrap();
                assert!(
                    server.next().await.is_none(),
                    "failed setup leaked another command"
                );
            });
            let error = connection
                .apply_setup(
                    &ConnectionSetup::new()
                        .with_client_name("private-name")
                        .with_client_no_evict(true),
                )
                .await
                .unwrap_err();
            assert!(error.is_connection_error());
            assert!(error.to_string().contains("CLIENT SETNAME"));
            assert!(!format!("{error:?} {error}").contains("private"));
            assert!(matches!(
                connection
                    .execute_pipeline(vec![array(vec![bulk("PING")])])
                    .await,
                Err(RedisError::ConnectionClosed)
            ));
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn cancelled_setup_cannot_reuse_socket() {
        let (mut connection, mut server) = pair().await;
        let task = tokio::spawn(async move {
            server.next().await.unwrap().unwrap();
            assert!(server.next().await.is_none());
        });
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(30),
                connection.apply_setup(&ConnectionSetup::new().with_client_name("client"))
            )
            .await
            .is_err()
        );
        assert!(matches!(
            connection
                .execute_pipeline(vec![array(vec![bulk("PING")])])
                .await,
            Err(RedisError::ConnectionClosed)
        ));
        task.await.unwrap();
    }
}
