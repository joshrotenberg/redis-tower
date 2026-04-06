use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

#[cfg(unix)]
use tokio::net::UnixStream;

/// Transport abstraction over TCP, Unix, and TLS connections.
///
/// Implements [`AsyncRead`] + [`AsyncWrite`] so it can be used with
/// `tokio_util::codec::Framed`. The active variant is determined at
/// connection time based on the URL scheme or connect method used.
pub enum RedisStream {
    /// Plain TCP connection.
    Tcp(TcpStream),

    /// Unix domain socket connection.
    #[cfg(unix)]
    Unix(UnixStream),

    /// TLS connection via native-tls.
    #[cfg(feature = "tls-native-tls")]
    NativeTls(Box<tokio_native_tls::TlsStream<TcpStream>>),

    /// TLS connection via rustls.
    #[cfg(feature = "tls-rustls")]
    Rustls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for RedisStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            RedisStream::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(unix)]
            RedisStream::Unix(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls-native-tls")]
            RedisStream::NativeTls(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls-rustls")]
            RedisStream::Rustls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for RedisStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            RedisStream::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(unix)]
            RedisStream::Unix(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls-native-tls")]
            RedisStream::NativeTls(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls-rustls")]
            RedisStream::Rustls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            RedisStream::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(unix)]
            RedisStream::Unix(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls-native-tls")]
            RedisStream::NativeTls(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls-rustls")]
            RedisStream::Rustls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            RedisStream::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(unix)]
            RedisStream::Unix(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls-native-tls")]
            RedisStream::NativeTls(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls-rustls")]
            RedisStream::Rustls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}
