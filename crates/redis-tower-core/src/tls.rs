//! TLS configuration for Redis connections.

use crate::error::RedisError;
use crate::stream::RedisStream;
use tokio::net::TcpStream;

/// TLS configuration for connecting to Redis.
///
/// Supports two backends via feature flags:
/// - `tls-native-tls`: Uses the platform's native TLS (OpenSSL on Linux,
///   Secure Transport on macOS, SChannel on Windows).
/// - `tls-rustls`: Uses rustls (pure Rust, no system dependencies).
///
/// # Example
///
/// ```ignore
/// use redis_tower_core::tls::TlsConfig;
///
/// // Use platform defaults (system root certs, verify hostname).
/// let config = TlsConfig::default_native_tls();
///
/// // Or with rustls:
/// let config = TlsConfig::default_rustls();
///
/// // Disable certificate verification (DANGEROUS, for testing only).
/// let config = TlsConfig::default_native_tls().danger_accept_invalid_certs(true);
/// ```
pub struct TlsConfig {
    pub(crate) backend: TlsBackend,
    pub(crate) accept_invalid_certs: bool,
    pub(crate) accept_invalid_hostnames: bool,
}

pub(crate) enum TlsBackend {
    #[cfg(feature = "tls-native-tls")]
    NativeTls,
    #[cfg(feature = "tls-rustls")]
    Rustls,
}

impl TlsConfig {
    /// Create a TLS config using the native-tls backend with platform defaults.
    #[cfg(feature = "tls-native-tls")]
    pub fn default_native_tls() -> Self {
        Self {
            backend: TlsBackend::NativeTls,
            accept_invalid_certs: false,
            accept_invalid_hostnames: false,
        }
    }

    /// Create a TLS config using the rustls backend with system root certs.
    #[cfg(feature = "tls-rustls")]
    pub fn default_rustls() -> Self {
        Self {
            backend: TlsBackend::Rustls,
            accept_invalid_certs: false,
            accept_invalid_hostnames: false,
        }
    }

    /// Accept invalid TLS certificates.
    ///
    /// **DANGEROUS**: This disables certificate verification. Only use for
    /// testing with self-signed certificates.
    pub fn danger_accept_invalid_certs(mut self, accept: bool) -> Self {
        self.accept_invalid_certs = accept;
        self
    }

    /// Accept invalid hostnames in TLS certificates.
    ///
    /// **DANGEROUS**: This disables hostname verification.
    pub fn danger_accept_invalid_hostnames(mut self, accept: bool) -> Self {
        self.accept_invalid_hostnames = accept;
        self
    }

    /// Perform the TLS handshake on a TCP stream.
    pub(crate) async fn connect(
        &self,
        tcp: TcpStream,
        hostname: &str,
    ) -> Result<RedisStream, RedisError> {
        match &self.backend {
            #[cfg(feature = "tls-native-tls")]
            TlsBackend::NativeTls => self.connect_native_tls(tcp, hostname).await,
            #[cfg(feature = "tls-rustls")]
            TlsBackend::Rustls => self.connect_rustls(tcp, hostname).await,
        }
    }

    #[cfg(feature = "tls-native-tls")]
    async fn connect_native_tls(
        &self,
        tcp: TcpStream,
        hostname: &str,
    ) -> Result<RedisStream, RedisError> {
        let mut builder = native_tls::TlsConnector::builder();
        builder.danger_accept_invalid_certs(self.accept_invalid_certs);
        builder.danger_accept_invalid_hostnames(self.accept_invalid_hostnames);

        let connector = builder.build().map_err(|e| {
            RedisError::Connection(std::io::Error::other(e))
        })?;
        let connector = tokio_native_tls::TlsConnector::from(connector);

        let tls_stream = connector.connect(hostname, tcp).await.map_err(|e| {
            RedisError::Connection(std::io::Error::other(e))
        })?;

        Ok(RedisStream::NativeTls(Box::new(tls_stream)))
    }

    #[cfg(feature = "tls-rustls")]
    async fn connect_rustls(
        &self,
        tcp: TcpStream,
        hostname: &str,
    ) -> Result<RedisStream, RedisError> {
        use std::sync::Arc;

        let config = if self.accept_invalid_certs {
            // Dangerous: skip all verification.
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(DangerousVerifier))
                .with_no_client_auth()
        } else {
            // Load system root certs, falling back to webpki-roots.
            let mut root_store = rustls::RootCertStore::empty();
            let native_result = rustls_native_certs::load_native_certs();
            for cert in native_result.certs {
                let _ = root_store.add(cert);
            }
            if root_store.is_empty() {
                root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            }
            rustls::ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth()
        };

        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let server_name =
            rustls::pki_types::ServerName::try_from(hostname.to_string()).map_err(|e| {
                RedisError::Connection(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
            })?;

        let tls_stream = connector
            .connect(server_name, tcp)
            .await
            .map_err(RedisError::Connection)?;

        Ok(RedisStream::Rustls(Box::new(tls_stream)))
    }
}

/// Certificate verifier that accepts everything. Used when
/// `danger_accept_invalid_certs` is enabled.
#[cfg(feature = "tls-rustls")]
#[derive(Debug)]
struct DangerousVerifier;

#[cfg(feature = "tls-rustls")]
impl rustls::client::danger::ServerCertVerifier for DangerousVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}
