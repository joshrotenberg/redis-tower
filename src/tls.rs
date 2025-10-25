//! TLS support for secure Redis connections
//!
//! This module provides TLS/SSL support with both `native-tls` and `rustls` backends.
//!
//! # Feature Flags
//!
//! - `tls-native-tls` - Enable TLS via native-tls (OpenSSL/Schannel/Security Framework)
//! - `tls-rustls` - Enable TLS via rustls (pure Rust implementation)
//! - `tls-rustls-ring` - Use rustls with ring crypto backend
//! - `tls-rustls-webpki` - Use rustls with webpki-roots for certificate validation
//!
//! # Examples
//!
//! ## Native TLS
//!
//! ```no_run
//! use redis_tower::tls::TlsConfig;
//!
//! let tls = TlsConfig::native_tls()
//!     .danger_accept_invalid_certs(false)
//!     .build()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Rustls
//!
//! ```no_run
//! use redis_tower::tls::TlsConfig;
//!
//! let tls = TlsConfig::rustls()
//!     .with_native_roots()
//!     .build()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

#[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
use crate::types::RedisError;
#[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
use std::sync::Arc;
#[cfg(any(feature = "tls-native-tls", feature = "tls-rustls"))]
use tokio::net::TcpStream;

/// TLS configuration for Redis connections
#[derive(Clone, Default)]
pub enum TlsConfig {
    /// No TLS (plaintext connection)
    #[default]
    None,

    #[cfg(feature = "tls-native-tls")]
    /// Native TLS configuration (OpenSSL/Schannel/Security Framework)
    NativeTls(NativeTlsConfig),

    #[cfg(feature = "tls-rustls")]
    /// Rustls configuration (pure Rust TLS)
    Rustls(RustlsConfig),
}

impl TlsConfig {
    /// Create a native-tls builder
    #[cfg(feature = "tls-native-tls")]
    pub fn native_tls() -> NativeTlsBuilder {
        NativeTlsBuilder::default()
    }

    /// Create a rustls builder
    #[cfg(feature = "tls-rustls")]
    pub fn rustls() -> RustlsBuilder {
        RustlsBuilder::default()
    }

    /// Check if TLS is enabled
    pub fn is_enabled(&self) -> bool {
        !matches!(self, TlsConfig::None)
    }
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsConfig::None => write!(f, "TlsConfig::None"),
            #[cfg(feature = "tls-native-tls")]
            TlsConfig::NativeTls(_) => write!(f, "TlsConfig::NativeTls {{ ... }}"),
            #[cfg(feature = "tls-rustls")]
            TlsConfig::Rustls(_) => write!(f, "TlsConfig::Rustls {{ ... }}"),
        }
    }
}

// =============================================================================
// Native TLS Implementation
// =============================================================================

#[cfg(feature = "tls-native-tls")]
/// Native TLS configuration
#[derive(Clone)]
pub struct NativeTlsConfig {
    connector: Arc<tokio_native_tls::TlsConnector>,
    server_name: Option<String>,
}

#[cfg(feature = "tls-native-tls")]
impl NativeTlsConfig {
    /// Connect to a server with TLS
    pub async fn connect(
        &self,
        domain: &str,
        stream: TcpStream,
    ) -> Result<tokio_native_tls::TlsStream<TcpStream>, RedisError> {
        let server_name = self.server_name.as_deref().unwrap_or(domain);
        self.connector
            .connect(server_name, stream)
            .await
            .map_err(|e| RedisError::Connection(format!("TLS connection failed: {}", e)))
    }
}

#[cfg(feature = "tls-native-tls")]
/// Builder for native-tls configuration
#[derive(Default)]
pub struct NativeTlsBuilder {
    danger_accept_invalid_certs: bool,
    danger_accept_invalid_hostnames: bool,
    server_name: Option<String>,
}

#[cfg(feature = "tls-native-tls")]
impl NativeTlsBuilder {
    /// Accept invalid certificates (DANGEROUS - for testing only)
    pub fn danger_accept_invalid_certs(mut self, accept: bool) -> Self {
        self.danger_accept_invalid_certs = accept;
        self
    }

    /// Accept invalid hostnames (DANGEROUS - for testing only)
    pub fn danger_accept_invalid_hostnames(mut self, accept: bool) -> Self {
        self.danger_accept_invalid_hostnames = accept;
        self
    }

    /// Override the server name for SNI
    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    /// Build the TLS configuration
    pub fn build(self) -> Result<TlsConfig, RedisError> {
        let mut builder = native_tls::TlsConnector::builder();

        builder
            .danger_accept_invalid_certs(self.danger_accept_invalid_certs)
            .danger_accept_invalid_hostnames(self.danger_accept_invalid_hostnames);

        let connector = builder
            .build()
            .map_err(|e| RedisError::Connection(format!("Failed to build TLS connector: {}", e)))?;

        Ok(TlsConfig::NativeTls(NativeTlsConfig {
            connector: Arc::new(tokio_native_tls::TlsConnector::from(connector)),
            server_name: self.server_name,
        }))
    }
}

// =============================================================================
// Rustls Implementation
// =============================================================================

#[cfg(feature = "tls-rustls")]
/// Rustls TLS configuration
#[derive(Clone)]
pub struct RustlsConfig {
    connector: Arc<tokio_rustls::TlsConnector>,
    server_name: Option<String>,
}

#[cfg(feature = "tls-rustls")]
impl RustlsConfig {
    /// Connect to a server with TLS
    pub async fn connect(
        &self,
        domain: &str,
        stream: TcpStream,
    ) -> Result<tokio_rustls::client::TlsStream<TcpStream>, RedisError> {
        let server_name = self.server_name.as_deref().unwrap_or(domain);
        let server_name = rustls::pki_types::ServerName::try_from(server_name.to_string())
            .map_err(|e| RedisError::Connection(format!("Invalid server name: {}", e)))?;

        self.connector
            .connect(server_name, stream)
            .await
            .map_err(|e| RedisError::Connection(format!("TLS connection failed: {}", e)))
    }
}

#[cfg(feature = "tls-rustls")]
/// Builder for rustls configuration
#[derive(Default)]
pub struct RustlsBuilder {
    danger_accept_invalid_certs: bool,
    use_native_roots: bool,
    use_webpki_roots: bool,
    server_name: Option<String>,
}

#[cfg(feature = "tls-rustls")]
impl RustlsBuilder {
    /// Accept invalid certificates (DANGEROUS - for testing only)
    pub fn danger_accept_invalid_certs(mut self, accept: bool) -> Self {
        self.danger_accept_invalid_certs = accept;
        self
    }

    /// Use native system certificate roots
    pub fn with_native_roots(mut self) -> Self {
        self.use_native_roots = true;
        self
    }

    /// Use webpki-roots for certificate validation
    #[cfg(feature = "tls-rustls-webpki")]
    pub fn with_webpki_roots(mut self) -> Self {
        self.use_webpki_roots = true;
        self
    }

    /// Override the server name for SNI
    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    /// Build the TLS configuration
    pub fn build(self) -> Result<TlsConfig, RedisError> {
        use rustls::ClientConfig;
        use std::sync::Arc as StdArc;

        let config = if self.danger_accept_invalid_certs {
            // Dangerous: accept any certificate
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(StdArc::new(NoVerifier))
                .with_no_client_auth()
        } else {
            // Safe: verify certificates
            let mut root_store = rustls::RootCertStore::empty();

            if self.use_native_roots {
                let cert_result = rustls_native_certs::load_native_certs();
                if !cert_result.errors.is_empty() {
                    return Err(RedisError::Connection(format!(
                        "Failed to load some native certificates: {:?}",
                        cert_result.errors
                    )));
                }
                for cert in cert_result.certs {
                    root_store.add(cert).map_err(|e| {
                        RedisError::Connection(format!("Failed to add certificate: {}", e))
                    })?;
                }
            }

            #[cfg(feature = "tls-rustls-webpki")]
            if self.use_webpki_roots {
                root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            }

            // If no roots specified, use native by default
            #[cfg(not(feature = "tls-rustls-webpki"))]
            if !self.use_native_roots && !self.use_webpki_roots {
                let cert_result = rustls_native_certs::load_native_certs();
                if !cert_result.errors.is_empty() {
                    return Err(RedisError::Connection(format!(
                        "Failed to load some native certificates: {:?}",
                        cert_result.errors
                    )));
                }
                for cert in cert_result.certs {
                    root_store.add(cert).map_err(|e| {
                        RedisError::Connection(format!("Failed to add certificate: {}", e))
                    })?;
                }
            }

            ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth()
        };

        let connector = tokio_rustls::TlsConnector::from(StdArc::new(config));

        Ok(TlsConfig::Rustls(RustlsConfig {
            connector: Arc::new(connector),
            server_name: self.server_name,
        }))
    }
}

#[cfg(feature = "tls-rustls")]
/// Certificate verifier that accepts any certificate (DANGEROUS - testing only)
#[derive(Debug)]
struct NoVerifier;

#[cfg(feature = "tls-rustls")]
impl rustls::client::danger::ServerCertVerifier for NoVerifier {
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
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();
        assert!(!config.is_enabled());
    }

    #[cfg(feature = "tls-native-tls")]
    #[test]
    fn test_native_tls_builder() {
        let config = TlsConfig::native_tls()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        assert!(config.is_enabled());
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn test_rustls_builder() {
        let config = TlsConfig::rustls()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        assert!(config.is_enabled());
    }
}
