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
    /// Custom root CA certificate(s) in PEM, replacing the default trust roots.
    /// Applies to the default backends; ignored for a custom `ClientConfig` /
    /// `TlsConnector` (the caller controls those).
    pub(crate) root_ca_pem: Option<Vec<u8>>,
    /// Client certificate chain + PKCS#8 private key in PEM, for mutual TLS.
    pub(crate) client_auth_pem: Option<(Vec<u8>, Vec<u8>)>,
}

pub(crate) enum TlsBackend {
    #[cfg(feature = "tls-native-tls")]
    NativeTls,
    #[cfg(feature = "tls-native-tls")]
    NativeTlsCustom(native_tls::TlsConnector),
    #[cfg(feature = "tls-rustls")]
    Rustls,
    #[cfg(feature = "tls-rustls")]
    RustlsCustom(std::sync::Arc<rustls::ClientConfig>),
}

impl TlsConfig {
    /// Create a TLS config using the native-tls backend with platform defaults.
    #[cfg(feature = "tls-native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tls-native-tls")))]
    pub fn default_native_tls() -> Self {
        Self {
            backend: TlsBackend::NativeTls,
            accept_invalid_certs: false,
            accept_invalid_hostnames: false,
            root_ca_pem: None,
            client_auth_pem: None,
        }
    }

    /// Create a TLS config using the rustls backend with system root certs.
    #[cfg(feature = "tls-rustls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tls-rustls")))]
    pub fn default_rustls() -> Self {
        Self {
            backend: TlsBackend::Rustls,
            accept_invalid_certs: false,
            accept_invalid_hostnames: false,
            root_ca_pem: None,
            client_auth_pem: None,
        }
    }

    /// Create from a pre-built rustls `ClientConfig`.
    ///
    /// This allows full control over the TLS configuration (custom root
    /// certs, client certificates, protocol versions, etc.). The
    /// `danger_accept_invalid_certs` and `danger_accept_invalid_hostnames`
    /// settings are ignored when using a custom config since the caller
    /// controls verification through the provided `ClientConfig`.
    #[cfg(feature = "tls-rustls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tls-rustls")))]
    pub fn from_rustls_config(config: std::sync::Arc<rustls::ClientConfig>) -> Self {
        Self {
            backend: TlsBackend::RustlsCustom(config),
            accept_invalid_certs: false,
            accept_invalid_hostnames: false,
            root_ca_pem: None,
            client_auth_pem: None,
        }
    }

    /// Create from a pre-built native-tls `TlsConnector`.
    ///
    /// This allows full control over the TLS configuration (custom root
    /// certs, client certificates, protocol versions, etc.). The
    /// `danger_accept_invalid_certs` and `danger_accept_invalid_hostnames`
    /// settings are ignored when using a custom connector since the caller
    /// controls verification through the provided `TlsConnector`.
    #[cfg(feature = "tls-native-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tls-native-tls")))]
    pub fn from_native_tls_connector(connector: native_tls::TlsConnector) -> Self {
        Self {
            backend: TlsBackend::NativeTlsCustom(connector),
            accept_invalid_certs: false,
            accept_invalid_hostnames: false,
            root_ca_pem: None,
            client_auth_pem: None,
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

    /// Trust a custom root CA, given its certificate(s) in PEM.
    ///
    /// Replaces the default trust roots (system store + webpki bundle) with the
    /// provided CA(s) -- the standard private-CA / enterprise posture. Works
    /// with either backend. Ignored if the config was built from a pre-made
    /// `ClientConfig` / `TlsConnector`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let tls = TlsConfig::default_rustls()
    ///     .with_root_ca_pem(std::fs::read("ca.pem")?);
    /// ```
    pub fn with_root_ca_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.root_ca_pem = Some(pem.into());
        self
    }

    /// Present a client certificate for mutual TLS (mTLS).
    ///
    /// `cert_pem` is the client certificate chain and `key_pem` its PKCS#8
    /// private key, both in PEM. Works with either backend. Combine with
    /// [`with_root_ca_pem`](Self::with_root_ca_pem) for the typical
    /// custom-CA + mTLS enterprise posture.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let tls = TlsConfig::default_rustls()
    ///     .with_root_ca_pem(std::fs::read("ca.pem")?)
    ///     .with_client_auth_pem(std::fs::read("client.pem")?, std::fs::read("client.key")?);
    /// ```
    pub fn with_client_auth_pem(
        mut self,
        cert_pem: impl Into<Vec<u8>>,
        key_pem: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_auth_pem = Some((cert_pem.into(), key_pem.into()));
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
            #[cfg(feature = "tls-native-tls")]
            TlsBackend::NativeTlsCustom(connector) => {
                Self::connect_native_tls_custom(connector.clone(), tcp, hostname).await
            }
            #[cfg(feature = "tls-rustls")]
            TlsBackend::Rustls => self.connect_rustls(tcp, hostname).await,
            #[cfg(feature = "tls-rustls")]
            TlsBackend::RustlsCustom(config) => {
                Self::connect_rustls_custom(config.clone(), tcp, hostname).await
            }
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
        if let Some(ref ca) = self.root_ca_pem {
            let cert = native_tls::Certificate::from_pem(ca)
                .map_err(|e| RedisError::Connection(std::io::Error::other(e)))?;
            builder.add_root_certificate(cert);
        }
        if let Some((ref cert_pem, ref key_pem)) = self.client_auth_pem {
            let identity = native_tls::Identity::from_pkcs8(cert_pem, key_pem)
                .map_err(|e| RedisError::Connection(std::io::Error::other(e)))?;
            builder.identity(identity);
        }

        let connector = builder
            .build()
            .map_err(|e| RedisError::Connection(std::io::Error::other(e)))?;
        let connector = tokio_native_tls::TlsConnector::from(connector);

        let tls_stream = connector
            .connect(hostname, tcp)
            .await
            .map_err(|e| RedisError::Connection(std::io::Error::other(e)))?;

        Ok(RedisStream::NativeTls(Box::new(tls_stream)))
    }

    #[cfg(feature = "tls-native-tls")]
    async fn connect_native_tls_custom(
        connector: native_tls::TlsConnector,
        tcp: TcpStream,
        hostname: &str,
    ) -> Result<RedisStream, RedisError> {
        let connector = tokio_native_tls::TlsConnector::from(connector);

        let tls_stream = connector
            .connect(hostname, tcp)
            .await
            .map_err(|e| RedisError::Connection(std::io::Error::other(e)))?;

        Ok(RedisStream::NativeTls(Box::new(tls_stream)))
    }

    /// Build the rustls root cert store: the custom CA if one was provided,
    /// otherwise the system store with a webpki-roots fallback.
    #[cfg(feature = "tls-rustls")]
    fn rustls_root_store(&self) -> Result<rustls::RootCertStore, RedisError> {
        let mut root_store = rustls::RootCertStore::empty();
        if let Some(ref ca) = self.root_ca_pem {
            for cert in parse_certs_pem(ca)? {
                root_store
                    .add(cert)
                    .map_err(|e| RedisError::Connection(std::io::Error::other(e)))?;
            }
        } else {
            let native_result = rustls_native_certs::load_native_certs();
            for cert in native_result.certs {
                let _ = root_store.add(cert);
            }
            if root_store.is_empty() {
                root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            }
        }
        Ok(root_store)
    }

    #[cfg(feature = "tls-rustls")]
    async fn connect_rustls(
        &self,
        tcp: TcpStream,
        hostname: &str,
    ) -> Result<RedisStream, RedisError> {
        use std::sync::Arc;

        // Choose the server-cert verifier; both arms yield a builder awaiting
        // the client-auth choice, so client certs are applied uniformly below.
        let builder = rustls::ClientConfig::builder();
        let builder = if self.accept_invalid_certs {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(DangerousVerifier))
        } else {
            builder.with_root_certificates(self.rustls_root_store()?)
        };
        let config = match &self.client_auth_pem {
            Some((cert_pem, key_pem)) => builder
                .with_client_auth_cert(parse_certs_pem(cert_pem)?, parse_private_key_pem(key_pem)?)
                .map_err(|e| RedisError::Connection(std::io::Error::other(e)))?,
            None => builder.with_no_client_auth(),
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

    #[cfg(feature = "tls-rustls")]
    async fn connect_rustls_custom(
        config: std::sync::Arc<rustls::ClientConfig>,
        tcp: TcpStream,
        hostname: &str,
    ) -> Result<RedisStream, RedisError> {
        let connector = tokio_rustls::TlsConnector::from(config);
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

/// Parse one or more certificates from PEM into rustls DER form.
#[cfg(feature = "tls-rustls")]
fn parse_certs_pem(
    pem: &[u8],
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, RedisError> {
    rustls_pemfile::certs(&mut &pem[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| RedisError::Connection(std::io::Error::other(e)))
}

/// Parse a single PKCS#8 / SEC1 / RSA private key from PEM into rustls DER form.
#[cfg(feature = "tls-rustls")]
fn parse_private_key_pem(
    pem: &[u8],
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, RedisError> {
    rustls_pemfile::private_key(&mut &pem[..])
        .map_err(|e| RedisError::Connection(std::io::Error::other(e)))?
        .ok_or_else(|| {
            RedisError::Connection(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "no private key found in PEM",
            ))
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    // A self-signed P-256 cert + its PKCS#8 key, used to exercise PEM parsing.
    const TEST_CERT: &str = "-----BEGIN CERTIFICATE-----\n\
MIIBijCCATGgAwIBAgIUeYWzxqGH+ak/HDH9RMB1+u5Lhf4wCgYIKoZIzj0EAwIw\n\
GzEZMBcGA1UEAwwQcmVkaXMtdG93ZXItdGVzdDAeFw0yNjA2MTIyMTAxNTNaFw0z\n\
NjA2MDkyMTAxNTNaMBsxGTAXBgNVBAMMEHJlZGlzLXRvd2VyLXRlc3QwWTATBgcq\n\
hkjOPQIBBggqhkjOPQMBBwNCAARTJOAo81mVG4sncY5w6LVlG+y3O4llaHzx7UOq\n\
MKxs4Csh4kTiDRmwUoIq9DISRM1uYUR5dR9MjoMk/NEt3Jxpo1MwUTAdBgNVHQ4E\n\
FgQUkxm6d0TuRcV/ZOvIdRHtudvybO0wHwYDVR0jBBgwFoAUkxm6d0TuRcV/ZOvI\n\
dRHtudvybO0wDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNHADBEAiBMx2WV\n\
8zEWSydXPqn7rabmh91JYNG3ikU1ggxscXSnDAIgVKG6VLZwvTHDRA6HSFdoAqyc\n\
2c6Rr4ZJh79vhNSj8jM=\n\
-----END CERTIFICATE-----\n";

    const TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----\n\
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgju5KBtu0Tyd5YVXR\n\
JwQS3S1hW6Ts2I3ASTerbjBlJPahRANCAARTJOAo81mVG4sncY5w6LVlG+y3O4ll\n\
aHzx7UOqMKxs4Csh4kTiDRmwUoIq9DISRM1uYUR5dR9MjoMk/NEt3Jxp\n\
-----END PRIVATE KEY-----\n";

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn with_root_ca_pem_sets_field_and_builds_store() {
        let tls = TlsConfig::default_rustls().with_root_ca_pem(TEST_CERT.as_bytes().to_vec());
        assert!(tls.root_ca_pem.is_some());
        // The custom CA replaces the default roots: the store holds exactly it.
        let store = tls.rustls_root_store().expect("custom CA should parse");
        assert_eq!(store.len(), 1);
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn with_client_auth_pem_builds_a_client_config() {
        let tls = TlsConfig::default_rustls()
            .with_root_ca_pem(TEST_CERT.as_bytes().to_vec())
            .with_client_auth_pem(TEST_CERT.as_bytes().to_vec(), TEST_KEY.as_bytes().to_vec());
        assert!(tls.client_auth_pem.is_some());
        // The full mTLS client config (custom roots + client cert) builds.
        let certs = parse_certs_pem(TEST_CERT.as_bytes()).unwrap();
        let key = parse_private_key_pem(TEST_KEY.as_bytes()).unwrap();
        rustls::ClientConfig::builder()
            .with_root_certificates(tls.rustls_root_store().unwrap())
            .with_client_auth_cert(certs, key)
            .expect("mTLS client config should build");
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn parse_helpers_reject_garbage() {
        // Non-cert PEM yields no certs (not an error -- rustls_pemfile skips it).
        assert!(parse_certs_pem(b"not a pem").unwrap().is_empty());
        // A missing private key is an error.
        assert!(parse_private_key_pem(b"-----BEGIN CERTIFICATE-----\nx\n").is_err());
    }
}
