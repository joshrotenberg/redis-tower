//! TLS configuration for Redis connections.
//!
//! Enable `tls-rustls` for a pure-Rust backend or `tls-native-tls` for the
//! platform TLS stack. [`TlsConfig`] can use system roots, custom root CAs,
//! mutual TLS certificates, or a caller-built backend configuration.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! use redis_tower_core::RedisConnection;
//! use redis_tower_core::tls::TlsConfig;
//!
//! let tls = TlsConfig::default_rustls();
//! let connection = RedisConnection::connect_tls(
//!     "redis.example.com:6380",
//!     "redis.example.com",
//!     &tls,
//! )
//! .await?;
//! # let _ = connection;
//! # Ok(())
//! # }
//! ```

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
/// Each constructor is available only when its own backend feature is enabled;
/// this example shows both together.
///
/// ```no_run
/// use redis_tower_core::tls::TlsConfig;
///
/// // Use platform defaults (system root certs, verify hostname).
/// let native = TlsConfig::default_native_tls();
///
/// // Or with rustls:
/// let rustls = TlsConfig::default_rustls();
///
/// // Disable certificate verification (DANGEROUS, for testing only).
/// let insecure = TlsConfig::default_native_tls().danger_accept_invalid_certs(true);
/// # let _ = (native, rustls, insecure);
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
    ///
    /// The native root store (or its loading error) is cached on first use for
    /// the lifetime of the process. An empty native store uses the webpki bundle;
    /// loading failures with no usable native roots return an error instead.
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
    /// The certificate chain, validity period, and handshake signature are still
    /// verified against the trust roots; only the match between the server's
    /// certificate and the connected hostname or IP is skipped. Useful when a
    /// cluster reports node IPs the certificate does not name. Honored by both
    /// backends. Ignored for caller-built backend configurations, which control
    /// their own verification.
    pub fn danger_accept_invalid_hostnames(mut self, accept: bool) -> Self {
        self.accept_invalid_hostnames = accept;
        self
    }

    /// Trust a custom root CA, given its certificate(s) in PEM.
    ///
    /// Replaces the default trust roots (system store + webpki bundle) with the
    /// provided CA(s), including every certificate in a PEM bundle -- the
    /// standard private-CA / enterprise posture. Works with either backend.
    /// Ignored if the config was built from a pre-made
    /// `ClientConfig` / `TlsConnector`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use redis_tower_core::tls::TlsConfig;
    ///
    /// let tls = TlsConfig::default_rustls()
    ///     .with_root_ca_pem(std::fs::read("ca.pem")?);
    /// # let _ = tls;
    /// # Ok(())
    /// # }
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
    /// ```no_run
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// use redis_tower_core::tls::TlsConfig;
    ///
    /// let tls = TlsConfig::default_rustls()
    ///     .with_root_ca_pem(std::fs::read("ca.pem")?)
    ///     .with_client_auth_pem(std::fs::read("client.pem")?, std::fs::read("client.key")?);
    /// # let _ = tls;
    /// # Ok(())
    /// # }
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
            let certs = native_tls::Certificate::stack_from_pem(ca)
                .map_err(|e| RedisError::from(std::io::Error::other(e)))?;
            builder.disable_built_in_roots(true);
            for cert in certs {
                builder.add_root_certificate(cert);
            }
        }
        if let Some((ref cert_pem, ref key_pem)) = self.client_auth_pem {
            let identity = native_tls::Identity::from_pkcs8(cert_pem, key_pem)
                .map_err(|e| RedisError::from(std::io::Error::other(e)))?;
            builder.identity(identity);
        }

        let connector = builder
            .build()
            .map_err(|e| RedisError::from(std::io::Error::other(e)))?;
        let connector = tokio_native_tls::TlsConnector::from(connector);

        let tls_stream = connector
            .connect(hostname, tcp)
            .await
            .map_err(|e| RedisError::from(std::io::Error::other(e)))?;

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
            .map_err(|e| RedisError::from(std::io::Error::other(e)))?;

        Ok(RedisStream::NativeTls(Box::new(tls_stream)))
    }

    /// Build the rustls root cert store: the custom CA if one was provided,
    /// otherwise the system store with a webpki-roots fallback.
    #[cfg(feature = "tls-rustls")]
    fn rustls_root_store(&self) -> Result<std::sync::Arc<rustls::RootCertStore>, RedisError> {
        let mut root_store = rustls::RootCertStore::empty();
        if let Some(ref ca) = self.root_ca_pem {
            for cert in parse_certs_pem(ca)? {
                root_store
                    .add(cert)
                    .map_err(|e| RedisError::from(std::io::Error::other(e)))?;
            }
        } else {
            return rustls_native_root_store();
        }
        Ok(std::sync::Arc::new(root_store))
    }

    #[cfg(feature = "tls-rustls")]
    async fn connect_rustls(
        &self,
        tcp: TcpStream,
        hostname: &str,
    ) -> Result<RedisStream, RedisError> {
        use std::sync::Arc;

        // Choose the server-cert verifier; every arm yields a builder awaiting
        // the client-auth choice, so client certs are applied uniformly below.
        // redis-tower enables rustls' ring backend explicitly. Select it on
        // this builder as well: downstream SDKs may enable aws-lc-rs through
        // Cargo feature unification, in which case rustls cannot infer one
        // process-wide default provider and `ClientConfig::builder()` panics.
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|error| RedisError::from(std::io::Error::other(error)))?;
        let builder = if self.accept_invalid_certs {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(DangerousVerifier))
        } else if self.accept_invalid_hostnames {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(HostnameBypassVerifier {
                    roots: self.rustls_root_store()?,
                    algorithms: rustls::crypto::ring::default_provider()
                        .signature_verification_algorithms,
                }))
        } else {
            builder.with_root_certificates(self.rustls_root_store()?)
        };
        let config = match &self.client_auth_pem {
            Some((cert_pem, key_pem)) => builder
                .with_client_auth_cert(parse_certs_pem(cert_pem)?, parse_private_key_pem(key_pem)?)
                .map_err(|e| RedisError::from(std::io::Error::other(e)))?,
            None => builder.with_no_client_auth(),
        };

        let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
        let server_name =
            rustls::pki_types::ServerName::try_from(hostname.to_string()).map_err(|e| {
                RedisError::from(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
            })?;

        let tls_stream = connector
            .connect(server_name, tcp)
            .await
            .map_err(RedisError::from)?;

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
                RedisError::from(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
            })?;

        let tls_stream = connector
            .connect(server_name, tcp)
            .await
            .map_err(RedisError::from)?;

        Ok(RedisStream::Rustls(Box::new(tls_stream)))
    }
}

/// Share the native trust snapshot, including a useful failure, across connects.
#[cfg(feature = "tls-rustls")]
fn rustls_native_root_store() -> Result<std::sync::Arc<rustls::RootCertStore>, RedisError> {
    use std::sync::{Arc, OnceLock};

    static ROOTS: OnceLock<Result<Arc<rustls::RootCertStore>, String>> = OnceLock::new();
    ROOTS
        .get_or_init(|| {
            let native = rustls_native_certs::load_native_certs();
            let mut errors: Vec<_> = native.errors.iter().map(ToString::to_string).collect();
            let mut roots = rustls::RootCertStore::empty();
            for cert in native.certs {
                if let Err(error) = roots.add(cert) {
                    errors.push(error.to_string());
                }
            }
            if roots.is_empty() {
                if !errors.is_empty() {
                    return Err(format!(
                        "failed to load rustls native root certificates: {}",
                        errors.join("; ")
                    ));
                }
                roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            } else if !errors.is_empty() {
                tracing::warn!(errors = ?errors, "some rustls native root certificates could not be loaded");
            }
            Ok(Arc::new(roots))
        })
        .as_ref()
        .map(Arc::clone)
        .map_err(|error| RedisError::from(std::io::Error::other(error.clone())))
}

/// Parse one or more certificates from PEM into rustls DER form.
#[cfg(feature = "tls-rustls")]
fn parse_certs_pem(
    pem: &[u8],
) -> Result<Vec<rustls::pki_types::CertificateDer<'static>>, RedisError> {
    use rustls::pki_types::pem::PemObject;
    rustls::pki_types::CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| RedisError::from(std::io::Error::other(e)))
}

/// Parse a single PKCS#8 / SEC1 / RSA private key from PEM into rustls DER form.
#[cfg(feature = "tls-rustls")]
fn parse_private_key_pem(
    pem: &[u8],
) -> Result<rustls::pki_types::PrivateKeyDer<'static>, RedisError> {
    use rustls::pki_types::pem::PemObject;
    rustls::pki_types::PrivateKeyDer::from_pem_slice(pem)
        .map_err(|e| RedisError::from(std::io::Error::other(e)))
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
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Certificate verifier that checks everything except the server name. Used
/// when `danger_accept_invalid_hostnames` is enabled without
/// `danger_accept_invalid_certs`.
///
/// The chain must still lead to a trusted root, be within its validity period,
/// and carry the server-auth usage, and the handshake signature must verify
/// against the presented certificate. Only the final hostname / IP SAN match is
/// skipped -- the same check rustls' `WebPkiServerVerifier` performs last.
#[cfg(feature = "tls-rustls")]
#[derive(Debug)]
struct HostnameBypassVerifier {
    roots: std::sync::Arc<rustls::RootCertStore>,
    algorithms: rustls::crypto::WebPkiSupportedAlgorithms,
}

#[cfg(feature = "tls-rustls")]
impl rustls::client::danger::ServerCertVerifier for HostnameBypassVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        let cert = rustls::server::ParsedCertificate::try_from(end_entity)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &cert,
            &self.roots,
            intermediates,
            now,
            self.algorithms.all,
        )?;
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[cfg(feature = "tls-rustls")]
    use std::sync::Arc;

    // Static fixtures avoid adding a core dev-dependency outside this task's
    // manifest scope. Generated with OpenSSL; the leaf is signed by TEST_CERT,
    // uses TEST_KEY, and has localhost / 127.0.0.1 SANs. Its 824-day validity
    // respects Secure Transport's maximum; renew the leaf before February 2029.
    #[cfg(feature = "tls-rustls")]
    const SERVER_CERT: &str = include_str!("../tests/tls/server.pem");
    #[cfg(feature = "tls-rustls")]
    const UNRELATED_CA: &str = include_str!("../tests/tls/unrelated-ca.pem");

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
    pub(crate) fn server_config() -> std::sync::Arc<rustls::ServerConfig> {
        let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                parse_certs_pem(TEST_CERT.as_bytes()).unwrap(),
                parse_private_key_pem(TEST_KEY.as_bytes()).unwrap(),
            )
            .unwrap();
        std::sync::Arc::new(config)
    }

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
        rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(tls.rustls_root_store().unwrap())
        .with_client_auth_cert(certs, key)
        .expect("mTLS client config should build");
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn parse_helpers_reject_garbage() {
        // Non-cert PEM yields no certs (not an error -- the PEM iterator skips it).
        assert!(parse_certs_pem(b"not a pem").unwrap().is_empty());
        // A missing private key is an error.
        assert!(parse_private_key_pem(b"-----BEGIN CERTIFICATE-----\nx\n").is_err());
    }

    /// A real TLS handshake on an ephemeral port, without a Redis process.
    #[cfg(feature = "tls-rustls")]
    async fn handshake(tls: TlsConfig, hostname: &str) -> Result<(), RedisError> {
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            parse_certs_pem(SERVER_CERT.as_bytes()).unwrap(),
            parse_private_key_pem(TEST_KEY.as_bytes()).unwrap(),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = async move {
            let (tcp, _) = listener.accept().await.unwrap();
            tokio_rustls::TlsAcceptor::from(Arc::new(config))
                .accept(tcp)
                .await
        };
        let client = async {
            let tcp = TcpStream::connect(addr).await.unwrap();
            tls.connect(tcp, hostname).await
        };
        let (client, server) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(client, server)
        })
        .await
        .expect("TLS handshake timed out");
        if client.is_ok() {
            assert!(
                server.is_ok(),
                "server rejected a successful client handshake"
            );
        }
        client.map(|_| ())
    }

    #[cfg(all(feature = "tls-rustls", feature = "tls-native-tls"))]
    #[tokio::test]
    async fn native_tls_trusts_every_ca_in_a_bundle() {
        for bundle in [
            format!("{UNRELATED_CA}{TEST_CERT}"),
            format!("{TEST_CERT}{UNRELATED_CA}"),
        ] {
            handshake(
                TlsConfig::default_native_tls().with_root_ca_pem(bundle),
                "localhost",
            )
            .await
            .expect("the signing CA must be trusted in either bundle position");
        }
        assert!(
            handshake(
                TlsConfig::default_native_tls().with_root_ca_pem(UNRELATED_CA),
                "localhost",
            )
            .await
            .is_err(),
            "an unrelated CA must not trust the server"
        );
    }

    #[cfg(feature = "tls-rustls")]
    #[tokio::test]
    async fn rustls_hostname_bypass_skips_only_the_name_check() {
        // The leaf's SANs are localhost / 127.0.0.1; a mismatched DNS name and
        // a mismatched IP both exercise the skipped check.
        for hostname in ["localhost", "wrong.example", "127.0.0.2"] {
            handshake(
                TlsConfig::default_rustls()
                    .with_root_ca_pem(TEST_CERT)
                    .danger_accept_invalid_hostnames(true),
                hostname,
            )
            .await
            .unwrap_or_else(|error| panic!("hostname bypass must accept {hostname}: {error}"));
        }
        for hostname in ["wrong.example", "127.0.0.2"] {
            assert!(
                handshake(
                    TlsConfig::default_rustls().with_root_ca_pem(TEST_CERT),
                    hostname,
                )
                .await
                .is_err(),
                "without the bypass, {hostname} must fail hostname verification"
            );
            assert!(
                handshake(
                    TlsConfig::default_rustls()
                        .with_root_ca_pem(TEST_CERT)
                        .danger_accept_invalid_hostnames(true)
                        .danger_accept_invalid_hostnames(false),
                    hostname,
                )
                .await
                .is_err(),
                "turning the option back off must restore hostname verification"
            );
        }
        // The chain is still verified: an untrusted issuer fails even with the
        // hostname check skipped.
        assert!(
            handshake(
                TlsConfig::default_rustls()
                    .with_root_ca_pem(UNRELATED_CA)
                    .danger_accept_invalid_hostnames(true),
                "wrong.example",
            )
            .await
            .is_err(),
            "hostname bypass must not trust an unrelated CA"
        );
    }

    #[cfg(feature = "tls-rustls")]
    #[tokio::test]
    async fn rustls_invalid_certs_and_custom_config_keep_their_verification_policy() {
        for accept_hostnames in [false, true] {
            handshake(
                TlsConfig::default_rustls()
                    .danger_accept_invalid_certs(true)
                    .danger_accept_invalid_hostnames(accept_hostnames),
                "wrong.example",
            )
            .await
            .expect("certificate bypass must retain its existing behavior");
        }
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(
            TlsConfig::default_rustls()
                .with_root_ca_pem(TEST_CERT)
                .rustls_root_store()
                .unwrap(),
        )
        .with_no_client_auth();
        handshake(
            TlsConfig::from_rustls_config(Arc::new(config)).danger_accept_invalid_hostnames(true),
            "localhost",
        )
        .await
        .expect("a custom config must continue to ignore the builder option");
    }

    /// Set loader environment variables only in a child process, before any TLS
    /// initialization. This avoids unsafe environment mutation and cache races.
    #[cfg(feature = "tls-rustls")]
    fn isolated_root_file(
        test: &str,
        contents: Option<&str>,
        check: impl FnOnce(&std::path::Path),
    ) {
        const CASE: &str = "REDIS_TOWER_TLS_ROOT_TEST";
        if std::env::var(CASE).as_deref() == Ok(test) {
            check(std::path::Path::new(
                &std::env::var_os("SSL_CERT_FILE").unwrap(),
            ));
            return;
        }
        let dir =
            std::env::temp_dir().join(format!("redis-tower-tls-{}-{test}", std::process::id()));
        std::fs::create_dir(&dir).unwrap();
        let file = dir.join("roots.pem");
        if let Some(contents) = contents {
            std::fs::write(&file, contents).unwrap();
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &format!("tls::tests::{test}"),
                "--test-threads=1",
                "--nocapture",
            ])
            .env(CASE, test)
            .env("SSL_CERT_FILE", &file)
            .env_remove("SSL_CERT_DIR")
            .output()
            .unwrap();
        std::fs::remove_dir_all(dir).unwrap();
        assert!(
            output.status.success(),
            "isolated TLS test failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_native_roots_are_cached_and_custom_roots_stay_separate() {
        isolated_root_file(
            "rustls_native_roots_are_cached_and_custom_roots_stay_separate",
            Some(TEST_CERT),
            |file| {
                let first = TlsConfig::default_rustls().rustls_root_store().unwrap();
                assert_eq!(first.len(), 1);
                std::fs::remove_file(file).unwrap();
                let second = TlsConfig::default_rustls().rustls_root_store().unwrap();
                assert!(Arc::ptr_eq(&first, &second));
                let custom = TlsConfig::default_rustls()
                    .with_root_ca_pem(UNRELATED_CA)
                    .rustls_root_store()
                    .unwrap();
                assert_eq!(custom.len(), 1);
                assert_ne!(first.roots, custom.roots);
            },
        );
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_native_root_loading_errors_are_cached_and_descriptive() {
        isolated_root_file(
            "rustls_native_root_loading_errors_are_cached_and_descriptive",
            None,
            |file| {
                let first = TlsConfig::default_rustls()
                    .rustls_root_store()
                    .unwrap_err()
                    .to_string();
                assert!(first.contains("failed to load rustls native root certificates"));
                assert!(
                    first.contains("roots.pem"),
                    "loader context missing: {first}"
                );
                std::fs::write(file, TEST_CERT).unwrap();
                let second = TlsConfig::default_rustls()
                    .rustls_root_store()
                    .unwrap_err()
                    .to_string();
                assert_eq!(first, second);
                assert!(
                    TlsConfig::default_rustls()
                        .with_root_ca_pem(TEST_CERT)
                        .rustls_root_store()
                        .is_ok(),
                    "a failed native loader must not affect explicit custom roots"
                );
            },
        );
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_empty_native_store_keeps_webpki_fallback() {
        isolated_root_file(
            "rustls_empty_native_store_keeps_webpki_fallback",
            Some(""),
            |_| {
                let roots = TlsConfig::default_rustls().rustls_root_store().unwrap();
                assert_eq!(roots.len(), webpki_roots::TLS_SERVER_ROOTS.len());
            },
        );
    }

    // SSL_CERT_FILE controls OpenSSL's default roots, allowing us to test root
    // replacement without installing a CA into the user's OS trust store.
    #[cfg(all(
        feature = "tls-rustls",
        feature = "tls-native-tls",
        target_os = "linux"
    ))]
    #[test]
    fn native_tls_custom_ca_replaces_default_roots() {
        isolated_root_file(
            "native_tls_custom_ca_replaces_default_roots",
            Some(TEST_CERT),
            |_| {
                tokio::runtime::Runtime::new().unwrap().block_on(async {
                    handshake(TlsConfig::default_native_tls(), "localhost")
                        .await
                        .expect("the default root file must trust the server");
                    assert!(
                        handshake(
                            TlsConfig::default_native_tls().with_root_ca_pem(UNRELATED_CA),
                            "localhost",
                        )
                        .await
                        .is_err(),
                        "custom roots must replace the default root file"
                    );
                });
            },
        );
    }
}
