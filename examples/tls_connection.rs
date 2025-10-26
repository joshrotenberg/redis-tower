//! Example demonstrating TLS connections to Redis
//!
//! This example shows how to connect to Redis using TLS with both
//! native-tls and rustls backends.
//!
//! # Requirements
//! - A Redis server with TLS enabled (typically on port 6380)
//! - Enable tls features: `--features tls-rustls` or `--features tls-native-tls`
//!
//! # Running
//! ```bash
//! # With rustls
//! cargo run --example tls_connection --features tls-rustls,tls-rustls-ring
//!
//! # With native-tls
//! cargo run --example tls_connection --features tls-native-tls
//! ```

use redis_tower::commands::Ping;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("Redis TLS Connection Examples\n");

    // Example 1: Connect using rediss:// URL (automatic TLS)
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    {
        println!("=== Example 1: Using rediss:// URL ===");
        match RedisClient::connect_url("rediss://localhost:6380").await {
            Ok(client) => {
                println!("Connected to Redis with TLS via URL!");

                // Test the connection
                match client.call(Ping::new()).await {
                    Ok(_) => println!("PING successful"),
                    Err(e) => println!("PING failed: {}", e),
                }
            }
            Err(e) => println!("Failed to connect: {}", e),
        }
        println!();
    }

    // Example 2: Rustls with native certificate roots
    #[cfg(feature = "tls-rustls")]
    {
        println!("=== Example 2: Rustls with native roots ===");
        use redis_tower::tls::TlsConfig;

        let tls = TlsConfig::rustls().with_native_roots().build()?;

        match RedisClient::connect_with_config("localhost:6380", tls).await {
            Ok(client) => {
                println!("Connected with Rustls!");

                // Perform some operations
                if let Err(e) = client.call(Set::new("tls_test", "rustls")).await {
                    println!("SET failed: {}", e);
                } else {
                    println!("SET tls_test = rustls");

                    match client.call(Get::new("tls_test")).await {
                        Ok(value) => {
                            let value_str = value.map(|b| String::from_utf8_lossy(&b).to_string());
                            println!("GET tls_test = {:?}", value_str);
                        }
                        Err(e) => println!("GET failed: {}", e),
                    }
                }
            }
            Err(e) => println!("Failed to connect: {}", e),
        }
        println!();
    }

    // Example 3: Rustls with webpki roots
    #[cfg(feature = "tls-rustls-webpki")]
    {
        println!("=== Example 3: Rustls with webpki roots ===");
        use redis_tower::tls::TlsConfig;

        let tls = TlsConfig::rustls().with_webpki_roots().build()?;

        match RedisClient::connect_with_config("localhost:6380", tls).await {
            Ok(client) => {
                println!("Connected with Rustls (webpki)!");
                if client.call(Ping::new()).await.is_ok() {
                    println!("PING successful");
                }
            }
            Err(e) => println!("Failed to connect: {}", e),
        }
        println!();
    }

    // Example 4: Native TLS
    #[cfg(feature = "tls-native-tls")]
    {
        println!("=== Example 4: Native TLS ===");
        use redis_tower::tls::TlsConfig;

        let tls = TlsConfig::native_tls().build()?;

        match RedisClient::connect_with_config("localhost:6380", tls).await {
            Ok(client) => {
                println!("Connected with native-tls!");

                if let Err(e) = client.call(Set::new("tls_test", "native")).await {
                    println!("SET failed: {}", e);
                } else {
                    println!("SET tls_test = native");

                    match client.call(Get::new("tls_test")).await {
                        Ok(value) => {
                            let value_str = value.map(|b| String::from_utf8_lossy(&b).to_string());
                            println!("GET tls_test = {:?}", value_str);
                        }
                        Err(e) => println!("GET failed: {}", e),
                    }
                }
            }
            Err(e) => println!("Failed to connect: {}", e),
        }
        println!();
    }

    // Example 5: Dangerous mode for testing (accepts invalid certs)
    #[cfg(any(feature = "tls-rustls", feature = "tls-native-tls"))]
    {
        println!("=== Example 5: Testing mode (accepts invalid certs) ===");
        println!("WARNING: Only use this for testing with self-signed certificates!");

        #[cfg(feature = "tls-rustls")]
        {
            use redis_tower::tls::TlsConfig;

            let tls = TlsConfig::rustls()
                .danger_accept_invalid_certs(true)
                .build()?;

            match RedisClient::connect_with_config("localhost:6380", tls).await {
                Ok(client) => {
                    println!("Connected (ignoring certificate validation)");
                    if client.call(Ping::new()).await.is_ok() {
                        println!("PING successful");
                    }
                }
                Err(e) => println!("Failed to connect: {}", e),
            }
        }

        #[cfg(all(feature = "tls-native-tls", not(feature = "tls-rustls")))]
        {
            use redis_tower::tls::TlsConfig;

            let tls = TlsConfig::native_tls()
                .danger_accept_invalid_certs(true)
                .build()?;

            match RedisClient::connect_with_config("localhost:6380", tls).await {
                Ok(client) => {
                    println!("Connected (ignoring certificate validation)");
                    if let Ok(_) = client.call(Ping::new()).await {
                        println!("PING successful");
                    }
                }
                Err(e) => println!("Failed to connect: {}", e),
            }
        }
        println!();
    }

    #[cfg(not(any(feature = "tls-rustls", feature = "tls-native-tls")))]
    {
        println!("No TLS features enabled!");
        println!(
            "Enable with: cargo run --example tls_connection --features tls-rustls,tls-rustls-ring"
        );
    }

    Ok(())
}
