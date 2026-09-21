#![allow(clippy::expect_used, clippy::unwrap_used, reason = "test assertions")]

use std::path::PathBuf;
use std::time::Duration;

use mysql_async::prelude::Queryable as _;
use testcontainers::CopyTargetOptions;
use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use transferia_connector_mysql::mysql::{connect, MySqlConnectionConfig};

// This is a separate test executable: no other connector can install a global
// provider before MySQL connects. The dev dependency enables ring alongside the
// production aws-lc-rs backend, reproducing workspace feature unification.
#[tokio::test]
async fn mysql_tls_with_both_crypto_backends_needs_no_process_default() -> anyhow::Result<()> {
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    let fixtures = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let container = GenericImage::new("mysql", "8.4.6")
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var("MYSQL_ROOT_PASSWORD", "test")
        .with_env_var("MYSQL_DATABASE", "transferia")
        .with_copy_to("/tls/ca.pem", fixtures.join("localhost-ca.pem"))
        .with_copy_to("/tls/server.pem", fixtures.join("localhost-server.pem"))
        .with_copy_to(
            CopyTargetOptions::new("/tls/key.pem").with_mode(0o644),
            fixtures.join("localhost-key.pem"),
        )
        .with_cmd([
            "--ssl-ca=/tls/ca.pem",
            "--ssl-cert=/tls/server.pem",
            "--ssl-key=/tls/key.pem",
            "--require-secure-transport=ON",
        ])
        .start()
        .await?;
    let host = container.get_host().await?.to_string();
    anyhow::ensure!(
        matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1" | "[::1]"),
        "TLS fixture certificate requires a local Docker endpoint, got {host}"
    );
    let config = MySqlConnectionConfig {
        host: "localhost".into(),
        port: container.get_host_port_ipv4(3306.tcp()).await?,
        database: "transferia".into(),
        username: "root".into(),
        password: "test".into(),
        trusted_plaintext: false,
        tls_ca_file: Some(fixtures.join("localhost-ca.pem").to_string_lossy().into_owned()),
    };
    // MySQL emits readiness once for its socket-only initialization server too.
    let mut connection = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            match connect(&config).await {
                Ok(connection) => break connection,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await?;
    let (_, cipher): (String, String) = connection
        .query_first("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        .await?
        .expect("TLS status exists");
    assert!(!cipher.is_empty(), "the successful connection must use TLS");
    assert_eq!(connection.query_first::<u64, _>("SELECT 9007199254740993").await?, Some(9_007_199_254_740_993));
    connection.disconnect().await?;

    let mut untrusted = config.clone();
    untrusted.tls_ca_file = None;
    assert!(connect(&untrusted).await.is_err(), "unknown CA must still fail");
    let mut wrong_name = config;
    wrong_name.host = "127.0.0.1".into();
    assert!(connect(&wrong_name).await.is_err(), "DNS-only certificate must not authenticate an IP");
    assert!(rustls::crypto::CryptoProvider::get_default().is_none());
    Ok(())
}
