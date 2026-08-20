use super::*;

#[tokio::test]
async fn incomplete_credentials_produce_a_network_only_result() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accept = tokio::spawn(async move { listener.accept().await.unwrap() });
    let result = check_postgres_connection(postgres::PostgresConnectionCheckConfig {
        host: address.ip().to_string(),
        port: address.port(),
        database: String::new(),
        username: String::new(),
        password: String::new(),
        trusted_plaintext: true,
        tls_ca_file: None,
    })
    .await
    .unwrap();

    assert!(matches!(
        result.status,
        transferia_registry::ConnectionCheckStatus::NetworkReachable
    ));
    assert!(result.message.unwrap().contains("Authentication was not checked"));
    accept.await.unwrap();
}

#[test]
fn checker_config_ignores_source_and_sink_specific_fields() {
    let source: postgres::PostgresConnectionCheckConfig = serde_yaml::from_str(
        "host: db.example\nport: 5432\ntables: [{schema: public, name: events}]\nbatch_rows: 10\n",
    )
    .unwrap();
    let sink: postgres::PostgresConnectionCheckConfig = serde_yaml::from_str(
        "host: db.example\nport: 5432\ncreate_tables: true\n",
    )
    .unwrap();

    assert!(!source.credentials_complete());
    assert!(!sink.credentials_complete());
}
