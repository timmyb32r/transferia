use super::*;

#[tokio::test]
async fn incomplete_credentials_produce_a_network_only_result() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accept = tokio::spawn(async move { listener.accept().await.unwrap() });
    let result = check_mysql_connection(mysql::MySqlConnectionCheckConfig {
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
    assert!(result
        .message
        .unwrap()
        .contains("Authentication was not checked"));
    accept.await.unwrap();
}

#[test]
fn checker_config_ignores_source_specific_fields() {
    let source: mysql::MySqlConnectionCheckConfig = serde_yaml::from_str(
        "host: db.example\nport: 3306\ntables: [{name: events}]\nbatch_rows: 10\n",
    )
    .unwrap();

    assert!(!source.credentials_complete());
}

#[test]
fn sink_hides_internal_insert_batch_tuning() {
    let schema = serde_json::to_value(schemars::schema_for!(mysql::sink::MySqlSinkConfig)).unwrap();
    assert_eq!(
        schema["properties"]["insert_rows"]["x-ui"]["widget"],
        "hidden"
    );
    assert_eq!(schema["properties"]["insert_rows"]["default"], 1_000);
}
