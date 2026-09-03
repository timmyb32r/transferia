use super::*;

#[test]
fn registration_exposes_only_the_bounded_batch_tuning_surface() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    register(&mut builder, &Arc::new(MetricsRegistry::new()))?;
    let registry = builder.build();

    assert_eq!(
        tuning_contract(
            registry.tuning_parameters("mysql", transferia_registry::EndpointRole::Source)?
        )?,
        vec![
            (
                "/batch_rows",
                serde_json::json!(16_384),
                vec![
                    serde_json::json!(16_384),
                    serde_json::json!(65_536),
                    serde_json::json!(262_144),
                    serde_json::json!(1_048_576),
                ],
            ),
            (
                "/read_protocol",
                serde_json::json!("binary"),
                vec![serde_json::json!("text"), serde_json::json!("binary")],
            ),
        ]
    );
    assert_eq!(
        tuning_contract(
            registry.tuning_parameters("mysql", transferia_registry::EndpointRole::Sink)?
        )?,
        vec![(
            "/insert_rows",
            serde_json::json!(1_000),
            vec![
                serde_json::json!(100),
                serde_json::json!(250),
                serde_json::json!(1_000),
                serde_json::json!(4_000),
            ],
        )]
    );
    Ok(())
}

fn tuning_contract(
    parameters: &[transferia_registry::tuning::TuningParameter],
) -> anyhow::Result<Vec<(&str, serde_json::Value, Vec<serde_json::Value>)>> {
    parameters
        .iter()
        .map(|parameter| {
            let candidates = match parameter {
                transferia_registry::tuning::TuningParameter::UnsignedInteger {
                    candidates,
                    ..
                } => candidates.iter().copied().map(serde_json::Value::from).collect(),
                transferia_registry::tuning::TuningParameter::Choice { values, .. } => {
                    values.clone()
                }
                other => anyhow::bail!("unexpected MySQL tuning parameter: {other:?}"),
            };
            Ok((parameter.pointer(), parameter.baseline(), candidates))
        })
        .collect()
}

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
