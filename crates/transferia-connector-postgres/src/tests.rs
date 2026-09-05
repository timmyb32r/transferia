use super::*;

#[test]
fn registration_exposes_only_the_bounded_copy_tuning_surface() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    register(&mut builder, &Arc::new(MetricsRegistry::new()))?;
    let registry = builder.build();

    assert_eq!(
        tuning_contract(
            registry.tuning_parameters("postgres", transferia_registry::EndpointRole::Source)?
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
                "/copy_to_format",
                serde_json::json!("binary"),
                vec![serde_json::json!("binary"), serde_json::json!("text")],
            ),
        ]
    );
    assert_eq!(
        tuning_contract(
            registry.tuning_parameters("postgres", transferia_registry::EndpointRole::Sink)?
        )?,
        vec![(
            "/copy_from_format",
            serde_json::json!("binary"),
            vec![serde_json::json!("binary"), serde_json::json!("text")],
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
                } => candidates
                    .iter()
                    .copied()
                    .map(serde_json::Value::from)
                    .collect(),
                transferia_registry::tuning::TuningParameter::Choice { values, .. } => {
                    values.clone()
                }
                other => anyhow::bail!("unexpected PostgreSQL tuning parameter: {other:?}"),
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
    assert!(result
        .message
        .unwrap()
        .contains("Authentication was not checked"));
    accept.await.unwrap();
}

#[test]
fn checker_config_ignores_source_and_sink_specific_fields() {
    let source: postgres::PostgresConnectionCheckConfig = serde_yaml::from_str(
        "host: db.example\nport: 5432\ntables: {rules: [{include: public.events}]}\nbatch_rows: 10\n",
    )
    .unwrap();
    let sink: postgres::PostgresConnectionCheckConfig =
        serde_yaml::from_str("host: db.example\nport: 5432\ncreate_tables: true\n").unwrap();

    assert!(!source.credentials_complete());
    assert!(!sink.credentials_complete());
}
