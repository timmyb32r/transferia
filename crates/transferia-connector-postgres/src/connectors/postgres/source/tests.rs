use std::sync::Arc;

#[test]
fn metadata_batch_keeps_native_types_and_explicit_pseudo_type_policy() -> anyhow::Result<()> {
    use tokio_postgres::types::Type;
    use super::{metadata::catalog_type, UnsupportedTypePolicy};
    for native in [Type::INT8, Type::TIMESTAMPTZ, Type::BYTEA, Type::NUMERIC, Type::INT4_ARRAY] {
        let catalog = catalog_type(native.oid(), native.name().into(), "b", "pg_catalog".into())?;
        assert_eq!(UnsupportedTypePolicy::Fail.arrow_type(&catalog)?, UnsupportedTypePolicy::Fail.arrow_type(&native)?);
    }
    for kind in ["b", "e", "c", "r", "m"] {
        assert_eq!(UnsupportedTypePolicy::Fail.arrow_type(&catalog_type(90000, "custom_type".into(), kind, "public".into())?)?, arrow::datatypes::DataType::Utf8);
    }
    let pseudo = catalog_type(Type::ANYARRAY.oid(), "anyarray".into(), "p", "pg_catalog".into())?;
    assert!(UnsupportedTypePolicy::Fail.arrow_type(&pseudo).is_err());
    assert_eq!(UnsupportedTypePolicy::ToString.arrow_type(&pseudo)?, arrow::datatypes::DataType::Utf8);
    assert!(catalog_type(90000, "unknown".into(), "?", "public".into()).is_err());
    assert!(super::metadata::CATALOG_QUERY.contains("WITH RECURSIVE"));
    assert!(super::metadata::CATALOG_QUERY.contains("t.physical_oid = a.atttypid AND t.typbasetype = 0"));
    Ok(())
}

#[test]
fn metadata_projection_batch_preserves_quoted_tables_without_combining_column_widths() {
    let tables = (0..100).map(|index| (transferia_registry::TableIdentity {
        namespace: "schema\"quoted".into(), name: format!("table{index}"),
    }, "\"id\", \"value\"::text AS \"value\"".into())).collect::<Vec<_>>();
    let query = super::metadata::projection_query(&tables);
    assert_eq!(query.matches("SELECT 1 FROM (SELECT").count(), 100);
    assert_eq!(query.matches(" UNION ALL ").count(), 99);
    assert!(query.contains("FROM \"schema\"\"quoted\".\"table99\" LIMIT 0) AS metadata_99"));
    assert!(!query.contains("pg_export_snapshot"));
}

#[tokio::test]
async fn validation_pins_pooler_backend_and_preserves_database_error() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    fn message(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut bytes = vec![tag];
        bytes.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
        bytes.extend_from_slice(body);
        bytes
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let size = socket.read_u32().await.unwrap();
        let mut startup = vec![0; size as usize - 4];
        socket.read_exact(&mut startup).await.unwrap();
        socket.write_all(&[message(b'R', &0_u32.to_be_bytes()), message(b'Z', b"I")].concat()).await.unwrap();
        let mut began = false;
        loop {
            let tag = socket.read_u8().await.unwrap();
            let size = socket.read_u32().await.unwrap();
            let mut body = vec![0; size as usize - 4];
            socket.read_exact(&mut body).await.unwrap();
            if tag == b'Q' {
                let sql = std::str::from_utf8(&body).unwrap();
                if !began {
                    assert!(sql.starts_with("BEGIN TRANSACTION"));
                    assert!(sql.contains("READ ONLY"));
                    began = true;
                    socket.write_all(&[message(b'C', b"BEGIN\0"), message(b'Z', b"T")].concat()).await.unwrap();
                } else {
                    assert_eq!(sql, "ROLLBACK\0");
                    socket.write_all(&[message(b'C', b"ROLLBACK\0"), message(b'Z', b"I")].concat()).await.unwrap();
                    break;
                }
            } else if tag == b'P' {
                assert!(began, "metadata must not prepare outside a transaction");
                socket.write_all(&message(b'E', b"SERROR\0C42501\0Mpermission denied for table private_table\0Dprivate details\0\0")).await.unwrap();
            } else if tag == b'S' {
                socket.write_all(&message(b'Z', b"E")).await.unwrap();
            }
        }
    });
    let (client, connection) = tokio_postgres::connect(
        &format!("host={} port={} user=reader sslmode=disable", address.ip(), address.port()),
        tokio_postgres::NoTls,
    ).await.unwrap();
    tokio::spawn(async move { drop(connection.await); });
    let error = super::connector::discover_validation_tables(&client, &[TableConfig {
        schema: "public".into(), name: "private_table".into(),
    }], super::UnsupportedTypePolicy::Fail).await.err().expect("discovery must report the database failure").to_string();
    server.await.unwrap();
    assert!(error.contains("public.private_table"));
    assert!(error.contains("permission denied for table private_table (SQLSTATE 42501)"));
    assert!(!error.contains("private details"));
}

use arrow::datatypes::DataType;

use super::config::PostgresSourceConfig;
use super::connector::{
    classify_replication_connector_error, incoming_user_schema,
    require_replication_replay_identity, validate_replication_table_identities, DiscoveredTable,
    PostgresSourceConnector,
};
use super::TableConfig;
use crate::connectors::postgres::src_stream::replication_safety_violation;
use crate::connectors::postgres::PostgresCopyFormat;
use crate::metrics::MetricsRegistry;
use transferia_delivery_contracts::semantics::{RecordSemantics, SourceBehavior};
use transferia_delivery_contracts::DeliveryType;
use transferia_registry::SourceConnector;

const MINIMAL_SOURCE_CONFIG: &str = "host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables: {type: all}\n";

#[test]
fn system_table_filter_defaults_to_enabled_above_table_selection() -> anyhow::Result<()> {
    let config: PostgresSourceConfig = serde_yaml::from_str(MINIMAL_SOURCE_CONFIG)?;
    assert!(config.hide_system_tables);
    let explicit: PostgresSourceConfig = serde_yaml::from_str(&format!(
        "{MINIMAL_SOURCE_CONFIG}hide_system_tables: false\n"
    ))?;
    assert!(!explicit.hide_system_tables);
    let schema = serde_json::to_value(schemars::schema_for!(PostgresSourceConfig))?;
    let field = &schema["properties"]["hide_system_tables"];
    assert_eq!(field["title"], "Hide system tables");
    assert_eq!(field["default"], true);
    assert_eq!(field["x-ui"]["order"], 1);
    assert_eq!(schema["properties"]["tables"]["x-ui"]["order"], 2);
    Ok(())
}

#[test]
fn table_selection_filters_system_schemas_without_hiding_user_table_names() -> anyhow::Result<()> {
    let mut config: PostgresSourceConfig = serde_yaml::from_str(MINIMAL_SOURCE_CONFIG)?;
    let namespaces = [
        "pg_catalog",
        "pg_toast",
        "pg_temp_1",
        "pg_toast_temp_1",
        "information_schema",
        "public",
        "reports",
        "pgreports",
        "information_schema_extra",
        "PG_CATALOG",
    ];
    let catalog = namespaces
        .iter()
        .map(|namespace| transferia_registry::TableIdentity {
            namespace: (*namespace).into(),
            name: "pg_events".into(),
        })
        .collect::<Vec<_>>();
    let mut visible = catalog[5..].to_vec();
    visible.sort();
    let mut all = catalog.clone();
    all.sort();
    for selection in [
        "type: all",
        "type: selected\nrules:\n  - include: '*'",
        "type: selected\nrules:\n  - include: '.*'\n    include_mode: regex",
    ] {
        config.tables = serde_yaml::from_str(selection)?;
        config.hide_system_tables = true;
        assert_eq!(config.resolve_tables(catalog.clone())?, visible);
        config.hide_system_tables = false;
        assert_eq!(config.resolve_tables(catalog.clone())?, all);
    }
    Ok(())
}

#[test]
fn hidden_table_rules_fail_before_startup_instead_of_silently_selecting_nothing(
) -> anyhow::Result<()> {
    let mut config: PostgresSourceConfig = serde_yaml::from_str(MINIMAL_SOURCE_CONFIG)?;
    let catalog = vec![transferia_registry::TableIdentity {
        namespace: "pg_catalog".into(),
        name: "pg_class".into(),
    }];
    for selection in [
        "type: selected\nrules:\n  - include: pg_catalog.pg_class",
        "type: all",
    ] {
        config.tables = serde_yaml::from_str(selection)?;
        config.hide_system_tables = true;
        assert!(config.resolve_tables(catalog.clone()).is_err());
        config.hide_system_tables = false;
        assert_eq!(config.resolve_tables(catalog.clone())?, catalog);
    }
    Ok(())
}

#[test]
fn replication_safety_violations_are_fatal_source_build_failures() {
    let error = replication_safety_violation(anyhow::anyhow!(
        "durable PostgreSQL replication identity changed"
    ));
    let classified = classify_replication_connector_error(error);
    let failure = classified
        .downcast_ref::<transferia_core::failure::DataPlaneFailure>()
        .expect("replication safety violation must retain a data-plane disposition");
    assert!(!failure.is_retryable());
}

#[test]
fn transient_source_build_failures_keep_their_original_classification() {
    let classified = classify_replication_connector_error(anyhow::anyhow!(
        "temporary PostgreSQL connection failure"
    ));
    assert!(classified
        .downcast_ref::<transferia_core::failure::DataPlaneFailure>()
        .is_none());
    assert_eq!(
        classified.to_string(),
        "temporary PostgreSQL connection failure"
    );
}

#[test]
fn replication_requires_a_nonempty_replay_identity_before_durable_state() {
    for replay_identity in [None, Some(Arc::<str>::from(""))] {
        let error = require_replication_replay_identity(replay_identity)
            .expect_err("missing replay identity must fail closed");
        assert!(crate::connectors::postgres::src_stream::is_replication_safety_violation(&error));
    }
}

#[test]
fn snapshot_and_replication_share_nullable_incoming_user_fields_without_weakening_storage() {
    let stored = transferia_core::data::schema::DatasetSchema::new(vec![
        transferia_core::data::schema::SchemaColumn::new("id".into(), DataType::Int64, false),
        transferia_core::data::schema::SchemaColumn::new("payload".into(), DataType::Utf8, true),
    ]);

    let incoming = incoming_user_schema(&stored);

    assert!(incoming.columns.iter().all(|column| column.nullable));
    assert!(!stored.columns[0].nullable);
    assert!(stored.columns[1].nullable);
}

#[test]
fn source_config_requires_explicit_plaintext_trust_and_tables() {
    let config: PostgresSourceConfig =
        serde_yaml::from_str("host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: false\ntables: {type: selected, rules: []}\n").unwrap();
    assert!(config.validate().is_err());
}

#[test]
fn snapshot_copy_to_format_defaults_to_binary_and_accepts_explicit_text() {
    let binary: PostgresSourceConfig = serde_yaml::from_str(
        "host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  type: selected\n  rules:\n    - include: public.events\n",
    )
    .unwrap();
    let text: PostgresSourceConfig = serde_yaml::from_str(
        "host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: true\ncopy_to_format: text\ntables:\n  type: selected\n  rules:\n    - include: public.events\n",
    )
    .unwrap();

    assert_eq!(binary.copy_to_format, PostgresCopyFormat::Binary);
    assert_eq!(text.copy_to_format, PostgresCopyFormat::Text);
}

#[test]
fn unsupported_type_default_is_to_string_for_batch_and_fail_for_replication() {
    use super::UnsupportedTypePolicy;
    let base = serde_json::json!({
        "host": "localhost", "port": 5432, "database": "postgres", "username": "postgres",
        "password": "test", "trusted_plaintext": true, "tables": {"type": "all"}
    });
    for explicit in [None, Some("fail"), Some("to_string")] {
        for mode in [DeliveryType::Batch, DeliveryType::Stream, DeliveryType::BatchAndStream] {
            let mut value = base.clone();
            if let Some(policy) = explicit { value["unsupported_types"] = serde_json::json!(policy); }
            let config: PostgresSourceConfig = serde_json::from_value(value).unwrap();
            let result = config.unsupported_type_policy(mode);
            let unsupported = explicit == Some("to_string") && mode != DeliveryType::Batch;
            if unsupported {
                assert!(result.unwrap_err().to_string().contains("supported only for batch"));
            } else {
                let policy = result.unwrap();
                let converts = mode == DeliveryType::Batch && explicit != Some("fail");
                assert_eq!(policy, if converts { UnsupportedTypePolicy::ToString } else { UnsupportedTypePolicy::Fail });
                let arrow_type = policy.arrow_type(&tokio_postgres::types::Type::ANYARRAY);
                if converts { assert_eq!(arrow_type.unwrap(), DataType::Utf8); }
                else { assert!(arrow_type.is_err()); }
                assert_eq!(policy.arrow_type(&tokio_postgres::types::Type::INT8).unwrap(), DataType::Int64);
            }
            let connector = PostgresSourceConnector::from_config(config, Arc::new(MetricsRegistry::default())).unwrap();
            assert_eq!(connector.metadata_reader(mode).is_err(), unsupported);
        }
    }
    for invalid in [serde_json::Value::Null, serde_json::json!("invalid")] {
        let mut value = base.clone();
        value["unsupported_types"] = invalid;
        assert!(serde_json::from_value::<PostgresSourceConfig>(value).is_err());
    }
    let schema = serde_json::to_value(schemars::schema_for!(PostgresSourceConfig)).unwrap();
    let property = &schema["properties"]["unsupported_types"];
    assert_eq!(property["default"], "to_string");
    assert_eq!(property["$ref"], "#/$defs/UnsupportedTypePolicy");
    assert_eq!(property["x-ui"]["section"], "advanced");
    assert_eq!(property["x-ui"]["delivery_types"], serde_json::json!(["batch"]));
    assert_eq!(schema["$defs"]["UnsupportedTypePolicy"]["oneOf"][0]["const"], "to_string");
}

#[test]
fn source_rejects_the_old_connection_string() {
    assert!(serde_yaml::from_str::<PostgresSourceConfig>(
        "connection: host=localhost port=5432\ntrusted_plaintext: true\ntables: {type: selected, rules: []}\n"
    )
    .is_err());
}

#[test]
fn delivery_type_alone_selects_postgres_record_semantics() {
    for settings in [
        "",
        "\nreplication:\n  plugin:\n    type: pgoutput\n    publication: transferia_publication\n",
    ] {
        let config: PostgresSourceConfig = serde_yaml::from_str(&format!(
            "host: localhost\nport: 5432\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  type: selected\n  rules:\n    - include: public.events\n{settings}",
        )).unwrap();
        let connector =
            PostgresSourceConnector::from_config(config, Arc::new(MetricsRegistry::default()))
                .unwrap();
        for mode in [
            DeliveryType::Batch,
            DeliveryType::Stream,
            DeliveryType::BatchAndStream,
        ] {
            let descriptor = connector.compatibility(mode);
            assert!(descriptor.supports_delivery_type(mode));
            assert_eq!(
                descriptor.record_semantics(),
                Some(if mode == DeliveryType::Batch {
                    RecordSemantics::AppendOnly
                } else {
                    RecordSemantics::Changelog
                })
            );
            assert_eq!(
                descriptor.source_behavior(),
                Some(if mode == DeliveryType::Batch {
                    SourceBehavior::FiniteAppendOnlyRows
                } else {
                    SourceBehavior::ChangelogRows
                })
            );
        }
    }
}

#[test]
fn source_schema_inlines_replication_plugin_only_for_replication_modes() {
    let schema = serde_json::to_value(schemars::schema_for!(PostgresSourceConfig)).unwrap();
    assert_eq!(
        schema.pointer("/x-ui/capabilities"),
        Some(&serde_json::json!({
            "component": "source", "key": "postgres",
            "batch_stream_handoff": "exact_switchover",
            "delivery_modes": ["batch", "stream", "batch_and_stream"],
            "record_semantics": ["append_only", "changelog"]
        }))
    );
    assert_eq!(
        schema.pointer("/properties/replication/x-ui"),
        Some(&serde_json::json!({
            "widget": "inline_object", "section": "advanced",
            "delivery_types": ["stream", "batch_and_stream"]
        }))
    );
    assert!(schema
        .pointer("/$defs/PostgresReplicationConfig/properties/plugin/x-ui/section")
        .is_none());
    assert!(schema.pointer("/properties/replication/anyOf").is_none());
    assert!(schema
        .pointer("/$defs/PostgresReplicationConfig/x-ui/capabilities")
        .is_none());
}

#[tokio::test]
async fn batch_preparation_needs_no_replication_context_and_cannot_reuse_stream_caches() {
    let config: PostgresSourceConfig = serde_yaml::from_str(
        "host: 127.0.0.1\nport: 1\ndatabase: postgres\nusername: postgres\npassword: test\ntrusted_plaintext: true\ntables:\n  type: selected\n  rules:\n    - include: public.events\n",
    ).unwrap();
    let context = |delivery_type| transferia_registry::SourceExecutionContext {
        request: transferia_core::delivery::DeliveryDiscoveryRequest {
            keep_system_columns: true,
        },
        cancellation: tokio_util::sync::CancellationToken::new(),
        delivery_type,
        replay_identity: None,
        durable: transferia_test_support::durable_context(),
    };
    let connector =
        PostgresSourceConnector::from_config(config.clone(), Arc::new(MetricsRegistry::default()))
            .unwrap();
    // No server is listening: batch preparation must not connect or validate a slot ID.
    assert!(connector
        .prepare_execution(context(DeliveryType::Batch))
        .await
        .unwrap()
        .is_none());
    let error = connector
        .prepare_execution(context(DeliveryType::Stream))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("different delivery type"));
    let stream =
        PostgresSourceConnector::from_config(config, Arc::new(MetricsRegistry::default())).unwrap();
    let error = stream
        .prepare_execution(context(DeliveryType::Stream))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("replay identity"));
}

#[test]
fn replication_requires_an_exact_primary_key_or_full_old_row_identity() {
    let table = |replica_identity: &str, primary_key: bool| DiscoveredTable {
        config: TableConfig {
            schema: "public".to_owned(),
            name: "events".to_owned(),
        },
        schema: transferia_core::data::schema::DatasetSchema::new(vec![
            transferia_core::data::schema::SchemaColumn::new(
                "id".to_owned(),
                DataType::Int64,
                false,
            )
            .with_constraints(primary_key, false, None),
        ]),
        type_oids: vec![20],
        replica_identity_full: replica_identity == "f",
        replica_identity: replica_identity.to_owned(),
        relation_oid: 42,
    };

    validate_replication_table_identities(&[table("d", true)]).unwrap();
    validate_replication_table_identities(&[table("f", false)]).unwrap();

    let no_primary_key = validate_replication_table_identities(&[table("d", false)])
        .unwrap_err()
        .to_string();
    assert!(
        no_primary_key.contains("has no primary key"),
        "{no_primary_key}"
    );

    let index_identity = validate_replication_table_identities(&[table("i", true)])
        .unwrap_err()
        .to_string();
    assert!(
        index_identity.contains("USING INDEX")
            && index_identity.contains("primary-key row identity"),
        "{index_identity}"
    );

    let no_identity = validate_replication_table_identities(&[table("n", true)])
        .unwrap_err()
        .to_string();
    assert!(no_identity.contains("IDENTITY NOTHING"), "{no_identity}");
}
