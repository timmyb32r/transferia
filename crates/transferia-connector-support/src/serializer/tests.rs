use std::sync::Arc;

use super::*;
use base64::Engine as _;

#[test]
fn schema_registry_serializer_exposes_connection_subject_and_format() {
    let schema = schemars::schema_for!(SerializerConfig);
    let json = serde_json::to_value(schema).expect("schema serializes");
    let schema = json.to_string();
    for field in ["url", "auth", "ca_certificate", "subject", "format"] {
        assert!(
            schema.contains(field),
            "serializer schema is missing {field}"
        );
    }
}

#[test]
fn debezium_is_one_serializer_with_four_conditionally_typed_formats() {
    let schema =
        serde_json::to_string(&schemars::schema_for!(SerializerConfig)).expect("schema serializes");
    assert!(schema.contains("\"title\":\"Debezium\""));
    for title in [
        "JSON (without Schema Registry)",
        "JSON (with Schema Registry)",
        "Avro",
        "Protobuf",
    ] {
        assert!(
            schema.contains(&format!("\"title\":\"{title}\"")),
            "{title}"
        );
    }
    assert!(!schema.contains("debezium_json"));
    assert!(!schema.contains("debezium_schema_registry"));

    for format in ["json", "json_schema", "avro", "protobuf"] {
        let registry = if format == "json" {
            String::new()
        } else {
            "\n  connection:\n    url: http://registry\n    request_timeout_ms: 1000\n    auth:\n      type: none\n  key_subject: inventory.accounts-key\n  value_subject: inventory.accounts-value".to_owned()
        };
        let indexes = if format == "protobuf" {
            "\n  key_message_indexes: [0]\n  value_message_indexes: [0]"
        } else {
            ""
        };
        let yaml = format!(
            "type: debezium\nformat:\n  type: {format}{registry}{indexes}\n"
        );
        let config: SerializerConfig = serde_yaml::from_str(&yaml).unwrap();
        config.validate().unwrap();
    }
}

#[test]
fn protobuf_serializer_rejects_empty_message_path() {
    let config = SerializerConfig::SchemaRegistry {
        connection: crate::schema_registry::SchemaRegistryConnection {
            url: "http://registry".to_owned(),
            request_timeout_ms: 1_000,
            auth: crate::schema_registry::SchemaRegistryAuth::None,
            ca_certificate: None,
        },
        subject: "topic-value".to_owned(),
        format: crate::schema_registry::SchemaFormat::Protobuf,
        protobuf_message_indexes: Vec::new(),
    };
    assert!(config.validate().is_err());
}

#[test]
fn debezium_uses_delivery_name_instead_of_exposing_a_logical_name_setting() {
    let schema = serde_json::to_value(schemars::schema_for!(SerializerConfig))
        .expect("serializer schema serializes");
    let schema = schema.to_string();
    assert!(!schema.contains("logical_name"), "{schema}");
    assert!(!schema.contains("Logical source name"), "{schema}");

    let config = SerializerConfig::Debezium {
        format: DebeziumFormat::Json,
    };
    for invalid_name in ["", " Inventory delivery", "Inventory delivery "] {
        assert!(DeliverySerializer::new(
            &config,
            QueueMessageMode::KeyedWithTombstones,
            invalid_name,
        )
        .is_err());
    }
    DeliverySerializer::new(
        &config,
        QueueMessageMode::KeyedWithTombstones,
        "Inventory delivery",
    )
    .expect("a valid delivery name is the Debezium logical source name");
}

#[test]
fn serializers_publish_their_record_semantics() {
    use transferia_delivery_contracts::semantics::RecordSemantics;

    let serializers = [
        (SerializerConfig::Json, &[RecordSemantics::AppendOnly][..]),
        (
            SerializerConfig::SchemaRegistry {
                connection: registry_connection(),
                subject: "events-value".to_owned(),
                format: crate::schema_registry::SchemaFormat::JsonSchema,
                protobuf_message_indexes: vec![0],
            },
            &[RecordSemantics::AppendOnly][..],
        ),
        (
            SerializerConfig::Debezium {
                format: DebeziumFormat::Json,
            },
            &[RecordSemantics::AppendOnly, RecordSemantics::Changelog][..],
        ),
        (
            SerializerConfig::Debezium {
                format: DebeziumFormat::JsonSchema {
                    connection: registry_connection(),
                    key_subject: Some("inventory.events-key".to_owned()),
                    value_subject: "inventory.events-value".to_owned(),
                },
            },
            &[RecordSemantics::AppendOnly, RecordSemantics::Changelog][..],
        ),
    ];

    for (serializer, expected) in serializers {
        assert_eq!(serializer.record_semantics(), expected);
        assert_eq!(
            serializer.supports_changelog(),
            expected.contains(&RecordSemantics::Changelog)
        );
    }
    assert_eq!(
        SerializerConfig::SUPPORTED_RECORD_SEMANTICS,
        [RecordSemantics::AppendOnly, RecordSemantics::Changelog]
    );
}

#[test]
fn keyed_debezium_schema_registry_requires_both_subjects() {
    let config = SerializerConfig::Debezium {
        format: DebeziumFormat::JsonSchema {
            connection: registry_connection(),
            key_subject: None,
            value_subject: "inventory.accounts-value".to_owned(),
        },
    };

    assert!(config.validate().is_ok());
    let error = DeliverySerializer::new(
        &config,
        QueueMessageMode::KeyedWithTombstones,
        "Inventory delivery",
    )
    .err()
    .expect("keyed mode must reject an absent key subject")
    .to_string();
    assert!(error.contains("key_subject"), "{error}");
    DeliverySerializer::new(
        &config,
        QueueMessageMode::ValuesOnly,
        "Inventory delivery",
    )
    .unwrap();
}

#[test]
fn debezium_schema_registry_rejects_invalid_subjects_and_protobuf_paths() {
    let mut config = SerializerConfig::Debezium {
        format: DebeziumFormat::Protobuf {
            connection: registry_connection(),
            key_subject: Some("inventory.accounts-key".to_owned()),
            value_subject: " inventory.accounts-value".to_owned(),
            key_message_indexes: vec![0],
            value_message_indexes: vec![0],
        },
    };
    assert!(config.validate().is_err());

    let SerializerConfig::Debezium {
        format:
            DebeziumFormat::Protobuf {
                value_subject,
                value_message_indexes,
                ..
            },
        ..
    } = &mut config
    else {
        panic!("test fixture changed serializer variant")
    };
    *value_subject = "inventory.accounts-value".to_owned();
    value_message_indexes.clear();
    assert!(config.validate().is_err());
}

#[test]
fn debezium_discovery_fails_closed_without_every_cdc_control() {
    use arrow::datatypes::DataType;
    use transferia_core::data::schema::{
        DatasetSchema, SchemaColumn, SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
        SYSTEM_ROLE_EVENT_TIMESTAMP_NS, SYSTEM_ROLE_EVENT_TIMESTAMP_US,
        SYSTEM_ROLE_SOURCE_DATABASE, SYSTEM_ROLE_SOURCE_SCHEMA, SYSTEM_ROLE_SOURCE_TABLE,
        SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
        SYSTEM_ROLE_SOURCE_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
    };
    use transferia_core::delivery::{
        DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
    };
    use transferia_core::SystemColumnKind;

    let config = SerializerConfig::Debezium {
        format: DebeziumFormat::Json,
    };
    let id = SchemaColumn::new("id".to_owned(), DataType::Int64, true)
        .with_constraints(true, false, None);
    let old_id = SchemaColumn::new("_system_old_key_0".to_owned(), DataType::Int64, true)
        .with_old_key_of("id".to_owned());
    let mut incoming = vec![id.clone(), old_id];
    incoming.extend(
        [
            (SYSTEM_ROLE_SOURCE_DATABASE, DataType::Utf8),
            (SYSTEM_ROLE_SOURCE_SCHEMA, DataType::Utf8),
            (SYSTEM_ROLE_SOURCE_TABLE, DataType::Utf8),
            (SYSTEM_ROLE_SOURCE_TRANSACTION_ID, DataType::UInt64),
            (SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, DataType::Int64),
            (SYSTEM_ROLE_SOURCE_TIMESTAMP_US, DataType::Int64),
            (SYSTEM_ROLE_SOURCE_TIMESTAMP_NS, DataType::Int64),
            (SYSTEM_ROLE_EVENT_TIMESTAMP_MS, DataType::Int64),
            (SYSTEM_ROLE_EVENT_TIMESTAMP_US, DataType::Int64),
            (SYSTEM_ROLE_EVENT_TIMESTAMP_NS, DataType::Int64),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (role, data_type))| {
            SchemaColumn::new(format!("_system_role_{index}"), data_type, false)
                .with_system_role(role)
        }),
    );
    incoming.extend(
        [
            SystemColumnKind::Offset,
            SystemColumnKind::ChangeOperation,
            SystemColumnKind::ChangedColumns,
        ]
        .into_iter()
        .map(|kind| SchemaColumn::new(kind.default_name().to_owned(), kind.data_type(), false)),
    );
    let mut discovery = DeliveryDiscovery {
        source_name: "postgres".into(),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: "accounts".into(),
            incoming_schema: DatasetSchema::new(incoming),
            stored_schema: DatasetSchema::new(vec![id]),
            system_columns: [
                SystemColumnKind::Offset,
                SystemColumnKind::ChangeOperation,
                SystemColumnKind::ChangedColumns,
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
        }],
        performance_advice: Vec::new(),
    };
    config.validate_discovery(&discovery).unwrap();

    discovery.datasets[0]
        .incoming_schema
        .columns
        .retain(|column| column.system_role.as_deref() != Some(SYSTEM_ROLE_SOURCE_TABLE));
    let error = config
        .validate_discovery(&discovery)
        .unwrap_err()
        .to_string();
    assert!(error.contains(SYSTEM_ROLE_SOURCE_TABLE), "{error}");
}

#[test]
fn debezium_discovery_accepts_snapshot_metadata_without_cdc_old_values() {
    use arrow::datatypes::DataType;
    use transferia_core::data::schema::{
        DatasetSchema, SchemaColumn, SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
        SYSTEM_ROLE_EVENT_TIMESTAMP_NS, SYSTEM_ROLE_EVENT_TIMESTAMP_US,
        SYSTEM_ROLE_SOURCE_DATABASE, SYSTEM_ROLE_SOURCE_SCHEMA, SYSTEM_ROLE_SOURCE_TABLE,
        SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
        SYSTEM_ROLE_SOURCE_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
    };
    use transferia_core::delivery::{
        DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
    };
    use transferia_core::SystemColumnKind;

    let id = SchemaColumn::new("id".to_owned(), DataType::Int64, false)
        .with_constraints(true, false, None);
    let roles = [
        (SYSTEM_ROLE_SOURCE_DATABASE, DataType::Utf8),
        (SYSTEM_ROLE_SOURCE_SCHEMA, DataType::Utf8),
        (SYSTEM_ROLE_SOURCE_TABLE, DataType::Utf8),
        (SYSTEM_ROLE_SOURCE_TRANSACTION_ID, DataType::UInt64),
        (SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, DataType::Int64),
        (SYSTEM_ROLE_SOURCE_TIMESTAMP_US, DataType::Int64),
        (SYSTEM_ROLE_SOURCE_TIMESTAMP_NS, DataType::Int64),
        (SYSTEM_ROLE_EVENT_TIMESTAMP_MS, DataType::Int64),
        (SYSTEM_ROLE_EVENT_TIMESTAMP_US, DataType::Int64),
        (SYSTEM_ROLE_EVENT_TIMESTAMP_NS, DataType::Int64),
    ];
    let mut incoming = vec![id.clone()];
    incoming.extend(
        roles
            .into_iter()
            .enumerate()
            .map(|(index, (role, data_type))| {
                SchemaColumn::new(format!("_system_role_{index}"), data_type, false)
                    .with_system_role(role)
            }),
    );
    incoming.push(SchemaColumn::new(
        SystemColumnKind::Offset.default_name().to_owned(),
        DataType::Int64,
        false,
    ));
    let mut discovery = DeliveryDiscovery {
        source_name: "postgres".into(),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: true,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: "accounts".into(),
            incoming_schema: DatasetSchema::new(incoming),
            stored_schema: DatasetSchema::new(vec![id]),
            system_columns: vec![SystemColumnKind::Offset.into()],
        }],
        performance_advice: Vec::new(),
    };
    let config = SerializerConfig::Debezium {
        format: DebeziumFormat::Json,
    };

    config.validate_discovery(&discovery).unwrap();

    discovery.datasets[0]
        .incoming_schema
        .columns
        .retain(|column| column.system_role.as_deref() != Some(SYSTEM_ROLE_SOURCE_SCHEMA));
    let error = config
        .validate_discovery(&discovery)
        .unwrap_err()
        .to_string();
    assert!(error.contains(SYSTEM_ROLE_SOURCE_SCHEMA), "{error}");
}

#[test]
fn mysql_debezium_discovery_requires_exact_source_changelog_and_role_contracts() {
    use arrow::datatypes::DataType;
    use transferia_core::data::schema::{
        SYSTEM_ROLE_SOURCE_GTID, SYSTEM_ROLE_SOURCE_SERVER_ID, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
    };
    use transferia_core::SystemColumnKind;

    let config = SerializerConfig::Debezium {
        format: DebeziumFormat::Json,
    };
    let discovery = mysql_debezium_discovery();
    config.validate_discovery(&discovery).unwrap();

    let mut invalid_source = discovery.clone();
    invalid_source.source_name = "MySQL".into();
    let error = config
        .validate_discovery(&invalid_source)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("exactly 'postgres', 'mysql', or 'ydb'"),
        "{error}"
    );

    let mut missing_changelog = discovery.clone();
    missing_changelog.datasets[0]
        .system_columns
        .retain(|column| column.kind != SystemColumnKind::ChangeOperation);
    let error = config
        .validate_discovery(&missing_changelog)
        .unwrap_err()
        .to_string();
    assert!(error.contains("changelog replication schema"), "{error}");

    let mut wrong_transaction_identity = discovery.clone();
    let transaction = wrong_transaction_identity.datasets[0]
        .incoming_schema
        .columns
        .iter_mut()
        .find(|column| column.system_role.as_deref() == Some(SYSTEM_ROLE_SOURCE_TRANSACTION_ID))
        .unwrap();
    transaction.data_type = DataType::UInt64;
    let error = config
        .validate_discovery(&wrong_transaction_identity)
        .unwrap_err()
        .to_string();
    assert!(error.contains(SYSTEM_ROLE_SOURCE_TRANSACTION_ID), "{error}");
    assert!(error.contains("Binary"), "{error}");

    let mut missing_server_id = discovery.clone();
    missing_server_id.datasets[0]
        .incoming_schema
        .columns
        .retain(|column| column.system_role.as_deref() != Some(SYSTEM_ROLE_SOURCE_SERVER_ID));
    let error = config
        .validate_discovery(&missing_server_id)
        .unwrap_err()
        .to_string();
    assert!(error.contains(SYSTEM_ROLE_SOURCE_SERVER_ID), "{error}");

    let mut missing_extension_metadata = discovery.clone();
    missing_extension_metadata.datasets[0]
        .incoming_schema
        .columns
        .iter_mut()
        .find(|column| column.name == "payload")
        .unwrap()
        .arrow_extension_metadata = None;
    let error = config
        .validate_discovery(&missing_extension_metadata)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("physical Arrow extension metadata"),
        "{error}"
    );

    let mut wrong_physical_type = discovery.clone();
    wrong_physical_type.datasets[0]
        .incoming_schema
        .columns
        .iter_mut()
        .find(|column| column.name == "id")
        .unwrap()
        .data_type = DataType::UInt64;
    let error = config
        .validate_discovery(&wrong_physical_type)
        .unwrap_err()
        .to_string();
    assert!(error.contains("requires Arrow Int64"), "{error}");

    let mut mismatched_old_extension = discovery.clone();
    mismatched_old_extension.datasets[0]
        .incoming_schema
        .columns
        .iter_mut()
        .find(|column| column.old_value_of.as_deref() == Some("payload"))
        .unwrap()
        .arrow_extension_metadata = Some(
            r#"{"version":1,"data_type":"char","column_type":"char(255)","unsigned":false,"character_set":"utf8mb4"}"#
                .to_owned(),
        );
    let error = config
        .validate_discovery(&mismatched_old_extension)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("does not preserve its exact physical"),
        "{error}"
    );

    let mut nonnullable_gtid = discovery;
    nonnullable_gtid.datasets[0]
        .incoming_schema
        .columns
        .iter_mut()
        .find(|column| column.system_role.as_deref() == Some(SYSTEM_ROLE_SOURCE_GTID))
        .unwrap()
        .nullable = false;
    let error = config
        .validate_discovery(&nonnullable_gtid)
        .unwrap_err()
        .to_string();
    assert!(error.contains(SYSTEM_ROLE_SOURCE_GTID), "{error}");
    assert!(error.contains("nullable Utf8"), "{error}");
}

#[test]
fn ydb_debezium_discovery_requires_exact_stream_roles_and_full_old_images() {
    use arrow::datatypes::DataType;
    use transferia_core::data::schema::{
        SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
    };
    use transferia_core::SystemColumnKind;

    let config = SerializerConfig::Debezium {
        format: DebeziumFormat::Json,
    };
    let discovery = ydb_debezium_discovery();
    config.validate_discovery(&discovery).unwrap();

    let mut wrong_identity = discovery.clone();
    wrong_identity.datasets[0]
        .incoming_schema
        .columns
        .iter_mut()
        .find(|column| column.system_role.as_deref() == Some(SYSTEM_ROLE_SOURCE_TRANSACTION_ID))
        .unwrap()
        .data_type = DataType::Binary;
    let error = config
        .validate_discovery(&wrong_identity)
        .unwrap_err()
        .to_string();
    assert!(error.contains("FixedSizeBinary(16)"), "{error}");

    let mut missing_timestamp = discovery.clone();
    missing_timestamp.datasets[0]
        .incoming_schema
        .columns
        .retain(|column| column.system_role.as_deref() != Some(SYSTEM_ROLE_SOURCE_TIMESTAMP_MS));
    let error = config
        .validate_discovery(&missing_timestamp)
        .unwrap_err()
        .to_string();
    assert!(error.contains(SYSTEM_ROLE_SOURCE_TIMESTAMP_MS), "{error}");

    let mut missing_write_timestamp = discovery.clone();
    missing_write_timestamp.datasets[0]
        .system_columns
        .retain(|column| column.kind != SystemColumnKind::WriteTimestampMs);
    let error = config
        .validate_discovery(&missing_write_timestamp)
        .unwrap_err()
        .to_string();
    assert!(error.contains("WriteTimestampMs"), "{error}");

    let mut missing_old = discovery.clone();
    missing_old.datasets[0]
        .incoming_schema
        .columns
        .retain(|column| column.old_value_of.as_deref() != Some("payload"));
    let error = config
        .validate_discovery(&missing_old)
        .unwrap_err()
        .to_string();
    assert!(error.contains("full old-value mapping"), "{error}");

    let mut unsupported = discovery;
    unsupported.datasets[0]
        .incoming_schema
        .columns
        .iter_mut()
        .find(|column| column.name == "payload")
        .unwrap()
        .data_type = DataType::Decimal128(22, 9);
    let error = config
        .validate_discovery(&unsupported)
        .unwrap_err()
        .to_string();
    assert!(error.contains("unsupported Arrow type"), "{error}");
}

fn ydb_debezium_discovery() -> transferia_core::delivery::DeliveryDiscovery {
    use arrow::datatypes::DataType;
    use transferia_core::data::schema::{
        DatasetSchema, SchemaColumn, SYSTEM_ROLE_SOURCE_DATABASE, SYSTEM_ROLE_SOURCE_TABLE,
        SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
    };
    use transferia_core::delivery::{
        DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
    };
    use transferia_core::SystemColumnKind;

    let id = SchemaColumn::new("id".to_owned(), DataType::UInt64, false)
        .with_constraints(true, false, None);
    let payload = SchemaColumn::new("payload".to_owned(), DataType::Utf8, true);
    let old_id = SchemaColumn::new("_system_old_value_0".to_owned(), DataType::UInt64, true)
        .with_old_value_of("id".to_owned());
    let old_payload = SchemaColumn::new("_system_old_value_1".to_owned(), DataType::Utf8, true)
        .with_old_value_of("payload".to_owned());
    let mut incoming = vec![id.clone(), payload.clone(), old_id, old_payload];
    incoming.extend(
        [
            (SYSTEM_ROLE_SOURCE_DATABASE, DataType::Utf8),
            (SYSTEM_ROLE_SOURCE_TABLE, DataType::Utf8),
            (
                SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
                DataType::FixedSizeBinary(16),
            ),
            (SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, DataType::Int64),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (role, data_type))| {
            SchemaColumn::new(format!("_system_role_{index}"), data_type, false)
                .with_system_role(role)
        }),
    );
    let system_kinds = [
        SystemColumnKind::Topic,
        SystemColumnKind::Partition,
        SystemColumnKind::Offset,
        SystemColumnKind::MessageIndex,
        SystemColumnKind::WriteTimestampMs,
        SystemColumnKind::ChangeOperation,
        SystemColumnKind::ChangedColumns,
    ];
    incoming.extend(
        system_kinds
            .into_iter()
            .map(|kind| SchemaColumn::new(kind.default_name().to_owned(), kind.data_type(), false)),
    );
    DeliveryDiscovery {
        source_name: "ydb".into(),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: "accounts".into(),
            incoming_schema: DatasetSchema::new(incoming),
            stored_schema: DatasetSchema::new(vec![id, payload]),
            system_columns: system_kinds.into_iter().map(Into::into).collect(),
        }],
        performance_advice: Vec::new(),
    }
}

fn mysql_debezium_discovery() -> transferia_core::delivery::DeliveryDiscovery {
    use arrow::datatypes::DataType;
    use transferia_core::data::schema::{
        DatasetSchema, SchemaColumn, SYSTEM_ROLE_EVENT_TIMESTAMP_MS,
        SYSTEM_ROLE_EVENT_TIMESTAMP_NS, SYSTEM_ROLE_EVENT_TIMESTAMP_US,
        SYSTEM_ROLE_SOURCE_BINLOG_FILE, SYSTEM_ROLE_SOURCE_BINLOG_POSITION,
        SYSTEM_ROLE_SOURCE_BINLOG_ROW, SYSTEM_ROLE_SOURCE_DATABASE, SYSTEM_ROLE_SOURCE_GTID,
        SYSTEM_ROLE_SOURCE_SCHEMA, SYSTEM_ROLE_SOURCE_SERVER_ID, SYSTEM_ROLE_SOURCE_TABLE,
        SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, SYSTEM_ROLE_SOURCE_TIMESTAMP_NS,
        SYSTEM_ROLE_SOURCE_TIMESTAMP_US, SYSTEM_ROLE_SOURCE_TRANSACTION_ID,
    };
    use transferia_core::delivery::{
        DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
    };
    use transferia_core::SystemColumnKind;

    let id = SchemaColumn::new("id".to_owned(), DataType::Int64, false)
        .with_constraints(true, false, None)
        .with_arrow_extension_metadata(
            "transferia.mysql.signed_integer",
            r#"{"version":1,"data_type":"bigint","column_type":"bigint","unsigned":false}"#,
        );
    let payload = SchemaColumn::new("payload".to_owned(), DataType::Utf8, true)
        .with_arrow_extension_metadata(
            "transferia.mysql.text",
            r#"{"version":1,"data_type":"varchar","column_type":"varchar(255)","unsigned":false,"character_set":"utf8mb4"}"#,
        );
    let old_id = SchemaColumn::new("_system_old_value_0".to_owned(), DataType::Int64, true)
        .with_old_value_of("id".to_owned())
        .with_arrow_extension_metadata(
            "transferia.mysql.signed_integer",
            r#"{"version":1,"data_type":"bigint","column_type":"bigint","unsigned":false}"#,
        );
    let old_payload = SchemaColumn::new("_system_old_value_1".to_owned(), DataType::Utf8, true)
        .with_old_value_of("payload".to_owned())
        .with_arrow_extension_metadata(
            "transferia.mysql.text",
            r#"{"version":1,"data_type":"varchar","column_type":"varchar(255)","unsigned":false,"character_set":"utf8mb4"}"#,
        );
    let mut incoming = vec![id.clone(), payload.clone(), old_id, old_payload];
    incoming.extend(
        [
            (SYSTEM_ROLE_SOURCE_DATABASE, DataType::Utf8, false),
            (SYSTEM_ROLE_SOURCE_SCHEMA, DataType::Utf8, false),
            (SYSTEM_ROLE_SOURCE_TABLE, DataType::Utf8, false),
            (SYSTEM_ROLE_SOURCE_TRANSACTION_ID, DataType::Binary, false),
            (SYSTEM_ROLE_SOURCE_TIMESTAMP_MS, DataType::Int64, false),
            (SYSTEM_ROLE_SOURCE_TIMESTAMP_US, DataType::Int64, false),
            (SYSTEM_ROLE_SOURCE_TIMESTAMP_NS, DataType::Int64, false),
            (SYSTEM_ROLE_EVENT_TIMESTAMP_MS, DataType::Int64, false),
            (SYSTEM_ROLE_EVENT_TIMESTAMP_US, DataType::Int64, false),
            (SYSTEM_ROLE_EVENT_TIMESTAMP_NS, DataType::Int64, false),
            (SYSTEM_ROLE_SOURCE_SERVER_ID, DataType::Int64, false),
            (SYSTEM_ROLE_SOURCE_GTID, DataType::Utf8, true),
            (SYSTEM_ROLE_SOURCE_BINLOG_FILE, DataType::Utf8, false),
            (SYSTEM_ROLE_SOURCE_BINLOG_POSITION, DataType::Int64, false),
            (SYSTEM_ROLE_SOURCE_BINLOG_ROW, DataType::Int32, false),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (role, data_type, nullable))| {
            SchemaColumn::new(format!("_system_role_{index}"), data_type, nullable)
                .with_system_role(role)
        }),
    );
    incoming.extend(
        [
            SystemColumnKind::Offset,
            SystemColumnKind::ChangeOperation,
            SystemColumnKind::ChangedColumns,
        ]
        .into_iter()
        .map(|kind| SchemaColumn::new(kind.default_name().to_owned(), kind.data_type(), false)),
    );
    DeliveryDiscovery {
        source_name: "mysql".into(),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: "accounts".into(),
            incoming_schema: DatasetSchema::new(incoming),
            stored_schema: DatasetSchema::new(vec![id, payload]),
            system_columns: [
                SystemColumnKind::Offset,
                SystemColumnKind::ChangeOperation,
                SystemColumnKind::ChangedColumns,
            ]
            .into_iter()
            .map(Into::into)
            .collect(),
        }],
        performance_advice: Vec::new(),
    }
}

#[test]
fn json_schema_serializer_rejects_values_outside_contract() -> anyhow::Result<()> {
    let schema = compile_writer_schema(
        &crate::schema_registry::RegistrySchema {
            id: 11,
            definition: r#"{"type":"object","required":["id"],"properties":{"id":{"type":"integer"}},"additionalProperties":false}"#.to_owned(),
            format: crate::schema_registry::SchemaFormat::JsonSchema,
            references: Arc::from([]),
        },
        &[0],
    )?;
    assert!(encode_registered(&schema, &[0], br#"{"id":1}"#).is_ok());
    assert!(encode_registered(&schema, &[0], br#"{"id":"bad"}"#).is_err());
    Ok(())
}

#[test]
fn avro_and_protobuf_serializers_emit_confluent_envelopes() -> anyhow::Result<()> {
    let avro_definition = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"name","type":["null","string"],"default":null}]}"#;
    let avro = compile_writer_schema(
        &crate::schema_registry::RegistrySchema {
            id: 12,
            definition: avro_definition.to_owned(),
            format: crate::schema_registry::SchemaFormat::Avro,
            references: Arc::from([]),
        },
        &[0],
    )?;
    let encoded = encode_registered(&avro, &[0], br#"{"id":7,"name":null}"#)?;
    let envelope = crate::schema_registry::ConfluentEnvelope::decode(&encoded)?;
    assert_eq!(envelope.schema_id, 12);
    let schema = apache_avro::Schema::parse_str(avro_definition)?;
    let mut payload = envelope.payload;
    let value = apache_avro::from_avro_datum(&schema, &mut payload, None)?;
    let value: serde_json::Value = apache_avro::from_value(&value)?;
    assert_eq!(value["id"], 7);
    assert!(value["name"].is_null());

    let protobuf_definition = protobuf_definition()?;
    let protobuf = compile_writer_schema(
        &crate::schema_registry::RegistrySchema {
            id: 13,
            definition: protobuf_definition,
            format: crate::schema_registry::SchemaFormat::Protobuf,
            references: Arc::from([]),
        },
        &[0],
    )?;
    let encoded = encode_registered(
        &protobuf,
        &[0],
        br#"{"id":"8","name":"event","enabled":true}"#,
    )?;
    assert!(encode_registered(
        &protobuf,
        &[0],
        br#"{"id":"8","name":"event","enabled":true,"unknown":"must not disappear"}"#,
    )
    .is_err());
    let envelope = crate::schema_registry::ConfluentEnvelope::decode(&encoded)?;
    assert_eq!(envelope.schema_id, 13);
    let (indexes, payload) = crate::schema_registry::decode_message_indexes(envelope.payload)?;
    assert_eq!(indexes, [0]);
    assert!(!payload.is_empty());
    Ok(())
}

#[test]
fn debezium_registry_wrapper_supports_json_avro_and_protobuf_without_losing_tombstones(
) -> anyhow::Result<()> {
    let json = br#"{"id":8,"name":"event","enabled":true}"#;
    let definitions = [
        (
            crate::schema_registry::SchemaFormat::JsonSchema,
            r#"{"type":"object","required":["id","name","enabled"],"properties":{"id":{"type":"integer"},"name":{"type":"string"},"enabled":{"type":"boolean"}},"additionalProperties":false}"#.to_owned(),
        ),
        (
            crate::schema_registry::SchemaFormat::Avro,
            r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"name","type":"string"},{"name":"enabled","type":"boolean"}]}"#.to_owned(),
        ),
        (
            crate::schema_registry::SchemaFormat::Protobuf,
            protobuf_definition()?,
        ),
    ];

    for (ordinal, (format, definition)) in definitions.into_iter().enumerate() {
        let schema = compile_writer_schema(
            &crate::schema_registry::RegistrySchema {
                id: i32::try_from(20 + ordinal)?,
                definition,
                format,
                references: Arc::from([]),
            },
            &[0],
        )?;
        let batch = SerializedBatch {
            table: "accounts".into(),
            messages: vec![
                SerializedMessage {
                    key: Some(json.to_vec()),
                    value: Some(json.to_vec()),
                },
                SerializedMessage {
                    key: Some(json.to_vec()),
                    value: None,
                },
            ],
        };
        let encoded = encode_debezium_registered_batch(
            batch,
            Some(&schema),
            &[0],
            &schema,
            &[0],
            usize::MAX,
        )?;
        assert_eq!(encoded.messages.len(), 2);
        for field in [
            encoded.messages[0].key.as_deref(),
            encoded.messages[0].value.as_deref(),
            encoded.messages[1].key.as_deref(),
        ] {
            let envelope = crate::schema_registry::ConfluentEnvelope::decode(field.unwrap())?;
            assert_eq!(envelope.schema_id, i32::try_from(20 + ordinal)?);
            assert!(!envelope.payload.is_empty());
        }
        assert!(encoded.messages[1].value.is_none());
    }
    Ok(())
}

#[test]
fn mysql_debezium_envelope_survives_json_schema_registry_wrapper() -> anyhow::Result<()> {
    let definition = r#"{"type":"object","required":["after","source","op"],"properties":{"after":{"type":"object"},"source":{"type":"object","required":["connector","server_id","gtid","file","pos","row"],"properties":{"connector":{"const":"mysql"},"server_id":{"type":"integer"},"gtid":{"type":"string"},"file":{"type":"string"},"pos":{"type":"integer"},"row":{"type":"integer"}},"additionalProperties":true},"op":{"const":"c"}},"additionalProperties":true}"#;
    let schema = compile_writer_schema(
        &crate::schema_registry::RegistrySchema {
            id: 32,
            definition: definition.to_owned(),
            format: crate::schema_registry::SchemaFormat::JsonSchema,
            references: Arc::from([]),
        },
        &[0],
    )?;
    let value = br#"{"before":null,"after":{"u64":"AP//////////"},"source":{"version":"transferia","connector":"mysql","name":"inventory","ts_ms":1000,"snapshot":"false","db":"inventory","sequence":null,"ts_us":1000000,"ts_ns":1000000000,"table":"accounts","server_id":4294967295,"gtid":"11111111-1111-1111-1111-111111111111:blue:2","file":"mysql-bin.000001","pos":100,"row":0,"thread":null,"query":null},"op":"c","ts_ms":2000,"ts_us":2000000,"ts_ns":2000000000,"transaction":null}"#;
    let encoded = encode_debezium_registered_batch(
        SerializedBatch {
            table: "accounts".into(),
            messages: vec![SerializedMessage {
                key: None,
                value: Some(value.to_vec()),
            }],
        },
        None,
        &[0],
        &schema,
        &[0],
        usize::MAX,
    )?;
    let envelope = crate::schema_registry::ConfluentEnvelope::decode(
        encoded.messages[0]
            .value
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("registry wrapper dropped MySQL value"))?,
    )?;
    assert_eq!(envelope.schema_id, 32);
    let wrapped: serde_json::Value = serde_json::from_slice(envelope.payload)?;
    assert_eq!(wrapped["source"]["connector"], "mysql");
    assert_eq!(wrapped["source"]["server_id"], 4_294_967_295_u64);
    assert_eq!(wrapped["after"]["u64"], "AP//////////");
    Ok(())
}

#[test]
fn avro_debezium_encoding_preserves_non_finite_floats() -> anyhow::Result<()> {
    let definition =
        r#"{"type":"record","name":"Floating","fields":[{"name":"value","type":"double"}]}"#;
    let schema = compile_writer_schema(
        &crate::schema_registry::RegistrySchema {
            id: 31,
            definition: definition.to_owned(),
            format: crate::schema_registry::SchemaFormat::Avro,
            references: Arc::from([]),
        },
        &[0],
    )?;

    for value in ["NaN", "Infinity", "-Infinity"] {
        let json = serde_json::to_vec(&serde_json::json!({"value": value}))?;
        let encoded = encode_registered(&schema, &[0], &json)?;
        let envelope = crate::schema_registry::ConfluentEnvelope::decode(&encoded)?;
        let parsed = apache_avro::Schema::parse_str(definition)?;
        let mut payload = envelope.payload;
        let decoded = apache_avro::from_avro_datum(&parsed, &mut payload, None)?;
        let apache_avro::types::Value::Record(fields) = decoded else {
            anyhow::bail!("expected Avro record")
        };
        let apache_avro::types::Value::Double(decoded) = fields[0].1 else {
            anyhow::bail!("expected Avro double")
        };
        match value {
            "NaN" => assert!(decoded.is_nan()),
            "Infinity" => assert!(decoded.is_infinite() && decoded.is_sign_positive()),
            "-Infinity" => assert!(decoded.is_infinite() && decoded.is_sign_negative()),
            _ => panic!("test fixture contains an unknown non-finite value"),
        }
    }
    Ok(())
}

fn registry_connection() -> crate::schema_registry::SchemaRegistryConnection {
    crate::schema_registry::SchemaRegistryConnection {
        url: "http://registry".to_owned(),
        request_timeout_ms: 1_000,
        auth: crate::schema_registry::SchemaRegistryAuth::None,
        ca_certificate: None,
    }
}

fn protobuf_definition() -> anyhow::Result<String> {
    use prost_reflect::prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto,
    };

    let fields = [
        ("id", Type::Int64),
        ("name", Type::String),
        ("enabled", Type::Bool),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (name, r#type))| {
        Ok(FieldDescriptorProto {
            name: Some(name.to_owned()),
            number: Some(i32::try_from(index + 1)?),
            label: Some(Label::Optional as i32),
            r#type: Some(r#type as i32),
            json_name: Some(name.to_owned()),
            ..Default::default()
        })
    })
    .collect::<anyhow::Result<Vec<_>>>()?;
    let file = FileDescriptorProto {
        name: Some("event.proto".to_owned()),
        package: Some("demo".to_owned()),
        syntax: Some("proto3".to_owned()),
        message_type: vec![DescriptorProto {
            name: Some("Event".to_owned()),
            field: fields,
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut encoded = Vec::new();
    file.encode(&mut encoded)?;
    Ok(base64::engine::general_purpose::STANDARD.encode(encoded))
}
