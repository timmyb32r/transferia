use super::config::{YdbAuth, YdbConnectionConfig, YdbSinkConfig, YdbSourceConfig, YdbTableConfig};
use super::sink::{
    cleanup_ydb_speedtest_scope, cleanup_ydb_speedtest_table, create_speedtest_table_request,
    create_table_query, encode_arrow_batch, encode_delete, encode_update,
    is_ydb_speedtest_table_name, isolate_ydb_discovery, isolated_ydb_config, physical_target_set,
    prepare_ydb_speedtest_table, validate_speedtest_isolation_id, validate_ydb_cleanup_scope,
    verify_ydb_speedtest_description, ydb_speedtest_table, YdbSinkConnector, YdbSpeedtestScope,
    YdbSpeedtestTableClient,
};
use super::source::{close_ydb_session, YdbSessionClient};
use super::types::{column_plans, dataset_schema, result_set_to_batch, ColumnKind};
use arrow::array::{Array as _, Decimal128Array, FixedSizeBinaryArray, StringArray, UInt64Array};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamDecoder;
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_registry::{SinkConnector as _, SinkSpeedtestIsolation};
use ydb_grpc::ydb_proto::r#type::{PrimitiveTypeId, Type as TypeKind};
use ydb_grpc::ydb_proto::table::{ColumnMeta, CreateTableRequest, DescribeTableResult};

#[derive(Default)]
struct RecordingSessionClient {
    deleted: Vec<String>,
    outcomes: VecDeque<SessionDeleteOutcome>,
    persistent_failure: bool,
}

enum SessionDeleteOutcome {
    Success,
    RetryableFailure,
    AlreadyAbsent,
}

impl YdbSessionClient for RecordingSessionClient {
    fn delete_session(&mut self, session_id: String) -> BoxFuture<'_, anyhow::Result<()>> {
        self.deleted.push(session_id);
        let result = match self
            .outcomes
            .pop_front()
            .unwrap_or(if self.persistent_failure {
                SessionDeleteOutcome::RetryableFailure
            } else {
                SessionDeleteOutcome::Success
            }) {
            SessionDeleteOutcome::Success => Ok(()),
            SessionDeleteOutcome::RetryableFailure => {
                Err(anyhow::anyhow!("retryable session deletion failure"))
            }
            SessionDeleteOutcome::AlreadyAbsent => {
                Err(anyhow::anyhow!("session is already absent"))
            }
        };
        Box::pin(async move { result })
    }

    fn is_session_absent(&self, error: &anyhow::Error) -> bool {
        error.to_string() == "session is already absent"
    }
}

#[tokio::test]
async fn source_session_cleanup_deletes_each_session_exactly_once() {
    let mut client = RecordingSessionClient::default();
    let mut session_id = Some("session-1".to_owned());

    close_ydb_session(
        &mut client,
        &mut session_id,
        Duration::from_secs(1),
        Duration::from_millis(1),
    )
    .await
    .unwrap();
    close_ydb_session(
        &mut client,
        &mut session_id,
        Duration::from_secs(1),
        Duration::from_millis(1),
    )
    .await
    .unwrap();

    assert_eq!(client.deleted, ["session-1"]);
    assert!(session_id.is_none());
}

#[tokio::test(start_paused = true)]
async fn source_session_cleanup_retries_after_an_unconfirmed_failure() {
    let mut client = RecordingSessionClient {
        deleted: Vec::new(),
        outcomes: VecDeque::from([
            SessionDeleteOutcome::RetryableFailure,
            SessionDeleteOutcome::Success,
        ]),
        persistent_failure: false,
    };
    let mut session_id = Some("session-1".to_owned());

    close_ydb_session(
        &mut client,
        &mut session_id,
        Duration::from_secs(1),
        Duration::from_millis(10),
    )
    .await
    .unwrap();

    assert_eq!(client.deleted, ["session-1", "session-1"]);
    assert!(session_id.is_none());
}

#[tokio::test]
async fn source_session_cleanup_treats_already_absent_as_idempotent_success() {
    let mut client = RecordingSessionClient {
        deleted: Vec::new(),
        outcomes: VecDeque::from([SessionDeleteOutcome::AlreadyAbsent]),
        persistent_failure: false,
    };
    let mut session_id = Some("session-1".to_owned());

    close_ydb_session(
        &mut client,
        &mut session_id,
        Duration::from_secs(1),
        Duration::from_millis(1),
    )
    .await
    .unwrap();

    assert_eq!(client.deleted, ["session-1"]);
    assert!(session_id.is_none());
}

#[tokio::test(start_paused = true)]
async fn source_session_cleanup_exhaustion_is_bounded_sanitized_and_retryable() {
    let mut client = RecordingSessionClient {
        deleted: Vec::new(),
        outcomes: VecDeque::new(),
        persistent_failure: true,
    };
    let mut session_id = Some("session-1".to_owned());

    let error = close_ydb_session(
        &mut client,
        &mut session_id,
        Duration::from_millis(100),
        Duration::from_millis(10),
    )
    .await
    .expect_err("persistent failure must exhaust the configured deadline");

    assert!(error.to_string().contains("configured 100 ms"));
    assert!(!error
        .to_string()
        .contains("retryable session deletion failure"));
    assert_eq!(session_id.as_deref(), Some("session-1"));
    assert!(client.deleted.len() >= 4);
    let exhausted_attempts = client.deleted.len();

    client.persistent_failure = false;
    close_ydb_session(
        &mut client,
        &mut session_id,
        Duration::from_millis(100),
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    assert!(session_id.is_none());
    assert_eq!(client.deleted.len(), exhausted_attempts + 1);
}

#[test]
fn source_shutdown_retry_controls_are_visible_defaulted_and_validated() {
    let schema = serde_json::to_value(schemars::schema_for!(YdbSourceConfig)).unwrap();
    let properties = &schema["properties"];
    assert_eq!(properties["session_shutdown_timeout_ms"]["default"], 60_000);
    assert_eq!(
        properties["session_shutdown_timeout_ms"]["x-ui"]["section"],
        "advanced"
    );
    assert_eq!(
        properties["session_shutdown_retry_initial_ms"]["default"],
        50
    );
    assert_eq!(
        properties["session_shutdown_retry_initial_ms"]["x-ui"]["section"],
        "advanced"
    );

    let base = r"
endpoint: grpc://localhost:2136
database: /local
trusted_plaintext: true
auth:
  type: anonymous
tables:
  - path: /local/events
batch_rows: 1024
";
    let defaults: YdbSourceConfig = serde_yaml::from_str(base).unwrap();
    assert_eq!(defaults.session_shutdown_timeout_ms, 60_000);
    assert_eq!(defaults.session_shutdown_retry_initial_ms, 50);
    defaults.validate().unwrap();

    let mut zero_timeout = defaults.clone();
    zero_timeout.session_shutdown_timeout_ms = 0;
    assert!(zero_timeout.validate().is_err());
    let mut zero_backoff = defaults;
    zero_backoff.session_shutdown_retry_initial_ms = 0;
    assert!(zero_backoff.validate().is_err());

    let invalid: YdbSourceConfig = serde_yaml::from_str(&format!(
        "{base}session_shutdown_timeout_ms: 10\nsession_shutdown_retry_initial_ms: 11\n"
    ))
    .unwrap();
    assert!(invalid.validate().is_err());
}

#[test]
fn sink_schema_exposes_only_create_tables_tuning() {
    let schema = serde_json::to_value(schemars::schema_for!(YdbSinkConfig)).unwrap();
    let properties = &schema["properties"];
    let table_schema = serde_json::to_value(schemars::schema_for!(YdbTableConfig)).unwrap();
    let table_properties = &table_schema["properties"];
    assert!(table_properties.get("name").is_none());
    assert!(table_properties.get("path").is_some());
    assert_eq!(properties["auth"]["x-ui"]["control_width"], "auth");
    assert_eq!(properties["create_tables"]["default"], true);
    assert!(properties["create_tables"].get("x-ui").is_none());
    assert_eq!(properties["request_timeout_ms"]["x-ui"]["widget"], "hidden");
    assert_eq!(properties["retry_max_ms"]["x-ui"]["widget"], "hidden");
}

#[test]
fn delete_encoding_uses_only_typed_primary_key_rows() -> anyhow::Result<()> {
    let columns = vec![
        SchemaColumn::new("tenant".into(), DataType::Utf8, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("id".into(), DataType::UInt64, false).with_constraints(true, false, None),
    ];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("id", DataType::UInt64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["alpha", "beta"])),
            Arc::new(UInt64Array::from(vec![7, 8])),
        ],
    )?;

    let (query, parameters) = encode_delete("/local/events", &batch, &columns)?;
    assert!(query.contains("DECLARE $batch AS List<Struct<`tenant`:Utf8, `id`:Uint64>>"));
    assert!(query.contains("DELETE FROM `/local/events` ON SELECT `tenant`, `id`"));
    let parameter = parameters.get("$batch").expect("delete batch parameter");
    assert_eq!(parameter.value.as_ref().expect("list value").items.len(), 2);
    Ok(())
}

#[test]
fn update_encoding_checks_every_primary_key_before_commit() -> anyhow::Result<()> {
    let primary_key =
        SchemaColumn::new("id".into(), DataType::UInt64, false).with_constraints(true, false, None);
    let value = SchemaColumn::new("value".into(), DataType::Utf8, true);
    let columns = vec![primary_key.clone(), value];
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("value", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(UInt64Array::from(vec![7, 8])),
            Arc::new(StringArray::from(vec![Some("new"), None])),
        ],
    )?;

    let (query, parameters) = encode_update("/local/events", &batch, &columns, &[primary_key])?;
    assert!(query.contains("SELECT COUNT(*) AS matched"));
    assert!(query.contains("target.`id` = staged.`id`"));
    assert!(query.contains("UPDATE `/local/events` ON SELECT `id`, `value`"));
    assert_eq!(
        parameters
            .get("$batch")
            .expect("update batch parameter")
            .value
            .as_ref()
            .expect("list value")
            .items
            .len(),
        2
    );
    Ok(())
}
use ydb_grpc::ydb_proto::{
    result_set, value, Column, DecimalType, ListType, OptionalType, ResultSet, Type, Value,
};

#[test]
fn plaintext_endpoint_requires_explicit_trust() {
    let config = YdbConnectionConfig {
        endpoint: "grpc://localhost:2136".to_owned(),
        database: "/local".to_owned(),
        trusted_plaintext: false,
        auth: YdbAuth::Anonymous,
        request_timeout_ms: 30_000,
        max_rpc_message_bytes: 256 * 1024 * 1024,
    };
    assert!(config.validate().is_err());
}

#[test]
fn discovery_preserves_optional_decimal_and_uuid_types() -> anyhow::Result<()> {
    let columns = column_plans(
        vec![
            column("key", primitive(PrimitiveTypeId::Uint64), None),
            column(
                "amount",
                optional(Type {
                    r#type: Some(TypeKind::DecimalType(DecimalType {
                        precision: 22,
                        scale: 7,
                    })),
                }),
                None,
            ),
            column("event_id", primitive(PrimitiveTypeId::Uuid), Some(true)),
        ],
        &["key".to_owned()],
    )?;
    assert_eq!(columns[0].kind, ColumnKind::UInt64);
    assert!(!columns[0].nullable);
    assert!(columns[0].primary_key);
    assert_eq!(
        columns[1].kind,
        ColumnKind::Decimal {
            precision: 22,
            scale: 7
        }
    );
    assert!(columns[1].nullable);
    let schema = dataset_schema(&columns);
    assert_eq!(schema.columns[1].data_type, DataType::Decimal128(22, 7));
    assert_eq!(schema.columns[2].arrow_extension_name, Some("arrow.uuid"));
    Ok(())
}

#[test]
fn source_decodes_values_nulls_decimal_and_uuid_losslessly() -> anyhow::Result<()> {
    let columns = column_plans(
        vec![
            column("name", primitive(PrimitiveTypeId::Utf8), Some(true)),
            column(
                "amount",
                optional(Type {
                    r#type: Some(TypeKind::DecimalType(DecimalType {
                        precision: 22,
                        scale: 7,
                    })),
                }),
                None,
            ),
            column("event_id", primitive(PrimitiveTypeId::Uuid), Some(true)),
        ],
        &[],
    )?;
    let uuid = uuid::Uuid::parse_str("12345678-1234-4abc-89ab-1234567890ab")?;
    let little_endian = uuid.to_bytes_le();
    let low = u64::from_le_bytes(little_endian[..8].try_into()?);
    let high = u64::from_le_bytes(little_endian[8..].try_into()?);
    let decimal = -12_345_678_901_i128;
    let decimal_bits = decimal as u128;
    let result = ResultSet {
        columns: vec![
            result_column("name", primitive(PrimitiveTypeId::Utf8)),
            result_column(
                "amount",
                optional(Type {
                    r#type: Some(TypeKind::DecimalType(DecimalType {
                        precision: 22,
                        scale: 7,
                    })),
                }),
            ),
            result_column("event_id", primitive(PrimitiveTypeId::Uuid)),
        ],
        rows: vec![
            Value {
                items: vec![
                    scalar(value::Value::TextValue("alpha".to_owned())),
                    high_low(decimal_bits as u64, (decimal_bits >> 64) as u64),
                    high_low(low, high),
                ],
                ..Value::default()
            },
            Value {
                items: vec![
                    scalar(value::Value::TextValue("beta".to_owned())),
                    scalar(value::Value::NullFlagValue(0)),
                    high_low(low, high),
                ],
                ..Value::default()
            },
        ],
        truncated: false,
        format: result_set::Format::Value as i32,
        arrow_format_meta: None,
        data: Vec::new(),
    };
    let batch = result_set_to_batch(&result, &columns)?;
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "alpha"
    );
    let amounts = batch
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amounts.value(0), decimal);
    assert!(amounts.is_null(1));
    let ids = batch
        .column(2)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(ids.value(0), uuid.as_bytes());
    Ok(())
}

#[test]
fn unsupported_complex_type_fails_before_reading() {
    let complex = Type {
        r#type: Some(TypeKind::ListType(Box::new(ListType {
            item: Some(Box::new(primitive(PrimitiveTypeId::Utf8))),
        }))),
    };
    let error = column_plans(vec![column("items", complex, None)], &[]).unwrap_err();
    assert!(error.to_string().contains("unsupported YDB column type"));
}

#[test]
fn schema_drift_is_rejected_before_emitting_rows() -> anyhow::Result<()> {
    let columns = column_plans(
        vec![column("value", primitive(PrimitiveTypeId::Utf8), None)],
        &[],
    )?;
    let result = ResultSet {
        columns: vec![result_column("value", primitive(PrimitiveTypeId::String))],
        rows: Vec::new(),
        truncated: false,
        format: result_set::Format::Value as i32,
        arrow_format_meta: None,
        data: Vec::new(),
    };
    assert!(result_set_to_batch(&result, &columns).is_err());
    Ok(())
}

#[test]
fn streamed_result_chunk_may_be_marked_truncated_without_losing_rows() -> anyhow::Result<()> {
    let columns = column_plans(
        vec![column("value", primitive(PrimitiveTypeId::Utf8), None)],
        &[],
    )?;
    let result = ResultSet {
        columns: vec![result_column("value", primitive(PrimitiveTypeId::Utf8))],
        rows: vec![Value {
            items: vec![scalar(value::Value::TextValue("kept".to_owned()))],
            ..Value::default()
        }],
        truncated: true,
        format: result_set::Format::Value as i32,
        arrow_format_meta: None,
        data: Vec::new(),
    };
    let batch = result_set_to_batch(&result, &columns)?;
    assert_eq!(batch.num_rows(), 1);
    Ok(())
}

#[test]
fn sink_requires_exact_table_mappings_and_primary_key() -> anyhow::Result<()> {
    let config = YdbSinkConfig {
        connection: YdbConnectionConfig {
            endpoint: "grpc://localhost:2136".to_owned(),
            database: "/local".to_owned(),
            trusted_plaintext: true,
            auth: YdbAuth::Anonymous,
            request_timeout_ms: 30_000,
            max_rpc_message_bytes: 256 * 1024 * 1024,
        },
        tables: vec![YdbTableConfig {
            path: "/local/events".to_owned(),
        }],
        create_tables: true,
        retry_max_ms: 30_000,
    };
    config.validate()?;
    assert_eq!(config.table_path("events")?, "/local/events");
    assert!(config.table_path("missing").is_err());
    Ok(())
}

fn speedtest_discovery() -> DeliveryDiscovery {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".to_owned(), DataType::UInt64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, false),
    ]);
    DeliveryDiscovery {
        source_name: Arc::from("source"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![
            DiscoveredDataset {
                role: DatasetRole::Main,
                name: Arc::from("events"),
                incoming_schema: schema.clone(),
                stored_schema: schema.clone(),
                system_columns: Vec::new(),
            },
            DiscoveredDataset {
                role: DatasetRole::DeadLetterQueue,
                name: Arc::from("events_dlq"),
                incoming_schema: schema.clone(),
                stored_schema: schema,
                system_columns: Vec::new(),
            },
        ],
        performance_advice: Vec::new(),
    }
}

fn speedtest_sink_config(create_tables: bool) -> YdbSinkConfig {
    YdbSinkConfig {
        connection: YdbConnectionConfig {
            endpoint: "grpc://localhost:2136".to_owned(),
            database: "/local".to_owned(),
            trusted_plaintext: true,
            auth: YdbAuth::Anonymous,
            request_timeout_ms: 30_000,
            max_rpc_message_bytes: 256 * 1024 * 1024,
        },
        tables: vec![
            YdbTableConfig {
                path: "/local/primary/events".to_owned(),
            },
            YdbTableConfig {
                path: "/local/dead-letter/events_dlq".to_owned(),
            },
        ],
        create_tables,
        retry_max_ms: 30_000,
    }
}

fn assert_same_schema(left: &DatasetSchema, right: &DatasetSchema) {
    assert_eq!(left.columns.len(), right.columns.len());
    for (left, right) in left.columns.iter().zip(&right.columns) {
        assert_eq!(left.name, right.name);
        assert_eq!(left.data_type, right.data_type);
        assert_eq!(left.nullable, right.nullable);
        assert_eq!(left.primary_key, right.primary_key);
        assert_eq!(left.low_cardinality, right.low_cardinality);
        assert_eq!(left.max_length, right.max_length);
        assert_eq!(left.arrow_extension_name, right.arrow_extension_name);
        assert_eq!(left.system_role, right.system_role);
        assert_eq!(left.old_value_of, right.old_value_of);
        assert_eq!(left.old_key_of, right.old_key_of);
    }
}

#[tokio::test]
async fn speedtest_isolation_uses_same_parents_and_preserves_dataset_semantics(
) -> anyhow::Result<()> {
    let production_config = speedtest_sink_config(false);
    let connector = Arc::new(YdbSinkConnector::from_config(production_config.clone())?);
    let original = Arc::new(speedtest_discovery());
    let isolation = Arc::clone(&connector)
        .isolate_speedtest(
            Arc::clone(&original),
            "0123456789abcdef0123456789abcdef".to_owned(),
        )
        .await?;

    assert!(!production_config.create_tables);
    assert_eq!(isolation.discovery.datasets.len(), original.datasets.len());
    for (index, dataset) in isolation.discovery.datasets.iter().enumerate() {
        assert_eq!(dataset.role, original.datasets[index].role);
        assert_same_schema(
            &dataset.incoming_schema,
            &original.datasets[index].incoming_schema,
        );
        assert_same_schema(
            &dataset.stored_schema,
            &original.datasets[index].stored_schema,
        );
        assert_eq!(
            dataset.system_columns,
            original.datasets[index].system_columns
        );
        assert!(is_ydb_speedtest_table_name(&dataset.name));
    }
    assert_eq!(
        isolation.table_name("events")?.as_ref(),
        "_transferia_st_0123456789abcdef0123456789abcdef_0"
    );
    assert_eq!(
        isolation.table_name("events_dlq")?.as_ref(),
        "_transferia_st_0123456789abcdef0123456789abcdef_1"
    );
    let scratch_targets = isolation
        .physical_targets()
        .iter()
        .map(|target| serde_json::from_str::<(String, String)>(&target.scratch))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(
        scratch_targets[0].1.rsplit_once('/').unwrap().0,
        "/local/primary"
    );
    assert_eq!(
        scratch_targets[1].1.rsplit_once('/').unwrap().0,
        "/local/dead-letter"
    );
    isolation.connector().cleanup_speedtest(&isolation).await?;
    assert!(connector.cleanup_speedtest(&isolation).await.is_err());
    Ok(())
}

#[test]
fn speedtest_clone_forces_create_only_on_the_isolated_config() -> anyhow::Result<()> {
    let production = speedtest_sink_config(false);
    let original = speedtest_discovery();
    let (_, _, tables, _) =
        isolate_ydb_discovery(&production, &original, "0123456789abcdef0123456789abcdef")?;
    let isolated = isolated_ydb_config(&production, &tables)?;

    assert!(!production.create_tables);
    assert!(isolated.create_tables);
    assert_eq!(isolated.tables.len(), production.tables.len());
    assert!(isolated
        .tables
        .iter()
        .all(|table| is_ydb_speedtest_table_name(table.name())));
    Ok(())
}

#[test]
fn speedtest_names_reject_noncanonical_ids_and_preserve_exact_parent() -> anyhow::Result<()> {
    let id = "0123456789abcdef0123456789abcdef";
    let (name, path) = ydb_speedtest_table("/odd`parent/events", id, usize::MAX)?;
    assert!(is_ydb_speedtest_table_name(&name));
    assert_eq!(path.rsplit_once('/').unwrap().0, "/odd`parent");
    assert!(name.len() <= 255);
    let schema = DatasetSchema::new(vec![SchemaColumn::new(
        "id".to_owned(),
        DataType::UInt64,
        false,
    )
    .with_constraints(true, false, None)]);
    let request = create_speedtest_table_request(&path, &schema, "owner-token")?;
    assert_eq!(request.path, path);
    assert_eq!(
        request.attributes.get("transferia.speedtest.owner"),
        Some(&"owner-token".to_owned())
    );
    let description = ydb_grpc::ydb_proto::table::DescribeTableResult {
        columns: request.columns.clone(),
        primary_key: request.primary_key.clone(),
        attributes: request.attributes,
        ..Default::default()
    };
    verify_ydb_speedtest_description(&path, &schema, "owner-token", &description)?;
    assert!(verify_ydb_speedtest_description(&path, &schema, "foreign", &description).is_err());
    assert!(create_speedtest_table_request("/local/events", &schema, "owner").is_err());
    assert!(validate_speedtest_isolation_id("0123456789ABCDEF0123456789ABCDEF").is_err());
    assert!(validate_speedtest_isolation_id("0123; DROP TABLE events").is_err());
    assert!(ydb_speedtest_table("relative/events", id, 0).is_err());
    Ok(())
}

#[test]
fn cleanup_refuses_tampered_targets_and_prevalidates_every_drop() -> anyhow::Result<()> {
    let production = speedtest_sink_config(false);
    let original = speedtest_discovery();
    let (isolated_discovery, table_names, tables, targets) =
        isolate_ydb_discovery(&production, &original, "0123456789abcdef0123456789abcdef")?;
    let isolated_config = isolated_ydb_config(&production, &tables)?;
    let connector: Arc<dyn transferia_registry::SinkConnector> =
        Arc::new(YdbSinkConnector::from_config(isolated_config.clone())?);
    let isolation = SinkSpeedtestIsolation::scratch(
        connector,
        &original,
        isolated_discovery,
        table_names,
        targets.clone(),
    )?;
    let schemas = tables
        .values()
        .cloned()
        .map(|path| {
            (
                path,
                DatasetSchema::new(vec![SchemaColumn::new(
                    "id".to_owned(),
                    DataType::UInt64,
                    false,
                )
                .with_constraints(true, false, None)]),
            )
        })
        .collect();
    let scope = YdbSpeedtestScope {
        tables: tables.clone(),
        schemas,
        owner: Arc::from("owner"),
        physical_targets: physical_target_set(&targets),
        attempted: Mutex::new(tables.values().cloned().collect()),
    };
    validate_ydb_cleanup_scope(&isolated_config, &isolation, &scope)?;

    let mut wrong_targets = scope.physical_targets.clone();
    let first = wrong_targets.pop_first().unwrap();
    wrong_targets.insert((first.0, Arc::from("tampered")));
    let wrong_scope = YdbSpeedtestScope {
        tables: tables.clone(),
        schemas: scope.schemas.clone(),
        owner: Arc::clone(&scope.owner),
        physical_targets: wrong_targets,
        attempted: Mutex::new(tables.values().cloned().collect()),
    };
    assert!(validate_ydb_cleanup_scope(&isolated_config, &isolation, &wrong_scope).is_err());

    let mut unsafe_tables = tables;
    let key = unsafe_tables.keys().next().unwrap().clone();
    unsafe_tables.insert(key, Arc::from("/local/production/events"));
    let unsafe_scope = YdbSpeedtestScope {
        attempted: Mutex::new(unsafe_tables.values().cloned().collect()),
        tables: unsafe_tables,
        schemas: scope.schemas,
        owner: scope.owner,
        physical_targets: scope.physical_targets,
    };
    assert!(validate_ydb_cleanup_scope(&isolated_config, &isolation, &unsafe_scope).is_err());
    Ok(())
}

#[derive(Clone, Copy)]
enum FakeCreateOutcome {
    Success,
    LostResponse,
    Collision,
}

struct FakeSpeedtestYdb {
    tables: BTreeMap<String, DescribeTableResult>,
    creates: VecDeque<FakeCreateOutcome>,
    describe_failures: BTreeSet<String>,
    lost_drop_responses: BTreeSet<String>,
    drops: Vec<String>,
}

impl FakeSpeedtestYdb {
    fn new(outcomes: impl IntoIterator<Item = FakeCreateOutcome>) -> Self {
        Self {
            tables: BTreeMap::new(),
            creates: outcomes.into_iter().collect(),
            describe_failures: BTreeSet::new(),
            lost_drop_responses: BTreeSet::new(),
            drops: Vec::new(),
        }
    }

    fn description(request: &CreateTableRequest) -> DescribeTableResult {
        DescribeTableResult {
            columns: request.columns.clone(),
            primary_key: request.primary_key.clone(),
            attributes: request.attributes.clone(),
            ..Default::default()
        }
    }
}

impl YdbSpeedtestTableClient for FakeSpeedtestYdb {
    fn create_owned(
        &mut self,
        request: CreateTableRequest,
    ) -> futures_util::future::BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            match self
                .creates
                .pop_front()
                .unwrap_or(FakeCreateOutcome::Success)
            {
                FakeCreateOutcome::Success => {
                    self.tables
                        .insert(request.path.clone(), Self::description(&request));
                    Ok(())
                }
                FakeCreateOutcome::LostResponse => {
                    self.tables
                        .insert(request.path.clone(), Self::description(&request));
                    anyhow::bail!("create response lost")
                }
                FakeCreateOutcome::Collision => anyhow::bail!("table already exists"),
            }
        })
    }

    fn describe_owned<'a>(
        &'a mut self,
        path: &'a str,
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<DescribeTableResult>> {
        Box::pin(async move {
            if self.describe_failures.contains(path) {
                anyhow::bail!("describe timed out")
            }
            self.tables
                .get(path)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("not found"))
        })
    }

    fn drop_owned<'a>(
        &'a mut self,
        path: &'a str,
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            self.drops.push(path.to_owned());
            self.tables.remove(path);
            if self.lost_drop_responses.contains(path) {
                anyhow::bail!("drop response lost")
            }
            Ok(())
        })
    }

    fn is_not_found(&self, error: &anyhow::Error) -> bool {
        error.to_string() == "not found"
    }
}

fn speedtest_table_schema() -> DatasetSchema {
    DatasetSchema::new(vec![
        SchemaColumn::new("id".to_owned(), DataType::UInt64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, true),
    ])
}

fn fake_owned_description(path: &str, owner: &str) -> DescribeTableResult {
    let request = create_speedtest_table_request(path, &speedtest_table_schema(), owner).unwrap();
    FakeSpeedtestYdb::description(&request)
}

#[tokio::test]
async fn lost_create_response_is_accepted_only_after_owner_and_schema_proof() -> anyhow::Result<()>
{
    let path = "/db/_transferia_st_0123456789abcdef0123456789abcdef_0";
    let schema = speedtest_table_schema();
    let mut client = FakeSpeedtestYdb::new([FakeCreateOutcome::LostResponse]);
    prepare_ydb_speedtest_table(&mut client, path, &schema, "owner").await?;
    cleanup_ydb_speedtest_table(&mut client, path, &schema, "owner").await?;
    assert_eq!(client.drops, [path]);
    Ok(())
}

#[tokio::test]
async fn foreign_collision_is_never_accepted_or_dropped() -> anyhow::Result<()> {
    let path = "/db/_transferia_st_0123456789abcdef0123456789abcdef_0";
    let schema = speedtest_table_schema();
    let mut client = FakeSpeedtestYdb::new([FakeCreateOutcome::Collision]);
    client
        .tables
        .insert(path.to_owned(), fake_owned_description(path, "foreign"));
    assert!(
        prepare_ydb_speedtest_table(&mut client, path, &schema, "owner")
            .await
            .is_err()
    );
    assert!(
        cleanup_ydb_speedtest_table(&mut client, path, &schema, "owner")
            .await
            .is_err()
    );
    assert!(client.drops.is_empty());
    assert!(client.tables.contains_key(path));
    Ok(())
}

#[tokio::test]
async fn unreadable_owner_never_permits_drop() -> anyhow::Result<()> {
    let path = "/db/_transferia_st_0123456789abcdef0123456789abcdef_0";
    let schema = speedtest_table_schema();
    let mut client = FakeSpeedtestYdb::new([]);
    client
        .tables
        .insert(path.to_owned(), fake_owned_description(path, "owner"));
    client.describe_failures.insert(path.to_owned());
    assert!(
        cleanup_ydb_speedtest_table(&mut client, path, &schema, "owner")
            .await
            .is_err()
    );
    assert!(client.drops.is_empty());
    Ok(())
}

#[tokio::test]
async fn replaced_marker_survives_cleanup() -> anyhow::Result<()> {
    let path = "/db/_transferia_st_0123456789abcdef0123456789abcdef_0";
    let schema = speedtest_table_schema();
    let mut client = FakeSpeedtestYdb::new([FakeCreateOutcome::Success]);
    prepare_ydb_speedtest_table(&mut client, path, &schema, "owner").await?;
    client
        .tables
        .insert(path.to_owned(), fake_owned_description(path, "replacement"));
    assert!(
        cleanup_ydb_speedtest_table(&mut client, path, &schema, "owner")
            .await
            .is_err()
    );
    assert!(client.drops.is_empty());
    assert!(client.tables.contains_key(path));
    Ok(())
}

#[tokio::test]
async fn lost_drop_response_is_success_only_after_not_found_proof() -> anyhow::Result<()> {
    let path = "/db/_transferia_st_0123456789abcdef0123456789abcdef_0";
    let schema = speedtest_table_schema();
    let mut client = FakeSpeedtestYdb::new([FakeCreateOutcome::Success]);
    prepare_ydb_speedtest_table(&mut client, path, &schema, "owner").await?;
    client.lost_drop_responses.insert(path.to_owned());
    cleanup_ydb_speedtest_table(&mut client, path, &schema, "owner").await?;
    cleanup_ydb_speedtest_table(&mut client, path, &schema, "owner").await?;
    assert_eq!(client.drops, [path]);
    assert!(!client.tables.contains_key(path));
    Ok(())
}

#[tokio::test]
async fn partial_prepare_cleanup_attempts_all_and_drops_only_proven_owned_tables(
) -> anyhow::Result<()> {
    let first = "/db/_transferia_st_0123456789abcdef0123456789abcdef_0";
    let second = "/db/_transferia_st_0123456789abcdef0123456789abcdef_1";
    let schema = speedtest_table_schema();
    let mut client =
        FakeSpeedtestYdb::new([FakeCreateOutcome::Success, FakeCreateOutcome::Collision]);
    prepare_ydb_speedtest_table(&mut client, first, &schema, "owner").await?;
    client
        .tables
        .insert(second.to_owned(), fake_owned_description(second, "foreign"));
    assert!(
        prepare_ydb_speedtest_table(&mut client, second, &schema, "owner")
            .await
            .is_err()
    );
    let scope = YdbSpeedtestScope {
        tables: BTreeMap::from([
            (Arc::from("first"), Arc::from(first)),
            (Arc::from("second"), Arc::from(second)),
        ]),
        schemas: BTreeMap::from([
            (Arc::from(first), schema.clone()),
            (Arc::from(second), schema),
        ]),
        owner: Arc::from("owner"),
        physical_targets: BTreeSet::new(),
        attempted: Mutex::new(BTreeSet::from([Arc::from(first), Arc::from(second)])),
    };
    let error = cleanup_ydb_speedtest_scope(&mut client, &scope)
        .await
        .unwrap_err();
    assert!(error.to_string().contains(second));
    assert!(cleanup_ydb_speedtest_scope(&mut client, &scope)
        .await
        .is_err());
    assert_eq!(client.drops, [first]);
    assert!(!client.tables.contains_key(first));
    assert!(client.tables.contains_key(second));
    Ok(())
}

#[test]
fn sink_arrow_payload_round_trips_without_semantic_metadata() -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["one", "two"])),
        ],
    )?;
    let (schema, data) = encode_arrow_batch(&batch)?;
    let mut decoder = StreamDecoder::new();
    let mut schema = Buffer::from_vec(schema);
    assert!(decoder.decode(&mut schema)?.is_none());
    let mut data = Buffer::from_vec(data);
    let decoded = decoder.decode(&mut data)?.expect("record batch");
    assert_eq!(decoded, batch);
    Ok(())
}

#[test]
fn sink_create_table_query_preserves_composite_primary_key() -> anyhow::Result<()> {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("partition".to_owned(), DataType::UInt64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("offset".to_owned(), DataType::UInt64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, true),
    ]);
    let query = create_table_query("/local/events", &schema)?;
    assert!(query.contains("CREATE TABLE IF NOT EXISTS `/local/events`"));
    assert!(query.contains("PRIMARY KEY (`partition`, `offset`)"));
    assert!(query.contains("`payload` Utf8"));
    Ok(())
}

fn primitive(id: PrimitiveTypeId) -> Type {
    Type {
        r#type: Some(TypeKind::TypeId(id as i32)),
    }
}

fn optional(item: Type) -> Type {
    Type {
        r#type: Some(TypeKind::OptionalType(Box::new(OptionalType {
            item: Some(Box::new(item)),
        }))),
    }
}

fn column(name: &str, r#type: Type, not_null: Option<bool>) -> ColumnMeta {
    ColumnMeta {
        name: name.to_owned(),
        r#type: Some(r#type),
        family: String::new(),
        not_null,
        default_value: None,
    }
}

fn result_column(name: &str, r#type: Type) -> Column {
    Column {
        name: name.to_owned(),
        r#type: Some(r#type),
    }
}

fn scalar(value: value::Value) -> Value {
    Value {
        value: Some(value),
        ..Value::default()
    }
}

fn high_low(low: u64, high: u64) -> Value {
    Value {
        high_128: high,
        value: Some(value::Value::Low128(low)),
        ..Value::default()
    }
}
