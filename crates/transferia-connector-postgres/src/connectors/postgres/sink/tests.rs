use arrow::array::{
    ArrayRef, BinaryArray, Date32Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, TimestampMicrosecondArray, TimestampNanosecondArray, TimestampSecondArray,
    UInt16Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::config::PostgresSinkConfig;
use super::connector::{
    ambiguous_drop_is_complete, classify_owner_marker, cleanup_ownership_action,
    isolate_discovery, owner_marker_allows_side_effect, physical_target_set,
    postgres_cleanup_ddl, postgres_owned_create_ddl, postgres_physical_target,
    postgres_sql_type, validate_cleanup_scope, CleanupOwnershipAction, OwnerMarkerEvidence,
    PostgresSinkConnector, PostgresSpeedtestScope,
};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::delivery::{
    ArrowTypeFamily, DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SinkLimits,
    SourceTopology,
};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::SinkBatch;
use transferia_registry::{
    DatasetPrepare, SinkConnector, SinkSpeedtestIsolation, SpeedtestPhysicalTarget,
};
use crate::connectors::postgres::PostgresCopyFormat;

fn discovery(table: &str) -> DeliveryDiscovery {
    let schema = DatasetSchema::new(vec![SchemaColumn::new(
        "value".to_owned(),
        DataType::Int64,
        false,
    )]);
    DeliveryDiscovery {
        source_name: Arc::from("source"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![
            DiscoveredDataset {
                role: DatasetRole::Main,
                name: Arc::from(table),
                incoming_schema: schema.clone(),
                stored_schema: schema.clone(),
                system_columns: Vec::new(),
            },
            DiscoveredDataset {
                role: DatasetRole::DeadLetterQueue,
                name: Arc::from(format!("{table}_dlq")),
                incoming_schema: schema.clone(),
                stored_schema: schema,
                system_columns: Vec::new(),
            },
        ],
        performance_advice: Vec::new(),
    }
}

fn config() -> PostgresSinkConfig {
    serde_yaml::from_str(
        "host: 127.0.0.1\nport: 1\ndatabase: analytics\nusername: test\npassword: ''\ntrusted_plaintext: true\ncreate_tables: true\n",
    )
    .unwrap()
}

#[test]
fn postgres_sink_declares_its_lossless_bytea_binary_contract() {
    let limits = config().description();

    assert!(limits
        .supported_arrow_types
        .contains(&ArrowTypeFamily::Binary));
}

#[test]
fn clickbench_types_pass_postgres_startup_and_runtime_validation() -> anyhow::Result<()> {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("WatchID".to_owned(), DataType::Int64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("JavaEnable".to_owned(), DataType::Int16, false),
        SchemaColumn::new("Title".to_owned(), DataType::Binary, false),
        SchemaColumn::new(
            "EventTime".to_owned(),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
            false,
        )
        .with_constraints(true, false, None),
        SchemaColumn::new("EventDate".to_owned(), DataType::Date32, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("CounterID".to_owned(), DataType::Int32, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("UserID".to_owned(), DataType::Int64, false)
            .with_constraints(true, false, None),
    ]);
    let discovery = DeliveryDiscovery {
        source_name: Arc::from("clickbench"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("hits"),
            incoming_schema: schema.clone(),
            stored_schema: schema.clone(),
            system_columns: Vec::new(),
        }],
        performance_advice: Vec::new(),
    };
    let config = config();
    config.validate_discovery(&discovery)?;

    let fields = schema
        .columns
        .iter()
        .map(|column| {
            Field::new(&column.name, column.data_type.clone(), column.nullable)
                .with_metadata(column.arrow_metadata())
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Int64Array::from(vec![7])) as ArrayRef,
            Arc::new(Int16Array::from(vec![1])) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values([&[0, 0xff, b'\t'][..]])) as ArrayRef,
            Arc::new(TimestampSecondArray::from(vec![1_704_067_200])) as ArrayRef,
            Arc::new(Date32Array::from(vec![19_723])) as ArrayRef,
            Arc::new(Int32Array::from(vec![42])) as ArrayRef,
            Arc::new(Int64Array::from(vec![9])) as ArrayRef,
        ],
    )?;
    let byte_size = batch.get_array_memory_size();
    let runtime = SinkBatch {
        table: Arc::from("hits"),
        is_dlq: false,
        batch,
        byte_size,
        memory: PipelineMemory::new(1).reserve_transform(1),
        system_columns: SystemColumns::default(),
    };
    config.validate_batch(&discovery, &runtime)?;

    assert!(super::copy_binary::encode(&runtime.batch).is_ok());
    assert!(super::copy_text::encode(&runtime.batch).is_ok());
    Ok(())
}

#[test]
fn sink_copy_from_format_defaults_to_binary_and_accepts_explicit_text() {
    let binary = config();
    let text: PostgresSinkConfig = serde_yaml::from_str(
        "host: 127.0.0.1\nport: 1\ndatabase: analytics\nusername: test\npassword: ''\ntrusted_plaintext: true\ncreate_tables: true\ncopy_from_format: text\n",
    )
    .unwrap();

    assert_eq!(binary.copy_from_format, PostgresCopyFormat::Binary);
    assert_eq!(text.copy_from_format, PostgresCopyFormat::Text);
}

#[test]
fn postgres_sink_ddl_covers_every_copy_encoder_type() {
    for (data_type, sql) in [
        (DataType::Boolean, "boolean"),
        (DataType::Int8, "\"char\""),
        (DataType::Int16, "smallint"),
        (DataType::Int32, "integer"),
        (DataType::Int64, "bigint"),
        (DataType::UInt8, "smallint"),
        (DataType::UInt16, "integer"),
        (DataType::UInt32, "oid"),
        (DataType::UInt64, "numeric(20,0)"),
        (DataType::Float32, "real"),
        (DataType::Float64, "double precision"),
        (DataType::Binary, "bytea"),
        (DataType::Utf8, "text"),
        (DataType::Date32, "date"),
        (
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            "timestamp",
        ),
        (
            DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
            "timestamp with time zone",
        ),
    ] {
        assert_eq!(postgres_sql_type(&data_type).unwrap(), sql);
    }
}

#[test]
fn postgres_sink_rejects_non_utc_timestamp_metadata_during_discovery() {
    let mut discovery = discovery("events");
    let schema = DatasetSchema::new(vec![SchemaColumn::new(
        "value".to_owned(),
        DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            Some("Europe/Moscow".into()),
        ),
        false,
    )]);
    for dataset in &mut discovery.datasets {
        dataset.incoming_schema = schema.clone();
        dataset.stored_schema = schema.clone();
    }

    let error = config()
        .validate_discovery(&discovery)
        .expect_err("PostgreSQL cannot preserve a non-UTC Arrow timezone annotation");
    assert!(error.to_string().contains("use explicit UTC or no timezone"));
}

#[test]
fn copy_encoders_preserve_every_unsigned_integer_without_narrowing() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("u8", DataType::UInt8, false),
            Field::new("u16", DataType::UInt16, false),
            Field::new("u64", DataType::UInt64, false),
        ])),
        vec![
            Arc::new(UInt8Array::from(vec![u8::MAX])) as ArrayRef,
            Arc::new(UInt16Array::from(vec![u16::MAX])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef,
        ],
    )
    .unwrap();

    let text = super::copy_text::encode(&batch).unwrap();
    assert_eq!(text.as_ref(), b"255\t65535\t18446744073709551615\n");

    let binary = super::copy_binary::encode(&batch).unwrap();
    let numeric = [
        0, 0, 0, 18, // field length
        0, 5, // ndigits
        0, 4, // weight
        0, 0, // positive sign
        0, 0, // scale
        7, 52, // 1844
        26, 88, // 6744
        2, 225, // 737
        3, 187, // 955
        6, 79, // 1615
    ];
    assert!(binary.windows(numeric.len()).any(|window| window == numeric));
}

#[test]
fn sink_rejects_the_old_connection_string() {
    assert!(serde_yaml::from_str::<PostgresSinkConfig>(
        "connection: host=localhost port=5432\ntrusted_plaintext: true\ncreate_tables: true\n"
    )
    .is_err());
}

#[test]
fn binary_copy_encoder_writes_header_rows_null_and_trailer() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![7])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        ],
    )
    .unwrap();
    let encoded = super::copy_binary::encode(&batch).unwrap();
    assert!(encoded.starts_with(b"PGCOPY\n\xFF\r\n\0"));
    assert_eq!(&encoded[encoded.len() - 2..], &(-1_i16).to_be_bytes());
}

#[test]
fn text_copy_encoder_escapes_values_and_preserves_binary_date_and_timestamp() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, false),
            Field::new("day", DataType::Date32, false),
            Field::new(
                "created_at",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                false,
            ),
        ])),
        vec![
            Arc::new(StringArray::from(vec![Some("a\tb\\c\n"), None])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![b"\0\xff".as_slice(), b"".as_slice()])) as ArrayRef,
            Arc::new(Date32Array::from(vec![19_723, 0])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![
                1_704_067_200_123_456_000,
                -1_000,
            ])) as ArrayRef,
        ],
    )
    .unwrap();

    let encoded = super::copy_text::encode(&batch).unwrap();
    assert_eq!(
        encoded.as_ref(),
        b"a\\tb\\\\c\\n\t\\\\x00ff\t2024-01-01\t2024-01-01 00:00:00.123456\n\\N\t\\\\x\t1970-01-01\t1969-12-31 23:59:59.999999\n"
    );
}

#[test]
fn text_copy_encoder_preserves_the_complete_postgres_internal_char_domain() {
    let values = (u8::MIN..=u8::MAX)
        .map(|byte| i8::from_ne_bytes([byte]))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("value", DataType::Int8, false)])),
        vec![Arc::new(Int8Array::from(values)) as ArrayRef],
    )
    .unwrap();
    let encoded = super::copy_text::encode(&batch).unwrap();
    let lines = encoded.split(|byte| *byte == b'\n').collect::<Vec<_>>();
    assert_eq!(lines.len(), 257);
    assert_eq!(lines[0], b"");
    assert_eq!(lines[9], b"\\t");
    assert_eq!(lines[10], b"\\n");
    assert_eq!(lines[92], b"\\\\");
    assert_eq!(lines[127], b"\x7f");
    assert_eq!(lines[128], b"\\\\200");
    assert_eq!(lines[255], b"\\\\377");
    assert_eq!(lines[256], b"");
}

#[test]
fn both_copy_encoders_reject_nanosecond_values_postgres_cannot_store_losslessly() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "created_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            false,
        )])),
        vec![Arc::new(TimestampNanosecondArray::from(vec![1])) as ArrayRef],
    )
    .unwrap();

    assert!(super::copy_binary::encode(&batch).is_err());
    assert!(super::copy_text::encode(&batch).is_err());
}

#[test]
fn both_copy_encoders_preserve_explicit_utc_and_reject_unrepresentable_temporals() {
    let utc = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "observed_at",
            DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
            false,
        )])),
        vec![Arc::new(
            TimestampMicrosecondArray::from(vec![1_704_067_200_123_456])
                .with_timezone("UTC"),
        ) as ArrayRef],
    )
    .unwrap();
    assert_eq!(
        super::copy_text::encode(&utc).unwrap().as_ref(),
        b"2024-01-01 00:00:00.123456+00\n"
    );
    assert!(super::copy_binary::encode(&utc).is_ok());

    let invalid_timezone = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "observed_at",
            DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("Europe/Moscow".into()),
            ),
            false,
        )])),
        vec![Arc::new(
            TimestampMicrosecondArray::from(vec![0]).with_timezone("Europe/Moscow"),
        ) as ArrayRef],
    )
    .unwrap();
    assert!(super::copy_text::encode(&invalid_timezone).is_err());
    assert!(super::copy_binary::encode(&invalid_timezone).is_err());

    let invalid_date = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "day",
            DataType::Date32,
            false,
        )])),
        vec![Arc::new(Date32Array::from(vec![i32::MIN])) as ArrayRef],
    )
    .unwrap();
    assert!(super::copy_text::encode(&invalid_date).is_err());
    assert!(super::copy_binary::encode(&invalid_date).is_err());

    let invalid_timestamp = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "observed_at",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            false,
        )])),
        vec![Arc::new(TimestampMicrosecondArray::from(vec![i64::MIN])) as ArrayRef],
    )
    .unwrap();
    assert!(super::copy_text::encode(&invalid_timestamp).is_err());
    assert!(super::copy_binary::encode(&invalid_timestamp).is_err());
}

#[test]
fn speedtest_rewrites_every_dataset_within_postgres_name_limit() -> anyhow::Result<()> {
    let original = discovery("events");
    let (rewritten, mapping, tables) =
        isolate_discovery(&original, "0123456789abcdef0123456789abcdef")?;

    assert_eq!(rewritten.datasets.len(), original.datasets.len());
    assert_eq!(mapping.len(), original.datasets.len());
    assert_eq!(tables.len(), original.datasets.len());
    assert!(tables.iter().all(|table| table.len() <= 63));
    assert_eq!(
        mapping.get("events").map(AsRef::as_ref),
        Some("_transferia_st_0123456789abcdef0123456789abcdef_0")
    );
    assert_eq!(
        rewritten
            .datasets
            .iter()
            .map(|dataset| Arc::clone(&dataset.name))
            .collect::<BTreeSet<_>>(),
        tables
    );
    assert!(isolate_discovery(&original, "not-random; DROP TABLE events").is_err());
    Ok(())
}

#[test]
fn postgres_physical_proof_contains_database_schema_and_table() {
    assert_eq!(
        postgres_physical_target("analytics", "custom", "events").as_ref(),
        "\"analytics\".\"custom\".\"events\""
    );
}

#[test]
fn postgres_cleanup_ddl_quotes_exact_schema_and_table() -> anyhow::Result<()> {
    let table = "_transferia_st_0123456789abcdef0123456789abcdef_f";
    assert_eq!(
        postgres_cleanup_ddl("odd\"schema", table)?,
        "DROP TABLE IF EXISTS \"odd\"\"schema\".\"_transferia_st_0123456789abcdef0123456789abcdef_f\""
    );
    assert!(postgres_cleanup_ddl("public", "events").is_err());
    Ok(())
}

#[tokio::test]
async fn production_postgres_connector_cannot_cleanup_speedtest_tables() -> anyhow::Result<()> {
    let connector = Arc::new(PostgresSinkConnector::from_config(config())?);
    let original = discovery("events");
    let scratch: Arc<str> =
        Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let mut rewritten = original.clone();
    rewritten.datasets[0].name = Arc::clone(&scratch);
    rewritten.datasets.truncate(1);
    let single_original = DeliveryDiscovery {
        datasets: original.datasets[..1].to_vec(),
        ..original
    };
    let target = SpeedtestPhysicalTarget {
        production: postgres_physical_target("analytics", "public", "events"),
        scratch: postgres_physical_target("analytics", "public", &scratch),
    };
    let isolation = SinkSpeedtestIsolation::scratch(
        Arc::clone(&connector) as Arc<dyn SinkConnector>,
        &single_original,
        rewritten,
        BTreeMap::from([(Arc::from("events"), Arc::clone(&scratch))]),
        vec![target],
    )?;

    let error = connector.cleanup_speedtest(&isolation).await.unwrap_err();
    assert!(error
        .to_string()
        .contains("production PostgreSQL connector"));
    Ok(())
}

#[test]
fn postgres_cleanup_requires_exact_table_and_physical_target_sets() -> anyhow::Result<()> {
    let scratch: Arc<str> =
        Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let original = discovery("events");
    let mut rewritten = original.clone();
    rewritten.datasets[0].name = Arc::clone(&scratch);
    rewritten.datasets.truncate(1);
    let single_original = DeliveryDiscovery {
        datasets: original.datasets[..1].to_vec(),
        ..original
    };
    let target = SpeedtestPhysicalTarget {
        production: postgres_physical_target("analytics", "public", "events"),
        scratch: postgres_physical_target("analytics", "public", &scratch),
    };
    let isolation = SinkSpeedtestIsolation::scratch(
        Arc::new(PostgresSinkConnector::from_config(config())?),
        &single_original,
        rewritten,
        BTreeMap::from([(Arc::from("events"), Arc::clone(&scratch))]),
        vec![target.clone()],
    )?;
    let valid = PostgresSpeedtestScope {
        database: Arc::from("analytics"),
        schema: Arc::from("public"),
        owner_marker: Arc::from("transferia-speedtest-owner:test"),
        tables: BTreeSet::from([Arc::clone(&scratch)]),
        schemas: BTreeMap::new(),
        physical_targets: physical_target_set(std::slice::from_ref(&target)),
        attempted_tables: std::sync::Mutex::new(BTreeSet::new()),
        claimed_tables: std::sync::Mutex::new(BTreeSet::new()),
    };
    validate_cleanup_scope(&isolation, &valid)?;

    let wrong_table = PostgresSpeedtestScope {
        database: Arc::from("analytics"),
        schema: Arc::from("public"),
        owner_marker: Arc::clone(&valid.owner_marker),
        tables: BTreeSet::from([Arc::from(
            "_transferia_st_0123456789abcdef0123456789abcdef_1",
        )]),
        schemas: BTreeMap::new(),
        physical_targets: valid.physical_targets.clone(),
        attempted_tables: std::sync::Mutex::new(BTreeSet::new()),
        claimed_tables: std::sync::Mutex::new(BTreeSet::new()),
    };
    assert!(validate_cleanup_scope(&isolation, &wrong_table).is_err());

    let wrong_target = PostgresSpeedtestScope {
        database: Arc::from("analytics"),
        schema: Arc::from("public"),
        owner_marker: Arc::clone(&valid.owner_marker),
        tables: BTreeSet::from([scratch]),
        schemas: BTreeMap::new(),
        physical_targets: BTreeSet::from([(
            Arc::from("\"analytics\".\"public\".\"events\""),
            Arc::from("\"analytics\".\"public\".\"other\""),
        )]),
        attempted_tables: std::sync::Mutex::new(BTreeSet::new()),
        claimed_tables: std::sync::Mutex::new(BTreeSet::new()),
    };
    assert!(validate_cleanup_scope(&isolation, &wrong_target).is_err());
    Ok(())
}

#[test]
fn postgres_owner_marker_rejects_collisions_and_replacements() {
    let owner = "transferia-speedtest-owner:ours";
    assert_eq!(
        classify_owner_marker(Some(Some(owner)), owner),
        OwnerMarkerEvidence::Owned
    );
    assert_eq!(
        classify_owner_marker(Some(Some("transferia-speedtest-owner:foreign")), owner),
        OwnerMarkerEvidence::Foreign
    );
    assert_eq!(
        classify_owner_marker(Some(None), owner),
        OwnerMarkerEvidence::Unmarked
    );
    assert_eq!(
        classify_owner_marker(None, owner),
        OwnerMarkerEvidence::Missing
    );
    assert!(owner_marker_allows_side_effect(OwnerMarkerEvidence::Owned));
    assert!(!owner_marker_allows_side_effect(OwnerMarkerEvidence::Foreign));
    assert!(!owner_marker_allows_side_effect(OwnerMarkerEvidence::Unmarked));
    assert!(!owner_marker_allows_side_effect(OwnerMarkerEvidence::Missing));
    assert!(ambiguous_drop_is_complete(OwnerMarkerEvidence::Missing));
    assert!(!ambiguous_drop_is_complete(OwnerMarkerEvidence::Owned));
    assert!(!ambiguous_drop_is_complete(OwnerMarkerEvidence::Foreign));
}

#[test]
fn postgres_scratch_create_is_exclusive_and_marks_owner_atomically() -> anyhow::Result<()> {
    let table: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let dataset = DatasetPrepare {
        role: DatasetRole::Main,
        table,
        schema: DatasetSchema::new(vec![SchemaColumn::new(
            "value".to_owned(),
            DataType::Int64,
            false,
        )]),
        changelog: false,
    };
    let ddl = postgres_owned_create_ddl(
        "odd\"schema",
        &dataset,
        "transferia-speedtest-owner:a'b",
    )?;
    assert!(!ddl.contains("IF NOT EXISTS"));
    assert!(ddl.starts_with(
        "CREATE TABLE \"odd\"\"schema\".\"_transferia_st_0123456789abcdef0123456789abcdef_0\""
    ));
    assert!(ddl.contains(
        "COMMENT ON TABLE \"odd\"\"schema\".\"_transferia_st_0123456789abcdef0123456789abcdef_0\" IS 'transferia-speedtest-owner:a''b'"
    ));
    Ok(())
}

#[test]
fn postgres_partial_setup_tracks_every_attempt_but_only_proven_ownership() {
    let first: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let second: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_1");
    let scope = PostgresSpeedtestScope {
        database: Arc::from("analytics"),
        schema: Arc::from("public"),
        owner_marker: Arc::from("transferia-speedtest-owner:ours"),
        tables: BTreeSet::from([Arc::clone(&first), Arc::clone(&second)]),
        schemas: BTreeMap::new(),
        physical_targets: BTreeSet::new(),
        attempted_tables: std::sync::Mutex::new(BTreeSet::new()),
        claimed_tables: std::sync::Mutex::new(BTreeSet::new()),
    };

    scope.record_attempt(Arc::clone(&first));
    scope.claim(Arc::clone(&first));
    scope.record_attempt(Arc::clone(&second));
    assert_eq!(
        scope.attempted_tables(),
        BTreeSet::from([Arc::clone(&first), Arc::clone(&second)])
    );
    assert_eq!(scope.claimed_tables(), BTreeSet::from([Arc::clone(&first)]));
    scope.unclaim(&first);
    scope.unclaim(&first);
    assert!(scope.claimed_tables().is_empty());
    assert_eq!(scope.attempted_tables(), BTreeSet::from([second]));
}

fn fake_postgres_cleanup(
    scope: &PostgresSpeedtestScope,
    table: &Arc<str>,
    owner_probe: Result<OwnerMarkerEvidence, &'static str>,
    schema_matches: bool,
) -> Result<bool, &'static str> {
    match cleanup_ownership_action(owner_probe?) {
        CleanupOwnershipAction::AlreadyAbsent => {
            scope.unclaim(table);
            Ok(false)
        }
        CleanupOwnershipAction::VerifySchemaAndDrop if schema_matches => {
            scope.unclaim(table);
            Ok(true)
        }
        CleanupOwnershipAction::VerifySchemaAndDrop | CleanupOwnershipAction::Preserve => {
            Err("preserved: ownership or schema is not proven")
        }
    }
}

fn fault_scope(table: &Arc<str>) -> PostgresSpeedtestScope {
    PostgresSpeedtestScope {
        database: Arc::from("analytics"),
        schema: Arc::from("public"),
        owner_marker: Arc::from("transferia-speedtest-owner:ours"),
        tables: BTreeSet::from([Arc::clone(table)]),
        schemas: BTreeMap::new(),
        physical_targets: BTreeSet::new(),
        attempted_tables: std::sync::Mutex::new(BTreeSet::new()),
        claimed_tables: std::sync::Mutex::new(BTreeSet::new()),
    }
}

#[test]
fn postgres_lost_committed_create_remains_recoverable_after_unreadable_probes() {
    let table: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let scope = fault_scope(&table);
    scope.record_attempt(Arc::clone(&table));

    for _ in 0..2 {
        assert!(fake_postgres_cleanup(&scope, &table, Err("unreadable"), true).is_err());
        assert_eq!(
            scope.attempted_tables(),
            BTreeSet::from([Arc::clone(&table)])
        );
    }
    assert!(fake_postgres_cleanup(&scope, &table, Ok(OwnerMarkerEvidence::Owned), true)
        .expect("an exact late owner and schema proof permits exact cleanup"));
    assert!(scope.attempted_tables().is_empty());
}

#[test]
fn postgres_foreign_unmarked_or_wrong_schema_collision_is_preserved() {
    let table: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let scope = fault_scope(&table);
    scope.record_attempt(Arc::clone(&table));

    for evidence in [OwnerMarkerEvidence::Foreign, OwnerMarkerEvidence::Unmarked] {
        let error = fake_postgres_cleanup(&scope, &table, Ok(evidence), true).unwrap_err();
        assert!(error.contains("preserved"));
        assert_eq!(
            scope.attempted_tables(),
            BTreeSet::from([Arc::clone(&table)])
        );
    }
    assert!(fake_postgres_cleanup(&scope, &table, Ok(OwnerMarkerEvidence::Owned), false)
        .unwrap_err()
        .contains("preserved"));
    assert_eq!(scope.attempted_tables(), BTreeSet::from([table]));
}
