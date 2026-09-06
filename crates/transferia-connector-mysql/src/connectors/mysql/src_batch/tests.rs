use std::mem::size_of;

#[test]
fn metadata_batch_uses_one_parameterized_catalog_join_for_one_hundred_tables() {
    let query = super::metadata::catalog_query(100, true);
    assert_eq!(query.matches('?').count(), 200);
    assert_eq!(query.matches("information_schema.COLUMNS AS c").count(), 1);
    assert_eq!(query.matches("information_schema.TABLES AS t").count(), 1);
    assert!(query.contains("SELECT 99 AS request_index"));
    assert!(query.contains("ORDER BY r.request_index, c.ORDINAL_POSITION"));
    for metadata in ["c.COLUMN_TYPE", "c.GENERATION_EXPRESSION", "c.NUMERIC_PRECISION", "c.NUMERIC_SCALE",
        "c.CHARACTER_OCTET_LENGTH", "c.SRS_ID AS SRS_ID", "col.PAD_ATTRIBUTE AS COLLATION_PADDING", "s.SEQ_IN_INDEX", "s.SUB_PART"] {
        assert!(query.contains(metadata), "lost native metadata {metadata}");
    }
    let maria = super::metadata::catalog_query(2, false);
    assert!(maria.contains("NULL AS COLLATION_PADDING"));
    assert!(maria.contains("NULL AS SRS_ID"));
    assert!(!maria.contains("c.SRS_ID"));
}

#[test]
fn table_sample_select_quotes_identifiers_and_limits_rows_in_database() {
    let table = transferia_registry::TableIdentity { namespace: "some`database".into(), name: "events`; DROP TABLE x; --".into() };
    assert_eq!(super::sample::sample_query(&table, "`id`", 7).unwrap(),
        "SELECT `id` FROM `some``database`.`events``; DROP TABLE x; --` LIMIT 7");
    assert!(super::sample::sample_query(&table, "`id`", 0).is_err());
}

#[test]
fn table_sample_deadline_uses_each_server_dialects_explicit_units() {
    assert_eq!(super::sample::timeout_statement("8.4.6", 1250), "SET SESSION max_execution_time = 1250");
    assert_eq!(super::sample::timeout_statement("11.8.3-MariaDB", 1250), "SET SESSION max_statement_time = 1.250");
}

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use mysql_async::{DriverError, Value};
use transferia_core::data::schema::{
    DatasetSchema, SchemaColumn, META_ARROW_EXTENSION_METADATA, META_ARROW_EXTENSION_NAME,
    META_MAX_LENGTH, META_OLD_VALUE_OF, META_PRIMARY_KEY, META_SYSTEM_ROLE,
    SYSTEM_ROLE_SOURCE_BINLOG_FILE, SYSTEM_ROLE_SOURCE_BINLOG_POSITION,
    SYSTEM_ROLE_SOURCE_BINLOG_ROW, SYSTEM_ROLE_SOURCE_GTID, SYSTEM_ROLE_SOURCE_SERVER_ID,
};
use transferia_core::delivery::DeliveryDiscoveryRequest;
use transferia_core::failure::FailureDisposition;
use transferia_delivery_contracts::DeliveryType;

use super::config::{
    MySqlReadProtocol, MySqlSourceConfig, DEFAULT_MYSQL_BATCH_TARGET_BYTES,
    DEFAULT_MYSQL_MAX_ROW_BYTES, MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES,
};
use super::connector::{
    build_delivery_discovery, collation_identity_is_consistent, column_generation,
    column_visibility, mysql_column_kind, parse_enum_set_values, snapshot_expression,
    validate_snapshot_read_protocol, ColumnPlan, DiscoveredTable, MySqlColumnKind,
};

#[test]
fn collation_identity_accepts_non_text_columns_and_requires_complete_mysql8_text_metadata() {
    assert!(collation_identity_is_consistent(
        false, false, false, false, true
    ));
    assert!(collation_identity_is_consistent(
        true, true, true, true, true
    ));
    assert!(!collation_identity_is_consistent(
        true, true, false, true, true
    ));
    assert!(!collation_identity_is_consistent(
        false, false, false, true, true
    ));
}
use super::reader::{
    build_output_schema, build_output_schema_with_memory, column_array,
    estimate_arrow_working_set_bytes, max_decoded_row_admission_bytes, next_snapshot_rows_capacity,
    optional_value_column_array, output_schema_allocation_bound, output_schema_heap_bytes,
    retained_row_value_heap_bytes, retained_rows_heap_bytes, rows_to_changelog_snapshot_batch,
    should_read_snapshot_row, snapshot_row_error, validate_snapshot_batch_growth,
    validate_snapshot_memory_limits, value_f64, value_i64, value_u64, MySqlSnapshotMetadata,
};
use crate::connectors::mysql::src_batch_and_stream::{
    MySqlBinlogBoundary, MySqlCollationPadding, MySqlColumnGeneration, MySqlColumnVisibility,
};

const MINIMAL_SOURCE_CONFIG: &str = "\
host: db.example
port: 3306
database: transferia
username: reader
password: secret
trusted_plaintext: true
tables:
  type: selected
  rules:
    - include: transferia.events
";

#[test]
fn system_table_filter_defaults_to_enabled_above_table_selection() -> anyhow::Result<()> {
    let config: MySqlSourceConfig = serde_yaml::from_str(MINIMAL_SOURCE_CONFIG)?;
    assert!(config.hide_system_tables);
    let explicit: MySqlSourceConfig = serde_yaml::from_str(&format!(
        "{MINIMAL_SOURCE_CONFIG}hide_system_tables: false\n"
    ))?;
    assert!(!explicit.hide_system_tables);
    let schema = serde_json::to_value(schemars::schema_for!(MySqlSourceConfig))?;
    let field = &schema["properties"]["hide_system_tables"];
    assert_eq!(field["title"], "Hide system tables");
    assert_eq!(field["default"], true);
    assert_eq!(field["x-ui"]["order"], 1);
    assert_eq!(schema["properties"]["tables"]["x-ui"]["order"], 2);
    assert_eq!(schema["properties"]["new_tables"]["x-ui"]["order"], 3);
    Ok(())
}

#[test]
fn table_selection_filters_only_exact_mysql_system_databases() -> anyhow::Result<()> {
    let mut config: MySqlSourceConfig = serde_yaml::from_str(MINIMAL_SOURCE_CONFIG)?;
    config.tables = serde_yaml::from_str("type: all")?;
    let namespaces = [
        "mysql",
        "information_schema",
        "performance_schema",
        "sys",
        "reports",
        "mysql_backup",
        "information_schema_extra",
        "performance_schema_backup",
        "syslog",
    ];
    let catalog = namespaces
        .iter()
        .map(|namespace| transferia_registry::TableIdentity {
            namespace: (*namespace).into(),
            name: format!("{namespace}_table"),
        })
        .collect::<Vec<_>>();
    let mut visible = catalog[4..].to_vec();
    visible.sort();
    assert_eq!(config.resolve_tables(catalog.clone())?, visible);
    for table in &catalog {
        assert_eq!(
            config.includes_database(&table.namespace),
            catalog[4..].contains(table)
        );
    }
    config.hide_system_tables = false;
    let mut all = catalog.clone();
    all.sort();
    assert_eq!(config.resolve_tables(catalog.clone())?, all);
    assert!(catalog
        .iter()
        .all(|table| config.includes_database(&table.namespace)));
    Ok(())
}

#[test]
fn hidden_table_rules_fail_before_startup_instead_of_silently_selecting_nothing(
) -> anyhow::Result<()> {
    let mut config: MySqlSourceConfig = serde_yaml::from_str(MINIMAL_SOURCE_CONFIG)?;
    config.tables = serde_yaml::from_str("type: selected\nrules:\n  - include: mysql.user\n")?;
    let catalog = vec![transferia_registry::TableIdentity {
        namespace: "mysql".into(),
        name: "user".into(),
    }];
    assert!(config.resolve_tables(catalog.clone()).is_err());
    config.tables = serde_yaml::from_str("type: all")?;
    assert!(config.resolve_tables(catalog.clone()).is_err());
    config.hide_system_tables = false;
    assert_eq!(config.resolve_tables(catalog.clone())?, catalog);
    Ok(())
}

#[test]
fn canonical_snapshot_session_removes_only_pad_char_mode() {
    assert_eq!(
        super::MYSQL_CANONICAL_SNAPSHOT_SQL_MODE,
        "SET SESSION sql_mode = TRIM(BOTH ',' FROM REPLACE(CONCAT(',', @@SESSION.sql_mode, ','), ',PAD_CHAR_TO_FULL_LENGTH,', ','))"
    );
}

#[test]
fn read_protocol_defaults_to_binary_and_accepts_text_explicitly() -> anyhow::Result<()> {
    let default: MySqlSourceConfig = serde_yaml::from_str(MINIMAL_SOURCE_CONFIG)?;
    assert_eq!(default.batch_rows, 16_384);
    assert_eq!(default.batch_target_bytes, DEFAULT_MYSQL_BATCH_TARGET_BYTES);
    assert_eq!(default.max_row_bytes, DEFAULT_MYSQL_MAX_ROW_BYTES);
    assert_eq!(default.read_protocol, MySqlReadProtocol::Binary);

    let text: MySqlSourceConfig =
        serde_yaml::from_str(&format!("{MINIMAL_SOURCE_CONFIG}read_protocol: text\n"))?;
    assert_eq!(text.read_protocol, MySqlReadProtocol::Text);

    assert!(serde_yaml::from_str::<MySqlSourceConfig>(&format!(
        "{MINIMAL_SOURCE_CONFIG}read_protocol: native\n"
    ))
    .is_err());
    Ok(())
}

#[test]
fn snapshot_memory_limits_are_visible_and_validated_before_execution() -> anyhow::Result<()> {
    let schema = serde_json::to_value(schemars::schema_for!(MySqlSourceConfig))?;
    let target = &schema["properties"]["batch_target_bytes"];
    assert_eq!(target["minimum"], 1);
    assert_eq!(target["maximum"], MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES);
    let row = &schema["properties"]["max_row_bytes"];
    assert_eq!(row["minimum"], 1_024);
    assert_eq!(row["maximum"], 1_073_741_824_u64);

    let mut config: MySqlSourceConfig = serde_yaml::from_str(MINIMAL_SOURCE_CONFIG)?;
    config.batch_target_bytes = 0;
    assert!(config.validate().is_err());
    config.batch_target_bytes = MYSQL_SNAPSHOT_BATCH_TARGET_MAX_BYTES + 1;
    assert!(config.validate().is_err());
    config.batch_target_bytes = DEFAULT_MYSQL_BATCH_TARGET_BYTES;
    config.max_row_bytes = 1_023;
    assert!(config.validate().is_err());
    config.max_row_bytes = 1_073_741_825;
    assert!(config.validate().is_err());
    validate_snapshot_memory_limits(
        16_384,
        DEFAULT_MYSQL_BATCH_TARGET_BYTES,
        DEFAULT_MYSQL_MAX_ROW_BYTES,
    )?;
    assert!(validate_snapshot_memory_limits(0, 1, 1_024).is_err());
    assert!(validate_snapshot_memory_limits(1, 0, 1_024).is_err());
    assert!(validate_snapshot_memory_limits(1, 1, 1_023).is_err());
    Ok(())
}

#[test]
fn snapshot_batch_stops_after_one_indivisible_target_overshoot() -> anyhow::Result<()> {
    assert!(should_read_snapshot_row(0, 0, 10, 100));
    assert!(should_read_snapshot_row(1, 99, 10, 100));
    assert!(!should_read_snapshot_row(2, 100, 10, 100));
    assert!(!should_read_snapshot_row(10, 99, 10, 100));
    validate_snapshot_batch_growth(99, 180, 100)?;
    assert!(validate_snapshot_batch_growth(100, 180, 100).is_err());
    assert!(validate_snapshot_batch_growth(99, 99, 100).is_err());
    assert_eq!(next_snapshot_rows_capacity(0, 0)?, 4);
    assert_eq!(next_snapshot_rows_capacity(3, 4)?, 4);
    assert_eq!(next_snapshot_rows_capacity(4, 4)?, 8);
    Ok(())
}

#[test]
fn snapshot_row_heap_uses_retained_vector_and_payload_capacities() -> anyhow::Result<()> {
    let mut payload = Vec::with_capacity(64);
    payload.extend_from_slice(b"payload");
    let mut row = Vec::with_capacity(8);
    row.push(Some(Value::Bytes(payload)));
    let row_bytes = retained_row_value_heap_bytes(&row)?;
    assert_eq!(row_bytes, 8 * size_of::<Option<Value>>() + 64);
    assert_eq!(
        retained_rows_heap_bytes(3, row_bytes)?,
        3 * size_of::<Vec<Option<Value>>>() + row_bytes
    );
    Ok(())
}

#[test]
fn low_wire_limit_pre_admits_high_column_decoded_row_overhead() -> anyhow::Result<()> {
    let admission = max_decoded_row_admission_bytes(1_024, 1_000)?;
    assert_eq!(
        admission,
        1_024 + 1_000 * size_of::<Option<Value>>() + size_of::<mysql_async::Row>()
    );
    assert!(admission > 1_024);
    Ok(())
}

#[test]
fn arrow_working_set_is_derived_from_rows_schema_and_payload() -> anyhow::Result<()> {
    let columns = vec![
        test_column("id", MySqlColumnKind::UInt64, "bigint unsigned", None),
        test_column(
            "body",
            MySqlColumnKind::Utf8,
            "varchar(255)",
            Some("utf8mb4"),
        ),
    ];
    let rows = vec![vec![
        Some(Value::UInt(7)),
        Some(Value::Bytes(vec![b'x'; 4_096])),
    ]];
    let estimate = estimate_arrow_working_set_bytes(&rows, &columns, None)?;
    assert!(estimate > 4_096);
    Ok(())
}

#[tokio::test]
async fn large_physical_metadata_is_pre_admitted_and_retained_with_snapshot_schema(
) -> anyhow::Result<()> {
    let mut column = test_column(
        "choice",
        MySqlColumnKind::EnumOrdinal,
        "enum('placeholder')",
        Some("utf8mb4"),
    );
    let large_members = (0..200)
        .map(|index| format!("{index:03}{}", "x".repeat(197)))
        .collect::<Vec<_>>();
    column.column_type = format!(
        "enum({})",
        large_members
            .iter()
            .map(|member| format!("'{member}'"))
            .collect::<Vec<_>>()
            .join(",")
    );
    column.enum_set_values = Some(large_members);
    let extension_metadata = column.arrow_extension_metadata()?;
    let schema = DatasetSchema::new(vec![SchemaColumn::new(
        column.name.clone(),
        column.kind.arrow_type(),
        column.nullable,
    )
    .with_arrow_extension_metadata(column.kind.arrow_extension_name(), extension_metadata)]);
    let allocation_bound =
        output_schema_allocation_bound(&schema, std::slice::from_ref(&column), true)?;

    assert!(
        allocation_bound > 128 * 1024,
        "pre-admission includes current and old extension payloads"
    );
    let memory = transferia_core::memory::PipelineMemory::new(1_024);
    let (output_schema, schema_memory) =
        build_output_schema_with_memory(&memory, &schema, std::slice::from_ref(&column), true)
            .await?;
    let schema_bytes = output_schema_heap_bytes(&output_schema)?;
    assert!(allocation_bound >= schema_bytes);
    assert_eq!(memory.used(), schema_bytes);
    assert_eq!(schema_memory.bytes(), schema_bytes);
    let rows = vec![vec![Some(Value::UInt(1))]];
    let snapshot = MySqlSnapshotMetadata {
        partition_id: 0,
        database: "inventory".to_owned(),
        table: "values".to_owned(),
        boundary: MySqlBinlogBoundary {
            filename: "mysql-bin.000001".to_owned(),
            position: 4,
            gtid_executed: "24bc7856-9a41-11ee-b9d1-0242ac120002:1".to_owned(),
            source_timestamp_micros: 1_700_000_000_000_000,
        },
    };
    let estimate =
        estimate_arrow_working_set_bytes(&rows, std::slice::from_ref(&column), Some(&snapshot))?;
    assert!(estimate > 0);
    Ok(())
}

#[test]
fn text_protocol_rejects_only_lossy_float32_snapshots_before_execution() -> anyhow::Result<()> {
    let float = test_column("f", MySqlColumnKind::Float32, "float", None);
    let double = test_column("d", MySqlColumnKind::Float64, "double", None);
    assert!(
        validate_snapshot_read_protocol(MySqlReadProtocol::Text, std::slice::from_ref(&float))
            .is_err()
    );
    validate_snapshot_read_protocol(MySqlReadProtocol::Binary, std::slice::from_ref(&float))?;
    validate_snapshot_read_protocol(MySqlReadProtocol::Text, std::slice::from_ref(&double))?;

    for bits in [0x8000_0000, 0x3f80_0001] {
        let value = f32::from_bits(bits);
        assert_eq!(value_f64::<f32>(&Value::Float(value))?.to_bits(), bits);
    }
    Ok(())
}

#[test]
fn optional_value_slice_conversion_does_not_require_row_reconstruction() -> anyhow::Result<()> {
    let column = test_column("value", MySqlColumnKind::UInt64, "bigint unsigned", None);
    let present = vec![Some(Value::UInt(7))];
    let rows = [Some(present.as_slice()), None];
    let array = optional_value_column_array(&rows, 0, &column)?;
    assert_eq!(array.len(), 2);
    assert_eq!(array.null_count(), 1);
    Ok(())
}

#[test]
fn snapshot_packet_limit_failure_is_fatal_but_transport_failure_retries() {
    let packet = snapshot_row_error(DriverError::PacketTooLarge.into(), 1_024);
    assert_eq!(packet.disposition(), FailureDisposition::Fatal);
    assert_eq!(
        packet.to_string(),
        "MySQL snapshot row exceeds configured max_row_bytes=1024"
    );

    let transport = snapshot_row_error(
        mysql_async::Error::Io(mysql_async::IoError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "fixture reset",
        ))),
        1_024,
    );
    assert_eq!(transport.disposition(), FailureDisposition::Retryable);
}

#[test]
fn mysql_and_mariadb_physical_families_have_explicit_lossless_snapshot_kinds() -> anyhow::Result<()>
{
    let cases = [
        ("tinyint", false, None, MySqlColumnKind::Int8),
        ("tinyint", true, None, MySqlColumnKind::UInt8),
        ("smallint", false, None, MySqlColumnKind::Int16),
        ("smallint", true, None, MySqlColumnKind::UInt16),
        ("mediumint", false, None, MySqlColumnKind::Int32),
        ("mediumint", true, None, MySqlColumnKind::UInt32),
        ("int", false, None, MySqlColumnKind::Int32),
        ("int", true, None, MySqlColumnKind::UInt32),
        ("integer", false, None, MySqlColumnKind::Int32),
        ("integer", true, None, MySqlColumnKind::UInt32),
        ("bigint", false, None, MySqlColumnKind::Int64),
        ("bigint", true, None, MySqlColumnKind::UInt64),
        ("float", false, None, MySqlColumnKind::Float32),
        ("double", false, None, MySqlColumnKind::Float64),
        ("real", false, None, MySqlColumnKind::Float64),
        ("bit", false, None, MySqlColumnKind::Binary),
        ("binary", false, None, MySqlColumnKind::Binary),
        ("varbinary", false, None, MySqlColumnKind::Binary),
        ("tinyblob", false, None, MySqlColumnKind::Binary),
        ("blob", false, None, MySqlColumnKind::Binary),
        ("mediumblob", false, None, MySqlColumnKind::Binary),
        ("longblob", false, None, MySqlColumnKind::Binary),
        ("char", false, Some("ascii"), MySqlColumnKind::Utf8),
        ("varchar", false, Some("utf8mb4"), MySqlColumnKind::Utf8),
        ("tinytext", false, Some("utf8mb3"), MySqlColumnKind::Utf8),
        ("text", false, Some("latin1"), MySqlColumnKind::TextBytes),
        (
            "mediumtext",
            false,
            Some("latin1"),
            MySqlColumnKind::TextBytes,
        ),
        (
            "longtext",
            false,
            Some("latin1"),
            MySqlColumnKind::TextBytes,
        ),
        ("inet4", false, Some("ascii"), MySqlColumnKind::Utf8),
        ("inet6", false, Some("ascii"), MySqlColumnKind::Utf8),
        ("uuid", false, Some("ascii"), MySqlColumnKind::Utf8),
        ("json", false, None, MySqlColumnKind::Json),
        ("decimal", false, None, MySqlColumnKind::DecimalText),
        ("numeric", false, None, MySqlColumnKind::DecimalText),
        ("date", false, None, MySqlColumnKind::DateText),
        ("datetime", false, None, MySqlColumnKind::DateTimeText),
        ("timestamp", false, None, MySqlColumnKind::TimestampText),
        ("time", false, None, MySqlColumnKind::TimeText),
        ("year", false, None, MySqlColumnKind::YearText),
        ("enum", false, Some("utf8mb4"), MySqlColumnKind::EnumOrdinal),
        ("enum", false, Some("latin1"), MySqlColumnKind::EnumOrdinal),
        ("set", false, Some("utf8mb4"), MySqlColumnKind::SetBits),
        ("set", false, Some("latin1"), MySqlColumnKind::SetBits),
        ("geometry", false, None, MySqlColumnKind::Binary),
        ("point", false, None, MySqlColumnKind::Binary),
        ("linestring", false, None, MySqlColumnKind::Binary),
        ("polygon", false, None, MySqlColumnKind::Binary),
        ("multipoint", false, None, MySqlColumnKind::Binary),
        ("multilinestring", false, None, MySqlColumnKind::Binary),
        ("multipolygon", false, None, MySqlColumnKind::Binary),
        ("geometrycollection", false, None, MySqlColumnKind::Binary),
        ("vector", false, None, MySqlColumnKind::Binary),
    ];
    for (data_type, unsigned, character_set, expected) in cases {
        assert_eq!(
            mysql_column_kind(data_type, unsigned, character_set)?,
            expected,
            "wrong snapshot contract for {data_type}"
        );
    }
    assert!(mysql_column_kind("varchar", false, None).is_err());
    assert!(mysql_column_kind("unsupported", false, None).is_err());
    Ok(())
}

#[test]
fn snapshot_projection_preserves_non_utf8_bytes_and_zero_capable_temporals() {
    assert_eq!(
        snapshot_expression("latin`,payload", "text", MySqlColumnKind::TextBytes),
        "CAST(`latin``,payload` AS BINARY) AS `latin``,payload`"
    );
    assert_eq!(
        snapshot_expression("fixed", "char", MySqlColumnKind::TextBytes),
        "CAST(RTRIM(`fixed`) AS BINARY) AS `fixed`"
    );
    assert_eq!(
        snapshot_expression("variable", "varchar", MySqlColumnKind::TextBytes),
        "CAST(`variable` AS BINARY) AS `variable`"
    );
    for kind in [
        MySqlColumnKind::DecimalText,
        MySqlColumnKind::DateText,
        MySqlColumnKind::DateTimeText,
        MySqlColumnKind::TimestampText,
        MySqlColumnKind::TimeText,
        MySqlColumnKind::YearText,
    ] {
        assert_eq!(
            snapshot_expression("value", "date", kind),
            "CAST(`value` AS CHAR) AS `value`"
        );
        assert_eq!(kind.arrow_type(), DataType::Utf8);
    }
    assert_eq!(MySqlColumnKind::TextBytes.arrow_type(), DataType::Binary);
    assert_eq!(MySqlColumnKind::EnumOrdinal.arrow_type(), DataType::UInt16);
    assert_eq!(MySqlColumnKind::SetBits.arrow_type(), DataType::UInt64);
    assert_eq!(
        snapshot_expression("choice", "enum", MySqlColumnKind::EnumOrdinal),
        "CAST(`choice` AS UNSIGNED) AS `choice`"
    );
    assert_eq!(
        snapshot_expression("flags", "set", MySqlColumnKind::SetBits),
        "CAST(`flags` AS UNSIGNED) AS `flags`"
    );
}

#[test]
fn enum_ordinal_and_set_bits_preserve_ambiguous_textual_values() -> anyhow::Result<()> {
    let enum_column = test_column(
        "choice",
        MySqlColumnKind::EnumOrdinal,
        "enum('','plain')",
        Some("utf8mb4"),
    );
    let enum_rows = [vec![Some(Value::UInt(0))], vec![Some(Value::UInt(1))]];
    let enum_array = column_array(&enum_rows, 0, &enum_column)?;
    let enum_array = enum_array
        .as_any()
        .downcast_ref::<arrow::array::UInt16Array>()
        .expect("ENUM physical identity must use UInt16 ordinals");
    assert_eq!(enum_array.values().as_ref(), &[0, 1]);

    let set_column = test_column(
        "flags",
        MySqlColumnKind::SetBits,
        "set('a,b','a','b')",
        Some("utf8mb4"),
    );
    let set_rows = [vec![Some(Value::UInt(1))], vec![Some(Value::UInt(6))]];
    let set_array = column_array(&set_rows, 0, &set_column)?;
    let set_array = set_array
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("SET physical identity must use UInt64 bitsets");
    assert_eq!(set_array.values().as_ref(), &[1, 6]);
    Ok(())
}

#[test]
fn raw_text_snapshot_accepts_bytes_that_must_not_be_coerced_to_utf8() -> anyhow::Result<()> {
    let raw = test_column("value", MySqlColumnKind::TextBytes, "text", Some("latin1"));
    let rows = [vec![Some(Value::Bytes(vec![0xff, 0x80, b'a']))]];
    let array = column_array(&rows, 0, &raw)?;
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .expect("raw text must use Arrow Binary");
    assert_eq!(array.value(0), [0xff, 0x80, b'a']);

    let utf8 = test_column("value", MySqlColumnKind::Utf8, "text", Some("utf8mb4"));
    assert!(column_array(&rows, 0, &utf8).is_err());
    Ok(())
}

#[test]
fn temporal_text_snapshot_preserves_zero_partial_and_fractional_values() -> anyhow::Result<()> {
    let cases = [
        (MySqlColumnKind::DateText, "0000-00-00"),
        (MySqlColumnKind::DateText, "2024-00-01"),
        (MySqlColumnKind::DateTimeText, "0000-00-00 00:00:00.000000"),
        (MySqlColumnKind::TimestampText, "2038-01-19 03:14:07.499999"),
        (MySqlColumnKind::TimeText, "-838:59:59.123456"),
        (MySqlColumnKind::YearText, "0000"),
    ];
    for (kind, expected) in cases {
        let column = test_column("value", kind, "temporal(6)", None);
        let rows = [vec![Some(Value::Bytes(expected.as_bytes().to_vec()))]];
        let array = column_array(&rows, 0, &column)?;
        let array = array
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("temporal logical value must use Arrow Utf8");
        assert_eq!(array.value(0), expected);
    }
    Ok(())
}

#[test]
fn enum_set_declarations_are_parsed_in_order_without_losing_members() -> anyhow::Result<()> {
    assert_eq!(
        parse_enum_set_values(
            "enum",
            r"enum('plain','comma,value','quote\'value','slash\\value','line\nvalue','')"
        )?,
        Some(vec![
            "plain".to_owned(),
            "comma,value".to_owned(),
            "quote'value".to_owned(),
            "slash\\value".to_owned(),
            "line\nvalue".to_owned(),
            String::new(),
        ])
    );
    assert_eq!(
        parse_enum_set_values("set", "set('first','second','third')")?,
        Some(vec![
            "first".to_owned(),
            "second".to_owned(),
            "third".to_owned(),
        ])
    );
    assert!(parse_enum_set_values("enum", "enum('unterminated)").is_err());
    assert!(parse_enum_set_values("set", "set('a' 'b')").is_err());
    assert!(parse_enum_set_values("set", "set('a',)").is_err());
    assert_eq!(parse_enum_set_values("varchar", "varchar(8)")?, None);
    Ok(())
}

#[test]
fn generation_and_visibility_are_structured_without_matching_default_generated() {
    assert_eq!(
        column_generation("", Some("")).unwrap(),
        MySqlColumnGeneration::None
    );
    assert_eq!(
        column_generation("DEFAULT_GENERATED", Some("")).unwrap(),
        MySqlColumnGeneration::None
    );
    assert_eq!(
        column_generation("VIRTUAL GENERATED", Some("`base` + 1")).unwrap(),
        MySqlColumnGeneration::Virtual
    );
    assert_eq!(
        column_generation("STORED GENERATED INVISIBLE", Some("`base` + 1")).unwrap(),
        MySqlColumnGeneration::Stored
    );
    assert!(column_generation("VIRTUAL GENERATED", Some("")).is_err());
    assert_eq!(column_visibility(""), MySqlColumnVisibility::Visible);
    assert_eq!(
        column_visibility("STORED GENERATED INVISIBLE"),
        MySqlColumnVisibility::Invisible
    );
}

#[test]
fn arrow_extension_metadata_retains_exact_mysql_physical_contract() -> anyhow::Result<()> {
    let mut column = test_column(
        "amount",
        MySqlColumnKind::DecimalText,
        "decimal(65,30) unsigned zerofill",
        None,
    );
    column.unsigned = true;
    column.zerofill = true;
    column.numeric_precision = Some(65);
    column.numeric_scale = Some(30);
    column.visibility = MySqlColumnVisibility::Invisible;
    column.extra = "INVISIBLE".to_owned();

    let metadata: serde_json::Value = serde_json::from_str(&column.arrow_extension_metadata()?)?;
    assert_eq!(
        metadata,
        serde_json::json!({
            "version": 1,
            "data_type": "decimal",
            "column_type": "decimal(65,30) unsigned zerofill",
            "unsigned": true,
            "zerofill": true,
            "auto_increment": false,
            "character_maximum_length": null,
            "character_octet_length": null,
            "numeric_precision": 65,
            "numeric_scale": 30,
            "datetime_precision": null,
            "character_set": null,
            "collation": null,
            "collation_id": null,
            "collation_padding": null,
            "enum_set_values": null,
            "srs_id": null,
            "visibility": "invisible",
            "generation": "none",
            "extra": "INVISIBLE",
            "generation_expression": "",
            "primary_key_ordinal": null,
            "primary_key_prefix_length": null,
            "primary_key_direction": null
        })
    );
    assert_eq!(
        column.kind.arrow_extension_name(),
        "transferia.mysql.decimal"
    );
    Ok(())
}

#[test]
fn text_extension_metadata_retains_collation_padding_semantics() -> anyhow::Result<()> {
    let column = test_column("fixed", MySqlColumnKind::Utf8, "char(8)", Some("utf8mb4"));
    let metadata: serde_json::Value = serde_json::from_str(&column.arrow_extension_metadata()?)?;
    assert_eq!(metadata["collation_padding"], serde_json::json!("no_pad"));
    Ok(())
}

#[test]
fn physical_extension_identity_survives_discovery_and_snapshot_old_values() -> anyhow::Result<()> {
    let column = test_column("created_at", MySqlColumnKind::DateText, "date", None);
    let extension_name = column.kind.arrow_extension_name();
    let extension_metadata = column.arrow_extension_metadata()?;
    let schema = DatasetSchema::new(vec![SchemaColumn::new(
        column.name.clone(),
        column.kind.arrow_type(),
        column.nullable,
    )
    .with_constraints(true, false, Some(16))
    .with_arrow_extension_metadata(extension_name, extension_metadata.clone())]);
    let table = DiscoveredTable {
        config: super::config::TableConfig {
            database: "transferia".to_owned(),
            name: "events".to_owned(),
        },
        schema: schema.clone(),
        columns: vec![column.clone()],
        engine: "InnoDB".to_owned(),
    };
    let discovery = build_delivery_discovery(
        true,
        DeliveryType::BatchAndStream,
        DeliveryDiscoveryRequest {
            keep_system_columns: false,
        },
        &[table],
    )?;
    let incoming = &discovery.datasets[0].incoming_schema.columns;
    assert_eq!(incoming[1].arrow_extension_name, Some(extension_name));
    assert_eq!(
        incoming[1].arrow_extension_metadata.as_deref(),
        Some(extension_metadata.as_str())
    );

    let rows = [vec![Some(Value::Bytes(b"0000-00-00".to_vec()))]];
    let batch = rows_to_changelog_snapshot_batch(
        build_output_schema(&schema, std::slice::from_ref(&column), true)?,
        &[column],
        &rows,
        0,
        &MySqlSnapshotMetadata {
            partition_id: 0,
            database: "inventory".to_owned(),
            table: "events".to_owned(),
            boundary: MySqlBinlogBoundary {
                filename: "mysql-bin.000001".to_owned(),
                position: 4,
                gtid_executed: "24bc7856-9a41-11ee-b9d1-0242ac120002:1".to_owned(),
                source_timestamp_micros: 1_700_000_000_000_000,
            },
        },
    )?;
    let runtime_schema = batch.schema();
    let old_metadata = runtime_schema.field(1).metadata();
    assert_eq!(
        old_metadata
            .get(META_ARROW_EXTENSION_NAME)
            .map(String::as_str),
        Some(extension_name)
    );
    assert_eq!(
        old_metadata
            .get(META_ARROW_EXTENSION_METADATA)
            .map(String::as_str),
        Some(extension_metadata.as_str())
    );
    assert_eq!(
        old_metadata.get(META_OLD_VALUE_OF).map(String::as_str),
        Some("created_at")
    );
    assert!(!old_metadata.contains_key(META_PRIMARY_KEY));
    assert!(!old_metadata.contains_key(META_MAX_LENGTH));

    let role_index = |role: &str| {
        runtime_schema
            .fields()
            .iter()
            .position(|field| {
                field.metadata().get(META_SYSTEM_ROLE).map(String::as_str) == Some(role)
            })
            .expect("snapshot source role must be present exactly once")
    };
    let server_id = batch
        .column(role_index(SYSTEM_ROLE_SOURCE_SERVER_ID))
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("source server id type");
    assert_eq!(server_id.value(0), 0);
    let gtid = batch
        .column(role_index(SYSTEM_ROLE_SOURCE_GTID))
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("source gtid type");
    assert!(gtid.is_null(0));
    let file = batch
        .column(role_index(SYSTEM_ROLE_SOURCE_BINLOG_FILE))
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("source binlog filename type");
    assert_eq!(file.value(0), "mysql-bin.000001");
    let position = batch
        .column(role_index(SYSTEM_ROLE_SOURCE_BINLOG_POSITION))
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("source binlog position type");
    assert_eq!(position.value(0), 4);
    let row = batch
        .column(role_index(SYSTEM_ROLE_SOURCE_BINLOG_ROW))
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("source binlog row type");
    assert_eq!(row.value(0), 0);
    Ok(())
}

fn test_column(
    name: &str,
    kind: MySqlColumnKind,
    column_type: &str,
    character_set: Option<&str>,
) -> ColumnPlan {
    let data_type = column_type
        .split(['(', ' '])
        .next()
        .expect("test type has a family")
        .to_owned();
    let unsigned = column_type
        .split_ascii_whitespace()
        .any(|token| token == "unsigned");
    let enum_set_values = parse_enum_set_values(&data_type, column_type)
        .expect("test enum/set declaration must be valid");
    ColumnPlan {
        name: name.to_owned(),
        data_type,
        kind,
        unsigned,
        zerofill: false,
        auto_increment: false,
        nullable: true,
        primary_key: false,
        character_maximum_length: character_set.map(|_| 8),
        character_octet_length: character_set.map(|_| 32),
        numeric_precision: None,
        numeric_scale: None,
        datetime_precision: None,
        max_length: None,
        expression: format!("`{name}`"),
        column_type: column_type.to_owned(),
        character_set: character_set.map(str::to_owned),
        collation: character_set.map(|charset| format!("{charset}_fixture")),
        collation_id: character_set.map(|_| 255),
        collation_padding: character_set.map(|_| MySqlCollationPadding::NoPad),
        enum_set_values,
        srs_id: None,
        visibility: MySqlColumnVisibility::Visible,
        generation: MySqlColumnGeneration::None,
        extra: String::new(),
        generation_expression: Some(String::new()),
        primary_key_ordinal: None,
        primary_key_prefix_length: None,
        primary_key_direction: None,
    }
}

#[test]
fn read_protocol_is_a_user_visible_advanced_choice() -> anyhow::Result<()> {
    let schema = serde_json::to_value(schemars::schema_for!(MySqlSourceConfig))?;
    let read_protocol = &schema["properties"]["read_protocol"];

    assert_eq!(read_protocol["$ref"], "#/$defs/MySqlReadProtocol");
    assert_eq!(read_protocol["x-ui"]["section"], "advanced");
    assert_eq!(
        schema["$defs"]["MySqlReadProtocol"]["enum"],
        serde_json::json!(["text", "binary"])
    );
    Ok(())
}

#[test]
fn source_schema_hides_replication_and_declares_all_delivery_modes() {
    let schema = serde_json::to_value(schemars::schema_for!(MySqlSourceConfig)).unwrap();
    assert_eq!(
        schema["properties"]["replication"]["x-ui"]["widget"],
        "hidden"
    );
    assert!(schema
        .pointer("/$defs/MySqlReplicationConfig/properties/server_id")
        .is_none());
    assert_eq!(
        schema.pointer("/x-ui/capabilities"),
        Some(&serde_json::json!({
            "component": "source",
            "key": "mysql",
            "delivery_modes": ["batch", "stream", "batch_and_stream"],
            "record_semantics": ["append_only", "changelog"],
            "batch_stream_handoff": "exact_switchover"
        }))
    );
    assert_eq!(
        schema.pointer("/$defs/MySqlReplicationConfig/x-ui/capabilities"),
        None
    );
    for (property, minimum) in [
        ("max_events", 1),
        ("max_transaction_bytes", 19),
        ("poll_interval_ms", 1),
        ("bootstrap_timeout_ms", 1),
    ] {
        assert_eq!(
            schema.pointer(&format!(
                "/$defs/MySqlReplicationConfig/properties/{property}/minimum"
            )),
            Some(&serde_json::json!(minimum)),
            "{property} did not expose its positive backend constraint"
        );
    }
}

#[test]
fn numeric_conversion_accepts_text_and_native_protocol_values() -> anyhow::Result<()> {
    assert_eq!(value_i64::<i8>(&Value::Int(-7))?, -7);
    assert_eq!(value_i64::<i32>(&Value::Bytes(b"42".to_vec()))?, 42);
    assert_eq!(value_u64::<u64>(&Value::UInt(u64::MAX))?, u64::MAX);
    assert_eq!(value_u64::<u16>(&Value::Bytes(b"65535".to_vec()))?, 65_535);
    assert!((value_f64::<f32>(&Value::Float(1.5))? - 1.5).abs() < f32::EPSILON);
    assert!((value_f64::<f64>(&Value::Bytes(b"2.25".to_vec()))? - 2.25).abs() < f64::EPSILON);
    Ok(())
}

#[test]
fn numeric_conversion_rejects_lossy_or_out_of_range_values() {
    assert!(value_i64::<i8>(&Value::Int(128)).is_err());
    assert!(value_u64::<u8>(&Value::Int(-1)).is_err());
    assert!(value_i64::<i64>(&Value::Bytes(b"1.5".to_vec())).is_err());
}
#[test]
fn cached_preview_rejects_changed_native_types_even_when_storage_is_the_same() {
    let make = |kind, declaration| super::connector::DiscoveredTable {
        config: super::config::TableConfig { database: "db".into(), name: "events".into() },
        schema: transferia_core::DatasetSchema::default(),
        columns: vec![test_column("value", kind, declaration, None)], engine: "InnoDB".into(),
    };
    for (old_kind, old, new_kind, new) in [
        (MySqlColumnKind::EnumOrdinal, "enum('a','b')", MySqlColumnKind::EnumOrdinal, "enum('b','a')"),
        (MySqlColumnKind::SetBits, "set('a','b')", MySqlColumnKind::SetBits, "set('b','a')"),
        (MySqlColumnKind::Int32, "int", MySqlColumnKind::Utf8, "varchar(8)"),
    ] {
        let cached = make(old_kind, old);
        assert!(super::sample::validate_cached_schema(&cached, &cached).is_ok());
        let error = super::sample::validate_cached_schema(&cached, &make(new_kind, new)).unwrap_err();
        assert!(error.to_string().contains("db.events"));
        assert!(error.to_string().contains("refresh metadata"));
        assert_eq!(cached.columns[0].column_type, old, "validation must not rewrite the cached plan");
    }
}
