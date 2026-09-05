use mysql_async::binlog::jsonb::{JsonbString, ObjectKey, OpaqueValue, Value as JsonbValue};
use mysql_async::consts::ColumnType;
use mysql_async::Value;
use transferia_core::data::schema::{
    DatasetSchema, SchemaColumn, META_ARROW_EXTENSION_METADATA, META_ARROW_EXTENSION_NAME,
    META_MAX_LENGTH, META_OLD_VALUE_OF, META_PRIMARY_KEY,
};

use super::super::config::heartbeat_period_nanoseconds;
use super::super::decoder::{
    MySqlBinlogColumnIdentity, MySqlTableIdentity, MySqlTransactionIdentity,
};
use super::super::source::{
    append_json_array, append_json_object, build_table_schema, format_datetime_text,
    format_time_text, format_timestamp_text, format_transaction_gtid, normalize_binlog_value,
    normalize_enum, normalize_set, schema_materialization_admission_bytes, serialize_mysql_json,
    validate_replication_column_plan, validate_selected_table_map, verify_binlog_heartbeat,
};
use crate::connectors::mysql::src_batch::{
    ColumnPlan, DiscoveredTable, MySqlColumnKind, TableConfig,
};
use crate::connectors::mysql::src_batch_and_stream::{
    MySqlCollationPadding, MySqlColumnGeneration, MySqlColumnVisibility,
};

#[test]
fn binlog_heartbeat_is_exact_checked_and_verified_before_stream_handoff() {
    assert_eq!(heartbeat_period_nanoseconds(10).unwrap(), 10_000_000);
    assert!(heartbeat_period_nanoseconds(u64::MAX).is_err());

    verify_binlog_heartbeat(10_000_000, Some((10_000_000, 10_000_000))).unwrap();
    for observed in [None, Some((0, 10_000_000)), Some((10_000_000, 0))] {
        assert!(verify_binlog_heartbeat(10_000_000, observed).is_err());
    }
}

#[test]
fn tagged_gtid_metadata_is_canonical_and_never_derived_from_opaque_identity() {
    let identity = MySqlTransactionIdentity::Gtid {
        sid: [0x11; 16],
        tag: Some("blue".to_owned()),
        gno: 42,
    };
    assert_eq!(
        format_transaction_gtid(&identity).unwrap(),
        "11111111-1111-1111-1111-111111111111:blue:42"
    );
    assert!(
        format_transaction_gtid(&MySqlTransactionIdentity::FilePosition {
            begin_position:
                super::super::MySqlBinlogPosition::new(b"mysql-bin.000001".to_vec(), 4,).unwrap(),
        })
        .is_err()
    );
}

#[test]
fn table_map_rejects_same_count_column_rename_and_type_change() {
    let table = table(vec![column("id", "int", Some(1))]);
    let mut identity = table_identity(vec![binlog_column(
        "id",
        ColumnType::MYSQL_TYPE_LONG,
        Some(1),
    )]);
    validate_selected_table_map(&table, &identity).unwrap();

    identity.column_identities[0].name = b"renamed".to_vec();
    assert!(validate_selected_table_map(&table, &identity).is_err());

    identity.column_identities[0].name = b"id".to_vec();
    identity.column_identities[0].column_type = ColumnType::MYSQL_TYPE_SHORT;
    assert!(validate_selected_table_map(&table, &identity).is_err());
}

#[test]
fn table_map_rejects_primary_key_order_change_with_the_same_columns() {
    let table = table(vec![
        column("tenant_id", "int", Some(1)),
        column("item_id", "int", Some(2)),
    ]);
    let identity = table_identity(vec![
        binlog_column("tenant_id", ColumnType::MYSQL_TYPE_LONG, Some(2)),
        binlog_column("item_id", ColumnType::MYSQL_TYPE_LONG, Some(1)),
    ]);
    assert!(validate_selected_table_map(&table, &identity).is_err());
}

#[test]
fn table_map_rejects_collation_change_without_a_column_count_change() {
    let table = table(vec![text_column("body", 255)]);
    let mut column = binlog_column("body", ColumnType::MYSQL_TYPE_VARCHAR, None);
    column.collation_id = Some(45);
    column.metadata = 1_020_u16.to_le_bytes().to_vec();
    let identity = table_identity(vec![column]);
    assert!(validate_selected_table_map(&table, &identity).is_err());
}

#[test]
fn replication_discovery_accepts_lossless_item14_families_and_rejects_unknowns() {
    let mut json = column("value", "json", None);
    json.data_type = "json".to_owned();
    json.kind = MySqlColumnKind::Json;
    json.numeric_precision = None;
    json.numeric_scale = None;
    validate_replication_column_plan(&json).unwrap();

    for (data_type, column_type, kind) in [
        ("timestamp", "timestamp(6)", MySqlColumnKind::TimestampText),
        ("datetime", "datetime(6)", MySqlColumnKind::DateTimeText),
        ("time", "time(6)", MySqlColumnKind::TimeText),
    ] {
        let mut temporal = column("value", column_type, None);
        temporal.data_type = data_type.to_owned();
        temporal.kind = kind;
        temporal.datetime_precision = Some(6);
        temporal.numeric_precision = None;
        temporal.numeric_scale = None;
        validate_replication_column_plan(&temporal).unwrap();
    }

    let mut year = column("value", "year", None);
    year.data_type = "year".to_owned();
    year.kind = MySqlColumnKind::YearText;
    validate_replication_column_plan(&year).unwrap();

    for (data_type, column_type, kind) in [
        ("enum", "enum('a','b')", MySqlColumnKind::EnumOrdinal),
        ("set", "set('a','b')", MySqlColumnKind::SetBits),
    ] {
        let mut values = text_column("value", 255);
        values.data_type = data_type.to_owned();
        values.column_type = column_type.to_owned();
        values.kind = kind;
        values.enum_set_values = Some(vec!["a".to_owned(), "b".to_owned()]);
        values.character_maximum_length = Some(1);
        values.character_octet_length = Some(4);
        validate_replication_column_plan(&values).unwrap();
    }

    let mut latin1 = text_column("body", 8);
    latin1.character_set = Some("latin1".to_owned());
    latin1.collation = Some("latin1_swedish_ci".to_owned());
    latin1.kind = MySqlColumnKind::TextBytes;
    latin1.character_octet_length = Some(255);
    validate_replication_column_plan(&latin1).unwrap();
    latin1.character_set = Some("gbk".to_owned());
    assert!(validate_replication_column_plan(&latin1).is_err());

    let mut gbk_enum = enum_column(MySqlColumnKind::EnumOrdinal);
    gbk_enum.character_set = Some("gbk".to_owned());
    gbk_enum.collation = Some("gbk_chinese_ci".to_owned());
    assert!(validate_replication_column_plan(&gbk_enum).is_err());

    let mut virtual_column = column("generated", "int", None);
    virtual_column.generation = MySqlColumnGeneration::Virtual;
    assert!(validate_replication_column_plan(&virtual_column).is_err());

    let mut empty_enum = enum_column(MySqlColumnKind::EnumOrdinal);
    empty_enum.enum_set_values = Some(Vec::new());
    assert!(validate_replication_column_plan(&empty_enum).is_err());

    let mut oversized_set = enum_column(MySqlColumnKind::SetBits);
    oversized_set.data_type = "set".to_owned();
    oversized_set.column_type = "set('a')".to_owned();
    oversized_set.enum_set_values = Some((0..65).map(|index| index.to_string()).collect());
    assert!(validate_replication_column_plan(&oversized_set).is_err());
}

#[test]
fn unsigned_integer_normalization_preserves_mysql_common_wire_semantics() {
    for (kind, data_type, column_type, wire_type, value) in [
        (
            MySqlColumnKind::UInt8,
            "tinyint",
            "tinyint unsigned",
            ColumnType::MYSQL_TYPE_TINY,
            255,
        ),
        (
            MySqlColumnKind::UInt16,
            "smallint",
            "smallint unsigned",
            ColumnType::MYSQL_TYPE_SHORT,
            65_535,
        ),
        (
            MySqlColumnKind::UInt32,
            "mediumint",
            "mediumint unsigned",
            ColumnType::MYSQL_TYPE_INT24,
            16_777_215,
        ),
        (
            MySqlColumnKind::UInt64,
            "bigint",
            "bigint unsigned",
            ColumnType::MYSQL_TYPE_LONGLONG,
            2,
        ),
    ] {
        let mut column = column("value", column_type, None);
        column.data_type = data_type.to_owned();
        column.kind = kind;
        column.unsigned = true;
        let identity = binlog_column("value", wire_type, None);
        assert_eq!(
            normalize_binlog_value(
                mysql_async::binlog::value::BinlogValue::Value(Value::Int(value)),
                &column,
                &identity,
            )
            .unwrap(),
            Value::UInt(u64::try_from(value).unwrap())
        );
    }
}

#[test]
fn signed_mediumint_normalization_restores_mysql_common_zero_extended_wire_value() {
    let mut column = column("value", "mediumint", None);
    column.data_type = "mediumint".to_owned();
    column.kind = MySqlColumnKind::Int32;
    let identity = binlog_column("value", ColumnType::MYSQL_TYPE_INT24, None);
    assert_eq!(
        normalize_binlog_value(
            mysql_async::binlog::value::BinlogValue::Value(Value::Int(0x80_0000)),
            &column,
            &identity,
        )
        .unwrap(),
        Value::Int(-8_388_608)
    );
}

#[test]
fn char_normalization_matches_select_padding_without_touching_binary() {
    let mut char_column = text_column("value", 8);
    char_column.data_type = "char".to_owned();
    char_column.column_type = "char(8)".to_owned();
    let char_identity = binlog_column("value", ColumnType::MYSQL_TYPE_STRING, None);
    assert_eq!(
        normalize_binlog_value(
            mysql_async::binlog::value::BinlogValue::Value(Value::Bytes(b"a b     ".to_vec())),
            &char_column,
            &char_identity,
        )
        .unwrap(),
        Value::Bytes(b"a b".to_vec())
    );

    let mut binary_column = column("value", "binary(8)", None);
    binary_column.data_type = "binary".to_owned();
    binary_column.kind = MySqlColumnKind::Binary;
    binary_column.character_maximum_length = Some(8);
    binary_column.character_octet_length = Some(8);
    let binary_identity = binlog_column("value", ColumnType::MYSQL_TYPE_STRING, None);
    assert_eq!(
        normalize_binlog_value(
            mysql_async::binlog::value::BinlogValue::Value(Value::Bytes(vec![0, 0xff])),
            &binary_column,
            &binary_identity,
        )
        .unwrap(),
        Value::Bytes(vec![0, 0xff, 0, 0, 0, 0, 0, 0])
    );
    assert!(normalize_binlog_value(
        mysql_async::binlog::value::BinlogValue::Value(Value::Bytes(vec![0; 9])),
        &binary_column,
        &binary_identity,
    )
    .is_err());
}

#[test]
fn geometry_normalization_fences_the_authoritative_srid() {
    let mut column = column("shape", "point", None);
    column.data_type = "point".to_owned();
    column.kind = MySqlColumnKind::Binary;
    column.srs_id = Some(4_326);
    let identity = binlog_column("shape", ColumnType::MYSQL_TYPE_GEOMETRY, None);
    let mut geometry = 4_326_u32.to_le_bytes().to_vec();
    geometry.extend_from_slice(&[1, 1, 0, 0, 0]);
    assert_eq!(
        normalize_binlog_value(
            mysql_async::binlog::value::BinlogValue::Value(Value::Bytes(geometry.clone())),
            &column,
            &identity,
        )
        .unwrap(),
        Value::Bytes(geometry)
    );
    assert!(normalize_binlog_value(
        mysql_async::binlog::value::BinlogValue::Value(Value::Bytes(
            3_857_u32.to_le_bytes().to_vec()
        )),
        &column,
        &identity,
    )
    .is_err());
}

#[test]
fn schema_admission_scales_with_large_extension_metadata() {
    let plan = enum_column(MySqlColumnKind::EnumOrdinal);
    let schema_column = SchemaColumn::new(plan.name.clone(), plan.kind.arrow_type(), plan.nullable)
        .with_arrow_extension_metadata(
            plan.kind.arrow_extension_name(),
            plan.arrow_extension_metadata().unwrap(),
        );
    let mut table = table(vec![plan]);
    table.schema = DatasetSchema::new(vec![schema_column]);
    let baseline = schema_materialization_admission_bytes(&table).unwrap();
    table.schema.columns[0].arrow_extension_metadata = Some("x".repeat(1_000_000));
    let with_large_metadata = schema_materialization_admission_bytes(&table).unwrap();
    assert!(with_large_metadata >= baseline + 4_000_000);
}

#[test]
fn cached_stream_schema_preserves_only_extension_metadata_on_old_values() {
    let plan = enum_column(MySqlColumnKind::EnumOrdinal);
    let extension_metadata = plan.arrow_extension_metadata().unwrap();
    let current = SchemaColumn::new(plan.name.clone(), plan.kind.arrow_type(), false)
        .with_arrow_extension_metadata(plan.kind.arrow_extension_name(), extension_metadata.clone())
        .with_constraints(true, false, Some(65_535));
    let mut table = table(vec![plan]);
    table.schema = DatasetSchema::new(vec![current]);
    let schema = build_table_schema(&table).unwrap();
    let current = schema.field(0).metadata();
    assert_eq!(
        current.get(META_PRIMARY_KEY).map(String::as_str),
        Some("true")
    );
    assert_eq!(
        current.get(META_MAX_LENGTH).map(String::as_str),
        Some("65535")
    );
    let old = schema.field(1).metadata();
    assert!(!old.contains_key(META_PRIMARY_KEY));
    assert!(!old.contains_key(META_MAX_LENGTH));
    assert_eq!(
        old.get(META_ARROW_EXTENSION_NAME).map(String::as_str),
        Some(MySqlColumnKind::EnumOrdinal.arrow_extension_name())
    );
    assert_eq!(
        old.get(META_ARROW_EXTENSION_METADATA).map(String::as_str),
        Some(extension_metadata.as_str())
    );
    assert_eq!(
        old.get(META_OLD_VALUE_OF).map(String::as_str),
        Some("choice")
    );
}

#[test]
fn temporal_normalization_preserves_zero_partial_fsp_and_utc_timestamp() {
    assert_eq!(
        format_datetime_text(0, 0, 0, 0, 0, 0, 123_456, 6).unwrap(),
        "0000-00-00 00:00:00.123456"
    );
    assert_eq!(
        format_datetime_text(2024, 0, 7, 8, 9, 10, 120_000, 3).unwrap(),
        "2024-00-07 08:09:10.120"
    );
    assert_eq!(
        format_time_text(true, 1, 2, 3, 4, 500_000, 1).unwrap(),
        "-26:03:04.5"
    );
    assert_eq!(
        format_timestamp_text(Value::Bytes(b"1.123456".to_vec()), 4).unwrap(),
        "1970-01-01 00:00:01.1234"
    );
    assert_eq!(
        format_timestamp_text(Value::Bytes(b"0".to_vec()), 0).unwrap(),
        "0000-00-00 00:00:00"
    );
}

#[test]
fn enum_and_set_normalization_preserves_injective_ordinal_and_bitset_values() {
    let column = enum_column(MySqlColumnKind::EnumOrdinal);
    let mut identity = binlog_column("choice", ColumnType::MYSQL_TYPE_ENUM, None);
    identity.enum_values = Some(vec![b"".to_vec(), b"comma,value".to_vec()]);
    assert_eq!(
        normalize_enum(Value::Int(0), &identity, &column).unwrap(),
        0
    );
    assert_eq!(
        normalize_enum(Value::Int(1), &identity, &column).unwrap(),
        1
    );
    assert_eq!(
        normalize_enum(Value::Int(2), &identity, &column).unwrap(),
        2
    );
    assert!(normalize_enum(Value::Int(3), &identity, &column).is_err());

    let mut set_column = enum_column(MySqlColumnKind::SetBits);
    set_column.data_type = "set".to_owned();
    set_column.column_type = "set('a','b,c','d')".to_owned();
    let mut set_identity = binlog_column("choice", ColumnType::MYSQL_TYPE_SET, None);
    set_identity.set_values = Some(vec![b"a".to_vec(), b"b,c".to_vec(), b"d".to_vec()]);
    assert_eq!(
        normalize_set(Value::Bytes(vec![0b101]), &set_identity, &set_column).unwrap(),
        0b101
    );
    assert!(normalize_set(Value::Bytes(vec![0b1000]), &set_identity, &set_column).is_err());
}

#[test]
fn json_serializer_matches_mysql_spacing_escaping_and_opaque_values() {
    let mut column = column("payload", "json", None);
    column.data_type = "json".to_owned();
    column.kind = MySqlColumnKind::Json;
    let encoded = serialize_mysql_json(
        JsonbValue::String(JsonbString::new(b"line\n\"\\\0\xe2\x82\xac".as_slice())),
        &column,
    )
    .unwrap();
    assert_eq!(encoded, "\"line\\n\\\"\\\\\\u0000€\"".as_bytes());

    let mut object = Vec::new();
    append_json_object(
        vec![
            Ok::<_, std::io::Error>((ObjectKey::new(b"a".as_slice()), JsonbValue::I16(-2))),
            Ok::<_, std::io::Error>((ObjectKey::new(b"b".as_slice()), JsonbValue::Bool(true))),
        ]
        .into_iter(),
        &mut object,
        0,
    )
    .unwrap();
    assert_eq!(object, br#"{"a": -2, "b": true}"#);

    let mut array = Vec::new();
    append_json_array(
        vec![
            Ok::<_, std::io::Error>(JsonbValue::Null),
            Ok::<_, std::io::Error>(JsonbValue::U64(u64::MAX)),
            Ok::<_, std::io::Error>(JsonbValue::F64(1.0)),
        ]
        .into_iter(),
        &mut array,
        0,
    )
    .unwrap();
    assert_eq!(array, format!("[null, {}, 1.0]", u64::MAX).as_bytes());

    let decimal = mysql_async::binlog::decimal::Decimal::parse_str_bytes(b"-123.45").unwrap();
    let mut packed_decimal = vec![5, 2];
    decimal.write_bin(&mut packed_decimal).unwrap();
    assert_eq!(
        serialize_mysql_json(
            JsonbValue::Opaque(OpaqueValue::new(
                ColumnType::MYSQL_TYPE_NEWDECIMAL,
                packed_decimal,
            )),
            &column,
        )
        .unwrap(),
        b"-123.45"
    );

    assert_eq!(
        serialize_mysql_json(
            JsonbValue::Opaque(OpaqueValue::new(
                ColumnType::MYSQL_TYPE_VAR_STRING,
                b"line\n".as_slice(),
            )),
            &column,
        )
        .unwrap(),
        br#""line\n""#,
    );
    assert_eq!(
        serialize_mysql_json(
            JsonbValue::Opaque(OpaqueValue::new(
                ColumnType::MYSQL_TYPE_BLOB,
                b"raw".as_slice(),
            )),
            &column,
        )
        .unwrap(),
        br#""base64:type252:cmF3""#,
    );
    assert_eq!(
        serialize_mysql_json(
            JsonbValue::Opaque(OpaqueValue::new(
                ColumnType::MYSQL_TYPE_BLOB,
                [0_u8; 60].as_slice(),
            )),
            &column,
        )
        .unwrap(),
        format!("\"base64:type252:{}\\n{}\"", "A".repeat(76), "A".repeat(4)).as_bytes(),
    );
    assert!(serialize_mysql_json(
        JsonbValue::Opaque(OpaqueValue::new(
            ColumnType::MYSQL_TYPE_NEWDECIMAL,
            [1_u8, 2].as_slice(),
        )),
        &column,
    )
    .is_err());
    assert!(serialize_mysql_json(
        JsonbValue::Opaque(OpaqueValue::new(
            ColumnType::MYSQL_TYPE_TIME,
            i64::MIN.to_le_bytes().to_vec(),
        )),
        &column,
    )
    .is_err());

    let identity = binlog_column("payload", ColumnType::MYSQL_TYPE_JSON, None);
    assert!(normalize_binlog_value(
        mysql_async::binlog::value::BinlogValue::JsonDiff(Vec::new()),
        &column,
        &identity,
    )
    .is_err());
}

#[test]
fn json_double_format_matches_mysql_gcvt_notation_and_rounding() {
    let cases = [
        (999_999_999_999_999.0, "999999999999999.0"),
        (1e15, "1e15"),
        (1_000_000_000_000_000.5, "1000000000000000.5"),
        (1e16, "1e16"),
        (1e-15, "0.000000000000001"),
        (1e-16, "1e-16"),
        (1e200, "1e200"),
        (-0.0, "-0.0"),
        (1.234_567_890_123_456_7, "1.2345678901234567"),
    ];
    for (value, expected) in cases {
        let mut encoded = Vec::new();
        append_json_array(
            std::iter::once(Ok::<_, std::io::Error>(JsonbValue::F64(value))),
            &mut encoded,
            0,
        )
        .unwrap();
        assert_eq!(encoded, format!("[{expected}]").as_bytes());
    }
}

fn enum_column(kind: MySqlColumnKind) -> ColumnPlan {
    let mut column = text_column("choice", 255);
    column.data_type = "enum".to_owned();
    column.column_type = "enum('','comma,value')".to_owned();
    column.kind = kind;
    column.enum_set_values = Some(vec![String::new(), "comma,value".to_owned()]);
    column.character_maximum_length = Some(11);
    column.character_octet_length = Some(44);
    column
}

fn table(columns: Vec<ColumnPlan>) -> DiscoveredTable {
    DiscoveredTable {
        config: TableConfig {
            database: "inventory".to_owned(),
            name: "items".to_owned(),
        },
        schema: DatasetSchema::default(),
        columns,
        engine: "InnoDB".to_owned(),
    }
}

fn column(name: &str, column_type: &str, primary_key_ordinal: Option<u64>) -> ColumnPlan {
    ColumnPlan {
        name: name.to_owned(),
        data_type: column_type.to_owned(),
        kind: MySqlColumnKind::Int32,
        unsigned: false,
        zerofill: false,
        auto_increment: false,
        nullable: false,
        primary_key: primary_key_ordinal.is_some(),
        character_maximum_length: None,
        character_octet_length: None,
        numeric_precision: Some(10),
        numeric_scale: Some(0),
        datetime_precision: None,
        max_length: None,
        expression: format!("`{name}`"),
        column_type: column_type.to_owned(),
        character_set: None,
        collation: None,
        collation_id: None,
        collation_padding: None,
        enum_set_values: None,
        srs_id: None,
        visibility: MySqlColumnVisibility::Visible,
        generation: MySqlColumnGeneration::None,
        extra: String::new(),
        generation_expression: Some(String::new()),
        primary_key_ordinal,
        primary_key_prefix_length: None,
        primary_key_direction: primary_key_ordinal.map(|_| "A".to_owned()),
    }
}

fn text_column(name: &str, collation_id: u16) -> ColumnPlan {
    ColumnPlan {
        name: name.to_owned(),
        data_type: "varchar".to_owned(),
        kind: MySqlColumnKind::Utf8,
        unsigned: false,
        zerofill: false,
        auto_increment: false,
        nullable: false,
        primary_key: false,
        character_maximum_length: Some(255),
        character_octet_length: Some(1_020),
        numeric_precision: None,
        numeric_scale: None,
        datetime_precision: None,
        max_length: Some(255),
        expression: format!("`{name}`"),
        column_type: "varchar(255)".to_owned(),
        character_set: Some("utf8mb4".to_owned()),
        collation: Some("utf8mb4_0900_ai_ci".to_owned()),
        collation_id: Some(collation_id),
        collation_padding: Some(MySqlCollationPadding::NoPad),
        enum_set_values: None,
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

fn table_identity(column_identities: Vec<MySqlBinlogColumnIdentity>) -> MySqlTableIdentity {
    MySqlTableIdentity {
        table_id: 7,
        database: b"inventory".to_vec(),
        table: b"items".to_vec(),
        columns: column_identities.len() as u64,
        column_identities,
    }
}

fn binlog_column(
    name: &str,
    column_type: ColumnType,
    primary_key_ordinal: Option<u64>,
) -> MySqlBinlogColumnIdentity {
    MySqlBinlogColumnIdentity {
        name: name.as_bytes().to_vec(),
        column_type,
        metadata: Vec::new(),
        nullable: false,
        unsigned: column_type.is_numeric_type().then_some(false),
        collation_id: None,
        enum_values: None,
        set_values: None,
        geometry_type: None,
        vector_dimensionality: None,
        visible: true,
        primary_key_ordinal,
        primary_key_prefix_length: None,
    }
}
