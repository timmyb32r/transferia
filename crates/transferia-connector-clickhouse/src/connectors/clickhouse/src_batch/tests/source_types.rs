use super::super::{config::UnsupportedTypePolicy, types};
use arrow::datatypes::{DataType, Field};

#[test]
fn column_kind_readable_defaults_do_not_block_source_discovery() {
    let table = super::super::config::TableConfig {
        database: "_system".into(),
        name: "audit_log".into(),
    };
    for kind in ["", "DEFAULT", "MATERIALIZED", "ALIAS"] {
        super::super::connector::validate_source_column_kind(&table, "databases", kind)
            .unwrap();
    }
}

#[test]
fn column_kind_ephemeral_fails_with_specific_table_and_column_context() {
    let table = super::super::config::TableConfig {
        database: "analytics".into(),
        name: "events".into(),
    };
    let error = super::super::connector::validate_source_column_kind(
        &table, "input_only", "EPHEMERAL",
    ).unwrap_err().to_string();
    for expected in ["analytics", "events", "input_only", "EPHEMERAL", "cannot be read"] {
        assert!(error.contains(expected), "{error}");
    }
    assert!(!error.contains("SELECT *"), "{error}");
}

#[test]
fn column_kind_unknown_fails_closed() {
    let table = super::super::config::TableConfig {
        database: "analytics".into(), name: "events".into(),
    };
    assert!(super::super::connector::validate_source_column_kind(
        &table, "value", "FUTURE_KIND",
    ).is_err());
}

#[test]
fn reported_information_schema_enum_is_supported_without_conversion() {
    let declaration = "Enum8('NO' = 0, 'YES' = 1)";
    let column = types::source_column("is_updatable", declaration, UnsupportedTypePolicy::Fail).unwrap();
    assert_eq!(column.data_type, DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)));
    assert!(!types::is_string_conversion(&column));
    assert!(column.arrow_extension_metadata.unwrap().contains(declaration));
}

#[test]
fn unsupported_types_require_explicit_string_conversion() {
    for declaration in ["Dynamic", "Variant(String, UInt64)", "JSON", "AggregateFunction(sum, UInt64)", "Time64(6)", "QBit(Float32, 16)"] {
        assert!(types::source_column("value", declaration, UnsupportedTypePolicy::Fail).is_err(), "{declaration}");
        let column = types::source_column("value", declaration, UnsupportedTypePolicy::ToString).unwrap();
        assert_eq!(column.data_type, DataType::Utf8);
        assert_eq!(column.nullable, declaration != "JSON");
        assert!(types::is_string_conversion(&column));
        assert!(column.arrow_extension_metadata.unwrap().contains(declaration));
    }
}

#[test]
fn recursive_types_preserve_named_tuple_fields_and_timezone() {
    let column = types::source_column("value", "Array(Tuple(`event time` DateTime64(6, 'Europe/Moscow'), status Nullable(Enum8('NO'=0, 'YES'=1))))", UnsupportedTypePolicy::Fail).unwrap();
    let DataType::List(item) = column.data_type else { panic!("expected list") };
    let DataType::Struct(fields) = item.data_type() else { panic!("expected struct") };
    assert_eq!(fields[0].name(), "event time");
    assert_eq!(fields[0].data_type(), &DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("Europe/Moscow".into())));
    assert_eq!(fields[1].name(), "status");
    assert!(fields[1].is_nullable());
}

#[test]
fn wire_metadata_must_match_before_restoring_tuple_names() {
    let column = types::source_column("value", "Tuple(a Int64)", UnsupportedTypePolicy::Fail).unwrap();
    let field = Field::new("value", column.data_type.clone(), false).with_metadata(std::collections::HashMap::from([("clickhouse.type".into(), "Tuple(b Int64)".into())]));
    assert!(types::validate_wire_type(&field, &column).is_err());
    let field = field.with_metadata(std::collections::HashMap::from([("clickhouse.type".into(), "Tuple(a Int64)".into())]));
    assert!(types::validate_wire_type(&field, &column).unwrap());
}

#[test]
fn source_policy_is_fail_by_default_and_is_an_advanced_option() {
    let config: super::super::config::ClickHouseSourceConfig = serde_json::from_value(serde_json::json!({"hosts":["localhost"], "port":9000,"trusted_plaintext":true,"username":"default","tables":{"type":"all"}})).unwrap();
    assert_eq!(config.unsupported_types, UnsupportedTypePolicy::Fail);
    let schema = serde_json::to_value(schemars::schema_for!(super::super::config::ClickHouseSourceConfig)).unwrap();
    assert_eq!(schema["properties"]["unsupported_types"]["x-ui"]["section"], "advanced");
}

#[test]
fn decimal_precision_and_scale_are_not_widened_to_storage_capacity() {
    for (declaration, expected) in [("Decimal(4, 0)", DataType::Decimal128(4, 0)),
        ("Decimal(40, 12)", DataType::Decimal256(40, 12))] {
        let column = types::source_column("amount", declaration, UnsupportedTypePolicy::Fail).unwrap();
        assert_eq!(column.data_type, expected);
    }
}

#[test]
fn anonymous_tuple_and_geo_names_follow_clickhouse_numbering() {
    for declaration in ["Tuple(Float64, Float64)", "Point"] {
        let column = types::source_column("point", declaration, UnsupportedTypePolicy::Fail).unwrap();
        let DataType::Struct(fields) = column.data_type else { panic!("expected struct") };
        assert_eq!(fields[0].name(), "1");
        assert_eq!(fields[1].name(), "2");
    }
    let column = types::source_column("point", "Tuple(field_0 Float64, field_1 Float64)", UnsupportedTypePolicy::Fail).unwrap();
    let DataType::Struct(fields) = column.data_type else { panic!("expected struct") };
    assert_eq!(fields[0].name(), "field_0");
}

#[test]
fn snapshot_query_converts_only_opted_in_columns_and_guards_original_types() {
    use super::super::{config::TableConfig, connector::{DiscoveredTable, snapshot_query}};
    use transferia_core::data::schema::DatasetSchema;
    let table = DiscoveredTable {
        config: TableConfig { database: "db".into(), name: "events".into() },
        schema: DatasetSchema::new(vec![
            types::source_column("id", "Int64", UnsupportedTypePolicy::ToString).unwrap(),
            types::source_column("payload", "Dynamic", UnsupportedTypePolicy::ToString).unwrap(),
        ]),
        physical_system_columns: Default::default(),
    };
    let query = snapshot_query(&table);
    assert!(query.contains("source.`id` AS `id`"), "{query}");
    assert!(!query.contains("toString(source.`id`)"), "{query}");
    assert!(query.contains("CAST(toString(source.`payload`) AS Nullable(String)) AS `payload`"), "{query}");
    assert!(query.contains("throwIf(toTypeName(source.`payload`) != 'Dynamic'"), "{query}");
    assert!(query.contains("throwIf(toTypeName(source.`id`) != 'Int64'"), "{query}");
}

#[test]
fn quoted_tuple_identifiers_preserve_clickhouse_escapes_and_delimiters() {
    let declaration = r#"Tuple(`with space` String, `a,b(c)` UInt8, `a``b` Int64, "a""b" String, `line\nbreak` String, `\xD0\xAF` String, `back\\slash` UInt8, `unknown\q` UInt8, `nul\0byte` UInt8)"#;
    let column = types::source_column("value", declaration, UnsupportedTypePolicy::Fail).unwrap();
    let DataType::Struct(fields) = column.data_type else { panic!("expected struct") };
    let names = fields.iter().map(|field| field.name().as_str()).collect::<Vec<_>>();
    assert_eq!(names, ["with space", "a,b(c)", "a`b", "a\"b", "line\nbreak", "Я", "back\\slash", "unknown\\q", "nul\0byte"]);
}

#[test]
fn malformed_tuple_identifiers_and_remaining_types_fail_closed() {
    for declaration in [
        r"Tuple(`\xFF` String)", r"Tuple(`\xD0` String)",
        r"Tuple(`\x0` String)", r"Tuple(`unclosed String)",
        "Tuple(`name`Int64)", "Tuple(`name` Int64 garbage)",
        "Tuple(\"name\"\" String)",
    ] {
        assert!(types::source_column("value", declaration, UnsupportedTypePolicy::Fail).is_err(), "{declaration}");
    }
}

fn native_scalar_cases() -> Vec<(String, DataType)> {
    use arrow::datatypes::TimeUnit;
    let mut cases = vec![
        ("Int8", DataType::Int8), ("Int16", DataType::Int16),
        ("Int32", DataType::Int32), ("Int64", DataType::Int64),
        ("UInt8", DataType::UInt8), ("UInt16", DataType::UInt16),
        ("UInt32", DataType::UInt32), ("UInt64", DataType::UInt64),
        ("Bool", DataType::Boolean),
        ("Float32", DataType::Float32), ("Float64", DataType::Float64),
        ("String", DataType::Binary), ("FixedString(17)", DataType::FixedSizeBinary(17)),
        ("Decimal32(0)", DataType::Decimal128(9, 0)),
        ("Decimal64(18)", DataType::Decimal128(18, 18)),
        ("Decimal128(17)", DataType::Decimal128(38, 17)),
        ("Decimal256(76)", DataType::Decimal256(76, 76)),
        ("Decimal(4, 2)", DataType::Decimal128(4, 2)),
        ("Decimal(16, 0)", DataType::Decimal128(16, 0)),
        ("Decimal(34, 12)", DataType::Decimal128(34, 12)),
        ("Decimal(70, 30)", DataType::Decimal256(70, 30)),
        ("Date", DataType::Date32), ("Date32", DataType::Date32),
        ("DateTime", DataType::Timestamp(TimeUnit::Second, None)),
        ("DateTime('Europe/Moscow')", DataType::Timestamp(TimeUnit::Second, Some("Europe/Moscow".into()))),
        ("Enum8('negative' = -128, 'zero' = 0, 'positive' = 127)", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))),
        ("Enum16('negative' = -32768, 'zero' = 0, 'positive' = 32767)", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))),
    ].into_iter().map(|(declaration, data_type)| (declaration.to_owned(), data_type)).collect::<Vec<_>>();
    for precision in 0..=9 {
        let unit = match precision {
            0 => TimeUnit::Second, 1..=3 => TimeUnit::Millisecond,
            4..=6 => TimeUnit::Microsecond, _ => TimeUnit::Nanosecond,
        };
        cases.push((format!("DateTime64({precision})"), DataType::Timestamp(unit.clone(), None)));
        cases.push((format!("DateTime64({precision}, 'Asia/Kolkata')"), DataType::Timestamp(unit, Some("Asia/Kolkata".into()))));
    }
    cases
}

fn assert_native_contract(declaration: &str, expected: &DataType, nullable: bool) {
    for policy in [UnsupportedTypePolicy::Fail, UnsupportedTypePolicy::ToString] {
        let column = types::source_column("source_value", declaration, policy).unwrap_or_else(|error| panic!("{declaration}: {error:#}"));
        assert_eq!(&column.data_type, expected, "{declaration}");
        assert_eq!(column.nullable, nullable, "{declaration}");
        assert!(!types::is_string_conversion(&column), "supported {declaration} must remain native even when conversion is enabled");
        assert_eq!(types::source_declaration(&column).as_deref(), Some(declaration));
        assert_eq!(types::wire_declaration(&column).as_deref(), Some(declaration));
        let metadata: serde_json::Value = serde_json::from_str(column.arrow_extension_metadata.as_deref().unwrap()).unwrap();
        assert_eq!(metadata["source_type"], declaration);
        assert_eq!(metadata["conversion"], "native");
    }
}

#[test]
fn native_scalar_family_and_recursive_modifier_matrix_preserves_declared_contracts() {
    use std::sync::Arc;
    for (declaration, data_type) in native_scalar_cases() {
        assert_native_contract(&declaration, &data_type, false);
        assert_native_contract(&format!("Nullable({declaration})"), &data_type, true);
        assert_native_contract(&format!("Array(Nullable({declaration}))"),
            &DataType::List(Arc::new(Field::new("item", data_type.clone(), true))), false);
        assert_native_contract(&format!("Tuple(value Nullable({declaration}), marker UInt8)"),
            &DataType::Struct(vec![Field::new("value", data_type.clone(), true), Field::new("marker", DataType::UInt8, false)].into()), false);
        assert_native_contract(&format!("Map(String, Nullable({declaration}))"),
            &DataType::Map(Arc::new(Field::new("entries", DataType::Struct(vec![
                Field::new("key", DataType::Binary, false), Field::new("value", data_type, true),
            ].into()), false)), false), false);
    }
}

#[test]
fn low_cardinality_scalar_matrix_preserves_dictionary_and_nullability() {
    for (declaration, data_type) in native_scalar_cases().into_iter().filter(|(declaration, _)| {
        matches!(declaration.as_str(), "String" | "FixedString(17)" | "Date" | "DateTime")
    }) {
        let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(data_type));
        assert_native_contract(&format!("LowCardinality({declaration})"), &dictionary, false);
        assert_native_contract(&format!("LowCardinality(Nullable({declaration}))"), &dictionary, true);
        assert_native_contract(&format!("Array(LowCardinality(Nullable({declaration})))"),
            &DataType::List(std::sync::Arc::new(Field::new("item", dictionary, true))), false);
    }
}

#[test]
fn composite_native_families_remain_native_through_nested_shapes() {
    for declaration in [
        "Point", "Ring", "Polygon", "MultiPolygon", "Tuple()",
        "Array(Map(String, Tuple(value Int64, shape Polygon)))",
        "Map(String, Array(Tuple(shape MultiPolygon, label LowCardinality(Nullable(String)))))",
        "Tuple(point Point, rings Array(Ring), map Map(String, DateTime64(9, 'UTC')))",
    ] {
        let original = types::source_column("source_value", declaration, UnsupportedTypePolicy::Fail).unwrap();
        assert!(!original.nullable, "{declaration}");
        assert_native_contract(declaration, &original.data_type, false);
    }
}

#[test]
fn unsupported_family_and_recursive_modifier_matrix_requires_whole_column_opt_in() {
    for declaration in [
        "Int128", "UInt128", "Int256", "UInt256", "UUID", "IPv4", "IPv6",
        "JSON", "JSON(max_dynamic_paths=64)", "Object('json')",
        "AggregateFunction(sum, UInt64)", "SimpleAggregateFunction(sum, UInt64)",
        "Nested(name String, amount Decimal(10,2))", "Time", "Time64(6)",
        "QBit(Float32, 16)", "Variant(String, UInt64)", "Dynamic", "Nothing",
        "FutureTypeV999(Tuple(`value name` String), 42)",
    ] {
        for shape in [
            declaration.to_owned(), format!("Array({declaration})"),
            format!("Tuple(marker UInt8, value {declaration})"),
            format!("Map(String, {declaration})"),
        ] {
            assert!(types::source_column("source_value", &shape, UnsupportedTypePolicy::Fail).is_err(), "{shape}");
            let converted = types::source_column("source_value", &shape, UnsupportedTypePolicy::ToString).unwrap();
            assert_eq!(converted.data_type, DataType::Utf8, "the entire {shape} column is converted");
            assert!(types::is_string_conversion(&converted), "{shape}");
            assert_eq!(types::source_declaration(&converted).as_deref(), Some(shape.as_str()));
            assert_eq!(types::wire_declaration(&converted).as_deref(), Some(if converted.nullable { "Nullable(String)" } else { "String" }));
            let metadata: serde_json::Value = serde_json::from_str(converted.arrow_extension_metadata.as_deref().unwrap()).unwrap();
            assert_eq!(metadata["source_type"], shape);
            assert_eq!(metadata["conversion"], "to_string");
        }
    }
}
