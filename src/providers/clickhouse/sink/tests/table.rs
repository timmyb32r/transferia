use std::sync::Arc;

use super::*;

fn target_column(type_name: &str) -> anyhow::Result<TargetColumn> {
    target_column_with_metadata(type_name, "", false)
}

fn schema(columns: Vec<SchemaColumn>) -> DatasetSchema {
    DatasetSchema::new(columns)
}

#[test]
fn ddl_preserves_timestamp_timezone_and_quotes_it() -> anyhow::Result<()> {
    let schema = schema(vec![SchemaColumn::new(
        "created_at".into(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("Europe/Moscow"))),
        true,
    )]);
    assert_eq!(
        create_table_ddl("events", &schema, &[])?,
        "CREATE TABLE IF NOT EXISTS `events` (`created_at` Nullable(DateTime64(6, 'Europe/Moscow'))) ENGINE = MergeTree ORDER BY (tuple())"
    );
    assert_eq!(quote_string_literal("db'\\name"), "'db\\'\\\\name'");
    Ok(())
}

#[test]
fn target_schema_allows_extra_and_more_nullable_columns() -> anyhow::Result<()> {
    let expected = schema(vec![
        SchemaColumn::new("id".into(), DataType::Int64, false),
        SchemaColumn::new("name".into(), DataType::Utf8, true),
        SchemaColumn::new("enabled".into(), DataType::Boolean, false),
    ]);
    let target = HashMap::from([
        ("id".into(), target_column("Nullable(Int64)")?),
        ("name".into(), target_column("Nullable(String)")?),
        ("enabled".into(), target_column("Bool")?),
        ("extra".into(), target_column("String")?),
    ]);
    validate_target_schema("events", &expected, &target, &[])
}

#[test]
fn target_schema_rejects_missing_type_and_nullability_mismatches() -> anyhow::Result<()> {
    let expected = schema(vec![SchemaColumn::new(
        "value".into(),
        DataType::Int64,
        true,
    )]);
    assert!(validate_target_schema("events", &expected, &HashMap::new(), &[]).is_err());

    let wrong_type = HashMap::from([("value".into(), target_column("String")?)]);
    assert!(validate_target_schema("events", &expected, &wrong_type, &[]).is_err());

    let non_nullable = HashMap::from([("value".into(), target_column("Int64")?)]);
    assert!(validate_target_schema("events", &expected, &non_nullable, &[]).is_err());

    let date_schema = schema(vec![SchemaColumn::new(
        "date".into(),
        DataType::Date32,
        false,
    )]);
    let narrow_date = HashMap::from([("date".into(), target_column("Date")?)]);
    assert!(validate_target_schema("events", &date_schema, &narrow_date, &[]).is_err());
    Ok(())
}

#[test]
fn target_schema_checks_datetime_precision_and_timezone() -> anyhow::Result<()> {
    let expected = schema(vec![SchemaColumn::new(
        "ts".into(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("Europe/Moscow"))),
        false,
    )]);
    let matching = HashMap::from([(
        "ts".into(),
        target_column("DateTime64(6, 'Europe/Moscow')")?,
    )]);
    validate_target_schema("events", &expected, &matching, &[])?;

    let wrong_precision = HashMap::from([(
        "ts".into(),
        target_column("DateTime64(3, 'Europe/Moscow')")?,
    )]);
    assert!(validate_target_schema("events", &expected, &wrong_precision, &[]).is_err());

    let wrong_timezone = HashMap::from([("ts".into(), target_column("DateTime64(6, 'UTC')")?)]);
    assert!(validate_target_schema("events", &expected, &wrong_timezone, &[]).is_err());
    Ok(())
}

#[test]
fn seconds_use_signed_datetime64_and_reject_lossy_datetime() -> anyhow::Result<()> {
    let expected = schema(vec![SchemaColumn::new(
        "ts".into(),
        DataType::Timestamp(TimeUnit::Second, None),
        false,
    )]);
    assert_eq!(
        create_table_ddl("events", &expected, &[])?,
        "CREATE TABLE IF NOT EXISTS `events` (`ts` DateTime64(0)) ENGINE = MergeTree ORDER BY (tuple())"
    );

    let signed = HashMap::from([("ts".into(), target_column("DateTime64(0)")?)]);
    validate_target_schema("events", &expected, &signed, &[])?;
    let unsigned = HashMap::from([("ts".into(), target_column("DateTime")?)]);
    assert!(validate_target_schema("events", &expected, &unsigned, &[]).is_err());
    Ok(())
}

#[test]
fn date32_is_rejected_before_table_creation() {
    let date32 = schema(vec![SchemaColumn::new(
        "date".into(),
        DataType::Date32,
        false,
    )]);
    let error = create_table_ddl("events", &date32, &[]).unwrap_err();
    assert!(error.to_string().contains("shifts values by 25,567 days"));
}

#[test]
fn date64_requires_an_explicit_parser_conversion() {
    let date64 = schema(vec![SchemaColumn::new(
        "date".into(),
        DataType::Date64,
        false,
    )]);
    let error = create_table_ddl("events", &date64, &[]).unwrap_err();
    assert!(error.to_string().contains("explicit configured conversion"));
}

#[test]
fn target_schema_rejects_generated_input_columns_and_wrong_sorting_set() -> anyhow::Result<()> {
    let expected = schema(vec![SchemaColumn::new("id".into(), DataType::Int64, false)]);
    let materialized = HashMap::from([(
        "id".into(),
        target_column_with_metadata("Int64", "MATERIALIZED", true)?,
    )]);
    assert!(validate_target_schema("events", &expected, &materialized, &["id".into()]).is_err());

    let sorted = HashMap::from([("id".into(), target_column_with_metadata("Int64", "", true)?)]);
    validate_target_schema("events", &expected, &sorted, &["id".into()])?;
    assert!(validate_target_schema("events", &expected, &sorted, &[]).is_err());
    Ok(())
}

#[test]
fn ddl_rejects_duplicate_sorting_columns() {
    let schema = schema(vec![SchemaColumn::new("id".into(), DataType::Int64, false)]);
    let error = create_table_ddl("events", &schema, &["id".into(), "id".into()])
        .expect_err("duplicate sorting columns must fail");
    assert!(error.to_string().contains("duplicate column 'id'"));
}

#[test]
fn ddl_materializes_low_cardinality_metadata() -> anyhow::Result<()> {
    let schema = DatasetSchema::new(vec![SchemaColumn::new(
        "kind".into(),
        DataType::Utf8,
        false,
    )
    .with_constraints(true, true, Some(32))]);
    let ddl = create_table_ddl("events", &schema, &["kind".into()])?;
    assert!(ddl.contains("`kind` LowCardinality(String)"), "{ddl}");
    assert!(ddl.contains("ORDER BY (`kind`)"), "{ddl}");
    Ok(())
}

#[test]
fn target_schema_rejects_low_cardinality_drift() -> anyhow::Result<()> {
    let expected = DatasetSchema::new(vec![SchemaColumn::new(
        "kind".into(),
        DataType::Utf8,
        false,
    )
    .with_constraints(false, true, None)]);
    let mut target = HashMap::new();
    target.insert(
        "kind".into(),
        target_column_with_metadata("String", "", false)?,
    );
    let error = validate_target_schema("events", &expected, &target, &[])
        .expect_err("plain String must not satisfy LowCardinality discovery");
    assert!(error.to_string().contains("LowCardinality"));
    Ok(())
}

#[test]
fn ddl_rejects_identifiers_outside_the_canonical_ascii_subset() {
    let valid_schema = schema(vec![SchemaColumn::new(
        "value".into(),
        DataType::Int64,
        false,
    )]);
    assert!(create_table_ddl("events,archive", &valid_schema, &[]).is_err());

    let invalid_schema = schema(vec![SchemaColumn::new(
        "nested.value".into(),
        DataType::Int64,
        false,
    )]);
    assert!(create_table_ddl("events", &invalid_schema, &[]).is_err());
}

#[test]
fn only_row_preserving_mergetree_engines_are_accepted() -> anyhow::Result<()> {
    for engine in ["MergeTree", "ReplicatedMergeTree"] {
        validate_target_engine("events", engine)?;
    }
    for engine in [
        "ReplacingMergeTree",
        "SummingMergeTree",
        "CollapsingMergeTree",
        "AggregatingMergeTree",
        "Null",
        "Memory",
        "Buffer",
        "View",
        "MaterializedView",
    ] {
        let error = validate_target_engine("events", engine).unwrap_err();
        assert!(error
            .to_string()
            .contains("expected exactly MergeTree or ReplicatedMergeTree"));
    }
    Ok(())
}

#[test]
fn sorting_key_requires_plain_columns_in_configured_order() -> anyhow::Result<()> {
    validate_sorting_key("events", &[], "")?;
    validate_sorting_key("events", &[], "tuple()")?;
    validate_sorting_key("events", &["id".into(), "ts".into()], "id, ts")?;
    validate_sorting_key("events", &["id".into(), "ts".into()], "(`id`, `ts`)")?;
    assert!(validate_sorting_key("events", &["id".into(), "ts".into()], "ts, id").is_err());
    assert!(validate_sorting_key("events", &["id".into()], "toDate(id)").is_err());
    Ok(())
}
