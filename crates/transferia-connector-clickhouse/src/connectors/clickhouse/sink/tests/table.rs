use std::sync::Arc;

use super::*;

fn target_column(type_name: &str) -> anyhow::Result<TargetColumn> {
    target_column_with_metadata(type_name, "", false)
}

fn schema(columns: Vec<SchemaColumn>) -> DatasetSchema {
    DatasetSchema::new(columns)
}

fn merge_tree_ddl(
    name: &str,
    schema: &DatasetSchema,
    sorting_key: &[String],
) -> anyhow::Result<String> {
    create_table_ddl(name, schema, sorting_key, TableEngine::MergeTree)
}

#[test]
fn destination_type_matches_the_physical_clickhouse_ddl_type() -> anyhow::Result<()> {
    let mut column = SchemaColumn::new("name".into(), DataType::Utf8, true);
    column.low_cardinality = true;
    assert_eq!(
        destination_type(&column)?,
        "Nullable(LowCardinality(String))"
    );
    Ok(())
}

#[test]
fn destination_type_preserves_decimal_precision_and_scale() -> anyhow::Result<()> {
    let column = SchemaColumn::new("amount".into(), DataType::Decimal128(20, 6), false);
    assert_eq!(destination_type(&column)?, "Decimal(20, 6)");

    let unsupported = SchemaColumn::new("amount".into(), DataType::Decimal128(20, -1), false);
    assert!(destination_type(&unsupported).is_err());
    Ok(())
}

#[test]
fn ddl_preserves_timestamp_timezone_and_quotes_it() -> anyhow::Result<()> {
    let schema = schema(vec![SchemaColumn::new(
        "created_at".into(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("Europe/Moscow"))),
        true,
    )]);
    assert_eq!(
        merge_tree_ddl("events", &schema, &[])?,
        "CREATE TABLE IF NOT EXISTS `events` (`created_at` Nullable(DateTime64(6, 'Europe/Moscow'))) ENGINE = MergeTree ORDER BY (tuple())"
    );
    assert_eq!(quote_string_literal("db'\\name"), "'db\\'\\\\name'");
    Ok(())
}

#[test]
fn ddl_engine_follows_data_host_count() -> anyhow::Result<()> {
    let schema = schema(vec![SchemaColumn::new(
        "id".into(),
        DataType::UInt64,
        false,
    )]);
    for (data_host_count, expected_engine) in [
        (1, "ENGINE = MergeTree"),
        (2, "ENGINE = ReplicatedMergeTree"),
        (3, "ENGINE = ReplicatedMergeTree"),
    ] {
        let engine = TableEngine::for_data_host_count(data_host_count, false);
        let ddl = create_table_ddl("events", &schema, &[], engine)?;
        assert!(ddl.contains(expected_engine), "{ddl}");
    }
    Ok(())
}

#[test]
fn replicated_table_is_created_on_the_selected_cluster() -> anyhow::Result<()> {
    let schema = schema(vec![SchemaColumn::new(
        "id".into(),
        DataType::UInt64,
        false,
    )]);
    let ddl = create_table_ddl_for_cluster(
        "events",
        &schema,
        &[],
        TableEngine::ReplicatedMergeTree,
        Some("default"),
        false,
    )?;
    assert_eq!(
        ddl,
        "CREATE TABLE IF NOT EXISTS `events` ON CLUSTER `default` (`id` UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}') ORDER BY (tuple())"
    );
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
        merge_tree_ddl("events", &expected, &[])?,
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
    let error = merge_tree_ddl("events", &date32, &[]).unwrap_err();
    assert!(error.to_string().contains("shifts values by 25,567 days"));
}

#[test]
fn date64_requires_an_explicit_parser_conversion() {
    let date64 = schema(vec![SchemaColumn::new(
        "date".into(),
        DataType::Date64,
        false,
    )]);
    let error = merge_tree_ddl("events", &date64, &[]).unwrap_err();
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
    let error = merge_tree_ddl("events", &schema, &["id".into(), "id".into()])
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
    let ddl = merge_tree_ddl("events", &schema, &["kind".into()])?;
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
    assert!(merge_tree_ddl("events,archive", &valid_schema, &[]).is_err());

    let invalid_schema = schema(vec![SchemaColumn::new(
        "nested.value".into(),
        DataType::Int64,
        false,
    )]);
    assert!(merge_tree_ddl("events", &invalid_schema, &[]).is_err());
}

#[test]
fn target_engine_must_match_the_delivery_semantics() -> anyhow::Result<()> {
    validate_target_engine("events", "MergeTree", "MergeTree", false)?;
    validate_target_engine(
        "events",
        "ReplicatedMergeTree",
        "ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')",
        false,
    )?;
    for (engine, engine_full) in [
        (
            "ReplacingMergeTree",
            "ReplacingMergeTree(__data_transfer_commit_time, __data_transfer_is_deleted) ORDER BY id SETTINGS index_granularity = 8192",
        ),
        (
            "ReplicatedReplacingMergeTree",
            "ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}', __data_transfer_commit_time, __data_transfer_is_deleted)",
        ),
    ] {
        validate_target_engine("events", engine, engine_full, true)?;
        assert!(validate_target_engine("events", engine, engine_full, false).is_err());
    }
    for (engine, engine_full) in [
        ("MergeTree", "MergeTree"),
        (
            "ReplicatedMergeTree",
            "ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')",
        ),
    ] {
        assert!(validate_target_engine("events", engine, engine_full, true).is_err());
    }
    for engine in ["SummingMergeTree", "CollapsingMergeTree", "Null", "View"] {
        assert!(validate_target_engine("events", engine, engine, false).is_err());
        assert!(validate_target_engine("events", engine, engine, true).is_err());
    }
    Ok(())
}

#[test]
fn changelog_engine_rejects_wrong_version_and_delete_columns() {
    for engine_full in [
        "ReplacingMergeTree(other_version, __data_transfer_is_deleted)",
        "ReplacingMergeTree(__data_transfer_commit_time, other_delete_flag)",
        "ReplacingMergeTree(__data_transfer_commit_time)",
    ] {
        let error = validate_target_engine(
            "events",
            "ReplacingMergeTree",
            engine_full,
            true,
        )
        .expect_err("an incompatible ReplacingMergeTree must fail before INSERT");
        assert!(error.to_string().contains("incompatible engine definition"));
    }
}

#[test]
fn engine_signature_handles_parentheses_inside_replicated_paths() -> anyhow::Result<()> {
    assert_eq!(
        engine_signature(
            "ReplicatedReplacingMergeTree('/clickhouse/(tables)', '{replica}', version, deleted) ORDER BY id"
        )?,
        "ReplicatedReplacingMergeTree('/clickhouse/(tables)', '{replica}', version, deleted)"
    );
    assert!(engine_signature("ReplacingMergeTree(version, deleted").is_err());
    Ok(())
}

#[test]
fn changelog_ddl_uses_replacing_mergetree_and_lossless_tombstones() -> anyhow::Result<()> {
    let schema = schema(vec![
        SchemaColumn::new("id".into(), DataType::Int64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("value".into(), DataType::Utf8, true),
    ]);
    let ddl = create_table_ddl(
        "events",
        &schema,
        &["id".into()],
        TableEngine::ReplacingMergeTree,
    )?;
    assert!(ddl.contains("`__data_transfer_commit_time` UInt64"), "{ddl}");
    assert!(ddl.contains("`__data_transfer_delete_time` UInt64"), "{ddl}");
    assert!(ddl.contains(
        "`__data_transfer_is_deleted` UInt8 MATERIALIZED if(`__data_transfer_delete_time` != 0, 1, 0)"
    ), "{ddl}");
    assert!(ddl.contains(
        "ENGINE = ReplacingMergeTree(__data_transfer_commit_time, __data_transfer_is_deleted)"
    ), "{ddl}");
    assert!(ddl.ends_with("ORDER BY (`id`)"), "{ddl}");
    Ok(())
}

#[test]
fn changelog_ddl_requires_a_primary_key_and_reserves_metadata_names() {
    let no_key = schema(vec![SchemaColumn::new("value".into(), DataType::Int64, false)]);
    assert!(create_table_ddl(
        "events",
        &no_key,
        &[],
        TableEngine::ReplacingMergeTree,
    )
    .is_err());

    let collision = schema(vec![
        SchemaColumn::new("id".into(), DataType::Int64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new(CHANGE_COMMIT_TIME.into(), DataType::UInt64, false),
    ]);
    assert!(create_table_ddl(
        "events",
        &collision,
        &["id".into()],
        TableEngine::ReplacingMergeTree,
    )
    .is_err());
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
