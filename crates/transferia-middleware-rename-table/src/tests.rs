use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use transferia_core::{DatasetRole, DiscoveredDataset, SchemaColumn, SystemColumns};

use super::*;

fn regex(pattern: &str, replacement: &str) -> anyhow::Result<RenameTableMiddleware> {
    RenameTableMiddleware::new(RenameTableConfig::Regex {
        pattern: pattern.into(), replacement: replacement.into(),
    })
}

fn dataset(name: &str) -> DiscoveredDataset {
    let mut column = SchemaColumn::new("value".into(), DataType::Utf8, false);
    column.primary_key = true;
    column.arrow_extension_name = Some("test.extension");
    column.arrow_extension_metadata = Some("exact metadata".into());
    let schema = DatasetSchema::new(vec![column]);
    DiscoveredDataset {
        namespace: Some(Arc::from("schema.with.dot")), name: Arc::from(name),
        role: DatasetRole::Main,
        update_policy: transferia_core::delivery::UpdatePolicy::Strict,
        incoming_schema: schema.clone(), stored_schema: schema, system_columns: Vec::new(),
    }
}

fn data(name: &str) -> anyhow::Result<TableData> {
    let schema = Schema::new(vec![Field::new("value", DataType::Utf8, false)
        .with_metadata([("custom".into(), "preserved".into())].into())]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(StringArray::from(vec!["original"]))])?;
    Ok(TableData::new(Arc::from(name), false, batch, SystemColumns::default())
        .with_namespace(Arc::from("schema.with.dot")))
}

#[tokio::test]
async fn exact_and_regex_project_the_same_identity_at_catalog_startup_and_runtime() -> anyhow::Result<()> {
    for middleware in [
        RenameTableMiddleware::new(RenameTableConfig::Exact { name: "  archive.$\\🦀  ".into() })?,
        regex("^raw_(.*)$", "archive_${1}")?,
    ] {
        let input = dataset("raw_events");
        let expected = middleware.output_table_name(input.namespace.as_deref(), &input.name)?;
        let projected = middleware.output_dataset(&input).await?;
        let original = data("raw_events")?;
        let batch = original.batch.clone();
        let result = middleware.process(original).await?;
        assert_eq!(projected.name, expected);
        assert_eq!(result.table, expected);
        assert_eq!(projected.namespace, input.namespace);
        assert_eq!(result.namespace, input.namespace);
        assert!(Arc::ptr_eq(result.batch.column(0), batch.column(0)), "rename must not copy or mutate data");
        assert!(Arc::ptr_eq(&result.batch.schema(), &batch.schema()));
        assert!(projected.stored_schema.columns[0].primary_key);
        assert_eq!(projected.stored_schema.columns[0].arrow_extension_metadata.as_deref(), Some("exact metadata"));
        assert_eq!(projected.incoming_schema.columns[0].arrow_extension_name, Some("test.extension"));
    }
    Ok(())
}

#[test]
fn regex_replaces_all_matches_with_strict_captures_and_literal_dollars() -> anyhow::Result<()> {
    for (pattern, replacement, input, expected) in [
        ("raw", "archive", "raw_raw", "archive_archive"),
        ("^raw_(?<table>.*)$", "${table}_$$$1", "raw_события", "события_$события"),
        ("^raw_(.*)$", "${1}_new", "raw_events", "events_new"),
        ("old", "", "old_events", "_events"),
        ("^", "prefix_", "events", "prefix_events"),
        ("^raw_", "archive_", "events", "events"),
    ] {
        assert_eq!(&*regex(pattern, replacement)?.output_table_name(None, input)?, expected);
    }
    Ok(())
}

#[test]
fn invalid_configuration_fails_before_discovery() {
    for name in ["", " \t\n", "events\0suffix"] {
        assert!(RenameTableMiddleware::new(RenameTableConfig::Exact { name: name.into() }).is_err());
    }
    for (pattern, replacement) in [
        ("", "new"), ("[", "new"), ("(.*)", "$2"), ("(.*)", "${missing}"),
        ("(.*)", "${1"), ("(.*)", "$"), ("(.*)", "new\0name"), ("(.*)", "$1_suffix"),
    ] {
        assert!(regex(pattern, replacement).is_err(), "{pattern:?} / {replacement:?}");
    }
}

#[tokio::test]
async fn invalid_data_dependent_names_fail_at_startup_and_runtime() -> anyhow::Result<()> {
    for middleware in [regex("^.*$", "")?, regex("^(old_)?(events)$", "$1")?] {
        assert!(middleware.output_table_name(None, "events").is_err());
        assert!(middleware.output_dataset(&dataset("events")).await.is_err());
        assert!(middleware.process(data("events")?).await.is_err());
    }
    Ok(())
}

#[test]
fn registration_exposes_both_typed_modes_and_rejects_mixed_or_unknown_fields() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    register(&mut builder)?;
    let registry = builder.build();
    assert_eq!(registry.middleware_definitions()[0].key, "rename_table");
    for yaml in ["mode: exact\nname: events", "mode: regex\npattern: '^raw_(.*)$'\nreplacement: '${1}'"] {
        registry.build_middleware("rename_table", serde_yaml::from_str(yaml)?)?;
    }
    for yaml in ["mode: exact", "mode: exact\nname: events\npattern: x", "mode: regex\npattern: x", "mode: unknown\nname: events"] {
        assert!(registry.build_middleware("rename_table", serde_yaml::from_str(yaml)?).is_err());
    }
    Ok(())
}
