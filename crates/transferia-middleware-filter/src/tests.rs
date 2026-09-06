use arrow::datatypes::DataType;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_delivery_contracts::middleware::Middleware as _;
use transferia_registry::RegistryBuilder;

use super::{register, FilterMiddleware};

#[tokio::test]
async fn validates_the_selected_column_before_execution() -> anyhow::Result<()> {
    let schema = DatasetSchema::new(vec![SchemaColumn::new(
        "kind".into(),
        DataType::Utf8,
        false,
    )]);
    let filter = FilterMiddleware::new("kind".into(), "event".into())?;

    assert_eq!(filter.output_schema(&schema).await?.columns.len(), 1);

    let missing = FilterMiddleware::new("missing".into(), "event".into())?;
    assert!(missing.output_schema(&schema).await.is_err());
    Ok(())
}

#[test]
fn registration_exposes_a_typed_factory() -> anyhow::Result<()> {
    let mut builder = RegistryBuilder::new();
    register(&mut builder)?;
    let registry = builder.build();

    assert_eq!(registry.middleware_definitions()[0].key, "filter");
    registry.build_middleware("filter", serde_yaml::from_str("field: kind\nvalue: event")?)?;
    assert!(registry
        .build_middleware("filter", serde_yaml::from_str("field: kind")?)
        .is_err());
    Ok(())
}

#[tokio::test]
async fn resolves_the_column_for_each_table_schema() -> anyhow::Result<()> {
    use std::sync::Arc;
    use arrow::array::StringArray;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use transferia_core::{TableData, SystemColumns};

    let filter = FilterMiddleware::new("kind".into(), "event".into())?;
    for fields in [["kind", "other"], ["other", "kind"]] {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(fields.map(|name| Field::new(name, DataType::Utf8, false)).to_vec())),
            fields.iter().map(|name| {
                Arc::new(StringArray::from(vec![if *name == "kind" { "event" } else { "ignored" }])) as _
            }).collect(),
        )?;
        let output = filter.process(TableData::new(Arc::from("events"), false, batch, SystemColumns::default())
            .with_namespace(Arc::from("public"))).await?;
        assert_eq!(output.batch.num_rows(), 1, "field order: {fields:?}");
        assert_eq!(output.namespace.as_deref(), Some("public"));
    }
    Ok(())
}
