use super::*;

#[test]
fn decodes_an_opaque_middleware_entry_without_owning_its_implementation() -> anyhow::Result<()> {
    let entry: MiddlewareEntry =
        serde_yaml::from_str("filter:\n  field: event_name\n  value: page_view\n")?;
    anyhow::ensure!(entry.kind()? == "filter");
    anyhow::ensure!(entry.raw()?["field"] == "event_name");
    Ok(())
}

#[test]
fn rejects_multiple_middleware_kinds() -> anyhow::Result<()> {
    let entry: MiddlewareEntry = serde_yaml::from_str("first: {}\nsecond: {}\n")?;
    anyhow::ensure!(entry.kind().is_err());
    Ok(())
}

#[test]
fn table_scope_is_not_interpreted_as_a_second_action() -> anyhow::Result<()> {
    let entry: MiddlewareEntry = serde_yaml::from_str(
        "tables:\n  include: public.reports_*\n  exclude: public.reports_test\nfilter:\n  field: kind\n  value: event\n",
    )?;
    assert_eq!(entry.kind()?, "filter");
    Ok(())
}

fn test_dataset(namespace: Option<&str>, name: &str) -> transferia_core::DiscoveredDataset {
    transferia_core::DiscoveredDataset {
        namespace: namespace.map(std::sync::Arc::from),
        name: std::sync::Arc::from(name),
        role: transferia_core::DatasetRole::Main,
        update_policy: transferia_core::delivery::UpdatePolicy::Strict,
        incoming_schema: transferia_core::DatasetSchema::default(),
        stored_schema: transferia_core::DatasetSchema::default(),
        system_columns: Vec::new(),
    }
}

struct RenameTestTable;

#[async_trait::async_trait]
impl Middleware for RenameTestTable {
    async fn output_schema(&self, schema: &transferia_core::DatasetSchema) -> anyhow::Result<transferia_core::DatasetSchema> {
        Ok(schema.clone())
    }

    async fn output_dataset(&self, dataset: &transferia_core::DiscoveredDataset) -> anyhow::Result<transferia_core::DiscoveredDataset> {
        let mut output = dataset.clone();
        output.name = std::sync::Arc::from("renamed");
        Ok(output)
    }

    async fn process(&self, mut data: transferia_core::TableData) -> anyhow::Result<transferia_core::TableData> {
        data.table = std::sync::Arc::from("renamed");
        Ok(data)
    }
}

struct RejectTestTable;

#[async_trait::async_trait]
impl Middleware for RejectTestTable {
    async fn output_schema(&self, _: &transferia_core::DatasetSchema) -> anyhow::Result<transferia_core::DatasetSchema> {
        anyhow::bail!("selected table was validated")
    }

    async fn process(&self, _: transferia_core::TableData) -> anyhow::Result<transferia_core::TableData> {
        anyhow::bail!("selected table was processed")
    }
}

fn scoped(include: &str, exclude: Option<&str>, action: Box<dyn Middleware>) -> ScopedMiddleware {
    ScopedMiddleware {
        tables: TableRule {
            include: include.into(),
            exclude: exclude.map(str::to_owned),
            include_mode: transferia_registry::table_selection::PatternMode::Glob,
            exclude_mode: transferia_registry::table_selection::PatternMode::Glob,
        }.compile().unwrap(),
        action,
    }
}

fn empty_batch(namespace: Option<&str>, name: &str) -> transferia_core::TableData {
    let mut data = transferia_core::TableData::new(
        std::sync::Arc::from(name), false,
        arrow::record_batch::RecordBatch::new_empty(std::sync::Arc::new(arrow::datatypes::Schema::empty())),
        transferia_core::SystemColumns::default(),
    );
    data.namespace = namespace.map(std::sync::Arc::from);
    data
}

#[tokio::test]
async fn excludes_skip_both_schema_validation_and_runtime_processing() -> anyhow::Result<()> {
    let step = scoped("public.*", Some("public.ignored"), Box::new(RejectTestTable));
    for (namespace, name) in [(Some("public"), "ignored"), (Some("other"), "events")] {
        assert_eq!(step.output_dataset(&test_dataset(namespace, name)).await?.name.as_ref(), name);
        assert_eq!(step.process(empty_batch(namespace, name)).await?.table.as_ref(), name);
    }
    assert!(step.output_dataset(&test_dataset(Some("public"), "events")).await.is_err());
    assert!(step.process(empty_batch(Some("public"), "events")).await.is_err());
    Ok(())
}

#[tokio::test]
async fn subsequent_scope_uses_current_name_after_previous_step() -> anyhow::Result<()> {
    let rename = scoped("public.original", None, Box::new(RenameTestTable));
    let selected = scoped("public.renamed", None, Box::new(RejectTestTable));
    let output = rename.output_dataset(&test_dataset(Some("public"), "original")).await?;
    assert!(selected.output_dataset(&output).await.is_err());
    let output = rename.process(empty_batch(Some("public"), "original")).await?;
    assert!(selected.process(output).await.is_err());
    Ok(())
}

#[test]
fn literal_dots_never_supply_a_missing_namespace() {
    let namespaced = scoped("public.events", None, Box::new(RejectTestTable));
    assert!(namespaced.applies_to(Some("public"), "events"));
    assert!(!namespaced.applies_to(None, "public.events"));
    let unqualified = scoped("events", None, Box::new(RejectTestTable));
    assert!(unqualified.applies_to(None, "events"));
    assert!(!unqualified.applies_to(Some("public"), "events"));
}

#[tokio::test]
async fn builder_preserves_order_and_allows_overlapping_steps() -> anyhow::Result<()> {
    let mut builder = transferia_registry::RegistryBuilder::new();
    builder.register_middleware(transferia_registry::MiddlewareRegistration::new::<serde_json::Value, _, _>(
        "rename_test", "Rename test", || serde_json::json!({}),
        |_| Ok(Box::new(RenameTestTable)),
    )?)?;
    builder.register_middleware(transferia_registry::MiddlewareRegistration::new::<serde_json::Value, _, _>(
        "reject_test", "Reject test", || serde_json::json!({}),
        |_| Ok(Box::new(RejectTestTable)),
    )?)?;
    let entries: Vec<MiddlewareEntry> = serde_yaml::from_str("- rename_test: {}\n- tables:\n    include: public.renamed\n  reject_test: {}\n")?;
    let middlewares = build_middlewares(&builder.build(), &entries)?;
    let output = middlewares[0].process(empty_batch(Some("public"), "original")).await?;
    assert!(middlewares[1].process(output).await.is_err());
    Ok(())
}

#[test]
fn invalid_scope_fails_before_constructing_an_action() -> anyhow::Result<()> {
    let registry = transferia_registry::RegistryBuilder::new().build();
    for scope in ["include: ''", "include: '['\n  include_mode: regex"] {
        let entry: MiddlewareEntry = serde_yaml::from_str(&format!("tables:\n  {scope}\nmissing_action: {{}}\n"))?;
        let error = entry.build(&registry).err().unwrap().to_string();
        assert!(error.contains("Invalid table rule"), "{error}");
    }
    Ok(())
}
