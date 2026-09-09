use alloc::sync::Arc;

use async_trait::async_trait;

use transferia_core::data::schema::DatasetSchema;
use transferia_core::data::table_data::TableData;
use transferia_core::DiscoveredDataset;

/// Explicit per-request resource budget for interactive table-row previews.
/// The production delivery path continues to use its own configured resources.
#[derive(Clone, Copy, Debug)]
pub struct MiddlewarePreviewContext {
    pub memory_limit_bytes: usize,
}

/// The Middleware trait transforms a `TableData` into another `TableData`.
///
/// Implementations filter, enrich, or otherwise transform the batch. The
/// result carries the same `table` / `is_dlq` unless the
/// implementation intentionally changes them.
///
/// **Contract:** middleware implementations are only called for **non-DLQ**
/// batches (`is_dlq == false`). DLQ short-circuits in `pipeline::apply_middlewares`
/// before reaching any implementation — implementations do not need to handle
/// DLQ schema mismatches.
#[async_trait]
pub trait Middleware: Send + Sync {
    /// Execute the production transform semantics within the preview budget.
    /// Engines with their own allocator must override this to configure that
    /// allocator before executing, rather than checking only the final output.
    async fn preview(
        &self,
        data: TableData,
        context: MiddlewarePreviewContext,
    ) -> anyhow::Result<TableData> {
        anyhow::ensure!(
            context.memory_limit_bytes > 0,
            "preview memory_limit_bytes must be positive"
        );
        anyhow::ensure!(
            data.batch.get_array_memory_size() <= context.memory_limit_bytes,
            "preview input exceeds memory_limit_bytes"
        );
        let output = self.process(data).await?;
        anyhow::ensure!(
            output.batch.get_array_memory_size() <= context.memory_limit_bytes,
            "preview output exceeds memory_limit_bytes"
        );
        Ok(output)
    }
    /// Whether the step applies to the current input identity. Sequential steps
    /// deliberately may overlap; exclusion affects only this step.
    fn applies_to(&self, _namespace: Option<&str>, _name: &str) -> bool {
        true
    }

    /// Schema-independent identity projection for catalog matching. Identity
    /// transforms must use the same semantics at preparation and runtime.
    fn output_table_identity(&self, namespace: Option<&str>, name: &str) -> anyhow::Result<(Option<Arc<str>>, Arc<str>)> {
        Ok((namespace.map(Arc::from), Arc::from(name)))
    }

    /// Validate each in-scope identity before reading any preview samples.
    /// The caller supplies the entire selected catalog, not just the sample.
    fn requires_preview_identity_validation(&self) -> bool {
        false
    }

    fn validate_preview_identity(&self, _namespace: Option<&str>, _name: &str) -> anyhow::Result<()> {
        Ok(())
    }

    /// Project the current identity and schema before destination preparation.
    /// Identity-changing implementations must make the identical change in
    /// `process`, so the next step observes the same input at both boundaries.
    async fn output_dataset(
        &self,
        dataset: &DiscoveredDataset,
    ) -> anyhow::Result<DiscoveredDataset> {
        let mut output = dataset.clone();
        (output.namespace, output.name) = self.output_table_identity(dataset.namespace.as_deref(), &dataset.name)?;
        output.stored_schema = self.output_schema(&dataset.stored_schema).await?;
        // Compare the actual post-transform sink input, not a stale source
        // schema, when subsequent renames combine otherwise different inputs.
        output.incoming_schema = if dataset.incoming_schema == dataset.stored_schema {
            output.stored_schema.clone()
        } else {
            self.output_schema(&dataset.incoming_schema).await?
        };
        Ok(output)
    }

    /// Validate this transform against the discovered main-dataset schema.
    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema>;

    /// Transform a batch into a (possibly filtered/transformed) batch.
    async fn process(&self, data: TableData) -> anyhow::Result<TableData>;
}

// ---------------------------------------------------------------------------
// Blanket impls
// ---------------------------------------------------------------------------

#[async_trait]
impl<T: Middleware + ?Sized> Middleware for &T {
    fn requires_preview_identity_validation(&self) -> bool {
        (**self).requires_preview_identity_validation()
    }
    fn output_table_identity(&self, namespace: Option<&str>, name: &str) -> anyhow::Result<(Option<Arc<str>>, Arc<str>)> {
        (**self).output_table_identity(namespace, name)
    }
    fn validate_preview_identity(&self, namespace: Option<&str>, name: &str) -> anyhow::Result<()> {
        (**self).validate_preview_identity(namespace, name)
    }
    async fn preview(
        &self,
        data: TableData,
        context: MiddlewarePreviewContext,
    ) -> anyhow::Result<TableData> {
        (**self).preview(data, context).await
    }
    fn applies_to(&self, namespace: Option<&str>, name: &str) -> bool {
        (**self).applies_to(namespace, name)
    }

    async fn output_dataset(
        &self,
        dataset: &DiscoveredDataset,
    ) -> anyhow::Result<DiscoveredDataset> {
        (**self).output_dataset(dataset).await
    }

    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
        (**self).output_schema(schema).await
    }

    async fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        (**self).process(data).await
    }
}

#[async_trait]
impl<T: Middleware + Send + Sync + ?Sized> Middleware for Box<T> {
    fn requires_preview_identity_validation(&self) -> bool {
        (**self).requires_preview_identity_validation()
    }
    fn output_table_identity(&self, namespace: Option<&str>, name: &str) -> anyhow::Result<(Option<Arc<str>>, Arc<str>)> {
        (**self).output_table_identity(namespace, name)
    }
    fn validate_preview_identity(&self, namespace: Option<&str>, name: &str) -> anyhow::Result<()> {
        (**self).validate_preview_identity(namespace, name)
    }
    async fn preview(
        &self,
        data: TableData,
        context: MiddlewarePreviewContext,
    ) -> anyhow::Result<TableData> {
        (**self).preview(data, context).await
    }
    fn applies_to(&self, namespace: Option<&str>, name: &str) -> bool {
        (**self).applies_to(namespace, name)
    }

    async fn output_dataset(
        &self,
        dataset: &DiscoveredDataset,
    ) -> anyhow::Result<DiscoveredDataset> {
        (**self).output_dataset(dataset).await
    }

    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
        (**self).output_schema(schema).await
    }

    async fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        (**self).process(data).await
    }
}

#[async_trait]
impl<T: Middleware + ?Sized> Middleware for Arc<T> {
    fn requires_preview_identity_validation(&self) -> bool {
        (**self).requires_preview_identity_validation()
    }
    fn output_table_identity(&self, namespace: Option<&str>, name: &str) -> anyhow::Result<(Option<Arc<str>>, Arc<str>)> {
        (**self).output_table_identity(namespace, name)
    }
    fn validate_preview_identity(&self, namespace: Option<&str>, name: &str) -> anyhow::Result<()> {
        (**self).validate_preview_identity(namespace, name)
    }
    async fn preview(
        &self,
        data: TableData,
        context: MiddlewarePreviewContext,
    ) -> anyhow::Result<TableData> {
        (**self).preview(data, context).await
    }
    fn applies_to(&self, namespace: Option<&str>, name: &str) -> bool {
        (**self).applies_to(namespace, name)
    }

    async fn output_dataset(
        &self,
        dataset: &DiscoveredDataset,
    ) -> anyhow::Result<DiscoveredDataset> {
        (**self).output_dataset(dataset).await
    }

    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
        (**self).output_schema(schema).await
    }

    async fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        (**self).process(data).await
    }
}
