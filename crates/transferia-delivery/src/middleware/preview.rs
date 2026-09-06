//! Read-only preview of the same Arrow middleware sequence used by delivery.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use tokio_util::sync::CancellationToken;
use transferia_core::TableData;
use transferia_delivery_contracts::middleware::{Middleware, MiddlewarePreviewContext};

pub struct TransformPreview {
    pub before: TableData,
    pub after: TableData,
    pub applied: bool,
}

/// No source, sink, destination preparation, or JSON conversion is performed
/// here. Empty batches retain their Arrow schema between every step.
pub async fn preview_chain(
    middlewares: &[Box<dyn Middleware>],
    mut input: TableData,
    through_step: usize,
    context: MiddlewarePreviewContext,
    cancellation: CancellationToken,
) -> anyhow::Result<TransformPreview> {
    anyhow::ensure!(through_step < middlewares.len(), "preview step index is out of range");
    anyhow::ensure!(!input.is_dlq, "transform preview requires a main dataset");
    anyhow::ensure!(context.memory_limit_bytes > 0, "preview memory_limit_bytes must be positive");
    anyhow::ensure!(input.batch.get_array_memory_size() <= context.memory_limit_bytes,
        "preview input exceeds memory_limit_bytes");
    let mut before = None;
    let mut applied = false;
    for (index, middleware) in middlewares.iter().enumerate().take(through_step + 1) {
        if index == through_step {
            applied = middleware.applies_to(input.namespace.as_deref(), &input.table);
            before = Some(input.clone());
        }
        let operation = async {
            // Execute the native Arrow input, including its complete metadata.
            // Delivery startup owns output_dataset validation; reconstructing
            // it from this sample would invent or lose source constraints.
            let output = middleware.preview(input, context).await.context(
                "table-row preview contains source table columns only; synthetic transport and CDC metadata are not available"
            )?;
            anyhow::ensure!(!output.is_dlq, "transform unexpectedly changed the dataset role");
            Ok(output)
        };
        input = tokio::select! {
            biased;
            () = cancellation.cancelled() => anyhow::bail!("transform preview cancelled"),
            result = operation => result,
        }.with_context(|| format!("transform step {}", index + 1))?;
    }
    Ok(TransformPreview {
        before: before.context("preview step was not executed")?,
        after: input,
        applied,
    })
}

/// Display is deliberately separate from data execution. Every non-null cell
/// is Arrow-formatted text, not a JSON number; exact integers, decimals and
/// temporal values therefore cannot be narrowed by the browser. Column types
/// accompany this display in the HTTP response.
pub fn display_rows(data: &TableData) -> anyhow::Result<Vec<BTreeMap<String, Option<String>>>> {
    let schema = data.batch.schema();
    let mut names = BTreeSet::new();
    for field in schema.fields() {
        anyhow::ensure!(names.insert(field.name()), "preview cannot display duplicate column name {:?}", field.name());
    }
    let options = FormatOptions::default().with_display_error(true);
    let formatters = data.batch.columns().iter().map(|array| {
        ArrayFormatter::try_new(array.as_ref(), &options)
    }).collect::<Result<Vec<_>, _>>()?;
    (0..data.batch.num_rows()).map(|row| {
        let mut values = BTreeMap::new();
        for ((field, array), formatter) in schema.fields().iter().zip(data.batch.columns()).zip(&formatters) {
            let value = if array.is_null(row) {
                None
            } else {
                Some(formatter.value(row).try_to_string()
                    .with_context(|| format!("preview column {:?}, row {}", field.name(), row + 1))?)
            };
            values.insert(field.name().clone(), value);
        }
        Ok(values)
    }).collect()
}

#[cfg(test)]
#[path = "tests/preview.rs"]
mod tests;
