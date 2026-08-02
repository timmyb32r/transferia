use arrow::array::{Array, BooleanArray, LargeStringArray, StringArray};
use arrow::compute;
use arrow::datatypes::DataType;
use async_trait::async_trait;
use anyhow::anyhow;

use crate::pipeline::middleware::Middleware;
use crate::types::arrow_batch::ArrowBatch;

/// Middleware that keeps only rows where the given string column equals `value`.
///
/// NULL values never pass the filter. Only `Utf8`/`LargeUtf8` columns are
/// supported.
pub struct FilterMiddleware {
    field: String,
    value: String,
}

impl FilterMiddleware {
    pub fn new(field: String, value: String) -> anyhow::Result<Self> {
        if field.is_empty() {
            anyhow::bail!("FilterMiddleware: field must not be empty");
        }
        Ok(Self { field, value })
    }
}

#[async_trait]
impl Middleware for FilterMiddleware {
    async fn process(&self, batch: ArrowBatch) -> anyhow::Result<ArrowBatch> {
        let col_idx = batch
            .batch
            .schema()
            .index_of(&self.field)
            .map_err(|_| anyhow!("FilterMiddleware: column '{}' not found", self.field))?;

        let schema = batch.batch.schema();
        let field_dt = schema.field(col_idx).data_type();
        let mask: Vec<bool> = match field_dt {
            DataType::Utf8 => {
                let arr = batch
                    .batch
                    .column(col_idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow!("downcast to StringArray failed"))?;
                (0..arr.len())
                    .map(|i| !arr.is_null(i) && arr.value(i) == self.value)
                    .collect()
            }
            DataType::LargeUtf8 => {
                let arr = batch
                    .batch
                    .column(col_idx)
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| anyhow!("downcast to LargeStringArray failed"))?;
                (0..arr.len())
                    .map(|i| !arr.is_null(i) && arr.value(i) == self.value)
                    .collect()
            }
            other => anyhow::bail!(
                "FilterMiddleware: column '{}' is {:?}, only Utf8/LargeUtf8 supported",
                self.field,
                other
            ),
        };

        let mask_array = BooleanArray::from(mask);
        let filtered = compute::filter_record_batch(&batch.batch, &mask_array)?;

        // CONTRACT: preserve meta
        Ok(ArrowBatch {
            batch: filtered,
            meta: batch.meta.clone(),
        })
    }
}
