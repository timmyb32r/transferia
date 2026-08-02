use std::sync::OnceLock;

use arrow::array::Scalar;
use arrow::compute;
use arrow::compute::kernels::cmp::eq;
use arrow::datatypes::DataType;
use anyhow::anyhow;

use crate::pipeline::middleware::Middleware;
use crate::types::arrow_batch::ArrowBatch;

/// Middleware that keeps only rows where the given string column equals `value`.
///
/// NULL values never pass. Only `Utf8`/`LargeUtf8` supported.
/// Scalar arrays cached via `OnceLock` — no per-batch allocation, only Arc-bumps.
pub struct FilterMiddleware {
    field: String,
    value: String,
    scalar_utf8: OnceLock<arrow::array::StringArray>,
    scalar_large_utf8: OnceLock<arrow::array::LargeStringArray>,
}

impl FilterMiddleware {
    pub fn new(field: String, value: String) -> anyhow::Result<Self> {
        if field.is_empty() {
            anyhow::bail!("FilterMiddleware: field must not be empty");
        }
        Ok(Self {
            field, value,
            scalar_utf8: OnceLock::new(),
            scalar_large_utf8: OnceLock::new(),
        })
    }
}

impl Middleware for FilterMiddleware {
    fn process(&self, batch: ArrowBatch) -> anyhow::Result<ArrowBatch> {
        let schema = batch.batch.schema();
        let col_idx = schema
            .index_of(&self.field)
            .map_err(|_| anyhow!("FilterMiddleware: column '{}' not found", self.field))?;

        let field_dt = schema.field(col_idx).data_type();
        let col = batch.batch.column(col_idx);

        let mask = match field_dt {
            DataType::Utf8 => {
                let scalar_arr = self.scalar_utf8.get_or_init(|| {
                    arrow::array::StringArray::from(vec![self.value.as_str()])
                });
                let lhs: &dyn arrow::array::Datum = col;
                eq(lhs, &Scalar::new(scalar_arr.clone()))?
            }
            DataType::LargeUtf8 => {
                let scalar_arr = self.scalar_large_utf8.get_or_init(|| {
                    arrow::array::LargeStringArray::from(vec![self.value.as_str()])
                });
                let lhs: &dyn arrow::array::Datum = col;
                eq(lhs, &Scalar::new(scalar_arr.clone()))?
            }
            other => anyhow::bail!(
                "FilterMiddleware: column '{}' is {:?}, only Utf8/LargeUtf8 supported",
                self.field, other
            ),
        };

        let filtered = compute::filter_record_batch(&batch.batch, &mask)?;
        Ok(ArrowBatch { batch: filtered, meta: batch.meta.clone() })
    }
}
