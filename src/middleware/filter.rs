use arrow::array::Scalar;
use arrow::compute;
use arrow::compute::kernels::cmp::eq;
use arrow::datatypes::DataType;
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

impl Middleware for FilterMiddleware {
    fn process(&self, batch: ArrowBatch) -> anyhow::Result<ArrowBatch> {
        let schema = batch.batch.schema();
        let col_idx = schema
            .index_of(&self.field)
            .map_err(|_| anyhow!("FilterMiddleware: column '{}' not found", self.field))?;

        let field_dt = schema.field(col_idx).data_type();
        let col = batch.batch.column(col_idx);

        // Vectorised mask via SIMD `eq` kernel — no scalar loop.
        let mask = match field_dt {
            DataType::Utf8 => {
                let lhs: &dyn arrow::array::Datum = col; // auto-deref &Arc<dyn Array> → &dyn Array → &dyn Datum
                let scalar_arr = arrow::array::StringArray::from(vec![self.value.as_str()]);
                eq(lhs, &Scalar::new(scalar_arr))?
            }
            DataType::LargeUtf8 => {
                let lhs: &dyn arrow::array::Datum = col;
                let scalar_arr = arrow::array::LargeStringArray::from(vec![self.value.as_str()]);
                eq(lhs, &Scalar::new(scalar_arr))?
            }
            other => anyhow::bail!(
                "FilterMiddleware: column '{}' is {:?}, only Utf8/LargeUtf8 supported",
                self.field,
                other
            ),
        };

        let filtered = compute::filter_record_batch(&batch.batch, &mask)?;

        // CONTRACT: preserve meta
        Ok(ArrowBatch {
            batch: filtered,
            meta: batch.meta.clone(),
        })
    }
}
