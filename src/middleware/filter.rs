use std::sync::OnceLock;

use arrow::array::Scalar;
use arrow::compute;
use arrow::compute::kernels::cmp::eq;
use arrow::datatypes::DataType;

use crate::pipeline::middleware::Middleware;
use crate::types::arrow_batch::ArrowBatch;

/// Middleware that keeps only rows where the given string column equals `value`.
///
/// NULL values never pass. Only `Utf8`/`LargeUtf8` supported.
/// Column index and scalar arrays are cached after first use.
pub struct FilterMiddleware {
    field: String,
    value: String,
    /// Cached column index — resolved once from the first batch's schema.
    col_idx: OnceLock<usize>,
    /// Cached scalar StringArray (Utf8).
    scalar_utf8: OnceLock<arrow::array::StringArray>,
    /// Cached scalar LargeStringArray (LargeUtf8).
    scalar_large_utf8: OnceLock<arrow::array::LargeStringArray>,
}

impl FilterMiddleware {
    pub fn new(field: String, value: String) -> anyhow::Result<Self> {
        if field.is_empty() {
            anyhow::bail!("FilterMiddleware: field must not be empty");
        }
        Ok(Self {
            field, value,
            col_idx: OnceLock::new(),
            scalar_utf8: OnceLock::new(),
            scalar_large_utf8: OnceLock::new(),
        })
    }
}

impl Middleware for FilterMiddleware {
    fn process(&self, batch: ArrowBatch) -> anyhow::Result<ArrowBatch> {
        let schema = batch.batch.schema();
        let col_idx = match self.col_idx.get() {
            Some(&i) => i,
            None => {
                let i = schema.index_of(&self.field).map_err(|_| {
                    anyhow::anyhow!("FilterMiddleware: column '{}' not found in schema", self.field)
                })?;
                // set() races harmlessly — same value from same (field, schema) pair.
                let _ = self.col_idx.set(i);
                i
            }
        };

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
