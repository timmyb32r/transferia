use std::sync::OnceLock;

use arrow::array::Scalar;
use arrow::compute;
use arrow::compute::kernels::cmp::eq;
use arrow::datatypes::DataType;
use async_trait::async_trait;
use schemars::JsonSchema;
use serde::Deserialize;

use transferia_core::data::schema::DatasetSchema;
use transferia_core::data::table_data::TableData;
use transferia_delivery_contracts::middleware::Middleware;
use transferia_registry::{MiddlewareRegistration, RegistryBuilder};

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FilterConfig {
    pub field: String,

    pub value: String,
}

/// Middleware that keeps only rows where the given string column equals `value`.
///
/// NULL values never pass. Only `Utf8`/`LargeUtf8` supported.
/// Scalar arrays are cached; the column is resolved per input schema.
pub struct FilterMiddleware {
    field: String,
    value: String,
    /// Cached scalar `StringArray` (Utf8).
    scalar_utf8: OnceLock<arrow::array::StringArray>,
    /// Cached scalar `LargeStringArray` (`LargeUtf8`).
    scalar_large_utf8: OnceLock<arrow::array::LargeStringArray>,
}

impl FilterMiddleware {
    pub fn new(field: String, value: String) -> anyhow::Result<Self> {
        if field.is_empty() {
            anyhow::bail!("FilterMiddleware: field must not be empty");
        }
        Ok(Self {
            field,
            value,
            scalar_utf8: OnceLock::new(),
            scalar_large_utf8: OnceLock::new(),
        })
    }
}

#[async_trait]
impl Middleware for FilterMiddleware {
    async fn output_schema(&self, schema: &DatasetSchema) -> anyhow::Result<DatasetSchema> {
        let column = schema
            .columns
            .iter()
            .find(|column| column.name == self.field)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "FilterMiddleware: column '{}' not found in discovered schema",
                    self.field
                )
            })?;
        anyhow::ensure!(
            matches!(column.data_type, DataType::Utf8 | DataType::LargeUtf8),
            "FilterMiddleware: column '{}' is {:?}, only Utf8/LargeUtf8 supported",
            self.field,
            column.data_type
        );
        Ok(schema.clone())
    }

    async fn process(&self, data: TableData) -> anyhow::Result<TableData> {
        let schema = data.batch.schema();
        let col_idx = schema.index_of(&self.field).map_err(|error| {
            anyhow::anyhow!(
                "FilterMiddleware: column '{}' not found in schema: {error}",
                self.field
            )
        })?;

        let field_dt = schema.field(col_idx).data_type();
        let col = data.batch.column(col_idx);

        let mask = match field_dt {
            &DataType::Utf8 => {
                let scalar_arr = self
                    .scalar_utf8
                    .get_or_init(|| arrow::array::StringArray::from(vec![self.value.as_str()]));
                let lhs: &dyn arrow::array::Datum = col;
                eq(lhs, &Scalar::new(scalar_arr.clone()))?
            }
            &DataType::LargeUtf8 => {
                let scalar_arr = self.scalar_large_utf8.get_or_init(|| {
                    arrow::array::LargeStringArray::from(vec![self.value.as_str()])
                });
                let lhs: &dyn arrow::array::Datum = col;
                eq(lhs, &Scalar::new(scalar_arr.clone()))?
            }
            other @ (&DataType::Null
            | &DataType::Boolean
            | &DataType::Int8
            | &DataType::Int16
            | &DataType::Int32
            | &DataType::Int64
            | &DataType::UInt8
            | &DataType::UInt16
            | &DataType::UInt32
            | &DataType::UInt64
            | &DataType::Float16
            | &DataType::Float32
            | &DataType::Float64
            | &DataType::Timestamp(..)
            | &DataType::Date32
            | &DataType::Date64
            | &DataType::Time32(_)
            | &DataType::Time64(_)
            | &DataType::Duration(_)
            | &DataType::Interval(_)
            | &DataType::Binary
            | &DataType::FixedSizeBinary(_)
            | &DataType::LargeBinary
            | &DataType::BinaryView
            | &DataType::Utf8View
            | &DataType::List(_)
            | &DataType::ListView(_)
            | &DataType::FixedSizeList(..)
            | &DataType::LargeList(_)
            | &DataType::LargeListView(_)
            | &DataType::Struct(_)
            | &DataType::Union(..)
            | &DataType::Dictionary(..)
            | &DataType::Decimal32(..)
            | &DataType::Decimal64(..)
            | &DataType::Decimal128(..)
            | &DataType::Decimal256(..)
            | &DataType::Map(..)
            | &DataType::RunEndEncoded(..)) => anyhow::bail!(
                "FilterMiddleware: column '{}' is {:?}, only Utf8/LargeUtf8 supported",
                self.field,
                other
            ),
        };

        let filtered = compute::filter_record_batch(&data.batch, &mask)?;
        Ok(TableData {
            batch: filtered,
            ..data
        })
    }
}

pub fn register(builder: &mut RegistryBuilder) -> anyhow::Result<()> {
    builder.register_middleware(MiddlewareRegistration::new::<FilterConfig, _, _>(
        "filter",
        "Filter",
        || serde_json::json!({ "field": "", "value": "" }),
        |config| Ok(Box::new(FilterMiddleware::new(config.field, config.value)?)),
    )?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
