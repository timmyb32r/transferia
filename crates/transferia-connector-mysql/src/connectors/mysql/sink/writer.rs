use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, Duration, NaiveDate, Utc};
use futures_util::future::BoxFuture;
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Params, Value};

use crate::connectors::mysql::common::quote_identifier;
use crate::metrics::SinkCounters;
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::delivery::{DeliveryDiscovery, SinkLimits};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::sink::{Delivery, Sink, SinkEvent, SinkIo};

const MAX_PREPARED_PARAMETERS: usize = 65_535;

pub struct MySqlSink {
    connection: Conn,
    counters: Arc<SinkCounters>,
    discovery: Arc<DeliveryDiscovery>,
    limits: Arc<dyn SinkLimits>,
    insert_rows: usize,
}

impl MySqlSink {
    pub fn new(
        connection: Conn,
        counters: Arc<SinkCounters>,
        discovery: Arc<DeliveryDiscovery>,
        limits: Arc<dyn SinkLimits>,
        insert_rows: usize,
    ) -> Self {
        Self {
            connection,
            counters,
            discovery,
            limits,
            insert_rows,
        }
    }

    async fn write_delivery(&mut self, delivery: &Delivery) -> anyhow::Result<()> {
        for batch in &delivery.outputs {
            self.limits.validate_batch(&self.discovery, batch)?;
        }
        let started = std::time::Instant::now();
        self.connection.query_drop("START TRANSACTION").await?;
        let write_result = async {
            let mut flushes = 0_u64;
            let mut rows = 0_u64;
            let mut bytes = 0_u64;
            for batch in &delivery.outputs {
                if batch.rows() == 0 {
                    continue;
                }
                let stored_batch = if self.discovery.keep_system_columns {
                    batch.batch.clone()
                } else {
                    without_system_columns(&batch.batch, &batch.system_columns)?
                };
                anyhow::ensure!(
                    stored_batch.num_columns() > 0,
                    "MySQL table '{}' cannot receive a batch with no stored columns",
                    batch.table
                );
                let max_rows = self
                    .insert_rows
                    .min(MAX_PREPARED_PARAMETERS / stored_batch.num_columns());
                anyhow::ensure!(
                    max_rows > 0,
                    "MySQL table '{}' has too many columns for one prepared row",
                    batch.table
                );
                for offset in (0..stored_batch.num_rows()).step_by(max_rows) {
                    let len = max_rows.min(stored_batch.num_rows() - offset);
                    insert_chunk(
                        &mut self.connection,
                        &batch.table,
                        &stored_batch,
                        offset,
                        len,
                    )
                    .await?;
                    flushes += 1;
                }
                rows += batch.rows() as u64;
                bytes += batch.bytes() as u64;
            }
            Ok::<_, anyhow::Error>((rows, bytes, flushes))
        }
        .await;
        let (rows, bytes, flushes) = match write_result {
            Ok(counters) => {
                self.connection.query_drop("COMMIT").await?;
                counters
            }
            Err(error) => {
                let rollback = self.connection.query_drop("ROLLBACK").await;
                if let Err(rollback) = rollback {
                    return Err(error.context(format!(
                        "MySQL write failed and transaction rollback also failed: {rollback}"
                    )));
                }
                return Err(error);
            }
        };
        self.counters.add_busy(started.elapsed());
        self.counters.add_rows(rows);
        self.counters.add_bytes(bytes);
        for _ in 0..flushes {
            self.counters.add_flush();
        }
        Ok(())
    }
}

async fn insert_chunk(
    connection: &mut Conn,
    table: &str,
    batch: &RecordBatch,
    offset: usize,
    len: usize,
) -> anyhow::Result<()> {
    let columns = batch
        .schema()
        .fields()
        .iter()
        .map(|field| quote_identifier(field.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let row_placeholders = format!(
        "({})",
        std::iter::repeat_n("?", batch.num_columns())
            .collect::<Vec<_>>()
            .join(", ")
    );
    let values_clause = std::iter::repeat_n(row_placeholders.as_str(), len)
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "INSERT INTO {} ({columns}) VALUES {values_clause}",
        quote_identifier(table)
    );
    let mut values = Vec::with_capacity(len.saturating_mul(batch.num_columns()));
    for row in offset..offset + len {
        for column in batch.columns() {
            values.push(arrow_value(column.as_ref(), row)?);
        }
    }
    connection
        .exec_drop(query, Params::Positional(values))
        .await?;
    Ok(())
}

fn arrow_value(column: &dyn Array, row: usize) -> anyhow::Result<Value> {
    if column.is_null(row) {
        return Ok(Value::NULL);
    }
    macro_rules! value {
        ($array:ty, $variant:ident) => {
            Value::$variant(downcast::<$array>(column)?.value(row).into())
        };
    }
    Ok(match column.data_type() {
        DataType::Boolean => Value::Int(i64::from(downcast::<BooleanArray>(column)?.value(row))),
        DataType::Int8 => value!(Int8Array, Int),
        DataType::UInt8 => value!(UInt8Array, UInt),
        DataType::Int16 => value!(Int16Array, Int),
        DataType::UInt16 => value!(UInt16Array, UInt),
        DataType::Int32 => value!(Int32Array, Int),
        DataType::UInt32 => value!(UInt32Array, UInt),
        DataType::Int64 => value!(Int64Array, Int),
        DataType::UInt64 => value!(UInt64Array, UInt),
        DataType::Float32 => {
            let value = downcast::<Float32Array>(column)?.value(row);
            anyhow::ensure!(value.is_finite(), "MySQL FLOAT cannot preserve non-finite {value}");
            Value::Float(value)
        }
        DataType::Float64 => {
            let value = downcast::<Float64Array>(column)?.value(row);
            anyhow::ensure!(value.is_finite(), "MySQL DOUBLE cannot preserve non-finite {value}");
            Value::Double(value)
        }
        DataType::Utf8 => Value::Bytes(
            downcast::<StringArray>(column)?
                .value(row)
                .as_bytes()
                .to_vec(),
        ),
        DataType::Binary => Value::Bytes(downcast::<BinaryArray>(column)?.value(row).to_vec()),
        DataType::Decimal128(_, scale) => Value::Bytes(
            decimal_text(
                &downcast::<Decimal128Array>(column)?.value(row).to_string(),
                *scale,
            )
            .into_bytes(),
        ),
        DataType::Decimal256(_, scale) => Value::Bytes(
            decimal_text(
                &downcast::<Decimal256Array>(column)?.value(row).to_string(),
                *scale,
            )
            .into_bytes(),
        ),
        DataType::Date32 => Value::Bytes(
            date_text(downcast::<Date32Array>(column)?.value(row))?.into_bytes(),
        ),
        DataType::Date64 => Value::Bytes(
            timestamp_text(
                downcast::<Date64Array>(column)?.value(row),
                TimeUnit::Millisecond,
            )?
            .into_bytes(),
        ),
        DataType::Timestamp(unit, None) => {
            let value = match unit {
                TimeUnit::Second => downcast::<TimestampSecondArray>(column)?.value(row),
                TimeUnit::Millisecond => {
                    downcast::<TimestampMillisecondArray>(column)?.value(row)
                }
                TimeUnit::Microsecond => {
                    downcast::<TimestampMicrosecondArray>(column)?.value(row)
                }
                TimeUnit::Nanosecond => downcast::<TimestampNanosecondArray>(column)?.value(row),
            };
            Value::Bytes(timestamp_text(value, unit.clone())?.into_bytes())
        }
        data_type => anyhow::bail!("unsupported Arrow value type {data_type:?} for MySQL sink"),
    })
}

pub(super) fn decimal_text(unscaled: &str, scale: i8) -> String {
    let (sign, digits) = unscaled
        .strip_prefix('-')
        .map_or(("", unscaled), |digits| ("-", digits));
    if scale == 0 {
        return unscaled.to_owned();
    }
    if scale < 0 {
        return format!("{sign}{digits}{}", "0".repeat(usize::from(scale.unsigned_abs())));
    }
    let scale = usize::try_from(scale).unwrap_or_default();
    if digits.len() <= scale {
        format!("{sign}0.{}{digits}", "0".repeat(scale - digits.len()))
    } else {
        let split = digits.len() - scale;
        format!("{sign}{}.{}", &digits[..split], &digits[split..])
    }
}

pub(super) fn date_text(days: i32) -> anyhow::Result<String> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| anyhow::anyhow!("invalid Unix epoch"))?;
    let date = epoch
        .checked_add_signed(Duration::days(i64::from(days)))
        .ok_or_else(|| anyhow::anyhow!("Arrow Date32 {days} is outside the calendar range"))?;
    ensure_mysql_year(date.year())?;
    Ok(date.format("%Y-%m-%d").to_string())
}

pub(super) fn timestamp_text(value: i64, unit: TimeUnit) -> anyhow::Result<String> {
    let micros = match unit {
        TimeUnit::Second => value.checked_mul(1_000_000),
        TimeUnit::Millisecond => value.checked_mul(1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond => {
            anyhow::ensure!(
                value.rem_euclid(1_000) == 0,
                "MySQL DATETIME(6) cannot preserve nanosecond timestamp {value} exactly"
            );
            Some(value.div_euclid(1_000))
        }
    }
    .ok_or_else(|| anyhow::anyhow!("Arrow timestamp conversion overflow"))?;
    let seconds = micros.div_euclid(1_000_000);
    let subsecond_micros = micros.rem_euclid(1_000_000);
    let datetime = chrono::DateTime::<Utc>::from_timestamp(
        seconds,
        u32::try_from(subsecond_micros)?.saturating_mul(1_000),
    )
    .ok_or_else(|| anyhow::anyhow!("Arrow timestamp {value:?} is outside the calendar range"))?;
    ensure_mysql_year(datetime.year())?;
    Ok(datetime.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
}

fn ensure_mysql_year(year: i32) -> anyhow::Result<()> {
    anyhow::ensure!(
        (1_000..=9_999).contains(&year),
        "MySQL temporal types cannot preserve year {year}; supported range is 1000..=9999"
    );
    Ok(())
}

fn downcast<T: Array + 'static>(column: &dyn Array) -> anyhow::Result<&T> {
    column.as_any().downcast_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "Arrow array does not match declared type {:?}",
            column.data_type()
        )
    })
}

fn without_system_columns(
    batch: &RecordBatch,
    system_columns: &SystemColumns,
) -> anyhow::Result<RecordBatch> {
    if system_columns.is_empty() {
        return Ok(batch.clone());
    }
    let system_indexes = system_columns
        .iter()
        .map(|column| column.index)
        .collect::<std::collections::HashSet<_>>();
    let indexes = (0..batch.num_columns())
        .filter(|index| !system_indexes.contains(index))
        .collect::<Vec<_>>();
    Ok(batch.project(&indexes)?)
}

impl Sink for MySqlSink {
    fn run(
        mut self: Box<Self>,
        mut io: SinkIo,
    ) -> BoxFuture<'static, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            let result: anyhow::Result<()> = async {
                while let Some(delivery) = tokio::select! {
                    biased;
                    () = io.cancellation.cancelled() => None,
                    delivery = io.deliveries.recv() => delivery,
                } {
                    let id = delivery.id;
                    let source_messages = delivery.meta.source_messages;
                    self.write_delivery(&delivery).await?;
                    self.counters.add_source_messages(source_messages);
                    io.events
                        .send(SinkEvent::CommittedThrough(id))
                        .await
                        .map_err(|_| anyhow::anyhow!("MySQL sink event receiver closed"))?;
                }
                Ok(())
            }
            .await;
            result.map_err(DataPlaneFailure::retryable_or_passthrough)
        })
    }
}
