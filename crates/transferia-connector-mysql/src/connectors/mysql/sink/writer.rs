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
use transferia_core::delivery::{DeliveryDiscovery, SinkLimits};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::sink::{Delivery, Sink, SinkEvent, SinkIo};
use transferia_core::{project_sink_batch, ProjectedSinkBatch};

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
        let projected = delivery
            .outputs
            .iter()
            .map(|batch| project_sink_batch(&self.discovery, batch))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let started = std::time::Instant::now();
        self.connection.query_drop("START TRANSACTION").await?;
        let write_result = async {
            let mut flushes = 0_u64;
            let mut rows = 0_u64;
            let mut bytes = 0_u64;
            for (batch_index, (batch, projected)) in
                delivery.outputs.iter().zip(projected).enumerate()
            {
                if batch.rows() == 0 {
                    continue;
                }
                match projected {
                    ProjectedSinkBatch::AppendOnly(stored) => {
                        flushes += write_insert_batches(
                            &mut self.connection,
                            &batch.table,
                            &stored,
                            self.insert_rows,
                            None,
                        )
                        .await?;
                    }
                    ProjectedSinkBatch::Changelog(changelog) => {
                        for (run_index, run) in
                            changelog.collapsed_runs()?.into_iter().enumerate()
                        {
                            flushes += match run.operation {
                                transferia_core::ChangeOperation::Create
                                | transferia_core::ChangeOperation::SnapshotRead => {
                                    write_insert_batches(
                                        &mut self.connection,
                                        &batch.table,
                                        &run.batch,
                                        self.insert_rows,
                                        Some(&changelog.primary_keys),
                                    )
                                    .await?
                                }
                                transferia_core::ChangeOperation::Update => {
                                    write_update_batch(
                                        &mut self.connection,
                                        &batch.table,
                                        &run.batch,
                                        &changelog.primary_keys,
                                        self.insert_rows,
                                        &format!(
                                            "__transferia_{}_{}_{}",
                                            delivery.id.get(),
                                            batch_index,
                                            run_index
                                        ),
                                    )
                                    .await?
                                }
                                transferia_core::ChangeOperation::Delete => {
                                    write_delete_batches(
                                        &mut self.connection,
                                        &batch.table,
                                        &run.batch,
                                        self.insert_rows,
                                    )
                                    .await?
                                }
                            };
                        }
                    }
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

async fn write_update_batch(
    connection: &mut Conn,
    table: &str,
    batch: &RecordBatch,
    primary_keys: &[String],
    insert_rows: usize,
    staging: &str,
) -> anyhow::Result<u64> {
    let columns = batch
        .schema()
        .fields()
        .iter()
        .map(|field| quote_identifier(field.name()))
        .collect::<Vec<_>>();
    let non_keys = batch
        .schema()
        .fields()
        .iter()
        .filter(|field| !primary_keys.iter().any(|key| key == field.name()))
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    connection
        .query_drop(format!(
            "CREATE TEMPORARY TABLE {} AS SELECT {} FROM {} WHERE FALSE",
            quote_identifier(staging),
            columns.join(", "),
            quote_identifier(table)
        ))
        .await?;
    let result = async {
        let mut flushes = write_insert_batches(
            connection,
            staging,
            batch,
            insert_rows,
            None,
        )
        .await?;
        let predicate = primary_keys
            .iter()
            .map(|key| {
                let key = quote_identifier(key);
                format!("target.{key} = staged.{key}")
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let matched = connection
            .query_first::<u64, _>(format!(
                "SELECT COUNT(*) FROM {} AS target JOIN {} AS staged ON {predicate}",
                quote_identifier(table),
                quote_identifier(staging)
            ))
            .await?
            .unwrap_or_default();
        anyhow::ensure!(
            matched == batch.num_rows() as u64,
            "MySQL UPDATE matched {matched} rows, expected {}; destination state is incomplete",
            batch.num_rows()
        );
        if !non_keys.is_empty() {
            let updates = non_keys
                .iter()
                .map(|column| {
                    let column = quote_identifier(column);
                    format!("target.{column} = staged.{column}")
                })
                .collect::<Vec<_>>()
                .join(", ");
            connection
                .query_drop(format!(
                    "UPDATE {} AS target JOIN {} AS staged ON {predicate} SET {updates}",
                    quote_identifier(table),
                    quote_identifier(staging)
                ))
                .await?;
            flushes += 1;
        }
        Ok::<_, anyhow::Error>(flushes)
    }
    .await;
    let drop_result = connection
        .query_drop(format!("DROP TEMPORARY TABLE {}", quote_identifier(staging)))
        .await;
    match (result, drop_result) {
        (Ok(flushes), Ok(())) => Ok(flushes),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error.into()),
    }
}

async fn write_insert_batches(
    connection: &mut Conn,
    table: &str,
    batch: &RecordBatch,
    insert_rows: usize,
    upsert_primary_keys: Option<&[String]>,
) -> anyhow::Result<u64> {
    anyhow::ensure!(
        batch.num_columns() > 0,
        "MySQL table '{table}' cannot receive a batch with no stored columns"
    );
    let max_rows = insert_rows.min(MAX_PREPARED_PARAMETERS / batch.num_columns());
    anyhow::ensure!(
        max_rows > 0,
        "MySQL table '{table}' has too many columns for one prepared row"
    );
    let mut flushes = 0;
    for offset in (0..batch.num_rows()).step_by(max_rows) {
        let len = max_rows.min(batch.num_rows() - offset);
        insert_chunk(
            connection,
            table,
            batch,
            offset,
            len,
            upsert_primary_keys,
        )
        .await?;
        flushes += 1;
    }
    Ok(flushes)
}

async fn insert_chunk(
    connection: &mut Conn,
    table: &str,
    batch: &RecordBatch,
    offset: usize,
    len: usize,
    upsert_primary_keys: Option<&[String]>,
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
    let mut query = format!(
        "INSERT INTO {} ({columns}) VALUES {values_clause}",
        quote_identifier(table)
    );
    if let Some(primary_keys) = upsert_primary_keys {
        let updates = batch
            .schema()
            .fields()
            .iter()
            .filter(|field| !primary_keys.iter().any(|key| key == field.name()))
            .map(|field| {
                let column = quote_identifier(field.name());
                format!("{column} = VALUES({column})")
            })
            .collect::<Vec<_>>();
        let updates = if updates.is_empty() {
            let key = quote_identifier(&primary_keys[0]);
            format!("{key} = VALUES({key})")
        } else {
            updates.join(", ")
        };
        query.push_str(&format!(" ON DUPLICATE KEY UPDATE {updates}"));
    }
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

async fn write_delete_batches(
    connection: &mut Conn,
    table: &str,
    primary_keys: &RecordBatch,
    delete_rows: usize,
) -> anyhow::Result<u64> {
    anyhow::ensure!(
        primary_keys.num_columns() > 0,
        "MySQL changelog delete for table '{table}' requires a primary key"
    );
    let max_rows = delete_rows.min(MAX_PREPARED_PARAMETERS / primary_keys.num_columns());
    anyhow::ensure!(max_rows > 0, "MySQL primary key has too many columns");
    let columns = primary_keys
        .schema()
        .fields()
        .iter()
        .map(|field| quote_identifier(field.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let tuple = format!(
        "({})",
        std::iter::repeat_n("?", primary_keys.num_columns())
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut flushes = 0;
    for offset in (0..primary_keys.num_rows()).step_by(max_rows) {
        let len = max_rows.min(primary_keys.num_rows() - offset);
        let placeholders = std::iter::repeat_n(tuple.as_str(), len)
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "DELETE FROM {} WHERE ({columns}) IN ({placeholders})",
            quote_identifier(table)
        );
        let mut values = Vec::with_capacity(len.saturating_mul(primary_keys.num_columns()));
        for row in offset..offset + len {
            for column in primary_keys.columns() {
                values.push(arrow_value(column.as_ref(), row)?);
            }
        }
        connection
            .exec_drop(query, Params::Positional(values))
            .await?;
        flushes += 1;
    }
    Ok(flushes)
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
            anyhow::ensure!(
                value.is_finite(),
                "MySQL FLOAT cannot preserve non-finite {value}"
            );
            Value::Float(value)
        }
        DataType::Float64 => {
            let value = downcast::<Float64Array>(column)?.value(row);
            anyhow::ensure!(
                value.is_finite(),
                "MySQL DOUBLE cannot preserve non-finite {value}"
            );
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
        DataType::Date32 => {
            Value::Bytes(date_text(downcast::<Date32Array>(column)?.value(row))?.into_bytes())
        }
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
                TimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(column)?.value(row),
                TimeUnit::Microsecond => downcast::<TimestampMicrosecondArray>(column)?.value(row),
                TimeUnit::Nanosecond => downcast::<TimestampNanosecondArray>(column)?.value(row),
            };
            Value::Bytes(timestamp_text(value, *unit)?.into_bytes())
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
        return format!(
            "{sign}{digits}{}",
            "0".repeat(usize::from(scale.unsigned_abs()))
        );
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
    let epoch =
        NaiveDate::from_ymd_opt(1970, 1, 1).ok_or_else(|| anyhow::anyhow!("invalid Unix epoch"))?;
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
