//! Read-only native table samples; deliberately independent of replication and
//! exported delivery snapshots.
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use tokio_util::sync::CancellationToken;
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::TableData;
use transferia_registry::{TableIdentity, TableSampleLimits};

use super::copy_out::CopyOutReader;
use super::reader::{column_array, source_select_projection, source_user_field};
use crate::connectors::postgres::common::{connect_sample, postgres_to_arrow, quote_identifier, PostgresCopyFormat, MAX_IDENTIFIER_BYTES};
use crate::connectors::postgres::source::{discover_table, PostgresSourceConfig, TableConfig};
use crate::metrics::SourceCounters;

pub(crate) async fn sample_table(config: PostgresSourceConfig, table: TableIdentity, limits: TableSampleLimits,
    cancellation: CancellationToken) -> anyhow::Result<TableData> {
    limits.validate()?;
    let row_limit = limits.row_limit;
    config.connection.validate()?;
    anyhow::ensure!(i32::try_from(limits.timeout_ms).is_ok(), "timeout_ms exceeds PostgreSQL statement_timeout range");
    let query_identity = sample_query(&table, "*", row_limit)?;
    let classification = config.tables.compile()?.classify(&table);
    anyhow::ensure!(classification.selected_by.len() == 1 && classification.issues.is_empty(), "sample table must be selected by exactly one table rule");
    anyhow::ensure!(!config.hide_system_tables || (table.namespace != "information_schema" && !table.namespace.starts_with("pg_")),
        "sample table is hidden by Hide system tables");
    tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("PostgreSQL table sample cancelled"),
        result = async {
            let client = observe_external_request("postgres", "connect_table_sample", connect_sample(&config.connection)).await?;
            observe_external_request("postgres", "begin_read_only_table_sample",
                client.batch_execute(&format!("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL statement_timeout = {}", limits.timeout_ms))).await?;
            let discovered = observe_external_request("postgres", "discover_sample_table",
                discover_table(&client, TableConfig { schema: table.namespace.clone(), name: table.name.clone() }, config.unsupported_types)).await?;
            let metadata = observe_external_request("postgres", "prepare_sample_columns", client.prepare(&query_identity)).await?;
            let projection = source_select_projection(metadata.columns(), config.unsupported_types)?;
            let select = sample_query(&table, &projection, row_limit)?;
            let statement = observe_external_request("postgres", "prepare_table_sample", client.prepare(&select)).await?;
            anyhow::ensure!(statement.columns().len() == discovered.schema.columns.len(), "PostgreSQL sample schema changed during discovery");
            let format = match config.copy_to_format { PostgresCopyFormat::Binary => "BINARY", PostgresCopyFormat::Text => "TEXT" };
            let stream = observe_external_request("postgres", "start_table_sample",
                client.copy_out(&format!("COPY ({select}) TO STDOUT (FORMAT {format})"))).await?;
            let mut reader = CopyOutReader::new(stream, config.copy_to_format, statement.columns().len())
                .with_byte_limit(limits.max_bytes);
            let counters = SourceCounters::new();
            let mut rows = Vec::new();
            let mut descriptor_bytes = 0_usize;
            observe_external_request("postgres", "read_table_sample", async {
                while let Some(row) = reader.next_row(&counters).await? {
                    anyhow::ensure!(rows.len() < row_limit, "PostgreSQL sample exceeded row_limit");
                    descriptor_bytes = descriptor_bytes.checked_add(row.fields.capacity()
                        .checked_mul(size_of::<Option<bytes::Bytes>>()).ok_or_else(|| anyhow::anyhow!("sample byte accounting overflow"))?)
                        .and_then(|value| value.checked_add(size_of::<super::copy_out::RawCopyRow>()))
                        .ok_or_else(|| anyhow::anyhow!("sample byte accounting overflow"))?;
                    limits.check_bytes(reader.received_bytes().checked_add(descriptor_bytes)
                        .ok_or_else(|| anyhow::anyhow!("sample byte accounting overflow"))?)?;
                    rows.try_reserve_exact(1)?;
                    rows.push(row);
                }
                Ok::<_, anyhow::Error>(())
            }).await?;
            let mut fields = Vec::with_capacity(statement.columns().len());
            let mut arrays = Vec::with_capacity(statement.columns().len());
            let mut retained_bytes = reader.received_bytes().checked_add(descriptor_bytes)
                .ok_or_else(|| anyhow::anyhow!("sample byte accounting overflow"))?;
            for (index, (column, expected)) in statement.columns().iter().zip(&discovered.schema.columns).enumerate() {
                anyhow::ensure!(column.name() == expected.name && postgres_to_arrow(column.type_())? == expected.data_type,
                    "PostgreSQL sample schema changed at column '{}'", expected.name);
                fields.push(source_user_field(expected, false));
                let array = column_array(&rows, index, column.type_(), config.copy_to_format)?;
                retained_bytes = retained_bytes.checked_add(array.get_array_memory_size())
                    .ok_or_else(|| anyhow::anyhow!("sample byte accounting overflow"))?;
                limits.check_bytes(retained_bytes)?;
                arrays.push(array);
            }
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
            observe_external_request("postgres", "finish_read_only_table_sample", client.batch_execute("ROLLBACK")).await?;
            Ok(TableData::new(Arc::from(table.name.as_str()), false, batch, SystemColumns::default())
                .with_namespace(Arc::from(table.namespace.as_str())))
        } => result,
    }
}

pub(super) fn sample_query(table: &TableIdentity, projection: &str, row_limit: usize) -> anyhow::Result<String> {
    anyhow::ensure!(row_limit > 0, "row_limit must be positive");
    for name in [&table.namespace, &table.name] {
        anyhow::ensure!(!name.is_empty() && name.len() <= MAX_IDENTIFIER_BYTES && !name.contains('\0'),
            "sample schema and table names must be non-empty PostgreSQL identifiers without NUL and at most {MAX_IDENTIFIER_BYTES} bytes");
    }
    Ok(format!("SELECT {projection} FROM {}.{} LIMIT {row_limit}", quote_identifier(&table.namespace), quote_identifier(&table.name)))
}
