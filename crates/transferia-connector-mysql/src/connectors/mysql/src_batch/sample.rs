//! Native bounded reads for the transformation preview; no binlog or delivery
//! snapshot state is created.
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use mysql_async::prelude::{Protocol, Queryable};
use mysql_async::QueryResult;
use tokio_util::sync::CancellationToken;
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::data::system_columns::SystemColumns;
use transferia_core::TableData;
use transferia_registry::{TableIdentity, TableSampleLimits};

use super::config::{MySqlReadProtocol, MySqlSourceConfig, TableConfig};
use super::connector::{discover_table, ColumnPlan};
use super::reader::{column_array, estimate_arrow_working_set_bytes, next_snapshot_rows_capacity,
    retained_row_value_heap_bytes, retained_rows_heap_bytes, SnapshotRow};
use super::MYSQL_CANONICAL_SNAPSHOT_SQL_MODE;
use crate::connectors::mysql::common::{connect_sample_with_max_allowed_packet, quote_identifier, validate_identifier, MYSQL_CLIENT_PACKET_MIN_BYTES};

pub(crate) async fn sample_table(config: MySqlSourceConfig, table: TableIdentity, limits: TableSampleLimits,
    cancellation: CancellationToken) -> anyhow::Result<TableData> {
    limits.validate()?;
    let row_limit = limits.row_limit;
    config.connection.validate()?;
    anyhow::ensure!(u32::try_from(limits.timeout_ms).is_ok(), "timeout_ms exceeds MySQL max_execution_time range");
    sample_query(&table, "*", row_limit)?;
    let classification = config.tables.compile()?.classify(&table);
    anyhow::ensure!(classification.selected_by.len() == 1 && classification.issues.is_empty(), "sample table must be selected by exactly one table rule");
    anyhow::ensure!(config.includes_database(&table.namespace), "sample table is hidden by Hide system tables");
    tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("MySQL table sample cancelled"),
        result = async {
            let mut connection = observe_external_request("mysql", "connect_table_sample",
                connect_sample_with_max_allowed_packet(&config.connection,
                    config.max_row_bytes.min(limits.max_bytes.max(MYSQL_CLIENT_PACKET_MIN_BYTES)))).await?;
            let server_version = observe_external_request("mysql", "discover_sample_server_version",
                connection.query_first::<String, _>("SELECT VERSION()")).await?
                .ok_or_else(|| anyhow::anyhow!("MySQL server version is unavailable for configuring the sample deadline"))?;
            observe_external_request("mysql", "set_sample_timeout", connection.query_drop(
                timeout_statement(&server_version, limits.timeout_ms))).await?;
            // MySQL metadata-lock timeouts have whole-second granularity. The
            // client deadline remains exact; server work gets the nearest upper
            // whole-second bound instead of lingering on a metadata lock.
            observe_external_request("mysql", "set_sample_lock_timeout", connection.query_drop(
                format!("SET SESSION lock_wait_timeout = {}", limits.timeout_ms.div_ceil(1000)))).await?;
            observe_external_request("mysql", "set_sample_timezone", connection.query_drop("SET SESSION time_zone = '+00:00'")).await?;
            observe_external_request("mysql", "set_sample_sql_mode", connection.query_drop(MYSQL_CANONICAL_SNAPSHOT_SQL_MODE)).await?;
            let forbidden = observe_external_request("mysql", "verify_sample_sql_mode",
                connection.query_first::<u64, _>("SELECT FIND_IN_SET('PAD_CHAR_TO_FULL_LENGTH', @@SESSION.sql_mode)")).await?;
            anyhow::ensure!(forbidden == Some(0), "MySQL sample session retained PAD_CHAR_TO_FULL_LENGTH");
            observe_external_request("mysql", "begin_read_only_table_sample", connection.query_drop("START TRANSACTION READ ONLY")).await?;
            let discovered = discover_table(&mut connection, &table.namespace,
                TableConfig { database: table.namespace.clone(), name: table.name.clone() }, false, config.read_protocol).await?;
            let projection = discovered.columns.iter().map(|column| column.expression.as_str()).collect::<Vec<_>>().join(", ");
            let query = sample_query(&table, &projection, row_limit)?;
            let (rows, retained_bytes) = observe_external_request("mysql", "read_table_sample", async {
                match config.read_protocol {
                    MySqlReadProtocol::Text => collect_rows(connection.query_iter(query).await?, &discovered.columns, limits).await,
                    MySqlReadProtocol::Binary => collect_rows(connection.exec_iter(query, ()).await?, &discovered.columns, limits).await,
                }
            }).await?;
            let fields = discovered.schema.columns.iter().map(|column|
                Field::new(&column.name, column.data_type.clone(), column.nullable).with_metadata(column.arrow_metadata())).collect::<Vec<_>>();
            limits.check_bytes(retained_bytes.checked_add(estimate_arrow_working_set_bytes(&rows, &discovered.columns, None)?)
                .ok_or_else(|| anyhow::anyhow!("sample byte accounting overflow"))?)?;
            let mut arrays = Vec::with_capacity(discovered.columns.len());
            let mut working_bytes = retained_bytes;
            for (index, column) in discovered.columns.iter().enumerate() {
                let array = column_array(&rows, index, column)?;
                working_bytes = working_bytes.checked_add(array.get_array_memory_size())
                    .ok_or_else(|| anyhow::anyhow!("sample byte accounting overflow"))?;
                limits.check_bytes(working_bytes)?;
                arrays.push(array);
            }
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
            observe_external_request("mysql", "finish_read_only_table_sample", connection.query_drop("ROLLBACK")).await?;
            observe_external_request("mysql", "disconnect_table_sample", connection.disconnect()).await?;
            Ok(TableData::new(Arc::from(table.name.as_str()), false, batch, SystemColumns::default())
                .with_namespace(Arc::from(table.namespace.as_str())))
        } => result,
    }
}

pub(super) fn timeout_statement(server_version: &str, timeout_ms: usize) -> String {
    if server_version.contains("MariaDB") {
        format!("SET SESSION max_statement_time = {}.{:03}", timeout_ms / 1000, timeout_ms % 1000)
    } else {
        format!("SET SESSION max_execution_time = {timeout_ms}")
    }
}

async fn collect_rows<P: Protocol>(mut result: QueryResult<'_, '_, P>, columns: &[ColumnPlan],
    limits: TableSampleLimits) -> anyhow::Result<(Vec<SnapshotRow>, usize)> {
    anyhow::ensure!(result.columns_ref().len() == columns.len()
        && result.columns_ref().iter().zip(columns).all(|(actual, expected)| actual.name_str() == expected.name),
        "MySQL sample schema changed after discovery");
    let mut rows = Vec::new();
    let mut value_bytes = 0_usize;
    while let Some(row) = result.next().await.map_err(|error| {
        if error.is_packet_too_large() {
            anyhow::anyhow!("MySQL source sample exceeds max_sample_bytes or the configured max_row_bytes packet limit")
        } else { error.into() }
    })? {
        anyhow::ensure!(rows.len() < limits.row_limit, "MySQL sample exceeded row_limit");
        let row = row.unwrap_raw();
        value_bytes = value_bytes.checked_add(retained_row_value_heap_bytes(&row)?)
            .ok_or_else(|| anyhow::anyhow!("sample byte accounting overflow"))?;
        let next_capacity = next_snapshot_rows_capacity(rows.len(), rows.capacity())?;
        let overlapping_capacity = if next_capacity == rows.capacity() { next_capacity } else {
            next_capacity.checked_add(rows.capacity()).ok_or_else(|| anyhow::anyhow!("sample byte accounting overflow"))?
        };
        limits.check_bytes(retained_rows_heap_bytes(overlapping_capacity, value_bytes)?)?;
        rows.try_reserve(1)?;
        rows.push(row);
        limits.check_bytes(retained_rows_heap_bytes(rows.capacity(), value_bytes)?)?;
    }
    let bytes = retained_rows_heap_bytes(rows.capacity(), value_bytes)?;
    Ok((rows, bytes))
}

pub(super) fn sample_query(table: &TableIdentity, projection: &str, row_limit: usize) -> anyhow::Result<String> {
    anyhow::ensure!(row_limit > 0, "row_limit must be positive");
    validate_identifier("database", &table.namespace)?;
    validate_identifier("table", &table.name)?;
    Ok(format!("SELECT {projection} FROM {}.{} LIMIT {row_limit}", quote_identifier(&table.namespace), quote_identifier(&table.name)))
}
