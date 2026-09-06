//! Bounded native table sampling without creating a delivery or destination.
use std::sync::Arc;

use anyhow::Context;
use arrow::compute::concat_batches;
use arrow::datatypes::{Field, Schema};
use clickhouse_arrow::ClientBuilder;
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::TableData;
use transferia_registry::{TableIdentity, TableSampleLimits};

use super::config::{ClickHouseSourceConfig, TableConfig};
use super::connector::{discover_table, snapshot_query};
use super::reader::normalize_snapshot_schema;
use crate::connectors::clickhouse::sink::client::ReconnectingClient;

pub(crate) async fn sample_table(config: ClickHouseSourceConfig, table: TableIdentity, limits: TableSampleLimits,
    cancellation: CancellationToken) -> anyhow::Result<TableData> {
    limits.validate()?;
    let row_limit = limits.row_limit;
    config.validate_connection()?;
    anyhow::ensure!(row_limit > 0, "row_limit must be positive");
    anyhow::ensure!(!table.namespace.is_empty() && !table.name.is_empty()
        && !table.namespace.contains('\0') && !table.name.contains('\0'), "invalid ClickHouse sample table identity");
    let classification = config.tables.compile()?.classify(&table);
    anyhow::ensure!(classification.selected_by.len() == 1 && classification.issues.is_empty(), "sample table must be selected by exactly one table rule");
    anyhow::ensure!(!config.hide_system_tables || !super::config::is_system_database(&table.namespace),
        "sample table is hidden by Hide system tables");
    let block_rows = i64::try_from(row_limit.min(config.batch_rows))?;
    let block_bytes = u64::try_from(limits.max_bytes)?;
    let timeout = std::time::Duration::from_millis(u64::try_from(limits.timeout_ms)?);
    let timeout_seconds = format!("{}.{:03}", limits.timeout_ms / 1000, limits.timeout_ms % 1000);
    let builders = config.hosts.iter().map(|host| {
        let builder = ClientBuilder::new()
            .with_destination(crate::connectors::address::host_port(host, config.port))
            .with_database("default")
            .with_username(&config.username)
            .with_password(&config.password)
            .with_arrow_options(clickhouse_arrow::ArrowOptions::strict().with_source_type_metadata(true))
            .with_setting("readonly", 2_i64)
            .with_setting("max_block_size", block_rows)
            .with_setting("preferred_block_size_bytes", block_bytes)
            .with_setting("max_execution_time", timeout_seconds.clone())
            .with_setting("timeout_overflow_mode", "throw")
            .with_tls(!config.trusted_plaintext);
        if let Some(path) = &config.tls_ca_file { builder.with_cafile(path) } else { builder }
    }).collect();
    let client = Arc::new(ReconnectingClient::from_connections(builders,
        config.connect_timeout().min(timeout), config.request_timeout().min(timeout)));
    tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("ClickHouse table sample cancelled"),
        result = async {
            observe_external_request("clickhouse", "connect_table_sample", client.ensure_connected()).await?;
            let discovered = observe_external_request("clickhouse", "discover_sample_table", discover_table(&client,
                TableConfig { database: table.namespace.clone(), name: table.name.clone() }, config.unsupported_types)).await?;
            let query = format!("{} SETTINGS max_result_bytes={}, result_overflow_mode='throw'",
                sample_query(&snapshot_query(&discovered), row_limit)?, limits.max_bytes);
            let mut stream = observe_external_request("clickhouse", "start_table_sample", client.query_stream(&query)).await?;
            let mut batches = Vec::new();
            let mut count = 0_usize;
            let mut retained_bytes = 0_usize;
            observe_external_request("clickhouse", "read_table_sample", async {
                while let Some(batch) = stream.next().await {
                    let batch = batch.with_context(|| format!("ClickHouse bounded sample read failed (max_sample_bytes={})", limits.max_bytes))?;
                    count = count.checked_add(batch.num_rows()).ok_or_else(|| anyhow::anyhow!("ClickHouse sample row count overflow"))?;
                    anyhow::ensure!(count <= row_limit, "ClickHouse sample exceeded row_limit");
                    let raw_bytes = batch.get_array_memory_size();
                    limits.check_bytes(retained_bytes.checked_add(raw_bytes)
                        .ok_or_else(|| anyhow::anyhow!("sample byte accounting overflow"))?)?;
                    let normalized = normalize_snapshot_schema(&batch, &discovered)?;
                    let normalized_bytes = normalized.get_array_memory_size();
                    limits.check_bytes(retained_bytes.checked_add(raw_bytes).and_then(|bytes| bytes.checked_add(normalized_bytes))
                        .ok_or_else(|| anyhow::anyhow!("sample byte accounting overflow"))?)?;
                    retained_bytes = retained_bytes.checked_add(normalized_bytes)
                        .ok_or_else(|| anyhow::anyhow!("sample byte accounting overflow"))?;
                    batches.push(normalized);
                }
                Ok::<_, anyhow::Error>(())
            }).await?;
            let schema = Arc::new(Schema::new(discovered.schema.columns.iter().map(|column|
                Field::new(&column.name, column.data_type.clone(), column.nullable).with_metadata(column.arrow_metadata())).collect::<Vec<_>>()));
            let batch = if batches.len() == 1 {
                batches.pop().expect("one batch was checked")
            } else {
                limits.check_bytes(retained_bytes.checked_mul(2)
                    .ok_or_else(|| anyhow::anyhow!("sample byte accounting overflow"))?)?;
                let batch = concat_batches(&schema, &batches)?;
                limits.check_bytes(retained_bytes.checked_add(batch.get_array_memory_size())
                    .ok_or_else(|| anyhow::anyhow!("sample byte accounting overflow"))?)?;
                batch
            };
            Ok(TableData::new(Arc::from(table.name.as_str()), false, batch, discovered.physical_system_columns)
                .with_namespace(Arc::from(table.namespace.as_str())))
        } => result,
    }
}

pub(super) fn sample_query(select: &str, row_limit: usize) -> anyhow::Result<String> {
    anyhow::ensure!(row_limit > 0, "row_limit must be positive");
    Ok(format!("{select} LIMIT {row_limit}"))
}
