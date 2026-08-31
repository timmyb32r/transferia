use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, StreamExt as _};
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::client::{classify_http_failure, YTsaurusClient};
use super::config::{YTsaurusPrimaryKeySemantics, YTsaurusSinkConfig, YTsaurusWriteFormat};
use super::native_rpc::NativeDynamicWriter;
use super::schema::{
    arrow_to_yt, parse_schema, schema_to_yt, schemas_equal, sorted_unique_schema_to_yt,
    validate_column_name, MAX_COLUMNS,
};
use super::yt_wire::encode_wire_batch;
use crate::metrics::SinkCounters;
use transferia_core::delivery::{
    validate_batch_against_discovery, validate_stored_projection, ArrowTypeFamily,
    DeliveryDiscovery, NameSyntax, SinkLimits, SinkLimitsDescription, SourceTopology, TextLimit,
};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::sink::{Delivery, Sink, SinkEvent, SinkIo};
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_registry::{SinkBuildContext, SinkConnector, SinkPrepare};

const MAX_STATIC_ROW_WEIGHT: usize = 128 * 1024 * 1024;

pub struct YTsaurusSinkConnector {
    config: Arc<YTsaurusSinkConfig>,
    client: YTsaurusClient,
}

impl YTsaurusSinkConnector {
    pub fn from_config(config: YTsaurusSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        let client = YTsaurusClient::new(&config.connection)?;
        Ok(Self {
            config: Arc::new(config),
            client,
        })
    }
}

impl SinkLimits for YTsaurusSinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        SinkLimitsDescription {
            sink: "ytsaurus",
            dataset_name: None,
            column_name: Some(TextLimit {
                syntax: NameSyntax::AnyNonEmptyUtf8,
                max_utf8_bytes: None,
            }),
            supported_arrow_types: vec![
                ArrowTypeFamily::Utf8,
                ArrowTypeFamily::Binary,
                ArrowTypeFamily::SignedInteger,
                ArrowTypeFamily::UnsignedInteger,
                ArrowTypeFamily::FloatingPoint,
                ArrowTypeFamily::Boolean,
                ArrowTypeFamily::Date32,
                ArrowTypeFamily::Date64,
                ArrowTypeFamily::Timestamp,
            ],
            object_key: None,
        }
    }

    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        anyhow::ensure!(
            !discovery.datasets.is_empty(),
            "YTsaurus sink requires at least one dataset"
        );
        let mut names = HashSet::new();
        let mut static_unique_sorted = false;
        for dataset in &discovery.datasets {
            anyhow::ensure!(
                names.insert(dataset.name.as_ref()),
                "delivery discovery repeats dataset '{}'",
                dataset.name
            );
            self.path_for_dataset(&dataset.name)?;
            validate_stored_projection(discovery, dataset)?;
            anyhow::ensure!(
                !dataset.stored_schema.columns.is_empty(),
                "YTsaurus table for dataset '{}' cannot have an empty schema",
                dataset.name
            );
            anyhow::ensure!(
                dataset.stored_schema.columns.len() <= MAX_COLUMNS,
                "YTsaurus table for dataset '{}' exceeds {MAX_COLUMNS} columns",
                dataset.name
            );
            for column in &dataset.stored_schema.columns {
                validate_column_name(&column.name)?;
                arrow_to_yt(&column.data_type)?;
            }
            let has_primary_key = dataset
                .stored_schema
                .columns
                .iter()
                .any(|column| column.primary_key);
            if self.static_tables()
                && self.primary_key_semantics == YTsaurusPrimaryKeySemantics::UniqueSorted
                && has_primary_key
            {
                static_unique_sorted = true;
                anyhow::ensure!(
                    self.replace_tables(),
                    "unique sorted primary-key semantics require replace_tables=true so the completed snapshot can atomically replace the destination"
                );
                sorted_unique_schema_to_yt(&dataset.stored_schema)?;
            }
            if !self.static_tables() {
                anyhow::ensure!(
                    self.primary_key_semantics == YTsaurusPrimaryKeySemantics::UniqueSorted,
                    "dynamic YTsaurus tables require unique_sorted primary-key semantics because duplicate keys are overwritten"
                );
                anyhow::ensure!(
                    has_primary_key,
                    "dynamic YTsaurus table for dataset '{}' requires at least one primary-key column",
                    dataset.name
                );
                sorted_unique_schema_to_yt(&dataset.stored_schema)?;
            }
        }
        if static_unique_sorted {
            anyhow::ensure!(
                matches!(
                    &discovery.source_topology,
                    SourceTopology::StaticPartitions(partitions) if partitions.len() == 1
                ),
                "unique sorted YTsaurus primary-key semantics require exactly one finite source partition so one atomic replacement contains the complete snapshot"
            );
        }
        Ok(())
    }

    fn validate_batch(
        &self,
        discovery: &DeliveryDiscovery,
        batch: &transferia_core::sink::SinkBatch,
    ) -> anyhow::Result<()> {
        validate_batch_against_discovery(discovery, batch)?;
        self.path_for_dataset(&batch.table)?;
        validate_row_weight(&batch.batch)?;
        Ok(())
    }
}

impl SinkConnector for YTsaurusSinkConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::YTsaurusSink
    }

    fn limits(&self) -> &dyn SinkLimits {
        self.config.as_ref()
    }

    fn destination_type(
        &self,
        column: &transferia_core::data::schema::SchemaColumn,
    ) -> anyhow::Result<String> {
        let data_type = arrow_to_yt(&column.data_type)?;
        Ok(if column.nullable {
            format!("optional<{data_type}>")
        } else {
            data_type.to_owned()
        })
    }

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            self.client.create_directory(self.config.path()).await?;
            for dataset in request.datasets {
                let unique_sorted = self.config.primary_key_semantics
                    == YTsaurusPrimaryKeySemantics::UniqueSorted
                    && dataset
                        .schema
                        .columns
                        .iter()
                        .any(|column| column.primary_key);
                if unique_sorted && self.config.static_tables() {
                    sorted_unique_schema_to_yt(&dataset.schema)?;
                    continue;
                }
                let path = self.config.path_for_dataset(&dataset.table)?;
                if self.config.replace_tables() {
                    self.client.remove_table(&path).await?;
                    if self.config.static_tables() {
                        self.client
                            .create_table(
                                &path,
                                schema_to_yt(&dataset.schema)?,
                                self.config.static_optimize_for().ok_or_else(|| {
                                    anyhow::anyhow!("static table has no optimize_for config")
                                })?,
                            )
                            .await?;
                    } else {
                        let write = self
                            .config
                            .dynamic_write()
                            .ok_or_else(|| anyhow::anyhow!("dynamic table has no write config"))?;
                        self.client
                            .create_dynamic_table(
                                &path,
                                sorted_unique_schema_to_yt(&dataset.schema)?,
                                self.config.tablet_cell_bundle(),
                                write.dynamic_store_overflow_threshold,
                            )
                            .await?;
                        self.client
                            .mount_table(
                                &path,
                                Duration::from_millis(self.config.connection.timeout_ms),
                            )
                            .await?;
                    }
                } else {
                    let dynamic = self
                        .client
                        .get_json(&super::attribute_path(&path, "dynamic"))
                        .await?;
                    anyhow::ensure!(
                        dynamic == serde_json::Value::Bool(!self.config.static_tables()),
                        "YTsaurus sink table '{path}' has a different static/dynamic mode than the configured destination"
                    );
                    let existing = parse_schema(
                        self.client
                            .get_json(&super::attribute_path(&path, "schema"))
                            .await?,
                    )?;
                    anyhow::ensure!(
                        if self.config.static_tables() {
                            schemas_equal(&existing, &dataset.schema)
                        } else {
                            dynamic_schemas_equal(&existing, &dataset.schema)
                        },
                        "YTsaurus sink table '{path}' schema differs from discovered dataset '{}'",
                        dataset.table
                    );
                    if !self.config.static_tables() {
                        let state = self
                            .client
                            .get_json(&super::attribute_path(&path, "tablet_state"))
                            .await?;
                        anyhow::ensure!(
                            state == serde_json::Value::String("mounted".to_owned()),
                            "YTsaurus dynamic sink table '{path}' must be mounted"
                        );
                    }
                }
            }
            Ok(())
        })
    }

    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            let static_unique_sorted = context.discovery.datasets.iter().any(|dataset| {
                self.config.static_tables()
                    && self.config.primary_key_semantics
                        == YTsaurusPrimaryKeySemantics::UniqueSorted
                    && dataset
                        .stored_schema
                        .columns
                        .iter()
                        .any(|column| column.primary_key)
            });
            anyhow::ensure!(
                !static_unique_sorted || context.finite_source,
                "unique sorted YTsaurus primary-key semantics require a finite snapshot source"
            );
            let dynamic_writer = if let Some(write) = self.config.dynamic_write() {
                Some(Arc::new(NativeDynamicWriter::new(
                    self.client.discover_rpc_endpoints().await?,
                    self.client.token().to_owned(),
                    write.transaction_concurrency,
                    Duration::from_millis(write.transaction_timeout_ms),
                    Duration::from_millis(write.retry_initial_ms),
                    Duration::from_millis(write.retry_max_ms),
                    Arc::clone(&context.counters),
                )?))
            } else {
                None
            };
            let limits: Arc<dyn SinkLimits> = Arc::clone(&self.config) as Arc<dyn SinkLimits>;
            Ok(Box::new(YTsaurusSink {
                client: self.client.clone(),
                dynamic_writer,
                config: Arc::clone(&self.config),
                counters: context.counters,
                discovery: context.discovery,
                limits,
                delivery_id: context.durable.delivery_id,
                partition_id: context.partition_id,
                attempt_id: Uuid::new_v4(),
            }) as Box<dyn Sink>)
        })
    }
}

struct YTsaurusSink {
    client: YTsaurusClient,
    dynamic_writer: Option<Arc<NativeDynamicWriter>>,
    config: Arc<YTsaurusSinkConfig>,
    counters: Arc<SinkCounters>,
    discovery: Arc<DeliveryDiscovery>,
    limits: Arc<dyn SinkLimits>,
    delivery_id: Arc<str>,
    partition_id: i64,
    attempt_id: Uuid,
}

impl YTsaurusSink {
    fn primary_keys(&self, table: &str) -> anyhow::Result<Vec<String>> {
        let dataset = self
            .discovery
            .datasets
            .iter()
            .find(|dataset| dataset.name.as_ref() == table)
            .ok_or_else(|| anyhow::anyhow!("YTsaurus discovery has no dataset '{table}'"))?;
        Ok(dataset
            .stored_schema
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .map(|column| column.name.clone())
            .collect())
    }

    fn unique_sorted(&self, table: &str) -> anyhow::Result<bool> {
        Ok(
            self.config.primary_key_semantics == YTsaurusPrimaryKeySemantics::UniqueSorted
                && !self.primary_keys(table)?.is_empty(),
        )
    }

    fn has_unique_sorted_tables(&self) -> bool {
        self.config.static_tables()
            && self.config.primary_key_semantics == YTsaurusPrimaryKeySemantics::UniqueSorted
            && self.discovery.datasets.iter().any(|dataset| {
                dataset
                    .stored_schema
                    .columns
                    .iter()
                    .any(|column| column.primary_key)
            })
    }

    fn internal_table_path(&self, table: &str, purpose: &str) -> anyhow::Result<String> {
        self.config.path_for_dataset(table)?;
        let mut digest = Sha256::new();
        digest.update(self.delivery_id.as_bytes());
        digest.update(self.partition_id.to_le_bytes());
        digest.update(self.attempt_id.as_bytes());
        digest.update(table.as_bytes());
        let digest = format!("{:x}", digest.finalize());
        Ok(format!(
            "{}/.transferia-{purpose}-{}-{table}",
            self.config.path().trim_end_matches('/'),
            &digest[..16]
        ))
    }

    fn staging_path(&self, table: &str) -> anyhow::Result<String> {
        self.internal_table_path(table, "stage")
    }

    fn sorted_path(&self, table: &str) -> anyhow::Result<String> {
        self.internal_table_path(table, "sorted")
    }

    fn sort_mutation_id(&self, table: &str) -> String {
        let mut digest = Sha256::new();
        digest.update(b"transferia-ytsaurus-unique-sort");
        digest.update(self.delivery_id.as_bytes());
        digest.update(self.partition_id.to_le_bytes());
        digest.update(self.attempt_id.as_bytes());
        digest.update(table.as_bytes());
        let bytes = digest.finalize();
        let mut id = [0_u8; 16];
        id.copy_from_slice(&bytes[..16]);
        yt_guid(id)
    }

    async fn prepare_unique_staging(&self) -> anyhow::Result<()> {
        for dataset in &self.discovery.datasets {
            if !self.unique_sorted(&dataset.name)? {
                continue;
            }
            let staging = self.staging_path(&dataset.name)?;
            let sorted = self.sorted_path(&dataset.name)?;
            self.client.remove_table(&staging).await?;
            self.client.remove_table(&sorted).await?;
            self.client
                .create_table(
                    &staging,
                    schema_to_yt(&dataset.stored_schema)?,
                    self.config.static_optimize_for().ok_or_else(|| {
                        anyhow::anyhow!("static table has no optimize_for config")
                    })?,
                )
                .await?;
            self.client
                .create_table(
                    &sorted,
                    sorted_unique_schema_to_yt(&dataset.stored_schema)?,
                    self.config.static_optimize_for().ok_or_else(|| {
                        anyhow::anyhow!("static table has no optimize_for config")
                    })?,
                )
                .await?;
        }
        Ok(())
    }

    async fn finalize_unique_tables(&self) -> anyhow::Result<()> {
        for dataset in &self.discovery.datasets {
            if !self.unique_sorted(&dataset.name)? {
                continue;
            }
            let staging = self.staging_path(&dataset.name)?;
            let sorted = self.sorted_path(&dataset.name)?;
            let destination = self.config.path_for_dataset(&dataset.name)?;
            self.client
                .sort_table_unique(
                    &staging,
                    &sorted,
                    &self.primary_keys(&dataset.name)?,
                    &self.sort_mutation_id(&dataset.name),
                    Duration::from_millis(self.config.primary_key_sort_timeout_ms),
                )
                .await?;
            self.client.move_table(&sorted, &destination).await?;
            self.client.remove_table(&staging).await?;
        }
        Ok(())
    }

    async fn write_table_batches(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
    ) -> anyhow::Result<()> {
        if let Some(writer) = self.dynamic_writer.as_ref() {
            return self.write_dynamic_batches(writer, table, batches).await;
        }
        let destination_path = if self.unique_sorted(table)? {
            self.staging_path(table)?
        } else {
            self.config.path_for_dataset(table)?
        };
        let configured_format = self
            .config
            .static_format()
            .ok_or_else(|| anyhow::anyhow!("static YTsaurus writer has no exchange format"))?;
        let concurrency = self.config.write_concurrency.min(batches.len());
        if concurrency <= 1 {
            let (format, payload) = encode_batches(configured_format, &batches)?;
            return self
                .client
                .write_table(
                    &destination_path,
                    format,
                    payload,
                    self.config.write_row_buffer_bytes,
                    &self.config.table_writer,
                )
                .await;
        }

        let shard_size = batches.len().div_ceil(concurrency);
        let shards = batches
            .chunks(shard_size)
            .map(<[RecordBatch]>::to_vec)
            .collect::<Vec<_>>();
        let session = self
            .client
            .start_distributed_write(
                &destination_path,
                shards.len(),
                self.config.connection.timeout_ms,
            )
            .await?;
        let (session, cookies) = session.into_parts();
        let results = stream::iter(shards.into_iter().zip(cookies))
            .map(|(shard, cookie)| {
                let client = self.client.clone();
                let table_writer = self.config.table_writer.clone();
                let format = configured_format;
                let row_buffer_bytes = self.config.write_row_buffer_bytes;
                async move {
                    let (format, payload) = encode_batches(format, &shard)?;
                    client
                        .write_table_fragment(
                            cookie,
                            format,
                            payload,
                            row_buffer_bytes,
                            &table_writer,
                        )
                        .await
                }
            })
            .buffered(concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.client.finish_distributed_write(session, results).await
    }

    async fn write_dynamic_batches(
        &self,
        writer: &Arc<NativeDynamicWriter>,
        table: &str,
        batches: Vec<RecordBatch>,
    ) -> anyhow::Result<()> {
        let config = self
            .config
            .dynamic_write()
            .ok_or_else(|| anyhow::anyhow!("dynamic YTsaurus writer has no transaction config"))?;
        let destination_path = self.config.path_for_dataset(table)?;
        let mut chunks = Vec::new();
        for batch in batches {
            let mut offset = 0;
            while offset < batch.num_rows() {
                let length = config.transaction_rows.min(batch.num_rows() - offset);
                chunks.push(batch.slice(offset, length));
                offset += length;
            }
        }
        let concurrency = config.transaction_concurrency.min(chunks.len());
        stream::iter(chunks)
            .map(|batch| {
                let writer = Arc::clone(writer);
                let path = destination_path.clone();
                let require_sync_replica = config.require_sync_replica;
                async move {
                    let row_count = batch.num_rows();
                    let encoded = tokio::task::spawn_blocking(move || encode_wire_batch(&batch))
                        .await
                        .map_err(|error| {
                            anyhow::anyhow!("YTsaurus wire encoder task failed: {error}")
                        })??;
                    writer
                        .write_rows(
                            &path,
                            row_count,
                            &encoded.column_names,
                            encoded.payload,
                            require_sync_replica,
                        )
                        .await
                }
            })
            .buffer_unordered(concurrency.max(1))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(())
    }

    async fn write_deliveries(&self, deliveries: &[Delivery]) -> anyhow::Result<()> {
        let mut tables = Vec::<(Arc<str>, Vec<RecordBatch>, u64, u64)>::new();
        for delivery in deliveries {
            for batch in &delivery.outputs {
                self.limits
                    .validate_batch(&self.discovery, batch)
                    .map_err(DataPlaneFailure::fatal)?;
                if batch.rows() == 0 {
                    continue;
                }
                let stored = if self.discovery.keep_system_columns {
                    batch.batch.clone()
                } else {
                    project_user_columns(&batch.batch, &batch.system_columns)?
                };
                let index = tables
                    .iter()
                    .position(|(table, _, _, _)| table.as_ref() == batch.table.as_ref())
                    .unwrap_or_else(|| {
                        tables.push((Arc::clone(&batch.table), Vec::new(), 0, 0));
                        tables.len() - 1
                    });
                let table = &mut tables[index];
                table.1.push(stored);
                table.2 = table.2.saturating_add(batch.rows() as u64);
                table.3 = table.3.saturating_add(batch.bytes() as u64);
            }
        }
        for (table, batches, rows, bytes) in tables {
            let started = Instant::now();
            self.write_table_batches(&table, batches)
                .await
                .map_err(classify_http_failure)?;
            self.counters.add_busy(started.elapsed());
            self.counters.add_rows(rows);
            self.counters.add_bytes(bytes);
            self.counters.add_flush();
        }
        Ok(())
    }

    async fn flush_pending(
        &self,
        pending: &mut Vec<Delivery>,
        events: &mpsc::Sender<SinkEvent>,
        deferred_commit: &mut Option<transferia_core::sink::DeliveryId>,
    ) -> anyhow::Result<()> {
        let Some(last) = pending.last() else {
            return Ok(());
        };
        let id = last.id;
        let source_messages = pending
            .iter()
            .map(|delivery| delivery.meta.source_messages)
            .sum();
        self.write_deliveries(pending).await?;
        self.counters.add_source_messages(source_messages);
        if self.has_unique_sorted_tables() {
            *deferred_commit = Some(id);
        } else {
            events
                .send(SinkEvent::CommittedThrough(id))
                .await
                .map_err(|_| anyhow::anyhow!("YTsaurus sink event receiver closed"))?;
        }
        pending.clear();
        self.counters.set_buffered_bytes(0);
        Ok(())
    }
}

pub(super) fn yt_guid(bytes: [u8; 16]) -> String {
    let first = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let second = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    let third = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    let fourth = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
    format!("{fourth:x}-{third:x}-{second:x}-{first:x}")
}

fn encode_batches(
    format: YTsaurusWriteFormat,
    batches: &[RecordBatch],
) -> anyhow::Result<(&'static str, Vec<u8>)> {
    match format {
        YTsaurusWriteFormat::Arrow => Ok(("arrow", encode_arrow_batches(batches)?)),
        YTsaurusWriteFormat::Yson => Ok(("yson", encode_yson_batches(batches)?)),
    }
}

impl Sink for YTsaurusSink {
    fn run(
        self: Box<Self>,
        mut io: SinkIo,
    ) -> BoxFuture<'static, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            let unique_sorted = self.has_unique_sorted_tables();
            let result: anyhow::Result<()> = async {
                let mut pending = Vec::<Delivery>::new();
                let mut buffered_bytes = 0_usize;
                let mut deferred_commit = None;
                if self.has_unique_sorted_tables() {
                    self.prepare_unique_staging().await?;
                }
                loop {
                    let delivery = if pending.is_empty() {
                        tokio::select! {
                            biased;
                            () = io.cancellation.cancelled() => None,
                            delivery = io.deliveries.recv() => delivery,
                        }
                    } else {
                        tokio::select! {
                            biased;
                            () = io.cancellation.cancelled() => None,
                            delivery = tokio::time::timeout(
                                Duration::from_millis(self.config.write_flush_interval_ms),
                                io.deliveries.recv(),
                            ) => match delivery {
                                Ok(delivery) => delivery,
                                Err(_) => {
                                    self.flush_pending(
                                        &mut pending,
                                        &io.events,
                                        &mut deferred_commit,
                                    )
                                    .await?;
                                    buffered_bytes = 0;
                                    continue;
                                }
                            },
                        }
                    };
                    let Some(delivery) = delivery else {
                        if io.cancellation.is_cancelled() {
                            self.counters.set_buffered_bytes(0);
                            return Ok(());
                        }
                        self.flush_pending(&mut pending, &io.events, &mut deferred_commit)
                            .await?;
                        if self.has_unique_sorted_tables() {
                            self.finalize_unique_tables().await.map_err(|error| {
                                anyhow::Error::from(DataPlaneFailure::fatal(error))
                            })?;
                            if let Some(id) = deferred_commit {
                                io.events
                                    .send(SinkEvent::CommittedThrough(id))
                                    .await
                                    .map_err(|_| {
                                        anyhow::anyhow!(
                                            "YTsaurus sink event receiver closed after primary-key finalization"
                                        )
                                    })?;
                            }
                        }
                        break;
                    };
                    buffered_bytes = buffered_bytes.saturating_add(
                        delivery
                            .outputs
                            .iter()
                            .map(transferia_core::SinkBatch::bytes)
                            .sum::<usize>(),
                    );
                    pending.push(delivery);
                    self.counters.set_buffered_bytes(buffered_bytes as u64);
                    let write_target_bytes = self
                        .config
                        .dynamic_write()
                        .map_or(self.config.write_target_bytes, |write| write.buffer_bytes);
                    if buffered_bytes < write_target_bytes {
                        continue;
                    }
                    self.flush_pending(&mut pending, &io.events, &mut deferred_commit)
                        .await?;
                    buffered_bytes = 0;
                }
                Ok(())
            }
            .await;
            if unique_sorted {
                result.map_err(DataPlaneFailure::fatal_or_passthrough)
            } else {
                result.map_err(DataPlaneFailure::retryable_or_passthrough)
            }
        })
    }
}

fn project_user_columns(
    batch: &RecordBatch,
    system_columns: &transferia_core::data::system_columns::SystemColumns,
) -> anyhow::Result<RecordBatch> {
    let system_indexes = system_columns
        .iter()
        .map(|column| column.index)
        .collect::<HashSet<_>>();
    let indexes = (0..batch.num_columns())
        .filter(|index| !system_indexes.contains(index))
        .collect::<Vec<_>>();
    Ok(batch.project(&indexes)?)
}

fn dynamic_schemas_equal(
    existing: &transferia_core::data::schema::DatasetSchema,
    expected: &transferia_core::data::schema::DatasetSchema,
) -> bool {
    existing.columns.len() == expected.columns.len()
        && expected.columns.iter().all(|expected_column| {
            existing.columns.iter().any(|existing_column| {
                existing_column.name == expected_column.name
                    && existing_column.data_type == expected_column.data_type
                    && existing_column.nullable == expected_column.nullable
                    && existing_column.primary_key == expected_column.primary_key
            })
        })
}

#[cfg(test)]
pub(super) fn encode_arrow(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    encode_arrow_batches(std::slice::from_ref(batch))
}

pub(super) fn encode_arrow_batches(batches: &[RecordBatch]) -> anyhow::Result<Vec<u8>> {
    let batch = batches
        .first()
        .ok_or_else(|| anyhow::anyhow!("cannot encode an empty Arrow batch list"))?;
    // YTsaurus maps the physical Arrow type to its table schema and rejects
    // Arrow extension annotations (for example `arrow.json`) even when their
    // storage type is a supported lossless Utf8 value.  Keep application
    // metadata in discovery, but send only the physical wire schema expected by
    // YTsaurus.  Values, nullability, names, and physical types are unchanged.
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let mut metadata = field.metadata().clone();
            metadata.retain(|key, _| !key.starts_with("ARROW:extension:"));
            field.as_ref().clone().with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    let wire_schema = Arc::new(Schema::new_with_metadata(
        fields,
        batch.schema().metadata().clone(),
    ));
    let mut output =
        Vec::with_capacity(batches.iter().map(RecordBatch::get_array_memory_size).sum());
    {
        let mut writer = StreamWriter::try_new(&mut output, &wire_schema)?;
        for batch in batches {
            let wire_batch =
                RecordBatch::try_new(Arc::clone(&wire_schema), batch.columns().to_vec())?;
            writer.write(&wire_batch)?;
        }
        writer.finish()?;
    }
    Ok(output)
}

#[cfg(test)]
pub(super) fn encode_yson(batch: &RecordBatch) -> anyhow::Result<Vec<u8>> {
    encode_yson_batches(std::slice::from_ref(batch))
}

pub(super) fn encode_yson_batches(batches: &[RecordBatch]) -> anyhow::Result<Vec<u8>> {
    let mut output =
        Vec::with_capacity(batches.iter().map(RecordBatch::get_array_memory_size).sum());
    for batch in batches {
        append_yson(&mut output, batch)?;
    }
    Ok(output)
}

fn append_yson(output: &mut Vec<u8>, batch: &RecordBatch) -> anyhow::Result<()> {
    for row in 0..batch.num_rows() {
        output.push(b'{');
        for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
            write_yson_string(output, field.name().as_bytes());
            output.push(b'=');
            write_yson_value(output, array.as_ref(), row)?;
            output.push(b';');
        }
        output.extend_from_slice(b"};");
    }
    Ok(())
}

fn write_yson_string(output: &mut Vec<u8>, value: &[u8]) {
    output.push(b'"');
    for byte in value {
        match *byte {
            b'"' | b'\\' => {
                output.push(b'\\');
                output.push(*byte);
            }
            b'\n' => output.extend_from_slice(b"\\n"),
            b'\r' => output.extend_from_slice(b"\\r"),
            b'\t' => output.extend_from_slice(b"\\t"),
            0x20..=0x7e => output.push(*byte),
            other => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                output.extend_from_slice(b"\\x");
                output.push(HEX[usize::from(other >> 4)]);
                output.push(HEX[usize::from(other & 0x0f)]);
            }
        }
    }
    output.push(b'"');
}

macro_rules! primitive_yson {
    ($array:expr_2021, $ty:ty, $row:expr_2021, $output:expr_2021) => {{
        let value = $array
            .as_any()
            .downcast_ref::<$ty>()
            .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
            .value($row);
        $output.extend_from_slice(value.to_string().as_bytes());
    }};
}

fn write_yson_value(output: &mut Vec<u8>, array: &dyn Array, row: usize) -> anyhow::Result<()> {
    if array.is_null(row) {
        output.push(b'#');
        return Ok(());
    }
    match array.data_type() {
        DataType::Boolean => {
            let value = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
                .value(row);
            output.extend_from_slice(if value { b"%true" } else { b"%false" });
        }
        DataType::Int8 => primitive_yson!(array, Int8Array, row, output),
        DataType::Int16 => primitive_yson!(array, Int16Array, row, output),
        DataType::Int32 => primitive_yson!(array, Int32Array, row, output),
        DataType::Int64 => primitive_yson!(array, Int64Array, row, output),
        DataType::UInt8 => {
            primitive_yson!(array, UInt8Array, row, output);
            output.push(b'u');
        }
        DataType::UInt16 => {
            primitive_yson!(array, UInt16Array, row, output);
            output.push(b'u');
        }
        DataType::UInt32 => {
            primitive_yson!(array, UInt32Array, row, output);
            output.push(b'u');
        }
        DataType::UInt64 => {
            primitive_yson!(array, UInt64Array, row, output);
            output.push(b'u');
        }
        DataType::Float32 => primitive_yson!(array, Float32Array, row, output),
        DataType::Float64 => primitive_yson!(array, Float64Array, row, output),
        DataType::Utf8 => write_yson_string(
            output,
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
                .value(row)
                .as_bytes(),
        ),
        DataType::Binary => write_yson_string(
            output,
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
                .value(row),
        ),
        DataType::LargeUtf8 => write_yson_string(
            output,
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
                .value(row)
                .as_bytes(),
        ),
        DataType::LargeBinary => write_yson_string(
            output,
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
                .value(row),
        ),
        DataType::Date32 => primitive_yson!(array, Date32Array, row, output),
        DataType::Date64 => primitive_yson!(array, Date64Array, row, output),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            primitive_yson!(array, TimestampMicrosecondArray, row, output);
            output.push(b'u');
        }
        other => anyhow::bail!("Arrow type {other:?} is not supported by YSON writer"),
    }
    Ok(())
}

pub(super) fn validate_row_weight(batch: &RecordBatch) -> anyhow::Result<()> {
    for row in 0..batch.num_rows() {
        let mut weight = 0_usize;
        for array in batch.columns() {
            if array.is_null(row) {
                continue;
            }
            weight = weight.saturating_add(match array.data_type() {
                DataType::Boolean | DataType::Int8 | DataType::UInt8 => 1,
                DataType::Int16 | DataType::UInt16 => 2,
                DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => 4,
                DataType::Int64
                | DataType::UInt64
                | DataType::Float64
                | DataType::Date64
                | DataType::Timestamp(TimeUnit::Microsecond, None) => 8,
                DataType::Utf8 => array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
                    .value_length(row) as usize,
                DataType::Binary => array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
                    .value_length(row) as usize,
                DataType::LargeUtf8 => array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
                    .value_length(row) as usize,
                DataType::LargeBinary => array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .ok_or_else(|| anyhow::anyhow!("Arrow array type does not match schema"))?
                    .value_length(row) as usize,
                other => anyhow::bail!("Arrow type {other:?} is not supported by YTsaurus sink"),
            });
        }
        anyhow::ensure!(
            weight <= MAX_STATIC_ROW_WEIGHT,
            "YTsaurus row {row} weight {weight} exceeds {MAX_STATIC_ROW_WEIGHT} bytes"
        );
    }
    Ok(())
}
