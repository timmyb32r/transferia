use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::SinkExt as _;

use super::copy_binary;
use crate::connectors::postgres::common::quote_identifier;
use crate::metrics::SinkCounters;
use transferia_core::delivery::{DeliveryDiscovery, SinkLimits};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::sink::{Delivery, Sink, SinkEvent, SinkIo};
use transferia_core::{project_sink_batch, ProjectedSinkBatch};

pub struct PostgresSink {
    client: tokio_postgres::Client,
    counters: Arc<SinkCounters>,
    discovery: Arc<DeliveryDiscovery>,
    limits: Arc<dyn SinkLimits>,
}

impl PostgresSink {
    pub fn new(
        client: tokio_postgres::Client,
        counters: Arc<SinkCounters>,
        discovery: Arc<DeliveryDiscovery>,
        limits: Arc<dyn SinkLimits>,
    ) -> Self {
        Self {
            client,
            counters,
            discovery,
            limits,
        }
    }

    async fn write_delivery(&mut self, delivery: &Delivery) -> anyhow::Result<()> {
        for batch in &delivery.outputs {
            self.limits
                .validate_batch(&self.discovery, batch)
                .map_err(DataPlaneFailure::fatal)?;
        }
        let projected = delivery
            .outputs
            .iter()
            .map(|batch| project_sink_batch(&self.discovery, batch))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let started = std::time::Instant::now();
        let transaction = self.client.transaction().await?;
        let mut flushes = 0;
        for (batch_index, (batch, projected)) in
            delivery.outputs.iter().zip(projected).enumerate()
        {
            match projected {
                ProjectedSinkBatch::AppendOnly(stored) => {
                    if stored.num_rows() > 0 {
                        copy_batch(&transaction, &batch.table, &stored).await?;
                        flushes += 1;
                    }
                }
                ProjectedSinkBatch::Changelog(changelog) => {
                    for (run_index, run) in
                        changelog.collapsed_runs()?.into_iter().enumerate()
                    {
                        if run.batch.num_rows() == 0 {
                            continue;
                        }
                        let staging = format!(
                            "__transferia_{}_{}_{}",
                            delivery.id.get(),
                            batch_index,
                            run_index
                        );
                        create_staging_table(
                            &transaction,
                            &batch.table,
                            &staging,
                            run.operation,
                            &run.batch,
                        )
                        .await?;
                        copy_batch(&transaction, &staging, &run.batch).await?;
                        match run.operation {
                            transferia_core::ChangeOperation::Create
                            | transferia_core::ChangeOperation::SnapshotRead => {
                                upsert_from_staging(
                                    &transaction,
                                    &batch.table,
                                    &staging,
                                    &run.batch,
                                    &changelog.primary_keys,
                                )
                                .await?;
                            }
                            transferia_core::ChangeOperation::Update => {
                                update_from_staging(
                                    &transaction,
                                    &batch.table,
                                    &staging,
                                    &run.batch,
                                    &changelog.primary_keys,
                                )
                                .await?;
                            }
                            transferia_core::ChangeOperation::Delete => {
                                delete_from_staging(
                                    &transaction,
                                    &batch.table,
                                    &staging,
                                    &changelog.primary_keys,
                                )
                                .await?;
                            }
                        }
                        flushes += 1;
                    }
                }
            }
        }
        transaction.commit().await?;
        self.counters.add_busy(started.elapsed());
        self.counters
            .add_rows(delivery.outputs.iter().map(|batch| batch.rows() as u64).sum());
        self.counters
            .add_bytes(delivery.outputs.iter().map(|batch| batch.bytes() as u64).sum());
        for _ in 0..flushes {
            self.counters.add_flush();
        }
        Ok(())
    }
}

async fn create_staging_table(
    transaction: &tokio_postgres::Transaction<'_>,
    table: &str,
    staging: &str,
    operation: transferia_core::ChangeOperation,
    batch: &arrow::record_batch::RecordBatch,
) -> anyhow::Result<()> {
    let query = match operation {
        transferia_core::ChangeOperation::Create
        | transferia_core::ChangeOperation::SnapshotRead => format!(
            "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP",
            quote_identifier(staging),
            quote_identifier(table)
        ),
        transferia_core::ChangeOperation::Update | transferia_core::ChangeOperation::Delete => format!(
            "CREATE TEMP TABLE {} ON COMMIT DROP AS SELECT {} FROM {} WITH NO DATA",
            quote_identifier(staging),
            batch
                .schema()
                .fields()
                .iter()
                .map(|field| quote_identifier(field.name()))
                .collect::<Vec<_>>()
                .join(", "),
            quote_identifier(table)
        ),
    };
    transaction.batch_execute(&query).await?;
    Ok(())
}

async fn update_from_staging(
    transaction: &tokio_postgres::Transaction<'_>,
    table: &str,
    staging: &str,
    batch: &arrow::record_batch::RecordBatch,
    primary_keys: &[String],
) -> anyhow::Result<()> {
    let updates = batch
        .schema()
        .fields()
        .iter()
        .filter(|field| !primary_keys.iter().any(|key| key == field.name()))
        .map(|field| {
            let column = quote_identifier(field.name());
            format!("{column} = staged.{column}")
        })
        .collect::<Vec<_>>();
    let predicate = primary_keys
        .iter()
        .map(|key| {
            let key = quote_identifier(key);
            format!("target.{key} = staged.{key}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let affected = if updates.is_empty() {
        let row = transaction
            .query_one(
                &format!(
                    "SELECT count(*)::bigint FROM {} AS target JOIN {} AS staged ON {predicate}",
                    quote_identifier(table),
                    quote_identifier(staging)
                ),
                &[],
            )
            .await?;
        u64::try_from(row.get::<_, i64>(0))?
    } else {
        transaction
            .execute(
                &format!(
                    "UPDATE {} AS target SET {} FROM {} AS staged WHERE {predicate}",
                    quote_identifier(table),
                    updates.join(", "),
                    quote_identifier(staging)
                ),
                &[],
            )
            .await?
    };
    anyhow::ensure!(
        affected == batch.num_rows() as u64,
        "PostgreSQL UPDATE matched {affected} rows, expected {}; destination state is incomplete",
        batch.num_rows()
    );
    Ok(())
}

async fn copy_batch(
    transaction: &tokio_postgres::Transaction<'_>,
    table: &str,
    batch: &arrow::record_batch::RecordBatch,
) -> anyhow::Result<()> {
    let columns = batch
        .schema()
        .fields()
        .iter()
        .map(|field| quote_identifier(field.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "COPY {} ({columns}) FROM STDIN BINARY",
        quote_identifier(table)
    );
    let payload = copy_binary::encode(batch).map_err(DataPlaneFailure::fatal)?;
    let sink = transaction.copy_in(&query).await?;
    tokio::pin!(sink);
    sink.as_mut().send(payload).await?;
    let rows = sink.as_mut().finish().await?;
    anyhow::ensure!(
        rows == batch.num_rows() as u64,
        "PostgreSQL COPY inserted {rows} rows, expected {}",
        batch.num_rows()
    );
    Ok(())
}

async fn upsert_from_staging(
    transaction: &tokio_postgres::Transaction<'_>,
    table: &str,
    staging: &str,
    batch: &arrow::record_batch::RecordBatch,
    primary_keys: &[String],
) -> anyhow::Result<()> {
    let columns = batch
        .schema()
        .fields()
        .iter()
        .map(|field| quote_identifier(field.name()))
        .collect::<Vec<_>>();
    let updates = batch
        .schema()
        .fields()
        .iter()
        .filter(|field| !primary_keys.iter().any(|key| key == field.name()))
        .map(|field| {
            let column = quote_identifier(field.name());
            format!("{column} = EXCLUDED.{column}")
        })
        .collect::<Vec<_>>();
    let conflict = if updates.is_empty() {
        "DO NOTHING".to_owned()
    } else {
        format!("DO UPDATE SET {}", updates.join(", "))
    };
    transaction
        .execute(
            &format!(
                "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) {conflict}",
                quote_identifier(table),
                columns.join(", "),
                columns.join(", "),
                quote_identifier(staging),
                primary_keys
                    .iter()
                    .map(|key| quote_identifier(key))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            &[],
        )
        .await?;
    Ok(())
}

async fn delete_from_staging(
    transaction: &tokio_postgres::Transaction<'_>,
    table: &str,
    staging: &str,
    primary_keys: &[String],
) -> anyhow::Result<()> {
    let predicate = primary_keys
        .iter()
        .map(|key| {
            let key = quote_identifier(key);
            format!("target.{key} = staged.{key}")
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    transaction
        .execute(
            &format!(
                "DELETE FROM {} AS target USING {} AS staged WHERE {predicate}",
                quote_identifier(table),
                quote_identifier(staging)
            ),
            &[],
        )
        .await?;
    Ok(())
}

impl Sink for PostgresSink {
    fn run(
        mut self: Box<Self>,
        mut io: SinkIo,
    ) -> BoxFuture<'static, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            let result: anyhow::Result<()> = async {
                while let Some(delivery) = tokio::select! { biased; () = io.cancellation.cancelled() => None, delivery = io.deliveries.recv() => delivery }
                {
                    let id = delivery.id;
                    let source_messages = delivery.meta.source_messages;
                    self.write_delivery(&delivery).await?;
                    self.counters.add_source_messages(source_messages);
                    io.events
                        .send(SinkEvent::CommittedThrough(id))
                        .await
                        .map_err(|_| anyhow::anyhow!("PostgreSQL sink event receiver closed"))?;
                }
                Ok(())
            }
            .await;
            result.map_err(DataPlaneFailure::retryable_or_passthrough)
        })
    }
}
