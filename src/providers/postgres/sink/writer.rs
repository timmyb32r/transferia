use std::sync::Arc;

use futures_util::future::BoxFuture;
use futures_util::SinkExt as _;

use super::copy_binary;
use crate::delivery::execution::sink::{Delivery, Sink, SinkEvent, SinkIo};
use crate::delivery::execution::PipelineFailure;
use crate::delivery::{DeliveryDiscovery, SinkLimits};
use crate::metrics::SinkCounters;
use crate::providers::postgres::common::quote_identifier;
use crate::types::system_columns::SystemColumns;

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

    async fn write_delivery(&self, delivery: &Delivery) -> anyhow::Result<()> {
        for batch in &delivery.outputs {
            self.limits
                .validate_batch(&self.discovery, batch)
                .map_err(PipelineFailure::fatal)?;
        }
        for batch in &delivery.outputs {
            if batch.rows() == 0 {
                continue;
            }
            let stored_batch = if self.discovery.keep_system_columns {
                batch.batch.clone()
            } else {
                without_system_columns(&batch.batch, &batch.system_columns)?
            };
            let columns = stored_batch
                .schema()
                .fields()
                .iter()
                .map(|field| quote_identifier(field.name()))
                .collect::<Vec<_>>()
                .join(", ");
            let query = format!(
                "COPY {} ({columns}) FROM STDIN BINARY",
                quote_identifier(&batch.table)
            );
            let payload = copy_binary::encode(&stored_batch).map_err(PipelineFailure::fatal)?;
            let started = std::time::Instant::now();
            let sink = self.client.copy_in(&query).await?;
            tokio::pin!(sink);
            sink.as_mut().send(payload).await?;
            let rows = sink.as_mut().finish().await?;
            anyhow::ensure!(
                rows == batch.rows() as u64,
                "PostgreSQL COPY inserted {rows} rows, expected {}",
                batch.rows()
            );
            self.counters.add_busy(started.elapsed());
            self.counters.add_rows(rows);
            self.counters.add_bytes(batch.bytes() as u64);
            self.counters.add_flush();
        }
        Ok(())
    }
}

fn without_system_columns(
    batch: &arrow::record_batch::RecordBatch,
    system_columns: &SystemColumns,
) -> anyhow::Result<arrow::record_batch::RecordBatch> {
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

impl Sink for PostgresSink {
    fn run(self: Box<Self>, mut io: SinkIo) -> BoxFuture<'static, anyhow::Result<()>> {
        Box::pin(async move {
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
        })
    }
}
