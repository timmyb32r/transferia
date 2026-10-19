use alloc::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::data::system_columns::SystemColumns;
use crate::failure::DataPlaneResult;
use crate::memory::{MemoryReservation, PipelineMemory};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeliveryId(u64);

impl DeliveryId {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DeliveryMeta {
    pub source_messages: u64,
}

/// Sink input DTO. A receiver must validate the carried schema, Arrow payload
/// and destination constraints before accepting it into operational buffers.
#[derive(Debug)]
pub struct SinkBatch {
    /// Exact logical schema of this batch. Destination compatibility is decided by the sink.
    pub schema: Arc<crate::DatasetSchema>,
    pub namespace: Option<Arc<str>>,
    pub table: Arc<str>,
    pub is_dlq: bool,
    pub batch: RecordBatch,
    pub byte_size: usize,
    pub memory: MemoryReservation,
    pub system_columns: SystemColumns,
}

impl SinkBatch {
    /// Describe this input at the sink boundary. Control metadata is never
    /// projected into stored user columns. Destination compatibility is not
    /// inferred here; the configured sink owns that decision.
    pub fn dataset(&self, keep_system_columns: bool) -> anyhow::Result<crate::DiscoveredDataset> {
        let actual = crate::DatasetSchema::from_arrow(self.batch.schema().as_ref())?;
        anyhow::ensure!(actual == *self.schema, "batch '{}' does not match its carried logical schema", self.table);
        let stored = self.schema.columns.iter().filter(|column| {
            if column.system_role.is_some() || column.old_value_of.is_some() || column.old_key_of.is_some() {
                return false;
            }
            match self.system_columns.iter().find(|system| system.name.as_ref() == column.name) {
                Some(system) => keep_system_columns && !matches!(system.kind, crate::SystemColumnKind::ChangeOperation | crate::SystemColumnKind::ChangedColumns),
                None => true,
            }
        }).cloned().collect();
        Ok(crate::DiscoveredDataset {
            namespace: self.namespace.clone(),
            name: Arc::clone(&self.table),
            role: crate::DatasetRole::from_is_dlq(self.is_dlq),
            update_policy: crate::delivery::UpdatePolicy::Strict,
            incoming_schema: self.schema.as_ref().clone(),
            stored_schema: crate::DatasetSchema::new(stored),
            system_columns: self.system_columns.iter().map(|column| crate::DiscoveredSystemColumn {
                kind: column.kind, name: Arc::clone(&column.name),
            }).collect(),
        })
    }

    #[must_use]
    pub fn rows(&self) -> usize {
        self.batch.num_rows()
    }

    #[must_use]
    pub const fn bytes(&self) -> usize {
        self.byte_size
    }
}

#[derive(Debug)]
pub struct Delivery {
    pub id: DeliveryId,
    pub outputs: Vec<SinkBatch>,
    pub meta: DeliveryMeta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkEvent {
    CommittedThrough(DeliveryId),
}

pub struct SinkIo {
    pub deliveries: mpsc::Receiver<Delivery>,
    pub events: mpsc::Sender<SinkEvent>,
    pub memory: PipelineMemory,
    pub cancellation: CancellationToken,
}

/// A sink is a long-lived actor. Receiving a [`Delivery`] transfers ownership;
/// durability is reported independently through [`SinkEvent`].
pub trait Sink: Send + 'static {
    /// Run one input session. Closing its channel drains all writes; the sink
    /// remains alive so a new dataset can be installed without reconnecting.
    fn run_session(&mut self, io: SinkIo) -> BoxFuture<'_, DataPlaneResult<()>>;

    /// Install a validated and prepared catalog between fully drained sessions.
    /// This updates contracts only; it must never replace destination contents.
    fn update_discovery(&mut self, discovery: Arc<crate::DeliveryDiscovery>) -> DataPlaneResult<()>;

    fn run(mut self: Box<Self>, io: SinkIo) -> BoxFuture<'static, DataPlaneResult<()>> {
        Box::pin(async move { self.run_session(io).await })
    }
}
