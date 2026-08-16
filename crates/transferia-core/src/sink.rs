use alloc::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::data::system_columns::SystemColumns;
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

#[derive(Debug)]
pub struct SinkBatch {
    pub table: Arc<str>,
    pub is_dlq: bool,
    pub batch: RecordBatch,
    pub byte_size: usize,
    pub memory: MemoryReservation,
    pub system_columns: SystemColumns,
}

impl SinkBatch {
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
pub trait Sink: Send {
    fn run(self: Box<Self>, io: SinkIo) -> BoxFuture<'static, anyhow::Result<()>>;
}
