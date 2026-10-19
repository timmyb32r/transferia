use futures_util::future::BoxFuture;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};
use transferia_core::sink::{Delivery, DeliveryId, Sink, SinkEvent, SinkIo};
use transferia_core::{DeliveryDiscovery, DiscoveredDataset, PipelineMemory};

/// Orchestration-owned schema validation and preparation. Called only after
/// the current sink session has drained all writes. The sink object remains alive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatasetStage {
    Source,
    Sink,
}

pub trait DatasetAdmission: Send {
    fn pending_datasets(&self, delivery: &Delivery) -> DataPlaneResult<Vec<DiscoveredDataset>>;

    fn prepare(
        &mut self,
        dataset: DiscoveredDataset,
        stage: DatasetStage,
    ) -> BoxFuture<'_, DataPlaneResult<Arc<DeliveryDiscovery>>>;
}

pub enum SinkInput {
    Data(Delivery),
    Dataset {
        id: DeliveryId,
        dataset: DiscoveredDataset,
    },
}

pub enum DeliveryOutput {
    Fixed(mpsc::Sender<Delivery>),
    Evolving(mpsc::Sender<SinkInput>),
}

impl DeliveryOutput {
    pub async fn send(&self, delivery: Delivery) -> Result<(), ()> {
        match self {
            Self::Fixed(sender) => sender.send(delivery).await.map_err(|_| ()),
            Self::Evolving(sender) => sender.send(SinkInput::Data(delivery)).await.map_err(|_| ()),
        }
    }

    pub async fn admit(&self, id: DeliveryId, dataset: DiscoveredDataset) -> anyhow::Result<()> {
        match self {
            Self::Fixed(_) => {
                anyhow::bail!("source emitted a dataset without a configured admission coordinator")
            }
            Self::Evolving(sender) => sender
                .send(SinkInput::Dataset { id, dataset })
                .await
                .map_err(|_| anyhow::anyhow!("dataset admission channel closed")),
        }
    }
}

enum Barrier {
    Rejected(DataPlaneFailure),
    Source { id: DeliveryId, dataset: DiscoveredDataset },
    Data { delivery: Delivery, datasets: Vec<DiscoveredDataset> },
}

pub async fn run(
    mut sink: Box<dyn Sink>,
    mut admission: Box<dyn DatasetAdmission>,
    mut input: mpsc::Receiver<SinkInput>,
    events: mpsc::Sender<SinkEvent>,
    memory: PipelineMemory,
    cancellation: CancellationToken,
) -> DataPlaneResult<()> {
    let mut pending = None;
    loop {
        let barrier = {
            let (sender, receiver) = mpsc::channel(super::CHANNEL_CAPACITY);
            let mut session = sink.run_session(SinkIo {
                deliveries: receiver,
                events: events.clone(),
                memory: memory.clone(),
                cancellation: cancellation.clone(),
            });
            if let Some(delivery) = pending.take() {
                sender.send(delivery).await.map_err(|_| DataPlaneFailure::retryable(anyhow::anyhow!("sink session input closed")))?;
            }
            let barrier = loop {
                let next = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => break None,
                    result = &mut session => {
                        return result.and_then(|()| Err(DataPlaneFailure::retryable(anyhow::anyhow!("sink stopped before input closed"))));
                    }
                    next = input.recv() => next,
                };
                match next {
                    Some(SinkInput::Data(delivery)) => {
                        let datasets = match admission.pending_datasets(&delivery) {
                            Ok(datasets) => datasets,
                            Err(error) => break Some(Barrier::Rejected(error)),
                        };
                        if !datasets.is_empty() {
                            break Some(Barrier::Data { delivery, datasets });
                        }
                        let sent = tokio::select! {
                            () = cancellation.cancelled() => false,
                            result = &mut session => {
                                return result.and_then(|()| Err(DataPlaneFailure::retryable(anyhow::anyhow!("sink stopped while forwarding a delivery"))));
                            }
                            result = sender.send(delivery) => result.is_ok(),
                        };
                        if !sent { break None; }
                    }
                    Some(SinkInput::Dataset { id, dataset }) => break Some(Barrier::Source { id, dataset }),
                    None => break None,
                }
            };
            drop(sender);
            // Poll to completion even on cancellation: destination writes must
            // never be detached. Only the session ends; the sink stays alive.
            session.await?;
            barrier
        };
        if cancellation.is_cancelled() { return Ok(()); }
        match barrier {
            None => return Ok(()),
            Some(Barrier::Rejected(error)) => return Err(error),
            Some(Barrier::Source { id, dataset }) => {
                let discovery = admission.prepare(dataset, DatasetStage::Source).await?;
                sink.update_discovery(discovery)?;
                if cancellation.is_cancelled() { return Ok(()); }
                events.send(SinkEvent::CommittedThrough(id)).await.map_err(|_| {
                    DataPlaneFailure::retryable(anyhow::anyhow!("source closed before dataset admission commit"))
                })?;
            }
            Some(Barrier::Data { delivery, datasets }) => {
                for dataset in datasets {
                    let discovery = admission.prepare(dataset, DatasetStage::Sink).await?;
                    if cancellation.is_cancelled() { return Ok(()); }
                    sink.update_discovery(discovery)?;
                }
                // The original delivery id is acknowledged only by the sink,
                // after its records have crossed the durability boundary.
                pending = Some(delivery);
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/admission.rs"]
mod tests;
