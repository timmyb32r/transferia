use futures_util::future::BoxFuture;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use transferia_core::failure::{DataPlaneFailure, DataPlaneResult};
use transferia_core::sink::{Delivery, DeliveryId, Sink, SinkEvent, SinkIo};
use transferia_core::{DiscoveredDataset, PipelineMemory};

/// Orchestration-owned schema validation and preparation. Called only after
/// the previous sink actor has drained all writes and shut down successfully.
pub trait DatasetAdmission: Send {
    fn prepare(
        &mut self,
        dataset: DiscoveredDataset,
    ) -> BoxFuture<'_, DataPlaneResult<Box<dyn Sink>>>;
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

pub async fn run(
    mut sink: Box<dyn Sink>,
    mut admission: Box<dyn DatasetAdmission>,
    mut input: mpsc::Receiver<SinkInput>,
    events: mpsc::Sender<SinkEvent>,
    memory: PipelineMemory,
    cancellation: CancellationToken,
) -> DataPlaneResult<()> {
    loop {
        let (sender, receiver) = mpsc::channel(super::CHANNEL_CAPACITY);
        let mut actor = tokio::spawn(sink.run(SinkIo {
            deliveries: receiver,
            events: events.clone(),
            memory: memory.clone(),
            cancellation: cancellation.clone(),
        }));
        let barrier = loop {
            let next = tokio::select! {
                biased;
                () = cancellation.cancelled() => break None,
                result = &mut actor => {
                    return super::data_plane_component_outcome("evolving sink", result).result
                        .and_then(|()| Err(DataPlaneFailure::retryable(anyhow::anyhow!("sink stopped before input closed"))));
                }
                next = input.recv() => next,
            };
            match next {
                Some(SinkInput::Data(delivery)) => {
                    let sent = tokio::select! {
                        () = cancellation.cancelled() => false,
                        result = sender.send(delivery) => result.is_ok(),
                    };
                    if !sent {
                        break None;
                    }
                }
                Some(SinkInput::Dataset { id, dataset }) => break Some((id, dataset)),
                None => break None,
            }
        };
        drop(sender);
        // Never detach in-flight destination writes, including on cancellation.
        super::data_plane_component_outcome("evolving sink drain", actor.await).result?;
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let Some((id, dataset)) = barrier else {
            return Ok(());
        };
        sink = admission.prepare(dataset).await?;
        if cancellation.is_cancelled() {
            return Ok(());
        }
        events
            .send(SinkEvent::CommittedThrough(id))
            .await
            .map_err(|_| {
                DataPlaneFailure::retryable(anyhow::anyhow!(
                    "source closed before dataset admission commit"
                ))
            })?;
    }
}

#[cfg(test)]
#[path = "tests/admission.rs"]
mod tests;
