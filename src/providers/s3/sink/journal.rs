use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::delivery::execution::PipelineFailure;
use crate::durable::{CompareExchangeResult, DurableStorage};

use super::actor::ClosedObject;

const JOURNAL_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ManifestObject {
    key: String,
    payload_sha256: String,
    payload_bytes: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum EpochState {
    Open,
    Closed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct EpochRecord {
    version: u32,
    state: EpochState,
    objects: Vec<ManifestObject>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OpenDisposition {
    Upload,
    AlreadyClosed,
}

pub(super) struct EpochJournal {
    storage: Arc<dyn DurableStorage>,
    key: String,
    open: EpochRecord,
    open_bytes: Vec<u8>,
    closed_bytes: Vec<u8>,
}

impl EpochJournal {
    pub(super) fn new(
        storage: Arc<dyn DurableStorage>,
        partition_id: i64,
        objects: &[ClosedObject],
    ) -> Result<Self, PipelineFailure> {
        let object_records = objects
            .iter()
            .map(|object| ManifestObject {
                key: object.key.as_str().to_owned(),
                payload_sha256: hex_digest(&object.payload),
                payload_bytes: object.payload.len(),
            })
            .collect::<Vec<_>>();
        let open = EpochRecord {
            version: JOURNAL_VERSION,
            state: EpochState::Open,
            objects: object_records,
        };
        let open_bytes = serde_json::to_vec(&open)
            .map_err(anyhow::Error::from)
            .map_err(PipelineFailure::fatal)?;
        let closed_bytes = serde_json::to_vec(&EpochRecord {
            state: EpochState::Closed,
            ..open.clone()
        })
        .map_err(anyhow::Error::from)
        .map_err(PipelineFailure::fatal)?;
        let identity = open
            .objects
            .iter()
            .flat_map(|object| object.key.bytes().chain(core::iter::once(0)))
            .collect::<Vec<_>>();
        let manifest_digest = hex_digest(&identity);
        Ok(Self {
            storage,
            key: format!("s3/partition-{partition_id}/epoch-{manifest_digest}"),
            open,
            open_bytes,
            closed_bytes,
        })
    }

    pub(super) async fn ensure_open(&self) -> Result<OpenDisposition, PipelineFailure> {
        for _ in 0..3 {
            let current = self
                .storage
                .read(&self.key)
                .await
                .map_err(PipelineFailure::fatal)?;
            match current {
                None => match self
                    .storage
                    .compare_exchange(&self.key, None, &self.open_bytes)
                    .await
                    .map_err(PipelineFailure::fatal)?
                {
                    CompareExchangeResult::Applied(_) => return Ok(OpenDisposition::Upload),
                    CompareExchangeResult::Conflict(_) => {}
                },
                Some(current) => {
                    let record = self.decode_and_validate(&current.payload)?;
                    return Ok(match record.state {
                        EpochState::Open => OpenDisposition::Upload,
                        EpochState::Closed => OpenDisposition::AlreadyClosed,
                    });
                }
            }
        }
        Err(PipelineFailure::fatal(anyhow::anyhow!(
            "S3 durable epoch state changed repeatedly while opening '{}'",
            self.key
        )))
    }

    pub(super) async fn mark_closed(&self) -> Result<(), PipelineFailure> {
        for _ in 0..3 {
            let current = self
                .storage
                .read(&self.key)
                .await
                .map_err(PipelineFailure::fatal)?
                .ok_or_else(|| {
                    PipelineFailure::fatal(anyhow::anyhow!(
                        "S3 durable epoch '{}' disappeared before close",
                        self.key
                    ))
                })?;
            let record = self.decode_and_validate(&current.payload)?;
            if record.state == EpochState::Closed {
                return Ok(());
            }
            match self
                .storage
                .compare_exchange(&self.key, Some(current.revision), &self.closed_bytes)
                .await
                .map_err(PipelineFailure::fatal)?
            {
                CompareExchangeResult::Applied(_) => return Ok(()),
                CompareExchangeResult::Conflict(_) => {}
            }
        }
        Err(PipelineFailure::fatal(anyhow::anyhow!(
            "S3 durable epoch state changed repeatedly while closing '{}'",
            self.key
        )))
    }

    fn decode_and_validate(&self, payload: &[u8]) -> Result<EpochRecord, PipelineFailure> {
        let record: EpochRecord = serde_json::from_slice(payload).map_err(|error| {
            PipelineFailure::fatal(anyhow::anyhow!(
                "invalid S3 durable epoch record '{}': {error}",
                self.key
            ))
        })?;
        if record.version != JOURNAL_VERSION || record.objects != self.open.objects {
            return Err(PipelineFailure::fatal(anyhow::anyhow!(
                "S3 durable epoch record '{}' does not match replayed object keys and payloads",
                self.key
            )));
        }
        Ok(record)
    }

    #[cfg(test)]
    pub(super) fn key(&self) -> &str {
        &self.key
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
#[path = "tests/journal.rs"]
mod tests;
