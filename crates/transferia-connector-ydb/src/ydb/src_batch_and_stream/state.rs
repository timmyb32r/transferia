use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use transferia_connector_support::external_request::observe_external_request;
use transferia_registry::durable::{CompareExchangeResult, DurableContext, DurableValue};
use ydb_grpc::ydb_proto::topic::{DescribeTopicRequest, DescribeTopicResult};

use super::super::config::YdbSourceConfig;
use super::super::src_stream::{PreparedReplication, decode_topic_operation, replication_contract_violation};
use super::super::transport::YdbClient;

const STATE_KEY: &str = "ydb-overlap";
const RECOVERY: &str = "YDB overlapping snapshot was interrupted or its completion is uncertain; automatic restart is unsafe. Use a new delivery with a clean destination and a dedicated CDC consumer. No destination data has been automatically deleted.";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct State {
    version: u8,
    delivery_id: String,
    replay_identity: String,
    start_offsets: HashMap<String, i64>,
    streaming: bool,
}

pub(in crate::ydb) struct OverlapExecution {
    pub(in crate::ydb) prepared: Arc<PreparedReplication>,
    pub(in crate::ydb) start_offsets: HashMap<String, i64>,
    pub(super) snapshot_finished: AtomicBool,
    snapshot_claimed: AtomicBool,
    state: Mutex<(DurableValue, State)>,
    durable: DurableContext,
}

impl OverlapExecution {
    pub(in crate::ydb) async fn prepare(
        config: &YdbSourceConfig,
        prepared: Arc<PreparedReplication>,
        durable: DurableContext,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<Self> {
        let existing = durable.storage.read(STATE_KEY).await?;
        let (value, state) = if let Some(value) = existing {
            let state = decode_resume(&value.payload, &prepared)?;
            (value, state)
        } else {
            let mut offsets = HashMap::new();
            let client = observe_external_request("ydb", "overlap_connect", YdbClient::connect(&config.connection)).await?;
            for (topic, partition) in prepared.resources.topics.iter()
                .zip(prepared.resources.topic_partition_ids.iter()) {
                let request = client.request(DescribeTopicRequest {
                    path: topic.clone(), include_stats: true,
                    include_location: false, operation_params: None,
                });
                let mut service = client.topic_service();
                let response = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => anyhow::bail!("YDB overlap preparation cancelled"),
                    () = prepared.fence_lost.cancelled() => anyhow::bail!("YDB overlap execution fence lost"),
                    response = observe_external_request("ydb", "overlap_capture_offset",
                        tokio::time::timeout(client.timeout(), service.describe_topic(request))) =>
                        response.map_err(|_| anyhow::anyhow!("YDB overlap offset capture timed out"))??.into_inner(),
                };
                let result: DescribeTopicResult = decode_topic_operation(response.operation, "DescribeTopic")?;
                anyhow::ensure!(result.partitions.len() == 1, "YDB overlap topic partition count changed");
                let info = result.partitions.first().ok_or_else(|| anyhow::anyhow!("YDB omitted overlap partition"))?;
                anyhow::ensure!(info.partition_id == *partition && info.active
                    && info.parent_partition_ids.is_empty() && info.child_partition_ids.is_empty(),
                    "YDB overlap partition identity changed");
                let range = info.partition_stats.and_then(|stats| stats.partition_offsets)
                    .ok_or_else(|| anyhow::anyhow!("YDB omitted overlap offset range"))?;
                anyhow::ensure!(range.start >= 0 && range.end >= range.start, "YDB invalid overlap offset range");
                offsets.insert(topic.clone(), range.end);
            }
            let state = State {
                version: 1, delivery_id: durable.delivery_id.to_string(),
                replay_identity: prepared.replay_identity.to_string(), start_offsets: offsets,
                streaming: false,
            };
            let payload = serde_json::to_vec(&state)?;
            // Persist before destination preparation or any snapshot read. Even an
            // ambiguous write stops recovery, rather than repeating a fresh snapshot.
            let value = match durable.storage.compare_exchange(STATE_KEY, None, &payload).await? {
                CompareExchangeResult::Applied(value) => value,
                CompareExchangeResult::Conflict(_) => anyhow::bail!("{RECOVERY}"),
            };
            (value, state)
        };
        Ok(Self {
            prepared, start_offsets: state.start_offsets.clone(),
            snapshot_finished: AtomicBool::new(state.streaming),
            snapshot_claimed: AtomicBool::new(state.streaming),
            state: Mutex::new((value, state)), durable,
        })
    }

    pub(in crate::ydb) async fn streaming(&self) -> bool {
        self.state.lock().await.1.streaming
    }

    pub(super) fn claim_snapshot(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.snapshot_claimed.swap(true, Ordering::AcqRel), "{RECOVERY}");
        self.check_fence()
    }

    pub(super) fn check_fence(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.prepared.fence_lost.is_cancelled(), "YDB overlap execution fence lost");
        Ok(())
    }

    pub(in crate::ydb) async fn complete_snapshot(&self) -> anyhow::Result<()> {
        self.check_fence()?;
        let mut current = self.state.lock().await;
        if current.1.streaming { return Ok(()); }
        let next = streaming_state(&current.1, self.snapshot_finished.load(Ordering::Acquire))?;
        let payload = serde_json::to_vec(&next)?;
        let value = match self.durable.storage.compare_exchange(STATE_KEY, Some(current.0.revision), &payload).await? {
            CompareExchangeResult::Applied(value) => value,
            CompareExchangeResult::Conflict(_) => anyhow::bail!("YDB overlap phase changed unexpectedly"),
        };
        *current = (value, next);
        self.check_fence()
    }
}

fn decode_resume(payload: &[u8], prepared: &PreparedReplication) -> anyhow::Result<State> {
    let state: State = serde_json::from_slice(payload).map_err(|_| replication_contract_violation(anyhow::anyhow!("invalid YDB overlap durable state")))?;
    validate_state(&state, &prepared.delivery_id, &prepared.replay_identity, &prepared.resources.topics)?;
    require_streaming(&state)?;
    Ok(state)
}

fn require_streaming(state: &State) -> anyhow::Result<()> {
    anyhow::ensure!(state.streaming, "{RECOVERY}");
    Ok(())
}

fn streaming_state(state: &State, finished: bool) -> anyhow::Result<State> {
    anyhow::ensure!(finished, "YDB snapshot has not finished");
    let mut next = state.clone();
    next.streaming = true;
    Ok(next)
}

fn validate_state(state: &State, delivery_id: &str, replay_identity: &str, topics: &[String]) -> anyhow::Result<()> {
    anyhow::ensure!(state.version == 1 && state.delivery_id == delivery_id && state.replay_identity == replay_identity,
        "YDB overlap durable state belongs to another delivery or replay identity");
    anyhow::ensure!(state.start_offsets.len() == topics.len()
        && topics.iter().all(|topic| state.start_offsets.get(topic).is_some_and(|offset| *offset >= 0)),
        "YDB overlap durable offsets do not match the source topics");
    Ok(())
}

#[cfg(test)]
#[path = "tests/state.rs"]
mod tests;
