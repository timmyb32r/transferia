use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::codec::Streaming;
use ydb_grpc::ydb_proto::coordination::session_request;
use ydb_grpc::ydb_proto::coordination::session_request::Request as CoordinationRequest;
use ydb_grpc::ydb_proto::coordination::session_response::Response as CoordinationResponse;
use ydb_grpc::ydb_proto::coordination::{SessionRequest, SessionResponse};
use ydb_grpc::ydb_proto::scheme::entry;
use ydb_grpc::ydb_proto::status_ids::StatusCode;
use ydb_grpc::ydb_proto::table::{
    changefeed_description, changefeed_format, changefeed_mode, DescribeTableResult,
};
use ydb_grpc::ydb_proto::topic::{
    describe_topic_result, AutoPartitioningStrategy, Codec, Consumer, DescribeTopicRequest,
    DescribeTopicResult, PartitioningSettings,
};

use super::super::config::YdbSourceConfig;
use super::super::source::DiscoveredTable;
use super::super::transport::YdbClient;
use super::super::types::{column_plans, dataset_schema, ColumnPlan};
use super::decoder::{validate_cdc_column_plans, YdbCdcDecoder};
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::failure::DataPlaneFailure;
use transferia_registry::durable::{CompareExchangeResult, DurableContext, DurableLease};

const RESOURCE_OWNER_VERSION: u8 = 1;
#[derive(Clone)]
pub(in crate::ydb) struct ReplicationResources {
    pub(super) tables: Arc<Vec<DiscoveredTable>>,
    pub(super) topics: Arc<[String]>,
    pub(super) topic_partition_ids: Arc<[i64]>,
    identities: Arc<[ReplicationResourceIdentity]>,
}

pub(in crate::ydb) struct PreparedReplication {
    pub(in crate::ydb) resources: ReplicationResources,
    pub(in crate::ydb) replay_identity: Arc<str>,
    pub(in crate::ydb) delivery_id: Arc<str>,
    pub(in crate::ydb) fence_lost: CancellationToken,
    active_source: Arc<AtomicBool>,
    _local_lease: DurableLease,
    _coordination_fence: CoordinationFence,
}

pub(in crate::ydb) struct ActiveReplicationSource {
    active: Arc<AtomicBool>,
}

impl PreparedReplication {
    pub(in crate::ydb) fn claim_source(&self) -> anyhow::Result<ActiveReplicationSource> {
        claim_active_source(&self.active_source).map_err(replication_contract_violation)
    }
}

impl Drop for ActiveReplicationSource {
    fn drop(&mut self) {
        self.active.store(false, Ordering::Release);
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplicationAggregateIdentity {
    endpoint: String,
    database: String,
    coordination_node_path: String,
    resources: Vec<ReplicationResourceIdentity>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ReplicationResourceIdentity {
    table_path: String,
    table_created_at: VirtualTimestampIdentity,
    columns: Vec<ColumnIdentity>,
    changefeed_name: String,
    changefeed_mode: i32,
    changefeed_format: i32,
    changefeed_state: i32,
    changefeed_virtual_timestamps: bool,
    changefeed_schema_changes: bool,
    changefeed_resolved_timestamps_interval: Option<DurationIdentity>,
    changefeed_aws_region: String,
    changefeed_initial_scan_progress_present: bool,
    changefeed_attributes: BTreeMap<String, String>,
    topic_path: String,
    topic_created_at: VirtualTimestampIdentity,
    topic_partitioning: TopicPartitioningIdentity,
    topic_partitions: Vec<TopicPartitionIdentity>,
    topic_supported_codecs: Vec<i32>,
    topic_attributes: BTreeMap<String, String>,
    consumer: ConsumerIdentity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TopicPartitioningIdentity {
    min_active_partitions: i64,
    max_active_partitions: i64,
    partition_count_limit: i64,
    auto_partitioning_strategy: i32,
    auto_partitioning_write_speed: Option<AutoPartitioningWriteSpeedIdentity>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AutoPartitioningWriteSpeedIdentity {
    stabilization_window: Option<DurationIdentity>,
    up_utilization_percent: i32,
    down_utilization_percent: i32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TopicPartitionIdentity {
    partition_id: i64,
    active: bool,
    child_partition_ids: Vec<i64>,
    parent_partition_ids: Vec<i64>,
    key_range_from_bound: Option<Vec<u8>>,
    key_range_to_bound: Option<Vec<u8>>,
}

struct ValidatedTopicTopology {
    partition_id: i64,
    partitioning: TopicPartitioningIdentity,
    partitions: Vec<TopicPartitionIdentity>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct VirtualTimestampIdentity {
    plan_step: u64,
    tx_id: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ColumnIdentity {
    name: String,
    declared_type: Vec<u8>,
    nullable: bool,
    primary_key_ordinal: Option<usize>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ConsumerIdentity {
    name: String,
    important: bool,
    supported_codecs: Vec<i32>,
    attributes: BTreeMap<String, String>,
    availability_period: Option<DurationIdentity>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DurationIdentity {
    seconds: i64,
    nanos: i32,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PersistedResourceOwner {
    version: u8,
    delivery_id: String,
    replay_identity: String,
    resource: ReplicationAggregateIdentity,
}

struct CoordinationFence {
    lost: CancellationToken,
    shutdown: CancellationToken,
    _actor: tokio::task::JoinHandle<()>,
}

#[derive(Serialize)]
struct ReplicationConflictDomain<'a> {
    database: &'a str,
    coordination_node_path: &'a str,
    consumer_name: &'a str,
}

struct CoordinationHeartbeat {
    window: std::time::Duration,
    period: std::time::Duration,
    proof_deadline: tokio::time::Instant,
    next_send: tokio::time::Instant,
    next_opaque: u64,
    outstanding: Option<HeartbeatProbe>,
}

struct HeartbeatProbe {
    opaque: u64,
    sent_at: tokio::time::Instant,
}

pub(in crate::ydb) fn replication_contract_violation(error: anyhow::Error) -> anyhow::Error {
    DataPlaneFailure::fatal(error).into()
}

pub(in crate::ydb) async fn discover_replication_resources(
    config: &YdbSourceConfig,
    cancellation: &CancellationToken,
) -> anyhow::Result<ReplicationResources> {
    discover_replication_resources_inner(config, cancellation).await
}

async fn discover_replication_resources_inner(
    config: &YdbSourceConfig,
    cancellation: &CancellationToken,
) -> anyhow::Result<ReplicationResources> {
    let replication = config.replication.as_ref().ok_or_else(|| {
        replication_contract_violation(anyhow::anyhow!("YDB replication configuration is missing"))
    })?;
    replication
        .validate()
        .map_err(replication_contract_violation)?;
    let mut client = tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("YDB replication discovery cancelled"),
        client = observe_external_request(
            "ydb",
            "replication_setup_connect",
            YdbClient::connect(&config.connection),
        ) => client?,
    };
    let mut tables = Vec::with_capacity(config.tables.len());
    let mut topics = Vec::with_capacity(config.tables.len());
    let mut topic_partition_ids = Vec::with_capacity(config.tables.len());
    let mut identities = Vec::with_capacity(config.tables.len());

    for table in &config.tables {
        let description = tokio::select! {
            biased;
            () = cancellation.cancelled() => anyhow::bail!("YDB replication discovery cancelled"),
            description = client.describe_table(table.path.clone()) => description?,
        };
        let table_identity = validate_table_and_changefeed(
            table.path.as_str(),
            &replication.changefeed_name,
            description,
        )
        .map_err(replication_contract_violation)?;
        let topic_path = table_identity.topic_path.clone();
        let topic = describe_topic(&client, &topic_path, cancellation).await?;
        let (topic_created_at, topic_topology, topic_supported_codecs, topic_attributes, consumer) =
            validate_topic_and_consumer(&topic_path, &replication.consumer_name, topic)
                .map_err(replication_contract_violation)?;

        let columns = table_identity.columns;
        let decoder = YdbCdcDecoder::new(Arc::from(columns.clone()), replication.max_message_bytes)
            .map_err(replication_contract_violation)?;
        decoder
            .decode_admission_bytes(replication.max_message_bytes)
        .map_err(|error| {
            replication_contract_violation(anyhow::anyhow!(
                    "ydb.replication.max_message_bytes={} cannot be admitted by the YDB CDC decoder: {error}",
                    replication.max_message_bytes
                ))
            })?;
        let resource_identity = ReplicationResourceIdentity {
            table_path: table_identity.table_path,
            table_created_at: table_identity.table_created_at,
            columns: column_identity(&columns),
            changefeed_name: replication.changefeed_name.clone(),
            changefeed_mode: table_identity.changefeed_mode,
            changefeed_format: table_identity.changefeed_format,
            changefeed_state: table_identity.changefeed_state,
            changefeed_virtual_timestamps: table_identity.changefeed_virtual_timestamps,
            changefeed_schema_changes: table_identity.changefeed_schema_changes,
            changefeed_resolved_timestamps_interval: table_identity
                .changefeed_resolved_timestamps_interval,
            changefeed_aws_region: table_identity.changefeed_aws_region,
            changefeed_initial_scan_progress_present: table_identity
                .changefeed_initial_scan_progress_present,
            changefeed_attributes: table_identity.changefeed_attributes,
            topic_path: topic_path.clone(),
            topic_created_at,
            topic_partitioning: topic_topology.partitioning,
            topic_partitions: topic_topology.partitions,
            topic_supported_codecs,
            topic_attributes,
            consumer,
        };
        tables.push(DiscoveredTable {
            config: table.clone(),
            schema: dataset_schema(&columns),
            columns,
        });
        topics.push(topic_path);
        topic_partition_ids.push(topic_topology.partition_id);
        identities.push(resource_identity);
    }

    Ok(ReplicationResources {
        tables: Arc::new(tables),
        topics: topics.into(),
        topic_partition_ids: topic_partition_ids.into(),
        identities: identities.into(),
    })
}

pub(in crate::ydb) async fn prepare_replication(
    config: &YdbSourceConfig,
    durable: &DurableContext,
    cancellation: &CancellationToken,
    replay_identity: Arc<str>,
) -> anyhow::Result<PreparedReplication> {
    if replay_identity.is_empty() {
        return Err(replication_contract_violation(anyhow::anyhow!(
            "YDB replication requires a non-empty replay identity bound to the complete replay-affecting delivery configuration"
        )));
    }
    let resources = discover_replication_resources_inner(config, cancellation).await?;
    let replication = config.replication.as_ref().ok_or_else(|| {
        replication_contract_violation(anyhow::anyhow!("YDB replication configuration is missing"))
    })?;
    let aggregate = aggregate_identity(
        config,
        replication.coordination_node_path.clone(),
        &resources,
    )
    .map_err(replication_contract_violation)?;
    let resource_key = replication_resource_key(&aggregate, &replication.consumer_name)
        .map_err(replication_contract_violation)?;
    let local_lease = durable
        .resource_storage
        .acquire_execution_lease(&resource_key)
        .await?;
    let owner = PersistedResourceOwner {
        version: RESOURCE_OWNER_VERSION,
        delivery_id: durable.delivery_id.to_string(),
        replay_identity: replay_identity.to_string(),
        resource: aggregate,
    };
    persist_resource_owner(durable, &resource_key, &owner).await?;

    let client = tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("YDB replication preparation cancelled"),
        client = observe_external_request(
            "ydb",
            "replication_fence_connect",
            YdbClient::connect(&config.connection),
        ) => client?,
    };
    let coordination_fence = CoordinationFence::acquire(
        &client,
        &replication.coordination_node_path,
        &resource_key,
        serde_json::to_vec(&owner)
            .map_err(|error| replication_contract_violation(anyhow::Error::new(error)))?,
        durable.delivery_id.as_ref(),
        cancellation,
    )
    .await?;
    let fence_lost = coordination_fence.lost.clone();

    Ok(PreparedReplication {
        resources,
        replay_identity,
        delivery_id: Arc::clone(&durable.delivery_id),
        fence_lost,
        active_source: Arc::new(AtomicBool::new(false)),
        _local_lease: local_lease,
        _coordination_fence: coordination_fence,
    })
}

fn claim_active_source(active: &Arc<AtomicBool>) -> anyhow::Result<ActiveReplicationSource> {
    active
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| {
            anyhow::anyhow!(
                "YDB replication already has an active source for this prepared consumer"
            )
        })?;
    Ok(ActiveReplicationSource {
        active: Arc::clone(active),
    })
}

fn replication_resource_key(
    identity: &ReplicationAggregateIdentity,
    consumer_name: &str,
) -> anyhow::Result<String> {
    let conflict_domain = ReplicationConflictDomain {
        database: &identity.database,
        coordination_node_path: &identity.coordination_node_path,
        consumer_name,
    };
    let digest =
        murmur3::murmur3_x64_128(&mut Cursor::new(serde_json::to_vec(&conflict_domain)?), 0)?;
    Ok(format!("ydb-replication-{digest:032x}"))
}

fn aggregate_identity(
    config: &YdbSourceConfig,
    coordination_node_path: String,
    resources: &ReplicationResources,
) -> anyhow::Result<ReplicationAggregateIdentity> {
    let mut exact_resources = resources.identities.iter().cloned().collect::<Vec<_>>();
    exact_resources.sort_unstable_by(|left, right| {
        left.table_path
            .cmp(&right.table_path)
            .then_with(|| left.topic_path.cmp(&right.topic_path))
            .then_with(|| left.consumer.name.cmp(&right.consumer.name))
    });
    Ok(ReplicationAggregateIdentity {
        endpoint: config.connection.tonic_endpoint()?,
        database: config.connection.database.clone(),
        coordination_node_path,
        resources: exact_resources,
    })
}

impl CoordinationFence {
    async fn acquire(
        client: &YdbClient,
        node_path: &str,
        semaphore_name: &str,
        identity: Vec<u8>,
        delivery_id: &str,
        cancellation: &CancellationToken,
    ) -> anyhow::Result<Self> {
        let timeout = client.timeout();
        let timeout_millis = u64::try_from(timeout.as_millis())?;
        let (sender, receiver) = mpsc::channel(8);
        sender
            .send(coordination_request(CoordinationRequest::SessionStart(
                session_request::SessionStart {
                    path: node_path.to_owned(),
                    session_id: 0,
                    timeout_millis,
                    description: format!("transferia-{delivery_id}"),
                    seq_no: 1,
                    protection_key: Vec::new(),
                },
            )))
            .await
            .map_err(|_| anyhow::anyhow!("YDB Coordination request stream closed before start"))?;
        let mut service = client.coordination_service();
        let response = tokio::select! {
            biased;
            () = cancellation.cancelled() => anyhow::bail!("YDB replication preparation cancelled"),
            response = observe_external_request(
                "ydb",
                "coordination_session_open",
                tokio::time::timeout(
                    timeout,
                    service.session(client.request(ReceiverStream::new(receiver))),
                ),
            ) => response
                .map_err(|_| anyhow::anyhow!("YDB Coordination Session timed out while opening"))??,
        };
        let mut responses = response.into_inner();
        let negotiated_timeout = tokio::select! {
            biased;
            () = cancellation.cancelled() => anyhow::bail!("YDB replication preparation cancelled"),
            result = observe_external_request(
                "ydb",
                "coordination_session_start",
                wait_for_session_started(&mut responses, &sender, timeout),
            ) => result?,
        };
        let operation_timeout = timeout.min(negotiated_timeout);
        let operation_timeout_millis = u64::try_from(operation_timeout.as_millis())?;

        let created = tokio::select! {
            biased;
            () = cancellation.cancelled() => anyhow::bail!("YDB replication preparation cancelled"),
            result = observe_external_request(
                "ydb",
                "coordination_semaphore_create",
                create_semaphore(
                    &mut responses,
                    &sender,
                    semaphore_name,
                    identity.clone(),
                    operation_timeout,
                ),
            ) => result?,
        };
        anyhow::ensure!(
            matches!(created, StatusCode::Success | StatusCode::AlreadyExists),
            "YDB Coordination CreateSemaphore failed with {}",
            created.as_str_name()
        );
        let description = tokio::select! {
            biased;
            () = cancellation.cancelled() => anyhow::bail!("YDB replication preparation cancelled"),
            result = observe_external_request(
                "ydb",
                "coordination_semaphore_describe",
                describe_semaphore(
                    &mut responses,
                    &sender,
                    semaphore_name,
                    operation_timeout,
                ),
            ) => result?,
        };
        if description.name != semaphore_name
            || description.limit != 1
            || description.ephemeral
            || description.data != identity
        {
            return Err(replication_contract_violation(anyhow::anyhow!(
                "YDB replication Coordination semaphore exists with a different exact resource, delivery, replay identity, limit, or persistence contract"
            )));
        }
        tokio::select! {
            biased;
            () = cancellation.cancelled() => anyhow::bail!("YDB replication preparation cancelled"),
            result = observe_external_request(
                "ydb",
                "coordination_semaphore_acquire",
                acquire_semaphore(
                    &mut responses,
                    &sender,
                    semaphore_name,
                    identity,
                    operation_timeout,
                    operation_timeout_millis,
                ),
            ) => result?,
        }

        let heartbeat = tokio::select! {
            biased;
            () = cancellation.cancelled() => anyhow::bail!("YDB replication preparation cancelled"),
            result = observe_external_request(
                "ydb",
                "coordination_fence_heartbeat",
                establish_coordination_heartbeat(
                    &mut responses,
                    &sender,
                    negotiated_timeout,
                ),
            ) => result?,
        };

        let lost = CancellationToken::new();
        let actor_lost = lost.clone();
        let shutdown = CancellationToken::new();
        let actor_shutdown = shutdown.clone();
        let actor_cancellation = cancellation.clone();
        let actor = tokio::spawn(async move {
            let result = maintain_coordination_session(
                responses,
                sender,
                heartbeat,
                actor_cancellation,
                actor_shutdown,
            )
            .await;
            actor_lost.cancel();
            if let Err(error) = result {
                tracing::error!(
                    error = %error,
                    "YDB replication lost its global Coordination fence"
                );
            }
        });
        Ok(Self {
            lost,
            shutdown,
            _actor: actor,
        })
    }
}

impl Drop for CoordinationFence {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.lost.cancel();
    }
}

const fn coordination_request(request: CoordinationRequest) -> SessionRequest {
    SessionRequest {
        request: Some(request),
    }
}

async fn send_coordination_request(
    sender: &mpsc::Sender<SessionRequest>,
    request: CoordinationRequest,
) -> anyhow::Result<()> {
    sender
        .send(coordination_request(request))
        .await
        .map_err(|_| anyhow::anyhow!("YDB Coordination request stream closed"))
}

async fn next_coordination_response(
    responses: &mut Streaming<SessionResponse>,
    sender: &mpsc::Sender<SessionRequest>,
    timeout: std::time::Duration,
) -> anyhow::Result<CoordinationResponse> {
    tokio::time::timeout(timeout, async {
        loop {
            let response = responses
                .message()
                .await?
                .ok_or_else(|| anyhow::anyhow!("YDB Coordination session ended"))?
                .response
                .ok_or_else(|| anyhow::anyhow!("YDB Coordination returned an empty response"))?;
            match response {
                CoordinationResponse::Ping(ping) => {
                    send_coordination_request(
                        sender,
                        CoordinationRequest::Pong(session_request::PingPong {
                            opaque: ping.opaque,
                        }),
                    )
                    .await?;
                }
                CoordinationResponse::Failure(failure) => {
                    let status =
                        StatusCode::try_from(failure.status).unwrap_or(StatusCode::Unspecified);
                    anyhow::bail!(
                        "YDB Coordination session failed with {}: {}",
                        status.as_str_name(),
                        serde_json::to_string(&failure.issues)?
                    );
                }
                response => return Ok(response),
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("YDB Coordination operation timed out"))?
}

async fn wait_for_session_started(
    responses: &mut Streaming<SessionResponse>,
    sender: &mpsc::Sender<SessionRequest>,
    timeout: std::time::Duration,
) -> anyhow::Result<std::time::Duration> {
    match next_coordination_response(responses, sender, timeout).await? {
        CoordinationResponse::SessionStarted(started) => {
            negotiated_session_timeout(started.session_id, started.timeout_millis, timeout)
        }
        response => anyhow::bail!(
            "YDB Coordination returned {:?} while starting a session",
            coordination_response_name(&response)
        ),
    }
}

fn negotiated_session_timeout(
    session_id: u64,
    timeout_millis: u64,
    requested_timeout: std::time::Duration,
) -> anyhow::Result<std::time::Duration> {
    anyhow::ensure!(session_id != 0, "YDB Coordination returned session id zero");
    anyhow::ensure!(
        !requested_timeout.is_zero(),
        "YDB Coordination session timeout must be positive"
    );
    Ok(if timeout_millis == 0 {
        requested_timeout
    } else {
        std::time::Duration::from_millis(timeout_millis)
    })
}

async fn create_semaphore(
    responses: &mut Streaming<SessionResponse>,
    sender: &mpsc::Sender<SessionRequest>,
    semaphore_name: &str,
    identity: Vec<u8>,
    timeout: std::time::Duration,
) -> anyhow::Result<StatusCode> {
    send_coordination_request(
        sender,
        CoordinationRequest::CreateSemaphore(session_request::CreateSemaphore {
            req_id: 1,
            name: semaphore_name.to_owned(),
            limit: 1,
            data: identity,
        }),
    )
    .await?;
    match next_coordination_response(responses, sender, timeout).await? {
        CoordinationResponse::CreateSemaphoreResult(result) if result.req_id == 1 => {
            Ok(StatusCode::try_from(result.status).unwrap_or(StatusCode::Unspecified))
        }
        response => anyhow::bail!(
            "YDB Coordination returned {:?} while creating the replication semaphore",
            coordination_response_name(&response)
        ),
    }
}

async fn describe_semaphore(
    responses: &mut Streaming<SessionResponse>,
    sender: &mpsc::Sender<SessionRequest>,
    semaphore_name: &str,
    timeout: std::time::Duration,
) -> anyhow::Result<ydb_grpc::ydb_proto::coordination::SemaphoreDescription> {
    send_coordination_request(
        sender,
        CoordinationRequest::DescribeSemaphore(session_request::DescribeSemaphore {
            req_id: 2,
            name: semaphore_name.to_owned(),
            include_owners: true,
            include_waiters: true,
            watch_data: false,
            watch_owners: false,
        }),
    )
    .await?;
    match next_coordination_response(responses, sender, timeout).await? {
        CoordinationResponse::DescribeSemaphoreResult(result) if result.req_id == 2 => {
            let status = StatusCode::try_from(result.status).unwrap_or(StatusCode::Unspecified);
            anyhow::ensure!(
                status == StatusCode::Success,
                "YDB Coordination DescribeSemaphore failed with {}: {}",
                status.as_str_name(),
                serde_json::to_string(&result.issues)?
            );
            result.semaphore_description.ok_or_else(|| {
                anyhow::anyhow!("YDB Coordination DescribeSemaphore returned no description")
            })
        }
        response => anyhow::bail!(
            "YDB Coordination returned {:?} while describing the replication semaphore",
            coordination_response_name(&response)
        ),
    }
}

async fn acquire_semaphore(
    responses: &mut Streaming<SessionResponse>,
    sender: &mpsc::Sender<SessionRequest>,
    semaphore_name: &str,
    identity: Vec<u8>,
    timeout: std::time::Duration,
    timeout_millis: u64,
) -> anyhow::Result<()> {
    send_coordination_request(
        sender,
        CoordinationRequest::AcquireSemaphore(session_request::AcquireSemaphore {
            req_id: 3,
            name: semaphore_name.to_owned(),
            timeout_millis,
            count: 1,
            data: identity,
            ephemeral: false,
        }),
    )
    .await?;
    loop {
        match next_coordination_response(responses, sender, timeout)
            .await
            .map_err(|error| {
                error.context(
                    "YDB replication could not acquire its Coordination semaphore; another execution may own it",
                )
            })?
        {
            CoordinationResponse::AcquireSemaphorePending(pending) if pending.req_id == 3 => {}
            CoordinationResponse::AcquireSemaphoreResult(result) if result.req_id == 3 => {
                let status = StatusCode::try_from(result.status).unwrap_or(StatusCode::Unspecified);
                anyhow::ensure!(
                    status == StatusCode::Success && result.acquired,
                    "YDB replication Coordination semaphore is owned by another execution or acquisition failed with {}: {}",
                    status.as_str_name(),
                    serde_json::to_string(&result.issues)?
                );
                return Ok(());
            }
            response => anyhow::bail!(
                "YDB Coordination returned {:?} while acquiring the replication semaphore",
                coordination_response_name(&response)
            ),
        }
    }
}

fn conservative_heartbeat_window(
    session_timeout: std::time::Duration,
) -> anyhow::Result<std::time::Duration> {
    anyhow::ensure!(
        !session_timeout.is_zero(),
        "YDB Coordination session timeout must be positive"
    );
    let safety_margin = session_timeout
        .checked_div(3)
        .ok_or_else(|| anyhow::anyhow!("YDB Coordination session timeout cannot be divided"))?;
    anyhow::ensure!(
        !safety_margin.is_zero(),
        "YDB Coordination session timeout is too short for a conservative heartbeat margin"
    );
    let window = session_timeout
        .checked_sub(safety_margin)
        .ok_or_else(|| anyhow::anyhow!("YDB Coordination heartbeat window underflow"))?;
    anyhow::ensure!(
        !window.is_zero() && window < session_timeout,
        "YDB Coordination heartbeat window must be positive and shorter than the server timeout"
    );
    Ok(window)
}

fn acknowledged_heartbeat_deadline(
    probe: &HeartbeatProbe,
    acknowledged_opaque: u64,
    window: std::time::Duration,
    observed_at: tokio::time::Instant,
) -> anyhow::Result<tokio::time::Instant> {
    anyhow::ensure!(
        acknowledged_opaque == probe.opaque,
        "YDB Coordination acknowledged an unexpected heartbeat"
    );
    let deadline = probe.sent_at.checked_add(window).ok_or_else(|| {
        anyhow::anyhow!("YDB Coordination heartbeat deadline exceeds clock range")
    })?;
    anyhow::ensure!(
        observed_at < deadline,
        "YDB Coordination acknowledged a heartbeat after its conservative deadline"
    );
    Ok(deadline)
}

async fn establish_coordination_heartbeat(
    responses: &mut Streaming<SessionResponse>,
    sender: &mpsc::Sender<SessionRequest>,
    session_timeout: std::time::Duration,
) -> anyhow::Result<CoordinationHeartbeat> {
    let window = conservative_heartbeat_window(session_timeout)?;
    let period = window
        .checked_div(2)
        .filter(|period| !period.is_zero())
        .ok_or_else(|| anyhow::anyhow!("YDB Coordination heartbeat period is too short"))?;
    let sent_at = tokio::time::Instant::now();
    let proof_deadline = sent_at.checked_add(window).ok_or_else(|| {
        anyhow::anyhow!("YDB Coordination heartbeat deadline exceeds clock range")
    })?;
    send_heartbeat(sender, 1, proof_deadline).await?;
    let remaining = proof_deadline.saturating_duration_since(tokio::time::Instant::now());
    anyhow::ensure!(
        !remaining.is_zero(),
        "YDB Coordination did not acknowledge the initial fence heartbeat before its conservative deadline"
    );
    match next_coordination_response(responses, sender, remaining).await? {
        CoordinationResponse::Pong(pong) => {
            let probe = HeartbeatProbe { opaque: 1, sent_at };
            let acknowledged_deadline = acknowledged_heartbeat_deadline(
                &probe,
                pong.opaque,
                window,
                tokio::time::Instant::now(),
            )?;
            anyhow::ensure!(
                acknowledged_deadline == proof_deadline,
                "YDB Coordination initial heartbeat deadline changed unexpectedly"
            );
            Ok(CoordinationHeartbeat {
                window,
                period,
                proof_deadline,
                next_send: sent_at.checked_add(period).ok_or_else(|| {
                    anyhow::anyhow!("YDB Coordination heartbeat schedule exceeds clock range")
                })?,
                next_opaque: 2,
                outstanding: None,
            })
        }
        response => anyhow::bail!(
            "YDB Coordination returned {:?} while proving the acquired replication fence",
            coordination_response_name(&response)
        ),
    }
}

async fn send_heartbeat(
    sender: &mpsc::Sender<SessionRequest>,
    opaque: u64,
    deadline: tokio::time::Instant,
) -> anyhow::Result<()> {
    tokio::time::timeout_at(
        deadline,
        send_coordination_request(
            sender,
            CoordinationRequest::Ping(session_request::PingPong { opaque }),
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("YDB Coordination heartbeat send timed out"))?
}

async fn maintain_coordination_session(
    mut responses: Streaming<SessionResponse>,
    sender: mpsc::Sender<SessionRequest>,
    mut heartbeat: CoordinationHeartbeat,
    cancellation: CancellationToken,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    loop {
        let wake_at = if heartbeat.outstanding.is_some() {
            heartbeat.proof_deadline
        } else {
            heartbeat.next_send.min(heartbeat.proof_deadline)
        };
        tokio::select! {
            biased;
            () = async {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {}
                    () = shutdown.cancelled() => {}
                }
            } => {
                let remaining = heartbeat
                    .proof_deadline
                    .saturating_duration_since(tokio::time::Instant::now());
                if !remaining.is_zero() {
                    observe_external_request(
                        "ydb",
                        "coordination_session_stop",
                        stop_coordination_session(&mut responses, &sender, remaining),
                    )
                    .await?;
                }
                return Ok(());
            }
            () = tokio::time::sleep_until(wake_at) => {
                let now = tokio::time::Instant::now();
                anyhow::ensure!(
                    now < heartbeat.proof_deadline,
                    "YDB Coordination fence heartbeat exceeded its conservative deadline"
                );
                anyhow::ensure!(
                    heartbeat.outstanding.is_none(),
                    "YDB Coordination did not acknowledge the outstanding fence heartbeat"
                );
                let opaque = heartbeat.next_opaque;
                heartbeat.next_opaque = heartbeat.next_opaque.checked_add(1).ok_or_else(|| {
                    anyhow::anyhow!("YDB Coordination heartbeat sequence overflow")
                })?;
                let sent_at = now;
                send_heartbeat(&sender, opaque, heartbeat.proof_deadline).await?;
                heartbeat.outstanding = Some(HeartbeatProbe { opaque, sent_at });
            }
            response = responses.message() => {
                let response = response?
                    .ok_or_else(|| anyhow::anyhow!("YDB Coordination session ended while fencing replication"))?
                    .response
                    .ok_or_else(|| anyhow::anyhow!("YDB Coordination returned an empty response while fencing replication"))?;
                match response {
                    CoordinationResponse::Ping(ping) => {
                        tokio::time::timeout_at(
                            heartbeat.proof_deadline,
                            send_coordination_request(
                                &sender,
                                CoordinationRequest::Pong(session_request::PingPong {
                                    opaque: ping.opaque,
                                }),
                            ),
                        )
                        .await
                        .map_err(|_| anyhow::anyhow!("YDB Coordination heartbeat response timed out"))??;
                    }
                    CoordinationResponse::Pong(pong) => {
                        let probe = heartbeat.outstanding.take().ok_or_else(|| {
                            anyhow::anyhow!("YDB Coordination returned a heartbeat acknowledgement with no outstanding heartbeat")
                        })?;
                        let renewed_deadline = acknowledged_heartbeat_deadline(
                            &probe,
                            pong.opaque,
                            heartbeat.window,
                            tokio::time::Instant::now(),
                        )?;
                        heartbeat.proof_deadline = renewed_deadline;
                        heartbeat.next_send = probe.sent_at.checked_add(heartbeat.period)
                            .ok_or_else(|| anyhow::anyhow!("YDB Coordination heartbeat schedule exceeds clock range"))?;
                    }
                    CoordinationResponse::Failure(failure) => {
                        let status = StatusCode::try_from(failure.status)
                            .unwrap_or(StatusCode::Unspecified);
                        anyhow::bail!(
                            "YDB Coordination session failed with {}",
                            status.as_str_name()
                        );
                    }
                    CoordinationResponse::SessionStopped(_) => return Ok(()),
                    response => anyhow::bail!(
                        "YDB Coordination returned unexpected {:?} after acquiring the replication fence",
                        coordination_response_name(&response)
                    ),
                }
            }
        }
    }
}

async fn stop_coordination_session(
    responses: &mut Streaming<SessionResponse>,
    sender: &mpsc::Sender<SessionRequest>,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| anyhow::anyhow!("YDB Coordination session-stop deadline overflow"))?;
    tokio::time::timeout_at(
        deadline,
        send_coordination_request(
            sender,
            CoordinationRequest::SessionStop(session_request::SessionStop {}),
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("YDB Coordination SessionStop send timed out"))??;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        anyhow::ensure!(
            !remaining.is_zero(),
            "YDB Coordination did not acknowledge SessionStop before the fence deadline"
        );
        match next_coordination_response(responses, sender, remaining).await? {
            CoordinationResponse::SessionStopped(_) => return Ok(()),
            // A heartbeat already enqueued before shutdown can be acknowledged first. It does
            // not extend the local ownership proof; only SessionStopped completes shutdown.
            CoordinationResponse::Pong(_) => {}
            response => anyhow::bail!(
                "YDB Coordination returned unexpected {:?} while stopping the fenced session",
                coordination_response_name(&response)
            ),
        }
    }
}

const fn coordination_response_name(response: &CoordinationResponse) -> &'static str {
    match response {
        CoordinationResponse::Ping(_) => "Ping",
        CoordinationResponse::Pong(_) => "Pong",
        CoordinationResponse::Failure(_) => "Failure",
        CoordinationResponse::SessionStarted(_) => "SessionStarted",
        CoordinationResponse::SessionStopped(_) => "SessionStopped",
        CoordinationResponse::Unsupported6(_) => "Unsupported6",
        CoordinationResponse::Unsupported7(_) => "Unsupported7",
        CoordinationResponse::AcquireSemaphorePending(_) => "AcquireSemaphorePending",
        CoordinationResponse::AcquireSemaphoreResult(_) => "AcquireSemaphoreResult",
        CoordinationResponse::ReleaseSemaphoreResult(_) => "ReleaseSemaphoreResult",
        CoordinationResponse::DescribeSemaphoreResult(_) => "DescribeSemaphoreResult",
        CoordinationResponse::DescribeSemaphoreChanged(_) => "DescribeSemaphoreChanged",
        CoordinationResponse::CreateSemaphoreResult(_) => "CreateSemaphoreResult",
        CoordinationResponse::UpdateSemaphoreResult(_) => "UpdateSemaphoreResult",
        CoordinationResponse::DeleteSemaphoreResult(_) => "DeleteSemaphoreResult",
        CoordinationResponse::Unsupported16(_) => "Unsupported16",
        CoordinationResponse::Unsupported17(_) => "Unsupported17",
        CoordinationResponse::Unsupported18(_) => "Unsupported18",
    }
}

struct ValidatedTable {
    table_path: String,
    table_created_at: VirtualTimestampIdentity,
    columns: Vec<ColumnPlan>,
    changefeed_mode: i32,
    changefeed_format: i32,
    changefeed_state: i32,
    changefeed_virtual_timestamps: bool,
    changefeed_schema_changes: bool,
    changefeed_resolved_timestamps_interval: Option<DurationIdentity>,
    changefeed_aws_region: String,
    changefeed_initial_scan_progress_present: bool,
    changefeed_attributes: BTreeMap<String, String>,
    topic_path: String,
}

fn validate_table_and_changefeed(
    table_path: &str,
    changefeed_name: &str,
    description: DescribeTableResult,
) -> anyhow::Result<ValidatedTable> {
    let entry = description
        .self_
        .ok_or_else(|| anyhow::anyhow!("YDB table '{table_path}' has no scheme identity"))?;
    let entry_type = entry::Type::try_from(entry.r#type).unwrap_or(entry::Type::Unspecified);
    anyhow::ensure!(
        entry_type == entry::Type::Table,
        "YDB replication path '{table_path}' is {entry_type:?}, not a row table"
    );
    let expected_name = table_path.rsplit('/').next().unwrap_or_default();
    anyhow::ensure!(
        !expected_name.is_empty() && entry.name == expected_name,
        "YDB DescribeTable returned scheme entry '{}' for requested path '{table_path}'",
        entry.name
    );
    let created_at = entry
        .created_at
        .ok_or_else(|| anyhow::anyhow!("YDB table '{table_path}' has no creation timestamp"))?;
    let matching = description
        .changefeeds
        .into_iter()
        .filter(|changefeed| changefeed.name == changefeed_name)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        matching.len() == 1,
        "YDB table '{table_path}' must have exactly one changefeed named '{changefeed_name}', found {}",
        matching.len()
    );
    let changefeed = matching.into_iter().next().ok_or_else(|| {
        anyhow::anyhow!(
            "YDB table '{table_path}' lost its validated changefeed '{changefeed_name}'"
        )
    })?;
    let mode = changefeed_mode::Mode::try_from(changefeed.mode)
        .unwrap_or(changefeed_mode::Mode::Unspecified);
    let format = changefeed_format::Format::try_from(changefeed.format)
        .unwrap_or(changefeed_format::Format::Unspecified);
    let state = changefeed_description::State::try_from(changefeed.state)
        .unwrap_or(changefeed_description::State::Unspecified);
    anyhow::ensure!(
        mode == changefeed_mode::Mode::NewAndOldImages,
        "YDB changefeed '{table_path}/{changefeed_name}' must use NEW_AND_OLD_IMAGES, got {}",
        mode.as_str_name()
    );
    anyhow::ensure!(
        format == changefeed_format::Format::Json,
        "YDB changefeed '{table_path}/{changefeed_name}' must use FORMAT JSON, got {}",
        format.as_str_name()
    );
    anyhow::ensure!(
        changefeed.virtual_timestamps,
        "YDB changefeed '{table_path}/{changefeed_name}' must enable virtual timestamps"
    );
    anyhow::ensure!(
        !changefeed.schema_changes,
        "YDB changefeed '{table_path}/{changefeed_name}' must disable schema change events"
    );
    anyhow::ensure!(
        changefeed.resolved_timestamps_interval.is_none(),
        "YDB changefeed '{table_path}/{changefeed_name}' must disable resolved timestamp events"
    );
    anyhow::ensure!(
        changefeed.aws_region.is_empty(),
        "YDB FORMAT JSON changefeed '{table_path}/{changefeed_name}' must not set aws_region"
    );
    anyhow::ensure!(
        changefeed.attributes.is_empty(),
        "YDB FORMAT JSON changefeed '{table_path}/{changefeed_name}' must not set attributes"
    );
    anyhow::ensure!(
        state == changefeed_description::State::Enabled,
        "YDB changefeed '{table_path}/{changefeed_name}' must be enabled without an initial scan, got {}",
        state.as_str_name()
    );
    anyhow::ensure!(
        changefeed.initial_scan_progress.is_none(),
        "YDB changefeed '{table_path}/{changefeed_name}' was created with an initial scan; ordinary stream replication requires no initial scan"
    );

    let columns = column_plans(description.columns, &description.primary_key)?;
    anyhow::ensure!(
        !columns.is_empty(),
        "YDB table '{table_path}' has no columns"
    );
    validate_cdc_column_plans(&columns)?;
    Ok(ValidatedTable {
        table_path: table_path.to_owned(),
        table_created_at: VirtualTimestampIdentity {
            plan_step: created_at.plan_step,
            tx_id: created_at.tx_id,
        },
        columns,
        changefeed_mode: changefeed.mode,
        changefeed_format: changefeed.format,
        changefeed_state: changefeed.state,
        changefeed_virtual_timestamps: changefeed.virtual_timestamps,
        changefeed_schema_changes: changefeed.schema_changes,
        changefeed_resolved_timestamps_interval: changefeed.resolved_timestamps_interval.map(
            |period| DurationIdentity {
                seconds: period.seconds,
                nanos: period.nanos,
            },
        ),
        changefeed_aws_region: changefeed.aws_region,
        changefeed_initial_scan_progress_present: changefeed.initial_scan_progress.is_some(),
        changefeed_attributes: changefeed.attributes.into_iter().collect(),
        topic_path: format!("{table_path}/{changefeed_name}"),
    })
}

async fn describe_topic(
    client: &YdbClient,
    topic_path: &str,
    cancellation: &CancellationToken,
) -> anyhow::Result<DescribeTopicResult> {
    let request = client.request(DescribeTopicRequest {
        operation_params: None,
        path: topic_path.to_owned(),
        include_stats: false,
        include_location: false,
    });
    let mut service = client.topic_service();
    let response = tokio::select! {
        biased;
        () = cancellation.cancelled() => anyhow::bail!("YDB replication discovery cancelled"),
        response = observe_external_request(
            "ydb",
            "describe_changefeed_topic",
            tokio::time::timeout(client.timeout(), service.describe_topic(request)),
        ) => response
            .map_err(|_| anyhow::anyhow!("YDB DescribeTopic timed out after {} ms", client.timeout().as_millis()))??
            .into_inner(),
    };
    decode_topic_operation(response.operation, "DescribeTopic")
}

fn decode_topic_operation<T: Message + Default>(
    operation: Option<ydb_grpc::ydb_proto::operations::Operation>,
    name: &str,
) -> anyhow::Result<T> {
    let operation = operation.ok_or_else(|| anyhow::anyhow!("YDB {name} returned no operation"))?;
    anyhow::ensure!(
        operation.ready,
        "YDB {name} returned an asynchronous operation"
    );
    let status = StatusCode::try_from(operation.status).unwrap_or(StatusCode::Unspecified);
    anyhow::ensure!(
        status == StatusCode::Success,
        "YDB {name} failed with {status:?}: {}",
        serde_json::to_string(&operation.issues)?
    );
    let result = operation
        .result
        .ok_or_else(|| anyhow::anyhow!("YDB {name} returned no result"))?;
    Ok(T::decode(result.value.as_slice())?)
}

type ValidatedTopicAndConsumer = (
    VirtualTimestampIdentity,
    ValidatedTopicTopology,
    Vec<i32>,
    BTreeMap<String, String>,
    ConsumerIdentity,
);

fn validate_topic_and_consumer(
    topic_path: &str,
    consumer_name: &str,
    topic: DescribeTopicResult,
) -> anyhow::Result<ValidatedTopicAndConsumer> {
    let topology = validate_single_partition_topology(
        topic_path,
        topic.partitioning_settings.as_ref(),
        &topic.partitions,
    )?;
    let entry = topic.self_.ok_or_else(|| {
        anyhow::anyhow!("YDB changefeed topic '{topic_path}' has no scheme identity")
    })?;
    let entry_type = entry::Type::try_from(entry.r#type).unwrap_or(entry::Type::Unspecified);
    anyhow::ensure!(
        entry_type == entry::Type::Topic,
        "YDB changefeed path '{topic_path}' is {entry_type:?}, not a topic"
    );
    let expected_name = topic_path.rsplit('/').next().unwrap_or_default();
    anyhow::ensure!(
        !expected_name.is_empty() && entry.name == expected_name,
        "YDB DescribeTopic returned scheme entry '{}' for requested path '{topic_path}'",
        entry.name
    );
    let created_at = entry.created_at.ok_or_else(|| {
        anyhow::anyhow!("YDB changefeed topic '{topic_path}' has no creation timestamp")
    })?;
    let mut topic_supported_codecs = topic
        .supported_codecs
        .as_ref()
        .map_or_else(Vec::new, |codecs| codecs.codecs.clone());
    topic_supported_codecs.sort_unstable();
    validate_topic_codecs(
        &format!("YDB changefeed topic '{topic_path}'"),
        &topic_supported_codecs,
    )?;
    let consumers = topic
        .consumers
        .into_iter()
        .filter(|consumer| consumer.name == consumer_name)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        consumers.len() == 1,
        "YDB changefeed topic '{topic_path}' must have exactly one consumer named '{consumer_name}', found {}",
        consumers.len()
    );
    let consumer = consumers.into_iter().next().ok_or_else(|| {
        anyhow::anyhow!(
            "YDB changefeed topic '{topic_path}' lost its validated consumer '{consumer_name}'"
        )
    })?;
    anyhow::ensure!(
        consumer.important,
        "YDB changefeed consumer '{topic_path}/{consumer_name}' must be important so unread records cannot expire"
    );
    anyhow::ensure!(
        consumer_read_from_is_beginning(
            consumer
                .read_from
                .as_ref()
                .map(|timestamp| (timestamp.seconds, timestamp.nanos)),
        ),
        "YDB changefeed consumer '{topic_path}/{consumer_name}' must not set read_from because it can skip source records"
    );
    let consumer_context = format!("YDB changefeed consumer '{topic_path}/{consumer_name}'");
    validate_consumer_availability_period(
        &consumer_context,
        consumer.availability_period.as_ref(),
    )?;
    let mut consumer_supported_codecs = consumer
        .supported_codecs
        .as_ref()
        .map_or_else(Vec::new, |codecs| codecs.codecs.clone());
    consumer_supported_codecs.sort_unstable();
    validate_raw_only_codecs(&consumer_context, &consumer_supported_codecs)?;
    Ok((
        VirtualTimestampIdentity {
            plan_step: created_at.plan_step,
            tx_id: created_at.tx_id,
        },
        topology,
        topic_supported_codecs,
        topic.attributes.into_iter().collect(),
        consumer_identity(consumer),
    ))
}

const fn consumer_read_from_is_beginning(read_from: Option<(i64, i32)>) -> bool {
    match read_from {
        None => true,
        Some((seconds, nanos)) => seconds == 0 && nanos == 0,
    }
}

#[allow(
    deprecated,
    reason = "the exact server topology identity includes the legacy field"
)]
fn validate_single_partition_topology(
    topic_path: &str,
    partitioning: Option<&PartitioningSettings>,
    partitions: &[describe_topic_result::PartitionInfo],
) -> anyhow::Result<ValidatedTopicTopology> {
    let partitioning = partitioning.ok_or_else(|| {
        anyhow::anyhow!(
            "YDB changefeed topic '{topic_path}' has no partitioning settings to prove fixed topology"
        )
    })?;
    let auto_partitioning = partitioning
        .auto_partitioning_settings
        .as_ref()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "YDB changefeed topic '{topic_path}' has no auto-partitioning settings to prove topology growth is disabled"
            )
        })?;
    let strategy =
        AutoPartitioningStrategy::try_from(auto_partitioning.strategy).map_err(|_| {
            anyhow::anyhow!(
                "YDB changefeed topic '{topic_path}' has unknown auto-partitioning strategy {}",
                auto_partitioning.strategy
            )
        })?;
    anyhow::ensure!(
        strategy == AutoPartitioningStrategy::Disabled,
        "YDB changefeed topic '{topic_path}' must disable auto-partitioning, got {}",
        strategy.as_str_name()
    );
    anyhow::ensure!(
        partitions.len() == 1,
        "YDB changefeed topic '{topic_path}' must have exactly one partition so Topic offset is a global row version, found {}",
        partitions.len()
    );
    let partition = partitions.first().ok_or_else(|| {
        anyhow::anyhow!("YDB changefeed topic '{topic_path}' lost its validated partition")
    })?;
    anyhow::ensure!(
        partition.partition_id >= 0,
        "YDB changefeed topic '{topic_path}' has negative partition id {}",
        partition.partition_id
    );
    anyhow::ensure!(
        partition.active,
        "YDB changefeed topic '{topic_path}' sole partition {} is not active",
        partition.partition_id
    );
    anyhow::ensure!(
        partition.parent_partition_ids.is_empty() && partition.child_partition_ids.is_empty(),
        "YDB changefeed topic '{topic_path}' partition {} has split/merge ancestry and does not prove a stable single-partition topology",
        partition.partition_id
    );

    let write_speed = auto_partitioning.partition_write_speed.map(|settings| {
        AutoPartitioningWriteSpeedIdentity {
            stabilization_window: settings
                .stabilization_window
                .map(|duration| DurationIdentity {
                    seconds: duration.seconds,
                    nanos: duration.nanos,
                }),
            up_utilization_percent: settings.up_utilization_percent,
            down_utilization_percent: settings.down_utilization_percent,
        }
    });
    let key_range = partition.key_range.as_ref();
    Ok(ValidatedTopicTopology {
        partition_id: partition.partition_id,
        partitioning: TopicPartitioningIdentity {
            min_active_partitions: partitioning.min_active_partitions,
            max_active_partitions: partitioning.max_active_partitions,
            partition_count_limit: partitioning.partition_count_limit,
            auto_partitioning_strategy: auto_partitioning.strategy,
            auto_partitioning_write_speed: write_speed,
        },
        partitions: vec![TopicPartitionIdentity {
            partition_id: partition.partition_id,
            active: partition.active,
            child_partition_ids: partition.child_partition_ids.clone(),
            parent_partition_ids: partition.parent_partition_ids.clone(),
            key_range_from_bound: key_range.and_then(|range| range.from_bound.clone()),
            key_range_to_bound: key_range.and_then(|range| range.to_bound.clone()),
        }],
    })
}

fn validate_raw_only_codecs(context: &str, codecs: &[i32]) -> anyhow::Result<()> {
    anyhow::ensure!(
        codecs == [Codec::Raw as i32],
        "{context} must allow exactly the RAW codec"
    );
    Ok(())
}

fn validate_topic_codecs(context: &str, codecs: &[i32]) -> anyhow::Result<()> {
    anyhow::ensure!(
        codecs.windows(2).all(|pair| pair[0] != pair[1]),
        "{context} repeats a supported codec"
    );
    for codec in codecs {
        let codec = Codec::try_from(*codec)
            .map_err(|_| anyhow::anyhow!("{context} advertises unknown codec id {codec}"))?;
        anyhow::ensure!(
            codec != Codec::Unspecified,
            "{context} advertises the unspecified codec"
        );
    }
    anyhow::ensure!(
        codecs.is_empty() || codecs.contains(&(Codec::Raw as i32)),
        "{context} must either disable codec compatibility checks or allow RAW records"
    );
    Ok(())
}

fn validate_consumer_availability_period(
    context: &str,
    availability_period: Option<&ydb_grpc::google_proto_workaround::protobuf::Duration>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        availability_period.is_none(),
        "{context} must not set availability_period; important-consumer retention must remain unbounded"
    );
    Ok(())
}

fn consumer_identity(consumer: Consumer) -> ConsumerIdentity {
    let mut supported_codecs = consumer
        .supported_codecs
        .map_or_else(Vec::new, |codecs| codecs.codecs);
    supported_codecs.sort_unstable();
    ConsumerIdentity {
        name: consumer.name,
        important: consumer.important,
        supported_codecs,
        attributes: consumer.attributes.into_iter().collect(),
        availability_period: consumer.availability_period.map(|period| DurationIdentity {
            seconds: period.seconds,
            nanos: period.nanos,
        }),
    }
}

fn column_identity(columns: &[ColumnPlan]) -> Vec<ColumnIdentity> {
    columns
        .iter()
        .map(|column| ColumnIdentity {
            name: column.name.clone(),
            declared_type: column.declared_type.encode_to_vec(),
            nullable: column.nullable,
            primary_key_ordinal: column.primary_key_ordinal,
        })
        .collect()
}

async fn persist_resource_owner(
    durable: &DurableContext,
    resource_key: &str,
    expected: &PersistedResourceOwner,
) -> anyhow::Result<()> {
    if let Some(current) = durable.resource_storage.read(resource_key).await? {
        return validate_resource_owner(&current.payload, expected)
            .map_err(replication_contract_violation);
    }
    let payload = serde_json::to_vec(expected)
        .map_err(|error| replication_contract_violation(anyhow::Error::new(error)))?;
    match durable
        .resource_storage
        .compare_exchange(resource_key, None, &payload)
        .await?
    {
        CompareExchangeResult::Applied(_) => Ok(()),
        CompareExchangeResult::Conflict(Some(current)) => {
            validate_resource_owner(&current.payload, expected)
                .map_err(replication_contract_violation)
        }
        CompareExchangeResult::Conflict(None) => {
            Err(replication_contract_violation(anyhow::anyhow!(
                "YDB replication resource ownership changed while it was being claimed"
            )))
        }
    }
}

fn validate_resource_owner(
    payload: &[u8],
    expected: &PersistedResourceOwner,
) -> anyhow::Result<()> {
    let actual: PersistedResourceOwner = serde_json::from_slice(payload)?;
    anyhow::ensure!(
        actual == *expected,
        "YDB changefeed consumer belongs to a different delivery, replay identity, or exact source resource"
    );
    Ok(())
}

#[cfg(test)]
#[path = "tests/setup.rs"]
mod tests;
