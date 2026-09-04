#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    reason = "test assertions intentionally fail fast"
)]

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::{
    acknowledged_heartbeat_deadline, claim_active_source, conservative_heartbeat_window,
    negotiated_session_timeout, replication_contract_violation, replication_resource_key,
    validate_consumer_availability_period,
    validate_consumer_owner_attribute_keys, validate_raw_only_codecs, validate_resource_owner,
    validate_single_partition_topology, validate_topic_codecs,
    ColumnIdentity, ConsumerIdentity, CoordinationFence, HeartbeatProbe, PersistedResourceOwner,
    ReplicationAggregateIdentity, ReplicationResourceIdentity, TopicPartitionIdentity,
    TopicPartitioningIdentity, VirtualTimestampIdentity, RESOURCE_OWNER_VERSION,
};
use tokio_util::sync::CancellationToken;
use transferia_core::failure::DataPlaneFailure;
use ydb_grpc::ydb_proto::topic::{
    describe_topic_result, AutoPartitioningSettings, AutoPartitioningStrategy, Codec,
    PartitioningSettings,
};

fn aggregate(endpoint: &str) -> ReplicationAggregateIdentity {
    ReplicationAggregateIdentity {
        endpoint: endpoint.to_owned(),
        database: "/production".to_owned(),
        coordination_node_path: "/production/transferia".to_owned(),
        resources: vec![ReplicationResourceIdentity {
            table_path: "/production/events".to_owned(),
            table_created_at: VirtualTimestampIdentity {
                plan_step: 11,
                tx_id: 12,
            },
            columns: vec![ColumnIdentity {
                name: "id".to_owned(),
                declared_type: vec![8, 1],
                nullable: false,
                primary_key_ordinal: Some(0),
            }],
            changefeed_name: "cdc".to_owned(),
            changefeed_mode: 3,
            changefeed_format: 1,
            changefeed_state: 1,
            changefeed_virtual_timestamps: true,
            changefeed_schema_changes: false,
            changefeed_resolved_timestamps_interval: None,
            changefeed_aws_region: String::new(),
            changefeed_initial_scan_progress_present: false,
            changefeed_attributes: BTreeMap::new(),
            topic_path: "/production/events/cdc".to_owned(),
            topic_created_at: VirtualTimestampIdentity {
                plan_step: 13,
                tx_id: 14,
            },
            topic_partitioning: TopicPartitioningIdentity {
                min_active_partitions: 1,
                max_active_partitions: 1,
                partition_count_limit: 1,
                auto_partitioning_strategy: AutoPartitioningStrategy::Disabled as i32,
                auto_partitioning_write_speed: None,
            },
            topic_partitions: vec![TopicPartitionIdentity {
                partition_id: 0,
                active: true,
                child_partition_ids: Vec::new(),
                parent_partition_ids: Vec::new(),
                key_range_from_bound: None,
                key_range_to_bound: None,
            }],
            topic_supported_codecs: vec![1],
            topic_attributes: BTreeMap::new(),
            consumer: ConsumerIdentity {
                name: "transferia".to_owned(),
                important: true,
                supported_codecs: vec![1],
                attributes: BTreeMap::from([
                    (
                        "transferia.coordination_node_path".to_owned(),
                        "/production/transferia".to_owned(),
                    ),
                    (
                        "transferia.delivery_id".to_owned(),
                        "delivery-a".to_owned(),
                    ),
                ]),
                availability_period: None,
            },
        }],
    }
}

fn owner(endpoint: &str) -> PersistedResourceOwner {
    PersistedResourceOwner {
        version: RESOURCE_OWNER_VERSION,
        delivery_id: "delivery-a".to_owned(),
        replay_identity: "revision-a".to_owned(),
        resource: aggregate(endpoint),
    }
}

#[test]
fn one_prepared_consumer_allows_exactly_one_live_source() {
    let active = Arc::new(AtomicBool::new(false));
    let first = claim_active_source(&active).expect("first source claim");
    assert!(claim_active_source(&active).is_err());
    drop(first);
    let restarted = claim_active_source(&active).expect("claim after source drop");
    assert!(active.load(Ordering::Acquire));
    drop(restarted);
    assert!(!active.load(Ordering::Acquire));
}

#[test]
fn persisted_owner_requires_exact_version_delivery_replay_and_source() {
    let expected = owner("https://canonical.example");
    let exact = serde_json::to_vec(&expected).expect("serialize exact owner");
    validate_resource_owner(&exact, &expected).expect("exact owner must validate");

    let mut changed = owner("https://canonical.example");
    changed.version += 1;
    assert!(validate_resource_owner(
        &serde_json::to_vec(&changed).expect("serialize version mismatch"),
        &expected,
    )
    .is_err());
    changed = owner("https://canonical.example");
    changed.delivery_id = "delivery-b".to_owned();
    assert!(validate_resource_owner(
        &serde_json::to_vec(&changed).expect("serialize delivery mismatch"),
        &expected,
    )
    .is_err());
    changed = owner("https://canonical.example");
    changed.replay_identity = "revision-b".to_owned();
    assert!(validate_resource_owner(
        &serde_json::to_vec(&changed).expect("serialize replay mismatch"),
        &expected,
    )
    .is_err());
    changed = owner("https://canonical.example");
    changed.resource.resources[0].table_created_at.tx_id += 1;
    assert!(validate_resource_owner(
        &serde_json::to_vec(&changed).expect("serialize source mismatch"),
        &expected,
    )
    .is_err());
}

#[test]
fn replication_contract_violations_carry_a_fatal_data_plane_disposition() {
    let classified = replication_contract_violation(anyhow::anyhow!("schema drift"));
    let failure = classified
        .downcast::<DataPlaneFailure>()
        .expect("contract violation must be explicitly typed");
    assert!(!failure.is_retryable());
}

#[test]
fn fence_name_ignores_endpoint_alias_but_payload_rejects_it() {
    let first = aggregate("https://first-alias.example");
    let second = aggregate("https://second-alias.example");
    assert_eq!(
        replication_resource_key(&first, "delivery-a").expect("first resource key"),
        replication_resource_key(&second, "delivery-a").expect("second resource key"),
    );

    let expected = owner("https://first-alias.example");
    let alias = serde_json::to_vec(&owner("https://second-alias.example"))
        .expect("serialize alias owner");
    assert!(validate_resource_owner(&alias, &expected).is_err());
}

#[test]
fn fence_name_covers_the_whole_delivery_not_one_aggregate_shape() {
    let full = aggregate("https://canonical.example");
    let mut subset = full.clone();
    subset.resources.clear();
    assert_eq!(
        replication_resource_key(&full, "delivery-a").expect("full resource key"),
        replication_resource_key(&subset, "delivery-a").expect("subset resource key"),
    );
    assert_ne!(
        replication_resource_key(&full, "delivery-a").expect("first delivery key"),
        replication_resource_key(&full, "delivery-b").expect("second delivery key"),
    );
}

#[allow(deprecated, reason = "the topology identity covers the legacy server field")]
fn fixed_partitioning(strategy: AutoPartitioningStrategy) -> PartitioningSettings {
    PartitioningSettings {
        min_active_partitions: 1,
        max_active_partitions: 1,
        partition_count_limit: 1,
        auto_partitioning_settings: Some(AutoPartitioningSettings {
            strategy: strategy as i32,
            partition_write_speed: None,
        }),
    }
}

fn active_partition(partition_id: i64) -> describe_topic_result::PartitionInfo {
    describe_topic_result::PartitionInfo {
        partition_id,
        active: true,
        child_partition_ids: Vec::new(),
        parent_partition_ids: Vec::new(),
        partition_stats: None,
        partition_location: None,
        key_range: None,
    }
}

#[test]
fn replication_requires_one_fixed_partition_with_auto_growth_disabled() {
    let settings = fixed_partitioning(AutoPartitioningStrategy::Disabled);
    let topology = validate_single_partition_topology(
        "/production/events/cdc",
        Some(&settings),
        &[active_partition(7)],
    )
    .expect("one fixed partition");
    assert_eq!(topology.partition_id, 7);

    assert!(validate_single_partition_topology(
        "/production/events/cdc",
        Some(&settings),
        &[active_partition(7), active_partition(8)],
    )
    .is_err());
    let growing = fixed_partitioning(AutoPartitioningStrategy::ScaleUp);
    assert!(validate_single_partition_topology(
        "/production/events/cdc",
        Some(&growing),
        &[active_partition(7)],
    )
    .is_err());
}

#[test]
fn persisted_partition_topology_mismatch_is_fatal() {
    let expected = owner("https://canonical.example");
    let mut changed = owner("https://canonical.example");
    changed.resource.resources[0].topic_partitions[0].partition_id = 1;
    let mismatch = validate_resource_owner(
        &serde_json::to_vec(&changed).expect("serialize partition mismatch"),
        &expected,
    )
    .map_err(replication_contract_violation)
    .expect_err("partition identity drift must fail");
    let failure = mismatch
        .downcast::<DataPlaneFailure>()
        .expect("partition drift must be explicitly typed");
    assert!(!failure.is_retryable());
}

#[test]
fn topic_allows_raw_among_known_unique_codecs_and_consumer_is_raw_only() {
    validate_topic_codecs("topic", &[Codec::Raw as i32, Codec::Gzip as i32])
        .expect("topic can advertise multiple codecs when RAW is included");
    validate_topic_codecs("topic", &[])
        .expect("empty topic codec list disables the compatibility check");
    assert!(validate_topic_codecs("topic", &[Codec::Gzip as i32]).is_err());
    assert!(validate_topic_codecs("topic", &[Codec::Raw as i32, Codec::Raw as i32]).is_err());
    assert!(validate_topic_codecs("topic", &[Codec::Unspecified as i32, Codec::Raw as i32]).is_err());
    assert!(validate_topic_codecs("topic", &[Codec::Raw as i32, i32::MAX]).is_err());

    validate_raw_only_codecs("consumer", &[Codec::Raw as i32]).expect("RAW-only codec");
    assert!(validate_raw_only_codecs("consumer", &[]).is_err());
    assert!(validate_raw_only_codecs(
        "consumer",
        &[Codec::Raw as i32, Codec::Gzip as i32],
    )
    .is_err());

    let exact = HashMap::from([
        (
            "transferia.coordination_node_path".to_owned(),
            "/production/transferia".to_owned(),
        ),
        (
            "transferia.delivery_id".to_owned(),
            "delivery-a".to_owned(),
        ),
    ]);
    validate_consumer_owner_attribute_keys("consumer", &exact).expect("exact owner attributes");
    let mut extra = exact;
    extra.insert("application.extra".to_owned(), "value".to_owned());
    assert!(validate_consumer_owner_attribute_keys("consumer", &extra).is_err());
}

#[test]
fn important_consumer_must_not_have_a_bounded_availability_period() {
    validate_consumer_availability_period("consumer", None)
        .expect("unbounded important consumer retention");
    let bounded = ydb_grpc::google_proto_workaround::protobuf::Duration {
        seconds: 60,
        nanos: 0,
    };
    assert!(validate_consumer_availability_period("consumer", Some(&bounded)).is_err());
}

#[test]
fn heartbeat_window_is_positive_and_strictly_before_server_expiry() {
    assert!(negotiated_session_timeout(0, 1_000).is_err());
    assert!(negotiated_session_timeout(1, 0).is_err());
    let timeout = negotiated_session_timeout(1, 30_000).expect("valid session timeout");
    let window = conservative_heartbeat_window(timeout).expect("conservative window");
    assert!(window > Duration::ZERO);
    assert!(window < timeout);
    assert_eq!(window, Duration::from_secs(20));
}

#[test]
fn heartbeat_acknowledgement_must_match_and_arrive_before_the_send_anchored_deadline() {
    let sent_at = tokio::time::Instant::now();
    let window = Duration::from_secs(20);
    let probe = HeartbeatProbe {
        opaque: 41,
        sent_at,
    };
    assert!(acknowledged_heartbeat_deadline(&probe, 42, window, sent_at).is_err());
    assert!(acknowledged_heartbeat_deadline(
        &probe,
        41,
        window,
        sent_at + window,
    )
    .is_err());
    assert_eq!(
        acknowledged_heartbeat_deadline(
            &probe,
            41,
            window,
            sent_at + Duration::from_secs(1),
        )
        .expect("matching timely acknowledgement"),
        sent_at + window
    );
}

#[tokio::test]
async fn dropping_fence_cancels_health_and_actor_shutdown() {
    let lost = CancellationToken::new();
    let shutdown = CancellationToken::new();
    let actor_shutdown = shutdown.clone();
    let actor_stopped = Arc::new(AtomicBool::new(false));
    let actor_stopped_signal = Arc::clone(&actor_stopped);
    let fence = CoordinationFence {
        lost: lost.clone(),
        shutdown: shutdown.clone(),
        _actor: tokio::spawn(async move {
            actor_shutdown.cancelled().await;
            actor_stopped_signal.store(true, Ordering::Release);
        }),
    };
    drop(fence);
    assert!(lost.is_cancelled());
    assert!(shutdown.is_cancelled());
    tokio::task::yield_now().await;
    assert!(actor_stopped.load(Ordering::Acquire));
}
