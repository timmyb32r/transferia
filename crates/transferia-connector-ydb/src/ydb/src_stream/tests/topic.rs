#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

use super::*;

#[test]
fn commit_ranges_coalesce_only_exactly_adjacent_offsets() -> anyhow::Result<()> {
    assert_eq!(
        coalesce_ranges(vec![
            OffsetsRange { start: 4, end: 5 },
            OffsetsRange { start: 2, end: 3 },
            OffsetsRange { start: 3, end: 4 },
            OffsetsRange { start: 7, end: 8 },
        ])?,
        vec![
            OffsetsRange { start: 2, end: 5 },
            OffsetsRange { start: 7, end: 8 },
        ]
    );
    assert!(coalesce_ranges(vec![
        OffsetsRange { start: 2, end: 5 },
        OffsetsRange { start: 4, end: 6 },
    ])
    .is_err());
    assert!(coalesce_ranges(vec![OffsetsRange { start: 2, end: 2 }]).is_err());
    Ok(())
}

#[test]
fn commit_request_preserves_exact_topic_partition_session_and_target() -> anyhow::Result<()> {
    let sessions = HashMap::from([(
        17,
        PartitionSessionState {
            topic_path: Arc::from("local/events/feed"),
            partition_id: 3,
            committed_offset: 10,
            commit_response_offset: 10,
            read_through: 13,
            pending_graceful_stop: None,
            invalidated: false,
        },
    )]);
    let markers = vec![
        CommitMarker::new(TopicCommitMarker {
            partitions: vec![PartitionCommitMarker {
                topic_path: Arc::from("local/events/feed"),
                partition_id: 3,
                partition_session_id: 17,
                range: OffsetsRange { start: 10, end: 11 },
            }],
            retained_bytes: 19,
        }),
        CommitMarker::new(TopicCommitMarker {
            partitions: vec![PartitionCommitMarker {
                topic_path: Arc::from("local/events/feed"),
                partition_id: 3,
                partition_session_id: 17,
                range: OffsetsRange { start: 11, end: 13 },
            }],
            retained_bytes: 22,
        }),
    ];

    let (request, targets, committed_bytes) = build_commit_request(&markers, &sessions)?;
    assert_eq!(targets, vec![(17, 13)]);
    assert_eq!(committed_bytes, 41);
    assert_eq!(request.len(), 1);
    assert_eq!(request[0].partition_session_id, 17);
    assert_eq!(
        request[0].offsets,
        vec![OffsetsRange { start: 10, end: 13 }]
    );
    Ok(())
}

#[tokio::test]
async fn one_credit_window_reservation_grows_while_an_earlier_batch_clone_is_live(
) -> anyhow::Result<()> {
    let memory = PipelineMemory::new(1);
    let credit = memory.reserve_progress_source(1).await;
    let earlier_batch = credit.clone();

    credit.grow_progress_source_to(17)?;
    assert_eq!(credit.bytes(), 17);
    assert_eq!(earlier_batch.bytes(), 17);
    assert_eq!(memory.source_used(), 17);

    let _ = credit.shrink_to(9);
    assert_eq!(earlier_batch.bytes(), 9);
    drop(earlier_batch);
    drop(credit);
    assert_eq!(memory.source_used(), 0);
    Ok(())
}

#[test]
fn stale_or_revoked_partition_cannot_be_reported_committed() {
    let targets = vec![(17, 13)];
    let mut sessions = HashMap::new();
    assert!(ensure_targets_live(&targets, &sessions).is_err());
    sessions.insert(
        17,
        PartitionSessionState {
            topic_path: Arc::from("local/events/feed"),
            partition_id: 3,
            committed_offset: 10,
            commit_response_offset: 10,
            read_through: 13,
            pending_graceful_stop: None,
            invalidated: true,
        },
    );
    assert!(ensure_targets_live(&targets, &sessions).is_err());
}

#[test]
fn server_cannot_ack_past_the_exact_requested_target() {
    let targets = vec![(17, 13)];
    let mut sessions = HashMap::from([(
        17,
        PartitionSessionState {
            topic_path: Arc::from("local/events/feed"),
            partition_id: 3,
            committed_offset: 13,
            commit_response_offset: 13,
            read_through: 15,
            pending_graceful_stop: None,
            invalidated: false,
        },
    )]);
    assert!(ensure_ack_does_not_exceed_targets(&targets, &sessions).is_ok());
    sessions.get_mut(&17).unwrap().commit_response_offset = 14;
    assert!(ensure_ack_does_not_exceed_targets(&targets, &sessions).is_err());
}

#[test]
fn commit_ack_is_scoped_to_the_exact_in_flight_request() -> anyhow::Result<()> {
    let targets = vec![(17, 13)];
    let mut sessions = HashMap::from([
        (
            17,
            PartitionSessionState {
                topic_path: Arc::from("local/events/feed"),
                partition_id: 3,
                committed_offset: 10,
                commit_response_offset: 10,
                read_through: 13,
                pending_graceful_stop: None,
                invalidated: false,
            },
        ),
        (
            18,
            PartitionSessionState {
                topic_path: Arc::from("local/events/feed"),
                partition_id: 4,
                committed_offset: 20,
                commit_response_offset: 20,
                read_through: 21,
                pending_graceful_stop: None,
                invalidated: false,
            },
        ),
    ]);

    assert!(apply_commit_ack(
        &targets,
        &mut sessions,
        vec![PartitionCommittedOffset {
            partition_session_id: 18,
            committed_offset: 21,
        }],
    )
    .is_err());
    assert_eq!(sessions[&18].committed_offset, 20);

    assert!(apply_commit_ack(
        &targets,
        &mut sessions,
        vec![
            PartitionCommittedOffset {
                partition_session_id: 17,
                committed_offset: 12,
            },
            PartitionCommittedOffset {
                partition_session_id: 17,
                committed_offset: 13,
            },
        ],
    )
    .is_err());
    assert_eq!(sessions[&17].committed_offset, 10);

    apply_commit_ack(
        &targets,
        &mut sessions,
        vec![PartitionCommittedOffset {
            partition_session_id: 17,
            committed_offset: 13,
        }],
    )?;
    assert_eq!(sessions[&17].commit_response_offset, 13);
    Ok(())
}

#[test]
fn committed_offset_must_be_inside_the_complete_retained_range() {
    let retained = OffsetsRange { start: 10, end: 20 };
    assert!(validate_retained_offset(10, &retained, "local/feed", 0).is_ok());
    assert!(validate_retained_offset(20, &retained, "local/feed", 0).is_ok());
    assert!(validate_retained_offset(9, &retained, "local/feed", 0).is_err());
    assert!(validate_retained_offset(21, &retained, "local/feed", 0).is_err());
    assert!(validate_retained_offset(
        10,
        &OffsetsRange { start: 20, end: 10 },
        "local/feed",
        0,
    )
    .is_err());
}

#[test]
fn invalidated_partition_state_is_removed_only_after_every_read_offset_is_acknowledged() {
    let mut state = PartitionSessionState {
        topic_path: Arc::from("local/events/feed"),
        partition_id: 3,
        committed_offset: 10,
        commit_response_offset: 10,
        read_through: 13,
        pending_graceful_stop: None,
        invalidated: true,
    };
    assert!(!settled_invalidated(&state));
    state.commit_response_offset = 13;
    assert!(settled_invalidated(&state));
}

#[test]
fn graceful_stop_waits_for_the_scoped_commit_ack() {
    let mut state = PartitionSessionState {
        topic_path: Arc::from("local/events/feed"),
        partition_id: 3,
        committed_offset: 10,
        commit_response_offset: 10,
        read_through: 13,
        pending_graceful_stop: Some(10),
        invalidated: false,
    };
    assert!(!graceful_stop_ready(&state));

    state.commit_response_offset = 13;
    assert!(graceful_stop_ready(&state));
}

#[test]
fn active_partition_session_id_cannot_be_reused() {
    let sessions = HashMap::from([(
        17,
        PartitionSessionState {
            topic_path: Arc::from("local/events/feed"),
            partition_id: 3,
            committed_offset: 10,
            commit_response_offset: 10,
            read_through: 10,
            pending_graceful_stop: None,
            invalidated: false,
        },
    )]);

    assert!(ensure_partition_session_id_is_new(&sessions, 18).is_ok());
    assert!(ensure_partition_session_id_is_new(&sessions, 17).is_err());
}

#[test]
fn ending_the_fixed_partition_is_fatal_topology_drift() {
    let sessions = HashMap::from([(
        17,
        PartitionSessionState {
            topic_path: Arc::from("local/events/feed"),
            partition_id: 3,
            committed_offset: 10,
            commit_response_offset: 10,
            read_through: 13,
            pending_graceful_stop: None,
            invalidated: false,
        },
    )]);

    let drift = reject_fixed_partition_end(&sessions, 17)
        .err()
        .expect("partition end must fail");
    let failure = drift
        .downcast::<DataPlaneFailure>()
        .expect("partition end must be explicitly fatal");
    assert!(!failure.is_retryable());
    assert!(reject_fixed_partition_end(&sessions, 18).is_err());
}

#[test]
fn topic_assignment_must_match_the_single_discovered_partition() {
    let configured = HashMap::from([(Arc::<str>::from("local/events/feed"), 3)]);
    let mut sessions = HashMap::new();

    assert!(validate_assigned_partition(&configured, &sessions, "local/events/feed", 3).is_ok());
    assert!(validate_assigned_partition(&configured, &sessions, "local/events/feed", 4).is_err());
    assert!(validate_assigned_partition(&configured, &sessions, "local/other/feed", 3).is_err());

    sessions.insert(
        17,
        PartitionSessionState {
            topic_path: Arc::from("local/events/feed"),
            partition_id: 3,
            committed_offset: 10,
            commit_response_offset: 10,
            read_through: 10,
            pending_graceful_stop: None,
            invalidated: false,
        },
    );
    assert!(validate_assigned_partition(&configured, &sessions, "local/events/feed", 3).is_err());
}

#[test]
fn topic_path_comparison_removes_at_most_the_wire_root_slash() {
    assert_eq!(canonical_topic_path("/local/events/feed"), "local/events/feed");
    assert_eq!(canonical_topic_path("local/events/feed"), "local/events/feed");
    assert_eq!(canonical_topic_path("//local/events/feed"), "/local/events/feed");
}

#[test]
fn timestamp_conversion_is_checked_and_exact_to_milliseconds() -> anyhow::Result<()> {
    assert_eq!(
        timestamp_millis(Some(&ydb_grpc::google_proto_workaround::protobuf::Timestamp {
            seconds: 1_700_000_000,
            nanos: 123_999_999,
        }))?,
        1_700_000_000_123
    );
    assert!(timestamp_millis(None).is_err());
    assert!(timestamp_millis(Some(&ydb_grpc::google_proto_workaround::protobuf::Timestamp {
        seconds: 0,
        nanos: 1_000_000_000,
    }))
    .is_err());
    assert!(timestamp_millis(Some(&ydb_grpc::google_proto_workaround::protobuf::Timestamp {
        seconds: i64::MAX,
        nanos: 0,
    }))
    .is_err());
    Ok(())
}

#[test]
fn local_grpc_response_limit_is_fatal_while_remote_pressure_is_retryable() {
    let local_limit = tonic_failure(
        "StreamRead receive",
        &tonic::Status::out_of_range(
            "Error, decoded message length too large: found 17 bytes, the limit is: 16 bytes",
        ),
    );
    let local_failure = local_limit
        .downcast_ref::<DataPlaneFailure>()
        .expect("local decoder limit must have an explicit disposition");
    assert!(!local_failure.is_retryable());

    let remote_pressure = tonic_failure(
        "StreamRead receive",
        &tonic::Status::resource_exhausted("remote quota"),
    );
    let remote_failure = remote_pressure
        .downcast_ref::<DataPlaneFailure>()
        .expect("remote pressure must have an explicit disposition");
    assert!(remote_failure.is_retryable());
}

#[test]
fn response_heap_accounting_includes_container_slack_and_message_metadata() -> anyhow::Result<()> {
    use prost::Message as _;
    use ydb_grpc::ydb_proto::topic::stream_read_message::read_response::{
        Batch, MessageData, PartitionData,
    };
    use ydb_grpc::ydb_proto::topic::stream_read_message::ReadResponse;
    use ydb_grpc::ydb_proto::topic::MetadataItem;

    let mut payload = Vec::with_capacity(64);
    payload.extend_from_slice(b"x");
    let mut messages = Vec::with_capacity(8);
    messages.push(MessageData {
        data: payload,
        message_group_id: String::from("group"),
        metadata_items: vec![MetadataItem {
            key: String::from("key"),
            value: b"value".to_vec(),
        }],
        ..MessageData::default()
    });
    let mut batches = Vec::with_capacity(4);
    batches.push(Batch {
        message_data: messages,
        producer_id: String::from("producer"),
        ..Batch::default()
    });
    let response = ReadResponse {
        partition_data: vec![PartitionData {
            partition_session_id: 7,
            batches,
        }],
        bytes_size: 1,
    };

    let retained = read_response_heap_bytes(&response)?;
    assert!(retained >= 64);
    assert!(retained > response.encoded_len());
    Ok(())
}

#[test]
fn decoded_response_multiplier_covers_generated_topic_node_layouts() {
    use std::mem::size_of;
    use ydb_grpc::ydb_proto::issue::IssueMessage;
    use ydb_grpc::ydb_proto::topic::stream_read_message::read_response::{
        Batch, MessageData, PartitionData,
    };
    use ydb_grpc::ydb_proto::topic::stream_read_message::{
        CommitOffsetResponse, EndPartitionSession, InitResponse, PartitionSession,
        PartitionSessionStatusResponse, ReadResponse, StartPartitionSessionRequest,
        StopPartitionSessionRequest, UpdatePartitionSession,
    };
    use ydb_grpc::ydb_proto::topic::MetadataItem;

    const MIN_ENCODED_REPEATED_MESSAGE_BYTES: usize = 2;
    const INITIAL_VEC_CAPACITY: usize = 4;
    let largest_inline_node = [
        size_of::<FromServer>(),
        size_of::<IssueMessage>(),
        size_of::<InitResponse>(),
        size_of::<ReadResponse>(),
        size_of::<PartitionData>(),
        size_of::<Batch>(),
        size_of::<MessageData>(),
        size_of::<MetadataItem>(),
        size_of::<CommitOffsetResponse>(),
        size_of::<PartitionSession>(),
        size_of::<PartitionSessionStatusResponse>(),
        size_of::<StartPartitionSessionRequest>(),
        size_of::<StopPartitionSessionRequest>(),
        size_of::<UpdatePartitionSession>(),
        size_of::<EndPartitionSession>(),
        size_of::<(String, String)>(),
    ]
    .into_iter()
    .max()
    .expect("generated layout list is nonempty");
    let minimum_structural_multiplier = largest_inline_node
        .checked_mul(INITIAL_VEC_CAPACITY)
        .expect("generated layout proof must fit usize")
        .div_ceil(MIN_ENCODED_REPEATED_MESSAGE_BYTES);

    assert!(
        MAX_DECODED_BYTES_PER_ENCODED_RESPONSE_BYTE >= minimum_structural_multiplier,
        "generated YDB Topic response layouts require at least {minimum_structural_multiplier} decoded bytes per encoded byte"
    );
}

#[test]
fn tonic_codec_and_hash_table_retained_bounds_cover_allocation_granularity(
) -> anyhow::Result<()> {
    assert_eq!(
        tonic_codec_buffer_bytes(1)?,
        2 * TONIC_CODEC_BUFFER_CHUNK_BYTES
    );
    assert_eq!(
        tonic_codec_buffer_bytes(TONIC_CODEC_BUFFER_CHUNK_BYTES)?,
        3 * TONIC_CODEC_BUFFER_CHUNK_BYTES
    );

    let entry_bytes = size_of::<(i64, PartitionSessionState)>();
    let bound = hash_table_allocation_bytes(14, entry_bytes)?;
    assert!(bound >= 16 * entry_bytes + 17);
    assert_eq!(hash_table_allocation_bytes(0, entry_bytes)?, 0);
    Ok(())
}

#[test]
fn read_credit_allows_one_oversize_crossing_then_requires_replenishment() -> anyhow::Result<()> {
    let (available, pending) = consume_read_response_credit(10, 0, 12)?;
    assert_eq!(available, -2);
    assert_eq!(pending, 12);
    assert!(consume_read_response_credit(available, pending, 1).is_err());

    let replenished = available.checked_add(pending).unwrap();
    let (available, pending) = consume_read_response_credit(replenished, 0, 3)?;
    assert_eq!(available, 7);
    assert_eq!(pending, 3);
    Ok(())
}

#[test]
fn read_credit_rejects_nonpositive_values_without_confusing_credit_with_frame_size() {
    assert!(consume_read_response_credit(8, 0, 0).is_err());
    assert!(consume_read_response_credit(8, 0, -1).is_err());
    assert_eq!(consume_read_response_credit(8, 0, 17).unwrap(), (-9, 17));
    assert!(consume_read_response_credit(0, 0, 1).is_err());
}

#[test]
fn only_the_exact_success_status_is_accepted() {
    let mut response = FromServer {
        status: StatusCode::Success as i32,
        issues: Vec::new(),
        server_message: None,
    };
    assert!(validate_status(&response).is_ok());

    response.status = StatusCode::Unspecified as i32;
    assert!(validate_status(&response).is_err());
    response.status = i32::MAX;
    assert!(validate_status(&response).is_err());
}
