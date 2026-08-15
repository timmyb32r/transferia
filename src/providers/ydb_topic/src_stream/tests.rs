use std::io::Write as _;

use ydb_grpc::ydb_proto::topic::stream_read_message::from_client::ClientMessage;
use ydb_grpc::ydb_proto::topic::{Codec, OffsetsRange};

use super::source::{
    build_commit_request, coalesce_ranges, decode_message, init_message, releasable_session_ids,
    PartitionCommitMarker, PartitionSessionState, YdbTopicCommitMarker,
};
use super::*;
use crate::pipeline::source::CommitMarker;

fn provider(extra: &str) -> anyhow::Result<YdbTopicSourceProvider> {
    provider_with_topics("  - path: topic\n    partitions: []\n", extra)
}

fn provider_with_topics(topics: &str, extra: &str) -> anyhow::Result<YdbTopicSourceProvider> {
    let value = serde_yaml::from_str(&format!(
        "host: localhost\nport: 2135\ntopics:\n{topics}consumer_name: consumer\nauth: {{ type: token, token: test }}\ndriver: ydb\ntrusted_plaintext: true\nallow_ttl_rewind: false\n{extra}parser:\n  common:\n    table_naming: {{ type: from_config, name: events }}\n  json_parser:\n    json_framing: single_document\n    columns:\n      - {{ jsonpath: $.id, column_name: id, json_data_type: number, arrow_type: Int64, nullable: false }}\n    conversion_error: dlq\n    unknown_fields: {{ action: fail }}\n"
    ))?;
    YdbTopicSourceProvider::from_config(value, Arc::new(MetricsRegistry::new()))
}

#[test]
fn accepts_dynamic_and_explicit_topic_partitions() -> anyhow::Result<()> {
    let provider = provider("")?;
    assert_eq!(provider.cfg.host, "localhost");
    assert!(provider.cfg.topics[0].partitions.is_empty());

    let explicit = provider_with_topics("  - path: selected\n    partitions: [1, 3]\n", "")?;
    assert_eq!(explicit.cfg.topics[0].path, "selected");
    assert_eq!(explicit.cfg.topics[0].partitions, [1, 3]);
    Ok(())
}

#[test]
fn rejects_old_hosts_and_database_fields() {
    let Err(error) = provider("hosts: [localhost]\n") else {
        panic!("hosts must be rejected");
    };
    assert!(
        error.to_string().contains("unknown field `hosts`"),
        "{error:#}"
    );

    let Err(error) = provider("database: /Root\n") else {
        panic!("database must be rejected");
    };
    assert!(
        error.to_string().contains("unknown field `database`"),
        "{error:#}"
    );

    let Err(error) = provider("network_timeout_ms: 1000\n") else {
        panic!("network_timeout_ms must be rejected");
    };
    assert!(
        error
            .to_string()
            .contains("unknown field `network_timeout_ms`"),
        "{error:#}"
    );
}

#[test]
fn rejects_implicit_plaintext_trust() {
    let mut config = provider("").expect("base config is valid").cfg;
    config.trusted_plaintext = false;
    let error = validate_config(&config).expect_err("trust must fail");
    assert!(error.to_string().contains("trusted_plaintext"), "{error:#}");
}

#[test]
fn rejects_topology_discovery_and_invalid_topic_filters() {
    let Err(error) = provider("topology_discovery: topic_api\n") else {
        panic!("topology discovery must be protocol-owned");
    };
    assert!(
        error.to_string().contains("topology_discovery"),
        "{error:#}"
    );

    let Err(error) = provider_with_topics("  - path: selected\n    partitions: [1, 1]\n", "")
    else {
        panic!("duplicate partitions must fail");
    };
    assert!(
        error.to_string().contains("duplicate partition 1"),
        "{error:#}"
    );

    let Err(error) = provider_with_topics("  - path: selected\n    partitions: [-1]\n", "") else {
        panic!("negative partitions must fail");
    };
    assert!(
        error.to_string().contains("negative partition -1"),
        "{error:#}"
    );
}

#[test]
fn token_debug_output_is_redacted() {
    let auth = YdbTopicAuthConfig::Token {
        token: "secret".to_owned(),
    };
    let debug = format!("{auth:?}");
    assert!(!debug.contains("secret"));
    assert!(debug.contains("[REDACTED]"));
}

#[test]
fn auth_is_exactly_one_explicit_variant() -> anyhow::Result<()> {
    let token_file: YdbTopicAuthConfig =
        serde_yaml::from_str("type: token_file\ntoken_file: ~/.logbroker/token\n")?;
    token_file.validate()?;

    let error =
        serde_yaml::from_str::<YdbTopicAuthConfig>("type: token\ntoken_file: ~/.logbroker/token\n")
            .expect_err("mismatched auth field must fail");
    assert!(error.to_string().contains("token"), "{error:#}");
    Ok(())
}

#[test]
fn pqv1_driver_is_selected_through_ydb_topic_and_validated() -> anyhow::Result<()> {
    let value = serde_yaml::from_str(
        "host: localhost\nport: 2135\ntopics: [{ path: topic, partitions: [0] }]\nconsumer_name: consumer\nauth: { type: token, token: test }\ndriver: pqv1\ntrusted_plaintext: true\nparser:\n  common:\n    table_naming: { type: from_config, name: events }\n  json_parser:\n    columns:\n      - { jsonpath: $.id, column_name: id, json_data_type: number, arrow_type: Int64, nullable: false }\n    conversion_error: drop\n    unknown_fields: { action: drop }\n",
    )?;
    let provider = build_source_provider(value, Arc::new(MetricsRegistry::new()))?;
    assert!(matches!(
        provider.compatibility(),
        EndpointDescriptor::YdbTopic(_)
    ));

    let dynamic = serde_yaml::from_str(
        "host: localhost\nport: 2135\ntopics: [{ path: topic, partitions: [] }]\nconsumer_name: consumer\nauth: { type: token, token: test }\ndriver: pqv1\ntrusted_plaintext: true\nparser:\n  common:\n    table_naming: { type: from_config, name: events }\n  benchmark_discard: {}\n",
    )?;
    let error = build_source_provider(dynamic, Arc::new(MetricsRegistry::new()))
        .err()
        .ok_or_else(|| anyhow::anyhow!("PQv1 without explicit partitions must fail"))?;
    assert!(error.to_string().contains("explicit topic partitions"));
    Ok(())
}

#[test]
fn stream_read_init_delegates_topology_to_the_protocol() -> anyhow::Result<()> {
    let config = provider_with_topics(
        "  - path: all\n    partitions: []\n  - path: selected\n    partitions: [2, 5]\n",
        "",
    )?
    .cfg;
    let init = init_message(&config, 3);
    let Some(ClientMessage::InitRequest(init)) = init.client_message else {
        panic!("expected StreamRead init request");
    };
    assert!(init.auto_partitioning_support);
    assert_eq!(init.reader_name, "transferia-rust-3");
    assert_eq!(init.topics_read_settings.len(), 2);
    assert_eq!(init.topics_read_settings[0].path, "all");
    assert!(init.topics_read_settings[0].partition_ids.is_empty());
    assert_eq!(init.topics_read_settings[1].path, "selected");
    assert_eq!(init.topics_read_settings[1].partition_ids, [2, 5]);
    Ok(())
}

#[test]
fn raw_gzip_and_zstd_payloads_decode() -> anyhow::Result<()> {
    let payload = b"{\"id\":1}";
    assert_eq!(
        decode_message(Codec::Raw, payload.to_vec())?,
        payload.as_slice()
    );

    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gzip.write_all(payload)?;
    assert_eq!(
        decode_message(Codec::Gzip, gzip.finish()?)?,
        payload.as_slice()
    );

    let zstd = zstd::stream::encode_all(payload.as_slice(), 1)?;
    assert_eq!(decode_message(Codec::Zstd, zstd)?, payload.as_slice());
    Ok(())
}

#[test]
fn commit_ranges_are_sorted_and_coalesced() -> anyhow::Result<()> {
    let ranges = coalesce_ranges(vec![
        OffsetsRange { start: 4, end: 5 },
        OffsetsRange { start: 1, end: 2 },
        OffsetsRange { start: 2, end: 4 },
        OffsetsRange { start: 9, end: 10 },
    ])?;
    assert_eq!(
        ranges,
        [
            OffsetsRange { start: 1, end: 5 },
            OffsetsRange { start: 9, end: 10 }
        ]
    );
    Ok(())
}

#[test]
fn commit_request_groups_multiple_dynamic_partition_sessions() -> anyhow::Result<()> {
    let topic: Arc<str> = Arc::from("topic");
    let sessions = HashMap::from([
        (
            10,
            PartitionSessionState {
                topic_path: Arc::clone(&topic),
                partition_id: 1,
                committed_offset: 0,
                read_through: 5,
                pending_graceful_stop: false,
                invalidated: false,
            },
        ),
        (
            20,
            PartitionSessionState {
                topic_path: Arc::clone(&topic),
                partition_id: 2,
                committed_offset: 3,
                read_through: 8,
                pending_graceful_stop: true,
                invalidated: false,
            },
        ),
    ]);
    let marker = CommitMarker::new(YdbTopicCommitMarker {
        partitions: vec![
            PartitionCommitMarker {
                topic_path: Arc::clone(&topic),
                partition_id: 1,
                partition_session_id: 10,
                ranges: vec![
                    OffsetsRange { start: 0, end: 2 },
                    OffsetsRange { start: 2, end: 5 },
                ],
            },
            PartitionCommitMarker {
                topic_path: topic,
                partition_id: 2,
                partition_session_id: 20,
                ranges: vec![OffsetsRange { start: 3, end: 8 }],
            },
        ],
    });

    let (request, targets) = build_commit_request(&[marker], &sessions)?;
    assert_eq!(request.len(), 2);
    assert_eq!(request[0].partition_session_id, 10);
    assert_eq!(request[0].offsets, [OffsetsRange { start: 0, end: 5 }]);
    assert_eq!(request[1].partition_session_id, 20);
    assert_eq!(targets, HashMap::from([(10, 5), (20, 8)]));
    assert!(releasable_session_ids(&sessions).is_empty());

    let mut committed = sessions;
    committed
        .get_mut(&20)
        .expect("session exists")
        .committed_offset = 8;
    assert_eq!(releasable_session_ids(&committed), [20]);
    Ok(())
}
