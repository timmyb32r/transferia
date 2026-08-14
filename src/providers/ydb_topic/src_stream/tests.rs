use std::io::Write as _;

use ydb_grpc::ydb_proto::topic::describe_topic_result::PartitionInfo;
use ydb_grpc::ydb_proto::topic::{Codec, DescribeTopicResult, OffsetsRange};

use super::source::{coalesce_ranges, decode_message};
use super::*;

fn provider(extra: &str) -> anyhow::Result<YdbTopicSourceProvider> {
    let value = serde_yaml::from_str(&format!(
        "hosts: [localhost]\nport: 2135\ndatabase: /Root\ntopic_path: topic\nconsumer_name: consumer\ntopology_discovery: topic_api\nauth: {{ type: access_token, token: test }}\ntrusted_plaintext: true\n{extra}parser:\n  common:\n    table_naming: {{ type: from_config, name: events }}\n  json_parser:\n    chunk_splitter: one-message-one-row\n    columns:\n      - {{ jsonpath: $.id, column_name: id, json_data_type: integer, arrow_type: Int64, nullable: false }}\n    conversion_error: dlq\n    unknown_fields: {{ action: fail }}\n"
    ))?;
    YdbTopicSourceProvider::from_config(value, Arc::new(MetricsRegistry::new()))
}

#[test]
fn accepts_explicit_logbroker_shape_without_partition_ids() -> anyhow::Result<()> {
    let provider = provider("")?;
    assert_eq!(provider.cfg.hosts, ["localhost"]);
    assert!(provider.cfg.partition_ids.is_empty());
    assert_eq!(provider.cfg.database, "/Root");
    Ok(())
}

#[test]
fn rejects_implicit_database_and_plaintext_trust() {
    let mut config = provider("").expect("base config is valid").cfg;
    config.database.clear();
    let error = validate_config(&config).expect_err("database must fail");
    assert!(error.to_string().contains("explicit absolute"), "{error:#}");

    config.database = "/Root".to_owned();
    config.trusted_plaintext = false;
    let error = validate_config(&config).expect_err("trust must fail");
    assert!(error.to_string().contains("trusted_plaintext"), "{error:#}");
}

#[test]
fn configured_topology_requires_explicit_partitions() {
    let mut config = provider("").expect("base config is valid").cfg;
    config.topology_discovery = TopologyDiscovery::Configured;
    let error = validate_config(&config).expect_err("partitions must be explicit");
    assert!(error.to_string().contains("must not be empty"), "{error:#}");

    config.partition_ids = vec![0];
    validate_config(&config).expect("explicit partition is valid");
}

#[test]
fn token_debug_output_is_redacted() {
    let auth = YdbTopicAuthConfig {
        auth_type: "access_token".to_owned(),
        token: Some("secret".to_owned()),
        token_file: None,
    };
    let debug = format!("{auth:?}");
    assert!(!debug.contains("secret"));
    assert!(debug.contains("[REDACTED]"));
}

#[test]
fn selects_all_active_or_explicit_partitions() -> anyhow::Result<()> {
    let mut provider = provider("")?;
    let topic = DescribeTopicResult {
        partitions: vec![
            PartitionInfo {
                partition_id: 0,
                active: true,
                ..PartitionInfo::default()
            },
            PartitionInfo {
                partition_id: 1,
                active: false,
                ..PartitionInfo::default()
            },
            PartitionInfo {
                partition_id: 2,
                active: true,
                ..PartitionInfo::default()
            },
        ],
        ..DescribeTopicResult::default()
    };
    assert_eq!(select_partitions(&provider.cfg, &topic)?, [0, 2]);
    provider.cfg.partition_ids = vec![2];
    assert_eq!(select_partitions(&provider.cfg, &topic)?, [2]);
    provider.cfg.partition_ids = vec![1];
    assert!(select_partitions(&provider.cfg, &topic).is_err());
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
