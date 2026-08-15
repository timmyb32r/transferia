use super::*;

fn topic_settings(partitions_count: i32, consumers: &[&str]) -> TopicSettings {
    TopicSettings {
        partitions_count,
        read_rules: consumers
            .iter()
            .map(
                |consumer_name| crate::Ydb::pers_queue::v1::topic_settings::ReadRule {
                    consumer_name: (*consumer_name).to_owned(),
                    ..Default::default()
                },
            )
            .collect(),
        ..Default::default()
    }
}

fn provider(config: &str) -> anyhow::Result<PqV1SourceProvider> {
    let value = serde_yaml::from_str(config)?;
    PqV1SourceProvider::from_config(value, Arc::new(MetricsRegistry::new()))
}

#[test]
fn endpoint_refresh_failure_is_backed_off_without_refreshing_stale_data() {
    let now = Instant::now();
    let mut cached = CachedEndpoints {
        fetched_at: now
            .checked_sub(ENDPOINT_CACHE_TTL + Duration::from_secs(1))
            .expect("test clock has enough history"),
        refresh_retry_at: None,
        main_host: "localhost:2135".into(),
        endpoints: Vec::new(),
    };
    assert!(cached.should_refresh(now));
    cached.defer_refresh(now);
    assert!(!cached.should_refresh(now));
    assert!(cached.should_refresh(now + ENDPOINT_REFRESH_BACKOFF));
    assert!(cached.fetched_at < now);
}

#[test]
fn proxy_failure_aggregation_preserves_a_fatal_disposition() {
    let fatal = PipelineFailure::fatal(anyhow::anyhow!("invalid credentials"));
    let error = connection_failure(7, &["proxy: timed out".into()], Some(fatal.into()));
    let failure = error
        .downcast_ref::<PipelineFailure>()
        .expect("fatal disposition must survive endpoint aggregation");
    assert!(!failure.is_retryable());
}

fn config(partition_group_ids: &str, extra: &str) -> String {
    format!(
        "host: localhost\nport: 2135\ntopic_path: topic\nconsumer_name: consumer\nauth: {{ type: access_token, token: test }}\n{partition_group_ids}{extra}parser:\n  common:\n    table_naming: {{ type: from_config, name: events }}\n  benchmark_discard: {{}}\n"
    )
}

fn json_config(extra: &str) -> String {
    format!(
        "host: localhost\nport: 2135\ntopic_path: topic\nconsumer_name: consumer\nauth: {{ type: access_token, token: test }}\npartition_group_ids: [0]\n{extra}parser:\n  common:\n    table_naming: {{ type: from_config, name: events }}\n  json_parser:\n    json_framing: single_document\n    columns:\n      - jsonpath: $.id\n        column_name: id\n        json_data_type: integer\n        arrow_type: Int64\n        nullable: false\n    conversion_error: dlq\n    unknown_fields: {{ action: fail }}\n"
    )
}

#[test]
fn validates_static_partition_group_ids() {
    for (ids, expected) in [
        ("partition_group_ids: [-1]\n", "must be nonnegative"),
        ("partition_group_ids: [1, 1]\n", "duplicate group 1"),
        ("partition_group_ids: []\n", "must not be empty"),
    ] {
        let error = provider(&config(ids, "")).err().expect("config must fail");
        assert!(error.to_string().contains(expected), "{error:#}");
    }
}

#[test]
fn validates_discovered_topic_metadata() {
    validate_topic_metadata(&topic_settings(3, &["consumer"]), "consumer", &[0, 2]).unwrap();
}

#[test]
fn discovered_topic_must_contain_the_configured_consumer() {
    let error =
        validate_topic_metadata(&topic_settings(3, &["other"]), "consumer", &[0]).unwrap_err();
    assert!(error.to_string().contains("is not configured"), "{error:#}");
}

#[test]
fn discovered_topic_bounds_configured_partition_group_ids() {
    let error =
        validate_topic_metadata(&topic_settings(3, &["consumer"]), "consumer", &[3]).unwrap_err();
    assert!(
        error.to_string().contains("groups in range 0..3"),
        "{error:#}"
    );
}

#[test]
fn discovered_topic_requires_a_stable_partition_topology() {
    for strategy in [
        AutoPartitioningStrategy::Unspecified,
        AutoPartitioningStrategy::ScaleUp,
        AutoPartitioningStrategy::ScaleUpAndDown,
    ] {
        let mut settings = topic_settings(3, &["consumer"]);
        settings.auto_partitioning_settings =
            Some(crate::Ydb::pers_queue::v1::AutoPartitioningSettings {
                strategy: strategy as i32,
                ..Default::default()
            });
        let error = validate_topic_metadata(&settings, "consumer", &[0]).unwrap_err();
        assert!(
            error.to_string().contains(strategy.as_str_name()),
            "{error:#}"
        );
    }

    for strategy in [
        AutoPartitioningStrategy::Disabled,
        AutoPartitioningStrategy::Paused,
    ] {
        let mut settings = topic_settings(3, &["consumer"]);
        settings.auto_partitioning_settings =
            Some(crate::Ydb::pers_queue::v1::AutoPartitioningSettings {
                strategy: strategy as i32,
                ..Default::default()
            });
        validate_topic_metadata(&settings, "consumer", &[0]).unwrap();
    }
}

#[test]
fn rejects_unreasonably_short_network_timeout() {
    let error = provider(&config(
        "partition_group_ids: [0]\n",
        "network_timeout_ms: 99\n",
    ))
    .err()
    .expect("a timeout that makes keepalive self-thrash must fail");

    assert!(
        error
            .to_string()
            .contains("network_timeout_ms must be at least 100ms"),
        "{error:#}"
    );
}

#[test]
fn rejects_unknown_host_field() {
    let invalid = config("partition_group_ids: [0]\n", "").replacen("host:", "host_typo:", 1);
    assert!(provider(&invalid).is_err());
}

#[tokio::test]
async fn rejects_builds_for_undeclared_partitions_before_network_io() {
    let source = provider(&config("partition_group_ids: [0]\n", "")).unwrap();
    let error = source
        .build_source(
            1,
            CancellationToken::new(),
            PipelineMemory::new(1 << 20),
            crate::durable::test_support::context(),
        )
        .await
        .err()
        .expect("undeclared partition must fail locally");
    assert!(error.to_string().contains("not declared"), "{error:#}");
}

#[test]
fn rejects_zero_decompression_concurrency() {
    let error = provider(&config(
        "partition_group_ids: [0]\n",
        "decompression_concurrency: 0\n",
    ))
    .err()
    .expect("zero decompression concurrency must fail");

    assert!(
        error
            .to_string()
            .contains("decompression_concurrency must be positive"),
        "{error:#}"
    );
}

#[test]
fn reports_benchmark_discard_behavior() {
    for cfg in [
        config("partition_group_ids: [0]\n", ""),
        config(
            "partition_group_ids: [0]\n",
            "benchmark_discard_before_decompression: true\n",
        ),
    ] {
        let source = provider(&cfg).unwrap();
        let endpoint = source.compatibility();
        let EndpointDescriptor::PqV1(descriptor) = &endpoint else {
            panic!("expected PQv1 descriptor")
        };
        assert_eq!(descriptor.behavior, SourceBehavior::BenchmarkDiscard);
        let discovery = source
            .configured_delivery_discovery(DeliveryDiscoveryRequest {
                keep_system_columns: false,
            })
            .unwrap();
        assert!(crate::compatibility::validate_pipeline(
            &endpoint,
            &EndpointDescriptor::ClickHouse,
            &discovery,
            false,
        )
        .ensure_valid()
        .is_err());
    }

    let source = provider(&json_config("")).unwrap();
    let EndpointDescriptor::PqV1(descriptor) = source.compatibility() else {
        panic!("expected PQv1 descriptor")
    };
    assert_eq!(descriptor.behavior, SourceBehavior::ProducesRows);
}

#[test]
fn configured_discovery_uses_the_parser_projection() -> anyhow::Result<()> {
    let source = provider(&json_config(""))?;
    let discovery = source.configured_delivery_discovery(DeliveryDiscoveryRequest {
        keep_system_columns: false,
    })?;

    assert_eq!(
        discovery.schema_origin,
        crate::delivery::SchemaOrigin::ParserProjection
    );
    assert_eq!(discovery.source_name.as_ref(), "topic");
    assert_eq!(discovery.source_partitions, [0]);
    assert_eq!(discovery.datasets.len(), 2);
    assert_eq!(
        discovery
            .dataset(crate::delivery::DatasetRole::Main)?
            .name
            .as_ref(),
        "events"
    );
    assert_eq!(
        discovery
            .dataset(crate::delivery::DatasetRole::DeadLetterQueue)?
            .name
            .as_ref(),
        "events_dlq"
    );
    Ok(())
}

#[test]
fn benchmark_discovery_has_no_row_datasets() -> anyhow::Result<()> {
    let source = provider(&config("partition_group_ids: [0]\n", ""))?;
    let discovery = source.configured_delivery_discovery(DeliveryDiscoveryRequest {
        keep_system_columns: false,
    })?;
    assert!(discovery.datasets.is_empty());
    Ok(())
}

#[test]
fn payload_discard_requires_the_discard_parser() {
    let error = provider(&json_config(
        "benchmark_discard_before_decompression: true\n",
    ))
    .err()
    .expect("payload discard with a row parser must fail");
    assert!(error
        .to_string()
        .contains("requires parser.benchmark_discard"));
}

#[test]
fn missing_partition_group_ids_fails_during_provider_construction() {
    let error = provider(&config("", ""))
        .err()
        .expect("missing partition_group_ids must fail");
    assert!(error
        .to_string()
        .contains("missing field `partition_group_ids`"));
}

#[tokio::test]
async fn static_partitions_are_split_without_truncating_ids() {
    let source = provider(&config("partition_group_ids: [0, 1, 4294967297]\n", "")).unwrap();
    assert_eq!(
        source.partitions_for_worker(2, 1).await.unwrap(),
        vec![1, 4_294_967_297]
    );
}

#[test]
fn retries_reuse_partition_source_counters() {
    let source = provider(&config("partition_group_ids: [0, 1]\n", "")).unwrap();
    let first = source.counters_for_partition(0);
    let retry = source.counters_for_partition(0);
    let other = source.counters_for_partition(1);

    assert!(Arc::ptr_eq(&first, &retry));
    assert!(!Arc::ptr_eq(&first, &other));
}

#[test]
fn retries_advance_the_endpoint_failover_cursor_per_partition() {
    let source = provider(&config("partition_group_ids: [0, 1]\n", "")).unwrap();

    assert_eq!(source.next_endpoint_attempt(0), 0);
    assert_eq!(source.next_endpoint_attempt(0), 1);
    assert_eq!(source.next_endpoint_attempt(1), 0);
}

#[test]
fn cached_endpoint_order_remains_partition_specific() {
    let main = "main.test:2135".to_string();
    let endpoints = vec![
        crate::Ydb::discovery::EndpointInfo {
            address: "a.test".into(),
            port: 2135,
            load_factor: 0.0,
            ..Default::default()
        },
        crate::Ydb::discovery::EndpointInfo {
            address: "b.test".into(),
            port: 2135,
            load_factor: 0.0,
            ..Default::default()
        },
    ];

    let first = PqV1Client::order_proxies(main.clone(), endpoints.clone(), 7);
    assert_eq!(first, PqV1Client::order_proxies(main, endpoints, 7));
    assert!(first.contains(&"main.test:2135".to_string()));
}
