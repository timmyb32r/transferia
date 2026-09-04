use arrow::array::{BinaryArray, Date32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::BTreeSet;

use super::*;

#[test]
fn selected_shard_group_must_be_visible_to_the_user() {
    let groups = vec!["analytics".to_owned(), "default".to_owned()];
    assert!(validate_selected_shard_group(Some("analytics"), &groups).is_ok());
    let error = validate_selected_shard_group(Some("private"), &groups).unwrap_err();
    assert!(error
        .to_string()
        .contains("shard group 'private' is not available"));
}

#[test]
fn multi_host_sink_uses_the_only_available_shard_group() -> anyhow::Result<()> {
    let mut config: ClickHouseSinkConfig = serde_yaml::from_str(
        "hosts: [localhost]\nport: 9000\ntrusted_plaintext: true\ndata_host_count: 2\ndatabase: default\nusername: default\n",
    )?;
    let groups = vec!["default".to_owned()];

    assert_eq!(effective_shard_group(&config, &groups)?, Some("default"));

    let ambiguous = vec!["analytics".to_owned(), "default".to_owned()];
    assert!(effective_shard_group(&config, &ambiguous)
        .unwrap_err()
        .to_string()
        .contains("select a shard group explicitly"));
    config.data_host_count = Some(1);
    assert_eq!(effective_shard_group(&config, &[])?, None);
    Ok(())
}

#[test]
fn shard_group_query_materializes_low_cardinality_names_as_plain_strings() {
    assert!(SHARD_GROUPS_QUERY.contains("toString(cluster) AS cluster"));

    let column = BinaryArray::from(vec![Some(b"default".as_slice()), Some(b"analytics")]);
    let mut groups = Vec::new();
    append_shard_groups(&column, &mut groups).unwrap();
    assert_eq!(groups, ["default", "analytics"]);
}

#[tokio::test]
async fn incomplete_credentials_still_verify_network_reachability() -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let port = listener.local_addr()?.port();
    let config: ClickHouseSinkConfig = serde_yaml::from_str(&format!(
        "hosts: [127.0.0.1]\nport: {port}\ntrusted_plaintext: true\ndatabase: ''\nusername: ''\nconnect_timeout_ms: 1000\n"
    ))?;

    let checked = ClickHouseSinkConnector::check_connection(config).await?;

    assert!(matches!(
        checked,
        ClickHouseConnectionCheck::NetworkReachable
    ));
    drop(listener);
    Ok(())
}

#[test]
fn authentication_failure_does_not_expose_server_details() {
    let error = clickhouse_arrow::Error::Client(
        "Exception(ServerError { error: AUTHENTICATION_FAILED, stack_trace: secret })".to_owned(),
    );

    assert_eq!(
        connection_check_error(&error).to_string(),
        "Network connection succeeded, but authentication failed: password is incorrect, or there is no user with such name."
    );
}

#[test]
fn speedtest_rewrites_every_dataset_into_disjoint_scratch_tables() -> anyhow::Result<()> {
    let original = discovery("events", DataType::Int64);
    let (rewritten, mapping, tables) =
        isolate_discovery(&original, "0123456789abcdef0123456789abcdef")?;

    assert_eq!(rewritten.datasets.len(), original.datasets.len());
    let names = rewritten
        .datasets
        .iter()
        .map(|dataset| dataset.name.as_ref())
        .collect::<BTreeSet<_>>();
    assert_eq!(names.len(), original.datasets.len());
    assert!(names.iter().all(|name| is_speedtest_table(name)));
    assert!(names.iter().all(|name| name.len() <= 63));
    assert_eq!(
        mapping.get("events").map(AsRef::as_ref),
        Some("_transferia_st_0123456789abcdef0123456789abcdef_0")
    );
    assert_eq!(tables.len(), original.datasets.len());
    Ok(())
}

#[tokio::test]
async fn speedtest_cleanup_requires_exact_connector_owned_sets() -> anyhow::Result<()> {
    let scratch: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let target = SpeedtestPhysicalTarget {
        production: Arc::from("`analytics`.`events`"),
        scratch: Arc::from(format!("`analytics`.`{scratch}`")),
    };
    let production = Arc::new(ClickHouseSinkConnector::from_config(
        serde_yaml::from_str(
            "hosts: [127.0.0.1]\nport: 1\ntrusted_plaintext: true\ndatabase: analytics\nusername: default\n",
        )?,
    )?);
    let connector: Arc<dyn SinkConnector> = Arc::clone(&production) as Arc<dyn SinkConnector>;
    let original = discovery("events", DataType::Int64);
    let mut rewritten = original.clone();
    rewritten.datasets[0].name = Arc::clone(&scratch);
    rewritten.datasets.truncate(1);
    let single_original = DeliveryDiscovery {
        datasets: original.datasets[..1].to_vec(),
        ..original
    };
    let isolation = SinkSpeedtestIsolation::scratch(
        connector,
        &single_original,
        rewritten,
        BTreeMap::from([(Arc::from("events"), Arc::clone(&scratch))]),
        vec![target.clone()],
    )?;
    let cleanup_error = production.cleanup_speedtest(&isolation).await.unwrap_err();
    assert!(cleanup_error
        .to_string()
        .contains("production ClickHouse connector"));

    let wrong_table_scope = ClickHouseSpeedtestScope {
        database: Arc::from("analytics"),
        owner_marker: Arc::from("transferia-speedtest-owner:test"),
        tables: BTreeSet::from([Arc::from(
            "_transferia_st_0123456789abcdef0123456789abcdef_1",
        )]),
        physical_targets: physical_target_set(std::slice::from_ref(&target)),
        shard_group: None,
        replica_hosts: BTreeSet::from([Arc::from("host-a")]),
        attempted_tables: Mutex::new(BTreeSet::new()),
        claimed_tables: Mutex::new(BTreeSet::new()),
    };
    assert!(validate_cleanup_scope(&isolation, &wrong_table_scope).is_err());

    let wrong_target_scope = ClickHouseSpeedtestScope {
        database: Arc::from("analytics"),
        owner_marker: Arc::from("transferia-speedtest-owner:test"),
        tables: BTreeSet::from([scratch]),
        physical_targets: BTreeSet::from([(
            Arc::from("`analytics`.`events`"),
            Arc::from("`analytics`.`different_scratch`"),
        )]),
        shard_group: None,
        replica_hosts: BTreeSet::from([Arc::from("host-a")]),
        attempted_tables: Mutex::new(BTreeSet::new()),
        claimed_tables: Mutex::new(BTreeSet::new()),
    };
    assert!(validate_cleanup_scope(&isolation, &wrong_target_scope).is_err());
    Ok(())
}

#[test]
fn clickhouse_cleanup_ddl_quotes_database_table_and_cluster() -> anyhow::Result<()> {
    let table = "_transferia_st_0123456789abcdef0123456789abcdef_f";
    assert_eq!(
        clickhouse_cleanup_ddl("odd`db", table, Some("odd`cluster"))?,
        "DROP TABLE IF EXISTS `odd\\`db`.`_transferia_st_0123456789abcdef0123456789abcdef_f` ON CLUSTER `odd\\`cluster` SYNC"
    );
    assert!(clickhouse_cleanup_ddl("analytics", "events", None).is_err());
    Ok(())
}

#[test]
fn speedtest_isolation_id_rejects_noncanonical_or_injectable_values() {
    assert!(validate_isolation_id("0123456789abcdef0123456789abcdef").is_ok());
    assert!(validate_isolation_id("0123456789ABCDEF0123456789ABCDEF").is_err());
    assert!(validate_isolation_id("0123456789abcdef; DROP TABLE events").is_err());
}

#[test]
fn replica_owner_proof_rejects_collisions_missing_replicas_and_replacements() {
    let hosts = BTreeSet::from([Arc::from("host-a"), Arc::from("host-b")]);
    let owner: Arc<str> = Arc::from("transferia-speedtest-owner:ours");
    let owned = BTreeMap::from([
        (Arc::from("host-a"), Some(Arc::clone(&owner))),
        (Arc::from("host-b"), Some(Arc::clone(&owner))),
    ]);
    assert_eq!(
        classify_replica_owners(&hosts, &owned, &owner),
        ReplicaOwnershipEvidence::Owned
    );
    assert_eq!(
        classify_replica_owners(&hosts, &BTreeMap::new(), &owner),
        ReplicaOwnershipEvidence::Missing
    );

    let partial = BTreeMap::from([(Arc::from("host-a"), Some(Arc::clone(&owner)))]);
    assert_eq!(
        classify_replica_owners(&hosts, &partial, &owner),
        ReplicaOwnershipEvidence::Unsafe
    );
    let replaced = BTreeMap::from([
        (Arc::from("host-a"), Some(Arc::clone(&owner))),
        (
            Arc::from("host-b"),
            Some(Arc::from("transferia-speedtest-owner:foreign")),
        ),
    ]);
    assert_eq!(
        classify_replica_owners(&hosts, &replaced, &owner),
        ReplicaOwnershipEvidence::Unsafe
    );
    assert!(replica_owner_allows_side_effect(
        ReplicaOwnershipEvidence::Owned
    ));
    assert!(!replica_owner_allows_side_effect(
        ReplicaOwnershipEvidence::Unsafe
    ));
    assert!(!replica_owner_allows_side_effect(
        ReplicaOwnershipEvidence::Missing
    ));
}

#[test]
fn clickhouse_partial_setup_tracks_every_attempt_but_only_proven_ownership() {
    let first: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let second: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_1");
    let scope = ClickHouseSpeedtestScope {
        database: Arc::from("analytics"),
        owner_marker: Arc::from("transferia-speedtest-owner:ours"),
        tables: BTreeSet::from([Arc::clone(&first), Arc::clone(&second)]),
        physical_targets: BTreeSet::new(),
        shard_group: Some(Arc::from("cluster-a")),
        replica_hosts: BTreeSet::from([Arc::from("host-a")]),
        attempted_tables: Mutex::new(BTreeSet::new()),
        claimed_tables: Mutex::new(BTreeSet::new()),
    };

    scope.record_attempt(Arc::clone(&first));
    scope.claim(Arc::clone(&first));
    scope.record_attempt(Arc::clone(&second));
    assert_eq!(
        scope.attempted_tables(),
        BTreeSet::from([Arc::clone(&first), Arc::clone(&second)])
    );
    assert_eq!(scope.claimed_tables(), BTreeSet::from([Arc::clone(&first)]));
    scope.unclaim(&first);
    scope.unclaim(&first);
    assert!(scope.claimed_tables().is_empty());
    assert_eq!(scope.attempted_tables(), BTreeSet::from([second]));
}

fn fake_clickhouse_cleanup(
    scope: &ClickHouseSpeedtestScope,
    table: &Arc<str>,
    owner_probe: Result<ReplicaOwnershipEvidence, &'static str>,
    schema_matches: bool,
) -> Result<bool, &'static str> {
    match cleanup_ownership_action(owner_probe?) {
        CleanupOwnershipAction::AlreadyAbsent => {
            scope.unclaim(table);
            Ok(false)
        }
        CleanupOwnershipAction::VerifySchemaAndDrop if schema_matches => {
            scope.unclaim(table);
            Ok(true)
        }
        CleanupOwnershipAction::VerifySchemaAndDrop | CleanupOwnershipAction::Preserve => {
            Err("preserved: ownership or schema is not proven")
        }
    }
}

fn fault_scope(table: &Arc<str>) -> ClickHouseSpeedtestScope {
    ClickHouseSpeedtestScope {
        database: Arc::from("analytics"),
        owner_marker: Arc::from("transferia-speedtest-owner:ours"),
        tables: BTreeSet::from([Arc::clone(table)]),
        physical_targets: BTreeSet::new(),
        shard_group: Some(Arc::from("cluster-a")),
        replica_hosts: BTreeSet::from([Arc::from("host-a"), Arc::from("host-b")]),
        attempted_tables: Mutex::new(BTreeSet::new()),
        claimed_tables: Mutex::new(BTreeSet::new()),
    }
}

#[test]
fn clickhouse_lost_committed_create_remains_recoverable_after_unreadable_probes() {
    let table: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let scope = fault_scope(&table);
    scope.record_attempt(Arc::clone(&table));

    for _ in 0..2 {
        assert!(fake_clickhouse_cleanup(&scope, &table, Err("unreadable"), true).is_err());
        assert_eq!(
            scope.attempted_tables(),
            BTreeSet::from([Arc::clone(&table)])
        );
    }
    assert!(
        fake_clickhouse_cleanup(&scope, &table, Ok(ReplicaOwnershipEvidence::Owned), true,)
            .expect("an exact late owner and schema proof permits exact cleanup")
    );
    assert!(scope.attempted_tables().is_empty());
}

#[test]
fn clickhouse_foreign_partial_or_wrong_schema_collision_is_preserved() {
    let table: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let scope = fault_scope(&table);
    scope.record_attempt(Arc::clone(&table));

    let error = fake_clickhouse_cleanup(&scope, &table, Ok(ReplicaOwnershipEvidence::Unsafe), true)
        .unwrap_err();
    assert!(error.contains("preserved"));
    assert_eq!(
        scope.attempted_tables(),
        BTreeSet::from([Arc::clone(&table)])
    );
    assert!(
        fake_clickhouse_cleanup(&scope, &table, Ok(ReplicaOwnershipEvidence::Owned), false,)
            .unwrap_err()
            .contains("preserved")
    );
    assert_eq!(scope.attempted_tables(), BTreeSet::from([table]));
}

#[test]
fn successful_cluster_drop_is_incomplete_while_any_pinned_replica_still_owns_table() {
    let table: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let scope = fault_scope(&table);
    scope.record_attempt(Arc::clone(&table));
    scope.claim(Arc::clone(&table));

    assert_eq!(
        classify_drop_completion(true, ReplicaOwnershipEvidence::Owned),
        DropCompletion::StillOwnedAfterSuccess
    );
    assert_eq!(
        classify_drop_completion(true, ReplicaOwnershipEvidence::Unsafe),
        DropCompletion::Unsafe
    );
    assert_eq!(
        classify_drop_completion(true, ReplicaOwnershipEvidence::Missing),
        DropCompletion::Complete
    );
    assert_eq!(
        scope.attempted_tables(),
        BTreeSet::from([Arc::clone(&table)])
    );
    assert_eq!(scope.claimed_tables(), BTreeSet::from([table]));
}
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::delivery::{DatasetRole, DiscoveredDataset, SchemaOrigin};

fn discovery(table: &str, data_type: DataType) -> DeliveryDiscovery {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("value".into(), data_type, false).with_constraints(true, false, None)
    ]);
    DeliveryDiscovery {
        source_name: Arc::from("source-topic"),
        source_topology: transferia_core::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::ParserProjection,
        keep_system_columns: false,
        datasets: vec![
            DiscoveredDataset {
                role: DatasetRole::Main,
                name: Arc::from(table),
                incoming_schema: schema.clone(),
                stored_schema: schema.clone(),
                system_columns: Vec::new(),
            },
            DiscoveredDataset {
                role: DatasetRole::DeadLetterQueue,
                name: Arc::from(format!("{table}_dlq")),
                incoming_schema: schema.clone(),
                stored_schema: schema,
                system_columns: Vec::new(),
            },
        ],
        performance_advice: Vec::new(),
    }
}

#[tokio::test]
async fn connector_constructs_shared_client_without_connecting() -> anyhow::Result<()> {
    let connector = ClickHouseSinkConnector::from_config(serde_yaml::from_str(
        "hosts: [127.0.0.1]\nport: 1\ntrusted_plaintext: true\ndatabase: default\nusername: default\nconnect_timeout_ms: 1\n",
    )?)?;

    let first = Arc::clone(&connector.client);
    let second = Arc::clone(&connector.client);

    assert!(Arc::ptr_eq(&first, &second));
    Ok(())
}

#[test]
fn limits_are_declarative_and_validate_discovered_schema() -> anyhow::Result<()> {
    let connector = ClickHouseSinkConnector::from_config(serde_yaml::from_str(
        "hosts: [127.0.0.1]\nport: 1\ntrusted_plaintext: true\ndatabase: default\nusername: default\n",
    )?)?;

    let description = connector.limits().description();
    assert_eq!(description.sink, "clickhouse");
    assert_eq!(
        description
            .dataset_name
            .as_ref()
            .expect("table limit")
            .syntax,
        NameSyntax::AsciiIdentifier,
    );
    assert_eq!(
        description
            .column_name
            .as_ref()
            .expect("column limit")
            .syntax,
        NameSyntax::AsciiIdentifier,
    );
    assert!(description.object_key.is_none());
    connector
        .limits()
        .validate_discovery(&discovery("events", DataType::Int64))?;

    let invalid_name = connector
        .limits()
        .validate_discovery(&discovery("default.events", DataType::Int64))
        .unwrap_err();
    assert!(format!("{invalid_name:#}").contains("invalid ClickHouse table name"));

    assert!(description
        .supported_arrow_types
        .contains(&ArrowTypeFamily::Date32));
    connector
        .limits()
        .validate_discovery(&discovery("events", DataType::Date32))?;
    Ok(())
}

#[test]
fn date32_runtime_validation_enforces_clickhouse_lossless_range() -> anyhow::Result<()> {
    let connector = ClickHouseSinkConnector::from_config(serde_yaml::from_str(
        "hosts: [127.0.0.1]\nport: 1\ntrusted_plaintext: true\ndatabase: default\nusername: default\n",
    )?)?;
    let discovery = discovery("events", DataType::Date32);
    let column = &discovery.datasets[0].incoming_schema.columns[0];
    let make_batch = |values: Vec<i32>| -> anyhow::Result<transferia_core::sink::SinkBatch> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                &column.name,
                DataType::Date32,
                false,
            )
            .with_metadata(column.arrow_metadata())])),
            vec![Arc::new(Date32Array::from(values))],
        )?;
        let byte_size = batch.get_array_memory_size();
        Ok(transferia_core::sink::SinkBatch {
            table: Arc::from("events"),
            is_dlq: false,
            batch,
            byte_size,
            memory: transferia_core::memory::PipelineMemory::new(byte_size.max(1))
                .reserve_transform(byte_size),
            system_columns: transferia_core::SystemColumns::default(),
        })
    };

    connector
        .limits()
        .validate_batch(&discovery, &make_batch(vec![-25_567, 0, 120_529])?)?;
    for invalid in [-25_568, 120_530] {
        let error = connector
            .limits()
            .validate_batch(&discovery, &make_batch(vec![invalid])?)
            .expect_err("out-of-range Date32 must fail before ClickHouse I/O");
        assert!(error
            .to_string()
            .contains("outside the lossless ClickHouse Date32 range"));
    }
    Ok(())
}
