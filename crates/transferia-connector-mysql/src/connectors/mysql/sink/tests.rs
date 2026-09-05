use arrow::datatypes::{DataType, TimeUnit};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::config::MySqlSinkConfig;
use super::connector::{
    ambiguous_drop_is_complete, classify_owner_marker, cleanup_ownership_action, decimal_sql_type,
    isolate_discovery, mysql_cleanup_ddl, mysql_owned_create_ddl, mysql_physical_target,
    mysql_sql_type, owner_marker_allows_side_effect, physical_target_set, validate_cleanup_scope,
    CleanupOwnershipAction, MySqlSinkConnector, MySqlSpeedtestScope, OwnerMarkerEvidence,
};
use super::writer::{date_text, decimal_text, timestamp_text};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SourceTopology,
};
use transferia_registry::{
    DatasetPrepare, SinkConnector, SinkSpeedtestIsolation, SpeedtestPhysicalTarget,
};

fn column(data_type: DataType) -> SchemaColumn {
    SchemaColumn::new("value".to_owned(), data_type, false)
}

fn discovery(table: &str) -> DeliveryDiscovery {
    let schema = DatasetSchema::new(vec![column(DataType::Int64)]);
    DeliveryDiscovery {
        source_name: Arc::from("source"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![
            DiscoveredDataset {
                namespace: None,
                update_policy: transferia_core::delivery::UpdatePolicy::Strict,
                role: DatasetRole::Main,
                name: Arc::from(table),
                incoming_schema: schema.clone(),
                stored_schema: schema.clone(),
                system_columns: Vec::new(),
            },
            DiscoveredDataset {
                namespace: None,
                update_policy: transferia_core::delivery::UpdatePolicy::Strict,
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

fn config() -> MySqlSinkConfig {
    serde_yaml::from_str(
        "host: 127.0.0.1\nport: 1\ndatabase: analytics\nusername: test\npassword: ''\ntrusted_plaintext: true\ncreate_tables: true\ninsert_rows: 1000\n",
    )
    .unwrap()
}

#[test]
fn database_override_is_explicit_and_absent_override_preserves_namespace() {
    use transferia_core::delivery::SinkLimits;
    let mut config = config();
    assert_eq!(config.target_database(Some("original")).unwrap(), "analytics");
    config.connection.database.clear();
    config.validate().unwrap();
    assert_eq!(config.target_database(Some("original")).unwrap(), "original");
    assert!(config.target_database(None).is_err());
    assert!(config.target_database(Some("")).is_err());
    let mut discovery = discovery("events");
    assert!(config.validate_discovery(&discovery).is_err());
    for dataset in &mut discovery.datasets { dataset.namespace = Some(Arc::from("original")); }
    config.validate_discovery(&discovery).unwrap();
    let prepared = transferia_registry::SinkPrepare::from_discovery(&discovery, true, "dtt-test", None).unwrap().unwrap();
    assert!(prepared.datasets.iter().all(|dataset| dataset.namespace.as_deref() == Some("original")));
}

#[test]
fn qualified_writes_quote_database_and_table_separately() {
    assert_eq!(super::writer::quote_table(("a.b", "c`d")), "`a.b`.`c``d`");
    assert_ne!(super::writer::quote_table(("a.b", "c")), super::writer::quote_table(("a", "b.c")));
}

#[test]
fn maps_lossless_mysql_column_types() {
    assert_eq!(
        mysql_sql_type(&column(DataType::UInt64)).unwrap(),
        "BIGINT UNSIGNED"
    );
    assert_eq!(
        mysql_sql_type(&column(DataType::Decimal128(65, 30))).unwrap(),
        "DECIMAL(65,30)"
    );
    assert_eq!(
        mysql_sql_type(&column(DataType::Timestamp(TimeUnit::Nanosecond, None))).unwrap(),
        "DATETIME(6)"
    );
    assert_eq!(
        mysql_sql_type(&column(DataType::Utf8).with_arrow_extension(ARROW_JSON_EXTENSION_NAME))
            .unwrap(),
        "JSON"
    );
}

#[test]
fn rejects_types_mysql_cannot_preserve() {
    assert!(decimal_sql_type(66, 0).is_err());
    assert!(decimal_sql_type(38, 31).is_err());
    assert!(mysql_sql_type(&column(DataType::Timestamp(
        TimeUnit::Microsecond,
        Some("Europe/Moscow".into()),
    )))
    .is_err());
    assert!(mysql_sql_type(&column(DataType::Utf8).with_constraints(true, false, None)).is_err());
}

#[test]
fn renders_decimal_values_without_rounding() {
    assert_eq!(decimal_text("12345", 2), "123.45");
    assert_eq!(decimal_text("-12", 4), "-0.0012");
    assert_eq!(decimal_text("12", -3), "12000");
}

#[test]
fn temporal_conversion_is_exact_and_bounded() {
    assert_eq!(date_text(19_782).unwrap(), "2024-02-29");
    assert_eq!(
        timestamp_text(1_709_210_096_123_456, TimeUnit::Microsecond).unwrap(),
        "2024-02-29 12:34:56.123456"
    );
    assert!(timestamp_text(1, TimeUnit::Nanosecond).is_err());
    assert!(date_text(-400_000).is_err());
}

#[test]
fn speedtest_rewrites_every_dataset_within_mysql_name_limit() -> anyhow::Result<()> {
    let original = discovery("events");
    let (rewritten, mapping, tables) =
        isolate_discovery(&original, "0123456789abcdef0123456789abcdef")?;

    assert_eq!(rewritten.datasets.len(), original.datasets.len());
    assert_eq!(mapping.len(), original.datasets.len());
    assert_eq!(tables.len(), original.datasets.len());
    assert!(tables.iter().all(|table| table.chars().count() <= 64));
    assert_eq!(
        mapping.get("events").map(std::convert::AsRef::as_ref),
        Some("_transferia_st_0123456789abcdef0123456789abcdef_0")
    );
    assert_eq!(
        rewritten
            .datasets
            .iter()
            .map(|dataset| Arc::clone(&dataset.name))
            .collect::<BTreeSet<_>>(),
        tables
    );
    assert!(isolate_discovery(&original, "not-random; DROP TABLE events").is_err());
    Ok(())
}

#[test]
fn mysql_physical_proof_contains_database_and_table() {
    assert_eq!(
        mysql_physical_target("analytics", "events").as_ref(),
        "`analytics`.`events`"
    );
}

#[test]
fn mysql_cleanup_ddl_quotes_exact_database_and_table() -> anyhow::Result<()> {
    let table = "_transferia_st_0123456789abcdef0123456789abcdef_f";
    assert_eq!(
        mysql_cleanup_ddl("odd`database", table)?,
        "DROP TABLE IF EXISTS `odd``database`.`_transferia_st_0123456789abcdef0123456789abcdef_f`"
    );
    assert!(mysql_cleanup_ddl("analytics", "events").is_err());
    Ok(())
}

#[tokio::test]
async fn production_mysql_connector_cannot_cleanup_speedtest_tables() -> anyhow::Result<()> {
    let connector = Arc::new(MySqlSinkConnector::from_config(config())?);
    let original = discovery("events");
    let scratch: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let mut rewritten = original.clone();
    rewritten.datasets[0].name = Arc::clone(&scratch);
    rewritten.datasets.truncate(1);
    let single_original = DeliveryDiscovery {
        datasets: original.datasets[..1].to_vec(),
        ..original
    };
    let target = SpeedtestPhysicalTarget {
        production: mysql_physical_target("analytics", "events"),
        scratch: mysql_physical_target("analytics", &scratch),
    };
    let isolation = SinkSpeedtestIsolation::scratch(
        Arc::clone(&connector) as Arc<dyn SinkConnector>,
        &single_original,
        rewritten,
        BTreeMap::from([(Arc::from("events"), Arc::clone(&scratch))]),
        vec![target],
    )?;

    let error = connector.cleanup_speedtest(&isolation).await.unwrap_err();
    assert!(error.to_string().contains("production MySQL connector"));
    Ok(())
}

#[test]
fn mysql_cleanup_requires_exact_table_and_physical_target_sets() -> anyhow::Result<()> {
    let scratch: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let original = discovery("events");
    let mut rewritten = original.clone();
    rewritten.datasets[0].name = Arc::clone(&scratch);
    rewritten.datasets.truncate(1);
    let single_original = DeliveryDiscovery {
        datasets: original.datasets[..1].to_vec(),
        ..original
    };
    let target = SpeedtestPhysicalTarget {
        production: mysql_physical_target("analytics", "events"),
        scratch: mysql_physical_target("analytics", &scratch),
    };
    let isolation = SinkSpeedtestIsolation::scratch(
        Arc::new(MySqlSinkConnector::from_config(config())?),
        &single_original,
        rewritten,
        BTreeMap::from([(Arc::from("events"), Arc::clone(&scratch))]),
        vec![target.clone()],
    )?;
    let valid = MySqlSpeedtestScope {
        database: Arc::from("analytics"),
        owner_marker: Arc::from("transferia-speedtest-owner:test"),
        tables: BTreeSet::from([Arc::clone(&scratch)]),
        schemas: BTreeMap::new(),
        physical_targets: physical_target_set(std::slice::from_ref(&target)),
        attempted_tables: std::sync::Mutex::new(BTreeSet::new()),
        claimed_tables: std::sync::Mutex::new(BTreeSet::new()),
    };
    validate_cleanup_scope(&isolation, &valid)?;

    let wrong_table = MySqlSpeedtestScope {
        database: Arc::from("analytics"),
        owner_marker: Arc::clone(&valid.owner_marker),
        tables: BTreeSet::from([Arc::from(
            "_transferia_st_0123456789abcdef0123456789abcdef_1",
        )]),
        schemas: BTreeMap::new(),
        physical_targets: valid.physical_targets.clone(),
        attempted_tables: std::sync::Mutex::new(BTreeSet::new()),
        claimed_tables: std::sync::Mutex::new(BTreeSet::new()),
    };
    assert!(validate_cleanup_scope(&isolation, &wrong_table).is_err());

    let wrong_target = MySqlSpeedtestScope {
        database: Arc::from("analytics"),
        owner_marker: Arc::clone(&valid.owner_marker),
        tables: BTreeSet::from([scratch]),
        schemas: BTreeMap::new(),
        physical_targets: BTreeSet::from([(
            Arc::from("`analytics`.`events`"),
            Arc::from("`analytics`.`other`"),
        )]),
        attempted_tables: std::sync::Mutex::new(BTreeSet::new()),
        claimed_tables: std::sync::Mutex::new(BTreeSet::new()),
    };
    assert!(validate_cleanup_scope(&isolation, &wrong_target).is_err());
    Ok(())
}

#[test]
fn mysql_owner_marker_rejects_collisions_and_replacements() {
    let owner = "transferia-speedtest-owner:ours";
    assert_eq!(
        classify_owner_marker(Some(owner), owner),
        OwnerMarkerEvidence::Owned
    );
    assert_eq!(
        classify_owner_marker(Some("transferia-speedtest-owner:foreign"), owner),
        OwnerMarkerEvidence::Foreign
    );
    assert_eq!(
        classify_owner_marker(Some(""), owner),
        OwnerMarkerEvidence::Unmarked
    );
    assert_eq!(
        classify_owner_marker(None, owner),
        OwnerMarkerEvidence::Missing
    );
    assert!(owner_marker_allows_side_effect(OwnerMarkerEvidence::Owned));
    assert!(!owner_marker_allows_side_effect(
        OwnerMarkerEvidence::Foreign
    ));
    assert!(!owner_marker_allows_side_effect(
        OwnerMarkerEvidence::Unmarked
    ));
    assert!(!owner_marker_allows_side_effect(
        OwnerMarkerEvidence::Missing
    ));
    assert!(ambiguous_drop_is_complete(OwnerMarkerEvidence::Missing));
    assert!(!ambiguous_drop_is_complete(OwnerMarkerEvidence::Owned));
    assert!(!ambiguous_drop_is_complete(OwnerMarkerEvidence::Foreign));
}

#[test]
fn mysql_scratch_create_is_exclusive_and_has_atomic_owner_comment() -> anyhow::Result<()> {
    let table: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let dataset = DatasetPrepare {
        namespace: None,
        role: DatasetRole::Main,
        table,
        schema: DatasetSchema::new(vec![column(DataType::Int64)]),
        changelog: false,
    };
    let ddl = mysql_owned_create_ddl("odd`database", &dataset, "transferia-speedtest-owner:a'b")?;
    assert!(!ddl.contains("IF NOT EXISTS"));
    assert!(ddl.starts_with(
        "CREATE TABLE `odd``database`.`_transferia_st_0123456789abcdef0123456789abcdef_0`"
    ));
    assert!(ddl.ends_with("ENGINE=InnoDB COMMENT='transferia-speedtest-owner:a''b'"));
    Ok(())
}

#[test]
fn mysql_partial_setup_tracks_every_attempt_but_only_proven_ownership() {
    let first: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let second: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_1");
    let scope = MySqlSpeedtestScope {
        database: Arc::from("analytics"),
        owner_marker: Arc::from("transferia-speedtest-owner:ours"),
        tables: BTreeSet::from([Arc::clone(&first), Arc::clone(&second)]),
        schemas: BTreeMap::new(),
        physical_targets: BTreeSet::new(),
        attempted_tables: std::sync::Mutex::new(BTreeSet::new()),
        claimed_tables: std::sync::Mutex::new(BTreeSet::new()),
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

fn fake_mysql_cleanup(
    scope: &MySqlSpeedtestScope,
    table: &Arc<str>,
    owner_probe: Result<OwnerMarkerEvidence, &'static str>,
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

fn fault_scope(table: &Arc<str>) -> MySqlSpeedtestScope {
    MySqlSpeedtestScope {
        database: Arc::from("analytics"),
        owner_marker: Arc::from("transferia-speedtest-owner:ours"),
        tables: BTreeSet::from([Arc::clone(table)]),
        schemas: BTreeMap::new(),
        physical_targets: BTreeSet::new(),
        attempted_tables: std::sync::Mutex::new(BTreeSet::new()),
        claimed_tables: std::sync::Mutex::new(BTreeSet::new()),
    }
}

#[test]
fn mysql_lost_committed_create_remains_recoverable_after_unreadable_probes() {
    let table: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let scope = fault_scope(&table);
    scope.record_attempt(Arc::clone(&table));

    for _ in 0..2 {
        assert!(fake_mysql_cleanup(&scope, &table, Err("unreadable"), true).is_err());
        assert_eq!(
            scope.attempted_tables(),
            BTreeSet::from([Arc::clone(&table)])
        );
    }
    assert!(
        fake_mysql_cleanup(&scope, &table, Ok(OwnerMarkerEvidence::Owned), true)
            .expect("an exact late owner and schema proof permits exact cleanup")
    );
    assert!(scope.attempted_tables().is_empty());
}

#[test]
fn mysql_foreign_unmarked_or_wrong_schema_collision_is_preserved() {
    let table: Arc<str> = Arc::from("_transferia_st_0123456789abcdef0123456789abcdef_0");
    let scope = fault_scope(&table);
    scope.record_attempt(Arc::clone(&table));

    for evidence in [OwnerMarkerEvidence::Foreign, OwnerMarkerEvidence::Unmarked] {
        let error = fake_mysql_cleanup(&scope, &table, Ok(evidence), true).unwrap_err();
        assert!(error.contains("preserved"));
        assert_eq!(
            scope.attempted_tables(),
            BTreeSet::from([Arc::clone(&table)])
        );
    }
    assert!(
        fake_mysql_cleanup(&scope, &table, Ok(OwnerMarkerEvidence::Owned), false)
            .unwrap_err()
            .contains("preserved")
    );
    assert_eq!(scope.attempted_tables(), BTreeSet::from([table]));
}
