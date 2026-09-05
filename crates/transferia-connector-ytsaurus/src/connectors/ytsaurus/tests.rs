#![allow(
    clippy::manual_let_else,
    reason = "the explicit match keeps the negative protocol assertion readable"
)]

use arrow::array::{
    Array as _, ArrayRef, BinaryArray, Date32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use prost::Message as _;
use schemars::schema_for;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use super::client::{
    dynamic_conversion_attributes, dynamic_table_attributes, json_header_value,
    normalize_rpc_proxy_roles, recursive_removal_parameters, resolved_link_suggestion,
    rich_read_path, rpc_proxy_discovery_url, sort_operation_parameters,
    speedtest_directory_parameters, static_table_attributes, suggestion_directory,
    table_path_suggestions, table_writer_spec, uniform_reshard_parameters, yson_header_value,
    ListedNode,
};
use super::config::{
    YTsaurusAtomicity, YTsaurusBigValuePolicy, YTsaurusOptimizeFor, YTsaurusPrimaryKeySemantics,
    YTsaurusReadFormat, YTsaurusReadOrdering, YTsaurusSinkConfig, YTsaurusSourceConfig,
    YTsaurusTableReaderConfig,
};
use super::discard::{output_format, DiscardDecoder};
use super::native_rpc::{
    checksum_matches, crc64, credentials, is_transient_dynamic_write_error,
    is_transient_dynamic_write_error_code, receive_read_worker_item, rowset_payload,
    NativeReadFormat, NativeRowModification,
};

#[test]
fn dynamic_row_modification_codes_match_the_native_rpc_contract() {
    assert_eq!(NativeRowModification::Write.rpc_value(), 0);
    assert_eq!(NativeRowModification::Delete.rpc_value(), 1);
    assert_eq!(NativeRowModification::Modify.rpc_value(), 3);
}
use super::schema::{parse_schema, schema_to_yt, schemas_equal, sorted_unique_schema_to_yt};
use super::sink::{
    cleanup_ytsaurus_speedtest_root, drop_oversized_rows, dynamic_row_modification, encode_arrow,
    encode_arrow_batches, exact_ytsaurus_row_count, is_speedtest_scratch_root,
    is_within_speedtest_root, physical_target_set, prepare_ytsaurus_speedtest_root,
    snapshot_ytsaurus_row_counts, speedtest_root_proof, speedtest_scratch_root,
    validate_initial_tablet_count, validate_row_weight, validate_speedtest_cleanup_scope,
    validate_speedtest_isolation_id, validate_temporal_values, yt_guid, YTsaurusRowCountClient,
    YTsaurusSinkConnector, YTsaurusSpeedtestClient, YTsaurusSpeedtestScope,
};
use super::src_batch::{
    dataset_arrow_schema, normalize_read_batch, performance_advice, system_column_layout,
    DiscoveredTable, PhysicalChunkLayout,
};
use super::yt_wire::{encode_wire_batch, YtWireDecoder};
use transferia_core::data::changelog::{project_sink_batch, ProjectedSinkBatch};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};
use transferia_core::data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
use transferia_core::delivery::{
    DatasetRole, DeliveryDiscovery, DiscoveredDataset, SchemaOrigin, SinkLimits as _,
    SourceTopology,
};
use transferia_core::memory::PipelineMemory;
use transferia_core::sink::SinkBatch;
use transferia_registry::{
    SinkConnector as _, SinkSpeedtestIsolation, SnapshotRowCountStrategy, SpeedtestPhysicalTarget,
};

fn speedtest_sink_discovery() -> DeliveryDiscovery {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".to_owned(), DataType::Int64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("payload".to_owned(), DataType::Utf8, true),
    ]);
    DeliveryDiscovery {
        source_name: Arc::from("speedtest-source"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![
            DiscoveredDataset {
                update_policy: transferia_core::delivery::UpdatePolicy::Strict,
                role: DatasetRole::Main,
                name: Arc::from("events"),
                incoming_schema: schema.clone(),
                stored_schema: schema.clone(),
                system_columns: Vec::new(),
            },
            DiscoveredDataset {
                update_policy: transferia_core::delivery::UpdatePolicy::Strict,
                role: DatasetRole::DeadLetterQueue,
                name: Arc::from("events_dlq"),
                incoming_schema: schema.clone(),
                stored_schema: schema,
                system_columns: Vec::new(),
            },
        ],
        performance_advice: Vec::new(),
    }
}

fn speedtest_static_config(replace_tables: bool) -> anyhow::Result<YTsaurusSinkConfig> {
    Ok(serde_yaml::from_str(&format!(
        "tables: {{ type: static_tables, replace_tables: {replace_tables}, path: //tmp/production }}\n\
         auth: {{ type: token, token: test }}\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n"
    ))?)
}

#[test]
fn sink_account_is_validated_and_added_to_created_tables() -> anyhow::Result<()> {
    let config: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: static_tables, replace_tables: true, path: //tmp/output }\n\
         account: transferia\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n",
    )?;
    let connector = YTsaurusSinkConnector::from_config(config)?;
    assert_eq!(
        connector
            .table_attributes_for_transfer("delivery")
            .get("account"),
        Some(&serde_json::Value::String("transferia".to_owned()))
    );

    let invalid: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: static_tables, replace_tables: true, path: //tmp/output }\n\
         account: '   '\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n",
    )?;
    assert!(invalid
        .validate()
        .unwrap_err()
        .to_string()
        .contains("ytsaurus.account"));
    Ok(())
}

#[test]
fn ytsaurus_row_count_verification_requires_lossless_replacement() -> anyhow::Result<()> {
    let replaced = YTsaurusSinkConnector::from_config(speedtest_static_config(true)?)?;
    assert_eq!(
        replaced.snapshot_row_count_strategy(),
        Some(SnapshotRowCountStrategy::ReplacedTotal)
    );

    let appended = YTsaurusSinkConnector::from_config(speedtest_static_config(false)?)?;
    assert_eq!(appended.snapshot_row_count_strategy(), None);

    let dropping: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: static_tables, replace_tables: true, path: //tmp/output, big_value_policy: drop }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n",
    )?;
    let dropping = YTsaurusSinkConnector::from_config(dropping)?;
    assert_eq!(
        dropping.snapshot_row_count_strategy(),
        None,
        "an explicit row-dropping policy cannot claim output-row equality"
    );
    Ok(())
}

#[test]
fn ytsaurus_row_count_attribute_must_be_an_exact_non_negative_integer() {
    for rows in [0_u64, 1, u64::MAX] {
        assert_eq!(
            exact_ytsaurus_row_count(&serde_json::json!(rows)).expect("exact row count"),
            rows
        );
    }
    for value in [
        serde_json::json!(-1),
        serde_json::json!(1.5),
        serde_json::json!("10"),
        serde_json::Value::Null,
    ] {
        assert!(exact_ytsaurus_row_count(&value).is_err());
    }
}

struct FakeYTsaurusRowCountClient {
    rows: BTreeMap<String, serde_json::Value>,
    row_reads: Mutex<Vec<String>>,
}

impl YTsaurusRowCountClient for FakeYTsaurusRowCountClient {
    fn node_exists<'a>(&'a self, path: &'a str) -> BoxFuture<'a, anyhow::Result<bool>> {
        Box::pin(async move { Ok(self.rows.contains_key(path)) })
    }

    fn row_count<'a>(&'a self, path: &'a str) -> BoxFuture<'a, anyhow::Result<serde_json::Value>> {
        Box::pin(async move {
            self.row_reads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(path.to_owned());
            self.rows
                .get(path)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing fake row count"))
        })
    }
}

#[tokio::test]
async fn ytsaurus_snapshot_probe_reports_every_dataset_without_scanning_missing_tables(
) -> anyhow::Result<()> {
    let config = speedtest_static_config(true)?;
    let client = FakeYTsaurusRowCountClient {
        rows: BTreeMap::from([("//tmp/production/events".to_owned(), serde_json::json!(41))]),
        row_reads: Mutex::new(Vec::new()),
    };

    let counts =
        snapshot_ytsaurus_row_counts(&client, &config, &speedtest_sink_discovery()).await?;

    assert_eq!(counts.len(), 2);
    assert_eq!(counts[0].role, DatasetRole::Main);
    assert_eq!(counts[0].table.as_ref(), "events");
    assert_eq!(counts[0].target.as_ref(), "//tmp/production/events");
    assert!(counts[0].exists);
    assert_eq!(counts[0].rows, 41);
    assert_eq!(counts[1].role, DatasetRole::DeadLetterQueue);
    assert_eq!(counts[1].target.as_ref(), "//tmp/production/events_dlq");
    assert!(!counts[1].exists);
    assert_eq!(counts[1].rows, 0);
    assert_eq!(
        *client
            .row_reads
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec!["//tmp/production/events".to_owned()]
    );
    Ok(())
}

#[tokio::test]
async fn ytsaurus_snapshot_probe_rejects_inexact_metadata() -> anyhow::Result<()> {
    let config = speedtest_static_config(true)?;
    let client = FakeYTsaurusRowCountClient {
        rows: BTreeMap::from([(
            "//tmp/production/events".to_owned(),
            serde_json::json!("41"),
        )]),
        row_reads: Mutex::new(Vec::new()),
    };
    let mut discovery = speedtest_sink_discovery();
    discovery.datasets.truncate(1);

    let error = snapshot_ytsaurus_row_counts(&client, &config, &discovery)
        .await
        .expect_err("string metadata must not be coerced to an integer");
    assert!(error.to_string().contains("not a non-negative integer"));
    Ok(())
}

fn assert_same_speedtest_schema(left: &DatasetSchema, right: &DatasetSchema) {
    assert_eq!(left.columns.len(), right.columns.len());
    for (left, right) in left.columns.iter().zip(&right.columns) {
        assert_eq!(left.name, right.name);
        assert_eq!(left.data_type, right.data_type);
        assert_eq!(left.nullable, right.nullable);
        assert_eq!(left.primary_key, right.primary_key);
        assert_eq!(left.low_cardinality, right.low_cardinality);
        assert_eq!(left.max_length, right.max_length);
        assert_eq!(left.arrow_extension_name, right.arrow_extension_name);
        assert_eq!(left.system_role, right.system_role);
        assert_eq!(left.old_value_of, right.old_value_of);
        assert_eq!(left.old_key_of, right.old_key_of);
    }
}

#[test]
fn speedtest_root_is_a_canonical_128_bit_sibling_namespace() -> anyhow::Result<()> {
    let id = "0123456789abcdef0123456789abcdef";
    let root = speedtest_scratch_root("//home/transfer/output/", id)?;
    assert_eq!(
        root,
        "//home/transfer/.transferia-speedtest-0123456789abcdef0123456789abcdef"
    );
    assert!(is_speedtest_scratch_root(&root));
    assert!(validate_speedtest_isolation_id(id).is_ok());
    assert!(validate_speedtest_isolation_id("0123456789ABCDEF0123456789ABCDEF").is_err());
    assert!(validate_speedtest_isolation_id("../../production").is_err());
    assert!(speedtest_scratch_root("//", id).is_err());
    let create = speedtest_directory_parameters(&root, "owner", "1-2-3-4", false);
    assert_eq!(create["path"], root);
    assert_eq!(create["recursive"], false);
    assert_eq!(create["ignore_existing"], false);
    assert_eq!(create["attributes"]["transferia_speedtest_owner"], "owner");
    assert_eq!(create["mutation_id"], "1-2-3-4");
    assert_eq!(create["retry"], false);
    let remove = recursive_removal_parameters(&root, 42, "4-3-2-1", true);
    assert_eq!(remove["path"], root);
    assert_eq!(remove["force"], true);
    assert_eq!(remove["recursive"], true);
    assert_eq!(remove["prerequisite_revisions"][0]["path"], root);
    assert_eq!(remove["prerequisite_revisions"][0]["revision"], 42);
    assert_eq!(remove["mutation_id"], "4-3-2-1");
    assert_eq!(remove["retry"], true);
    assert_eq!(
        speedtest_root_proof(
            &serde_json::json!({
                "transferia_speedtest_owner": "owner",
                "revision": 42
            }),
            "owner"
        )?,
        42
    );
    assert!(speedtest_root_proof(
        &serde_json::json!({
            "transferia_speedtest_owner": "foreign",
            "revision": 42
        }),
        "owner"
    )
    .is_err());
    Ok(())
}

#[tokio::test]
async fn speedtest_isolation_preserves_logical_datasets_and_never_mutates_production_config(
) -> anyhow::Result<()> {
    let production_config = speedtest_static_config(false)?;
    let connector = Arc::new(YTsaurusSinkConnector::from_config(
        production_config.clone(),
    )?);
    let original = Arc::new(speedtest_sink_discovery());
    let isolation = Arc::clone(&connector)
        .isolate_speedtest(
            Arc::clone(&original),
            "0123456789abcdef0123456789abcdef".to_owned(),
        )
        .await?;

    assert_eq!(production_config.path(), "//tmp/production");
    assert!(!production_config.replace_tables());
    assert_eq!(isolation.discovery.datasets.len(), original.datasets.len());
    for (isolated, original) in isolation.discovery.datasets.iter().zip(&original.datasets) {
        assert_eq!(isolated.name, original.name);
        assert_eq!(isolated.role, original.role);
        assert_same_speedtest_schema(&isolated.incoming_schema, &original.incoming_schema);
        assert_same_speedtest_schema(&isolated.stored_schema, &original.stored_schema);
        assert_eq!(isolated.system_columns, original.system_columns);
        assert_eq!(isolation.table_name(&original.name)?, original.name);
    }
    assert_eq!(isolation.physical_targets().len(), 2);
    assert!(isolation.physical_targets().iter().all(|target| {
        target.production.starts_with("//tmp/production/")
            && target
                .scratch
                .starts_with("//tmp/.transferia-speedtest-0123456789abcdef0123456789abcdef/")
            && target.production != target.scratch
    }));
    isolation.connector().cleanup_speedtest(&isolation).await?;
    assert!(connector.cleanup_speedtest(&isolation).await.is_err());
    Ok(())
}

#[test]
fn dynamic_via_static_clone_forces_replacement_and_keeps_every_artifact_inside_owned_root(
) -> anyhow::Result<()> {
    let production: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: dynamic_tables, replace_tables: false, path: //tmp/production }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n\
         trusted_native_rpc_plaintext: true\n",
    )?;
    assert!(production.stages_dynamic_snapshots());
    assert!(!production.replace_tables());
    let root = speedtest_scratch_root(production.path(), "0123456789abcdef0123456789abcdef")?;
    let isolated = production.clone_for_speedtest(root.clone())?;
    assert!(isolated.stages_dynamic_snapshots());
    assert!(isolated.replace_tables());
    assert_eq!(isolated.path(), root);

    let destination = format!("{root}/events");
    let staging = format!("{root}/.transferia-stage-deadbeef-events");
    let sorted = format!("{root}/.transferia-sorted-deadbeef-events");
    assert!(is_within_speedtest_root(&root, &destination));
    assert!(is_within_speedtest_root(&root, &staging));
    assert!(is_within_speedtest_root(&root, &sorted));
    assert!(!is_within_speedtest_root(
        &root,
        "//tmp/production/.transferia-stage-deadbeef-events"
    ));
    assert!(!is_within_speedtest_root(
        &root,
        &format!("{root}-lookalike/events")
    ));
    Ok(())
}

#[test]
fn speedtest_cleanup_refuses_tampered_root_discovery_or_physical_proof() -> anyhow::Result<()> {
    let production = speedtest_static_config(false)?;
    let discovery = speedtest_sink_discovery();
    let root = speedtest_scratch_root(production.path(), "0123456789abcdef0123456789abcdef")?;
    let isolated_config = production.clone_for_speedtest(root.clone())?;
    let mapping = discovery
        .datasets
        .iter()
        .map(|dataset| (Arc::clone(&dataset.name), Arc::clone(&dataset.name)))
        .collect::<BTreeMap<_, _>>();
    let targets = discovery
        .datasets
        .iter()
        .map(|dataset| {
            Ok(SpeedtestPhysicalTarget {
                production: Arc::from(production.path_for_dataset(&dataset.name)?),
                scratch: Arc::from(isolated_config.path_for_dataset(&dataset.name)?),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let connector: Arc<dyn transferia_registry::SinkConnector> =
        Arc::new(YTsaurusSinkConnector::from_config(isolated_config.clone())?);
    let isolation = SinkSpeedtestIsolation::scratch(
        connector,
        &discovery,
        discovery.clone(),
        mapping,
        targets.clone(),
    )?;
    let scope = YTsaurusSpeedtestScope {
        root: Arc::from(root),
        owner: Arc::from("owner"),
        create_mutation_id: Arc::from("1-2-3-4"),
        remove_mutation_id: Arc::from("4-3-2-1"),
        datasets: discovery
            .datasets
            .iter()
            .map(|dataset| Arc::clone(&dataset.name))
            .collect(),
        physical_targets: physical_target_set(&targets),
        creation_attempted: AtomicBool::new(false),
        cleaned: AtomicBool::new(false),
    };
    validate_speedtest_cleanup_scope(&isolated_config, &isolation, &scope)?;

    let wrong_root = YTsaurusSpeedtestScope {
        root: Arc::from("//tmp/production"),
        owner: Arc::clone(&scope.owner),
        create_mutation_id: Arc::clone(&scope.create_mutation_id),
        remove_mutation_id: Arc::clone(&scope.remove_mutation_id),
        datasets: scope.datasets.clone(),
        physical_targets: scope.physical_targets.clone(),
        creation_attempted: AtomicBool::new(false),
        cleaned: AtomicBool::new(false),
    };
    assert!(validate_speedtest_cleanup_scope(&isolated_config, &isolation, &wrong_root).is_err());

    let mut wrong_targets = scope.physical_targets.clone();
    let first = wrong_targets.pop_first().unwrap();
    wrong_targets.insert((first.0, Arc::from("//tmp/production/events")));
    let wrong_proof = YTsaurusSpeedtestScope {
        root: scope.root,
        owner: scope.owner,
        create_mutation_id: scope.create_mutation_id,
        remove_mutation_id: scope.remove_mutation_id,
        datasets: scope.datasets,
        physical_targets: wrong_targets,
        creation_attempted: AtomicBool::new(false),
        cleaned: AtomicBool::new(false),
    };
    assert!(validate_speedtest_cleanup_scope(&isolated_config, &isolation, &wrong_proof).is_err());
    Ok(())
}

#[derive(Clone, Copy)]
enum FakeYtCreateOutcome {
    Success,
    LostResponse,
    Collision,
}

struct FakeYtSpeedtestState {
    attributes: Option<serde_json::Value>,
    create_outcome: FakeYtCreateOutcome,
    attributes_unreadable: bool,
    replace_before_remove: bool,
    lose_remove_response: bool,
    remove_calls: usize,
}

struct FakeYtSpeedtestClient {
    state: Mutex<FakeYtSpeedtestState>,
}

impl FakeYtSpeedtestClient {
    fn new(create_outcome: FakeYtCreateOutcome) -> Self {
        Self {
            state: Mutex::new(FakeYtSpeedtestState {
                attributes: None,
                create_outcome,
                attributes_unreadable: false,
                replace_before_remove: false,
                lose_remove_response: false,
                remove_calls: 0,
            }),
        }
    }
}

impl YTsaurusSpeedtestClient for FakeYtSpeedtestClient {
    fn create_root<'a>(
        &'a self,
        _path: &'a str,
        owner: &'a str,
        _mutation_id: &'a str,
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            match state.create_outcome {
                FakeYtCreateOutcome::Success => {
                    state.attributes = Some(serde_json::json!({
                        "transferia_speedtest_owner": owner,
                        "revision": 7
                    }));
                    Ok(())
                }
                FakeYtCreateOutcome::LostResponse => {
                    state.attributes = Some(serde_json::json!({
                        "transferia_speedtest_owner": owner,
                        "revision": 7
                    }));
                    anyhow::bail!("create response lost")
                }
                FakeYtCreateOutcome::Collision => anyhow::bail!("path already exists"),
            }
        })
    }

    fn root_attributes<'a>(
        &'a self,
        _path: &'a str,
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<serde_json::Value>> {
        Box::pin(async move {
            let state = self.state.lock().unwrap();
            if state.attributes_unreadable {
                anyhow::bail!("attributes timed out")
            }
            state
                .attributes
                .clone()
                .ok_or_else(|| anyhow::anyhow!("root is missing"))
        })
    }

    fn remove_root<'a>(
        &'a self,
        _path: &'a str,
        revision: u64,
        _mutation_id: &'a str,
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let mut state = self.state.lock().unwrap();
            if state.replace_before_remove {
                state.attributes = Some(serde_json::json!({
                    "transferia_speedtest_owner": "replacement",
                    "revision": revision + 1
                }));
            }
            let current_revision = state
                .attributes
                .as_ref()
                .and_then(|attributes| attributes["revision"].as_u64());
            anyhow::ensure!(
                current_revision == Some(revision),
                "prerequisite revision changed"
            );
            state.remove_calls += 1;
            state.attributes = None;
            let lose_remove_response = state.lose_remove_response;
            drop(state);
            if lose_remove_response {
                anyhow::bail!("remove response lost")
            }
            Ok(())
        })
    }

    fn root_exists<'a>(
        &'a self,
        _path: &'a str,
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<bool>> {
        Box::pin(async move { Ok(self.state.lock().unwrap().attributes.is_some()) })
    }
}

fn fake_yt_scope() -> YTsaurusSpeedtestScope {
    YTsaurusSpeedtestScope {
        root: Arc::from("//tmp/.transferia-speedtest-0123456789abcdef0123456789abcdef"),
        owner: Arc::from("owner"),
        create_mutation_id: Arc::from("1-2-3-4"),
        remove_mutation_id: Arc::from("4-3-2-1"),
        datasets: BTreeSet::default(),
        physical_targets: BTreeSet::default(),
        creation_attempted: AtomicBool::new(false),
        cleaned: AtomicBool::new(false),
    }
}

#[tokio::test]
async fn yt_lost_create_response_is_cleaned_after_exact_owner_proof() -> anyhow::Result<()> {
    let client = FakeYtSpeedtestClient::new(FakeYtCreateOutcome::LostResponse);
    let scope = fake_yt_scope();
    prepare_ytsaurus_speedtest_root(&client, &scope).await?;
    cleanup_ytsaurus_speedtest_root(&client, &scope).await?;
    cleanup_ytsaurus_speedtest_root(&client, &scope).await?;
    let state = client.state.lock().unwrap();
    assert_eq!(state.remove_calls, 1);
    assert!(state.attributes.is_none());
    drop(state);
    Ok(())
}

#[tokio::test]
async fn yt_foreign_collision_and_unreadable_marker_never_permit_remove() -> anyhow::Result<()> {
    let client = FakeYtSpeedtestClient::new(FakeYtCreateOutcome::Collision);
    let scope = fake_yt_scope();
    client.state.lock().unwrap().attributes = Some(serde_json::json!({
        "transferia_speedtest_owner": "foreign",
        "revision": 7
    }));
    assert!(prepare_ytsaurus_speedtest_root(&client, &scope)
        .await
        .is_err());
    assert!(cleanup_ytsaurus_speedtest_root(&client, &scope)
        .await
        .is_err());
    assert_eq!(client.state.lock().unwrap().remove_calls, 0);

    let client = FakeYtSpeedtestClient::new(FakeYtCreateOutcome::Success);
    let scope = fake_yt_scope();
    prepare_ytsaurus_speedtest_root(&client, &scope).await?;
    client.state.lock().unwrap().attributes_unreadable = true;
    assert!(cleanup_ytsaurus_speedtest_root(&client, &scope)
        .await
        .is_err());
    assert_eq!(client.state.lock().unwrap().remove_calls, 0);
    Ok(())
}

#[tokio::test]
async fn yt_revision_prerequisite_preserves_replacement() -> anyhow::Result<()> {
    let client = FakeYtSpeedtestClient::new(FakeYtCreateOutcome::Success);
    let scope = fake_yt_scope();
    prepare_ytsaurus_speedtest_root(&client, &scope).await?;
    client.state.lock().unwrap().replace_before_remove = true;
    assert!(cleanup_ytsaurus_speedtest_root(&client, &scope)
        .await
        .is_err());
    let state = client.state.lock().unwrap();
    assert_eq!(state.remove_calls, 0);
    assert_eq!(
        state.attributes.as_ref().unwrap()["transferia_speedtest_owner"],
        "replacement"
    );
    drop(state);
    Ok(())
}

#[tokio::test]
async fn yt_lost_remove_response_is_success_only_after_absence_proof() -> anyhow::Result<()> {
    let client = FakeYtSpeedtestClient::new(FakeYtCreateOutcome::Success);
    let scope = fake_yt_scope();
    prepare_ytsaurus_speedtest_root(&client, &scope).await?;
    client.state.lock().unwrap().lose_remove_response = true;
    cleanup_ytsaurus_speedtest_root(&client, &scope).await?;
    cleanup_ytsaurus_speedtest_root(&client, &scope).await?;
    let state = client.state.lock().unwrap();
    assert_eq!(state.remove_calls, 1);
    assert!(state.attributes.is_none());
    drop(state);
    Ok(())
}

#[tokio::test]
async fn yt_cleanup_is_idempotent_when_the_owned_root_is_already_absent() -> anyhow::Result<()> {
    let client = FakeYtSpeedtestClient::new(FakeYtCreateOutcome::Success);
    let scope = fake_yt_scope();
    scope.creation_attempted.store(true, Ordering::Release);

    cleanup_ytsaurus_speedtest_root(&client, &scope).await?;
    cleanup_ytsaurus_speedtest_root(&client, &scope).await?;

    let state = client.state.lock().unwrap();
    assert_eq!(state.remove_calls, 0);
    assert!(state.attributes.is_none());
    drop(state);
    Ok(())
}

#[tokio::test]
async fn dynamic_partial_update_preserves_toast_by_using_modify_with_only_changed_columns(
) -> anyhow::Result<()> {
    let id =
        SchemaColumn::new("id".into(), DataType::Int64, false).with_constraints(true, false, None);
    let payload = SchemaColumn::new("payload".into(), DataType::Utf8, true);
    let discovery = DeliveryDiscovery {
        source_name: Arc::from("postgres-cdc"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            update_policy: transferia_core::delivery::UpdatePolicy::Strict,
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema: DatasetSchema::new(vec![
                id.clone(),
                payload.clone(),
                SchemaColumn::new(
                    SystemColumnKind::ChangeOperation.default_name().into(),
                    DataType::Utf8,
                    false,
                ),
                SchemaColumn::new(
                    SystemColumnKind::Offset.default_name().into(),
                    DataType::Int64,
                    false,
                ),
                SchemaColumn::new(
                    SystemColumnKind::ChangedColumns.default_name().into(),
                    DataType::Binary,
                    false,
                ),
            ]),
            stored_schema: DatasetSchema::new(vec![id, payload]),
            system_columns: vec![
                SystemColumnKind::ChangeOperation.into(),
                SystemColumnKind::Offset.into(),
                SystemColumnKind::ChangedColumns.into(),
            ],
        }],
        performance_advice: Vec::new(),
    };
    let fields = discovery.datasets[0]
        .incoming_schema
        .columns
        .iter()
        .map(|column| {
            Field::new(&column.name, column.data_type.clone(), column.nullable)
                .with_metadata(column.arrow_metadata())
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Int64Array::from(vec![7])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            Arc::new(StringArray::from(vec!["u"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![42])) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values([&[0b01_u8][..]])) as ArrayRef,
        ],
    )?;
    let bytes = batch.get_array_memory_size();
    let input = SinkBatch {
        table: Arc::from("events"),
        is_dlq: false,
        batch,
        byte_size: bytes,
        memory: PipelineMemory::new(1024 * 1024).reserve(1).await,
        system_columns: SystemColumns::new(vec![
            SystemColumn {
                kind: SystemColumnKind::ChangeOperation,
                name: Arc::from(SystemColumnKind::ChangeOperation.default_name()),
                index: 2,
            },
            SystemColumn {
                kind: SystemColumnKind::Offset,
                name: Arc::from(SystemColumnKind::Offset.default_name()),
                index: 3,
            },
            SystemColumn {
                kind: SystemColumnKind::ChangedColumns,
                name: Arc::from(SystemColumnKind::ChangedColumns.default_name()),
                index: 4,
            },
        ]),
    };

    let ProjectedSinkBatch::Changelog(changelog) = project_sink_batch(&discovery, &input)? else {
        panic!("changed-column metadata must produce a changelog batch")
    };
    let runs = changelog.collapsed_runs()?;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].operation, transferia_core::ChangeOperation::Update);
    assert_eq!(runs[0].batch.num_columns(), 1);
    assert_eq!(runs[0].batch.schema().field(0).name(), "id");
    assert_eq!(
        dynamic_row_modification(runs[0].operation).rpc_value(),
        NativeRowModification::Modify.rpc_value()
    );
    let encoded = encode_wire_batch(&runs[0].batch)?;
    assert_eq!(encoded.column_names, ["id"]);
    Ok(())
}

#[test]
fn attribute_path_keeps_exactly_one_separator_after_a_trailing_slash() {
    let path = "//home/example/benchmarks/run-65c147cb/";

    assert_eq!(
        super::attribute_path(path, "type"),
        "//home/example/benchmarks/run-65c147cb/@type"
    );
    assert_eq!(
        super::attribute_path(path.trim_end_matches('/'), "type"),
        "//home/example/benchmarks/run-65c147cb/@type"
    );
}

#[test]
fn loopback_heavy_proxy_detection_preserves_forwarded_endpoints() {
    assert!(super::client::is_loopback_host("localhost"));
    assert!(super::client::is_loopback_host("127.0.0.1"));
    assert!(super::client::is_loopback_host("::1"));
    assert!(!super::client::is_loopback_host("proxy.example.net"));
}

#[test]
fn distributed_write_parameters_are_safe_ascii_http_headers() {
    let header = json_header_value(&serde_json::json!({
        "cookie": {"opaque": "signed-куки-🦀\u{007f}"},
    }))
    .expect("serialize header parameters");

    assert!(header.is_ascii());
    assert!(header.contains("\\u043a\\u0443\\u043a\\u0438"));
    assert!(header.contains("\\ud83e\\udd80"));
    assert!(header.contains("\\u007f"));
    reqwest::header::HeaderValue::from_str(&header).expect("valid HTTP header value");
}

#[test]
fn yson_header_preserves_schema_attributes() {
    let header = yson_header_value(&serde_json::json!({
        "schema": {
            "$attributes": {"strict": true, "unique_keys": true},
            "$value": [{"name": "id", "type": "int64", "sort_order": "ascending"}],
        },
    }))
    .expect("serialize attributed YSON");

    assert_eq!(
        header,
        r#"{"schema"=<"strict"=%true;"unique_keys"=%true;>[{"name"="id";"sort_order"="ascending";"type"="int64";};];}"#
    );
    reqwest::header::HeaderValue::from_str(&header).expect("valid HTTP header value");
}

#[test]
fn mutation_ids_use_ytsaurus_guid_text_order() {
    assert_eq!(
        yt_guid([
            0x01, 0x02, 0x03, 0x04, 0x11, 0x12, 0x13, 0x14, 0x21, 0x22, 0x23, 0x24, 0x31, 0x32,
            0x33, 0x34,
        ]),
        "34333231-24232221-14131211-4030201"
    );
}

#[test]
fn optimized_bus_crc64_matches_protocol_vectors() {
    assert_eq!(crc64(b""), 0x0000_0000_0000_0000);
    assert_eq!(crc64(b"a"), 0x74b4_2565_ce62_32d5);
    assert_eq!(crc64(b"123456789"), 0xb3f7_fb23_2cb9_9be2);
    assert_eq!(crc64(b"YTsaurus native RPC"), 0x22de_79d3_2a71_78ff);
    assert_eq!(
        crc64(&(0_u8..=u8::MAX).collect::<Vec<_>>()),
        0x1102_45d7_f122_49d9,
    );
    let bytes = (0_u16..=4096)
        .map(|value| (value.wrapping_mul(131) ^ (value >> 3)) as u8)
        .collect::<Vec<_>>();
    for length in 0..=bytes.len() {
        assert_eq!(
            crc64(&bytes[..length]),
            reference_yt_crc64(&bytes[..length])
        );
    }
}

#[test]
fn bus_null_checksum_skips_verification() {
    let bytes = b"YTsaurus unchecked Bus attachment";
    assert!(checksum_matches(0, bytes));
    assert!(checksum_matches(crc64(bytes), bytes));
    assert!(!checksum_matches(crc64(b"different attachment"), bytes));
}

fn reference_yt_crc64(bytes: &[u8]) -> u64 {
    const POLYNOMIAL: u64 = 0xe543_2797_6592_7881;
    let mut remainder = 0_u64;
    for byte in bytes {
        remainder ^= u64::from(*byte) << 56;
        for _ in 0..8 {
            remainder = (remainder << 1)
                ^ if remainder & (1 << 63) == 0 {
                    0
                } else {
                    POLYNOMIAL
                };
        }
    }
    remainder
}

#[tokio::test]
async fn partition_worker_failure_cannot_be_mistaken_for_clean_eof() {
    let (_sender, mut receiver) = mpsc::channel::<anyhow::Result<()>>(1);
    let mut tasks = JoinSet::new();
    tasks.spawn(async { panic!("simulated worker failure") });

    let error = tokio::time::timeout(
        Duration::from_secs(1),
        receive_read_worker_item(&mut receiver, &mut tasks, "test reader"),
    )
    .await
    .expect("worker failure must be observed without waiting for channel closure")
    .expect_err("a panicked worker must fail the source");

    assert!(error.to_string().contains("test reader worker failed"));
}

#[test]
fn auth_uses_the_wide_credentials_control() {
    let schema = serde_json::to_value(schemars::schema_for!(YTsaurusSourceConfig))
        .expect("YTsaurus source schema must serialize");
    assert_eq!(
        schema
            .pointer("/properties/auth/x-ui/control_width")
            .and_then(serde_json::Value::as_str),
        Some("auth"),
    );
}

#[test]
fn source_read_ordering_is_an_advanced_ordered_by_default_choice() {
    let schema = serde_json::to_value(schemars::schema_for!(YTsaurusSourceConfig))
        .expect("YTsaurus source schema must serialize");
    let ordering = &schema["properties"]["read_ordering"];
    assert_eq!(ordering["x-ui"]["section"], "advanced");
    assert_eq!(ordering["$ref"], "#/$defs/YTsaurusReadOrdering");

    let properties = schema["properties"]
        .as_object()
        .expect("YTsaurus source properties must be an object");
    let advanced = properties
        .iter()
        .filter_map(|(name, property)| {
            (property
                .pointer("/x-ui/section")
                .and_then(serde_json::Value::as_str)
                == Some("advanced"))
            .then_some(name.as_str())
        })
        .collect::<Vec<_>>();
    assert_eq!(advanced, ["proxy_role", "read_ordering"]);
    for name in ["trusted_native_rpc_plaintext", "table_reader"] {
        assert_eq!(
            properties[name]
                .pointer("/x-ui/widget")
                .and_then(serde_json::Value::as_str),
            Some("hidden"),
            "{name} must remain configurable through YAML but hidden from the source form",
        );
    }
    assert!(!properties.contains_key("native_rpc_service_ticket_file"));

    let partition = schema
        .pointer("/$defs/YTsaurusReadOrdering/oneOf/2/properties")
        .and_then(serde_json::Value::as_object)
        .expect("PartitionTables schema must expose its configured properties");
    for name in [
        "compressed_data_size_per_partition",
        "max_partition_count",
        "concurrency",
    ] {
        assert_eq!(
            partition[name]
                .pointer("/x-ui/widget")
                .and_then(serde_json::Value::as_str),
            Some("hidden"),
            "{name} must not expand beneath the read-mode selector",
        );
    }
    assert!(!partition.contains_key("direct_data_node_access"));
    assert!(!partition.contains_key("direct_blocks_per_request"));
}

#[test]
fn rpc_proxy_role_is_forwarded_to_discovery_without_changing_it() -> anyhow::Result<()> {
    let endpoint = reqwest::Url::parse("https://hume.example.net:443/root?old=value")?;
    let url = rpc_proxy_discovery_url(&endpoint, Some("dedicated-transferia"));
    assert_eq!(url.path(), "/api/v4/discover_proxies");
    assert_eq!(url.query(), Some("type=rpc&role=dedicated-transferia"));

    let without_role = rpc_proxy_discovery_url(&endpoint, None);
    assert_eq!(without_role.query(), Some("type=rpc"));
    Ok(())
}

#[test]
fn rpc_proxy_role_options_are_sorted_deduplicated_and_never_empty() {
    assert_eq!(
        normalize_rpc_proxy_roles(vec![
            "shared".to_owned(),
            String::new(),
            "dedicated".to_owned(),
            "shared".to_owned(),
        ]),
        ["dedicated", "shared"]
    );
}

#[test]
fn proxy_role_is_advanced_in_source_and_both_sink_modes() -> anyhow::Result<()> {
    let source = serde_json::to_value(schema_for!(YTsaurusSourceConfig))?;
    assert_eq!(
        source["properties"]["proxy_role"]["x-ui"]["section"],
        "advanced"
    );

    let sink = serde_json::to_value(schema_for!(YTsaurusSinkConfig))?;
    let branches = sink["$defs"]["YTsaurusTableMode"]["oneOf"]
        .as_array()
        .expect("table modes");
    assert_eq!(branches.len(), 2);
    for branch in branches {
        assert_eq!(
            branch["properties"]["proxy_role"]["x-ui"]["section"],
            "advanced"
        );
    }
    Ok(())
}

#[test]
fn proxy_role_rejects_surrounding_whitespace_in_source_and_sink() -> anyhow::Result<()> {
    let source = serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\ntrusted_native_rpc_plaintext: true\ntables: [{ path: //tmp/input }]\nproxy_role: ' dedicated '\n",
    )?;
    assert!(source.validate().is_err());

    let sink = serde_yaml::from_str::<YTsaurusSinkConfig>(
        "tables: { type: static_tables, replace_tables: false, path: //tmp/output, proxy_role: ' dedicated ' }\nauth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\n",
    )?;
    assert!(sink.validate().is_err());
    Ok(())
}

#[test]
fn only_dynamic_sink_without_a_proxy_role_recommends_dedicated_proxies() -> anyhow::Result<()> {
    let dynamic = serde_yaml::from_str::<YTsaurusSinkConfig>(
        "tables: { type: dynamic_tables, replace_tables: true, path: //tmp/output }\nauth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\ntrusted_native_rpc_plaintext: true\n",
    )?;
    let connector = YTsaurusSinkConnector::from_config(dynamic)?;
    assert_eq!(connector.performance_advice().len(), 1);
    assert_eq!(
        connector.performance_advice()[0].code,
        "YT_SHARED_RPC_PROXIES"
    );

    let dedicated = serde_yaml::from_str::<YTsaurusSinkConfig>(
        "tables: { type: dynamic_tables, replace_tables: true, path: //tmp/output, proxy_role: dedicated }\nauth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\ntrusted_native_rpc_plaintext: true\n",
    )?;
    assert!(YTsaurusSinkConnector::from_config(dedicated)?
        .performance_advice()
        .is_empty());

    let static_tables = serde_yaml::from_str::<YTsaurusSinkConfig>(
        "tables: { type: static_tables, replace_tables: true, path: //tmp/output }\nauth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\n",
    )?;
    assert!(YTsaurusSinkConnector::from_config(static_tables)?
        .performance_advice()
        .is_empty());
    Ok(())
}

#[test]
fn destination_delivery_modes_are_independent_of_append_only_semantics() -> anyhow::Result<()> {
    use transferia_delivery_contracts::DeliveryType;
    let schema = serde_json::to_value(schema_for!(YTsaurusSinkConfig))?;
    let variants = schema
        .pointer("/$defs/YTsaurusTableMode/oneOf")
        .unwrap()
        .as_array()
        .unwrap();
    for table_type in ["static_tables", "dynamic_tables"] {
        let variant = variants
            .iter()
            .find(|variant| {
                variant
                    .pointer("/properties/type/const")
                    .and_then(serde_json::Value::as_str)
                    == Some(table_type)
            })
            .unwrap();
        let config = serde_yaml::from_str::<YTsaurusSinkConfig>(&format!(
            "tables: {{ type: {table_type}, replace_tables: true, path: //tmp/output }}\nauth: {{ type: token, token: test }}\nhost: localhost\nport: 8000\ntrusted_plaintext: true\ntrusted_native_rpc_plaintext: true\n",
        ))?;
        let connector = YTsaurusSinkConnector::from_config(config)?;
        assert!(connector.compatibility().accepts_record_semantics(
            transferia_delivery_contracts::semantics::RecordSemantics::AppendOnly,
        ));
        for mode in [
            DeliveryType::Batch,
            DeliveryType::Stream,
            DeliveryType::BatchAndStream,
        ] {
            let supported = table_type == "dynamic_tables" || mode == DeliveryType::Batch;
            let declared =
                variant
                    .pointer("/x-ui/capabilities/delivery_modes")
                    .is_none_or(|modes| {
                        modes
                            .as_array()
                            .unwrap()
                            .contains(&serde_json::to_value(mode).unwrap())
                    });
            assert_eq!(
                declared, supported,
                "schema and runtime must agree for {table_type}/{mode:?}"
            );
            assert_eq!(connector.delivery_modes().contains(&mode), supported);
            assert_eq!(connector.validate_delivery_type(mode).is_ok(), supported);
        }
    }
    Ok(())
}

#[test]
fn source_table_names_are_derived_from_unique_path_basenames() -> anyhow::Result<()> {
    let source = serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\ntrusted_native_rpc_plaintext: true\ntables:\n  - path: //tmp/input\n",
    )?;
    source.validate()?;
    assert_eq!(source.tables[0].dataset_name()?, "input");
    let tls_source = serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: false\ntables:\n  - path: //tmp/input\n"
    )?;
    tls_source.connection.validate()?;
    assert_eq!(tls_source.connection.endpoint(), "https://localhost:8000");
    assert!(serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\ntrusted_native_rpc_plaintext: true\ntables:\n  - path: //tmp/input\n"
    )?.validate().is_ok());
    assert!(serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\ntrusted_native_rpc_plaintext: true\ntables:\n  - { path: //tmp/a/events }\n  - { path: //tmp/b/events }\n"
    )?
    .validate()
    .is_err());
    assert!(serde_yaml::from_str::<YTsaurusSinkConfig>(
        "tables: { type: static_tables, replace_tables: false, path: relative }\nauth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\n"
    )?
    .validate()
    .is_err());
    Ok(())
}

#[test]
fn table_path_suggestions_include_only_directories_and_tables() -> anyhow::Result<()> {
    assert_eq!(suggestion_directory("")?, "/");
    assert_eq!(suggestion_directory("//")?, "/");
    assert_eq!(suggestion_directory("//home/logs/")?, "//home/logs");
    assert!(suggestion_directory("relative/path").is_err());

    let nodes = serde_json::from_value::<Vec<ListedNode>>(serde_json::json!([
        { "$value": "nested", "$attributes": { "type": "map_node" } },
        { "$value": "events", "$attributes": { "type": "table" } },
        { "$value": "link", "$attributes": { "type": "link" } }
    ]))?;
    assert_eq!(
        table_path_suggestions("//home/logs", nodes),
        vec!["//home/logs/events", "//home/logs/nested/"]
    );

    let root_nodes = serde_json::from_value::<Vec<ListedNode>>(serde_json::json!([
        { "$value": "home", "$attributes": { "type": "map_node" } },
        { "$value": "events", "$attributes": { "type": "table" } }
    ]))?;
    assert_eq!(
        table_path_suggestions("/", root_nodes),
        vec!["//events", "//home/"]
    );
    assert_eq!(
        resolved_link_suggestion("//logs".to_owned(), "map_node"),
        Some("//logs/".to_owned())
    );
    assert_eq!(
        resolved_link_suggestion("//latest".to_owned(), "table"),
        Some("//latest".to_owned())
    );
    Ok(())
}

#[test]
fn snapshot_recovery_materializes_an_exact_row_range() {
    assert_eq!(rich_read_path("//tmp/input", 0), "//tmp/input");
    assert_eq!(
        rich_read_path("//tmp/input", 42_971_400),
        "<ranges=[{lower_limit={row_index=42971400}}]>//tmp/input"
    );
}

#[test]
fn arrow_is_the_default_sink_format() -> anyhow::Result<()> {
    let mut config = serde_yaml::from_str::<YTsaurusSinkConfig>(
        "tables: { type: static_tables, replace_tables: false, path: //tmp/output }\nauth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\n",
    )?;
    assert_eq!(
        config.static_optimize_for(),
        Some(YTsaurusOptimizeFor::Scan)
    );
    assert_eq!(
        config.primary_key_semantics(),
        YTsaurusPrimaryKeySemantics::UniqueSorted
    );
    assert_eq!(config.path_for_dataset("events")?, "//tmp/output/events");
    assert_eq!(config.write_target_bytes, 512 * 1024 * 1024);
    assert_eq!(config.write_concurrency, 4);
    assert_eq!(config.write_flush_interval_ms, 1_000);
    assert_eq!(config.write_row_buffer_bytes, 512 * 1024);
    assert_eq!(config.table_writer.block_size, 16 * 1024 * 1024);
    assert_eq!(config.table_writer.max_buffer_size, 16 * 1024 * 1024);
    assert_eq!(config.table_writer.writer_window_size, 64 * 1024 * 1024);
    assert_eq!(config.table_writer.writer_group_size, 16 * 1024 * 1024);
    assert_eq!(
        config.table_writer.desired_chunk_size,
        2 * 1024 * 1024 * 1024
    );
    assert_eq!(config.primary_key_sort_timeout_ms, 24 * 60 * 60 * 1_000);
    let schema = serde_json::to_value(schema_for!(YTsaurusSinkConfig))?.to_string();
    assert!(!schema.contains("Driver exchange format"));
    assert!(!schema.contains("YTsaurusWriteFormat"));
    config.validate()?;
    config.write_concurrency = 0;
    assert!(config.validate().is_err());
    Ok(())
}

#[test]
fn static_table_layout_is_explicit_and_defaults_to_columnar_scan() -> anyhow::Result<()> {
    let scan = static_table_attributes(
        &serde_json::json!([{ "name": "id", "type": "int64" }]),
        YTsaurusOptimizeFor::Scan,
        "default",
        &BTreeMap::new(),
    )?;
    assert_eq!(scan["optimize_for"], "scan");
    assert_eq!(scan["chunk_format"], "table_unversioned_columnar");
    assert_eq!(scan["primary_medium"], "default");

    let config = serde_yaml::from_str::<YTsaurusSinkConfig>(
        "tables: { type: static_tables, replace_tables: false, path: //tmp/output, optimize_for: lookup }\nauth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\n",
    )?;
    assert_eq!(
        config.static_optimize_for(),
        Some(YTsaurusOptimizeFor::Lookup)
    );
    assert_eq!(config.primary_medium(), "default");
    config.validate()?;
    let lookup = static_table_attributes(
        &serde_json::json!([{ "name": "id", "type": "int64" }]),
        YTsaurusOptimizeFor::Lookup,
        "ssd_blobs",
        &BTreeMap::new(),
    )?;
    assert_eq!(lookup["optimize_for"], "lookup");
    assert_eq!(
        lookup["chunk_format"],
        "table_unversioned_schemaless_horizontal"
    );
    assert_eq!(lookup["primary_medium"], "ssd_blobs");

    let schema = serde_json::to_value(schemars::schema_for!(YTsaurusSinkConfig))?;
    assert_eq!(
        schema["$defs"]["YTsaurusTableMode"]["oneOf"][0]["properties"]["optimize_for"]["default"],
        "scan"
    );
    let serialized = serde_json::to_string(&schema)?;
    assert!(serialized.contains("Optimize for"));
    assert!(serialized.contains("Primary medium"));
    assert!(serialized.contains("advanced"));
    let invalid: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: static_tables, replace_tables: false, path: //tmp/output, primary_medium: '' }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n",
    )?;
    assert!(invalid.validate().is_err());
    Ok(())
}

#[test]
fn custom_table_attributes_are_typed_and_cannot_override_structural_settings() -> anyhow::Result<()>
{
    let schema = serde_json::to_value(schema_for!(YTsaurusSinkConfig))?;
    let encoded = schema.to_string();
    assert!(encoded.contains("\"title\":\"YSON value\""));
    assert!(!encoded.contains("\"title\":\"JSON value\""));

    let config: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: static_tables, replace_tables: false, path: //tmp/output, table_attributes: [{ name: compression_codec, value: '\"zstd_3\"' }, { name: custom_nested, value: '{enabled=%true;levels=[1;2;];}' }, { name: custom_unsigned, value: '18446744073709551615u' }, { name: attributed, value: '<source=\"explicit\";>42' }] }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n",
    )?;
    let custom = config.parsed_table_attributes()?;
    let attributes = static_table_attributes(
        &serde_json::json!([{ "name": "id", "type": "int64" }]),
        YTsaurusOptimizeFor::Scan,
        config.primary_medium(),
        &custom,
    )?;
    assert_eq!(attributes["compression_codec"], "zstd_3");
    assert_eq!(attributes["custom_nested"]["enabled"], true);
    assert_eq!(
        attributes["custom_nested"]["levels"],
        serde_json::json!([1, 2])
    );
    assert_eq!(
        yson_header_value(&attributes["custom_unsigned"])?,
        "18446744073709551615u"
    );
    assert_eq!(
        yson_header_value(&attributes["attributed"])?,
        "<\"source\"=\"explicit\";>42"
    );

    let reserved: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: static_tables, replace_tables: false, path: //tmp/output, table_attributes: [{ name: schema, value: '{}' }] }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n",
    )?;
    assert!(reserved.validate().is_err());

    let transfer_id_override: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: static_tables, replace_tables: false, path: //tmp/output, table_attributes: [{ name: _transfer_id, value: '\"spoofed\"' }] }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n",
    )?;
    assert!(transfer_id_override.validate().is_err());
    Ok(())
}

#[test]
fn created_table_attributes_include_the_authoritative_transfer_id() -> anyhow::Result<()> {
    let config: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: static_tables, replace_tables: true, path: //tmp/output, table_attributes: [{ name: owner, value: '\"team\"' }] }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n",
    )?;
    let connector = YTsaurusSinkConnector::from_config(config)?;
    let attributes = connector.table_attributes_for_transfer("dttabcdefghijklmnopq");
    assert_eq!(attributes["_transfer_id"], "dttabcdefghijklmnopq");
    assert_eq!(attributes["owner"], "team");
    Ok(())
}

#[test]
fn static_writer_spec_overrides_defaults_with_typed_values() -> anyhow::Result<()> {
    let config: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: static_tables, replace_tables: false, path: //tmp/output, spec: [{ name: block_size, value: '8388608' }, { name: validate_sorted, value: '%true' }] }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n",
    )?;
    let custom = config.parsed_writer_spec()?;
    let spec = table_writer_spec(&config.table_writer, &custom)?;
    assert_eq!(spec["block_size"], 8 * 1024 * 1024);
    assert_eq!(spec["validate_sorted"], true);
    assert_eq!(
        spec["desired_chunk_size"],
        config.table_writer.desired_chunk_size
    );

    let schema = serde_json::to_value(schemars::schema_for!(YTsaurusSinkConfig))?;
    assert!(serde_json::to_string(&schema)?.contains("YT Spec"));
    Ok(())
}

#[test]
fn unique_sorted_schema_preserves_primary_key_order_and_rejects_nullable_keys() -> anyhow::Result<()>
{
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("payload".into(), DataType::Binary, true),
        SchemaColumn::new("topic".into(), DataType::Utf8, false)
            .with_constraints(true, false, None),
        SchemaColumn::new("offset".into(), DataType::Int64, false)
            .with_constraints(true, false, None),
    ]);
    assert_eq!(
        sorted_unique_schema_to_yt(&schema)?,
        serde_json::json!({
            "$attributes": { "strict": true, "unique_keys": true },
            "$value": [
                { "name": "topic", "type": "utf8", "required": true, "sort_order": "ascending" },
                { "name": "offset", "type": "int64", "required": true, "sort_order": "ascending" },
                { "name": "payload", "type": "string", "required": false },
            ],
        })
    );

    let nullable =
        DatasetSchema::new(vec![SchemaColumn::new("id".into(), DataType::Int64, true)
            .with_constraints(true, false, None)]);
    assert!(sorted_unique_schema_to_yt(&nullable).is_err());
    Ok(())
}

#[test]
fn unique_sorted_snapshots_reject_multiple_source_partitions() -> anyhow::Result<()> {
    let config: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: static_tables, replace_tables: true, path: //tmp/output }\nauth: { type: token, token: test }\nhost: localhost\nport: 8000\ntrusted_plaintext: true\n",
    )?;
    let schema =
        DatasetSchema::new(vec![SchemaColumn::new("id".into(), DataType::Int64, false)
            .with_constraints(true, false, None)]);
    let mut discovery = DeliveryDiscovery {
        source_name: Arc::from("test"),
        source_topology: SourceTopology::StaticPartitions(vec![0, 1]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
            update_policy: transferia_core::delivery::UpdatePolicy::Strict,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema: schema.clone(),
            stored_schema: schema,
            system_columns: Vec::new(),
        }],
        performance_advice: Vec::new(),
    };

    assert!(config.validate_discovery(&discovery).is_err());
    discovery.source_topology = SourceTopology::StaticPartitions(vec![0]);
    config.validate_discovery(&discovery)?;
    discovery.source_topology = SourceTopology::CoLocatedStaticPartitions(vec![0, 1]);
    assert!(config.validate_discovery(&discovery).is_err());
    discovery.source_topology = SourceTopology::CoLocatedStaticPartitions(vec![0]);
    config.validate_discovery(&discovery)?;
    Ok(())
}

#[test]
fn schema_round_trip_and_arrow_writer_are_native() -> anyhow::Result<()> {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".into(), DataType::Int64, false),
        SchemaColumn::new("name".into(), DataType::Utf8, true),
    ]);
    let encoded = schema_to_yt(&schema)?;
    let response = serde_json::json!({
        "$attributes": { "strict": true },
        "$value": encoded
    });
    let parsed = parse_schema(response)?;
    assert_eq!(parsed.columns.len(), 2);
    assert_eq!(parsed.columns[0].data_type, DataType::Int64);
    assert_eq!(parsed.columns[1].data_type, DataType::Utf8);

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("alice"), None])) as ArrayRef,
        ],
    )?;
    validate_row_weight(&batch, true)?;
    assert!(!encode_arrow(&batch)?.is_empty());
    let payload = encode_arrow_batches(&[batch.clone(), batch])?;
    let decoded =
        StreamReader::try_new(Cursor::new(payload), None)?.collect::<Result<Vec<_>, _>>()?;
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    Ok(())
}

#[test]
fn timestamp_seconds_are_losslessly_widened_for_ytsaurus_storage() -> anyhow::Result<()> {
    let logical = DatasetSchema::new(vec![SchemaColumn::new(
        "event_time".into(),
        DataType::Timestamp(TimeUnit::Second, None),
        true,
    )]);
    let encoded_schema = schema_to_yt(&logical)?;
    let physical = parse_schema(serde_json::json!({
        "$attributes": { "strict": true },
        "$value": encoded_schema,
    }))?;
    assert_eq!(
        physical.columns[0].data_type,
        DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert!(schemas_equal(&physical, &logical));

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )])),
        vec![Arc::new(TimestampSecondArray::from(vec![
            Some(1),
            Some(4_291_747_199),
            None,
        ]))],
    )?;
    let payload = encode_arrow(&batch)?;
    let mut reader = StreamReader::try_new(Cursor::new(payload), None)?;
    let decoded = reader.next().expect("one Arrow batch")?;
    let values = decoded
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("physical microsecond timestamp");
    assert_eq!(values.value(0), 1_000_000);
    assert_eq!(values.value(1), 4_291_747_199_000_000);
    assert!(values.is_null(2));

    let encoded = encode_wire_batch(&batch)?;
    let decoded = YtWireDecoder::new(&physical).decode(&encoded.column_names, encoded.payload)?;
    let values = decoded
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("physical microsecond timestamp");
    assert_eq!(values.value(0), 1_000_000);
    assert_eq!(values.value(1), 4_291_747_199_000_000);
    assert!(values.is_null(2));
    Ok(())
}

#[test]
fn ytsaurus_temporal_validation_fails_before_encoding_side_effects() -> anyhow::Result<()> {
    let invalid_timezone = DatasetSchema::new(vec![SchemaColumn::new(
        "event_time".into(),
        DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
        false,
    )]);
    assert!(schema_to_yt(&invalid_timezone).is_err());

    for value in [-1, 4_291_747_200, i64::MAX] {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            )])),
            vec![Arc::new(TimestampSecondArray::from(vec![value]))],
        )?;
        assert!(validate_temporal_values(&batch).is_err());
        assert!(encode_arrow(&batch).is_err());
        assert!(encode_wire_batch(&batch).is_err());
    }
    for days in [-1, 49_673] {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "event_date",
                DataType::Date32,
                false,
            )])),
            vec![Arc::new(Date32Array::from(vec![days]))],
        )?;
        assert!(validate_temporal_values(&batch).is_err());
        assert!(encode_wire_batch(&batch).is_err());
    }
    Ok(())
}

#[test]
fn dynamic_wire_encoder_round_trips_values_and_explicit_nulls() -> anyhow::Result<()> {
    let dataset_schema = DatasetSchema::new(vec![
        SchemaColumn::new("id".into(), DataType::Int64, false).with_constraints(true, false, None),
        SchemaColumn::new("name".into(), DataType::Utf8, true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![-1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("alice"), None])) as ArrayRef,
        ],
    )?;

    let encoded = encode_wire_batch(&batch)?;
    let decoded =
        YtWireDecoder::new(&dataset_schema).decode(&encoded.column_names, encoded.payload)?;

    assert_eq!(decoded.column(0), batch.column(0));
    assert_eq!(decoded.column(1), batch.column(1));
    Ok(())
}

#[test]
fn dynamic_sink_defaults_to_lossless_bounded_tablet_transactions() -> anyhow::Result<()> {
    let config: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: dynamic_tables, replace_tables: false, path: //tmp/output }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n\
         trusted_native_rpc_plaintext: true\n",
    )?;
    let write = config.dynamic_write().expect("dynamic writer config");
    assert_eq!(config.dynamic_atomicity(), Some(YTsaurusAtomicity::Full));
    assert_eq!(config.initial_tablet_count(), Some(1));
    assert_eq!(config.dynamic_table_ttl_ms(), None);
    assert_eq!(config.big_value_policy(), YTsaurusBigValuePolicy::Fail);
    assert!(config.stages_dynamic_snapshots());
    assert_eq!(config.dynamic_snapshot_operation_pool(), None);
    assert_eq!(config.primary_medium(), "default");
    assert_eq!(write.transaction_rows, 50_000);
    assert_eq!(write.transaction_concurrency, 8);
    assert_eq!(write.transaction_timeout_ms, 60_000);
    assert_eq!(write.buffer_bytes, 256 * 1024 * 1024);
    assert!((write.dynamic_store_overflow_threshold - 0.5).abs() < f64::EPSILON);
    assert!(write.require_sync_replica);
    assert_eq!(write.retry_initial_ms, 100);
    assert_eq!(write.retry_max_ms, 5_000);
    config.validate()?;
    Ok(())
}

#[test]
fn dynamic_snapshot_mode_schema_materializes_static_staging_for_batch_deliveries() {
    let schema = serde_json::to_value(schema_for!(YTsaurusSinkConfig)).expect("serialize schema");
    let encoded = schema.to_string();
    assert!(encoded.contains("\"snapshot_mode\""));
    assert!(encoded.contains("\"default\":{\"type\":\"static_staging\",\"operation_pool\":null}"));
    assert!(encoded.contains("\"delivery_types\":[\"batch\"]"));
}

#[test]
fn dynamic_writer_tuning_is_not_exposed_as_an_unlabelled_ui_group() {
    let schema = serde_json::to_value(schema_for!(YTsaurusSinkConfig)).expect("serialize schema");
    let encoded = schema.to_string();
    assert!(encoded.contains(concat!(
        "\"write\":{\"$ref\":\"#/$defs/YTsaurusDynamicWriteConfig\",",
        "\"x-ui\":{\"widget\":\"hidden\"}}"
    )));
}

#[test]
fn sink_has_no_root_level_advanced_settings() {
    let schema = serde_json::to_value(schema_for!(YTsaurusSinkConfig)).expect("serialize schema");
    let properties = schema
        .pointer("/properties")
        .and_then(serde_json::Value::as_object)
        .expect("sink properties");
    for name in [
        "primary_key_semantics",
        "primary_medium",
        "table_attributes",
        "big_value_policy",
    ] {
        assert!(!properties.contains_key(name), "{name}");
    }
}

#[test]
fn common_table_settings_live_in_each_table_mode_advanced_section() {
    let schema = serde_json::to_value(schema_for!(YTsaurusSinkConfig)).expect("serialize schema");
    let branches = schema
        .pointer("/$defs/YTsaurusTableMode/oneOf")
        .and_then(serde_json::Value::as_array)
        .expect("table mode branches");
    for branch in branches {
        for name in [
            "primary_key_semantics",
            "primary_medium",
            "table_attributes",
            "big_value_policy",
        ] {
            assert_eq!(
                branch["properties"][name]["x-ui"]["section"], "advanced",
                "{name}"
            );
        }
    }
}

#[test]
fn table_mode_details_do_not_request_nested_ui_indentation() {
    let schema = serde_json::to_value(schema_for!(YTsaurusSinkConfig)).expect("serialize schema");
    assert_eq!(
        schema["properties"]["tables"]["x-ui"]["indent_variant_details"],
        false
    );
}

#[test]
fn dynamic_snapshot_staging_is_lossless_and_uses_the_configured_operation_pool(
) -> anyhow::Result<()> {
    let config: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: dynamic_tables, replace_tables: true, path: //tmp/output, snapshot_mode: { type: static_staging, operation_pool: transferia-bulk } }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n\
         trusted_native_rpc_plaintext: true\n",
    )?;
    assert!(config.stages_dynamic_snapshots());
    assert_eq!(
        config.dynamic_snapshot_operation_pool(),
        Some("transferia-bulk")
    );
    config.validate()?;

    let attributes = dynamic_conversion_attributes(
        YTsaurusAtomicity::Full,
        "default",
        &BTreeMap::new(),
        Some("cdc"),
        0.5,
        None,
    )?;
    assert_eq!(attributes["atomicity"], "full");
    assert_eq!(attributes["optimize_for"], "lookup");
    assert_eq!(attributes["primary_medium"], "default");
    assert_eq!(attributes["tablet_cell_bundle"], "cdc");
    assert!(attributes.get("schema").is_none());
    assert!(attributes.get("dynamic").is_none());

    let parameters = sort_operation_parameters(
        "//tmp/staging",
        "//tmp/sorted",
        &[serde_json::json!({ "name": "id", "sort_order": "ascending" })],
        "mutation-id",
        config.dynamic_snapshot_operation_pool(),
    );
    assert_eq!(parameters["spec"]["pool"], "transferia-bulk");
    assert_eq!(parameters["spec"]["schema_inference_mode"], "from_output");
    assert_eq!(parameters["spec"]["max_failed_job_count"], 1);
    Ok(())
}

#[test]
fn dynamic_snapshot_staging_requires_explicit_replacement_and_one_partition() -> anyhow::Result<()>
{
    let schema =
        DatasetSchema::new(vec![SchemaColumn::new("id".into(), DataType::Int64, false)
            .with_constraints(true, false, None)]);
    let discovery = |partitions| DeliveryDiscovery {
        source_name: Arc::from("test"),
        source_topology: SourceTopology::StaticPartitions(partitions),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
            update_policy: transferia_core::delivery::UpdatePolicy::Strict,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema: schema.clone(),
            stored_schema: schema.clone(),
            system_columns: Vec::new(),
        }],
        performance_advice: Vec::new(),
    };
    let config = |replace_tables| {
        serde_yaml::from_str::<YTsaurusSinkConfig>(&format!(
            "tables: {{ type: dynamic_tables, replace_tables: {replace_tables}, path: //tmp/output }}\n\
             auth: {{ type: token, token: test }}\n\
             host: localhost\n\
             port: 8000\n\
             trusted_plaintext: true\n\
             trusted_native_rpc_plaintext: true\n"
        ))
    };

    assert!(config(false)?
        .validate_discovery(&discovery(vec![0]))
        .is_err());
    assert!(config(true)?
        .validate_discovery(&discovery(vec![0, 1]))
        .is_err());
    config(true)?.validate_discovery(&discovery(vec![0]))?;

    let direct: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: dynamic_tables, replace_tables: false, path: //tmp/output, snapshot_mode: { type: direct } }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n\
         trusted_native_rpc_plaintext: true\n",
    )?;
    assert!(!direct.stages_dynamic_snapshots());
    direct.validate_discovery(&discovery(vec![0, 1]))?;
    Ok(())
}

#[test]
fn dynamic_initial_tablet_count_is_uniform_and_requires_an_integral_first_key() -> anyhow::Result<()>
{
    let config: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: dynamic_tables, replace_tables: false, path: //tmp/output, initial_tablet_count: 8 }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n\
         trusted_native_rpc_plaintext: true\n",
    )?;
    assert_eq!(config.initial_tablet_count(), Some(8));
    config.validate()?;
    assert_eq!(
        uniform_reshard_parameters("//tmp/output/events", 8)?,
        serde_json::json!({
            "path": "//tmp/output/events",
            "tablet_count": 8,
            "uniform": true,
        })
    );

    let integral = DatasetSchema::new(vec![
        SchemaColumn::new("id".into(), DataType::Int64, false).with_constraints(true, false, None),
        SchemaColumn::new("payload".into(), DataType::Utf8, true),
    ]);
    validate_initial_tablet_count(8, &integral, "events")?;

    let string = DatasetSchema::new(vec![
        SchemaColumn::new("id".into(), DataType::Utf8, false).with_constraints(true, false, None),
        SchemaColumn::new("payload".into(), DataType::Utf8, true),
    ]);
    let error = validate_initial_tablet_count(8, &string, "events")
        .expect_err("uniform pivots for a string key must fail before table creation");
    assert!(error.to_string().contains("integral first primary-key"));
    validate_initial_tablet_count(1, &string, "events")?;

    assert!(uniform_reshard_parameters("", 8).is_err());
    assert!(uniform_reshard_parameters("//tmp/output/events", 0).is_err());
    assert!(uniform_reshard_parameters("//tmp/output/events", 10_001).is_err());
    Ok(())
}

#[test]
fn dynamic_initial_tablet_count_rejects_out_of_range_configuration() {
    for tablet_count in [0, 10_001] {
        let config: YTsaurusSinkConfig = serde_yaml::from_str(&format!(
            "tables: {{ type: dynamic_tables, replace_tables: false, path: //tmp/output, initial_tablet_count: {tablet_count} }}\n\
             auth: {{ type: token, token: test }}\n\
             host: localhost\n\
             port: 8000\n\
             trusted_plaintext: true\n\
             trusted_native_rpc_plaintext: true\n"
        ))
        .expect("configuration should deserialize before semantic validation");
        let error = config
            .validate()
            .expect_err("out-of-range tablet count must fail");
        assert!(error.to_string().contains("between 1 and 10000"));
    }
}

#[test]
fn dynamic_table_atomicity_is_explicit_in_schema_creation_and_transactions() -> anyhow::Result<()> {
    let config: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: dynamic_tables, replace_tables: false, path: //tmp/output, atomicity: none }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n\
         trusted_native_rpc_plaintext: true\n",
    )?;
    assert_eq!(config.dynamic_atomicity(), Some(YTsaurusAtomicity::None));
    assert_eq!(YTsaurusAtomicity::None.rpc_value(), 1);
    assert_eq!(YTsaurusAtomicity::Full.rpc_value(), 0);

    let attributes = dynamic_table_attributes(
        &serde_json::json!([{ "name": "id", "type": "int64" }]),
        config.dynamic_atomicity().expect("dynamic atomicity"),
        config.primary_medium(),
        &BTreeMap::new(),
        Some("cdc"),
        0.5,
        None,
    )?;
    assert_eq!(attributes["atomicity"], "none");
    assert_eq!(attributes["primary_medium"], "default");
    assert_eq!(attributes["tablet_cell_bundle"], "cdc");

    let schema = serde_json::to_value(schemars::schema_for!(YTsaurusSinkConfig))?;
    let serialized = serde_json::to_string(&schema)?;
    assert!(serialized.contains("Atomicity"));
    assert!(serialized.contains("advanced"));
    Ok(())
}

#[test]
fn dynamic_table_ttl_is_opt_in_and_applies_to_direct_and_staged_tables() -> anyhow::Result<()> {
    let config: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: dynamic_tables, replace_tables: true, path: //tmp/output, table_ttl_ms: 86400000 }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n\
         trusted_native_rpc_plaintext: true\n",
    )?;
    config.validate()?;
    assert_eq!(config.dynamic_table_ttl_ms(), Some(86_400_000));

    let direct = dynamic_table_attributes(
        &serde_json::json!([{ "name": "id", "type": "int64" }]),
        YTsaurusAtomicity::Full,
        "default",
        &BTreeMap::new(),
        Some("cdc"),
        0.5,
        config.dynamic_table_ttl_ms(),
    )?;
    let staged = dynamic_conversion_attributes(
        YTsaurusAtomicity::Full,
        "default",
        &BTreeMap::new(),
        Some("cdc"),
        0.5,
        config.dynamic_table_ttl_ms(),
    )?;
    for attributes in [&direct, &staged] {
        assert_eq!(attributes["min_data_versions"], 0);
        assert_eq!(attributes["max_data_versions"], 1);
        assert_eq!(attributes["min_data_ttl"], 0);
        assert_eq!(attributes["max_data_ttl"], 86_400_000);
        assert_eq!(attributes["auto_compaction_period"], 86_400_000);
        assert_eq!(attributes["merge_rows_on_flush"], true);
    }

    let disabled: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: dynamic_tables, replace_tables: false, path: //tmp/output }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n\
         trusted_native_rpc_plaintext: true\n",
    )?;
    let attributes = dynamic_table_attributes(
        &serde_json::json!([]),
        YTsaurusAtomicity::Full,
        "default",
        &BTreeMap::new(),
        None,
        0.5,
        disabled.dynamic_table_ttl_ms(),
    )?;
    assert!(attributes.get("max_data_ttl").is_none());
    assert!(attributes.get("min_data_versions").is_none());

    let invalid: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: dynamic_tables, replace_tables: false, path: //tmp/output, table_ttl_ms: 0 }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n\
         trusted_native_rpc_plaintext: true\n",
    )?;
    assert!(invalid.validate().is_err());
    Ok(())
}

#[test]
fn oversized_value_policy_fails_closed_or_drops_the_entire_row() -> anyhow::Result<()> {
    let schema = serde_json::to_value(schema_for!(YTsaurusSinkConfig))?;
    let branches = schema["$defs"]["YTsaurusTableMode"]["oneOf"]
        .as_array()
        .expect("table mode branches");
    for branch in branches {
        assert_eq!(branch["properties"]["big_value_policy"]["default"], "fail");
    }

    let oversized = "x".repeat(16 * 1024 * 1024 + 1);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec!["small", oversized.as_str()])) as ArrayRef,
        ],
    )?;

    validate_row_weight(&batch, true)?;
    let error = validate_row_weight(&batch, false)
        .expect_err("dynamic tables must reject values above their 16 MiB limit");
    assert!(error.to_string().contains("16777216-byte table limit"));

    let filtered = drop_oversized_rows(&batch, false)?;
    assert_eq!(filtered.num_rows(), 1);
    assert_eq!(
        filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column")
            .value(0),
        1
    );
    assert_eq!(
        filtered
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("payload column")
            .value(0),
        "small"
    );

    let drop: YTsaurusSinkConfig = serde_yaml::from_str(
        "tables: { type: dynamic_tables, replace_tables: false, path: //tmp/output, big_value_policy: drop }\n\
         auth: { type: token, token: test }\n\
         host: localhost\n\
         port: 8000\n\
         trusted_plaintext: true\n\
         trusted_native_rpc_plaintext: true\n",
    )?;
    assert_eq!(drop.big_value_policy(), YTsaurusBigValuePolicy::Drop);
    drop.validate()?;
    Ok(())
}

#[test]
fn dynamic_sink_retries_yt_backpressure_and_rebalancing_indefinitely() {
    for code in [
        1700, 1701, 1702, 1703, 1704, 1706, 1707, 1712, 1713, 1720, 1721, 1725, 1732, 1735, 1736,
        1740, 1742, 1745, 1746, 1747, 1748,
    ] {
        assert!(
            is_transient_dynamic_write_error_code(code),
            "expected retryable YTsaurus RPC error code: {code}"
        );
    }
    for code in [
        0, 1, 1705, 1714, 1715, 1716, 1717, 1726, 1731, 1738, 1739, 1741,
    ] {
        assert!(
            !is_transient_dynamic_write_error_code(code),
            "expected terminal YTsaurus RPC error code: {code}"
        );
    }
    for message in [
        "cannot mount table since node is locked by mount-unmount operation",
        "node is out of tablet memory",
        "too many overlapping stores in tablet, all writes disabled",
        "active store is overflown, all writes disabled: dynamic store pool size limit reached",
        "tablet a481 is not in \"mounted\" state",
        "No such tablet 8a62",
    ] {
        assert!(
            is_transient_dynamic_write_error(&anyhow::anyhow!(message)),
            "expected retryable YTsaurus response: {message}"
        );
    }
    assert!(!is_transient_dynamic_write_error(&anyhow::anyhow!(
        "authentication failed"
    )));
    assert!(!is_transient_dynamic_write_error(&anyhow::anyhow!(
        "schema does not match"
    )));
}

#[test]
fn sorted_schema_marks_every_key_column_as_primary() -> anyhow::Result<()> {
    let parsed = parse_schema(serde_json::json!({
        "$attributes": { "strict": true, "unique_keys": true },
        "$value": [
            {
                "name": "topic",
                "type": "utf8",
                "required": true,
                "sort_order": "ascending"
            },
            {
                "name": "offset",
                "type": "int64",
                "required": true,
                "sort_order": "descending"
            },
            { "name": "payload", "type": "utf8", "required": true }
        ]
    }))?;

    assert!(parsed.columns[0].primary_key);
    assert!(parsed.columns[1].primary_key);
    assert!(!parsed.columns[2].primary_key);
    Ok(())
}

#[test]
fn non_unique_sort_columns_are_not_primary_keys() -> anyhow::Result<()> {
    let parsed = parse_schema(serde_json::json!({
        "$attributes": { "strict": true, "unique_keys": false },
        "$value": [{
            "name": "group",
            "type": "utf8",
            "required": true,
            "sort_order": "ascending"
        }]
    }))?;

    assert!(!parsed.columns[0].primary_key);
    Ok(())
}

#[test]
fn source_rejects_partial_or_mistyped_system_column_layouts() {
    let partial = DatasetSchema::new(vec![SchemaColumn::new(
        "_system_topic".into(),
        DataType::Utf8,
        false,
    )]);
    assert!(system_column_layout(&partial).is_err());

    let wrong_type = DatasetSchema::new(vec![
        SchemaColumn::new("_system_topic".into(), DataType::Utf8, false),
        SchemaColumn::new("_system_partition".into(), DataType::Int64, false),
        SchemaColumn::new("_system_offset".into(), DataType::Int64, false),
        SchemaColumn::new("_system_message_index".into(), DataType::Int64, false),
    ]);
    assert!(system_column_layout(&wrong_type).is_err());

    let complete = DatasetSchema::new(vec![
        SchemaColumn::new("_system_topic".into(), DataType::Utf8, false),
        SchemaColumn::new("_system_partition".into(), DataType::Int64, false),
        SchemaColumn::new("_system_offset".into(), DataType::Int64, false),
        SchemaColumn::new("_system_message_index".into(), DataType::UInt64, false),
    ]);
    let (present, columns) = system_column_layout(&complete).unwrap();
    assert!(present);
    assert_eq!(columns.iter().count(), 4);
}

#[test]
fn arrow_writer_strips_extension_annotations_from_the_ytsaurus_wire_schema() -> anyhow::Result<()> {
    let field = Field::new("payload", DataType::Utf8, false).with_metadata(HashMap::from([(
        "ARROW:extension:name".to_owned(),
        ARROW_JSON_EXTENSION_NAME.to_owned(),
    )]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![field])),
        vec![Arc::new(StringArray::from(vec!["{}"] as Vec<&str>)) as ArrayRef],
    )?;

    let encoded = encode_arrow(&batch)?;
    let mut reader = StreamReader::try_new(Cursor::new(encoded), None)?;
    let decoded = reader.next().expect("one Arrow batch")?;
    assert_eq!(decoded.column(0), batch.column(0));
    assert_eq!(
        decoded
            .schema()
            .field(0)
            .metadata()
            .get("ARROW:extension:name"),
        None
    );
    Ok(())
}

#[test]
fn unsupported_types_and_invalid_names_fail_during_validation() {
    let schema = DatasetSchema::new(vec![SchemaColumn::new(
        "@internal".into(),
        DataType::Decimal128(20, 2),
        false,
    )]);
    assert!(schema_to_yt(&schema).is_err());
}

#[test]
fn source_rejects_read_type_or_nullability_drift_instead_of_casting() -> anyhow::Result<()> {
    let expected = DatasetSchema::new(vec![SchemaColumn::new("id".into(), DataType::Int64, false)]);
    let expected_arrow = dataset_arrow_schema(&expected);
    let wrong_type = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(vec!["1"])) as ArrayRef],
    )?;
    let nullable = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef],
    )?;

    assert!(normalize_read_batch(wrong_type, &expected, &expected_arrow).is_err());
    assert!(normalize_read_batch(nullable, &expected, &expected_arrow).is_err());
    Ok(())
}

#[test]
fn source_restores_discovered_arrow_metadata_without_copying_arrays() -> anyhow::Result<()> {
    let expected = DatasetSchema::new(vec![SchemaColumn::new(
        "payload".into(),
        DataType::Utf8,
        false,
    )
    .with_arrow_extension(ARROW_JSON_EXTENSION_NAME)]);
    let values: ArrayRef = Arc::new(StringArray::from(vec!["{}"]));
    let input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::clone(&values)],
    )?;

    let normalized = normalize_read_batch(input, &expected, &dataset_arrow_schema(&expected))?;

    assert_eq!(
        normalized
            .schema()
            .field(0)
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some(ARROW_JSON_EXTENSION_NAME),
    );
    assert!(Arc::ptr_eq(normalized.column(0), &values));
    Ok(())
}

#[test]
fn benchmark_format_descriptors_are_valid_header_json() -> anyhow::Result<()> {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("name".into(), DataType::Utf8, false),
        SchemaColumn::new("count".into(), DataType::Int64, true),
    ]);
    for format in [
        YTsaurusReadFormat::Arrow,
        YTsaurusReadFormat::Skiff,
        YTsaurusReadFormat::SchemafulDsv,
        YTsaurusReadFormat::YsonBinary,
        YTsaurusReadFormat::YsonText,
        YTsaurusReadFormat::Json,
    ] {
        let descriptor = output_format(format, &schema)?;
        let parsed = serde_json::from_str::<serde_json::Value>(&descriptor)?;
        assert!(parsed.is_string() || parsed.get("$value").is_some());
    }
    Ok(())
}

#[test]
fn benchmark_discard_counters_survive_arbitrary_chunk_boundaries() -> anyhow::Result<()> {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("name".into(), DataType::Utf8, false),
        SchemaColumn::new("count".into(), DataType::Int64, true),
    ]);

    let arrow_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("count", DataType::Int64, true),
        ])),
        vec![
            Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
        ],
    )?;
    let arrow_wire = encode_arrow(&arrow_batch)?;
    let mut arrow = DiscardDecoder::new(YTsaurusReadFormat::Arrow, &schema)?;
    let mut arrow_rows = 0;
    for byte in arrow_wire {
        arrow_rows += arrow.decode(bytes::Bytes::from(vec![byte]))?;
    }
    arrow_rows += arrow.finish()?;
    assert_eq!(arrow_rows, 2);

    let mut yson = DiscardDecoder::new(YTsaurusReadFormat::YsonText, &schema)?;
    assert_eq!(yson.decode(bytes::Bytes::from_static(b"{\"name\"=\"a"))?, 0);
    assert_eq!(
        yson.decode(bytes::Bytes::from_static(b"\";};{\"name\"=\"b\";};"))?,
        2
    );
    assert_eq!(yson.finish()?, 0);

    let mut skiff = DiscardDecoder::new(YTsaurusReadFormat::Skiff, &schema)?;
    let mut wire = Vec::new();
    wire.extend_from_slice(&0_u16.to_le_bytes());
    wire.extend_from_slice(&3_u32.to_le_bytes());
    wire.extend_from_slice(b"one");
    wire.push(1);
    wire.extend_from_slice(&42_i64.to_le_bytes());
    wire.extend_from_slice(&0_u16.to_le_bytes());
    wire.extend_from_slice(&3_u32.to_le_bytes());
    wire.extend_from_slice(b"two");
    wire.push(0);
    assert_eq!(skiff.decode(bytes::Bytes::copy_from_slice(&wire[..7]))?, 0);
    assert_eq!(skiff.decode(bytes::Bytes::copy_from_slice(&wire[7..]))?, 2);
    assert_eq!(skiff.finish()?, 0);
    Ok(())
}

#[test]
fn benchmark_table_reader_validates_effective_server_limits() {
    assert!(YTsaurusTableReaderConfig {
        window_size: Some(64 * 1024 * 1024),
        ..YTsaurusTableReaderConfig::default()
    }
    .validate()
    .is_err());
    assert!(YTsaurusTableReaderConfig {
        group_size: Some(256 * 1024 * 1024),
        ..YTsaurusTableReaderConfig::default()
    }
    .validate()
    .is_err());
    assert!(YTsaurusTableReaderConfig {
        net_queue_size_factor: Some(f64::NAN),
        ..YTsaurusTableReaderConfig::default()
    }
    .validate()
    .is_err());
    assert!(YTsaurusTableReaderConfig {
        probe_peer_count: Some(0),
        ..YTsaurusTableReaderConfig::default()
    }
    .validate()
    .is_err());
}

#[test]
fn native_table_reader_materializes_measured_throughput_defaults() {
    assert_eq!(
        YTsaurusTableReaderConfig::default().to_yson(),
        concat!(
            "{window_size=134217728;group_size=134217728;",
            "max_buffer_size=536870912;}"
        )
    );
}

#[test]
fn native_table_reader_serializes_typed_yson_without_json_coercion() {
    let config = YTsaurusTableReaderConfig {
        window_size: Some(256 * 1024 * 1024),
        group_size: Some(128 * 1024 * 1024),
        max_buffer_size: Some(1024 * 1024 * 1024),
        max_parallel_readers: Some(1000),
        use_uncompressed_block_cache: Some(false),
        group_out_of_order_blocks: Some(true),
        use_block_cache: Some(false),
        use_async_block_cache: Some(true),
        populate_cache: Some(false),
        enable_workload_fifo_scheduling: Some(false),
        use_read_blocks_batcher: Some(true),
        prefer_local_data_center: Some(false),
        disk_queue_size_factor: Some(0.0),
        net_queue_size_factor: Some(2.0),
        cached_block_count_factor: Some(1.0),
        cached_block_size_factor: Some(1.5),
        use_direct_io: Some(true),
        fetch_from_peers: Some(false),
        probe_peer_count: Some(10),
        use_chunk_prober: Some(true),
        enable_chunk_meta_cache: Some(false),
        block_rpc_hedging_delay_ms: Some(100),
    };

    assert_eq!(
        config.to_yson(),
        concat!(
            "{window_size=268435456;group_size=134217728;",
            "max_buffer_size=1073741824;max_parallel_readers=1000;",
            "use_uncompressed_block_cache=%false;group_out_of_order_blocks=%true;",
            "use_block_cache=%false;use_async_block_cache=%true;populate_cache=%false;",
            "enable_workload_fifo_scheduling=%false;use_read_blocks_batcher=%true;",
            "prefer_local_data_center=%false;disk_queue_size_factor=0;",
            "net_queue_size_factor=2;cached_block_count_factor=1;",
            "cached_block_size_factor=1.5;use_direct_io=%true;fetch_from_peers=%false;",
            "probe_peer_count=10;use_chunk_prober=%true;enable_chunk_meta_cache=%false;",
            "block_rpc_hedging_delay=100;}"
        )
    );
}

#[test]
fn native_read_ordering_defaults_to_resumable_and_accepts_unordered() -> anyhow::Result<()> {
    let native_source = serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\n\
         host: cluster-a.example.net\n\
         port: 443\n\
         trusted_plaintext: true\n\
         trusted_native_rpc_plaintext: true\n\
         tables: [{ path: //tmp/input }]\n\
         read_ordering: { type: unordered }\n",
    )?;
    native_source.validate()?;
    assert_eq!(native_source.read_ordering, YTsaurusReadOrdering::Unordered);

    let default_source = serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\n\
         host: cluster-a.example.net\n\
         port: 443\n\
         trusted_plaintext: true\n\
         trusted_native_rpc_plaintext: true\n\
         tables: [{ path: //tmp/input }]\n",
    )?;
    assert_eq!(default_source.read_ordering, YTsaurusReadOrdering::Ordered);

    let partitioned = serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\n\
         host: cluster-a.example.net\n\
         port: 443\n\
         trusted_plaintext: true\n\
         trusted_native_rpc_plaintext: true\n\
         tables: [{ path: //tmp/input }]\n\
         read_ordering: { type: partition_tables, compressed_data_size_per_partition: 268435456, max_partition_count: 64, concurrency: 16 }\n",
    )?;
    partitioned.validate()?;
    assert!(matches!(
        partitioned.read_ordering,
        YTsaurusReadOrdering::PartitionTables {
            concurrency: 16,
            ..
        }
    ));
    Ok(())
}

#[test]
fn native_rpc_requires_explicit_plaintext_trust() -> anyhow::Result<()> {
    let source = serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\n\
         host: cluster-a.example.net\n\
         port: 9013\n\
         trusted_plaintext: false\n\
         trusted_native_rpc_plaintext: false\n\
         tables: [{ path: //tmp/input }]\n",
    )?;

    assert_eq!(
        source.validate().unwrap_err().to_string(),
        "ytsaurus native_rpc transport is plaintext; set trusted_native_rpc_plaintext=true to acknowledge that credentials and data will not be encrypted"
    );

    let trusted = serde_yaml::from_str::<YTsaurusSourceConfig>(
        "auth: { type: token, token: test }\n\
         host: cluster-a.example.net\n\
         port: 443\n\
         trusted_plaintext: false\n\
         trusted_native_rpc_plaintext: true\n\
         tables: [{ path: //tmp/input }]\n",
    )?;
    trusted.validate()?;
    assert_eq!(
        trusted.connection.endpoint(),
        "https://cluster-a.example.net:443"
    );
    Ok(())
}

#[test]
fn native_rpc_sends_the_oauth_token() {
    let proxy = credentials("oauth");
    assert_eq!(proxy.token.as_deref(), Some("oauth"));
}

#[test]
fn physical_chunk_layout_requires_complete_aggregate_statistics() -> anyhow::Result<()> {
    let columnar = PhysicalChunkLayout::from_statistics(
        3,
        &serde_json::json!({
            "table_unversioned_columnar": { "chunk_count": 3 }
        }),
    )?;
    assert!(columnar.all_columnar());

    let mixed = PhysicalChunkLayout::from_statistics(
        3,
        &serde_json::json!({
            "table_unversioned_columnar": { "chunk_count": 2 },
            "table_unversioned_schemaless_horizontal": { "chunk_count": 1 }
        }),
    )?;
    assert!(!mixed.all_columnar());
    assert!(PhysicalChunkLayout::from_statistics(
        4,
        &serde_json::json!({
            "table_unversioned_columnar": { "chunk_count": 3 }
        }),
    )
    .is_err());
    Ok(())
}

#[test]
fn physical_layout_advice_is_structured_and_actionable() {
    let table = |optimize_for_scan, physical_layout| DiscoveredTable {
        config: super::config::SourceTableConfig {
            path: "//tmp/events".to_owned(),
        },
        dataset_name: Arc::from("events"),
        schema: DatasetSchema::default(),
        optimize_for_scan,
        physical_layout,
    };
    let lookup = performance_advice(
        &[table(
            false,
            PhysicalChunkLayout {
                total: 2,
                columnar: 2,
                non_columnar: 0,
            },
        )],
        Some("dedicated"),
    );
    assert_eq!(lookup[0].code, "YT_OPTIMIZE_FOR_LOOKUP");

    let mixed_scan = performance_advice(
        &[table(
            true,
            PhysicalChunkLayout {
                total: 2,
                columnar: 1,
                non_columnar: 1,
            },
        )],
        Some("dedicated"),
    );
    assert_eq!(mixed_scan[0].code, "YT_SCAN_HAS_NON_COLUMNAR_CHUNKS");

    assert!(performance_advice(
        &[table(
            true,
            PhysicalChunkLayout {
                total: 2,
                columnar: 2,
                non_columnar: 0,
            },
        )],
        Some("dedicated"),
    )
    .is_empty());

    let shared_proxies = performance_advice(
        &[table(
            true,
            PhysicalChunkLayout {
                total: 2,
                columnar: 2,
                non_columnar: 0,
            },
        )],
        None,
    );
    assert_eq!(shared_proxies.len(), 1);
    assert_eq!(shared_proxies[0].code, "YT_SHARED_RPC_PROXIES");
}

#[derive(Clone, PartialEq, prost::Message)]
struct TestRowsetDescriptor {
    #[prost(int32, optional, tag = "4")]
    rowset_format: Option<i32>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct TestDataStatistics {
    #[prost(int64, optional, tag = "3")]
    row_count: Option<i64>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct TestRowsetStatistics {
    #[prost(int64, required, tag = "1")]
    total_row_count: i64,

    #[prost(message, required, tag = "2")]
    data_statistics: TestDataStatistics,
}

fn pack_refs(parts: &[&[u8]]) -> Bytes {
    let mut packed = Vec::new();
    packed.extend_from_slice(&i32::try_from(parts.len()).unwrap().to_le_bytes());
    for part in parts {
        packed.extend_from_slice(&i64::try_from(part.len()).unwrap().to_le_bytes());
        packed.extend_from_slice(part);
    }
    Bytes::from(packed)
}

#[test]
fn native_discard_uses_protocol_row_statistics_without_decoding_arrow() -> anyhow::Result<()> {
    let descriptor = TestRowsetDescriptor {
        rowset_format: Some(1),
    }
    .encode_to_vec();
    let arbitrary_arrow_payload = b"not decoded by the row counter";
    let rows = pack_refs(&[&descriptor, arbitrary_arrow_payload]);
    let statistics = TestRowsetStatistics {
        total_row_count: 50_000_000,
        data_statistics: TestDataStatistics {
            row_count: Some(12_345_678),
        },
    }
    .encode_to_vec();
    let envelope = pack_refs(&[&rows, &statistics]);

    let decoded =
        rowset_payload(&envelope, NativeReadFormat::Arrow, true)?.expect("non-empty Arrow rowset");
    assert_eq!(decoded.payload, Bytes::from_static(arbitrary_arrow_payload));
    assert_eq!(decoded.cumulative_rows, Some(12_345_678));
    assert_eq!(decoded.format, NativeReadFormat::Arrow);
    Ok(())
}

#[test]
fn native_arrow_accepts_only_the_empty_wire_stream_terminator() -> anyhow::Result<()> {
    let wire_descriptor = TestRowsetDescriptor {
        rowset_format: Some(0),
    }
    .encode_to_vec();
    let empty_wire_payload = 0_u64.to_le_bytes();
    let empty_rowset = pack_refs(&[&wire_descriptor, &empty_wire_payload]);

    assert!(rowset_payload(&empty_rowset, NativeReadFormat::Arrow, false)?.is_none());

    let one_row_payload = 1_u64.to_le_bytes();
    let non_empty_rowset = pack_refs(&[&wire_descriptor, &one_row_payload]);
    let error = match rowset_payload(&non_empty_rowset, NativeReadFormat::Arrow, false) {
        Ok(_) => anyhow::bail!("non-empty wire fallback must fail closed"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("refusing the fallback"));
    Ok(())
}
