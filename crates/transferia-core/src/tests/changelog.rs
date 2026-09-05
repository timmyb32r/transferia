use std::sync::Arc;

use arrow::array::{Array, BinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::*;
use crate::data::schema::SchemaColumn;
use crate::delivery::{
    validate_batch_against_discovery, validate_stored_projection, DatasetRole, DiscoveredDataset,
    DiscoveredSystemColumn, SchemaOrigin,
};
use crate::memory::PipelineMemory;
use crate::sink::SinkBatch;
use crate::{DatasetSchema, SystemColumn, SystemColumns};

fn schema_column(name: &str, nullable: bool, primary_key: bool) -> SchemaColumn {
    SchemaColumn::new(name.into(), arrow::datatypes::DataType::Int64, nullable).with_constraints(
        primary_key,
        false,
        None,
    )
}

#[tokio::test]
async fn logical_versions_are_independent_of_transport_offsets_and_validated() {
    let mut discovery = discovery();
    discovery.keep_system_columns = false;
    let mut batch = batch(vec![Some("r"), Some("u")], vec![Some(1), Some(1)]).await;
    let column = SchemaColumn::new("row_version".into(), DataType::UInt64, false)
        .with_system_role(crate::data::schema::SYSTEM_ROLE_SOURCE_VERSION);
    discovery.datasets[0]
        .incoming_schema
        .columns
        .push(column.clone());
    let mut fields = batch
        .batch
        .schema()
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.push(Arc::new(
        Field::new("row_version", DataType::UInt64, false).with_metadata(column.arrow_metadata()),
    ));
    let mut arrays = batch.batch.columns().to_vec();
    arrays.push(Arc::new(arrow::array::UInt64Array::from(vec![0, 1])));
    batch.batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    validate_stored_projection(&discovery, &discovery.datasets[0]).unwrap();
    let ProjectedSinkBatch::Changelog(changelog) = project_sink_batch(&discovery, &batch).unwrap()
    else {
        panic!("expected changelog")
    };
    assert_eq!(changelog.source_versions(), [0, 1]);
    assert_eq!(
        batch
            .batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        100
    );
    discovery.datasets[0]
        .incoming_schema
        .columns
        .last_mut()
        .unwrap()
        .nullable = true;
    assert!(validate_stored_projection(&discovery, &discovery.datasets[0]).is_err());
    assert!(project_sink_batch(&discovery, &batch).is_err());
    discovery.datasets[0]
        .incoming_schema
        .columns
        .last_mut()
        .unwrap()
        .nullable = false;
    discovery.datasets[0].incoming_schema.columns.push(column);
    assert!(validate_stored_projection(&discovery, &discovery.datasets[0]).is_err());
    assert!(project_sink_batch(&discovery, &batch).is_err());
}

fn discovery() -> DeliveryDiscovery {
    let operation = DiscoveredSystemColumn::from(SystemColumnKind::ChangeOperation);
    let offset = DiscoveredSystemColumn::from(SystemColumnKind::Offset);
    DeliveryDiscovery {
        source_name: Arc::from("postgres"),
        source_topology: crate::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: true,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema: DatasetSchema::new(vec![
                schema_column("id", true, true),
                schema_column("value", true, false),
                SchemaColumn::new(
                    operation.name.to_string(),
                    SystemColumnKind::ChangeOperation.data_type(),
                    false,
                ),
                SchemaColumn::new(
                    offset.name.to_string(),
                    SystemColumnKind::Offset.data_type(),
                    false,
                ),
            ]),
            stored_schema: DatasetSchema::new(vec![
                schema_column("id", false, true),
                schema_column("value", false, false),
            ]),
            system_columns: vec![operation, offset],
        }],
        performance_advice: Vec::new(),
    }
}

async fn batch(operations: Vec<Option<&str>>, ids: Vec<Option<i64>>) -> SinkBatch {
    let discovery = discovery();
    let columns = &discovery.datasets[0].incoming_schema.columns;
    let operation_nullable = operations.iter().any(Option::is_none);
    let fields = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Field::new(
                &column.name,
                column.data_type.clone(),
                column.nullable || (index == 2 && operation_nullable),
            )
            .with_metadata(column.arrow_metadata())
        })
        .collect::<Vec<_>>();
    let rows = ids.len();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(
                (0..rows)
                    .map(|row| Some(i64::try_from(row).unwrap() * 10))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(operations)),
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| 100 + i64::try_from(row).unwrap()),
            )),
        ],
    )
    .unwrap();
    SinkBatch {
        table: Arc::from("events"),
        is_dlq: false,
        byte_size: batch.get_array_memory_size(),
        batch,
        memory: PipelineMemory::new(1024 * 1024).reserve(1).await,
        system_columns: SystemColumns::new(vec![
            SystemColumn {
                kind: SystemColumnKind::ChangeOperation,
                index: 2,
                name: Arc::from(SystemColumnKind::ChangeOperation.default_name()),
            },
            SystemColumn {
                kind: SystemColumnKind::Offset,
                index: 3,
                name: Arc::from(SystemColumnKind::Offset.default_name()),
            },
        ]),
    }
}

#[tokio::test]
async fn splits_create_read_update_and_delete_without_storing_operation() {
    let batch = batch(
        vec![Some("c"), Some("r"), Some("u"), Some("d")],
        vec![Some(1), Some(2), Some(3), Some(4)],
    )
    .await;
    let ProjectedSinkBatch::Changelog(changelog) =
        project_sink_batch(&discovery(), &batch).unwrap()
    else {
        panic!("operation column must produce a changelog batch")
    };

    assert_eq!(changelog.primary_keys, ["id"]);
    assert_eq!(changelog.source_versions(), [100, 101, 102, 103]);
    let runs = changelog.collapsed_runs().unwrap();
    assert_eq!(runs.len(), 3);
    assert_eq!(runs[0].action, ChangelogAction::Upsert);
    assert_eq!(runs[0].batch.num_rows(), 2);
    assert_eq!(runs[0].batch.num_columns(), 2);
    assert_eq!(runs[1].operation, ChangeOperation::Update);
    assert_eq!(runs[1].batch.num_rows(), 1);
    assert_eq!(runs[1].batch.num_columns(), 2);
    assert_eq!(runs[2].action, ChangelogAction::Delete);
    assert_eq!(runs[2].batch.num_rows(), 1);
    assert_eq!(runs[2].batch.num_columns(), 1);
    let ids = runs[2]
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.value(0), 4);
}

#[tokio::test]
async fn runtime_schema_rejects_missing_arrow_extension_metadata() {
    let mut expected = discovery();
    expected.datasets[0].incoming_schema.columns[0] = expected.datasets[0].incoming_schema.columns
        [0]
    .clone()
    .with_arrow_extension_metadata(
        "transferia.mysql.signed_integer",
        r#"{"version":1,"column_type":"bigint"}"#,
    );
    let actual = batch(vec![Some("c")], vec![Some(1)]).await;

    assert!(validate_batch_against_discovery(&expected, &actual).is_err());
}

#[tokio::test]
async fn append_only_projection_rejects_null_before_the_sink_side_effect() {
    let incoming = SchemaColumn::new("value".into(), DataType::Int64, true);
    let stored = SchemaColumn::new("value".into(), DataType::Int64, false);
    let discovery = DeliveryDiscovery {
        source_name: Arc::from("snapshot"),
        source_topology: crate::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: false,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema: DatasetSchema::new(vec![incoming.clone()]),
            stored_schema: DatasetSchema::new(vec![stored]),
            system_columns: Vec::new(),
        }],
        performance_advice: Vec::new(),
    };
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            &incoming.name,
            incoming.data_type,
            true,
        )])),
        vec![Arc::new(Int64Array::from(vec![None]))],
    )
    .unwrap();
    let batch = SinkBatch {
        table: Arc::from("events"),
        is_dlq: false,
        byte_size: batch.get_array_memory_size(),
        batch,
        memory: PipelineMemory::new(1024).reserve(1).await,
        system_columns: SystemColumns::new(Vec::new()),
    };

    assert!(project_sink_batch(&discovery, &batch).is_err());
}

async fn batch_with_changed_masks(
    operations: Vec<Option<&str>>,
    ids: Vec<Option<i64>>,
    values: Vec<i64>,
    masks: Vec<Option<&[u8]>>,
) -> (DeliveryDiscovery, SinkBatch) {
    let mut discovery = discovery();
    let changed = DiscoveredSystemColumn::from(SystemColumnKind::ChangedColumns);
    discovery.datasets[0]
        .incoming_schema
        .columns
        .push(SchemaColumn::new(
            changed.name.to_string(),
            SystemColumnKind::ChangedColumns.data_type(),
            false,
        ));
    discovery.datasets[0].system_columns.push(changed.clone());
    let mut batch = batch(operations, ids).await;
    let mut arrays = batch.batch.columns().to_vec();
    arrays[1] = Arc::new(Int64Array::from(values));
    arrays.push(Arc::new(BinaryArray::from(masks)));
    let mut fields = batch
        .batch
        .schema()
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        changed.name.as_ref(),
        SystemColumnKind::ChangedColumns.data_type(),
        false,
    )));
    batch.batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let mut systems = batch.system_columns.iter().cloned().collect::<Vec<_>>();
    systems.push(SystemColumn {
        kind: SystemColumnKind::ChangedColumns,
        index: 4,
        name: Arc::clone(&changed.name),
    });
    batch.system_columns = SystemColumns::new(systems);
    (discovery, batch)
}

async fn full_old_value_batch(
    current_ids: Vec<i64>,
    old_ids: Vec<Option<i64>>,
    operations: Vec<Option<&str>>,
) -> (DeliveryDiscovery, SinkBatch) {
    let masks = vec![Some(&[0b11][..]); current_ids.len()];
    let (mut discovery, mut batch) = batch_with_changed_masks(
        operations,
        current_ids.iter().copied().map(Some).collect(),
        current_ids.iter().map(|id| id * 10).collect(),
        masks,
    )
    .await;
    discovery.keep_system_columns = false;
    let old_id = SchemaColumn::new("_system_old_value_0".into(), DataType::Int64, true)
        .with_old_value_of("id".into());
    let old_value = SchemaColumn::new("_system_old_value_1".into(), DataType::Int64, true)
        .with_old_value_of("value".into());
    discovery.datasets[0]
        .incoming_schema
        .columns
        .extend([old_id.clone(), old_value.clone()]);
    let mut fields = batch
        .batch
        .schema()
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.extend([old_id, old_value].into_iter().map(|column| {
        Arc::new(
            Field::new(&column.name, column.data_type.clone(), true)
                .with_metadata(column.arrow_metadata()),
        )
    }));
    let mut arrays = batch.batch.columns().to_vec();
    arrays.push(Arc::new(Int64Array::from(old_ids.clone())));
    arrays.push(Arc::new(Int64Array::from(
        old_ids
            .iter()
            .map(|id| id.map(|id| id * 10))
            .collect::<Vec<_>>(),
    )));
    batch.batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    (discovery, batch)
}

async fn old_key_batch(
    current_ids: Vec<i64>,
    old_ids: Vec<Option<i64>>,
    operations: Vec<Option<&str>>,
) -> (DeliveryDiscovery, SinkBatch) {
    let masks = vec![Some(&[0b11][..]); current_ids.len()];
    let (mut discovery, mut batch) = batch_with_changed_masks(
        operations,
        current_ids.iter().copied().map(Some).collect(),
        current_ids.iter().map(|id| id * 10).collect(),
        masks,
    )
    .await;
    discovery.keep_system_columns = false;
    let old_key = SchemaColumn::new("_system_old_key_0".into(), DataType::Int64, true)
        .with_old_key_of("id".into());
    discovery.datasets[0]
        .incoming_schema
        .columns
        .push(old_key.clone());
    let mut fields = batch
        .batch
        .schema()
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    fields.push(Arc::new(
        Field::new(&old_key.name, old_key.data_type.clone(), true)
            .with_metadata(old_key.arrow_metadata()),
    ));
    let mut arrays = batch.batch.columns().to_vec();
    arrays.push(Arc::new(Int64Array::from(old_ids)));
    arrays[3] = Arc::new(Int64Array::from_iter_values(vec![42; current_ids.len()]));
    batch.batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    (discovery, batch)
}

#[tokio::test]
async fn preserves_unchanged_columns_while_collapsing_same_key_events() {
    let (discovery, batch) = batch_with_changed_masks(
        vec![Some("c"), Some("u"), Some("u")],
        vec![Some(1), Some(1), Some(1)],
        vec![10, 999, 30],
        vec![Some(&[0b11]), Some(&[0b01]), Some(&[0b11])],
    )
    .await;
    let ProjectedSinkBatch::Changelog(changelog) = project_sink_batch(&discovery, &batch).unwrap()
    else {
        panic!("operation column must produce a changelog batch")
    };

    let runs = changelog.collapsed_runs().unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].operation, ChangeOperation::Create);
    assert_eq!(runs[0].source_versions, [102]);
    assert_eq!(
        runs[0]
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        30
    );
}

#[tokio::test]
async fn full_image_collapse_keeps_columns_whose_values_did_not_change() {
    let (discovery, batch) = batch_with_changed_masks(
        vec![Some("u")],
        vec![Some(1)],
        vec![77],
        vec![Some(&[0b01])],
    )
    .await;
    let ProjectedSinkBatch::Changelog(changelog) = project_sink_batch(&discovery, &batch).unwrap()
    else {
        panic!("operation column must produce a changelog batch")
    };

    assert_eq!(
        changelog.collapsed_runs().unwrap()[0].batch.num_columns(),
        1
    );
    let full = changelog.collapsed_full_image_runs().unwrap();
    assert_eq!(full[0].batch.num_columns(), 2);
    assert_eq!(
        full[0]
            .batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        77
    );
}

#[tokio::test]
async fn replica_identity_full_collapses_primary_key_changes_without_leaving_old_rows() {
    let (discovery, batch) = full_old_value_batch(
        vec![2, 3],
        vec![Some(1), Some(2)],
        vec![Some("u"), Some("u")],
    )
    .await;
    validate_stored_projection(&discovery, &discovery.datasets[0]).unwrap();
    let ProjectedSinkBatch::Changelog(changelog) = project_sink_batch(&discovery, &batch).unwrap()
    else {
        panic!("operation column must produce a changelog batch")
    };

    let runs = changelog.collapsed_runs().unwrap();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].operation, ChangeOperation::Delete);
    assert_eq!(runs[0].batch.num_columns(), 1);
    assert_eq!(
        runs[0]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 2]
    );
    assert_eq!(runs[1].operation, ChangeOperation::Create);
    assert_eq!(
        runs[1]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
}

#[tokio::test]
async fn old_key_metadata_collapses_same_lsn_primary_key_changes_in_event_order() {
    let (discovery, batch) = old_key_batch(
        vec![2, 3],
        vec![Some(1), Some(2)],
        vec![Some("u"), Some("u")],
    )
    .await;
    validate_stored_projection(&discovery, &discovery.datasets[0]).unwrap();
    let ProjectedSinkBatch::Changelog(changelog) = project_sink_batch(&discovery, &batch).unwrap()
    else {
        panic!("operation column must produce a changelog batch")
    };

    let runs = changelog.collapsed_runs().unwrap();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].action, ChangelogAction::Delete);
    assert_eq!(runs[0].source_versions, [42, 42]);
    assert_eq!(
        runs[0]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 2]
    );
    assert_eq!(runs[1].action, ChangelogAction::Upsert);
    assert_eq!(runs[1].source_versions, [42]);
    assert_eq!(
        runs[1]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
}

#[tokio::test]
async fn old_key_contract_rejects_missing_duplicate_and_null_primary_keys() {
    let (discovery, batch) = old_key_batch(vec![2], vec![Some(1)], vec![Some("u")]).await;

    let mut unknown = discovery.clone();
    unknown.datasets[0]
        .incoming_schema
        .columns
        .last_mut()
        .unwrap()
        .old_key_of = Some("value".into());
    assert!(validate_stored_projection(&unknown, &unknown.datasets[0]).is_err());

    let mut mixed = discovery.clone();
    mixed.datasets[0]
        .incoming_schema
        .columns
        .last_mut()
        .unwrap()
        .old_value_of = Some("id".into());
    assert!(validate_stored_projection(&mixed, &mixed.datasets[0]).is_err());

    let mut null_old_key = batch;
    let mut arrays = null_old_key.batch.columns().to_vec();
    *arrays.last_mut().unwrap() = Arc::new(Int64Array::from(vec![None]));
    null_old_key.batch = RecordBatch::try_new(null_old_key.batch.schema(), arrays).unwrap();
    assert!(project_sink_batch(&discovery, &null_old_key).is_err());
}

#[tokio::test]
async fn replica_identity_full_contract_is_bijective_and_fails_closed() {
    let (discovery, batch) = full_old_value_batch(vec![2], vec![Some(1)], vec![Some("u")]).await;

    let mut missing = discovery.clone();
    missing.datasets[0].incoming_schema.columns.pop();
    assert!(validate_stored_projection(&missing, &missing.datasets[0]).is_err());

    let mut duplicate = discovery.clone();
    duplicate.datasets[0].incoming_schema.columns[6].old_value_of = Some("id".into());
    assert!(validate_stored_projection(&duplicate, &duplicate.datasets[0]).is_err());

    let mut null_old_key = batch;
    let mut arrays = null_old_key.batch.columns().to_vec();
    arrays[5] = Arc::new(Int64Array::from(vec![None]));
    null_old_key.batch = RecordBatch::try_new(null_old_key.batch.schema(), arrays).unwrap();
    assert!(project_sink_batch(&discovery, &null_old_key).is_err());
}

#[tokio::test]
async fn emits_only_primary_key_and_changed_values_for_partial_update() {
    let (discovery, batch) = batch_with_changed_masks(
        vec![Some("u")],
        vec![Some(7)],
        vec![999],
        vec![Some(&[0b01])],
    )
    .await;
    let ProjectedSinkBatch::Changelog(changelog) = project_sink_batch(&discovery, &batch).unwrap()
    else {
        panic!("operation column must produce a changelog batch")
    };

    let runs = changelog.collapsed_runs().unwrap();
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].operation, ChangeOperation::Update);
    assert_eq!(runs[0].batch.num_columns(), 1);
    assert_eq!(runs[0].batch.schema().field(0).name(), "id");
}

#[tokio::test]
async fn rejects_invalid_changed_column_masks_before_sink_side_effects() {
    for mask in [vec![], vec![0b100], vec![0b10]] {
        let (discovery, batch) = batch_with_changed_masks(
            vec![Some("u")],
            vec![Some(1)],
            vec![10],
            vec![Some(mask.as_slice())],
        )
        .await;
        assert!(project_sink_batch(&discovery, &batch).is_err());
    }
}

#[tokio::test]
async fn rejects_unknown_null_operations_and_null_delete_keys() {
    for invalid in [
        batch(vec![Some("x")], vec![Some(1)]).await,
        batch(vec![None], vec![Some(1)]).await,
        batch(vec![Some("d")], vec![None]).await,
    ] {
        assert!(project_sink_batch(&discovery(), &invalid).is_err());
    }
}

#[tokio::test]
async fn rejects_missing_negative_and_null_source_versions() {
    let mut missing = batch(vec![Some("u")], vec![Some(1)]).await;
    missing.system_columns = SystemColumns::new(vec![missing
        .system_columns
        .get(SystemColumnKind::ChangeOperation)
        .unwrap()]);
    assert!(project_sink_batch(&discovery(), &missing).is_err());

    for versions in [vec![Some(-1)], vec![None]] {
        let mut invalid = batch(vec![Some("u")], vec![Some(1)]).await;
        let mut arrays = invalid.batch.columns().to_vec();
        let mut fields = invalid
            .batch
            .schema()
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        if versions[0].is_none() {
            fields[3] = Arc::new(fields[3].as_ref().clone().with_nullable(true));
        }
        arrays[3] = Arc::new(Int64Array::from(versions));
        invalid.batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
        assert!(project_sink_batch(&discovery(), &invalid).is_err());
    }
}

#[tokio::test]
async fn rejects_changelog_without_primary_key() {
    let mut discovery = discovery();
    discovery.datasets[0].stored_schema.columns[0].primary_key = false;
    let batch = batch(vec![Some("c")], vec![Some(1)]).await;
    assert!(project_sink_batch(&discovery, &batch).is_err());
}

#[tokio::test]
async fn collapses_repeated_primary_keys_before_any_sink_side_effect() {
    let batch = batch(
        vec![Some("c"), Some("u"), Some("c"), Some("d")],
        vec![Some(1), Some(1), Some(2), Some(2)],
    )
    .await;
    let ProjectedSinkBatch::Changelog(changelog) =
        project_sink_batch(&discovery(), &batch).unwrap()
    else {
        panic!("operation column must produce a changelog batch")
    };

    let runs = changelog.collapsed_runs().unwrap();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0].action, ChangelogAction::Upsert);
    assert_eq!(runs[0].source_versions, [101]);
    assert_eq!(runs[1].action, ChangelogAction::Delete);
    assert_eq!(runs[1].source_versions, [103]);
    let upsert_id = runs[0]
        .batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let upsert_value = runs[0]
        .batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(upsert_id.value(0), 1);
    assert_eq!(upsert_value.value(0), 10);
    assert_eq!(runs[1].batch.num_columns(), 1);
    assert_eq!(
        runs[1]
            .batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
}
