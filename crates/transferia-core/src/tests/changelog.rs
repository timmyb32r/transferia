use std::sync::Arc;

use arrow::array::{Array, BinaryArray, Int64Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;

use super::*;
use crate::data::schema::SchemaColumn;
use crate::delivery::{DatasetRole, DiscoveredDataset, DiscoveredSystemColumn, SchemaOrigin};
use crate::memory::PipelineMemory;
use crate::sink::SinkBatch;
use crate::{DatasetSchema, SystemColumn, SystemColumns};

fn schema_column(name: &str, nullable: bool, primary_key: bool) -> SchemaColumn {
    SchemaColumn::new(name.into(), arrow::datatypes::DataType::Int64, nullable)
        .with_constraints(primary_key, false, None)
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

async fn batch_with_changed_masks(
    operations: Vec<Option<&str>>,
    ids: Vec<Option<i64>>,
    values: Vec<i64>,
    masks: Vec<Option<&[u8]>>,
) -> (DeliveryDiscovery, SinkBatch) {
    let mut discovery = discovery();
    let changed = DiscoveredSystemColumn::from(SystemColumnKind::ChangedColumns);
    discovery.datasets[0].incoming_schema.columns.push(SchemaColumn::new(
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

#[tokio::test]
async fn preserves_unchanged_columns_while_collapsing_same_key_events() {
    let (discovery, batch) = batch_with_changed_masks(
        vec![Some("c"), Some("u"), Some("u")],
        vec![Some(1), Some(1), Some(1)],
        vec![10, 999, 30],
        vec![Some(&[0b11]), Some(&[0b01]), Some(&[0b11])],
    )
    .await;
    let ProjectedSinkBatch::Changelog(changelog) =
        project_sink_batch(&discovery, &batch).unwrap()
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
async fn emits_only_primary_key_and_changed_values_for_partial_update() {
    let (discovery, batch) = batch_with_changed_masks(
        vec![Some("u")],
        vec![Some(7)],
        vec![999],
        vec![Some(&[0b01])],
    )
    .await;
    let ProjectedSinkBatch::Changelog(changelog) =
        project_sink_batch(&discovery, &batch).unwrap()
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
    missing.system_columns = SystemColumns::new(vec![
        missing
            .system_columns
            .get(SystemColumnKind::ChangeOperation)
            .unwrap(),
    ]);
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
