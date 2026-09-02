use super::*;
use crate::data::schema::SchemaColumn;
use arrow::datatypes::DataType;

fn projection_discovery(keep_system_columns: bool) -> DeliveryDiscovery {
    let incoming_schema = DatasetSchema::new(vec![
        SchemaColumn::new("value".into(), DataType::Int64, false),
        SchemaColumn::new(
            SystemColumnKind::Offset.default_name().into(),
            DataType::Int64,
            false,
        ),
    ]);
    let stored_schema = if keep_system_columns {
        incoming_schema.clone()
    } else {
        DatasetSchema::new(vec![incoming_schema.columns[0].clone()])
    };
    DeliveryDiscovery {
        source_name: Arc::from("topic"),
        source_topology: crate::delivery::SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::ParserProjection,
        keep_system_columns,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema,
            stored_schema,
            system_columns: vec![SystemColumnKind::Offset.into()],
        }],
        performance_advice: Vec::new(),
    }
}

#[test]
fn dataset_role_tracks_the_existing_batch_flag() {
    assert_eq!(DatasetRole::from_is_dlq(false), DatasetRole::Main);
    assert_eq!(DatasetRole::from_is_dlq(true), DatasetRole::DeadLetterQueue);
}

#[test]
fn static_topology_validates_and_assigns_each_partition_once() -> anyhow::Result<()> {
    let topology = SourceTopology::StaticPartitions(vec![0, 3, 4, 7]);
    assert_eq!(topology.partitions_for_worker(3, 0)?, vec![0, 3]);
    assert_eq!(topology.partitions_for_worker(3, 1)?, vec![4, 7]);
    assert_eq!(topology.partitions_for_worker(3, 2)?, Vec::<i64>::new());

    assert!(SourceTopology::StaticPartitions(vec![]).validate().is_err());
    assert!(SourceTopology::StaticPartitions(vec![0, 0])
        .validate()
        .is_err());
    assert!(SourceTopology::StaticPartitions(vec![-1])
        .validate()
        .is_err());
    Ok(())
}

#[test]
fn dynamic_topology_assigns_one_lane_per_worker() -> anyhow::Result<()> {
    let topology = SourceTopology::DynamicWorkerLanes;
    assert_eq!(topology.partitions_for_worker(3, 0)?, vec![0]);
    assert_eq!(topology.partitions_for_worker(3, 1)?, vec![1]);
    assert_eq!(topology.partitions_for_worker(3, 2)?, vec![2]);
    assert!(topology.partitions_for_worker(3, 3).is_err());
    Ok(())
}

#[test]
fn stored_projection_follows_the_discovered_system_column_policy() -> anyhow::Result<()> {
    for keep in [false, true] {
        let discovery = projection_discovery(keep);
        validate_stored_projection(&discovery, discovery.dataset(DatasetRole::Main)?)?;
    }
    Ok(())
}

#[test]
fn stored_projection_rejects_partial_or_user_column_loss() {
    let mut partial = projection_discovery(true);
    partial.datasets[0].stored_schema.columns.pop();
    assert!(validate_stored_projection(&partial, &partial.datasets[0]).is_err());

    let mut user_loss = projection_discovery(false);
    user_loss.datasets[0].stored_schema.columns.clear();
    assert!(validate_stored_projection(&user_loss, &user_loss.datasets[0]).is_err());

    let mut duplicate = projection_discovery(false);
    duplicate.datasets[0]
        .system_columns
        .push(SystemColumnKind::Offset.into());
    assert!(validate_stored_projection(&duplicate, &duplicate.datasets[0]).is_err());

    let mut wrong_type = projection_discovery(false);
    wrong_type.datasets[0].incoming_schema.columns[1].data_type = DataType::Utf8;
    assert!(validate_stored_projection(&wrong_type, &wrong_type.datasets[0]).is_err());

    let mut nullable = projection_discovery(false);
    nullable.datasets[0].incoming_schema.columns[1].nullable = true;
    assert!(validate_stored_projection(&nullable, &nullable.datasets[0]).is_err());
}

#[test]
fn semantic_control_columns_are_never_part_of_stored_user_data() -> anyhow::Result<()> {
    let user = SchemaColumn::new("value".into(), DataType::Int64, false);
    let control = SchemaColumn::new("_system_source_database".into(), DataType::Utf8, false)
        .with_system_role(crate::data::schema::SYSTEM_ROLE_SOURCE_DATABASE);
    let discovery = DeliveryDiscovery {
        source_name: Arc::from("postgres"),
        source_topology: SourceTopology::StaticPartitions(vec![0]),
        schema_origin: SchemaOrigin::SourceNative,
        keep_system_columns: true,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema: DatasetSchema::new(vec![user.clone(), control.clone()]),
            stored_schema: DatasetSchema::new(vec![user]),
            system_columns: Vec::new(),
        }],
        performance_advice: Vec::new(),
    };

    validate_stored_projection(&discovery, &discovery.datasets[0])?;

    let mut leaked = discovery.clone();
    leaked.datasets[0].stored_schema.columns.push(control);
    assert!(validate_stored_projection(&leaked, &leaked.datasets[0]).is_err());
    Ok(())
}
