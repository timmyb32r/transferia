use super::*;
use crate::types::schema::SchemaColumn;
use arrow::datatypes::DataType;

fn projection_discovery(keep_system_columns: bool) -> DeliveryDiscovery {
    let incoming_schema = DatasetSchema::new(vec![
        SchemaColumn::new("value".into(), DataType::Int64, false),
        SchemaColumn::new(
            SystemColumnKind::Offset.name().into(),
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
        source_partitions: vec![0],
        schema_origin: SchemaOrigin::ParserProjection,
        keep_system_columns,
        datasets: vec![DiscoveredDataset {
            role: DatasetRole::Main,
            name: Arc::from("events"),
            incoming_schema,
            stored_schema,
            system_columns: vec![SystemColumnKind::Offset],
        }],
    }
}

#[test]
fn dataset_role_tracks_the_existing_batch_flag() {
    assert_eq!(DatasetRole::from_is_dlq(false), DatasetRole::Main);
    assert_eq!(DatasetRole::from_is_dlq(true), DatasetRole::DeadLetterQueue);
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
        .push(SystemColumnKind::Offset);
    assert!(validate_stored_projection(&duplicate, &duplicate.datasets[0]).is_err());

    let mut wrong_type = projection_discovery(false);
    wrong_type.datasets[0].incoming_schema.columns[1].data_type = DataType::Utf8;
    assert!(validate_stored_projection(&wrong_type, &wrong_type.datasets[0]).is_err());

    let mut nullable = projection_discovery(false);
    nullable.datasets[0].incoming_schema.columns[1].nullable = true;
    assert!(validate_stored_projection(&nullable, &nullable.datasets[0]).is_err());
}
