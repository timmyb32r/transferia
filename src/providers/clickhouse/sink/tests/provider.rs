use arrow::array::BinaryArray;
use arrow::datatypes::DataType;

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
fn shard_group_query_materializes_low_cardinality_names_as_plain_strings() {
    assert!(SHARD_GROUPS_QUERY.contains("toString(cluster) AS cluster"));

    let column = BinaryArray::from(vec![Some(b"default".as_slice()), Some(b"analytics")]);
    let mut groups = Vec::new();
    append_shard_groups(&column, &mut groups).unwrap();
    assert_eq!(groups, ["default", "analytics"]);
}
use crate::core::data::schema::{DatasetSchema, SchemaColumn};
use crate::core::delivery::{DatasetRole, DiscoveredDataset, SchemaOrigin};

fn discovery(table: &str, data_type: DataType) -> DeliveryDiscovery {
    let schema = DatasetSchema::new(vec![
        SchemaColumn::new("value".into(), data_type, false).with_constraints(true, false, None)
    ]);
    DeliveryDiscovery {
        source_name: Arc::from("source-topic"),
        source_topology: crate::core::delivery::SourceTopology::StaticPartitions(vec![0]),
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
    }
}

#[tokio::test]
async fn provider_constructs_shared_client_without_connecting() -> anyhow::Result<()> {
    let provider = ClickHouseSinkProvider::from_config(serde_yaml::from_str(
        "hosts: [127.0.0.1]\nport: 1\ntrusted_plaintext: true\ndatabase: default\nusername: default\nconnect_timeout_ms: 1\n",
    )?)?;

    let first = Arc::clone(&provider.client);
    let second = Arc::clone(&provider.client);

    assert!(Arc::ptr_eq(&first, &second));
    Ok(())
}

#[test]
fn limits_are_declarative_and_validate_discovered_schema() -> anyhow::Result<()> {
    let provider = ClickHouseSinkProvider::from_config(serde_yaml::from_str(
        "hosts: [127.0.0.1]\nport: 1\ntrusted_plaintext: true\ndatabase: default\nusername: default\n",
    )?)?;

    let description = provider.limits().description();
    assert_eq!(description.sink, "clickhouse");
    assert_eq!(
        description.dataset_name.expect("table limit").syntax,
        NameSyntax::AsciiIdentifier,
    );
    assert_eq!(
        description.column_name.expect("column limit").syntax,
        NameSyntax::AsciiIdentifier,
    );
    assert!(description.object_key.is_none());
    provider
        .limits()
        .validate_discovery(&discovery("events", DataType::Int64))?;

    let invalid_name = provider
        .limits()
        .validate_discovery(&discovery("default.events", DataType::Int64))
        .unwrap_err();
    assert!(format!("{invalid_name:#}").contains("invalid ClickHouse table name"));

    let unsupported_type = provider
        .limits()
        .validate_discovery(&discovery("events", DataType::Date32))
        .unwrap_err();
    assert!(format!("{unsupported_type:#}").contains("Arrow Date32 is unavailable"));
    Ok(())
}
