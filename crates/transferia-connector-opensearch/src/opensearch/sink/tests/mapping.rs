use arrow::datatypes::{DataType, TimeUnit};
use serde_json::json;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};

use super::super::mapping::{create_index_body, strict_mapping, validate_index_description};

fn schema() -> DatasetSchema {
    DatasetSchema::new(vec![
        SchemaColumn::new("id".to_owned(), DataType::UInt64, false)
            .with_constraints(true, false, None),
        SchemaColumn::new(
            "at".to_owned(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        SchemaColumn::new("amount".to_owned(), DataType::Decimal128(38, 9), true),
    ])
}

#[test]
fn generated_mapping_is_strict_and_preserves_logical_type_metadata() {
    let mapping = strict_mapping(&schema(), Some("owner")).unwrap();
    assert_eq!(mapping["dynamic"], "strict");
    assert_eq!(mapping["properties"]["id"]["type"], "unsigned_long");
    assert_eq!(mapping["properties"]["at"]["type"], "long");
    assert_eq!(
        mapping["properties"]["at"]["meta"]["time_unit"],
        "nanosecond"
    );
    assert_eq!(mapping["properties"]["amount"]["type"], "keyword");
    assert_eq!(mapping["_meta"]["transferia_speedtest_owner"], "owner");
}

#[test]
fn index_validation_requires_exact_mapping_owner_and_request_durability() {
    let body = create_index_body(&schema(), Some("owner")).unwrap();
    let description = json!({
        "logs": {
            "settings": { "index": { "translog": { "durability": "request" } } },
            "mappings": body["mappings"].clone()
        }
    });
    validate_index_description("logs", &description, &schema(), Some("owner")).unwrap();

    let mut foreign = description.clone();
    foreign["logs"]["mappings"]["_meta"]["transferia_speedtest_owner"] = "foreign".into();
    assert!(validate_index_description("logs", &foreign, &schema(), Some("owner")).is_err());

    let mut lossy = description.clone();
    lossy["logs"]["mappings"]["properties"]["amount"]["type"] = "double".into();
    assert!(validate_index_description("logs", &lossy, &schema(), Some("owner")).is_err());

    let mut async_durability = description;
    async_durability["logs"]["settings"]["index"]["translog"]["durability"] = "async".into();
    assert!(
        validate_index_description("logs", &async_durability, &schema(), Some("owner")).is_err()
    );
}
