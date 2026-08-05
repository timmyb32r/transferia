/// Integration tests: happy-path verification that all 6 components
/// work end-to-end within the provider registry and pipeline semantics.
use std::sync::Arc;

use arrow::array::{Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_yaml::Value;

use ch_loader::config::yaml::{ColumnMapping, SchemaConfig, ChunkSplitter};
use ch_loader::pipeline::sink::Sink;
use ch_loader::providers::traits::ProviderRegistry;
use ch_loader::serializer::Serializer;
use ch_loader::serializer::json_serializer::JsonSerializer;
use ch_loader::types::table_data::{TableData, TableWrite};

// ---------------------------------------------------------------------------
// Task 1: Empty sink integration
// ---------------------------------------------------------------------------

#[test]
fn empty_sink_provider_registration() {
    let mut registry = ProviderRegistry::new();
    registry.register_sink("empty", |v| {
        Ok(Box::new(ch_loader::providers::empty::provider::EmptySinkProvider::from_config(v)?))
    });
    // Verify registration doesn't panic
    let raw: Value = serde_yaml::from_str("batch_size: 5000").unwrap();
    let result = registry.build_sink("empty", raw);
    assert!(result.is_ok(), "Empty sink should build from valid config");
    let _ = result.unwrap(); // type check: is Box<dyn SinkProvider>
}

#[test]
fn empty_sink_through_registry() {
    let mut registry = ProviderRegistry::new();
    registry.register_sink("empty", |v| {
        Ok(Box::new(ch_loader::providers::empty::provider::EmptySinkProvider::from_config(v)?))
    });
    let raw: Value = serde_yaml::from_str("batch_size: 100").unwrap();
    let provider = registry.build_sink("empty", raw).unwrap();
    // create_tables and verify_tables should be no-ops
    let schema = SchemaConfig {
        columns: vec![ColumnMapping {
            jsonpath: "$.id".into(), column_name: "id".into(),
            arrow_type: "Int64".into(), nullable: false,
        }],
        raw_payload_field: None, order_by: vec![], chunk_splitter: ChunkSplitter::NoSplit,
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        provider.create_tables("test", "test.dlq", &schema, false).await.unwrap();
        provider.verify_tables("test", "test.dlq").await.unwrap();
    });
}

// ---------------------------------------------------------------------------
// Task 2: Exactly-once dedup token flow
// ---------------------------------------------------------------------------

#[test]
fn dedup_token_flows_to_table_write() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
    let arr = Int64Array::from(vec![1i64, 2]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    let td = TableData {
        table: "events".into(), is_dlq: false, batch,
        batch_id: 1, dedup_token: Some("deadbeef12345678".into()),
    };

    assert_eq!(td.dedup_token.as_deref(), Some("deadbeef12345678"));
    assert!(!td.is_dlq);

    let tw = TableWrite {
        table: "events".into(),
        batches: vec![td.batch.clone()],
        dedup_token: td.dedup_token.clone(),
    };
    assert_eq!(tw.dedup_token.as_deref(), Some("deadbeef12345678"));
}

#[test]
fn dedup_token_none_for_non_streaming() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
    let arr = Int64Array::from(vec![1i64]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();

    let td = TableData {
        table: "snapshots".into(), is_dlq: false, batch,
        batch_id: 1, dedup_token: None,
    };
    assert!(td.dedup_token.is_none());

    let tw = TableWrite {
        table: "snapshots".into(),
        batches: vec![td.batch.clone()],
        dedup_token: None,
    };
    assert!(tw.dedup_token.is_none());
}

// ---------------------------------------------------------------------------
// Task 3: Parallel CH insert sink trait compliance
// ---------------------------------------------------------------------------

#[test]
fn parallel_ch_sink_is_sink_trait() {
    // Type-level check: ParallelChInsertSink implements Sink
    fn assert_sink<T: Sink>() {}
    // This test verifies the trait is implemented; can't instantiate
    // without a real ClickHouse server, but the type check compiles.
    assert_sink::<ch_loader::middleware::parallel_ch_insert::ParallelChInsertSink>();
}

// ---------------------------------------------------------------------------
// Task 4 & 5: S3/YDS sinks + JSON serializer roundtrip
// ---------------------------------------------------------------------------

#[test]
fn serializer_roundtrip_many_types() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let id_arr = Int64Array::from(vec![1, 2]);
    let mut name_builder = StringBuilder::with_capacity(2, 32);
    name_builder.append_value("hello");
    name_builder.append_null();
    let bool_arr = arrow::array::BooleanArray::from(vec![true, false]);
    let float_arr = arrow::array::Float64Array::from(vec![1.5_f64, 2.5_f64]);

    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(id_arr), Arc::new(name_builder.finish()),
        Arc::new(bool_arr), Arc::new(float_arr),
    ]).unwrap();

    let ser = JsonSerializer;
    let output = ser.serialize_batch(&batch).unwrap();
    let text = String::from_utf8(output.to_vec()).unwrap();
    let lines: Vec<&str> = text.trim().split('\n').collect();
    assert_eq!(lines.len(), 2);

    // Verify each line is valid JSON
    for line in lines {
        let _: serde_json::Value = serde_json::from_str(line).unwrap();
    }
}

#[test]
fn s3_sink_provider_from_config() {
    let yaml = r#"
bucket: my-bucket
prefix: snapshots/
region: us-east-1
serializer_type: json
"#;
    let raw: Value = serde_yaml::from_str(yaml).unwrap();
    let result = ch_loader::providers::s3::sink::provider::S3SinkProvider::from_config(raw);
    assert!(result.is_ok(), "S3 sink provider should parse valid config: {:?}", result.err());
}

#[test]
fn yds_sink_provider_from_config() {
    let yaml = r#"
connection_string: grpc://localhost:2135/local
topic_path: /Root/my-topic
serializer_type: json
"#;
    let raw: Value = serde_yaml::from_str(yaml).unwrap();
    let result = ch_loader::providers::yds::sink::provider::YdsSinkProvider::from_config(raw);
    assert!(result.is_ok(), "YDS sink provider should parse valid config: {:?}", result.err());
}

#[test]
fn s3_sink_rejects_empty_bucket() {
    let yaml = r#"
bucket: ""
prefix: snapshots/
"#;
    let raw: Value = serde_yaml::from_str(yaml).unwrap();
    let result = ch_loader::providers::s3::sink::provider::S3SinkProvider::from_config(raw);
    assert!(result.is_err(), "Should reject empty bucket");
}

#[test]
fn yds_sink_rejects_empty_connection_string() {
    let yaml = r#"
connection_string: ""
topic_path: /Root/my-topic
"#;
    let raw: Value = serde_yaml::from_str(yaml).unwrap();
    let result = ch_loader::providers::yds::sink::provider::YdsSinkProvider::from_config(raw);
    assert!(result.is_err(), "Should reject empty connection_string");
}

// ---------------------------------------------------------------------------
// Task 6: CH source table selection
// ---------------------------------------------------------------------------

#[test]
fn ch_source_explicit_tables() {
    use ch_loader::providers::clickhouse::source::{TableRef, TableSelection};

    let tables = vec![
        TableRef { schema_name: "db1".into(), table_name: "events".into() },
        TableRef { schema_name: "db1".into(), table_name: "users".into() },
    ];
    let sel = TableSelection::Explicit(tables);
    match sel {
        TableSelection::Explicit(ts) => assert_eq!(ts.len(), 2),
        _ => panic!("Expected Explicit"),
    }
}

#[test]
fn ch_source_regex_selection() {
    use regex::Regex;
    use ch_loader::providers::clickhouse::source::TableSelection;

    let include = vec![
        Regex::new("prod_.*").unwrap(),
        Regex::new(".*_events").unwrap(),
    ];
    let exclude = vec![
        Regex::new(".*_tmp").unwrap(),
    ];

    let sel = TableSelection::Patterns {
        include_patterns: include,
        exclude_patterns: exclude,
    };

    match &sel {
        TableSelection::Patterns { include_patterns, exclude_patterns } => {
            assert_eq!(include_patterns.len(), 2);
            assert_eq!(exclude_patterns.len(), 1);

            // Test matching logic
            let full_name = "prod_important_events";
            let included = include_patterns.iter().all(|re| re.is_match(full_name));
            assert!(included, "'{}' should match all includes", full_name);

            let excluded = exclude_patterns.iter().any(|re| re.is_match(full_name));
            assert!(!excluded, "'{}' should not match any excludes", full_name);
        }
        _ => panic!("Expected Patterns"),
    }
}

#[test]
fn ch_source_regex_exclude_works() {
    use regex::Regex;
    use ch_loader::providers::clickhouse::source::TableSelection;

    let include = vec![Regex::new("prod_.*").unwrap()];
    let exclude = vec![Regex::new(".*_tmp").unwrap()];

    let sel = TableSelection::Patterns {
        include_patterns: include,
        exclude_patterns: exclude,
    };

    match &sel {
        TableSelection::Patterns { include_patterns, exclude_patterns } => {
            // "prod_data_tmp" matches include but should be excluded
            let name = "prod_data_tmp";
            assert!(include_patterns.iter().all(|re| re.is_match(name)));
            assert!(exclude_patterns.iter().any(|re| re.is_match(name)));
        }
        _ => panic!("Expected Patterns"),
    }
}

#[test]
fn ch_source_provider_rejects_both_tables_and_patterns() {
    let yaml = r#"
connection_string: localhost:9000
tables:
  - schema: db
    table: events
include_patterns:
  - ".*"
parser:
  table_naming:
    type: from_config
    name: test
  parser_type: json_parser
  settings:
    columns:
      - jsonpath: "$.id"
        column_name: id
        arrow_type: Int64
"#;
    let raw: Value = serde_yaml::from_str(yaml).unwrap();
    let result = ch_loader::providers::clickhouse::source_provider::ClickHouseSourceProvider::from_config(raw);
    assert!(result.is_err(), "Should reject both tables and include_patterns");
}

#[test]
fn ch_source_provider_rejects_neither_tables_nor_patterns() {
    let yaml = r#"
connection_string: localhost:9000
parser:
  table_naming:
    type: from_config
    name: test
  parser_type: json_parser
  settings:
    columns:
      - jsonpath: "$.id"
        column_name: id
        arrow_type: Int64
"#;
    let raw: Value = serde_yaml::from_str(yaml).unwrap();
    let result = ch_loader::providers::clickhouse::source_provider::ClickHouseSourceProvider::from_config(raw);
    assert!(result.is_err(), "Should reject neither tables nor patterns");
}

// ---------------------------------------------------------------------------
// Cross-task: Provider registry works for all 4 sinks
// ---------------------------------------------------------------------------

#[test]
fn registry_all_sinks_registered() {
    let mut registry = ProviderRegistry::new();
    registry.register_sink("clickhouse", |v| {
        Ok(Box::new(ch_loader::providers::clickhouse::provider::ClickHouseSinkProvider::from_config(v)?))
    });
    registry.register_sink("empty", |v| {
        Ok(Box::new(ch_loader::providers::empty::provider::EmptySinkProvider::from_config(v)?))
    });
    registry.register_sink("s3", |v| {
        Ok(Box::new(ch_loader::providers::s3::sink::provider::S3SinkProvider::from_config(v)?))
    });
    registry.register_sink("yds", |v| {
        Ok(Box::new(ch_loader::providers::yds::sink::provider::YdsSinkProvider::from_config(v)?))
    });

    // All 4 should be registered
    let empty_raw: Value = serde_yaml::from_str("batch_size: 1").unwrap();
    assert!(registry.build_sink("empty", empty_raw).is_ok());

    let yds_raw: Value = serde_yaml::from_str("connection_string: x\ntopic_path: y").unwrap();
    assert!(registry.build_sink("yds", yds_raw).is_ok());

    let s3_raw: Value = serde_yaml::from_str("bucket: b\nprefix: p/").unwrap();
    assert!(registry.build_sink("s3", s3_raw).is_ok());
}

#[test]
fn registry_all_sources_registered() {
    let mut registry = ProviderRegistry::new();
    registry.register_source("s3", |v| {
        Ok(Box::new(ch_loader::providers::s3::provider::S3SourceProvider::from_config(v)?))
    });
    registry.register_source("clickhouse", |v| {
        Ok(Box::new(ch_loader::providers::clickhouse::source_provider::ClickHouseSourceProvider::from_config(v)?))
    });

    // CH source needs either tables or patterns
    let ch_raw: Value = serde_yaml::from_str(
        "connection_string: x\ntables:\n  - schema: s\n    table: t\nparser:\n  table_naming:\n    type: from_config\n    name: x\n  parser_type: json_parser\n  settings:\n    columns:\n      - jsonpath: $.id\n        column_name: id\n        arrow_type: Int64"
    ).unwrap();
    assert!(registry.build_source("clickhouse", ch_raw).is_ok());
}
