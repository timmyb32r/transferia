#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

use arrow::array::{
    Array as _, BinaryArray, Decimal256Array, Int16Array, Int64Array, ListArray,
    StringArray, StructArray, TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use testcontainers::core::wait::HttpWaitStrategy;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio_util::sync::CancellationToken;

use transferia_connector_clickhouse::clickhouse::ClickHouseSourceConnector;
use transferia_core::data::message::SourceBatch;
use transferia_core::delivery::{DeliveryDiscoveryRequest, SchemaOrigin};
use transferia_core::memory::PipelineMemory;
use transferia_core::source::Source as _;
use transferia_delivery_contracts::metrics::MetricsRegistry;
use transferia_registry::{SourceBuildContext, SourceConnector as _, SourceDiscoveryContext};

const IMAGE: &str = "clickhouse/clickhouse-server";
const TAG: &str = "25.8.28.1";

#[tokio::test]
async fn native_table_preview_is_bounded_lossless_with_quoted_names() -> anyhow::Result<()> {
    let (_container, host, native_port, http_port) = type_fixture().await?;
    let http = reqwest::Client::new();
    let url = format!("http://{host}:{http_port}/");
    execute_fixture_query(&http, &url, "CREATE TABLE default.`events\\`quoted` (id Int64, amount Decimal(38,4), at DateTime64(6,'UTC'), payload String, status Enum8('open'=1,'closed'=2)) ENGINE=Memory").await?;
    execute_fixture_query(&http, &url, "INSERT INTO default.`events\\`quoted` SELECT 9007199254740993, toDecimal128('12345678901234567890.1234',4), toDateTime64('2024-01-01 00:00:00.123456',6,'UTC'), unhex('00FF'), 'open' FROM numbers(3)").await?;
    execute_fixture_query(&http, &url, "CREATE USER sample_reader IDENTIFIED WITH plaintext_password BY 'read-only-test'").await?;
    execute_fixture_query(&http, &url, "GRANT SELECT ON default.`events\\`quoted` TO sample_reader").await?;
    let mut builder = transferia_registry::RegistryBuilder::new();
    transferia_connector_clickhouse::register(&mut builder, &Arc::new(MetricsRegistry::new()))?;
    let registry = builder.build();
    let config = serde_yaml::to_value(serde_json::json!({
        "hosts": [host], "port":native_port, "http_port":http_port, "username":"sample_reader", "password":"read-only-test",
        "trusted_plaintext":true, "tables":{"type":"all"}
    }))?;
    let sample = registry.sample_source_table("clickhouse", config.clone(), transferia_registry::TableIdentity {
        namespace:"default".into(), name:"events`quoted".into(),
    }, transferia_registry::TableSampleLimits { row_limit: 1, max_bytes: 16 * 1024 * 1024, timeout_ms: 30_000 }, CancellationToken::new()).await?;
    assert_eq!(sample.namespace.as_deref(), Some("default"));
    assert_eq!(sample.table.as_ref(), "events`quoted");
    assert_eq!(sample.batch.num_rows(), 1);
    assert_eq!(sample.batch.num_columns(), 5);
    assert_eq!(sample.batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0), 9_007_199_254_740_993);
    assert_eq!(sample.batch.column(1).as_any().downcast_ref::<arrow::array::Decimal128Array>().unwrap().value(0), 123_456_789_012_345_678_901_234);
    assert_eq!(sample.batch.column(2).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(0), 1_704_067_200_123_456);
    assert_eq!(sample.batch.column(3).as_any().downcast_ref::<BinaryArray>().unwrap().value(0), &[0,255]);
    assert_eq!(sample.batch.column(4).data_type(), &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)));
    let statuses = arrow::compute::cast(sample.batch.column(4), &DataType::Utf8)?;
    assert_eq!(statuses.as_any().downcast_ref::<StringArray>().unwrap().value(0), "open");
    let count = http.post(&url).body("SELECT count(*) FROM default.`events\\`quoted`").send().await?.error_for_status()?.text().await?;
    assert_eq!(count.trim(), "3");
    execute_fixture_query(&http, &url, "CREATE TABLE default.wide_sample (payload String) ENGINE=Memory").await?;
    execute_fixture_query(&http, &url, "INSERT INTO default.wide_sample VALUES (repeat('a',65536))").await?;
    execute_fixture_query(&http, &url, "GRANT SELECT ON default.wide_sample TO sample_reader").await?;
    let error = registry.sample_source_table("clickhouse", config, transferia_registry::TableIdentity {
        namespace: "default".into(), name: "wide_sample".into(),
    }, transferia_registry::TableSampleLimits { row_limit: 1, max_bytes: 4096, timeout_ms: 30_000 }, CancellationToken::new()).await.unwrap_err();
    assert!(format!("{error:#}").contains("max_sample_bytes"), "{error:#}");
    Ok(())
}

fn reachable_host(host: &impl ToString) -> String {
    let host = host.to_string();
    if host == "localhost" {
        "127.0.0.1".into()
    } else {
        host
    }
}

async fn type_fixture() -> anyhow::Result<(testcontainers::ContainerAsync<GenericImage>, String, u16, u16)> {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(9000.tcp())
        .with_exposed_port(8123.tcp())
        .with_wait_for(WaitFor::http(HttpWaitStrategy::new("/ping")
            .with_port(8123.tcp()).with_expected_status_code(200_u16)))
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .start().await?;
    let host = reachable_host(&container.get_host().await?);
    let native_port = container.get_host_port_ipv4(9000.tcp()).await?;
    let http_port = container.get_host_port_ipv4(8123.tcp()).await?;
    Ok((container, host, native_port, http_port))
}

async fn execute_fixture_query(http: &reqwest::Client, url: &str, query: &str) -> anyhow::Result<()> {
    let response = http.post(url).body(query.to_owned()).send().await?;
    let status = response.status();
    let body = response.text().await?;
    anyhow::ensure!(status.is_success(), "ClickHouse fixture setup failed: {status}: {body}");
    Ok(())
}

fn type_connector(host: &str, native_port: u16, http_port: u16, reader: &str, table: &str, policy: &str)
    -> anyhow::Result<ClickHouseSourceConnector> {
    let snapshot_reader = if reader == "native" {
        "{ type: native, max_threads: 1, compression: lz4 }"
    } else {
        "{ type: parquet, max_threads: 1, row_group_rows: 32, decode_threads: 1 }"
    };
    let config = serde_yaml::from_str(&format!(
        "hosts: ['{host}']\nport: {native_port}\nhttp_port: {http_port}\ntrusted_plaintext: true\nusername: default\nbatch_rows: 64\nsnapshot_reader: {snapshot_reader}\n{policy}tables:\n  type: selected\n  rules:\n    - include: default.{table}\n",
    ))?;
    ClickHouseSourceConnector::from_config(config, Arc::new(MetricsRegistry::new()))
}

async fn discover_types(connector: &ClickHouseSourceConnector) -> anyhow::Result<transferia_core::delivery::DeliveryDiscovery> {
    connector.delivery_discovery(SourceDiscoveryContext {
        request: DeliveryDiscoveryRequest { keep_system_columns: false },
        cancellation: CancellationToken::new(),
        delivery_type: transferia_delivery_contracts::DeliveryType::Batch,
    }).await
}

async fn build_type_snapshot(connector: &ClickHouseSourceConnector) -> anyhow::Result<Box<dyn transferia_core::source::Source>> {
    connector.build_source(SourceBuildContext {
        partition_id: 0,
        delivery_type: transferia_delivery_contracts::DeliveryType::Batch,
        phase: transferia_registry::SourcePhase::Snapshot,
        replay_identity: None,
        cancellation: CancellationToken::new(),
        memory: PipelineMemory::new(256 * 1024 * 1024),
        durable: transferia_test_support::durable_context(),
    }).await
}

async fn read_type_snapshot(connector: &ClickHouseSourceConnector) -> anyhow::Result<RecordBatch> {
    let mut source = build_type_snapshot(connector).await?;
    let mut batches = Vec::new();
    loop {
        match source.read_batch().await? {
            SourceBatch::Typed { tables, .. } => {
                assert_eq!(tables.len(), 1);
                batches.push(tables.into_iter().next().unwrap().batch);
            }
            SourceBatch::Finished => break,
            _ => anyhow::bail!("expected typed ClickHouse source output"),
        }
    }
    let schema = batches.first().expect("fixture has rows").schema();
    Ok(arrow::compute::concat_batches(&schema, &batches)?)
}

#[tokio::test]
async fn quoted_source_identifiers_are_preserved_by_both_readers() -> anyhow::Result<()> {
    let quote_identifier = |name: &str| format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"));
    let (_container, host, native_port, http_port) = type_fixture().await?;
    let http = reqwest::Client::new();
    let url = format!("http://{host}:{http_port}");
    let names = ["entries.query_id", "ключ, id", "space name", "a`b", "a\\b", "select", "1st", "a\"b", "x` FROM nope --"];
    let declarations = names.iter().map(|name| format!("{} Int64", quote_identifier(name))).collect::<Vec<_>>().join(", ");
    execute_fixture_query(&http, &url, &format!(
        "CREATE TABLE quoted_events ({declarations}) ENGINE=MergeTree ORDER BY {}", quote_identifier(names[1]),
    )).await?;
    execute_fixture_query(&http, &url, "INSERT INTO quoted_events VALUES (100,101,102,103,104,105,106,107,108)").await?;
    execute_fixture_query(&http, &url,
        "CREATE TABLE nested_events (id Int64, entries Nested(query_id String, count UInt64)) ENGINE=MergeTree ORDER BY id",
    ).await?;
    execute_fixture_query(&http, &url, "INSERT INTO nested_events VALUES (1, ['q1','q2'], [2,3])").await?;
    for reader in ["native", "parquet"] {
        let connector = type_connector(&host, native_port, http_port, reader, "quoted_events", "")?;
        let discovery = discover_types(&connector).await?;
        let columns = &discovery.datasets[0].stored_schema.columns;
        assert_eq!(columns.iter().map(|column| column.name.as_str()).collect::<Vec<_>>(), names, "{reader}");
        assert!(columns[1].primary_key, "{reader}");
        let batch = read_type_snapshot(&connector).await?;
        assert_eq!(batch.num_rows(), 1);
        for (index, name) in names.iter().enumerate() {
            let values = batch.column_by_name(name).unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
            assert_eq!(values.value(0), 100 + i64::try_from(index)?, "{reader}: {name}");
        }
        let connector = type_connector(&host, native_port, http_port, reader, "nested_events", "")?;
        let discovery = discover_types(&connector).await?;
        assert!(discovery.datasets[0].stored_schema.columns.iter().any(|column| column.name == "entries.query_id"));
        let batch = read_type_snapshot(&connector).await?;
        let entries = batch.column_by_name("entries.query_id").unwrap().as_any().downcast_ref::<ListArray>().unwrap();
        let values = entries.value(0);
        let values = values.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(b"q1".as_slice()), Some(b"q2".as_slice())]);
    }
    Ok(())
}

#[tokio::test]
async fn readable_generated_columns_work_with_both_source_readers() -> anyhow::Result<()> {
    let (_container, host, native_port, http_port) = type_fixture().await?;
    let http = reqwest::Client::new();
    let url = format!("http://{host}:{http_port}");
    execute_fixture_query(&http, &url,
        "CREATE TABLE generated_events (id Int64, databases Array(String) DEFAULT ['db1'], computed Int64 MATERIALIZED id * 2, displayed Int64 ALIAS computed + 1) ENGINE=MergeTree ORDER BY id",
    ).await?;
    execute_fixture_query(&http, &url, "INSERT INTO generated_events (id) VALUES (1)").await?;
    execute_fixture_query(&http, &url,
        "INSERT INTO generated_events (id, databases) VALUES (2, ['explicit', 'db2'])",
    ).await?;
    for reader in ["native", "parquet"] {
        let connector = type_connector(&host, native_port, http_port, reader, "generated_events", "")?;
        let discovery = discover_types(&connector).await?;
        assert_eq!(discovery.datasets[0].stored_schema.columns.len(), 4, "{reader}");
        let batch = read_type_snapshot(&connector).await?;
        let ids = batch.column_by_name("id").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        let computed = batch.column_by_name("computed").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        let displayed = batch.column_by_name("displayed").unwrap().as_any().downcast_ref::<Int64Array>().unwrap();
        let databases = batch.column_by_name("databases").unwrap().as_any().downcast_ref::<ListArray>().unwrap();
        let mut seen = BTreeSet::new();
        for row in 0..batch.num_rows() {
            let id = ids.value(row);
            assert!(seen.insert(id), "duplicate id in {reader}");
            assert_eq!(computed.value(row), id * 2, "{reader}");
            assert_eq!(displayed.value(row), id * 2 + 1, "{reader}");
            let values = databases.value(row);
            let values = values.as_any().downcast_ref::<BinaryArray>().unwrap();
            let expected: Vec<&[u8]> = if id == 1 { vec![b"db1"] } else { vec![b"explicit", b"db2"] };
            assert_eq!(values.iter().collect::<Vec<_>>(), expected.into_iter().map(Some).collect::<Vec<_>>(), "{reader}");
        }
        assert_eq!(seen, BTreeSet::from([1, 2]), "{reader}");
    }
    Ok(())
}

#[tokio::test]
async fn ephemeral_columns_are_rejected_before_snapshot_for_both_readers() -> anyhow::Result<()> {
    let (_container, host, native_port, http_port) = type_fixture().await?;
    let http = reqwest::Client::new();
    execute_fixture_query(&http, &format!("http://{host}:{http_port}"),
        "CREATE TABLE ephemeral_events (id Int64, input_only String EPHEMERAL, stored String DEFAULT input_only) ENGINE=MergeTree ORDER BY id",
    ).await?;
    for reader in ["native", "parquet"] {
        let connector = type_connector(&host, native_port, http_port, reader, "ephemeral_events", "")?;
        let error = discover_types(&connector).await.unwrap_err().to_string();
        for expected in ["ephemeral_events", "input_only", "EPHEMERAL", "cannot be read"] {
            assert!(error.contains(expected), "{reader}: {error}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn both_source_readers_preserve_full_enum_and_nested_numeric_types() -> anyhow::Result<()> {
    let (_container, host, native_port, http_port) = type_fixture().await?;
    let http = reqwest::Client::new();
    let url = format!("http://{host}:{http_port}");
    let label = |code: i16| match code { 0 => "NO".to_owned(), 1 => "YES".to_owned(), _ => format!("label_{code}") };
    let labels = (-128_i16..=127).map(|code| format!("'{}' = {code}", label(code))).collect::<Vec<_>>().join(", ");
    let enum8 = format!("Enum8({labels})");
    let enum16 = "Enum16('negative' = -32768, 'positive' = 32767)";
    execute_fixture_query(&http, &url, &format!(
        "CREATE TABLE typed_events (id Int64, status {enum8}, wide_status {enum16}, nested Tuple(code Int16, items Array(Tuple(label String, values Array(Nullable(Int64))))), category LowCardinality(Nullable(String)), amount Decimal256(12), event_time DateTime64(5, 'UTC')) ENGINE=MergeTree ORDER BY id",
    )).await?;
    execute_fixture_query(&http, &url, &format!(
        "INSERT INTO typed_events SELECT toInt64(number), CAST(toInt8(toInt16(number) - 128) AS {enum8}), CAST(toInt16(if(number % 2 = 0, -32768, 32767)) AS {enum16}), tuple(toInt16(-7), [tuple('nested', [toNullable(toInt64(1)), CAST(NULL AS Nullable(Int64)), toNullable(toInt64(-2))])]), if(number % 3 = 0, NULL, if(number % 3 = 1, '', 'value')), toDecimal256('1234567890123456789012345678901234567890.123456789012', 12), toDateTime64(if(number % 2 = 0, '1970-01-01 00:00:01.23456', '1969-12-31 23:59:58.76544'), 5, 'UTC') FROM numbers(256)",
    )).await?;

    for reader in ["native", "parquet"] {
        let connector = type_connector(&host, native_port, http_port, reader, "typed_events", "")?;
        let discovery = discover_types(&connector).await?;
        assert_eq!(discovery.datasets[0].stored_schema.columns.len(), 7);
        let batch = read_type_snapshot(&connector).await?;
        assert_eq!(batch.num_rows(), 256, "{reader}");
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let statuses = arrow::compute::cast(batch.column(1), &DataType::Utf8)?;
        let statuses = statuses.as_any().downcast_ref::<StringArray>().unwrap();
        let wide = arrow::compute::cast(batch.column(2), &DataType::Utf8)?;
        let wide = wide.as_any().downcast_ref::<StringArray>().unwrap();
        let nested = batch.column(3).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(nested.fields()[0].name(), "code");
        assert_eq!(nested.fields()[1].name(), "items");
        let codes = nested.column(0).as_any().downcast_ref::<Int16Array>().unwrap();
        let items = nested.column(1).as_any().downcast_ref::<ListArray>().unwrap();
        let categories = arrow::compute::cast(batch.column(4), &DataType::Binary)?;
        let categories = categories.as_any().downcast_ref::<BinaryArray>().unwrap();
        let amounts = batch.column(5).as_any().downcast_ref::<Decimal256Array>().unwrap();
        assert_eq!(amounts.data_type(), &DataType::Decimal256(76, 12));
        let expected_amount = arrow::datatypes::i256::from_string("1234567890123456789012345678901234567890123456789012").unwrap();
        let times = batch.column(6).as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        assert_eq!(times.data_type(), &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())));
        let mut seen_ids = BTreeSet::new();
        for row in 0..256 {
            assert!(!ids.is_null(row));
            let id = ids.value(row);
            assert!(seen_ids.insert(id), "{reader}: duplicate id {id}");
            assert_eq!(statuses.value(row), label(i16::try_from(id)? - 128), "{reader}");
            assert_eq!(wide.value(row), if id % 2 == 0 { "negative" } else { "positive" });
            assert_eq!(codes.value(row), -7);
            let item = items.value(row);
            let item = item.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(item.fields()[0].name(), "label");
            assert_eq!(item.fields()[1].name(), "values");
            assert_eq!(item.column(0).as_any().downcast_ref::<BinaryArray>().unwrap().value(0), b"nested");
            let values = item.column(1).as_any().downcast_ref::<ListArray>().unwrap().value(0);
            assert_eq!(values.as_any().downcast_ref::<Int64Array>().unwrap(), &Int64Array::from(vec![Some(1), None, Some(-2)]));
            if id % 3 == 0 { assert!(categories.is_null(row)); }
            else { assert_eq!(categories.value(row), if id % 3 == 1 { b"".as_slice() } else { b"value".as_slice() }); }
            assert_eq!(amounts.value(row), expected_amount);
            assert_eq!(times.value(row), if id % 2 == 0 { 1_234_560 } else { -1_234_560 });
        }
        assert_eq!(seen_ids, (0_i64..256).collect(), "{reader}");
    }
    Ok(())
}

#[tokio::test]
async fn unsupported_dynamic_type_defaults_to_string_and_allows_explicit_fail_for_both_readers() -> anyhow::Result<()> {
    let (_container, host, native_port, http_port) = type_fixture().await?;
    let http = reqwest::Client::new();
    let url = format!("http://{host}:{http_port}");
    for query in [
        "CREATE TABLE unsupported_events (id Int64, payload Dynamic) ENGINE=MergeTree ORDER BY id",
        "INSERT INTO unsupported_events SELECT 1, toUInt64('18446744073709551615')",
        "INSERT INTO unsupported_events SELECT 2, 'unchanged text'",
        "INSERT INTO unsupported_events SELECT 3, NULL",
    ] { execute_fixture_query(&http, &url, query).await?; }
    for reader in ["native", "parquet"] {
        let strict = type_connector(&host, native_port, http_port, reader, "unsupported_events", "unsupported_types: fail\n")?;
        let error = discover_types(&strict).await.unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("payload") && message.contains("Dynamic") && message.contains("to_string"), "{message}");
        let converted = type_connector(&host, native_port, http_port, reader, "unsupported_events", "")?;
        let discovery = discover_types(&converted).await?;
        let payload = &discovery.datasets[0].stored_schema.columns[1];
        assert_eq!(payload.data_type, DataType::Utf8);
        assert!(payload.nullable);
        assert!(payload.arrow_extension_metadata.as_deref().unwrap().contains("Dynamic"));
        let batch = read_type_snapshot(&converted).await?;
        assert_eq!(batch.num_rows(), 3, "{reader}");
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let payloads = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let mut actual = BTreeMap::new();
        for row in 0..batch.num_rows() {
            assert!(!ids.is_null(row));
            let id = ids.value(row);
            let payload = payloads.is_valid(row).then(|| payloads.value(row));
            assert!(actual.insert(id, payload).is_none(), "{reader}: duplicate id {id}");
        }
        assert_eq!(actual, BTreeMap::from([
            (1, Some("18446744073709551615")), (2, Some("unchanged text")), (3, None),
        ]), "{reader}");
    }
    Ok(())
}

#[tokio::test]
async fn changed_enum_declaration_after_discovery_fails_before_any_source_batch() -> anyhow::Result<()> {
    let (_container, host, native_port, http_port) = type_fixture().await?;
    let http = reqwest::Client::new();
    let url = format!("http://{host}:{http_port}");
    execute_fixture_query(&http, &url, "CREATE TABLE drift_events (id Int64, status Enum8('a' = 1, 'b' = 2)) ENGINE=MergeTree ORDER BY id").await?;
    execute_fixture_query(&http, &url, "INSERT INTO drift_events VALUES (1, 'a')").await?;
    let mut connectors = Vec::new();
    for reader in ["native", "parquet"] {
        let connector = type_connector(&host, native_port, http_port, reader, "drift_events", "")?;
        discover_types(&connector).await?;
        connectors.push(connector);
    }
    execute_fixture_query(&http, &url, "ALTER TABLE drift_events MODIFY COLUMN status Enum8('a' = 1, 'b' = 2, 'c' = 3)").await?;
    for connector in connectors {
        let error = match build_type_snapshot(&connector).await {
            Err(error) => error,
            Ok(mut source) => match source.read_batch().await {
                Err(error) => anyhow::anyhow!(error),
                Ok(_) => anyhow::bail!("source emitted a batch after its enum definition changed"),
            },
        };
        assert!(format!("{error:#}").contains("drift"), "{error:#}");
    }
    Ok(())
}

#[tokio::test]
async fn clickhouse_source_discovers_and_streams_a_deterministic_native_snapshot(
) -> anyhow::Result<()> {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(9000.tcp())
        .with_exposed_port(8123.tcp())
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/ping")
                .with_port(8123.tcp())
                .with_expected_status_code(200_u16),
        ))
        .with_env_var("CLICKHOUSE_SKIP_USER_SETUP", "1")
        .start()
        .await?;
    let host = reachable_host(&container.get_host().await?);
    let native_port = container.get_host_port_ipv4(9000.tcp()).await?;
    let http_port = container.get_host_port_ipv4(8123.tcp()).await?;
    let http = reqwest::Client::new();
    for query in [
        "CREATE TABLE events (id Int64, payload Nullable(String)) ENGINE=MergeTree ORDER BY id",
        "INSERT INTO events VALUES (2, NULL), (1, 'one'), (3, 'three')",
    ] {
        http.post(format!("http://{host}:{http_port}"))
            .body(query)
            .send()
            .await?
            .error_for_status()?;
    }

    let config: transferia_connector_clickhouse::clickhouse::src_batch::ClickHouseSourceConfig =
        serde_yaml::from_str(&format!("hosts: ['{host}']\nport: {native_port}\ntrusted_plaintext: true\nusername: default\nbatch_rows: 2\nsnapshot_reader: {{ type: native, max_threads: 1, compression: lz4 }}\ntables:\n  type: selected\n  rules:\n    - include: default.events\n"))?;
    assert!(config.hide_system_tables);
    let checked = ClickHouseSourceConnector::check_connection(
        config.clone(),
        Arc::new(MetricsRegistry::new()),
    )
    .await?;
    let tables = checked
        .tables
        .expect("authenticated complete table catalog");
    assert!(tables
        .iter()
        .any(|table| table.namespace == "system" && table.name == "tables"));
    assert!(tables
        .iter()
        .any(|table| table.namespace == "default" && table.name == "events"));
    let connector =
        ClickHouseSourceConnector::from_config(config, Arc::new(MetricsRegistry::new()))?;
    let discovery = connector
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: CancellationToken::new(),
            delivery_type: transferia_delivery_contracts::DeliveryType::Batch,
        })
        .await?;
    assert_eq!(discovery.schema_origin, SchemaOrigin::SourceNative);
    assert_eq!(discovery.datasets[0].stored_schema.columns.len(), 2);

    let mut source = connector
        .build_source(SourceBuildContext {
            partition_id: 0,
            delivery_type: transferia_delivery_contracts::DeliveryType::Batch,
            phase: transferia_registry::SourcePhase::Snapshot,
            replay_identity: None,
            cancellation: CancellationToken::new(),
            memory: PipelineMemory::new(256 * 1024 * 1024),
            durable: transferia_test_support::durable_context(),
        })
        .await?;
    let first = source.read_batch().await?;
    let second = source.read_batch().await?;
    assert!(matches!(source.read_batch().await?, SourceBatch::Finished));

    let batches = [first, second]
        .into_iter()
        .map(|source| match source {
            SourceBatch::Typed { tables, .. } => {
                Ok(tables.into_iter().next().expect("one table").batch)
            }
            SourceBatch::Dataset { .. } | SourceBatch::Raw { .. } | SourceBatch::Finished => {
                anyhow::bail!("expected typed ClickHouse source batch")
            }
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let ids = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, [1, 2, 3]);
    let values = batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            (0..array.len())
                .map(|row| {
                    if array.is_null(row) {
                        None
                    } else {
                        Some(array.value(row).to_vec())
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        [Some(b"one".to_vec()), None, Some(b"three".to_vec())]
    );
    Ok(())
}
