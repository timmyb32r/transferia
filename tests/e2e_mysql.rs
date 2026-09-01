#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test assertions intentionally fail fast"
)]

mod support;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, BinaryArray, StringArray, UInt64Array};
use arrow::datatypes::DataType;
use mysql_async::prelude::Queryable as _;
use testcontainers::core::{IntoContainerPort as _, WaitFor};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{GenericImage, ImageExt as _};
use tokio_util::sync::CancellationToken;

use transferia::connectors::mysql::{MySqlConnectionConfig, MySqlSourceConnector};
use transferia::core::data::message::SourceBatch;
use transferia::core::delivery::{DeliveryDiscoveryRequest, SourceTopology};
use transferia::core::memory::PipelineMemory;
use transferia::metrics::MetricsRegistry;
use transferia::registry::{SourceBuildContext, SourceConnector as _, SourceDiscoveryContext};

fn reachable_host(host: &impl ToString) -> String {
    let host = host.to_string();
    if host == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host
    }
}

async fn wait_for_mysql(config: &MySqlConnectionConfig) -> anyhow::Result<mysql_async::Conn> {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match transferia::connectors::mysql::connect(config).await {
                Ok(connection) => return connection,
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("MySQL-compatible testcontainer did not become ready"))
}

async fn assert_mysql_family_source(image: GenericImage, password_env: &str) -> anyhow::Result<()> {
    let container = image
        .with_exposed_port(3306.tcp())
        .with_wait_for(WaitFor::message_on_stderr("ready for connections"))
        .with_env_var(password_env, "test")
        .with_env_var("MYSQL_DATABASE", "transferia")
        .start()
        .await?;
    let host = reachable_host(&container.get_host().await?);
    let port = container.get_host_port_ipv4(3306.tcp()).await?;
    let connection_config = MySqlConnectionConfig {
        host: host.clone(),
        port,
        database: "transferia".to_owned(),
        username: "root".to_owned(),
        password: "test".to_owned(),
        trusted_plaintext: true,
        tls_ca_file: None,
    };
    let mut connection = wait_for_mysql(&connection_config).await?;
    connection
        .query_drop(
            "CREATE TABLE all_types (\
                id BIGINT UNSIGNED PRIMARY KEY,\
                signed_tiny TINYINT, unsigned_tiny TINYINT UNSIGNED,\
                signed_small SMALLINT, unsigned_small SMALLINT UNSIGNED,\
                signed_medium MEDIUMINT, unsigned_medium MEDIUMINT UNSIGNED,\
                signed_int INT, unsigned_int INT UNSIGNED,\
                signed_big BIGINT, unsigned_big BIGINT UNSIGNED,\
                float_value FLOAT, double_value DOUBLE, decimal_value DECIMAL(65, 30),\
                bit_value BIT(64), binary_value VARBINARY(16), blob_value LONGBLOB,\
                char_value CHAR(8), varchar_value VARCHAR(255), text_value LONGTEXT,\
                enum_value ENUM('one', 'two'), set_value SET('red', 'green'), json_value JSON,\
                date_value DATE, time_value TIME(6), datetime_value DATETIME(6),\
                timestamp_value TIMESTAMP(6), year_value YEAR, point_value POINT\
            )",
        )
        .await?;
    connection
        .query_drop(
            "INSERT INTO all_types VALUES (\
                18446744073709551615, -128, 255, -32768, 65535, -8388608, 16777215,\
                -2147483648, 4294967295, -9223372036854775808, 18446744073709551615,\
                1.5, 2.25, 99999999999999999999999999999999999.123456789012345678901234567890,\
                X'FEDCBA9876543210', X'00FF', X'000102FF', 'chars', 'utf8 text',\
                'long text', 'two', 'red,green', JSON_OBJECT('answer', 42),\
                '2024-02-29', '838:59:58.123456', '2024-02-29 12:34:56.123456',\
                '2024-02-29 12:34:56.123456', 2024, ST_GeomFromText('POINT(1 2)')\
            ), (1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,\
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,\
                NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        )
        .await?;
    connection.disconnect().await?;

    let source = MySqlSourceConnector::from_config(
        serde_yaml::from_str(&format!(
            "host: '{host}'\nport: {port}\ndatabase: transferia\nusername: root\npassword: test\ntrusted_plaintext: true\nbatch_rows: 1\ntables:\n  - name: all_types\n"
        ))?,
        Arc::new(MetricsRegistry::new()),
    )?;
    let discovery = source
        .delivery_discovery(SourceDiscoveryContext {
            request: DeliveryDiscoveryRequest {
                keep_system_columns: false,
            },
            cancellation: CancellationToken::new(),
        })
        .await?;
    assert!(matches!(
        discovery.source_topology,
        SourceTopology::StaticPartitions(ref partitions) if partitions == &[0]
    ));
    let dataset = &discovery.datasets[0];
    let id = dataset
        .stored_schema
        .columns
        .iter()
        .find(|column| column.name == "id")
        .unwrap();
    assert_eq!(id.data_type, DataType::UInt64);
    assert!(id.primary_key);
    assert_eq!(
        dataset
            .stored_schema
            .columns
            .iter()
            .find(|column| column.name == "decimal_value")
            .unwrap()
            .data_type,
        DataType::Utf8
    );
    assert_eq!(
        dataset
            .stored_schema
            .columns
            .iter()
            .find(|column| column.name == "point_value")
            .unwrap()
            .data_type,
        DataType::Binary
    );

    let mut actor = source
        .build_source(SourceBuildContext {
            partition_id: 0,
            cancellation: CancellationToken::new(),
            memory: PipelineMemory::new(64 * 1024 * 1024),
            durable: support::durable_context(),
        })
        .await?;
    let mut batches = Vec::new();
    loop {
        match actor.read_batch().await? {
            SourceBatch::Typed { tables, .. } => batches.push(tables[0].batch.clone()),
            SourceBatch::Finished => break,
            other => anyhow::bail!("unexpected MySQL source batch: {other:?}"),
        }
    }
    assert_eq!(batches.iter().map(|batch| batch.num_rows()).sum::<usize>(), 2);
    let populated = batches
        .iter()
        .find(|batch| {
            batch
                .column_by_name("id")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .is_some_and(|column| column.value(0) == u64::MAX)
        })
        .unwrap();
    assert_eq!(
        populated
            .column_by_name("decimal_value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "99999999999999999999999999999999999.123456789012345678901234567890"
    );
    assert_eq!(
        populated
            .column_by_name("time_value")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "838:59:58.123456"
    );
    assert_eq!(
        populated
            .column_by_name("binary_value")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(0),
        &[0, 255]
    );
    let nullable = batches
        .iter()
        .find(|batch| {
            batch
                .column_by_name("id")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .is_some_and(|column| column.value(0) == 1)
        })
        .unwrap();
    assert!(nullable.column_by_name("signed_tiny").unwrap().is_null(0));
    Ok(())
}

#[tokio::test]
async fn mysql_source_reads_all_type_families_losslessly() -> anyhow::Result<()> {
    assert_mysql_family_source(GenericImage::new("mysql", "8.4.6"), "MYSQL_ROOT_PASSWORD").await
}

#[tokio::test]
async fn mariadb_source_reads_all_type_families_losslessly() -> anyhow::Result<()> {
    assert_mysql_family_source(
        GenericImage::new("mariadb", "11.8.3"),
        "MARIADB_ROOT_PASSWORD",
    )
    .await
}
