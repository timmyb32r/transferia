use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use futures_util::future::BoxFuture;
use mysql_async::prelude::Queryable;

use super::config::MySqlSinkConfig;
use super::writer::MySqlSink;
use crate::connectors::mysql::common::{connect, quote_identifier, validate_identifier};
use transferia_core::data::schema::{SchemaColumn, ARROW_JSON_EXTENSION_NAME};
use transferia_core::delivery::{
    validate_stored_projection, ArrowTypeFamily, DeliveryDiscovery, NameSyntax, SinkLimits,
    SinkLimitsDescription, TextLimit,
};
use transferia_core::sink::Sink;
use transferia_core::SystemColumnKind;
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_registry::{SinkBuildContext, SinkConnector, SinkPrepare};

pub struct MySqlSinkConnector {
    config: Arc<MySqlSinkConfig>,
}

impl MySqlSinkConnector {
    pub fn from_config(config: MySqlSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

impl SinkLimits for MySqlSinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        let name = TextLimit {
            syntax: NameSyntax::AnyNonEmptyUtf8,
            max_utf8_bytes: None,
        };
        SinkLimitsDescription {
            sink: "mysql",
            dataset_name: Some(name.clone()),
            column_name: Some(name),
            supported_arrow_types: vec![
                ArrowTypeFamily::Utf8,
                ArrowTypeFamily::Binary,
                ArrowTypeFamily::SignedInteger,
                ArrowTypeFamily::UnsignedInteger,
                ArrowTypeFamily::FloatingPoint,
                ArrowTypeFamily::Decimal,
                ArrowTypeFamily::Boolean,
                ArrowTypeFamily::Date32,
                ArrowTypeFamily::Date64,
                ArrowTypeFamily::Timestamp,
            ],
            object_key: None,
        }
    }

    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        anyhow::ensure!(
            !discovery.datasets.is_empty(),
            "MySQL sink requires at least one dataset"
        );
        let mut names = std::collections::HashSet::new();
        for dataset in &discovery.datasets {
            anyhow::ensure!(
                names.insert(dataset.name.as_ref()),
                "MySQL datasets repeat table '{}'",
                dataset.name
            );
            validate_identifier("table", &dataset.name)?;
            validate_stored_projection(discovery, dataset)?;
            anyhow::ensure!(
                !dataset.stored_schema.columns.is_empty(),
                "MySQL table '{}' cannot have an empty schema",
                dataset.name
            );
            let mut primary_keys = 0_usize;
            for column in &dataset.stored_schema.columns {
                validate_identifier("column", &column.name)?;
                mysql_sql_type(column)?;
                if column.primary_key {
                    primary_keys += 1;
                    anyhow::ensure!(
                        !column.nullable,
                        "MySQL primary-key column '{}.{}' must not be nullable",
                        dataset.name,
                        column.name
                    );
                }
            }
            anyhow::ensure!(
                primary_keys <= 16,
                "MySQL table '{}' has {primary_keys} primary-key columns; the portable limit is 16",
                dataset.name
            );
            if dataset
                .system_columns
                .iter()
                .any(|column| column.kind == SystemColumnKind::ChangeOperation)
            {
                anyhow::ensure!(
                    primary_keys > 0,
                    "MySQL changelog dataset '{}' requires a primary key",
                    dataset.name
                );
            }
        }
        Ok(())
    }
}

impl SinkConnector for MySqlSinkConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::MySqlSink
    }

    fn limits(&self) -> &dyn SinkLimits {
        self.config.as_ref()
    }

    fn destination_type(&self, column: &SchemaColumn) -> anyhow::Result<String> {
        Ok(format!(
            "{} {}",
            mysql_sql_type(column)?,
            if column.nullable { "NULL" } else { "NOT NULL" }
        ))
    }

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let mut connection = connect(&self.config.connection).await?;
            configure_strict_session(&mut connection).await?;
            for dataset in request.datasets {
                if self.config.create_tables {
                    let columns = dataset
                        .schema
                        .columns
                        .iter()
                        .map(|column| {
                            Ok(format!(
                                "{} {}{}",
                                quote_identifier(&column.name),
                                mysql_sql_type(column)?,
                                if column.nullable { "" } else { " NOT NULL" }
                            ))
                        })
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    let primary_key = dataset
                        .schema
                        .columns
                        .iter()
                        .filter(|column| column.primary_key)
                        .map(|column| quote_identifier(&column.name))
                        .collect::<Vec<_>>();
                    let mut definitions = columns;
                    if !primary_key.is_empty() {
                        definitions.push(format!("PRIMARY KEY ({})", primary_key.join(", ")));
                    }
                    connection
                        .query_drop(format!(
                            "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE=InnoDB",
                            quote_identifier(&dataset.table),
                            definitions.join(", ")
                        ))
                        .await?;
                }
                validate_changelog_primary_key(&mut connection, &dataset).await?;
            }
            connection.disconnect().await?;
            Ok(())
        })
    }

    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            let mut connection = connect(&self.config.connection).await?;
            configure_strict_session(&mut connection).await?;
            let limits: Arc<dyn SinkLimits> = Arc::clone(&self.config) as Arc<dyn SinkLimits>;
            Ok(Box::new(MySqlSink::new(
                connection,
                context.counters,
                context.discovery,
                limits,
                self.config.insert_rows,
            )) as Box<dyn Sink>)
        })
    }
}

async fn validate_changelog_primary_key(
    connection: &mut mysql_async::Conn,
    dataset: &transferia_registry::DatasetPrepare,
) -> anyhow::Result<()> {
    if !dataset.changelog {
        return Ok(());
    }
    let actual = connection
        .exec_map(
            "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY' \
             ORDER BY ORDINAL_POSITION",
            (dataset.table.as_ref(),),
            |name: String| name,
        )
        .await?;
    let expected = dataset
        .schema
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    anyhow::ensure!(
        actual.iter().map(String::as_str).collect::<Vec<_>>() == expected,
        "MySQL changelog table '{}' has primary key {actual:?}, expected {expected:?}",
        dataset.table
    );
    Ok(())
}

pub(super) async fn configure_strict_session(
    connection: &mut mysql_async::Conn,
) -> anyhow::Result<()> {
    connection
        .query_drop(
            "SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO'",
        )
        .await?;
    connection
        .query_drop("SET SESSION time_zone = '+00:00'")
        .await?;
    Ok(())
}

pub(super) fn mysql_sql_type(column: &SchemaColumn) -> anyhow::Result<String> {
    if column.arrow_extension_name == Some(ARROW_JSON_EXTENSION_NAME) {
        anyhow::ensure!(
            column.data_type == DataType::Utf8,
            "MySQL JSON extension requires Arrow Utf8, got {:?}",
            column.data_type
        );
        return Ok("JSON".to_owned());
    }
    Ok(match &column.data_type {
        DataType::Boolean => "BOOLEAN".to_owned(),
        DataType::Int8 => "TINYINT".to_owned(),
        DataType::UInt8 => "TINYINT UNSIGNED".to_owned(),
        DataType::Int16 => "SMALLINT".to_owned(),
        DataType::UInt16 => "SMALLINT UNSIGNED".to_owned(),
        DataType::Int32 => "INT".to_owned(),
        DataType::UInt32 => "INT UNSIGNED".to_owned(),
        DataType::Int64 => "BIGINT".to_owned(),
        DataType::UInt64 => "BIGINT UNSIGNED".to_owned(),
        DataType::Float32 => "FLOAT".to_owned(),
        DataType::Float64 => "DOUBLE".to_owned(),
        DataType::Utf8 if column.primary_key => {
            let max_length = column.max_length.ok_or_else(|| {
                anyhow::anyhow!(
                    "MySQL Utf8 primary-key column '{}' requires max_length so key size can be validated losslessly",
                    column.name
                )
            })?;
            anyhow::ensure!(
                max_length <= 768,
                "MySQL Utf8 primary-key column '{}' max_length {max_length} exceeds the portable utf8mb4 key limit 768",
                column.name
            );
            format!("VARCHAR({max_length})")
        }
        DataType::Utf8 => "LONGTEXT".to_owned(),
        DataType::Binary if column.primary_key => {
            let max_length = column.max_length.ok_or_else(|| {
                anyhow::anyhow!(
                    "MySQL Binary primary-key column '{}' requires max_length so key size can be validated losslessly",
                    column.name
                )
            })?;
            anyhow::ensure!(
                max_length <= 3_072,
                "MySQL Binary primary-key column '{}' max_length {max_length} exceeds the portable key limit 3072",
                column.name
            );
            format!("VARBINARY({max_length})")
        }
        DataType::Binary => "LONGBLOB".to_owned(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            decimal_sql_type(*precision, *scale)?
        }
        DataType::Date32 => "DATE".to_owned(),
        DataType::Date64 | DataType::Timestamp(TimeUnit::Millisecond, None) => {
            "DATETIME(3)".to_owned()
        }
        DataType::Timestamp(TimeUnit::Second, None) => "DATETIME".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond | TimeUnit::Nanosecond, None) => {
            "DATETIME(6)".to_owned()
        }
        DataType::Timestamp(_, Some(timezone)) => anyhow::bail!(
            "MySQL has no timestamp type that preserves Arrow timezone '{timezone}'; explicitly transform it before this sink"
        ),
        data_type => anyhow::bail!("unsupported Arrow type {data_type:?} for MySQL sink"),
    })
}

pub(super) fn decimal_sql_type(precision: u8, scale: i8) -> anyhow::Result<String> {
    let integer_digits = if scale < 0 {
        u16::from(precision) + u16::from(scale.unsigned_abs())
    } else {
        u16::from(precision)
    };
    let mysql_scale = u8::try_from(scale.max(0))?;
    anyhow::ensure!(
        integer_digits <= 65 && mysql_scale <= 30,
        "MySQL DECIMAL cannot preserve precision {precision}, scale {scale}; maximum precision is 65 and scale is 30"
    );
    Ok(format!("DECIMAL({integer_digits},{mysql_scale})"))
}
