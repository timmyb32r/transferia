use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, TimeUnit};
use futures_util::future::BoxFuture;
use mysql_async::prelude::Queryable;

use super::config::MySqlSinkConfig;
use super::writer::MySqlSink;
use crate::connectors::mysql::common::{connect, quote_identifier, validate_identifier};
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};
use transferia_core::delivery::{
    validate_stored_projection, ArrowTypeFamily, DeliveryDiscovery, NameSyntax, SinkLimits,
    SinkLimitsDescription, TextLimit,
};
use transferia_core::sink::Sink;
use transferia_core::SystemColumnKind;
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_registry::{
    SinkBuildContext, SinkConnector, SinkPrepare, SinkSpeedtestIsolation,
    SinkSpeedtestIsolationSafety, SpeedtestPhysicalTarget,
};

pub struct MySqlSinkConnector {
    config: Arc<MySqlSinkConfig>,
    speedtest_scope: Option<Arc<MySqlSpeedtestScope>>,
}

pub(super) struct MySqlSpeedtestScope {
    pub(super) database: Arc<str>,

    pub(super) owner_marker: Arc<str>,

    pub(super) tables: BTreeSet<Arc<str>>,

    pub(super) schemas: BTreeMap<Arc<str>, DatasetSchema>,

    pub(super) physical_targets: BTreeSet<(Arc<str>, Arc<str>)>,

    pub(super) attempted_tables: Mutex<BTreeSet<Arc<str>>>,

    pub(super) claimed_tables: Mutex<BTreeSet<Arc<str>>>,
}

impl MySqlSpeedtestScope {
    pub(super) fn record_attempt(&self, table: Arc<str>) {
        self.attempted_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(table);
    }

    pub(super) fn attempted_tables(&self) -> BTreeSet<Arc<str>> {
        self.attempted_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(super) fn claim(&self, table: Arc<str>) {
        self.claimed_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(table);
    }

    pub(super) fn claimed_tables(&self) -> BTreeSet<Arc<str>> {
        self.claimed_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub(super) fn unclaim(&self, table: &str) {
        self.claimed_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(table);
        self.attempted_tables
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(table);
    }
}

impl MySqlSinkConnector {
    pub fn from_config(config: MySqlSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config: Arc::new(config),
            speedtest_scope: None,
        })
    }

    async fn sink_connection(&self) -> anyhow::Result<mysql_async::Conn> {
        let mut connection =
            observe_external_request("mysql", "connect_sink", connect(&self.config.connection))
                .await?;
        configure_strict_session(&mut connection).await?;
        if let Some(scope) = &self.speedtest_scope {
            let database = observe_external_request(
                "mysql",
                "speedtest_resolve_database",
                connection.query_first::<String, _>("SELECT DATABASE()"),
            )
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("MySQL speedtest connection has no selected database")
            })?;
            anyhow::ensure!(
                database == scope.database.as_ref(),
                "MySQL speedtest connection resolved database '{database}', expected '{}'",
                scope.database
            );
        }
        Ok(connection)
    }

    async fn prepare_speedtest(
        &self,
        connection: &mut mysql_async::Conn,
        request: SinkPrepare,
        scope: &MySqlSpeedtestScope,
    ) -> anyhow::Result<()> {
        let requested = request
            .datasets
            .iter()
            .map(|dataset| Arc::clone(&dataset.table))
            .collect::<BTreeSet<_>>();
        anyhow::ensure!(
            requested == scope.tables,
            "MySQL speedtest preparation does not match its connector-owned scratch set"
        );
        for dataset in request.datasets {
            scope.record_attempt(Arc::clone(&dataset.table));
            let create_result = create_owned_mysql_table(connection, scope, &dataset).await;
            let mut verified =
                verify_owned_mysql_table(connection, scope, &dataset.table, &dataset.schema).await;
            if create_result.is_err() || verified.is_err() {
                let mut verifier = self.sink_connection().await?;
                verified =
                    verify_owned_mysql_table(&mut verifier, scope, &dataset.table, &dataset.schema)
                        .await;
                drop(
                    observe_external_request(
                        "mysql",
                        "speedtest_disconnect_verifier",
                        verifier.disconnect(),
                    )
                    .await,
                );
            }
            if verified.is_err() {
                anyhow::bail!(
                    "MySQL speedtest could not prove exclusive ownership of scratch table '{}' after CREATE",
                    dataset.table
                );
            }
            scope.claim(Arc::clone(&dataset.table));
        }
        Ok(())
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
            let mut connection = self.sink_connection().await?;
            if let Some(scope) = &self.speedtest_scope {
                return self
                    .prepare_speedtest(&mut connection, request, scope)
                    .await;
            }
            for dataset in request.datasets {
                if self.config.create_tables {
                    connection
                        .query_drop(format!(
                            "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE=InnoDB",
                            quote_identifier(&dataset.table),
                            mysql_table_definitions(&dataset)?
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
            let mut connection = self.sink_connection().await?;
            if let Some(scope) = &self.speedtest_scope {
                verify_all_mysql_tables(&mut connection, scope, context.discovery.as_ref()).await?;
            }
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

    fn isolate_speedtest(
        self: Arc<Self>,
        discovery: Arc<DeliveryDiscovery>,
        isolation_id: String,
    ) -> BoxFuture<'static, anyhow::Result<SinkSpeedtestIsolation>> {
        Box::pin(async move {
            let (isolated_discovery, table_names, tables) =
                isolate_discovery(discovery.as_ref(), &isolation_id)?;
            let physical_targets = discovery
                .datasets
                .iter()
                .map(|dataset| {
                    let scratch = table_names.get(&dataset.name).ok_or_else(|| {
                        anyhow::anyhow!(
                            "MySQL speedtest omitted dataset '{}' from its scratch mapping",
                            dataset.name
                        )
                    })?;
                    Ok(SpeedtestPhysicalTarget {
                        production: mysql_physical_target(
                            &self.config.connection.database,
                            &dataset.name,
                        ),
                        scratch: mysql_physical_target(&self.config.connection.database, scratch),
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let scope = Arc::new(MySqlSpeedtestScope {
                database: Arc::from(self.config.connection.database.as_str()),
                owner_marker: random_owner_marker()?,
                tables,
                schemas: isolated_discovery
                    .datasets
                    .iter()
                    .map(|dataset| (Arc::clone(&dataset.name), dataset.stored_schema.clone()))
                    .collect(),
                physical_targets: physical_target_set(&physical_targets),
                attempted_tables: Mutex::new(BTreeSet::new()),
                claimed_tables: Mutex::new(BTreeSet::new()),
            });
            let mut isolated_config = self.config.as_ref().clone();
            isolated_config.create_tables = true;
            let connector: Arc<dyn SinkConnector> = Arc::new(Self {
                config: Arc::new(isolated_config),
                speedtest_scope: Some(scope),
            });
            SinkSpeedtestIsolation::scratch(
                connector,
                discovery.as_ref(),
                isolated_discovery,
                table_names,
                physical_targets,
            )
        })
    }

    fn cleanup_speedtest<'a>(
        &'a self,
        isolation: &'a SinkSpeedtestIsolation,
    ) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let scope = self.speedtest_scope.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "refusing to clean speedtest tables with a production MySQL connector"
                )
            })?;
            validate_cleanup_scope(isolation, scope)?;
            let mut failures = Vec::new();
            for table in scope.attempted_tables() {
                if drop_owned_mysql_table(self, scope, &table).await.is_err() {
                    failures.push(format!("'{table}'"));
                }
            }
            anyhow::ensure!(
                failures.is_empty(),
                "Failed to remove MySQL speedtest tables: {}",
                failures.join("; ")
            );
            Ok(())
        })
    }
}

const SPEEDTEST_TABLE_PREFIX: &str = "_transferia_st_";
type IsolatedDiscovery = (
    DeliveryDiscovery,
    BTreeMap<Arc<str>, Arc<str>>,
    BTreeSet<Arc<str>>,
);

pub(super) fn isolate_discovery(
    original: &DeliveryDiscovery,
    isolation_id: &str,
) -> anyhow::Result<IsolatedDiscovery> {
    validate_isolation_id(isolation_id)?;
    let mut discovery = original.clone();
    let mut table_names = BTreeMap::new();
    let mut tables = BTreeSet::new();
    for (index, dataset) in discovery.datasets.iter_mut().enumerate() {
        let original_name = Arc::clone(&dataset.name);
        let scratch: Arc<str> =
            Arc::from(format!("{SPEEDTEST_TABLE_PREFIX}{isolation_id}_{index:x}"));
        validate_identifier("speedtest table", &scratch)?;
        anyhow::ensure!(
            table_names
                .insert(original_name, Arc::clone(&scratch))
                .is_none(),
            "MySQL speedtest source discovery repeats a dataset name"
        );
        anyhow::ensure!(
            tables.insert(Arc::clone(&scratch)),
            "MySQL speedtest generated a duplicate scratch table"
        );
        dataset.name = scratch;
    }
    Ok((discovery, table_names, tables))
}

fn validate_isolation_id(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.len() == 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "MySQL speedtest isolation ID must contain exactly 32 lowercase hexadecimal characters"
    );
    Ok(())
}

fn is_speedtest_table(value: &str) -> bool {
    let Some((id, index)) = value
        .strip_prefix(SPEEDTEST_TABLE_PREFIX)
        .and_then(|suffix| suffix.split_once('_'))
    else {
        return false;
    };
    validate_isolation_id(id).is_ok()
        && !index.is_empty()
        && index.len() <= 16
        && index
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn mysql_physical_target(database: &str, table: &str) -> Arc<str> {
    Arc::from(format!(
        "{}.{}",
        quote_identifier(database),
        quote_identifier(table)
    ))
}

pub(super) fn physical_target_set(
    targets: &[SpeedtestPhysicalTarget],
) -> BTreeSet<(Arc<str>, Arc<str>)> {
    targets
        .iter()
        .map(|target| (Arc::clone(&target.production), Arc::clone(&target.scratch)))
        .collect()
}

pub(super) fn validate_cleanup_scope(
    isolation: &SinkSpeedtestIsolation,
    scope: &MySqlSpeedtestScope,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        isolation.safety() == SinkSpeedtestIsolationSafety::Scratch,
        "refusing to clean a MySQL speedtest isolation without scratch safety"
    );
    let actual_tables = isolation
        .discovery
        .datasets
        .iter()
        .map(|dataset| Arc::clone(&dataset.name))
        .collect::<BTreeSet<_>>();
    anyhow::ensure!(
        actual_tables == scope.tables,
        "refusing to clean MySQL speedtest tables: isolated discovery does not match the connector-owned scratch set"
    );
    anyhow::ensure!(
        scope.tables.iter().all(|table| is_speedtest_table(table)),
        "refusing to clean a MySQL table outside the speedtest namespace"
    );
    anyhow::ensure!(
        physical_target_set(isolation.physical_targets()) == scope.physical_targets,
        "refusing to clean MySQL speedtest tables: physical target proof does not match the connector-owned scratch set"
    );
    Ok(())
}

pub(super) fn mysql_cleanup_ddl(database: &str, table: &str) -> anyhow::Result<String> {
    anyhow::ensure!(
        is_speedtest_table(table),
        "refusing to remove MySQL table outside the speedtest namespace"
    );
    Ok(format!(
        "DROP TABLE IF EXISTS {}.{}",
        quote_identifier(database),
        quote_identifier(table)
    ))
}

fn random_owner_marker() -> anyhow::Result<Arc<str>> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random)?;
    let mut marker = String::from("transferia-speedtest-owner:");
    for byte in random {
        write!(&mut marker, "{byte:02x}")?;
    }
    Ok(Arc::from(marker))
}

fn quote_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn mysql_table_definitions(
    dataset: &transferia_registry::DatasetPrepare,
) -> anyhow::Result<String> {
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
    Ok(definitions.join(", "))
}

pub(super) fn mysql_owned_create_ddl(
    database: &str,
    dataset: &transferia_registry::DatasetPrepare,
    owner_marker: &str,
) -> anyhow::Result<String> {
    anyhow::ensure!(
        is_speedtest_table(&dataset.table),
        "refusing to create a MySQL table outside the speedtest namespace"
    );
    Ok(format!(
        "CREATE TABLE {}.{} ({}) ENGINE=InnoDB COMMENT={}",
        quote_identifier(database),
        quote_identifier(&dataset.table),
        mysql_table_definitions(dataset)?,
        quote_string_literal(owner_marker)
    ))
}

async fn create_owned_mysql_table(
    connection: &mut mysql_async::Conn,
    scope: &MySqlSpeedtestScope,
    dataset: &transferia_registry::DatasetPrepare,
) -> anyhow::Result<()> {
    let ddl = mysql_owned_create_ddl(&scope.database, dataset, &scope.owner_marker)?;
    observe_external_request(
        "mysql",
        "speedtest_create_owned_table",
        connection.query_drop(ddl),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!("MySQL exclusive speedtest table creation did not complete successfully")
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerMarkerEvidence {
    Owned,
    Missing,
    Unmarked,
    Foreign,
}

pub(super) fn classify_owner_marker(actual: Option<&str>, expected: &str) -> OwnerMarkerEvidence {
    match actual {
        Some(value) if value == expected => OwnerMarkerEvidence::Owned,
        Some("") => OwnerMarkerEvidence::Unmarked,
        Some(_) => OwnerMarkerEvidence::Foreign,
        None => OwnerMarkerEvidence::Missing,
    }
}

pub(super) const fn owner_marker_allows_side_effect(evidence: OwnerMarkerEvidence) -> bool {
    matches!(evidence, OwnerMarkerEvidence::Owned)
}

pub(super) const fn ambiguous_drop_is_complete(evidence: OwnerMarkerEvidence) -> bool {
    matches!(evidence, OwnerMarkerEvidence::Missing)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CleanupOwnershipAction {
    AlreadyAbsent,
    VerifySchemaAndDrop,
    Preserve,
}

pub(super) const fn cleanup_ownership_action(
    evidence: OwnerMarkerEvidence,
) -> CleanupOwnershipAction {
    match evidence {
        OwnerMarkerEvidence::Missing => CleanupOwnershipAction::AlreadyAbsent,
        OwnerMarkerEvidence::Owned => CleanupOwnershipAction::VerifySchemaAndDrop,
        OwnerMarkerEvidence::Unmarked | OwnerMarkerEvidence::Foreign => {
            CleanupOwnershipAction::Preserve
        }
    }
}

async fn mysql_owner_evidence(
    connection: &mut mysql_async::Conn,
    scope: &MySqlSpeedtestScope,
    table: &str,
) -> anyhow::Result<OwnerMarkerEvidence> {
    anyhow::ensure!(
        scope.tables.contains(table) && is_speedtest_table(table),
        "MySQL speedtest ownership verification rejected an unknown table"
    );
    let marker = observe_external_request(
        "mysql",
        "speedtest_verify_owner",
        connection.exec_first::<String, _, _>(
            "SELECT TABLE_COMMENT FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'",
            (scope.database.as_ref(), table),
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("MySQL scratch table '{table}' owner marker is unreadable"))?;
    Ok(classify_owner_marker(
        marker.as_deref(),
        &scope.owner_marker,
    ))
}

async fn verify_owned_mysql_table(
    connection: &mut mysql_async::Conn,
    scope: &MySqlSpeedtestScope,
    table: &str,
    schema: &DatasetSchema,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        owner_marker_allows_side_effect(mysql_owner_evidence(connection, scope, table).await?),
        "MySQL scratch table '{table}' has a missing, unreadable, or foreign owner marker"
    );
    let actual = observe_external_request(
        "mysql",
        "speedtest_verify_schema",
        connection.exec::<(String, String), _, _>(
            "SELECT COLUMN_NAME, IS_NULLABLE FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            (scope.database.as_ref(), table),
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("MySQL scratch table '{table}' schema is unreadable"))?;
    let expected = schema
        .columns
        .iter()
        .map(|column| {
            (
                column.name.clone(),
                if column.nullable { "YES" } else { "NO" }.to_owned(),
            )
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        actual == expected,
        "MySQL scratch table '{table}' schema no longer matches its isolated discovery"
    );
    Ok(())
}

async fn verify_all_mysql_tables(
    connection: &mut mysql_async::Conn,
    scope: &MySqlSpeedtestScope,
    discovery: &DeliveryDiscovery,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        scope.claimed_tables() == scope.tables,
        "MySQL speedtest cannot write before every scratch table is proven owned"
    );
    for dataset in &discovery.datasets {
        let schema = scope.schemas.get(&dataset.name).ok_or_else(|| {
            anyhow::anyhow!(
                "MySQL speedtest has no connector-owned schema for '{}'",
                dataset.name
            )
        })?;
        verify_owned_mysql_table(connection, scope, &dataset.name, schema).await?;
    }
    Ok(())
}

async fn disconnect_mysql(connection: mysql_async::Conn, operation: &'static str) {
    drop(observe_external_request("mysql", operation, connection.disconnect()).await);
}

async fn drop_owned_mysql_table(
    connector: &MySqlSinkConnector,
    scope: &MySqlSpeedtestScope,
    table: &str,
) -> anyhow::Result<()> {
    let schema = scope.schemas.get(table).ok_or_else(|| {
        anyhow::anyhow!("MySQL speedtest has no connector-owned schema for '{table}'")
    })?;
    let mut connection = connector.sink_connection().await?;
    match cleanup_ownership_action(mysql_owner_evidence(&mut connection, scope, table).await?) {
        CleanupOwnershipAction::AlreadyAbsent => {
            scope.unclaim(table);
            disconnect_mysql(connection, "speedtest_disconnect_missing_table").await;
            return Ok(());
        }
        CleanupOwnershipAction::VerifySchemaAndDrop => {}
        CleanupOwnershipAction::Preserve => {
            disconnect_mysql(connection, "speedtest_disconnect_unowned_table").await;
            anyhow::bail!("MySQL scratch table '{table}' is not proven owned before cleanup");
        }
    }
    let lock = format!(
        "LOCK TABLES {}.{} WRITE",
        quote_identifier(&scope.database),
        quote_identifier(table)
    );
    if observe_external_request(
        "mysql",
        "speedtest_lock_before_drop",
        connection.query_drop(lock),
    )
    .await
    .is_err()
    {
        disconnect_mysql(connection, "speedtest_disconnect_after_lock_failure").await;
        anyhow::bail!("MySQL scratch table '{table}' could not be locked before cleanup");
    }
    if verify_owned_mysql_table(&mut connection, scope, table, schema)
        .await
        .is_err()
    {
        drop(
            observe_external_request(
                "mysql",
                "speedtest_unlock_unowned_table",
                connection.query_drop("UNLOCK TABLES"),
            )
            .await,
        );
        disconnect_mysql(connection, "speedtest_disconnect_unowned_table").await;
        anyhow::bail!(
            "MySQL scratch table '{table}' is not proven owned immediately before cleanup"
        );
    }
    let drop_result = observe_external_request(
        "mysql",
        "speedtest_drop_table",
        connection.query_drop(mysql_cleanup_ddl(&scope.database, table)?),
    )
    .await;
    drop(
        observe_external_request(
            "mysql",
            "speedtest_unlock_after_drop",
            connection.query_drop("UNLOCK TABLES"),
        )
        .await,
    );
    disconnect_mysql(connection, "speedtest_disconnect_after_drop").await;
    if drop_result.is_ok() {
        scope.unclaim(table);
        return Ok(());
    }

    let mut verifier = connector.sink_connection().await?;
    let evidence = mysql_owner_evidence(&mut verifier, scope, table).await?;
    disconnect_mysql(verifier, "speedtest_disconnect_drop_verifier").await;
    if ambiguous_drop_is_complete(evidence) {
        scope.unclaim(table);
        return Ok(());
    }
    anyhow::bail!("MySQL scratch table '{table}' cleanup result is ambiguous")
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
