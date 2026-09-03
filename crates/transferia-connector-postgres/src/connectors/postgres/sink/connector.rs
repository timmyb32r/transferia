use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use futures_util::future::BoxFuture;

use super::config::PostgresSinkConfig;
use super::writer::PostgresSink;
use crate::connectors::postgres::common::{
    arrow_to_postgres, connect, quote_identifier, validate_identifier, MAX_IDENTIFIER_BYTES,
};
use transferia_core::delivery::{
    validate_stored_projection, ArrowTypeFamily, DeliveryDiscovery, NameSyntax, SinkLimits,
    SinkLimitsDescription, TextLimit,
};
use transferia_core::sink::Sink;
use transferia_core::SystemColumnKind;
use transferia_connector_support::external_request::observe_external_request;
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_registry::{
    SinkBuildContext, SinkConnector, SinkPrepare, SinkSpeedtestIsolation,
    SinkSpeedtestIsolationSafety, SpeedtestPhysicalTarget,
};

pub struct PostgresSinkConnector {
    config: Arc<PostgresSinkConfig>,
    speedtest_scope: Option<Arc<PostgresSpeedtestScope>>,
}

pub(super) struct PostgresSpeedtestScope {
    pub(super) database: Arc<str>,

    pub(super) schema: Arc<str>,

    pub(super) owner_marker: Arc<str>,

    pub(super) tables: BTreeSet<Arc<str>>,

    pub(super) schemas: BTreeMap<Arc<str>, transferia_core::data::schema::DatasetSchema>,

    pub(super) physical_targets: BTreeSet<(Arc<str>, Arc<str>)>,

    pub(super) attempted_tables: Mutex<BTreeSet<Arc<str>>>,

    pub(super) claimed_tables: Mutex<BTreeSet<Arc<str>>>,
}

impl PostgresSpeedtestScope {
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

impl PostgresSinkConnector {
    pub fn from_config(config: PostgresSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config: Arc::new(config),
            speedtest_scope: None,
        })
    }

    async fn sink_client(&self) -> anyhow::Result<tokio_postgres::Client> {
        let client = observe_external_request(
            "postgresql",
            "connect_sink",
            connect(&self.config.connection),
        )
        .await?;
        if let Some(scope) = &self.speedtest_scope {
            let row = observe_external_request(
                "postgresql",
                "speedtest_resolve_database",
                client.query_one("SELECT current_database()", &[]),
            )
            .await?;
            let database: String = row.try_get(0)?;
            anyhow::ensure!(
                database == scope.database.as_ref(),
                "PostgreSQL speedtest connection resolved database '{database}', expected '{}'",
                scope.database
            );
            let quoted_schema = quote_identifier(&scope.schema);
            observe_external_request(
                "postgresql",
                "speedtest_select_schema",
                client.query_one(
                    "SELECT pg_catalog.set_config('search_path', $1, false)",
                    &[&quoted_schema],
                ),
            )
            .await?;
            let row = observe_external_request(
                "postgresql",
                "speedtest_verify_schema",
                client.query_one("SELECT current_schema()", &[]),
            )
            .await?;
            let schema: Option<String> = row.try_get(0)?;
            anyhow::ensure!(
                schema.as_deref() == Some(scope.schema.as_ref()),
                "PostgreSQL speedtest connection cannot select its isolated schema '{}'",
                scope.schema
            );
        }
        Ok(client)
    }

    async fn prepare_speedtest(
        &self,
        client: &mut tokio_postgres::Client,
        request: SinkPrepare,
        scope: &PostgresSpeedtestScope,
    ) -> anyhow::Result<()> {
        let requested = request
            .datasets
            .iter()
            .map(|dataset| Arc::clone(&dataset.table))
            .collect::<BTreeSet<_>>();
        anyhow::ensure!(
            requested == scope.tables,
            "PostgreSQL speedtest preparation does not match its connector-owned scratch set"
        );
        for dataset in request.datasets {
            scope.record_attempt(Arc::clone(&dataset.table));
            let create_result = create_owned_postgres_table(client, scope, &dataset).await;
            let mut verified = verify_owned_postgres_table(
                client,
                scope,
                &dataset.table,
                &dataset.schema,
            )
            .await;
            if create_result.is_err() || verified.is_err() {
                let verifier = self.sink_client().await?;
                verified = verify_owned_postgres_table(
                    &verifier,
                    scope,
                    &dataset.table,
                    &dataset.schema,
                )
                .await;
            }
            if verified.is_err() {
                anyhow::bail!(
                    "PostgreSQL speedtest could not prove exclusive ownership of scratch table '{}' after CREATE",
                    dataset.table
                );
            }
            scope.claim(Arc::clone(&dataset.table));
        }
        Ok(())
    }
}

impl SinkLimits for PostgresSinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        let name = TextLimit {
            syntax: NameSyntax::AsciiIdentifier,
            max_utf8_bytes: Some(MAX_IDENTIFIER_BYTES),
        };
        SinkLimitsDescription {
            sink: "postgres",
            dataset_name: Some(name.clone()),
            column_name: Some(name),
            supported_arrow_types: vec![
                ArrowTypeFamily::Utf8,
                ArrowTypeFamily::Binary,
                ArrowTypeFamily::SignedInteger,
                ArrowTypeFamily::UnsignedInteger,
                ArrowTypeFamily::FloatingPoint,
                ArrowTypeFamily::Boolean,
                ArrowTypeFamily::Date32,
                ArrowTypeFamily::Timestamp,
            ],
            object_key: None,
        }
    }

    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        anyhow::ensure!(
            !discovery.datasets.is_empty(),
            "PostgreSQL sink requires at least one dataset"
        );
        let mut names = std::collections::HashSet::new();
        for dataset in &discovery.datasets {
            anyhow::ensure!(
                names.insert(dataset.name.as_ref()),
                "PostgreSQL datasets repeat table '{}'",
                dataset.name
            );
            validate_identifier("table", &dataset.name)?;
            validate_stored_projection(discovery, dataset)?;
            anyhow::ensure!(
                !dataset.stored_schema.columns.is_empty(),
                "PostgreSQL table '{}' cannot have an empty schema",
                dataset.name
            );
            let mut primary_keys = 0;
            for column in &dataset.stored_schema.columns {
                validate_identifier("column", &column.name)?;
                arrow_to_postgres(&column.data_type)?;
                if column.primary_key {
                    primary_keys += 1;
                    anyhow::ensure!(
                        !column.nullable,
                        "PostgreSQL primary-key column '{}.{}' must not be nullable",
                        dataset.name,
                        column.name
                    );
                }
            }
            if dataset
                .system_columns
                .iter()
                .any(|column| column.kind == SystemColumnKind::ChangeOperation)
            {
                anyhow::ensure!(
                    primary_keys > 0,
                    "PostgreSQL changelog dataset '{}' requires a primary key",
                    dataset.name
                );
            }
        }
        Ok(())
    }
}

impl SinkConnector for PostgresSinkConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::PostgresSink
    }
    fn limits(&self) -> &dyn SinkLimits {
        self.config.as_ref()
    }

    fn destination_type(
        &self,
        column: &transferia_core::data::schema::SchemaColumn,
    ) -> anyhow::Result<String> {
        let data_type = postgres_sql_type(&column.data_type)?;
        Ok(format!(
            "{data_type} {}",
            if column.nullable { "NULL" } else { "NOT NULL" }
        ))
    }
    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let mut client = self.sink_client().await?;
            if let Some(scope) = &self.speedtest_scope {
                return self
                    .prepare_speedtest(&mut client, request, scope)
                    .await;
            }
            for dataset in request.datasets {
                if self.config.create_tables {
                    client
                        .batch_execute(&format!(
                            "CREATE TABLE IF NOT EXISTS {} ({})",
                            quote_identifier(&dataset.table),
                            postgres_table_definitions(&dataset)?
                        ))
                        .await?;
                }
                validate_changelog_primary_key(&client, &dataset).await?;
            }
            Ok(())
        })
    }
    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            let client = self.sink_client().await?;
            if let Some(scope) = &self.speedtest_scope {
                verify_all_postgres_tables(&client, scope, context.discovery.as_ref()).await?;
            }
            let limits: Arc<dyn SinkLimits> = Arc::clone(&self.config) as Arc<dyn SinkLimits>;
            Ok(Box::new(PostgresSink::new(
                client,
                context.counters,
                context.discovery,
                limits,
                self.config.copy_from_format,
            )) as Box<dyn Sink>)
        })
    }

    fn isolate_speedtest(
        self: Arc<Self>,
        discovery: Arc<DeliveryDiscovery>,
        isolation_id: String,
    ) -> BoxFuture<'static, anyhow::Result<SinkSpeedtestIsolation>> {
        Box::pin(async move {
            let client = observe_external_request(
                "postgresql",
                "speedtest_connect",
                connect(&self.config.connection),
            )
            .await?;
            let row = observe_external_request(
                "postgresql",
                "speedtest_resolve_scope",
                client.query_one("SELECT current_database(), current_schema()", &[]),
            )
            .await?;
            let database: String = row.try_get(0)?;
            let schema: Option<String> = row.try_get(1)?;
            let schema = schema.ok_or_else(|| {
                anyhow::anyhow!(
                    "PostgreSQL speedtest requires a resolvable current schema for scratch tables"
                )
            })?;

            let (isolated_discovery, table_names, tables) =
                isolate_discovery(discovery.as_ref(), &isolation_id)?;
            let physical_targets = discovery
                .datasets
                .iter()
                .map(|dataset| {
                    let scratch = table_names.get(&dataset.name).ok_or_else(|| {
                        anyhow::anyhow!(
                            "PostgreSQL speedtest omitted dataset '{}' from its scratch mapping",
                            dataset.name
                        )
                    })?;
                    Ok(SpeedtestPhysicalTarget {
                        production: postgres_physical_target(
                            &database,
                            &schema,
                            &dataset.name,
                        ),
                        scratch: postgres_physical_target(&database, &schema, scratch),
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            let scope = Arc::new(PostgresSpeedtestScope {
                database: Arc::from(database),
                schema: Arc::from(schema),
                owner_marker: random_owner_marker()?,
                tables: tables.clone(),
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
                    "refusing to clean speedtest tables with a production PostgreSQL connector"
                )
            })?;
            validate_cleanup_scope(isolation, scope)?;
            let mut failures = Vec::new();
            for table in scope.attempted_tables() {
                if drop_owned_postgres_table(self, scope, &table).await.is_err() {
                    failures.push(format!("'{table}'"));
                }
            }
            anyhow::ensure!(
                failures.is_empty(),
                "Failed to remove PostgreSQL speedtest tables: {}",
                failures.join("; ")
            );
            Ok(())
        })
    }
}

const SPEEDTEST_TABLE_PREFIX: &str = "_transferia_st_";

pub(super) fn isolate_discovery(
    original: &DeliveryDiscovery,
    isolation_id: &str,
) -> anyhow::Result<(
    DeliveryDiscovery,
    BTreeMap<Arc<str>, Arc<str>>,
    BTreeSet<Arc<str>>,
)> {
    validate_isolation_id(isolation_id)?;
    let mut discovery = original.clone();
    let mut table_names = BTreeMap::new();
    let mut tables = BTreeSet::new();
    for (index, dataset) in discovery.datasets.iter_mut().enumerate() {
        let original_name = Arc::clone(&dataset.name);
        let scratch: Arc<str> = Arc::from(format!(
            "{SPEEDTEST_TABLE_PREFIX}{isolation_id}_{index:x}"
        ));
        validate_identifier("speedtest table", &scratch)?;
        anyhow::ensure!(
            table_names
                .insert(original_name, Arc::clone(&scratch))
                .is_none(),
            "PostgreSQL speedtest source discovery repeats a dataset name"
        );
        anyhow::ensure!(
            tables.insert(Arc::clone(&scratch)),
            "PostgreSQL speedtest generated a duplicate scratch table"
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
        "PostgreSQL speedtest isolation ID must contain exactly 32 lowercase hexadecimal characters"
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

pub(super) fn postgres_physical_target(database: &str, schema: &str, table: &str) -> Arc<str> {
    Arc::from(format!(
        "{}.{}.{}",
        quote_identifier(database),
        quote_identifier(schema),
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
    scope: &PostgresSpeedtestScope,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        isolation.safety() == SinkSpeedtestIsolationSafety::Scratch,
        "refusing to clean a PostgreSQL speedtest isolation without scratch safety"
    );
    let actual_tables = isolation
        .discovery
        .datasets
        .iter()
        .map(|dataset| Arc::clone(&dataset.name))
        .collect::<BTreeSet<_>>();
    anyhow::ensure!(
        actual_tables == scope.tables,
        "refusing to clean PostgreSQL speedtest tables: isolated discovery does not match the connector-owned scratch set"
    );
    anyhow::ensure!(
        scope.tables.iter().all(|table| is_speedtest_table(table)),
        "refusing to clean a PostgreSQL table outside the speedtest namespace"
    );
    anyhow::ensure!(
        physical_target_set(isolation.physical_targets()) == scope.physical_targets,
        "refusing to clean PostgreSQL speedtest tables: physical target proof does not match the connector-owned scratch set"
    );
    Ok(())
}

pub(super) fn postgres_cleanup_ddl(schema: &str, table: &str) -> anyhow::Result<String> {
    anyhow::ensure!(
        is_speedtest_table(table),
        "refusing to remove PostgreSQL table outside the speedtest namespace"
    );
    Ok(format!(
        "DROP TABLE IF EXISTS {}.{}",
        quote_identifier(schema),
        quote_identifier(table)
    ))
}

fn random_owner_marker() -> anyhow::Result<Arc<str>> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random)?;
    let mut marker = String::from("transferia-speedtest-owner:");
    use std::fmt::Write as _;
    for byte in random {
        write!(&mut marker, "{byte:02x}")?;
    }
    Ok(Arc::from(marker))
}

fn quote_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn postgres_table_definitions(
    dataset: &transferia_registry::DatasetPrepare,
) -> anyhow::Result<String> {
    let columns = dataset
        .schema
        .columns
        .iter()
        .map(|column| {
            let sql_type = postgres_sql_type(&column.data_type)?;
            Ok(format!(
                "{} {sql_type}{}",
                quote_identifier(&column.name),
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

pub(super) fn postgres_owned_create_ddl(
    schema: &str,
    dataset: &transferia_registry::DatasetPrepare,
    owner_marker: &str,
) -> anyhow::Result<String> {
    anyhow::ensure!(
        is_speedtest_table(&dataset.table),
        "refusing to create a PostgreSQL table outside the speedtest namespace"
    );
    let table = format!(
        "{}.{}",
        quote_identifier(schema),
        quote_identifier(&dataset.table)
    );
    Ok(format!(
        "CREATE TABLE {table} ({}); COMMENT ON TABLE {table} IS {}",
        postgres_table_definitions(dataset)?,
        quote_string_literal(owner_marker)
    ))
}

async fn create_owned_postgres_table(
    client: &mut tokio_postgres::Client,
    scope: &PostgresSpeedtestScope,
    dataset: &transferia_registry::DatasetPrepare,
) -> anyhow::Result<()> {
    let transaction = observe_external_request(
        "postgresql",
        "speedtest_begin_create",
        client.transaction(),
    )
    .await?;
    let ddl = postgres_owned_create_ddl(&scope.schema, dataset, &scope.owner_marker)?;
    if observe_external_request(
        "postgresql",
        "speedtest_create_owned_table",
        transaction.batch_execute(&ddl),
    )
    .await
    .is_err()
    {
        drop(observe_external_request(
            "postgresql",
            "speedtest_rollback_create",
            transaction.rollback(),
        )
        .await);
        anyhow::bail!(
            "PostgreSQL exclusive speedtest table creation did not complete successfully"
        );
    }
    observe_external_request(
        "postgresql",
        "speedtest_commit_create",
        transaction.commit(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("PostgreSQL speedtest CREATE commit result is ambiguous"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerMarkerEvidence {
    Owned,
    Missing,
    Unmarked,
    Foreign,
}

pub(super) fn classify_owner_marker(
    actual: Option<Option<&str>>,
    expected: &str,
) -> OwnerMarkerEvidence {
    match actual {
        Some(Some(value)) if value == expected => OwnerMarkerEvidence::Owned,
        Some(Some(_)) => OwnerMarkerEvidence::Foreign,
        Some(None) => OwnerMarkerEvidence::Unmarked,
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

async fn verify_owned_postgres_table<C>(
    client: &C,
    scope: &PostgresSpeedtestScope,
    table: &str,
    schema: &transferia_core::data::schema::DatasetSchema,
) -> anyhow::Result<()>
where
    C: tokio_postgres::GenericClient + Sync,
{
    anyhow::ensure!(
        scope.tables.contains(table) && is_speedtest_table(table),
        "PostgreSQL speedtest ownership verification rejected an unknown table"
    );
    let evidence = postgres_owner_evidence(client, scope, table).await?;
    anyhow::ensure!(
        owner_marker_allows_side_effect(evidence),
        "PostgreSQL scratch table '{table}' has a missing, unreadable, or foreign owner marker"
    );

    let rows = observe_external_request(
        "postgresql",
        "speedtest_verify_schema",
        client.query(
            "SELECT attribute.attname, pg_catalog.format_type(attribute.atttypid, attribute.atttypmod), attribute.attnotnull \
             FROM pg_catalog.pg_attribute AS attribute \
             JOIN pg_catalog.pg_class AS table_class ON table_class.oid = attribute.attrelid \
             JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = table_class.relnamespace \
             WHERE namespace.nspname = $1 AND table_class.relname = $2 \
               AND attribute.attnum > 0 AND NOT attribute.attisdropped \
             ORDER BY attribute.attnum",
            &[&scope.schema.as_ref(), &table],
        ),
    )
    .await
    .map_err(|_| anyhow::anyhow!("PostgreSQL scratch table '{table}' schema is unreadable"))?;
    let actual = rows
        .into_iter()
        .map(|row| {
            Ok((
                row.try_get::<_, String>(0)?,
                row.try_get::<_, String>(1)?,
                row.try_get::<_, bool>(2)?,
            ))
        })
        .collect::<Result<Vec<_>, tokio_postgres::Error>>()?;
    let expected = schema
        .columns
        .iter()
        .map(|column| {
            Ok((
                column.name.clone(),
                postgres_catalog_type(&column.data_type)?.to_owned(),
                !column.nullable,
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    anyhow::ensure!(
        actual == expected,
        "PostgreSQL scratch table '{table}' schema no longer matches its isolated discovery"
    );
    Ok(())
}

async fn postgres_owner_evidence<C>(
    client: &C,
    scope: &PostgresSpeedtestScope,
    table: &str,
) -> anyhow::Result<OwnerMarkerEvidence>
where
    C: tokio_postgres::GenericClient + Sync,
{
    let marker_row = observe_external_request(
        "postgresql",
        "speedtest_verify_owner",
        client.query_opt(
            "SELECT pg_catalog.obj_description(table_class.oid, 'pg_class') \
             FROM pg_catalog.pg_class AS table_class \
             JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid = table_class.relnamespace \
             WHERE namespace.nspname = $1 AND table_class.relname = $2 AND table_class.relkind IN ('r', 'p')",
            &[&scope.schema.as_ref(), &table],
        ),
    )
    .await
    .map_err(|_| {
        anyhow::anyhow!("PostgreSQL scratch table '{table}' owner marker is unreadable")
    })?;
    let actual_marker = marker_row
        .as_ref()
        .map(|row| row.try_get::<_, Option<String>>(0))
        .transpose()
        .map_err(|_| {
            anyhow::anyhow!("PostgreSQL scratch table '{table}' owner marker is unreadable")
        })?;
    Ok(classify_owner_marker(
        actual_marker.as_ref().map(|value| value.as_deref()),
        &scope.owner_marker,
    ))
}

fn postgres_catalog_type(data_type: &DataType) -> anyhow::Result<&'static str> {
    Ok(match data_type {
        DataType::Timestamp(_, None) => "timestamp without time zone",
        DataType::Timestamp(_, Some(_)) => "timestamp with time zone",
        _ => postgres_sql_type(data_type)?,
    })
}

async fn verify_all_postgres_tables(
    client: &tokio_postgres::Client,
    scope: &PostgresSpeedtestScope,
    discovery: &DeliveryDiscovery,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        scope.claimed_tables() == scope.tables,
        "PostgreSQL speedtest cannot write before every scratch table is proven owned"
    );
    for dataset in &discovery.datasets {
        let schema = scope.schemas.get(&dataset.name).ok_or_else(|| {
            anyhow::anyhow!(
                "PostgreSQL speedtest has no connector-owned schema for '{}'",
                dataset.name
            )
        })?;
        verify_owned_postgres_table(client, scope, &dataset.name, schema).await?;
    }
    Ok(())
}

async fn drop_owned_postgres_table(
    connector: &PostgresSinkConnector,
    scope: &PostgresSpeedtestScope,
    table: &str,
) -> anyhow::Result<()> {
    let schema = scope.schemas.get(table).ok_or_else(|| {
        anyhow::anyhow!("PostgreSQL speedtest has no connector-owned schema for '{table}'")
    })?;
    let mut client = connector.sink_client().await?;
    match cleanup_ownership_action(postgres_owner_evidence(&client, scope, table).await?) {
        CleanupOwnershipAction::AlreadyAbsent => {
            scope.unclaim(table);
            return Ok(());
        }
        CleanupOwnershipAction::VerifySchemaAndDrop => {}
        CleanupOwnershipAction::Preserve => {
            anyhow::bail!(
                "PostgreSQL scratch table '{table}' is not proven owned before cleanup"
            );
        }
    }
    let transaction = observe_external_request(
        "postgresql",
        "speedtest_begin_cleanup",
        client.transaction(),
    )
    .await?;
    let lock = format!(
        "LOCK TABLE {}.{} IN ACCESS EXCLUSIVE MODE",
        quote_identifier(&scope.schema),
        quote_identifier(table)
    );
    if observe_external_request(
        "postgresql",
        "speedtest_lock_before_drop",
        transaction.batch_execute(&lock),
    )
    .await
    .is_err()
    {
        drop(observe_external_request(
            "postgresql",
            "speedtest_rollback_cleanup_after_lock_failure",
            transaction.rollback(),
        )
        .await);
        anyhow::bail!("PostgreSQL scratch table '{table}' could not be locked before cleanup");
    }
    if verify_owned_postgres_table(&transaction, scope, table, schema)
        .await
        .is_err()
    {
        drop(observe_external_request(
            "postgresql",
            "speedtest_rollback_unowned_cleanup",
            transaction.rollback(),
        )
        .await);
        anyhow::bail!(
            "PostgreSQL scratch table '{table}' is not proven owned immediately before cleanup"
        );
    }
    if observe_external_request(
        "postgresql",
        "speedtest_drop_table",
        transaction.batch_execute(&postgres_cleanup_ddl(&scope.schema, table)?),
    )
    .await
    .is_err()
    {
        drop(observe_external_request(
            "postgresql",
            "speedtest_rollback_cleanup_after_drop_failure",
            transaction.rollback(),
        )
        .await);
        anyhow::bail!("PostgreSQL scratch table '{table}' could not be removed");
    }
    let commit_result = observe_external_request(
        "postgresql",
        "speedtest_commit_cleanup",
        transaction.commit(),
    )
    .await;
    if commit_result.is_ok() {
        scope.unclaim(table);
        return Ok(());
    }
    let verifier = connector.sink_client().await?;
    if ambiguous_drop_is_complete(postgres_owner_evidence(&verifier, scope, table).await?) {
        scope.unclaim(table);
        return Ok(());
    }
    anyhow::bail!("PostgreSQL scratch table '{table}' cleanup result is ambiguous")
}

async fn validate_changelog_primary_key(
    client: &tokio_postgres::Client,
    dataset: &transferia_registry::DatasetPrepare,
) -> anyhow::Result<()> {
    if !dataset.changelog {
        return Ok(());
    }
    let rows = client
        .query(
            "SELECT attribute.attname \
             FROM pg_index AS idx \
             JOIN pg_class AS table_class ON table_class.oid = idx.indrelid \
             JOIN pg_namespace AS namespace ON namespace.oid = table_class.relnamespace \
             JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS key(attnum, position) ON TRUE \
             JOIN pg_attribute AS attribute ON attribute.attrelid = table_class.oid AND attribute.attnum = key.attnum \
             WHERE namespace.nspname = current_schema() AND table_class.relname = $1 AND idx.indisprimary \
             ORDER BY key.position",
            &[&dataset.table.as_ref()],
        )
        .await?;
    let actual = rows
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    let expected = dataset
        .schema
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    anyhow::ensure!(
        actual.iter().map(String::as_str).collect::<Vec<_>>() == expected,
        "PostgreSQL changelog table '{}' has primary key {actual:?}, expected {expected:?}",
        dataset.table
    );
    Ok(())
}

#[expect(
    clippy::unreachable,
    reason = "arrow_to_postgres rejects every type outside this exhaustive supported subset"
)]
pub(super) fn postgres_sql_type(data_type: &DataType) -> anyhow::Result<&'static str> {
    arrow_to_postgres(data_type)?;
    Ok(match data_type {
        DataType::Boolean => "boolean",
        DataType::Int8 => "\"char\"",
        DataType::Int16 => "smallint",
        DataType::Int32 => "integer",
        DataType::Int64 => "bigint",
        DataType::UInt8 => "smallint",
        DataType::UInt16 => "integer",
        DataType::UInt32 => "oid",
        DataType::UInt64 => "numeric(20,0)",
        DataType::Float32 => "real",
        DataType::Float64 => "double precision",
        DataType::Binary => "bytea",
        DataType::Utf8 => "text",
        DataType::Date32 => "date",
        DataType::Timestamp(_, None) => "timestamp",
        DataType::Timestamp(_, Some(_)) => "timestamp with time zone",
        _ => unreachable!(),
    })
}
