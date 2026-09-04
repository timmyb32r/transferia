use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, DurationMicrosecondArray,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, TimestampMicrosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::{
    write_message, CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
};
use arrow::record_batch::RecordBatch;
use futures_util::future::BoxFuture;

use super::config::{YdbSinkConfig, YdbTableConfig};
use super::transport::{is_not_found_error, is_retryable_error, YdbClient};
use super::types::{
    column_plans, ColumnKind, ARROW_UUID_EXTENSION, YDB_DYNUMBER_EXTENSION,
    YDB_TZ_DATETIME_EXTENSION, YDB_TZ_DATE_EXTENSION, YDB_TZ_TIMESTAMP_EXTENSION,
    YDB_YSON_EXTENSION,
};
use transferia_connector_support::external_request::observe_external_request;
use transferia_core::data::changelog::{project_sink_batch, ProjectedSinkBatch};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};
use transferia_core::delivery::{
    validate_stored_projection, ArrowTypeFamily, DeliveryDiscovery, NameSyntax, SinkLimits,
    SinkLimitsDescription, TextLimit,
};
use transferia_core::failure::DataPlaneFailure;
use transferia_core::sink::{Delivery, Sink, SinkEvent, SinkIo};
use transferia_delivery_contracts::metrics::SinkCounters;
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_registry::{
    SinkBuildContext, SinkConnector, SinkPrepare, SinkSpeedtestIsolation,
    SinkSpeedtestIsolationSafety, SpeedtestPhysicalTarget,
};
use ydb_grpc::ydb_proto::r#type::{PrimitiveTypeId, Type as TypeVariant};
use ydb_grpc::ydb_proto::table::{ColumnMeta, CreateTableRequest, DescribeTableResult};
use ydb_grpc::ydb_proto::value::Value as ValueVariant;
use ydb_grpc::ydb_proto::{
    DecimalType, ListType, OptionalType, StructMember, StructType, Type, TypedValue, Value,
};

pub struct YdbSinkConnector {
    config: Arc<YdbSinkConfig>,
    speedtest_scope: Option<Arc<YdbSpeedtestScope>>,
}

pub(super) struct YdbSpeedtestScope {
    pub(super) tables: BTreeMap<Arc<str>, Arc<str>>,
    pub(super) schemas: BTreeMap<Arc<str>, DatasetSchema>,
    pub(super) owner: Arc<str>,
    pub(super) physical_targets: BTreeSet<(Arc<str>, Arc<str>)>,
    pub(super) attempted: Mutex<BTreeSet<Arc<str>>>,
}

pub(super) trait YdbSpeedtestTableClient {
    fn create_owned(&mut self, request: CreateTableRequest) -> BoxFuture<'_, anyhow::Result<()>>;

    fn describe_owned<'a>(
        &'a mut self,
        path: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<DescribeTableResult>>;

    fn drop_owned<'a>(&'a mut self, path: &'a str) -> BoxFuture<'a, anyhow::Result<()>>;

    fn is_not_found(&self, error: &anyhow::Error) -> bool;
}

impl YdbSpeedtestTableClient for YdbClient {
    fn create_owned(&mut self, request: CreateTableRequest) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(self.create_table(request))
    }

    fn describe_owned<'a>(
        &'a mut self,
        path: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<DescribeTableResult>> {
        Box::pin(self.describe_table(path.to_owned()))
    }

    fn drop_owned<'a>(&'a mut self, path: &'a str) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(self.drop_table(path.to_owned()))
    }

    fn is_not_found(&self, error: &anyhow::Error) -> bool {
        is_not_found_error(error)
    }
}

impl YdbSpeedtestScope {
    fn record_attempted(&self, path: &str) -> anyhow::Result<()> {
        let path = self
            .tables
            .values()
            .find(|owned| owned.as_ref() == path)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "refusing to record a YDB speedtest table outside the owned scratch set"
                )
            })?;
        self.attempted
            .lock()
            .map_err(|_| anyhow::anyhow!("YDB speedtest ownership state is poisoned"))?
            .insert(path);
        Ok(())
    }

    fn attempted_paths(&self) -> anyhow::Result<BTreeSet<Arc<str>>> {
        self.attempted
            .lock()
            .map(|attempted| attempted.clone())
            .map_err(|_| anyhow::anyhow!("YDB speedtest ownership state is poisoned"))
    }

    fn record_cleaned(&self, path: &str) -> anyhow::Result<()> {
        self.attempted
            .lock()
            .map_err(|_| anyhow::anyhow!("YDB speedtest ownership state is poisoned"))?
            .remove(path);
        Ok(())
    }
}

impl YdbSinkConnector {
    pub fn from_config(config: YdbSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config: Arc::new(config),
            speedtest_scope: None,
        })
    }
}

impl SinkLimits for YdbSinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        let name = TextLimit {
            syntax: NameSyntax::AnyNonEmptyUtf8,
            max_utf8_bytes: None,
        };
        SinkLimitsDescription {
            sink: "ydb",
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
                ArrowTypeFamily::Timestamp,
                ArrowTypeFamily::Duration,
                ArrowTypeFamily::FixedSizeBinary,
            ],
            object_key: None,
        }
    }

    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        anyhow::ensure!(
            !discovery.datasets.is_empty(),
            "YDB sink requires at least one dataset"
        );
        let mut names = HashSet::new();
        for dataset in &discovery.datasets {
            anyhow::ensure!(
                names.insert(dataset.name.as_ref()),
                "YDB datasets repeat table '{}'",
                dataset.name
            );
            self.table_path(&dataset.name)?;
            validate_stored_projection(discovery, dataset)?;
            anyhow::ensure!(
                !dataset.stored_schema.columns.is_empty(),
                "YDB table '{}' cannot have an empty schema",
                dataset.name
            );
            let mut primary_keys = 0_usize;
            for column in &dataset.stored_schema.columns {
                validate_name("column", &column.name)?;
                let kind = column_kind(column)?;
                if column.primary_key {
                    primary_keys += 1;
                    anyhow::ensure!(
                        !column.nullable,
                        "YDB primary-key column '{}.{}' must not be nullable",
                        dataset.name,
                        column.name
                    );
                    ensure_primary_key_type(&kind, column)?;
                }
            }
            anyhow::ensure!(
                primary_keys > 0,
                "YDB table '{}' requires at least one primary-key column",
                dataset.name
            );
        }
        anyhow::ensure!(
            names.len() == self.tables.len(),
            "YDB sink table mappings must exactly match discovered datasets"
        );
        Ok(())
    }
}

impl SinkConnector for YdbSinkConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::YdbSink
    }

    fn limits(&self) -> &dyn SinkLimits {
        self.config.as_ref()
    }

    fn destination_type(&self, column: &SchemaColumn) -> anyhow::Result<String> {
        let data_type = yql_type(column)?;
        Ok(if column.nullable {
            format!("{data_type}?")
        } else {
            data_type
        })
    }

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            if let Some(scope) = &self.speedtest_scope {
                validate_ydb_connector_scope(&self.config, scope)?;
            }
            let mut client = YdbClient::connect(&self.config.connection).await?;
            for dataset in request.datasets {
                let path = self.config.table_path(&dataset.table)?;
                if let Some(scope) = &self.speedtest_scope {
                    scope.record_attempted(path)?;
                    prepare_ydb_speedtest_table(&mut client, path, &dataset.schema, &scope.owner)
                        .await?;
                } else if self.config.create_tables {
                    let query = create_table_query(path, &dataset.schema)?;
                    execute_scheme_query_with_retry(&mut client, query, self.config.retry_max_ms)
                        .await?;
                }
                let description =
                    describe_table_with_retry(&mut client, path, self.config.retry_max_ms).await?;
                let actual = column_plans(description.columns, &description.primary_key)?;
                ensure_table_schema(path, &dataset.schema, &actual)?;
            }
            Ok(())
        })
    }

    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            if let Some(scope) = &self.speedtest_scope {
                validate_ydb_connector_scope(&self.config, scope)?;
            }
            let limits: Arc<dyn SinkLimits> = Arc::clone(&self.config) as Arc<dyn SinkLimits>;
            let mut client = YdbClient::connect(&self.config.connection).await?;
            if let Some(scope) = &self.speedtest_scope {
                verify_ydb_speedtest_scope(&mut client, scope).await?;
            }
            Ok(Box::new(YdbSink {
                client,
                table_paths: self
                    .config
                    .tables
                    .iter()
                    .map(|table| (table.name().to_owned(), table.path.clone()))
                    .collect(),
                counters: context.counters,
                discovery: context.discovery,
                limits,
                speedtest_scope: self.speedtest_scope.clone(),
            }) as Box<dyn Sink>)
        })
    }

    fn isolate_speedtest(
        self: Arc<Self>,
        discovery: Arc<DeliveryDiscovery>,
        isolation_id: String,
    ) -> BoxFuture<'static, anyhow::Result<SinkSpeedtestIsolation>> {
        Box::pin(async move {
            let (isolated_discovery, table_names, tables, physical_targets) =
                isolate_ydb_discovery(&self.config, discovery.as_ref(), &isolation_id)?;
            let isolated_config = isolated_ydb_config(&self.config, &tables)?;
            let schemas = isolated_discovery
                .datasets
                .iter()
                .map(|dataset| {
                    Ok((
                        Arc::from(isolated_config.table_path(&dataset.name)?),
                        dataset.stored_schema.clone(),
                    ))
                })
                .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
            let scope = Arc::new(YdbSpeedtestScope {
                tables,
                schemas,
                owner: Arc::from(uuid::Uuid::new_v4().simple().to_string()),
                physical_targets: physical_target_set(&physical_targets),
                attempted: Mutex::new(BTreeSet::new()),
            });
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
                    "refusing to clean YDB speedtest tables with a production connector"
                )
            })?;
            validate_ydb_cleanup_scope(&self.config, isolation, scope)?;

            let paths = scope.attempted_paths()?;
            if paths.is_empty() {
                return Ok(());
            }
            let mut client = observe_external_request(
                "ydb",
                "speedtest_cleanup_connect",
                YdbClient::connect(&self.config.connection),
            )
            .await?;
            cleanup_ydb_speedtest_scope(&mut client, scope).await
        })
    }
}

const SPEEDTEST_TABLE_PREFIX: &str = "_transferia_st_";
const YDB_PATH_SEGMENT_MAX_BYTES: usize = 255;

type YdbIsolationPlan = (
    DeliveryDiscovery,
    BTreeMap<Arc<str>, Arc<str>>,
    BTreeMap<Arc<str>, Arc<str>>,
    Vec<SpeedtestPhysicalTarget>,
);

pub(super) fn isolated_ydb_config(
    production: &YdbSinkConfig,
    tables: &BTreeMap<Arc<str>, Arc<str>>,
) -> anyhow::Result<YdbSinkConfig> {
    let mut isolated = production.clone();
    isolated.tables = tables
        .values()
        .map(|path| YdbTableConfig {
            path: path.to_string(),
        })
        .collect();
    isolated.create_tables = true;
    isolated.validate()?;
    Ok(isolated)
}

pub(super) fn isolate_ydb_discovery(
    config: &YdbSinkConfig,
    original: &DeliveryDiscovery,
    isolation_id: &str,
) -> anyhow::Result<YdbIsolationPlan> {
    validate_speedtest_isolation_id(isolation_id)?;
    let mut discovery = original.clone();
    let mut table_names = BTreeMap::new();
    let mut tables = BTreeMap::new();
    let mut physical_targets = Vec::with_capacity(discovery.datasets.len());
    for (index, dataset) in discovery.datasets.iter_mut().enumerate() {
        let original_name = Arc::clone(&dataset.name);
        let production_path = config.table_path(&original_name)?;
        let (scratch_name, scratch_path) =
            ydb_speedtest_table(production_path, isolation_id, index)?;
        let scratch_name: Arc<str> = Arc::from(scratch_name);
        let scratch_path: Arc<str> = Arc::from(scratch_path);
        anyhow::ensure!(
            table_names
                .insert(Arc::clone(&original_name), Arc::clone(&scratch_name))
                .is_none(),
            "YDB speedtest discovery repeats dataset '{original_name}'"
        );
        anyhow::ensure!(
            tables
                .insert(Arc::clone(&scratch_name), Arc::clone(&scratch_path))
                .is_none(),
            "YDB speedtest generated a duplicate scratch table"
        );
        physical_targets.push(SpeedtestPhysicalTarget {
            production: ydb_physical_target(&config.connection.database, production_path)?,
            scratch: ydb_physical_target(&config.connection.database, &scratch_path)?,
        });
        dataset.name = scratch_name;
    }
    Ok((discovery, table_names, tables, physical_targets))
}

pub(super) fn validate_speedtest_isolation_id(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.len() == 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "YDB speedtest isolation ID must contain exactly 32 lowercase hexadecimal characters"
    );
    Ok(())
}

pub(super) fn ydb_speedtest_table(
    production_path: &str,
    isolation_id: &str,
    index: usize,
) -> anyhow::Result<(String, String)> {
    validate_speedtest_isolation_id(isolation_id)?;
    let (parent, production_name) = production_path.rsplit_once('/').ok_or_else(|| {
        anyhow::anyhow!("YDB production table path '{production_path}' has no parent")
    })?;
    anyhow::ensure!(
        production_path.starts_with('/'),
        "YDB production table path '{production_path}' must be absolute"
    );
    anyhow::ensure!(
        !production_name.is_empty(),
        "YDB production table path '{production_path}' has no table name"
    );
    let scratch_name = format!("{SPEEDTEST_TABLE_PREFIX}{isolation_id}_{index:x}");
    anyhow::ensure!(
        scratch_name.len() <= YDB_PATH_SEGMENT_MAX_BYTES,
        "YDB speedtest table name exceeds {YDB_PATH_SEGMENT_MAX_BYTES} bytes"
    );
    let scratch_path = format!("{parent}/{scratch_name}");
    anyhow::ensure!(
        scratch_path != production_path,
        "YDB speedtest table must not alias the production table"
    );
    Ok((scratch_name, scratch_path))
}

pub(super) fn is_ydb_speedtest_table_name(value: &str) -> bool {
    let Some((id, index)) = value
        .strip_prefix(SPEEDTEST_TABLE_PREFIX)
        .and_then(|suffix| suffix.split_once('_'))
    else {
        return false;
    };
    validate_speedtest_isolation_id(id).is_ok()
        && !index.is_empty()
        && index.len() <= 16
        && index
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn ydb_physical_target(database: &str, path: &str) -> anyhow::Result<Arc<str>> {
    Ok(Arc::from(serde_json::to_string(&(database, path))?))
}

pub(super) fn physical_target_set(
    targets: &[SpeedtestPhysicalTarget],
) -> BTreeSet<(Arc<str>, Arc<str>)> {
    targets
        .iter()
        .map(|target| (Arc::clone(&target.production), Arc::clone(&target.scratch)))
        .collect()
}

pub(super) fn validate_ydb_cleanup_scope(
    config: &YdbSinkConfig,
    isolation: &SinkSpeedtestIsolation,
    scope: &YdbSpeedtestScope,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        isolation.safety() == SinkSpeedtestIsolationSafety::Scratch,
        "refusing to clean a YDB speedtest isolation without scratch safety"
    );
    let discovered = isolation
        .discovery
        .datasets
        .iter()
        .map(|dataset| Arc::clone(&dataset.name))
        .collect::<BTreeSet<_>>();
    let owned = scope.tables.keys().cloned().collect::<BTreeSet<_>>();
    anyhow::ensure!(
        discovered == owned,
        "refusing to clean YDB speedtest tables: isolated discovery does not match the connector-owned table set"
    );
    validate_ydb_connector_scope(config, scope)?;
    let declared_paths = scope.tables.values().cloned().collect::<BTreeSet<_>>();
    anyhow::ensure!(
        scope.attempted_paths()?.is_subset(&declared_paths),
        "refusing to clean YDB speedtest tables: ownership state contains an undeclared path"
    );
    anyhow::ensure!(
        physical_target_set(isolation.physical_targets()) == scope.physical_targets,
        "refusing to clean YDB speedtest tables: physical target proof does not match the connector-owned scratch set"
    );
    let expected_scratch = scope
        .tables
        .values()
        .map(|path| ydb_physical_target(&config.connection.database, path))
        .collect::<anyhow::Result<BTreeSet<_>>>()?;
    let actual_scratch = scope
        .physical_targets
        .iter()
        .map(|(_, scratch)| Arc::clone(scratch))
        .collect::<BTreeSet<_>>();
    anyhow::ensure!(
        actual_scratch == expected_scratch,
        "refusing to clean YDB speedtest tables: scratch physical targets do not match the connector-owned paths"
    );
    Ok(())
}

fn validate_ydb_connector_scope(
    config: &YdbSinkConfig,
    scope: &YdbSpeedtestScope,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        config.create_tables,
        "refusing to use YDB speedtest tables from a connector without isolated create semantics"
    );
    let configured = config
        .tables
        .iter()
        .map(|table| (Arc::from(table.name()), Arc::from(table.path.as_str())))
        .collect::<BTreeMap<Arc<str>, Arc<str>>>();
    anyhow::ensure!(
        configured == scope.tables,
        "refusing to use YDB speedtest tables: connector config does not match its owned table set"
    );
    let declared_paths = scope.tables.values().cloned().collect::<BTreeSet<_>>();
    anyhow::ensure!(
        scope.schemas.keys().cloned().collect::<BTreeSet<_>>() == declared_paths,
        "refusing to use YDB speedtest tables: schema proofs do not exactly match the owned table set"
    );
    anyhow::ensure!(
        scope.tables.iter().all(|(name, path)| {
            is_ydb_speedtest_table_name(name) && path.rsplit('/').next() == Some(name.as_ref())
        }),
        "refusing to use a YDB table outside the speedtest namespace"
    );
    Ok(())
}

const YDB_SPEEDTEST_OWNER_ATTRIBUTE: &str = "transferia.speedtest.owner";

pub(super) fn create_speedtest_table_request(
    path: &str,
    schema: &DatasetSchema,
    owner: &str,
) -> anyhow::Result<CreateTableRequest> {
    let name = path.rsplit('/').next().unwrap_or_default();
    anyhow::ensure!(
        is_ydb_speedtest_table_name(name),
        "refusing to create YDB table outside the speedtest namespace"
    );
    let columns = schema
        .columns
        .iter()
        .map(|column| {
            Ok(ColumnMeta {
                name: column.name.clone(),
                r#type: Some(ydb_type(column)?),
                family: String::new(),
                not_null: Some(!column.nullable),
                default_value: None,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let primary_key = schema
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .map(|column| column.name.clone())
        .collect();
    Ok(CreateTableRequest {
        session_id: String::new(),
        path: path.to_owned(),
        columns,
        primary_key,
        profile: None,
        operation_params: None,
        indexes: Vec::new(),
        ttl_settings: None,
        storage_settings: None,
        column_families: Vec::new(),
        attributes: HashMap::from([(YDB_SPEEDTEST_OWNER_ATTRIBUTE.to_owned(), owner.to_owned())]),
        compaction_policy: String::new(),
        partitioning_settings: None,
        key_bloom_filter: 0,
        read_replicas_settings: None,
        tiering: String::new(),
        temporary: false,
        store_type: 0,
        partitions: None,
    })
}

pub(super) fn verify_ydb_speedtest_description(
    path: &str,
    schema: &DatasetSchema,
    expected_owner: &str,
    description: &DescribeTableResult,
) -> anyhow::Result<()> {
    let owner = description
        .attributes
        .get(YDB_SPEEDTEST_OWNER_ATTRIBUTE)
        .ok_or_else(|| anyhow::anyhow!("YDB speedtest owner marker is missing"))?;
    anyhow::ensure!(
        owner == expected_owner,
        "YDB speedtest table is owned by a different operation"
    );
    let actual = column_plans(description.columns.clone(), &description.primary_key)?;
    ensure_table_schema(path, schema, &actual)
}

pub(super) async fn prepare_ydb_speedtest_table<C: YdbSpeedtestTableClient>(
    client: &mut C,
    path: &str,
    schema: &DatasetSchema,
    owner: &str,
) -> anyhow::Result<()> {
    let request = create_speedtest_table_request(path, schema, owner)?;
    let create_error = observe_external_request(
        "ydb",
        "speedtest_create_table",
        client.create_owned(request),
    )
    .await
    .err();
    let description = observe_external_request(
        "ydb",
        "speedtest_describe_table",
        client.describe_owned(path),
    )
    .await
    .map_err(|proof_error| match create_error {
        Some(create_error) => anyhow::anyhow!(
            "YDB speedtest table creation failed and ownership could not be proven: create={create_error:#}; proof={proof_error:#}"
        ),
        None => proof_error,
    })?;
    verify_ydb_speedtest_description(path, schema, owner, &description)
}

async fn verify_ydb_speedtest_scope<C: YdbSpeedtestTableClient>(
    client: &mut C,
    scope: &YdbSpeedtestScope,
) -> anyhow::Result<()> {
    for (path, schema) in &scope.schemas {
        let description = observe_external_request(
            "ydb",
            "speedtest_describe_table",
            client.describe_owned(path),
        )
        .await
        .map_err(|error| {
            anyhow::anyhow!(
                "refusing to use YDB speedtest table '{path}' because ownership could not be read: {error:#}"
            )
        })?;
        verify_ydb_speedtest_description(path, schema, &scope.owner, &description).map_err(
            |error| anyhow::anyhow!("refusing to use YDB speedtest table '{path}': {error:#}"),
        )?;
    }
    Ok(())
}

pub(super) async fn cleanup_ydb_speedtest_table<C: YdbSpeedtestTableClient>(
    client: &mut C,
    path: &str,
    schema: &DatasetSchema,
    owner: &str,
) -> anyhow::Result<()> {
    let description = match observe_external_request(
        "ydb",
        "speedtest_describe_table",
        client.describe_owned(path),
    )
    .await
    {
        Ok(description) => description,
        Err(error) if client.is_not_found(&error) => return Ok(()),
        Err(error) => {
            anyhow::bail!("refusing to drop because current ownership could not be read: {error:#}")
        }
    };
    verify_ydb_speedtest_description(path, schema, owner, &description)
        .map_err(|error| anyhow::anyhow!("refusing to drop: {error:#}"))?;

    let drop_error =
        observe_external_request("ydb", "speedtest_drop_table", client.drop_owned(path))
            .await
            .err();
    let Some(drop_error) = drop_error else {
        return Ok(());
    };
    match observe_external_request(
        "ydb",
        "speedtest_describe_table_after_drop",
        client.describe_owned(path),
    )
    .await
    {
        Err(probe_error) if client.is_not_found(&probe_error) => Ok(()),
        Err(probe_error) => Err(anyhow::anyhow!(
            "drop failed and absence could not be proven: drop={drop_error:#}; probe={probe_error:#}"
        )),
        Ok(current) => {
            let current_proof = verify_ydb_speedtest_description(path, schema, owner, &current);
            Err(match current_proof {
                Ok(()) => anyhow::anyhow!("drop failed and the owned table is still present: {drop_error:#}"),
                Err(error) => anyhow::anyhow!(
                    "drop result was ambiguous and the path now has different ownership/schema; preserving it: drop={drop_error:#}; current={error:#}"
                ),
            })
        }
    }
}

pub(super) async fn cleanup_ydb_speedtest_scope<C: YdbSpeedtestTableClient>(
    client: &mut C,
    scope: &YdbSpeedtestScope,
) -> anyhow::Result<()> {
    let mut failures = Vec::new();
    for path in scope.attempted_paths()? {
        let Some(schema) = scope.schemas.get(&path) else {
            failures.push(format!("'{path}': owned schema proof is missing"));
            continue;
        };
        match cleanup_ydb_speedtest_table(client, &path, schema, &scope.owner).await {
            Ok(()) => scope.record_cleaned(&path)?,
            Err(error) => failures.push(format!("'{path}': {error:#}")),
        }
    }
    anyhow::ensure!(
        failures.is_empty(),
        "failed to remove YDB speedtest tables: {}",
        failures.join("; ")
    );
    Ok(())
}

async fn execute_scheme_query_with_retry(
    client: &mut YdbClient,
    query: String,
    retry_max_ms: u64,
) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let mut delay = std::time::Duration::from_millis(20);
    loop {
        match client.execute_scheme_query(query.clone()).await {
            Ok(()) => return Ok(()),
            Err(error)
                if is_retryable_error(&error)
                    && started.elapsed() < std::time::Duration::from_millis(retry_max_ms) =>
            {
                tracing::warn!(error = %error, delay_ms = delay.as_millis(), "retrying transient YDB schema operation");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(std::time::Duration::from_secs(1));
            }
            Err(error) => return Err(error),
        }
    }
}

async fn describe_table_with_retry(
    client: &mut YdbClient,
    path: &str,
    retry_max_ms: u64,
) -> anyhow::Result<ydb_grpc::ydb_proto::table::DescribeTableResult> {
    let started = std::time::Instant::now();
    let mut delay = std::time::Duration::from_millis(20);
    loop {
        match client.describe_table(path.to_owned()).await {
            Ok(description) => return Ok(description),
            Err(error)
                if is_retryable_error(&error)
                    && started.elapsed() < std::time::Duration::from_millis(retry_max_ms) =>
            {
                tracing::warn!(table = path, error = %error, delay_ms = delay.as_millis(), "retrying transient YDB table description");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(std::time::Duration::from_secs(1));
            }
            Err(error) => return Err(error),
        }
    }
}

struct YdbSink {
    client: YdbClient,
    table_paths: HashMap<String, String>,
    counters: Arc<SinkCounters>,
    discovery: Arc<DeliveryDiscovery>,
    limits: Arc<dyn SinkLimits>,
    speedtest_scope: Option<Arc<YdbSpeedtestScope>>,
}

struct EncodedBatch {
    path: String,
    schema: Vec<u8>,
    data: Vec<u8>,
    rows: u64,
    bytes: u64,
}

enum EncodedAction {
    Upsert(EncodedBatch),
    Update {
        path: String,
        query: String,
        parameters: HashMap<String, TypedValue>,
        rows: u64,
        bytes: u64,
    },
    Delete {
        path: String,
        query: String,
        parameters: HashMap<String, TypedValue>,
        rows: u64,
        bytes: u64,
    },
}

impl YdbSink {
    async fn write_delivery(&mut self, delivery: &Delivery) -> anyhow::Result<()> {
        if let Some(scope) = &self.speedtest_scope {
            verify_ydb_speedtest_scope(&mut self.client, scope).await?;
        }
        for batch in &delivery.outputs {
            self.limits.validate_batch(&self.discovery, batch)?;
        }
        let work = delivery
            .outputs
            .iter()
            .filter(|batch| batch.rows() > 0)
            .map(|batch| {
                let path = self
                    .table_paths
                    .get(batch.table.as_ref())
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "YDB sink has no physical table mapping for dataset '{}'",
                            batch.table
                        )
                    })?;
                Ok((
                    path,
                    project_sink_batch(&self.discovery, batch)?,
                    batch.bytes() as u64,
                ))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        // IPC serialization is CPU work. Keep it off Tokio's I/O workers, and
        // finish every conversion before the first irreversible BulkUpsert.
        let encoded = tokio::task::spawn_blocking(move || {
            let mut encoded = Vec::new();
            for (path, projected, source_bytes) in work {
                match projected {
                    ProjectedSinkBatch::AppendOnly(batch) => {
                        encoded.push(EncodedAction::Upsert(encode_upsert(
                            path,
                            &batch,
                            source_bytes,
                        )?));
                    }
                    ProjectedSinkBatch::Changelog(changelog) => {
                        for run in changelog.collapsed_runs()? {
                            let rows = run.batch.num_rows() as u64;
                            let bytes = run.batch.get_array_memory_size() as u64;
                            match run.operation {
                                transferia_core::ChangeOperation::Create
                                | transferia_core::ChangeOperation::SnapshotRead => {
                                    encoded.push(EncodedAction::Upsert(encode_upsert(
                                        path.clone(),
                                        &run.batch,
                                        bytes,
                                    )?));
                                }
                                transferia_core::ChangeOperation::Update => {
                                    let columns =
                                        columns_for_batch(&run.batch, &changelog.stored_columns)?;
                                    let (query, parameters) = encode_update(
                                        &path,
                                        &run.batch,
                                        &columns,
                                        &changelog.primary_key_columns,
                                    )?;
                                    encoded.push(EncodedAction::Update {
                                        path: path.clone(),
                                        query,
                                        parameters,
                                        rows,
                                        bytes,
                                    });
                                }
                                transferia_core::ChangeOperation::Delete => {
                                    let (query, parameters) = encode_delete(
                                        &path,
                                        &run.batch,
                                        &changelog.primary_key_columns,
                                    )?;
                                    encoded.push(EncodedAction::Delete {
                                        path: path.clone(),
                                        query,
                                        parameters,
                                        rows,
                                        bytes,
                                    });
                                }
                            }
                        }
                    }
                }
            }
            Ok::<_, anyhow::Error>(encoded)
        })
        .await??;

        let started = std::time::Instant::now();
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        for action in encoded {
            let (action_rows, action_bytes) = match action {
                EncodedAction::Upsert(batch) => {
                    self.client
                        .bulk_upsert(batch.path, batch.schema, batch.data)
                        .await?;
                    (batch.rows, batch.bytes)
                }
                EncodedAction::Update {
                    path,
                    query,
                    parameters,
                    rows,
                    bytes,
                } => {
                    self.client
                        .execute_checked_update(query, parameters, rows)
                        .await
                        .map_err(|error| error.context(format!("updating rows in '{path}'")))?;
                    (rows, bytes)
                }
                EncodedAction::Delete {
                    path,
                    query,
                    parameters,
                    rows,
                    bytes,
                } => {
                    self.client
                        .execute_data_query(query, parameters)
                        .await
                        .map_err(|error| error.context(format!("deleting rows from '{path}'")))?;
                    (rows, bytes)
                }
            };
            rows += action_rows;
            bytes += action_bytes;
            self.counters.add_flush();
        }
        self.counters.add_busy(started.elapsed());
        self.counters.add_rows(rows);
        self.counters.add_bytes(bytes);
        Ok(())
    }
}

fn encode_upsert(path: String, batch: &RecordBatch, bytes: u64) -> anyhow::Result<EncodedBatch> {
    let rows = batch.num_rows() as u64;
    let (schema, data) = encode_arrow_batch(batch)?;
    Ok(EncodedBatch {
        path,
        schema,
        data,
        rows,
        bytes,
    })
}

pub(super) fn encode_delete(
    path: &str,
    batch: &RecordBatch,
    columns: &[SchemaColumn],
) -> anyhow::Result<(String, HashMap<String, TypedValue>)> {
    encode_row_query(path, batch, columns, RowQuery::Delete, columns)
}

pub(super) fn encode_update(
    path: &str,
    batch: &RecordBatch,
    columns: &[SchemaColumn],
    primary_keys: &[SchemaColumn],
) -> anyhow::Result<(String, HashMap<String, TypedValue>)> {
    encode_row_query(path, batch, columns, RowQuery::Update, primary_keys)
}

#[derive(Clone, Copy)]
enum RowQuery {
    Update,
    Delete,
}

fn columns_for_batch(
    batch: &RecordBatch,
    stored_columns: &[SchemaColumn],
) -> anyhow::Result<Vec<SchemaColumn>> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            stored_columns
                .iter()
                .find(|column| column.name == field.name().as_str())
                .cloned()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "YDB changelog batch column '{}' is absent from discovery",
                        field.name()
                    )
                })
        })
        .collect()
}

fn encode_row_query(
    path: &str,
    batch: &RecordBatch,
    columns: &[SchemaColumn],
    operation: RowQuery,
    primary_keys: &[SchemaColumn],
) -> anyhow::Result<(String, HashMap<String, TypedValue>)> {
    anyhow::ensure!(
        batch.num_columns() == columns.len(),
        "YDB delete key batch has {} columns, expected {}",
        batch.num_columns(),
        columns.len()
    );
    let declared = columns
        .iter()
        .map(|column| {
            Ok(format!(
                "{}:{}{}",
                quote_identifier(&column.name),
                yql_type(column)?,
                if column.nullable { "?" } else { "" }
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(", ");
    let selected = columns
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let statement = match operation {
        RowQuery::Update => {
            let predicate = primary_keys
                .iter()
                .map(|column| {
                    let column = quote_identifier(&column.name);
                    format!("target.{column} = staged.{column}")
                })
                .collect::<Vec<_>>()
                .join(" AND ");
            format!(
                "SELECT COUNT(*) AS matched FROM {} AS target INNER JOIN AS_TABLE($batch) AS staged ON {predicate};\nUPDATE {} ON SELECT {selected} FROM AS_TABLE($batch);",
                quote_identifier(path),
                quote_identifier(path),
            )
        }
        RowQuery::Delete => format!(
            "DELETE FROM {} ON SELECT {selected} FROM AS_TABLE($batch);",
            quote_identifier(path)
        ),
    };
    let query = format!("--!syntax_v1\nDECLARE $batch AS List<Struct<{declared}>>;\n{statement}");
    let members = columns
        .iter()
        .map(|column| {
            Ok(StructMember {
                name: column.name.clone(),
                r#type: Some(ydb_type(column)?),
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let items = (0..batch.num_rows())
        .map(|row| {
            let items = columns
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    ydb_parameter_value(batch.column(index).as_ref(), row, column)
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            Ok(Value {
                items,
                ..Value::default()
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let parameter = TypedValue {
        r#type: Some(Type {
            r#type: Some(TypeVariant::ListType(Box::new(ListType {
                item: Some(Box::new(Type {
                    r#type: Some(TypeVariant::StructType(StructType { members })),
                })),
            }))),
        }),
        value: Some(Value {
            items,
            ..Value::default()
        }),
    };
    Ok((query, HashMap::from([("$batch".to_owned(), parameter)])))
}

impl Sink for YdbSink {
    fn run(
        mut self: Box<Self>,
        mut io: SinkIo,
    ) -> BoxFuture<'static, transferia_core::failure::DataPlaneResult<()>> {
        Box::pin(async move {
            let result: anyhow::Result<()> = async {
                while let Some(delivery) = tokio::select! {
                    biased;
                    () = io.cancellation.cancelled() => None,
                    delivery = io.deliveries.recv() => delivery,
                } {
                    let id = delivery.id;
                    let source_messages = delivery.meta.source_messages;
                    self.write_delivery(&delivery).await?;
                    self.counters.add_source_messages(source_messages);
                    io.events
                        .send(SinkEvent::CommittedThrough(id))
                        .await
                        .map_err(|_| anyhow::anyhow!("YDB sink event receiver closed"))?;
                }
                Ok(())
            }
            .await;
            result.map_err(DataPlaneFailure::retryable_or_passthrough)
        })
    }
}

pub(super) fn create_table_query(
    path: &str,
    schema: &transferia_core::data::schema::DatasetSchema,
) -> anyhow::Result<String> {
    let columns = schema
        .columns
        .iter()
        .map(|column| {
            Ok(format!(
                "{} {}{}",
                quote_identifier(&column.name),
                yql_type(column)?,
                if column.nullable { "" } else { " NOT NULL" }
            ))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let primary_key = schema
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !primary_key.is_empty(),
        "YDB table '{path}' requires a primary key"
    );
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({}, PRIMARY KEY ({}));",
        quote_identifier(path),
        columns.join(", "),
        primary_key.join(", ")
    ))
}

fn ensure_table_schema(
    path: &str,
    expected: &transferia_core::data::schema::DatasetSchema,
    actual: &[super::types::ColumnPlan],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        expected.columns.len() == actual.len(),
        "YDB table '{path}' has {} columns, delivery requires {}",
        actual.len(),
        expected.columns.len()
    );
    for (expected, actual) in expected.columns.iter().zip(actual) {
        let kind = column_kind(expected)?;
        anyhow::ensure!(
            expected.name == actual.name
                && expected.nullable == actual.nullable
                && expected.primary_key == actual.primary_key
                && kind == actual.kind,
            "YDB table '{path}' column '{}' does not match delivery discovery",
            expected.name
        );
    }
    Ok(())
}

fn validate_name(kind: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!value.is_empty(), "YDB {kind} name must not be empty");
    anyhow::ensure!(
        !value.contains('\0'),
        "YDB {kind} name must not contain NUL"
    );
    Ok(())
}

fn quote_identifier(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn yql_type(column: &SchemaColumn) -> anyhow::Result<String> {
    Ok(match column_kind(column)? {
        ColumnKind::Bool => "Bool".to_owned(),
        ColumnKind::Int8 => "Int8".to_owned(),
        ColumnKind::UInt8 => "Uint8".to_owned(),
        ColumnKind::Int16 => "Int16".to_owned(),
        ColumnKind::UInt16 => "Uint16".to_owned(),
        ColumnKind::Int32 => "Int32".to_owned(),
        ColumnKind::UInt32 => "Uint32".to_owned(),
        ColumnKind::Int64 => "Int64".to_owned(),
        ColumnKind::UInt64 => "Uint64".to_owned(),
        ColumnKind::Float32 => "Float".to_owned(),
        ColumnKind::Float64 => "Double".to_owned(),
        ColumnKind::Date32 => "Date32".to_owned(),
        ColumnKind::TimestampSecond => "Datetime64".to_owned(),
        ColumnKind::TimestampMicrosecond => "Timestamp64".to_owned(),
        ColumnKind::DurationMicrosecond => "Interval64".to_owned(),
        ColumnKind::Binary(None) => "String".to_owned(),
        ColumnKind::Binary(Some(YDB_YSON_EXTENSION)) => "Yson".to_owned(),
        ColumnKind::Utf8(None) => "Utf8".to_owned(),
        ColumnKind::Utf8(Some(ARROW_JSON_EXTENSION_NAME)) => "Json".to_owned(),
        ColumnKind::Utf8(Some(YDB_DYNUMBER_EXTENSION)) => "DyNumber".to_owned(),
        ColumnKind::Utf8(Some(YDB_TZ_DATE_EXTENSION)) => "TzDate".to_owned(),
        ColumnKind::Utf8(Some(YDB_TZ_DATETIME_EXTENSION)) => "TzDatetime".to_owned(),
        ColumnKind::Utf8(Some(YDB_TZ_TIMESTAMP_EXTENSION)) => "TzTimestamp".to_owned(),
        ColumnKind::Decimal { precision, scale } => format!("Decimal({precision}, {scale})"),
        ColumnKind::Uuid => "Uuid".to_owned(),
        ColumnKind::Binary(Some(extension)) | ColumnKind::Utf8(Some(extension)) => {
            anyhow::bail!("unsupported Arrow extension '{extension}' for YDB sink")
        }
    })
}

fn column_kind(column: &SchemaColumn) -> anyhow::Result<ColumnKind> {
    let extension = column.arrow_extension_name;
    Ok(match (&column.data_type, extension) {
        (DataType::Boolean, None) => ColumnKind::Bool,
        (DataType::Int8, None) => ColumnKind::Int8,
        (DataType::UInt8, None) => ColumnKind::UInt8,
        (DataType::Int16, None) => ColumnKind::Int16,
        (DataType::UInt16, None) => ColumnKind::UInt16,
        (DataType::Int32, None) => ColumnKind::Int32,
        (DataType::UInt32, None) => ColumnKind::UInt32,
        (DataType::Int64, None) => ColumnKind::Int64,
        (DataType::UInt64, None) => ColumnKind::UInt64,
        (DataType::Float32, None) => ColumnKind::Float32,
        (DataType::Float64, None) => ColumnKind::Float64,
        (DataType::Date32, None) => ColumnKind::Date32,
        (DataType::Timestamp(TimeUnit::Second, None), None) => ColumnKind::TimestampSecond,
        (DataType::Timestamp(TimeUnit::Microsecond, None), None) => {
            ColumnKind::TimestampMicrosecond
        }
        (DataType::Duration(TimeUnit::Microsecond), None) => ColumnKind::DurationMicrosecond,
        (DataType::Binary, None) => ColumnKind::Binary(None),
        (DataType::Binary, Some(YDB_YSON_EXTENSION)) => {
            ColumnKind::Binary(Some(YDB_YSON_EXTENSION))
        }
        (DataType::Utf8, None) => ColumnKind::Utf8(None),
        (DataType::Utf8, Some(ARROW_JSON_EXTENSION_NAME)) => {
            ColumnKind::Utf8(Some(ARROW_JSON_EXTENSION_NAME))
        }
        (DataType::Utf8, Some(YDB_DYNUMBER_EXTENSION)) => {
            ColumnKind::Utf8(Some(YDB_DYNUMBER_EXTENSION))
        }
        (DataType::Utf8, Some(YDB_TZ_DATE_EXTENSION)) => {
            ColumnKind::Utf8(Some(YDB_TZ_DATE_EXTENSION))
        }
        (DataType::Utf8, Some(YDB_TZ_DATETIME_EXTENSION)) => {
            ColumnKind::Utf8(Some(YDB_TZ_DATETIME_EXTENSION))
        }
        (DataType::Utf8, Some(YDB_TZ_TIMESTAMP_EXTENSION)) => {
            ColumnKind::Utf8(Some(YDB_TZ_TIMESTAMP_EXTENSION))
        }
        (DataType::Decimal128(precision, scale), None) if *precision <= 35 => ColumnKind::Decimal {
            precision: *precision,
            scale: *scale,
        },
        (DataType::FixedSizeBinary(16), Some(ARROW_UUID_EXTENSION)) => ColumnKind::Uuid,
        (data_type, extension) => anyhow::bail!(
            "unsupported Arrow type {data_type:?} with extension {extension:?} for YDB sink"
        ),
    })
}

fn ydb_type(column: &SchemaColumn) -> anyhow::Result<Type> {
    let kind = column_kind(column)?;
    let scalar = match kind {
        ColumnKind::Decimal { precision, scale } => {
            let scale = u32::try_from(scale).map_err(|_| {
                anyhow::anyhow!(
                    "YDB primary-key Decimal column '{}' has negative scale {scale}",
                    column.name
                )
            })?;
            TypeVariant::DecimalType(DecimalType {
                precision: u32::from(precision),
                scale,
            })
        }
        kind => TypeVariant::TypeId(ydb_primitive_type(&kind)?.into()),
    };
    let scalar = Type {
        r#type: Some(scalar),
    };
    Ok(if column.nullable {
        Type {
            r#type: Some(TypeVariant::OptionalType(Box::new(OptionalType {
                item: Some(Box::new(scalar)),
            }))),
        }
    } else {
        scalar
    })
}

fn ydb_primitive_type(kind: &ColumnKind) -> anyhow::Result<PrimitiveTypeId> {
    Ok(match kind {
        ColumnKind::Bool => PrimitiveTypeId::Bool,
        ColumnKind::Int8 => PrimitiveTypeId::Int8,
        ColumnKind::UInt8 => PrimitiveTypeId::Uint8,
        ColumnKind::Int16 => PrimitiveTypeId::Int16,
        ColumnKind::UInt16 => PrimitiveTypeId::Uint16,
        ColumnKind::Int32 => PrimitiveTypeId::Int32,
        ColumnKind::UInt32 => PrimitiveTypeId::Uint32,
        ColumnKind::Int64 => PrimitiveTypeId::Int64,
        ColumnKind::UInt64 => PrimitiveTypeId::Uint64,
        ColumnKind::Float32 => PrimitiveTypeId::Float,
        ColumnKind::Float64 => PrimitiveTypeId::Double,
        ColumnKind::Date32 => PrimitiveTypeId::Date32,
        ColumnKind::TimestampSecond => PrimitiveTypeId::Datetime64,
        ColumnKind::TimestampMicrosecond => PrimitiveTypeId::Timestamp64,
        ColumnKind::DurationMicrosecond => PrimitiveTypeId::Interval64,
        ColumnKind::Binary(None) => PrimitiveTypeId::String,
        ColumnKind::Binary(Some(YDB_YSON_EXTENSION)) => PrimitiveTypeId::Yson,
        ColumnKind::Utf8(None) => PrimitiveTypeId::Utf8,
        ColumnKind::Utf8(Some(ARROW_JSON_EXTENSION_NAME)) => PrimitiveTypeId::Json,
        ColumnKind::Utf8(Some(YDB_DYNUMBER_EXTENSION)) => PrimitiveTypeId::Dynumber,
        ColumnKind::Utf8(Some(YDB_TZ_DATE_EXTENSION)) => PrimitiveTypeId::TzDate,
        ColumnKind::Utf8(Some(YDB_TZ_DATETIME_EXTENSION)) => PrimitiveTypeId::TzDatetime,
        ColumnKind::Utf8(Some(YDB_TZ_TIMESTAMP_EXTENSION)) => PrimitiveTypeId::TzTimestamp,
        ColumnKind::Uuid => PrimitiveTypeId::Uuid,
        ColumnKind::Binary(Some(_)) | ColumnKind::Utf8(Some(_)) | ColumnKind::Decimal { .. } => {
            return Err(anyhow::anyhow!("YDB type is not primary-key compatible"));
        }
    })
}

fn ydb_parameter_value(
    array: &dyn Array,
    row: usize,
    column: &SchemaColumn,
) -> anyhow::Result<Value> {
    if array.is_null(row) {
        anyhow::ensure!(
            column.nullable,
            "YDB row has NULL in non-nullable column '{}' at row {row}",
            column.name
        );
        return Ok(Value {
            value: Some(ValueVariant::NullFlagValue(0)),
            ..Value::default()
        });
    }
    macro_rules! value {
        ($array:ty, $variant:ident) => {{
            let array = array.as_any().downcast_ref::<$array>().ok_or_else(|| {
                anyhow::anyhow!(
                    "YDB primary-key column '{}' is not {:?}",
                    column.name,
                    column.data_type
                )
            })?;
            Value {
                value: Some(ValueVariant::$variant(array.value(row).into())),
                ..Value::default()
            }
        }};
    }
    Ok(match column_kind(column)? {
        ColumnKind::Bool => value!(BooleanArray, BoolValue),
        ColumnKind::Int8 => value!(Int8Array, Int32Value),
        ColumnKind::UInt8 => value!(UInt8Array, Uint32Value),
        ColumnKind::Int16 => value!(Int16Array, Int32Value),
        ColumnKind::UInt16 => value!(UInt16Array, Uint32Value),
        ColumnKind::Int32 => value!(Int32Array, Int32Value),
        ColumnKind::UInt32 => value!(UInt32Array, Uint32Value),
        ColumnKind::Int64 => value!(Int64Array, Int64Value),
        ColumnKind::UInt64 => value!(UInt64Array, Uint64Value),
        ColumnKind::Float32 => value!(Float32Array, FloatValue),
        ColumnKind::Float64 => value!(Float64Array, DoubleValue),
        ColumnKind::Date32 => {
            let array = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| anyhow::anyhow!("YDB Date primary key is not Arrow Date32"))?;
            Value {
                value: Some(ValueVariant::Int32Value(array.value(row))),
                ..Value::default()
            }
        }
        ColumnKind::TimestampSecond => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| {
                    anyhow::anyhow!("YDB Datetime primary key is not TimestampSecond")
                })?;
            Value {
                value: Some(ValueVariant::Int64Value(array.value(row))),
                ..Value::default()
            }
        }
        ColumnKind::TimestampMicrosecond => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    anyhow::anyhow!("YDB Timestamp primary key is not TimestampMicrosecond")
                })?;
            Value {
                value: Some(ValueVariant::Int64Value(array.value(row))),
                ..Value::default()
            }
        }
        ColumnKind::DurationMicrosecond => {
            let array = array
                .as_any()
                .downcast_ref::<DurationMicrosecondArray>()
                .ok_or_else(|| {
                    anyhow::anyhow!("YDB Interval64 value is not DurationMicrosecond")
                })?;
            Value {
                value: Some(ValueVariant::Int64Value(array.value(row))),
                ..Value::default()
            }
        }
        ColumnKind::Binary(None | Some(YDB_YSON_EXTENSION)) => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow::anyhow!("YDB String primary key is not Arrow Binary"))?;
            Value {
                value: Some(ValueVariant::BytesValue(array.value(row).to_vec())),
                ..Value::default()
            }
        }
        ColumnKind::Utf8(
            None
            | Some(
                ARROW_JSON_EXTENSION_NAME
                | YDB_DYNUMBER_EXTENSION
                | YDB_TZ_DATE_EXTENSION
                | YDB_TZ_DATETIME_EXTENSION
                | YDB_TZ_TIMESTAMP_EXTENSION,
            ),
        ) => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("YDB Utf8 primary key is not Arrow Utf8"))?;
            Value {
                value: Some(ValueVariant::TextValue(array.value(row).to_owned())),
                ..Value::default()
            }
        }
        ColumnKind::Decimal { .. } => {
            let array = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| anyhow::anyhow!("YDB Decimal primary key is not Decimal128"))?;
            let bits = array.value(row).cast_unsigned();
            Value {
                high_128: (bits >> 64) as u64,
                value: Some(ValueVariant::Low128(bits as u64)),
                ..Value::default()
            }
        }
        ColumnKind::Uuid => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| anyhow::anyhow!("YDB Uuid primary key is not FixedSizeBinary"))?;
            let uuid = uuid::Uuid::from_slice(array.value(row))?;
            let little_endian = uuid.to_bytes_le();
            let low = u64::from_le_bytes(little_endian[..8].try_into()?);
            let high = u64::from_le_bytes(little_endian[8..].try_into()?);
            Value {
                high_128: high,
                value: Some(ValueVariant::Low128(low)),
                ..Value::default()
            }
        }
        ColumnKind::Binary(Some(_)) | ColumnKind::Utf8(Some(_)) => anyhow::bail!(
            "YDB column '{}' uses a type that is not primary-key compatible",
            column.name
        ),
    })
}

fn ensure_primary_key_type(kind: &ColumnKind, column: &SchemaColumn) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(
            kind,
            ColumnKind::Bool
                | ColumnKind::Int8
                | ColumnKind::UInt8
                | ColumnKind::Int16
                | ColumnKind::UInt16
                | ColumnKind::Int32
                | ColumnKind::UInt32
                | ColumnKind::Int64
                | ColumnKind::UInt64
                | ColumnKind::Date32
                | ColumnKind::TimestampSecond
                | ColumnKind::TimestampMicrosecond
                | ColumnKind::Binary(None)
                | ColumnKind::Utf8(None)
                | ColumnKind::Decimal { .. }
                | ColumnKind::Uuid
        ),
        "YDB type for primary-key column '{}' is not key-compatible",
        column.name
    );
    Ok(())
}

pub(super) fn encode_arrow_batch(batch: &RecordBatch) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    let fields = batch
        .schema()
        .fields()
        .iter()
        .map(|field| Field::new(field.name(), field.data_type().clone(), field.is_nullable()))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), batch.columns().to_vec())?;
    let options = IpcWriteOptions::default();
    let generator = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(true);
    let mut compression = CompressionContext::default();
    let schema_message = generator.schema_to_bytes_with_dictionary_tracker(
        batch.schema().as_ref(),
        &mut dictionary_tracker,
        &options,
    );
    let mut schema = Vec::new();
    write_message(&mut schema, schema_message, &options)?;
    let (dictionaries, record_batch) =
        generator.encode(&batch, &mut dictionary_tracker, &options, &mut compression)?;
    anyhow::ensure!(
        dictionaries.is_empty(),
        "YDB BulkUpsert does not accept Arrow dictionary side messages"
    );
    let mut data = Vec::new();
    write_message(&mut data, record_batch, &options)?;
    Ok((schema, data))
}
