use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use arrow::datatypes::DataType;
use futures_util::future::BoxFuture;
use reqwest::{Method, StatusCode};
use transferia_core::data::schema::{DatasetSchema, SchemaColumn, ARROW_JSON_EXTENSION_NAME};
use transferia_core::delivery::{
    validate_stored_projection, ArrowTypeFamily, DeliveryDiscovery, NameSyntax, SinkLimits,
    SinkLimitsDescription, TextLimit,
};
use transferia_core::sink::Sink;
use transferia_delivery_contracts::semantics::EndpointDescriptor;
use transferia_registry::{
    ConnectionCheckResult, SinkBuildContext, SinkConnector, SinkPrepare, SinkSpeedtestIsolation,
    SpeedtestPhysicalTarget,
};

use super::super::{validate_index_name, OpenSearchClient, OpenSearchHttpError};
use super::actor::OpenSearchSink;
use super::bulk::{BulkTransport, OpenSearchBulkTransport};
use super::config::OpenSearchSinkConfig;
use super::document::{document_shape, DocumentShape};
use super::mapping::{
    create_index_body, destination_type, strict_mapping, validate_index_description,
};

pub struct OpenSearchSinkConnector {
    config: Arc<OpenSearchSinkConfig>,

    speedtest_scope: Option<Arc<SpeedtestScope>>,
}

struct SpeedtestScope {
    owner: Arc<str>,

    schemas: BTreeMap<Arc<str>, DatasetSchema>,

    physical_targets: BTreeSet<(Arc<str>, Arc<str>)>,

    attempted: Mutex<BTreeSet<Arc<str>>>,

    claimed: Mutex<BTreeSet<Arc<str>>>,
}

struct VerifiedSpeedtestBulkTransport {
    verifier: Arc<dyn SpeedtestVerifier>,

    inner: Arc<dyn BulkTransport>,
}

trait SpeedtestVerifier: Send + Sync {
    fn verify(&self) -> BoxFuture<'_, anyhow::Result<()>>;
}

struct ScopeVerifier {
    client: OpenSearchClient,

    scope: Arc<SpeedtestScope>,
}

impl SpeedtestVerifier for ScopeVerifier {
    fn verify(&self) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(verify_claimed_scope(&self.client, &self.scope))
    }
}

impl BulkTransport for VerifiedSpeedtestBulkTransport {
    fn send(&self, payload: Vec<u8>) -> BoxFuture<'_, Result<Vec<u16>, super::bulk::BulkFailure>> {
        Box::pin(async move {
            self.verifier
                .verify()
                .await
                .map_err(super::bulk::BulkFailure::Fatal)?;
            self.inner.send(payload).await
        })
    }
}

impl SpeedtestScope {
    fn record_attempt(&self, index: &str) -> anyhow::Result<()> {
        let index = self
            .schemas
            .keys()
            .find(|value| value.as_ref() == index)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "refusing to prepare an OpenSearch index outside the speedtest scope"
                )
            })?;
        self.attempted
            .lock()
            .map_err(|_| anyhow::anyhow!("OpenSearch speedtest state is poisoned"))?
            .insert(index);
        Ok(())
    }

    fn claim(&self, index: &str) -> anyhow::Result<()> {
        let index = self
            .schemas
            .keys()
            .find(|value| value.as_ref() == index)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("refusing to claim an OpenSearch index outside the speedtest scope")
            })?;
        self.claimed
            .lock()
            .map_err(|_| anyhow::anyhow!("OpenSearch speedtest state is poisoned"))?
            .insert(index);
        Ok(())
    }

    fn attempted(&self) -> anyhow::Result<BTreeSet<Arc<str>>> {
        self.attempted
            .lock()
            .map(|value| value.clone())
            .map_err(|_| anyhow::anyhow!("OpenSearch speedtest state is poisoned"))
    }

    fn claimed(&self) -> anyhow::Result<BTreeSet<Arc<str>>> {
        self.claimed
            .lock()
            .map(|value| value.clone())
            .map_err(|_| anyhow::anyhow!("OpenSearch speedtest state is poisoned"))
    }

    fn unclaim(&self, index: &str) -> anyhow::Result<()> {
        self.claimed
            .lock()
            .map_err(|_| anyhow::anyhow!("OpenSearch speedtest state is poisoned"))?
            .remove(index);
        self.attempted
            .lock()
            .map_err(|_| anyhow::anyhow!("OpenSearch speedtest state is poisoned"))?
            .remove(index);
        Ok(())
    }
}

impl OpenSearchSinkConnector {
    pub fn from_config(config: OpenSearchSinkConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config: Arc::new(config),
            speedtest_scope: None,
        })
    }

    pub async fn check_connection(
        config: OpenSearchSinkConfig,
    ) -> anyhow::Result<ConnectionCheckResult> {
        config.validate()?;
        let client = OpenSearchClient::new(&config.connection)?;
        client
            .request(Method::GET, &[], &[], "application/json", None)
            .await?;
        Ok(ConnectionCheckResult::default())
    }
}

impl SinkLimits for OpenSearchSinkConfig {
    fn description(&self) -> SinkLimitsDescription {
        SinkLimitsDescription {
            sink: "opensearch",
            dataset_name: Some(TextLimit {
                syntax: NameSyntax::AnyNonEmptyUtf8,
                max_utf8_bytes: Some(255),
            }),
            column_name: Some(TextLimit {
                syntax: NameSyntax::AnyNonEmptyUtf8,
                max_utf8_bytes: None,
            }),
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
                ArrowTypeFamily::Duration,
                ArrowTypeFamily::FixedSizeBinary,
            ],
            object_key: None,
        }
    }

    fn validate_discovery(&self, discovery: &DeliveryDiscovery) -> anyhow::Result<()> {
        anyhow::ensure!(
            !discovery.datasets.is_empty(),
            "OpenSearch sink requires at least one dataset"
        );
        let mut indices = HashSet::new();
        for dataset in &discovery.datasets {
            validate_index_name(&dataset.name)?;
            anyhow::ensure!(
                indices.insert(dataset.name.as_ref()),
                "OpenSearch discovery repeats index '{}'",
                dataset.name
            );
            validate_stored_projection(discovery, dataset)?;
            validate_schema(&dataset.stored_schema)?;
            if self.create_indices {
                strict_mapping(&dataset.stored_schema, None)?;
            }
        }
        Ok(())
    }
}

impl SinkConnector for OpenSearchSinkConnector {
    fn compatibility(&self) -> EndpointDescriptor {
        EndpointDescriptor::OpenSearchSink
    }

    fn limits(&self) -> &dyn SinkLimits {
        self.config.as_ref()
    }

    fn destination_type(&self, column: &SchemaColumn) -> anyhow::Result<String> {
        if column.name == "_source"
            && column.arrow_extension_name == Some(ARROW_JSON_EXTENSION_NAME)
        {
            return Ok("object".to_owned());
        }
        destination_type(column)
    }

    fn prepare(&self, request: SinkPrepare) -> BoxFuture<'_, anyhow::Result<()>> {
        Box::pin(async move {
            let client = OpenSearchClient::new(&self.config.connection)?;
            for dataset in request.datasets {
                validate_index_name(&dataset.table)?;
                validate_schema(&dataset.schema)?;
                if let Some(scope) = &self.speedtest_scope {
                    scope.record_attempt(&dataset.table)?;
                    let expected = scope.schemas.get(&dataset.table).ok_or_else(|| {
                        anyhow::anyhow!("OpenSearch speedtest schema is outside its scope")
                    })?;
                    anyhow::ensure!(
                        schemas_equal(expected, &dataset.schema),
                        "OpenSearch speedtest schema changed before preparation"
                    );
                    create_or_claim_speedtest_index(
                        &client,
                        &dataset.table,
                        &dataset.schema,
                        &scope.owner,
                    )
                    .await?;
                    scope.claim(&dataset.table)?;
                } else {
                    prepare_production_index(
                        &client,
                        &dataset.table,
                        &dataset.schema,
                        self.config.create_indices,
                    )
                    .await?;
                }
            }
            Ok(())
        })
    }

    fn build_sink(
        &self,
        context: SinkBuildContext,
    ) -> BoxFuture<'_, anyhow::Result<Box<dyn Sink>>> {
        Box::pin(async move {
            self.config.validate_discovery(&context.discovery)?;
            let client = OpenSearchClient::new(&self.config.connection)?;
            if let Some(scope) = &self.speedtest_scope {
                verify_claimed_scope(&client, scope).await?;
            }
            let timeout_ms = self.config.connection.request_timeout_ms;
            let transport: Arc<dyn BulkTransport> = match &self.speedtest_scope {
                Some(scope) => Arc::new(VerifiedSpeedtestBulkTransport {
                    verifier: Arc::new(ScopeVerifier {
                        client: client.clone(),
                        scope: Arc::clone(scope),
                    }),
                    inner: Arc::new(OpenSearchBulkTransport::new(client, timeout_ms)),
                }),
                None => Arc::new(OpenSearchBulkTransport::new(client, timeout_ms)),
            };
            Ok(Box::new(OpenSearchSink::new(
                Arc::clone(&self.config),
                transport,
                context.counters,
                context.discovery,
                context.partition_id,
            )) as Box<dyn Sink>)
        })
    }

    fn isolate_speedtest(
        self: Arc<Self>,
        discovery: Arc<DeliveryDiscovery>,
        isolation_id: String,
    ) -> BoxFuture<'static, anyhow::Result<SinkSpeedtestIsolation>> {
        Box::pin(async move {
            self.config.validate_discovery(&discovery)?;
            validate_isolation_id(&isolation_id)?;
            let mut isolated = discovery.as_ref().clone();
            let mut table_names = BTreeMap::new();
            let mut schemas = BTreeMap::new();
            let mut physical_targets = Vec::new();
            for (position, dataset) in isolated.datasets.iter_mut().enumerate() {
                anyhow::ensure!(
                    document_shape(&dataset.stored_schema) == DocumentShape::Flat,
                    "OpenSearch speedtest cannot safely synthesize a strict mapping for an opaque _source envelope"
                );
                let production = Arc::clone(&dataset.name);
                let scratch: Arc<str> =
                    Arc::from(format!("transferia-st-{isolation_id}-{position:x}"));
                validate_index_name(&scratch)?;
                anyhow::ensure!(
                    scratch != production,
                    "OpenSearch speedtest index aliases production"
                );
                table_names.insert(Arc::clone(&production), Arc::clone(&scratch));
                schemas.insert(Arc::clone(&scratch), dataset.stored_schema.clone());
                physical_targets.push(SpeedtestPhysicalTarget {
                    production: Arc::from(physical_target(&self.config, &production)),
                    scratch: Arc::from(physical_target(&self.config, &scratch)),
                });
                dataset.name = scratch;
            }
            let physical_set = physical_targets
                .iter()
                .map(|target| (Arc::clone(&target.production), Arc::clone(&target.scratch)))
                .collect();
            let scope = Arc::new(SpeedtestScope {
                owner: Arc::from(random_owner()?),
                schemas,
                physical_targets: physical_set,
                attempted: Mutex::new(BTreeSet::new()),
                claimed: Mutex::new(BTreeSet::new()),
            });
            let connector: Arc<dyn SinkConnector> = Arc::new(Self {
                config: Arc::clone(&self.config),
                speedtest_scope: Some(scope),
            });
            SinkSpeedtestIsolation::scratch(
                connector,
                discovery.as_ref(),
                isolated,
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
                    "refusing to clean OpenSearch speedtest indices with a production connector"
                )
            })?;
            validate_cleanup_scope(isolation, scope)?;
            let client = OpenSearchClient::new(&self.config.connection)?;
            let mut failures = Vec::new();
            for index in scope.attempted()? {
                match describe_index(&client, &index).await {
                    Ok(description) => {
                        let schema = scope.schemas.get(&index).ok_or_else(|| {
                            anyhow::anyhow!("OpenSearch cleanup index is outside scope")
                        })?;
                        if validate_index_description(
                            &index,
                            &description,
                            schema,
                            Some(&scope.owner),
                        )
                        .is_err()
                        {
                            failures.push(format!("'{index}' has no exact ownership proof"));
                            continue;
                        }
                        let deleted = client
                            .json::<serde_json::Value>(Method::DELETE, &[&index], &[], None)
                            .await;
                        if !matches!(
                            deleted,
                            Ok(ref body) if body.get("acknowledged").and_then(serde_json::Value::as_bool) == Some(true)
                        ) {
                            failures.push(format!("'{index}' could not be deleted"));
                            continue;
                        }
                        match describe_index(&client, &index).await {
                            Err(OpenSearchHttpError::Status { status })
                                if status == StatusCode::NOT_FOUND =>
                            {
                                scope.unclaim(&index)?;
                            }
                            Ok(_) => failures.push(format!(
                                "'{index}' still exists after acknowledged deletion"
                            )),
                            Err(_) => {
                                failures.push(format!("'{index}' deletion could not be verified"));
                            }
                        }
                    }
                    Err(OpenSearchHttpError::Status { status })
                        if status == StatusCode::NOT_FOUND =>
                    {
                        scope.unclaim(&index)?;
                    }
                    Err(_) => failures.push(format!("'{index}' could not be verified")),
                }
            }
            anyhow::ensure!(
                failures.is_empty(),
                "OpenSearch speedtest cleanup failed: {}",
                failures.join("; ")
            );
            Ok(())
        })
    }
}

fn validate_schema(schema: &DatasetSchema) -> anyhow::Result<()> {
    anyhow::ensure!(
        !schema.columns.is_empty(),
        "OpenSearch index schema is empty"
    );
    let mut names = HashSet::new();
    for column in &schema.columns {
        anyhow::ensure!(
            !column.name.is_empty(),
            "OpenSearch field name must not be empty"
        );
        anyhow::ensure!(
            names.insert(column.name.as_str()),
            "OpenSearch schema repeats field '{}'",
            column.name
        );
    }
    if document_shape(schema) == DocumentShape::Envelope {
        return Ok(());
    }
    anyhow::ensure!(
        !schema.columns.iter().any(|column| column.name == "_source"),
        "OpenSearch _source is allowed only in the exact source envelope"
    );
    if let Some(id) = schema.columns.iter().find(|column| column.name == "_id") {
        anyhow::ensure!(
            id.data_type == DataType::Utf8 && !id.nullable,
            "OpenSearch _id must be non-null Arrow Utf8"
        );
        anyhow::ensure!(
            id.max_length.is_none_or(|length| length <= 512),
            "OpenSearch _id declared max_length exceeds the 512-byte limit"
        );
    }
    if let Some(routing) = schema
        .columns
        .iter()
        .find(|column| column.name == "_routing")
    {
        anyhow::ensure!(
            routing.data_type == DataType::Utf8 && !routing.primary_key,
            "OpenSearch _routing must be a non-primary-key Arrow Utf8 field"
        );
    }
    let primary_keys = schema
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .collect::<Vec<_>>();
    if let Some(id) = schema.columns.iter().find(|column| column.name == "_id") {
        anyhow::ensure!(
            id.primary_key && primary_keys.len() == 1,
            "OpenSearch _id must be the sole primary-key field"
        );
    } else {
        anyhow::ensure!(
            !primary_keys.is_empty(),
            "OpenSearch dataset requires a non-null primary key or '_id'"
        );
    }
    for column in primary_keys {
        anyhow::ensure!(
            !column.nullable,
            "OpenSearch primary-key field '{}' must not be nullable",
            column.name
        );
    }
    strict_mapping(schema, None)?;
    Ok(())
}

async fn prepare_production_index(
    client: &OpenSearchClient,
    index: &str,
    schema: &DatasetSchema,
    create: bool,
) -> anyhow::Result<()> {
    match describe_index(client, index).await {
        Ok(description) => {
            return validate_index_description(index, &description, schema, None);
        }
        Err(OpenSearchHttpError::Status { status })
            if status == StatusCode::NOT_FOUND && create => {}
        Err(error) => return Err(error.into()),
    }

    let body = create_index_body(schema, None)?;
    match client
        .json::<serde_json::Value>(Method::PUT, &[index], &[], Some(&body))
        .await
    {
        Ok(_) => {}
        Err(OpenSearchHttpError::Status { status }) if status == StatusCode::BAD_REQUEST => {
            let description = describe_index(client, index).await.map_err(|probe_error| {
                anyhow::anyhow!(
                    "OpenSearch rejected creation of index '{index}' with HTTP 400 and no exactly verifiable index exists: {probe_error}"
                )
            })?;
            return validate_index_description(index, &description, schema, None);
        }
        Err(error) => return Err(error.into()),
    }

    let description = describe_index(client, index).await?;
    validate_index_description(index, &description, schema, None)
}

async fn create_or_claim_speedtest_index(
    client: &OpenSearchClient,
    index: &str,
    schema: &DatasetSchema,
    owner: &str,
) -> anyhow::Result<()> {
    let body = create_index_body(schema, Some(owner))?;
    match client
        .json::<serde_json::Value>(Method::PUT, &[index], &[], Some(&body))
        .await
    {
        Ok(_) => {}
        Err(error) if error.retryable() => {}
        Err(OpenSearchHttpError::Status { status }) if status == StatusCode::BAD_REQUEST => {}
        Err(error) => return Err(error.into()),
    }
    let description = describe_index(client, index).await?;
    validate_index_description(index, &description, schema, Some(owner))
}

async fn describe_index(
    client: &OpenSearchClient,
    index: &str,
) -> Result<serde_json::Value, OpenSearchHttpError> {
    client
        .json::<serde_json::Value>(Method::GET, &[index], &[], None)
        .await
}

async fn verify_claimed_scope(
    client: &OpenSearchClient,
    scope: &SpeedtestScope,
) -> anyhow::Result<()> {
    let claimed = scope.claimed()?;
    anyhow::ensure!(
        claimed.len() == scope.schemas.len(),
        "OpenSearch speedtest scope is not fully prepared"
    );
    for (index, schema) in &scope.schemas {
        anyhow::ensure!(
            claimed.contains(index),
            "OpenSearch speedtest index is not claimed"
        );
        let description = describe_index(client, index).await?;
        validate_index_description(index, &description, schema, Some(&scope.owner))?;
    }
    Ok(())
}

fn validate_cleanup_scope(
    isolation: &SinkSpeedtestIsolation,
    scope: &SpeedtestScope,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        isolation.safety() == transferia_registry::SinkSpeedtestIsolationSafety::Scratch,
        "OpenSearch cleanup requires scratch isolation"
    );
    let actual = isolation
        .physical_targets()
        .iter()
        .map(|target| (Arc::clone(&target.production), Arc::clone(&target.scratch)))
        .collect::<BTreeSet<_>>();
    anyhow::ensure!(
        actual == scope.physical_targets,
        "OpenSearch cleanup isolation does not match connector ownership scope"
    );
    Ok(())
}

fn physical_target(config: &OpenSearchSinkConfig, index: &str) -> String {
    let scheme = if config.connection.trusted_plaintext {
        "http"
    } else {
        "https"
    };
    format!(
        "{scheme}://{}:{}/{index}",
        config.connection.hosts.join(","),
        config.connection.port
    )
}

fn validate_isolation_id(value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        value.len() == 32
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "OpenSearch speedtest isolation ID must contain 32 lowercase hexadecimal characters"
    );
    Ok(())
}

fn random_owner() -> anyhow::Result<String> {
    let mut random = [0_u8; 16];
    getrandom::fill(&mut random)?;
    let mut owner = String::with_capacity(32);
    for byte in random {
        write!(&mut owner, "{byte:02x}")?;
    }
    Ok(owner)
}

fn schemas_equal(left: &DatasetSchema, right: &DatasetSchema) -> bool {
    left.columns.len() == right.columns.len()
        && left
            .columns
            .iter()
            .zip(&right.columns)
            .all(|(left, right)| {
                left.name == right.name
                    && left.data_type == right.data_type
                    && left.nullable == right.nullable
                    && left.primary_key == right.primary_key
                    && left.low_cardinality == right.low_cardinality
                    && left.max_length == right.max_length
                    && left.arrow_extension_name == right.arrow_extension_name
                    && left.system_role == right.system_role
                    && left.old_value_of == right.old_value_of
                    && left.old_key_of == right.old_key_of
            })
}

#[cfg(test)]
#[path = "tests/connector.rs"]
mod tests;
