use schemars::{schema_for, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::model::{DeliveryRecord, RuntimeState, ValidationState};
use super::service::{
    ColumnView, DatasetRoleView, DatasetView, DestinationColumnView, DiscoveryResult,
    ValidationCommandResult,
};
use super::ui_catalog::UiCatalog;
use transferia::core::delivery::{ArrowTypeFamily, NameSyntax, SinkLimitsDescription, TextLimit};
use transferia::extension::{DynamicOptions, EndpointRole, OptionsRequest};
use transferia::providers::traits::ConnectionCheckResult;
use transferia::runtime::RunId;

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConfigRequest {
    #[schemars(
        with = "std::collections::BTreeMap<String, Value>",
        extend("x-typescript-type" = "JsonObject")
    )]
    pub config: Value,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectionCheckRequest {
    pub provider: String,

    pub role: EndpointRole,

    #[schemars(
        with = "std::collections::BTreeMap<String, Value>",
        extend("x-typescript-type" = "JsonObject")
    )]
    pub config: Value,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MessagePreviewRequest {
    pub provider: String,

    #[schemars(
        with = "std::collections::BTreeMap<String, Value>",
        extend("x-typescript-type" = "JsonObject")
    )]
    pub config: Value,

    pub max_bytes: usize,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SqlPlaygroundRequest {
    pub sql: String,

    pub rows: Vec<Value>,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YamlRequest {
    pub yaml: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateDraftRequest {
    pub name: String,

    #[serde(default)]
    pub description: String,

    #[schemars(
        with = "std::collections::BTreeMap<String, Value>",
        extend("x-typescript-type" = "JsonObject")
    )]
    pub config: Value,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateDraftRequest {
    pub expected_revision: u64,

    #[serde(deserialize_with = "super::model::decimal_u64::deserialize")]
    #[schemars(
        with = "String",
        extend("pattern" = "^(?:0|[1-9][0-9]*)$")
    )]
    pub expected_record_version: u64,

    pub name: String,

    #[serde(default)]
    pub description: String,

    #[schemars(
        with = "std::collections::BTreeMap<String, Value>",
        extend("x-typescript-type" = "JsonObject")
    )]
    pub config: Value,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RevisionRequest {
    pub expected_revision: u64,

    #[serde(deserialize_with = "super::model::decimal_u64::deserialize")]
    #[schemars(
        with = "String",
        extend("pattern" = "^(?:0|[1-9][0-9]*)$")
    )]
    pub expected_record_version: u64,
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[allow(
    clippy::struct_field_names,
    reason = "expected_* names are the optimistic-concurrency wire contract"
)]
pub struct StopRequest {
    pub expected_revision: u64,

    #[serde(deserialize_with = "super::model::decimal_u64::deserialize")]
    #[schemars(
        with = "String",
        extend("pattern" = "^(?:0|[1-9][0-9]*)$")
    )]
    pub expected_record_version: u64,

    pub expected_run_id: String,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkerLogReadQuery {
    #[serde(default)]
    pub cursor: Option<u64>,

    #[serde(default)]
    pub limit_bytes: Option<usize>,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct YamlResponse {
    pub yaml: String,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigResponse {
    #[schemars(
        with = "std::collections::BTreeMap<String, Value>",
        extend("x-typescript-type" = "JsonObject")
    )]
    pub config: Value,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HealthResponse {
    pub status: &'static str,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliverySummary {
    pub id: String,

    pub name: String,

    pub description: String,

    pub revision: u64,

    pub validation: ValidationState,

    pub runtime: RuntimeState,

    pub updated_at_ms: u64,
}

impl From<DeliveryRecord> for DeliverySummary {
    fn from(record: DeliveryRecord) -> Self {
        Self {
            id: record.id,
            name: record.name,
            description: record.description,
            revision: record.revision,
            validation: record.validation,
            runtime: record.runtime,
            updated_at_ms: record.updated_at_ms,
        }
    }
}

#[derive(Clone, Copy, Debug, JsonSchema, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiErrorCode {
    InvalidRequest,
    PayloadTooLarge,
    NotFound,
    Conflict,
    ValidationFailed,
    InternalError,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApiErrorBody {
    pub error: ApiErrorView,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ApiErrorView {
    pub code: ApiErrorCode,

    pub message: String,
}

#[derive(JsonSchema)]
#[expect(dead_code, reason = "fields define the generated HTTP contract roots")]
struct ServerApiContract {
    catalog_response: UiCatalog,

    delivery_list_response: Vec<DeliverySummary>,

    delivery_response: DeliveryRecord,

    discovery_response: DiscoveryResult,

    validation_response: ValidationCommandResult,

    dynamic_options_response: DynamicOptions,

    dynamic_options_request: OptionsRequest,

    connection_check_request: ConnectionCheckRequest,

    connection_check_response: ConnectionCheckResult,

    message_preview_request: MessagePreviewRequest,

    message_preview_response: super::service::MessagePreviewResult,

    sql_playground_request: SqlPlaygroundRequest,

    sql_playground_response: super::service::SqlPlaygroundResult,

    yaml_response: YamlResponse,

    config_response: ConfigResponse,

    health_response: HealthResponse,

    error_response: ApiErrorBody,

    config_request: ConfigRequest,

    yaml_request: YamlRequest,

    create_draft_request: CreateDraftRequest,

    update_draft_request: UpdateDraftRequest,

    revision_request: RevisionRequest,

    stop_request: StopRequest,

    worker_logs_response: super::service::WorkerLogsResult,

    worker_log_response: super::service::WorkerLogChunkView,

    worker_log_read_query: WorkerLogReadQuery,
}

pub fn schema() -> anyhow::Result<Value> {
    Ok(serde_json::to_value(schema_for!(ServerApiContract))?)
}

pub fn fixture() -> anyhow::Result<Value> {
    let catalog = UiCatalog {
        common_schema: serde_json::json!({ "type": "object" }),
        initial: serde_json::json!({}),
        providers: vec![transferia::providers::catalog::ProviderDefinition {
            key: "clickhouse",
            title: "ClickHouse",
            source: None,
            sink: Some(transferia::providers::catalog::EndpointDefinition {
                schema: serde_json::json!({ "type": "object" }),
                initial: serde_json::json!({}),
                delivery_modes: vec![
                    transferia::providers::catalog::DeliveryMode::Batch,
                    transferia::providers::catalog::DeliveryMode::Stream,
                ],
                partitioned: false,
                connection_check: true,
            }),
        }],
    };
    let delivery = DeliveryRecord {
        id: "delivery-1".to_owned(),
        name: "Example".to_owned(),
        description: "Contract fixture".to_owned(),
        config: serde_json::json!({ "delivery_type": "stream" }),
        revision: 7,
        record_version: 11,
        validation: ValidationState::Invalid {
            revision: 7,
            message: "invalid fixture".to_owned(),
        },
        runtime: RuntimeState::Running {
            run_id: RunId("run-7".to_owned()),
            pid: 42,
        },
        created_at_ms: 1000,
        updated_at_ms: 2000,
    };
    let runtime_states = [
        RuntimeState::Created,
        RuntimeState::Stopped,
        RuntimeState::Starting {
            run_id: RunId("run-1".to_owned()),
        },
        RuntimeState::Running {
            run_id: RunId("run-2".to_owned()),
            pid: 42,
        },
        RuntimeState::Stopping {
            run_id: RunId("run-3".to_owned()),
        },
        RuntimeState::Failed {
            run_id: RunId("run-4".to_owned()),
            message: "worker failed".to_owned(),
        },
    ];
    let discovery = DiscoveryResult {
        source: "logbroker".to_owned(),
        sink: "clickhouse".to_owned(),
        pipeline_count: 1,
        datasets: vec![DatasetView {
            role: DatasetRoleView::Main,
            name: "events".to_owned(),
            intermediate_columns: vec![
                ColumnView {
                    name: "id".to_owned(),
                    arrow_type: "Utf8".to_owned(),
                    nullable: false,
                    primary_key: true,
                    low_cardinality: true,
                    max_length: Some(64),
                },
                ColumnView {
                    name: "created_at".to_owned(),
                    arrow_type: "Timestamp(Millisecond, None)".to_owned(),
                    nullable: true,
                    primary_key: false,
                    low_cardinality: false,
                    max_length: None,
                },
            ],
            final_columns: vec![DestinationColumnView {
                column: ColumnView {
                    name: "id".to_owned(),
                    arrow_type: "Utf8".to_owned(),
                    nullable: false,
                    primary_key: true,
                    low_cardinality: true,
                    max_length: Some(64),
                },
                destination_type: "LowCardinality(String)".to_owned(),
            }],
        }],
        sink_limits: SinkLimitsDescription {
            sink: "clickhouse",
            dataset_name: Some(TextLimit {
                syntax: NameSyntax::AsciiIdentifier,
                max_utf8_bytes: Some(255),
            }),
            column_name: None,
            supported_arrow_types: vec![
                ArrowTypeFamily::Utf8,
                ArrowTypeFamily::SignedInteger,
                ArrowTypeFamily::Timestamp,
            ],
            object_key: None,
        },
    };
    let error = ApiErrorBody {
        error: ApiErrorView {
            code: ApiErrorCode::NotFound,
            message: "delivery 'missing' does not exist".to_owned(),
        },
    };

    Ok(serde_json::json!({
        "catalog": serde_json::to_value(catalog)?,
        "delivery_record": serde_json::to_value(delivery)?,
        "runtime_states": serde_json::to_value(runtime_states)?,
        "discovery_result": serde_json::to_value(discovery)?,
        "error_envelope": serde_json::to_value(error)?,
    }))
}

#[cfg(test)]
#[path = "tests/api_contract.rs"]
mod tests;
