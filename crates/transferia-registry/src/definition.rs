use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value as JsonValue;

use transferia_delivery_contracts::semantics::RecordSemantics;

#[derive(Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    Batch,

    Stream,

    BatchAndStream,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointDefinition {
    #[schemars(
        with = "BTreeMap<String, JsonValue>",
        extend("x-typescript-type" = "JsonSchema")
    )]
    pub schema: JsonValue,

    #[schemars(
        with = "BTreeMap<String, JsonValue>",
        extend("x-typescript-type" = "JsonObject")
    )]
    pub initial: JsonValue,

    pub delivery_modes: Vec<DeliveryMode>,

    /// Record streams this source can produce or this sink can accept.
    ///
    /// A connector that supports multiple semantics may require a matching
    /// endpoint mode; delivery preparation remains the authoritative check.
    pub record_semantics: Vec<RecordSemantics>,

    pub partitioned: bool,

    pub connection_check: bool,

    pub message_preview: bool,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorDefinition {
    pub key: &'static str,

    pub title: &'static str,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-omit-none" = true))]
    pub source: Option<EndpointDefinition>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-omit-none" = true))]
    pub sink: Option<EndpointDefinition>,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MiddlewareDefinition {
    pub key: &'static str,

    pub title: &'static str,

    #[schemars(
        with = "BTreeMap<String, JsonValue>",
        extend("x-typescript-type" = "JsonSchema")
    )]
    pub schema: JsonValue,

    #[schemars(
        with = "BTreeMap<String, JsonValue>",
        extend("x-typescript-type" = "JsonObject")
    )]
    pub initial: JsonValue,

    pub playground: bool,
}
