use std::collections::BTreeMap;

use schemars::{schema_for, JsonSchema};
use serde::Serialize;
use serde_json::Value as JsonValue;

#[derive(Clone, Copy, Debug, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    Batch,
    Stream,
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

    pub partitioned: bool,

    pub connection_check: bool,
}

#[derive(Clone, Debug, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDefinition {
    pub key: &'static str,

    pub title: &'static str,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-omit-none" = true))]
    pub source: Option<EndpointDefinition>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-omit-none" = true))]
    pub sink: Option<EndpointDefinition>,
}

pub(super) struct EndpointSpec {
    pub definition: EndpointDefinition,
}

impl EndpointSpec {
    pub fn new<C: JsonSchema>(
        initial: JsonValue,
        delivery_modes: Vec<DeliveryMode>,
        partitioned: bool,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            definition: EndpointDefinition {
                schema: serde_json::to_value(schema_for!(C))?,
                initial,
                delivery_modes,
                partitioned,
                connection_check: false,
            },
        })
    }
}
