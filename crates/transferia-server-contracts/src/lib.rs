use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use transferia_runtime::RunId;

pub const STATE_VERSION: u32 = 4;

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ValidationState {
    Draft,
    Ready { revision: u64 },
    Invalid { revision: u64, message: String },
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RuntimeState {
    Created,
    Stopped,
    Starting { run_id: RunId },
    Running { run_id: RunId, pid: u32 },
    Stopping { run_id: RunId },
    Failed { run_id: RunId, message: String },
}

impl RuntimeState {
    #[must_use]
    pub const fn is_running_or_transitioning(&self) -> bool {
        matches!(
            self,
            Self::Starting { .. } | Self::Running { .. } | Self::Stopping { .. }
        )
    }

    #[must_use]
    pub const fn run_id(&self) -> Option<&RunId> {
        match self {
            Self::Created | Self::Stopped => None,
            Self::Starting { run_id }
            | Self::Running { run_id, .. }
            | Self::Stopping { run_id }
            | Self::Failed { run_id, .. } => Some(run_id),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryRecord {
    pub id: String,
    pub name: String,
    pub description: String,

    #[schemars(
        with = "BTreeMap<String, Value>",
        extend("x-typescript-type" = "JsonObject")
    )]
    pub config: Value,

    pub revision: u64,

    #[serde(with = "decimal_u64")]
    #[schemars(
        with = "String",
        extend("pattern" = "^(?:0|[1-9][0-9]*)$")
    )]
    pub record_version: u64,

    pub validation: ValidationState,
    pub runtime: RuntimeState,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

pub mod decimal_u64 {
    use serde::{Deserialize as _, Deserializer, Serializer};

    #[allow(
        clippy::trivially_copy_pass_by_ref,
        reason = "serde's `with` module contract passes serialized fields by reference"
    )]
    pub fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&value.to_string())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

impl DeliveryRecord {
    pub fn normalize_after_server_restart(&mut self) -> anyhow::Result<bool> {
        if self.runtime.is_running_or_transitioning() {
            self.runtime = RuntimeState::Stopped;
            self.record_version = self
                .record_version
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("delivery record version overflow"))?;
            return Ok(true);
        }
        Ok(false)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StoredState {
    pub version: u32,
    pub deliveries: BTreeMap<String, DeliveryRecord>,
}

impl Default for StoredState {
    fn default() -> Self {
        Self {
            version: STATE_VERSION,
            deliveries: BTreeMap::new(),
        }
    }
}
