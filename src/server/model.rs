use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const STATE_VERSION: u32 = 2;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ValidationState {
    Draft,
    Ready { revision: u64 },
    Invalid { revision: u64, message: String },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RuntimeState {
    Stopped,
    Starting,
    Running { pid: u32 },
    Stopping,
    Failed { message: String },
}

impl RuntimeState {
    #[must_use]
    pub const fn is_running_or_transitioning(&self) -> bool {
        matches!(self, Self::Starting | Self::Running { .. } | Self::Stopping)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryRecord {
    pub id: String,
    pub name: String,
    pub description: String,
    pub config: Value,
    pub revision: u64,
    pub validation: ValidationState,
    pub runtime: RuntimeState,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

impl DeliveryRecord {
    pub fn normalize_after_server_restart(&mut self) -> bool {
        if self.runtime.is_running_or_transitioning() {
            self.runtime = RuntimeState::Stopped;
            return true;
        }
        false
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
