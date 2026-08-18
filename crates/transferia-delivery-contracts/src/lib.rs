extern crate alloc;

pub mod delivery_tracker;
pub mod metrics;
pub mod middleware;
pub mod parser;
pub mod retry;
pub mod semantics;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryType {
    Batch,
    Stream,
    BatchAndStream,
}

impl DeliveryType {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Batch => "batch",
            Self::Stream => "stream",
            Self::BatchAndStream => "batch + stream",
        }
    }
}
