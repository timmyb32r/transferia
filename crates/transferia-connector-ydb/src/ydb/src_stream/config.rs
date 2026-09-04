use std::time::Duration;

use schemars::JsonSchema;
use serde::Deserialize;

use super::super::config::validate_absolute_ydb_path;

const MAX_GRPC_FRAME_BYTES: usize = u32::MAX as usize;

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("x-ui" = { "capabilities": { "component": "source", "key": "replication", "delivery_modes": ["stream"], "record_semantics": ["changelog"] } }))]
pub struct YdbReplicationConfig {
    #[schemars(
        title = "Changefeed name",
        description = "Existing FORMAT JSON, NEW_AND_OLD_IMAGES YDB changefeed present on every configured table"
    )]
    pub changefeed_name: String,

    #[schemars(
        title = "Consumer name",
        description = "Existing important consumer present on every configured changefeed topic"
    )]
    pub consumer_name: String,

    #[schemars(
        title = "Coordination node path",
        description = "Existing YDB Coordination node used to fence this changefeed consumer globally"
    )]
    pub coordination_node_path: String,

    #[serde(default = "default_read_buffer_bytes")]
    #[schemars(
        range(min = 1),
        title = "Read buffer bytes",
        description = "YDB Topic flow-control credit granted by each replication reader",
        extend("x-ui" = { "section": "advanced", "widget": "byte_size" })
    )]
    pub read_buffer_bytes: usize,

    #[serde(default = "default_max_message_bytes")]
    #[schemars(
        range(min = 1, max = 4_294_967_295_u64),
        title = "Maximum change record bytes",
        description = "Maximum encoded bytes accepted for one YDB changefeed record before failing closed",
        extend("x-ui" = { "section": "advanced", "widget": "byte_size" })
    )]
    pub max_message_bytes: usize,

    #[serde(default = "default_max_batch_bytes")]
    #[schemars(
        range(min = 1, max = 4_294_967_295_u64),
        title = "Maximum change batch bytes",
        description = "Maximum encoded bytes accepted in one YDB Topic response before failing closed",
        extend("x-ui" = { "section": "advanced", "widget": "byte_size" })
    )]
    pub max_batch_bytes: usize,

    #[serde(default = "default_max_response_bytes")]
    #[schemars(
        range(min = 1, max = 4_294_967_295_u64),
        title = "Maximum Topic response bytes",
        description = "Maximum encoded gRPC response bytes accepted before YDB Topic protobuf decoding",
        extend("x-ui" = { "section": "advanced", "widget": "byte_size" })
    )]
    pub max_response_bytes: usize,

    #[serde(default = "default_commit_timeout_ms")]
    #[schemars(
        range(min = 1),
        title = "Offset commit timeout, ms",
        description = "Maximum time to wait for YDB to acknowledge source offsets after the destination commit",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub commit_timeout_ms: u64,
}

impl YdbReplicationConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        validate_resource_name("changefeed_name", &self.changefeed_name)?;
        validate_resource_name("consumer_name", &self.consumer_name)?;
        validate_absolute_ydb_path(
            "ydb.replication.coordination_node_path",
            &self.coordination_node_path,
        )?;
        anyhow::ensure!(self.read_buffer_bytes > 0, "ydb.replication.read_buffer_bytes must be positive");
        i64::try_from(self.read_buffer_bytes).map_err(|_| {
            anyhow::anyhow!("ydb.replication.read_buffer_bytes exceeds the YDB Topic credit range")
        })?;
        anyhow::ensure!(
            (1..=MAX_GRPC_FRAME_BYTES).contains(&self.max_message_bytes),
            "ydb.replication.max_message_bytes must be in 1..={MAX_GRPC_FRAME_BYTES}"
        );
        anyhow::ensure!(
            (1..=MAX_GRPC_FRAME_BYTES).contains(&self.max_batch_bytes),
            "ydb.replication.max_batch_bytes must be in 1..={MAX_GRPC_FRAME_BYTES}"
        );
        anyhow::ensure!(
            (1..=MAX_GRPC_FRAME_BYTES).contains(&self.max_response_bytes),
            "ydb.replication.max_response_bytes must be in 1..={MAX_GRPC_FRAME_BYTES}"
        );
        anyhow::ensure!(
            self.max_message_bytes <= self.max_batch_bytes,
            "ydb.replication.max_message_bytes must not exceed max_batch_bytes"
        );
        anyhow::ensure!(
            self.max_batch_bytes <= self.max_response_bytes,
            "ydb.replication.max_batch_bytes must not exceed max_response_bytes"
        );
        validate_response_admission(self.max_response_bytes)?;
        anyhow::ensure!(
            self.commit_timeout_ms > 0,
            "ydb.replication.commit_timeout_ms must be positive"
        );
        anyhow::ensure!(
            std::time::Instant::now()
                .checked_add(self.commit_timeout())
                .is_some(),
            "ydb.replication.commit_timeout_ms exceeds the platform clock range"
        );
        Ok(())
    }

    #[must_use]
    pub const fn commit_timeout(&self) -> Duration {
        Duration::from_millis(self.commit_timeout_ms)
    }

    pub(in crate::ydb) fn minimum_pipeline_memory_bytes(&self) -> anyhow::Result<usize> {
        super::topic::response_processing_bytes(self.max_response_bytes, 0).map_err(|_| {
            anyhow::anyhow!(
                "YDB Topic response/decode admission overflows this platform"
            )
        })
    }
}

fn validate_response_admission(max_response_bytes: usize) -> anyhow::Result<()> {
    super::topic::response_processing_bytes(max_response_bytes, 0).map(|_| ()).map_err(|_| {
            anyhow::anyhow!(
                "ydb.replication.max_response_bytes is too large for the Topic response/decode/materialization admission on this platform"
            )
        })
}

fn validate_resource_name(field: &str, value: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !value.is_empty() && value.trim() == value,
        "ydb.replication.{field} must be non-empty and have no surrounding whitespace"
    );
    anyhow::ensure!(
        !value.contains('/') && value != "." && value != "..",
        "ydb.replication.{field} must be one YDB path segment"
    );
    Ok(())
}

const fn default_read_buffer_bytes() -> usize {
    1024 * 1024
}

const fn default_max_message_bytes() -> usize {
    1024 * 1024
}

const fn default_max_batch_bytes() -> usize {
    1024 * 1024
}

const fn default_max_response_bytes() -> usize {
    1536 * 1024
}

const fn default_commit_timeout_ms() -> u64 {
    30_000
}

#[cfg(test)]
#[path = "tests/config.rs"]
mod tests;
