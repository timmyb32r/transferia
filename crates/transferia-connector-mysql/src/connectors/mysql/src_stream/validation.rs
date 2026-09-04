use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const LOG_BIN_QUERY: &str = "SELECT @@GLOBAL.log_bin";
pub const GTID_MODE_QUERY: &str = "SELECT @@GLOBAL.gtid_mode";
pub const ENFORCE_GTID_CONSISTENCY_QUERY: &str = "SELECT @@GLOBAL.enforce_gtid_consistency";
pub const BINLOG_FORMAT_QUERY: &str = "SELECT @@GLOBAL.binlog_format";
pub const BINLOG_CHECKSUM_QUERY: &str = "SELECT @@GLOBAL.binlog_checksum";
pub const BINLOG_ROW_IMAGE_QUERY: &str = "SELECT @@GLOBAL.binlog_row_image";
pub const BINLOG_ROW_METADATA_QUERY: &str = "SELECT @@GLOBAL.binlog_row_metadata";
pub const BINLOG_ROW_VALUE_OPTIONS_QUERY: &str = "SELECT @@GLOBAL.binlog_row_value_options";
pub const BINLOG_TRANSACTION_COMPRESSION_QUERY: &str =
    "SELECT @@GLOBAL.binlog_transaction_compression";

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MySqlReplicationPrerequisites {
    pub log_bin: String,

    pub gtid_mode: String,

    pub enforce_gtid_consistency: String,

    pub binlog_format: String,

    pub binlog_checksum: String,

    pub binlog_row_image: String,

    pub binlog_row_metadata: String,

    pub binlog_row_value_options: String,

    pub binlog_transaction_compression: String,
}

pub fn validate_replication_prerequisites(
    prerequisites: &MySqlReplicationPrerequisites,
) -> Result<(), String> {
    require_boolean("@@GLOBAL.log_bin", &prerequisites.log_bin, true)?;
    require_value("@@GLOBAL.gtid_mode", &prerequisites.gtid_mode, "ON")?;
    require_value(
        "@@GLOBAL.enforce_gtid_consistency",
        &prerequisites.enforce_gtid_consistency,
        "ON",
    )?;
    require_value(
        "@@GLOBAL.binlog_format",
        &prerequisites.binlog_format,
        "ROW",
    )?;
    require_value(
        "@@GLOBAL.binlog_checksum",
        &prerequisites.binlog_checksum,
        "CRC32",
    )?;
    require_value(
        "@@GLOBAL.binlog_row_image",
        &prerequisites.binlog_row_image,
        "FULL",
    )?;
    require_value(
        "@@GLOBAL.binlog_row_metadata",
        &prerequisites.binlog_row_metadata,
        "FULL",
    )?;
    require_value(
        "@@GLOBAL.binlog_row_value_options",
        &prerequisites.binlog_row_value_options,
        "",
    )?;
    require_boolean(
        "@@GLOBAL.binlog_transaction_compression",
        &prerequisites.binlog_transaction_compression,
        false,
    )?;
    Ok(())
}

fn require_boolean(variable: &str, actual: &str, required: bool) -> Result<(), String> {
    let matches = if required {
        actual.eq_ignore_ascii_case("ON") || actual == "1"
    } else {
        actual.eq_ignore_ascii_case("OFF") || actual == "0"
    };
    if matches {
        return Ok(());
    }
    Err(format!(
        "MySQL replication requires {variable}={}, received {actual:?}",
        if required { "ON" } else { "OFF" }
    ))
}

fn require_value(variable: &str, actual: &str, required: &str) -> Result<(), String> {
    if actual.eq_ignore_ascii_case(required) {
        return Ok(());
    }
    Err(format!(
        "MySQL replication requires {variable}={required}, received {actual:?}"
    ))
}
