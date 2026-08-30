#![allow(
    clippy::expect_used,
    reason = "formatting into an owned String is infallible"
)]

use std::collections::HashSet;
use std::fmt::{self, Write as _};
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_BATCH_ROWS: usize = 65_536;
const DEFAULT_TABLE_READER_WINDOW_SIZE: u64 = 128 * 1024 * 1024;
const DEFAULT_TABLE_READER_GROUP_SIZE: u64 = 128 * 1024 * 1024;
const DEFAULT_TABLE_READER_MAX_BUFFER_SIZE: u64 = 512 * 1024 * 1024;
const DEFAULT_STREAM_RETRY_MAX_ATTEMPTS: usize = 12;
const DEFAULT_STREAM_RETRY_INITIAL_MS: u64 = 100;
const DEFAULT_STREAM_RETRY_MAX_MS: u64 = 5_000;
const DEFAULT_STREAM_OPEN_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_STREAM_IDLE_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_PARTITION_COMPRESSED_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_PARTITION_COUNT: usize = 64;
const DEFAULT_PARTITION_CONCURRENCY: usize = 16;
const DEFAULT_WRITE_TARGET_BYTES: usize = 512 * 1024 * 1024;
const DEFAULT_WRITE_CONCURRENCY: usize = 4;
const DEFAULT_WRITE_FLUSH_INTERVAL_MS: u64 = 1_000;
const DEFAULT_WRITE_ROW_BUFFER_BYTES: u64 = 1024 * 1024;
const DEFAULT_TABLE_WRITER_BLOCK_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_TABLE_WRITER_BUFFER_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_TABLE_WRITER_WINDOW_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_TABLE_WRITER_GROUP_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_TABLE_WRITER_CHUNK_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const DEFAULT_PRIMARY_KEY_SORT_TIMEOUT_MS: u64 = 24 * 60 * 60 * 1_000;

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusConnectionConfig {
    #[schemars(extend("x-ui" = { "control_width": "auth" }))]
    pub auth: YTsaurusAuthConfig,

    pub host: String,

    pub port: u16,

    pub trusted_plaintext: bool,

    #[serde(default)]
    #[schemars(
        title = "Trust plaintext native RPC",
        description = "Explicitly allow credentials and table data over the unencrypted YTsaurus native RPC transport",
        extend("x-ui" = { "widget": "hidden" })
    )]
    pub trusted_native_rpc_plaintext: bool,

    #[serde(default = "default_timeout_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub timeout_ms: u64,
}

impl YTsaurusConnectionConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        crate::connectors::address::validate_host("ytsaurus.host", &self.host)?;
        crate::connectors::address::validate_port("ytsaurus.port", self.port)?;
        anyhow::ensure!(self.timeout_ms > 0, "ytsaurus.timeout_ms must be positive");
        self.auth.validate()?;
        Ok(())
    }

    #[must_use]
    pub const fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    pub(crate) fn endpoint(&self) -> String {
        crate::connectors::address::url(
            if self.trusted_plaintext {
                "http"
            } else {
                "https"
            },
            &self.host,
            self.port,
        )
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum YTsaurusAuthConfig {
    #[schemars(title = "Token")]
    Token {
        #[schemars(extend("x-ui" = { "widget": "password" }))]
        token: String,
    },

    #[schemars(title = "Token file")]
    TokenFile { token_file: String },
}

impl YTsaurusAuthConfig {
    fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::Token { token } => {
                anyhow::ensure!(
                    !token.trim().is_empty(),
                    "ytsaurus.auth.token must not be empty"
                );
            }
            Self::TokenFile { token_file } => {
                anyhow::ensure!(
                    !token_file.trim().is_empty(),
                    "ytsaurus.auth.token_file must not be empty"
                );
            }
        }
        Ok(())
    }

    pub(crate) fn load_token(&self) -> anyhow::Result<String> {
        self.validate()?;
        let token = match self {
            Self::Token { token } => token.clone(),
            Self::TokenFile { token_file } => {
                let expanded = shellexpand::full(token_file)?;
                std::fs::read_to_string(expanded.as_ref()).map_err(|error| {
                    anyhow::anyhow!("failed to read YTsaurus token file '{expanded}': {error}")
                })?
            }
        };
        let token = token.trim().to_owned();
        anyhow::ensure!(!token.is_empty(), "YTsaurus token is empty");
        Ok(token)
    }
}

impl fmt::Debug for YTsaurusAuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Token { .. } => formatter
                .debug_struct("Token")
                .field("token", &"[REDACTED]")
                .finish(),
            Self::TokenFile { token_file } => formatter
                .debug_struct("TokenFile")
                .field("token_file", token_file)
                .finish(),
        }
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusSourceConfig {
    #[serde(flatten)]
    pub connection: YTsaurusConnectionConfig,

    pub tables: Vec<SourceTableConfig>,

    #[serde(default)]
    #[schemars(
        title = "Read mode",
        description = "Ordered reads resume at the last row after a transient failure. Unordered reads maximize single-stream throughput but fail on interruption. PartitionTables performs concurrent distributed reads and is also non-resumable.",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub read_ordering: YTsaurusReadOrdering,

    #[serde(default)]
    #[schemars(
        title = "Native reader settings",
        extend("x-ui" = { "widget": "hidden" })
    )]
    pub table_reader: YTsaurusTableReaderConfig,

    /// Explicit benchmark-only mode that counts wire rows without materializing
    /// their values as Arrow arrays. Delivery semantics only permit this mode
    /// with the discard destination.
    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub benchmark_discard: Option<YTsaurusBenchmarkDiscardConfig>,

    #[serde(default = "default_batch_rows")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub batch_rows: usize,

    #[serde(default = "default_stream_retry_max_attempts")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub stream_retry_max_attempts: usize,

    #[serde(default = "default_stream_retry_initial_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub stream_retry_initial_ms: u64,

    #[serde(default = "default_stream_retry_max_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub stream_retry_max_ms: u64,

    #[serde(default = "default_stream_open_timeout_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub stream_open_timeout_ms: u64,

    #[serde(default = "default_stream_idle_timeout_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub stream_idle_timeout_ms: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum YTsaurusReadOrdering {
    #[default]
    #[schemars(title = "Ordered (resumable)")]
    Ordered,

    #[schemars(title = "Unordered (maximum throughput, non-resumable)")]
    Unordered,

    #[schemars(title = "PartitionTables (distributed, non-resumable)")]
    PartitionTables {
        #[serde(default = "default_partition_compressed_bytes")]
        #[schemars(
            title = "Compressed bytes per partition",
            extend("x-ui" = { "widget": "hidden" })
        )]
        compressed_data_size_per_partition: u64,

        #[serde(default = "default_partition_count")]
        #[schemars(
            title = "Maximum partition count",
            extend("x-ui" = { "widget": "hidden" })
        )]
        max_partition_count: usize,

        #[serde(default = "default_partition_concurrency")]
        #[schemars(
            title = "Concurrent partition readers",
            extend("x-ui" = { "widget": "hidden" })
        )]
        concurrency: usize,
    },
}

impl YTsaurusReadOrdering {
    #[must_use]
    pub const fn is_unordered(&self) -> bool {
        matches!(self, Self::Unordered | Self::PartitionTables { .. })
    }

    #[must_use]
    pub(super) const fn partition_tables(&self) -> Option<YTsaurusPartitionTablesConfig> {
        match self {
            Self::PartitionTables {
                compressed_data_size_per_partition,
                max_partition_count,
                concurrency,
            } => Some(YTsaurusPartitionTablesConfig {
                compressed_data_size_per_partition: *compressed_data_size_per_partition,
                max_partition_count: *max_partition_count,
                concurrency: *concurrency,
            }),
            Self::Ordered | Self::Unordered => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct YTsaurusPartitionTablesConfig {
    pub compressed_data_size_per_partition: u64,

    pub max_partition_count: usize,

    pub concurrency: usize,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusBenchmarkDiscardConfig {
    #[serde(default)]
    pub transport: YTsaurusBenchmarkTransport,

    pub format: YTsaurusReadFormat,

    #[serde(default)]
    pub unordered: bool,

    #[serde(default)]
    pub table_reader: YTsaurusTableReaderConfig,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum YTsaurusBenchmarkTransport {
    #[default]
    Http,
    NativeRpc,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum YTsaurusReadFormat {
    Arrow,
    YtWire,
    Skiff,
    SchemafulDsv,
    YsonBinary,
    YsonText,
    Json,
}

impl YTsaurusReadFormat {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Arrow => "arrow",
            Self::YtWire => "yt_wire",
            Self::Skiff => "skiff",
            Self::SchemafulDsv => "schemaful_dsv",
            Self::YsonBinary => "yson_binary",
            Self::YsonText => "yson_text",
            Self::Json => "json",
        }
    }
}

#[derive(Clone, Default, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusTableReaderConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_size: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_size: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_buffer_size: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_parallel_readers: Option<u16>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_uncompressed_block_cache: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_out_of_order_blocks: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_block_cache: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_async_block_cache: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub populate_cache: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_workload_fifo_scheduling: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_read_blocks_batcher: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefer_local_data_center: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_queue_size_factor: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub net_queue_size_factor: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_block_count_factor: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_block_size_factor: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_direct_io: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub fetch_from_peers: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe_peer_count: Option<u16>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_chunk_prober: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub enable_chunk_meta_cache: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_rpc_hedging_delay_ms: Option<u64>,
}

impl YTsaurusTableReaderConfig {
    pub(super) fn to_yson(&self) -> String {
        let mut yson = String::from("{");
        let window_size = self.window_size.unwrap_or(DEFAULT_TABLE_READER_WINDOW_SIZE);
        let group_size = self.group_size.unwrap_or(DEFAULT_TABLE_READER_GROUP_SIZE);
        let max_buffer_size = self
            .max_buffer_size
            .unwrap_or(DEFAULT_TABLE_READER_MAX_BUFFER_SIZE);
        write!(
            &mut yson,
            "window_size={window_size};group_size={group_size};max_buffer_size={max_buffer_size};"
        )
        .expect("writing to a String cannot fail");
        macro_rules! number {
            ($field:ident) => {
                if let Some(value) = self.$field {
                    write!(&mut yson, "{}={value};", stringify!($field))
                        .expect("writing to a String cannot fail");
                }
            };
        }
        macro_rules! boolean {
            ($field:ident) => {
                if let Some(value) = self.$field {
                    write!(
                        &mut yson,
                        "{}={};",
                        stringify!($field),
                        if value { "%true" } else { "%false" },
                    )
                    .expect("writing to a String cannot fail");
                }
            };
        }

        number!(max_parallel_readers);
        boolean!(use_uncompressed_block_cache);
        boolean!(group_out_of_order_blocks);
        boolean!(use_block_cache);
        boolean!(use_async_block_cache);
        boolean!(populate_cache);
        boolean!(enable_workload_fifo_scheduling);
        boolean!(use_read_blocks_batcher);
        boolean!(prefer_local_data_center);
        number!(disk_queue_size_factor);
        number!(net_queue_size_factor);
        number!(cached_block_count_factor);
        number!(cached_block_size_factor);
        boolean!(use_direct_io);
        boolean!(fetch_from_peers);
        number!(probe_peer_count);
        boolean!(use_chunk_prober);
        boolean!(enable_chunk_meta_cache);
        if let Some(value) = self.block_rpc_hedging_delay_ms {
            write!(&mut yson, "block_rpc_hedging_delay={value};")
                .expect("writing to a String cannot fail");
        }
        yson.push('}');
        yson
    }

    pub(super) fn validate(&self) -> anyhow::Result<()> {
        let window_size = self.window_size.unwrap_or(DEFAULT_TABLE_READER_WINDOW_SIZE);
        let group_size = self.group_size.unwrap_or(DEFAULT_TABLE_READER_GROUP_SIZE);
        let max_buffer_size = self
            .max_buffer_size
            .unwrap_or(DEFAULT_TABLE_READER_MAX_BUFFER_SIZE);
        anyhow::ensure!(
            window_size > 0,
            "ytsaurus.table_reader.window_size must be positive"
        );
        anyhow::ensure!(
            group_size > 0,
            "ytsaurus.table_reader.group_size must be positive"
        );
        anyhow::ensure!(
            group_size <= window_size,
            "ytsaurus.table_reader.group_size must not exceed window_size"
        );
        anyhow::ensure!(
            max_buffer_size > 0,
            "ytsaurus.table_reader.max_buffer_size must be positive"
        );
        anyhow::ensure!(
            max_buffer_size >= window_size.saturating_mul(2),
            "ytsaurus.table_reader.max_buffer_size must be at least twice window_size"
        );
        if let Some(max_parallel_readers) = self.max_parallel_readers {
            anyhow::ensure!(
                (1..=1000).contains(&max_parallel_readers),
                "ytsaurus.table_reader.max_parallel_readers must be between 1 and 1000"
            );
        }
        for (name, factor) in [
            ("disk_queue_size_factor", self.disk_queue_size_factor),
            ("net_queue_size_factor", self.net_queue_size_factor),
            ("cached_block_count_factor", self.cached_block_count_factor),
            ("cached_block_size_factor", self.cached_block_size_factor),
        ] {
            if let Some(factor) = factor {
                anyhow::ensure!(
                    factor.is_finite(),
                    "ytsaurus.table_reader.{name} must be finite"
                );
            }
        }
        if let Some(probe_peer_count) = self.probe_peer_count {
            anyhow::ensure!(
                probe_peer_count > 0,
                "ytsaurus.table_reader.probe_peer_count must be positive"
            );
        }
        Ok(())
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceTableConfig {
    #[schemars(
        title = "Path",
        description = "Absolute YTsaurus table path. The final path component is used as the logical dataset name."
    )]
    pub path: String,
}

impl SourceTableConfig {
    pub fn dataset_name(&self) -> anyhow::Result<&str> {
        self.path
            .rsplit('/')
            .next()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "YTsaurus table path '{}' must end with a table name",
                    self.path
                )
            })
    }
}

impl YTsaurusSourceConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        let uses_native_rpc = self
            .benchmark_discard
            .as_ref()
            .is_none_or(|benchmark| benchmark.transport == YTsaurusBenchmarkTransport::NativeRpc);
        anyhow::ensure!(
            !uses_native_rpc || self.connection.trusted_native_rpc_plaintext,
            "ytsaurus native_rpc transport is plaintext; set trusted_native_rpc_plaintext=true to acknowledge that credentials and data will not be encrypted"
        );
        anyhow::ensure!(!self.tables.is_empty(), "ytsaurus.tables must not be empty");
        anyhow::ensure!(self.batch_rows > 0, "ytsaurus.batch_rows must be positive");
        anyhow::ensure!(
            self.stream_retry_max_attempts > 0,
            "ytsaurus.stream_retry_max_attempts must be positive"
        );
        anyhow::ensure!(
            self.stream_retry_initial_ms > 0,
            "ytsaurus.stream_retry_initial_ms must be positive"
        );
        anyhow::ensure!(
            self.stream_retry_max_ms >= self.stream_retry_initial_ms,
            "ytsaurus.stream_retry_max_ms must be at least stream_retry_initial_ms"
        );
        anyhow::ensure!(
            self.stream_open_timeout_ms > 0,
            "ytsaurus.stream_open_timeout_ms must be positive"
        );
        anyhow::ensure!(
            self.stream_idle_timeout_ms > 0,
            "ytsaurus.stream_idle_timeout_ms must be positive"
        );
        if let Some(benchmark_discard) = &self.benchmark_discard {
            benchmark_discard.table_reader.validate()?;
            match benchmark_discard.transport {
                YTsaurusBenchmarkTransport::Http => anyhow::ensure!(
                    benchmark_discard.format != YTsaurusReadFormat::YtWire,
                    "ytsaurus benchmark YT wire format requires native_rpc transport"
                ),
                YTsaurusBenchmarkTransport::NativeRpc => anyhow::ensure!(
                    matches!(
                        benchmark_discard.format,
                        YTsaurusReadFormat::Arrow | YTsaurusReadFormat::YtWire
                    ),
                    "ytsaurus native_rpc benchmark transport supports only arrow or yt_wire"
                ),
            }
        }
        if let Some(partition_tables) = self.read_ordering.partition_tables() {
            anyhow::ensure!(
                partition_tables.compressed_data_size_per_partition > 0,
                "ytsaurus.read_ordering.compressed_data_size_per_partition must be positive"
            );
            anyhow::ensure!(
                partition_tables.max_partition_count > 0,
                "ytsaurus.read_ordering.max_partition_count must be positive"
            );
            anyhow::ensure!(
                partition_tables.concurrency > 0,
                "ytsaurus.read_ordering.concurrency must be positive"
            );
            anyhow::ensure!(
                partition_tables.concurrency <= partition_tables.max_partition_count,
                "ytsaurus.read_ordering.concurrency must not exceed max_partition_count"
            );
        }
        self.table_reader.validate()?;
        let mut paths = HashSet::new();
        let mut names = HashSet::new();
        for table in &self.tables {
            validate_path(&table.path)?;
            let name = table.dataset_name()?;
            anyhow::ensure!(
                paths.insert(table.path.as_str()),
                "ytsaurus.tables repeats path '{}'",
                table.path
            );
            anyhow::ensure!(
                names.insert(name),
                "YTsaurus table paths must have unique final components; '{name}' is repeated"
            );
        }
        Ok(())
    }
}

const fn default_stream_retry_max_attempts() -> usize {
    DEFAULT_STREAM_RETRY_MAX_ATTEMPTS
}

const fn default_stream_retry_initial_ms() -> u64 {
    DEFAULT_STREAM_RETRY_INITIAL_MS
}

const fn default_stream_retry_max_ms() -> u64 {
    DEFAULT_STREAM_RETRY_MAX_MS
}

const fn default_stream_open_timeout_ms() -> u64 {
    DEFAULT_STREAM_OPEN_TIMEOUT_MS
}

const fn default_stream_idle_timeout_ms() -> u64 {
    DEFAULT_STREAM_IDLE_TIMEOUT_MS
}

const fn default_partition_compressed_bytes() -> u64 {
    DEFAULT_PARTITION_COMPRESSED_BYTES
}

const fn default_partition_count() -> usize {
    DEFAULT_PARTITION_COUNT
}

const fn default_partition_concurrency() -> usize {
    DEFAULT_PARTITION_CONCURRENCY
}

#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum YTsaurusWriteFormat {
    #[default]
    Arrow,
    Yson,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum YTsaurusPrimaryKeySemantics {
    #[default]
    #[schemars(title = "One row per primary key (sorted)")]
    UniqueSorted,

    #[schemars(title = "Preserve every row (unsorted)")]
    PreserveRows,
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusSinkConfig {
    #[schemars(
        title = "Table type",
        extend("x-ui" = {
            "control_width": "full",
            "defer_variant_details": true,
            "order": -100,
            "reveal_rest_on_selection": true
        })
    )]
    pub tables: YTsaurusTableMode,

    #[serde(default)]
    #[schemars(
        title = "Primary key semantics",
        description = "For schemas with primary keys, the default stages the snapshot, sorts it by the key and fails on duplicates. Preserve every row keeps the unsorted append semantics.",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub primary_key_semantics: YTsaurusPrimaryKeySemantics,

    #[serde(flatten)]
    pub connection: YTsaurusConnectionConfig,

    #[serde(default = "default_write_target_bytes")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub write_target_bytes: usize,

    #[serde(default = "default_write_concurrency")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub write_concurrency: usize,

    #[serde(default = "default_write_flush_interval_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub write_flush_interval_ms: u64,

    #[serde(default = "default_write_row_buffer_bytes")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub write_row_buffer_bytes: u64,

    #[serde(default)]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub table_writer: YTsaurusTableWriterConfig,

    #[serde(default = "default_primary_key_sort_timeout_ms")]
    #[schemars(extend("x-ui" = { "widget": "hidden" }))]
    pub primary_key_sort_timeout_ms: u64,
}

#[derive(Clone, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
#[allow(
    clippy::struct_field_names,
    reason = "these public field names mirror the corresponding YTsaurus writer settings"
)]
pub struct YTsaurusTableWriterConfig {
    #[serde(default = "default_table_writer_block_bytes")]
    pub block_size: u64,

    #[serde(default = "default_table_writer_buffer_bytes")]
    pub max_buffer_size: u64,

    #[serde(default = "default_table_writer_window_bytes")]
    pub writer_window_size: u64,

    #[serde(default = "default_table_writer_group_bytes")]
    pub writer_group_size: u64,

    #[serde(default = "default_table_writer_chunk_bytes")]
    pub desired_chunk_size: u64,
}

impl Default for YTsaurusTableWriterConfig {
    fn default() -> Self {
        Self {
            block_size: default_table_writer_block_bytes(),
            max_buffer_size: default_table_writer_buffer_bytes(),
            writer_window_size: default_table_writer_window_bytes(),
            writer_group_size: default_table_writer_group_bytes(),
            desired_chunk_size: default_table_writer_chunk_bytes(),
        }
    }
}

impl YTsaurusTableWriterConfig {
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.block_size > 0,
            "ytsaurus.table_writer.block_size must be positive"
        );
        anyhow::ensure!(
            self.max_buffer_size > 0,
            "ytsaurus.table_writer.max_buffer_size must be positive"
        );
        anyhow::ensure!(
            self.writer_window_size > 0,
            "ytsaurus.table_writer.writer_window_size must be positive"
        );
        anyhow::ensure!(
            self.writer_group_size > 0,
            "ytsaurus.table_writer.writer_group_size must be positive"
        );
        anyhow::ensure!(
            self.writer_group_size <= self.writer_window_size,
            "ytsaurus.table_writer.writer_group_size must not exceed writer_window_size"
        );
        anyhow::ensure!(
            self.desired_chunk_size > 0,
            "ytsaurus.table_writer.desired_chunk_size must be positive"
        );
        Ok(())
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum YTsaurusTableMode {
    #[schemars(title = "Static tables")]
    StaticTables {
        #[schemars(
            title = "Path",
            description = "Directory where dataset tables are stored"
        )]
        path: String,

        replace_tables: bool,

        #[serde(default)]
        #[schemars(
            title = "Driver exchange format",
            extend("x-ui" = { "section": "advanced" })
        )]
        format: YTsaurusWriteFormat,
    },

    #[schemars(title = "Dynamic tables")]
    DynamicTables {
        #[schemars(
            title = "Path",
            description = "Directory where dataset tables are stored"
        )]
        path: String,

        replace_tables: bool,

        #[serde(default)]
        #[schemars(
            title = "Driver exchange format",
            extend("x-ui" = { "section": "advanced" })
        )]
        format: YTsaurusWriteFormat,
    },
}

impl YTsaurusSinkConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        validate_path(self.path())?;
        anyhow::ensure!(
            self.write_target_bytes > 0,
            "ytsaurus.write_target_bytes must be positive"
        );
        anyhow::ensure!(
            (1..=32).contains(&self.write_concurrency),
            "ytsaurus.write_concurrency must be between 1 and 32"
        );
        anyhow::ensure!(
            self.write_flush_interval_ms > 0,
            "ytsaurus.write_flush_interval_ms must be positive"
        );
        anyhow::ensure!(
            self.write_row_buffer_bytes > 0,
            "ytsaurus.write_row_buffer_bytes must be positive"
        );
        anyhow::ensure!(
            self.primary_key_sort_timeout_ms > 0,
            "ytsaurus.primary_key_sort_timeout_ms must be positive"
        );
        self.table_writer.validate()?;
        Ok(())
    }

    #[must_use]
    pub fn path(&self) -> &str {
        match &self.tables {
            YTsaurusTableMode::StaticTables { path, .. }
            | YTsaurusTableMode::DynamicTables { path, .. } => path,
        }
    }

    #[must_use]
    pub const fn replace_tables(&self) -> bool {
        match &self.tables {
            YTsaurusTableMode::StaticTables { replace_tables, .. }
            | YTsaurusTableMode::DynamicTables { replace_tables, .. } => *replace_tables,
        }
    }

    #[must_use]
    pub const fn format(&self) -> YTsaurusWriteFormat {
        match &self.tables {
            YTsaurusTableMode::StaticTables { format, .. }
            | YTsaurusTableMode::DynamicTables { format, .. } => *format,
        }
    }

    #[must_use]
    pub const fn static_tables(&self) -> bool {
        matches!(self.tables, YTsaurusTableMode::StaticTables { .. })
    }

    pub fn path_for_dataset(&self, dataset: &str) -> anyhow::Result<String> {
        anyhow::ensure!(
            !dataset.is_empty(),
            "YTsaurus dataset name must not be empty"
        );
        anyhow::ensure!(
            !dataset.contains('/')
                && !dataset.contains('<')
                && !dataset.contains('>')
                && !dataset.contains('\0'),
            "YTsaurus dataset name '{dataset}' cannot be used as one table path segment"
        );
        Ok(format!("{}/{dataset}", self.path().trim_end_matches('/')))
    }
}

pub fn validate_path(path: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        path.starts_with("//"),
        "YTsaurus table path must start with '//'"
    );
    anyhow::ensure!(path.len() > 2, "YTsaurus table path must not be the root");
    anyhow::ensure!(
        !path.contains('<') && !path.contains('>') && !path.contains('\0'),
        "YTsaurus table path must not contain rich-path attributes or NUL"
    );
    Ok(())
}

const fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

const fn default_batch_rows() -> usize {
    DEFAULT_BATCH_ROWS
}

const fn default_write_target_bytes() -> usize {
    DEFAULT_WRITE_TARGET_BYTES
}

const fn default_write_concurrency() -> usize {
    DEFAULT_WRITE_CONCURRENCY
}

const fn default_write_flush_interval_ms() -> u64 {
    DEFAULT_WRITE_FLUSH_INTERVAL_MS
}

const fn default_write_row_buffer_bytes() -> u64 {
    DEFAULT_WRITE_ROW_BUFFER_BYTES
}

const fn default_table_writer_block_bytes() -> u64 {
    DEFAULT_TABLE_WRITER_BLOCK_BYTES
}

const fn default_table_writer_buffer_bytes() -> u64 {
    DEFAULT_TABLE_WRITER_BUFFER_BYTES
}

const fn default_table_writer_window_bytes() -> u64 {
    DEFAULT_TABLE_WRITER_WINDOW_BYTES
}

const fn default_table_writer_group_bytes() -> u64 {
    DEFAULT_TABLE_WRITER_GROUP_BYTES
}

const fn default_table_writer_chunk_bytes() -> u64 {
    DEFAULT_TABLE_WRITER_CHUNK_BYTES
}

const fn default_primary_key_sort_timeout_ms() -> u64 {
    DEFAULT_PRIMARY_KEY_SORT_TIMEOUT_MS
}
