#![allow(
    clippy::expect_used,
    reason = "formatting into an owned String is infallible"
)]

use std::collections::{BTreeMap, HashSet};
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
const DEFAULT_DYNAMIC_TRANSACTION_ROWS: usize = 50_000;
const DEFAULT_DYNAMIC_TRANSACTION_CONCURRENCY: usize = 8;
const DEFAULT_DYNAMIC_TRANSACTION_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_DYNAMIC_BUFFER_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_DYNAMIC_STORE_OVERFLOW_THRESHOLD: f64 = 0.5;
const DEFAULT_DYNAMIC_RETRY_INITIAL_MS: u64 = 100;
const DEFAULT_DYNAMIC_RETRY_MAX_MS: u64 = 5_000;
const DEFAULT_INITIAL_TABLET_COUNT: usize = 1;
const MAX_TABLET_COUNT: usize = 10_000;
const DEFAULT_TABLE_WRITER_BLOCK_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_TABLE_WRITER_BUFFER_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_TABLE_WRITER_WINDOW_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_TABLE_WRITER_GROUP_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_TABLE_WRITER_CHUNK_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const DEFAULT_PRIMARY_KEY_SORT_TIMEOUT_MS: u64 = 24 * 60 * 60 * 1_000;

const RESERVED_TABLE_ATTRIBUTES: [&str; 14] = [
    "atomicity",
    "auto_compaction_period",
    "chunk_format",
    "dynamic",
    "max_data_ttl",
    "max_data_versions",
    "merge_rows_on_flush",
    "min_data_ttl",
    "min_data_versions",
    "mount_config",
    "optimize_for",
    "primary_medium",
    "schema",
    "tablet_cell_bundle",
];

fn default_primary_medium() -> String {
    "default".to_owned()
}

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusYsonEntry {
    pub name: String,

    #[schemars(
        title = "YSON value",
        description = "Exact text YSON representation of the YT attribute value"
    )]
    pub value: String,
}

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

#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum YTsaurusOptimizeFor {
    #[default]
    #[schemars(title = "Scan (columnar chunks)")]
    Scan,

    #[schemars(title = "Lookup (row-oriented chunks)")]
    Lookup,
}

fn default_optimize_for() -> YTsaurusOptimizeFor {
    YTsaurusOptimizeFor::default()
}

#[derive(Clone, Copy, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum YTsaurusBigValuePolicy {
    #[default]
    #[schemars(title = "Fail delivery")]
    Fail,

    #[schemars(title = "Drop oversized rows")]
    Drop,
}

fn default_big_value_policy() -> YTsaurusBigValuePolicy {
    YTsaurusBigValuePolicy::default()
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum YTsaurusDynamicSnapshotMode {
    #[schemars(title = "Through static staging (recommended)")]
    StaticStaging {
        #[serde(default)]
        #[schemars(
            title = "Sort/merge operation pool",
            description = "Optional YTsaurus scheduler pool used only for the snapshot sort operation"
        )]
        operation_pool: Option<String>,
    },

    #[schemars(title = "Direct tablet writes")]
    Direct,
}

impl Default for YTsaurusDynamicSnapshotMode {
    fn default() -> Self {
        Self::StaticStaging {
            operation_pool: None,
        }
    }
}

fn default_dynamic_snapshot_mode() -> YTsaurusDynamicSnapshotMode {
    YTsaurusDynamicSnapshotMode::default()
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum YTsaurusAtomicity {
    #[default]
    Full,

    None,
}

impl YTsaurusAtomicity {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::None => "none",
        }
    }

    #[must_use]
    pub(super) const fn rpc_value(self) -> i32 {
        match self {
            Self::Full => 0,
            Self::None => 1,
        }
    }
}

impl YTsaurusOptimizeFor {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Lookup => "lookup",
        }
    }

    #[must_use]
    pub const fn chunk_format(self) -> &'static str {
        match self {
            Self::Scan => "table_unversioned_columnar",
            Self::Lookup => "table_unversioned_schemaless_horizontal",
        }
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusDynamicWriteConfig {
    #[serde(default = "default_dynamic_transaction_rows")]
    #[schemars(
        title = "Rows per tablet transaction",
        description = "Maximum rows committed by one lossless tablet transaction",
        range(min = 1, max = 100_000),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub transaction_rows: usize,

    #[serde(default = "default_dynamic_transaction_concurrency")]
    #[schemars(
        title = "Concurrent tablet transactions",
        description = "Maximum number of independent synchronous tablet transactions in flight",
        range(min = 1, max = 32),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub transaction_concurrency: usize,

    #[serde(default = "default_dynamic_transaction_timeout_ms")]
    #[schemars(
        title = "Tablet transaction timeout (ms)",
        range(min = 1),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub transaction_timeout_ms: u64,

    #[serde(default = "default_dynamic_buffer_bytes")]
    #[schemars(
        title = "Dynamic write buffer bytes",
        description = "Maximum Arrow bytes accumulated before a dynamic-table flush",
        range(min = 1),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub buffer_bytes: usize,

    #[serde(default = "default_dynamic_store_overflow_threshold")]
    #[schemars(
        title = "Dynamic store overflow threshold",
        description = "Flush threshold installed on dynamic tables created by Transferia; lower values leave the tablet node more headroom for sustained writes",
        range(min = 0.01, max = 0.99),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub dynamic_store_overflow_threshold: f64,

    #[serde(default = "default_true")]
    #[schemars(
        title = "Require a synchronous replica",
        description = "Fail writes to replicated tables unless at least one synchronous replica participates",
        extend("x-ui" = { "section": "advanced" })
    )]
    pub require_sync_replica: bool,

    #[serde(default = "default_dynamic_retry_initial_ms")]
    #[schemars(
        title = "Initial YTsaurus backpressure retry delay (ms)",
        range(min = 1),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub retry_initial_ms: u64,

    #[serde(default = "default_dynamic_retry_max_ms")]
    #[schemars(
        title = "Maximum YTsaurus backpressure retry delay (ms)",
        range(min = 1),
        extend("x-ui" = { "section": "advanced" })
    )]
    pub retry_max_ms: u64,
}

impl Default for YTsaurusDynamicWriteConfig {
    fn default() -> Self {
        Self {
            transaction_rows: default_dynamic_transaction_rows(),
            transaction_concurrency: default_dynamic_transaction_concurrency(),
            transaction_timeout_ms: default_dynamic_transaction_timeout_ms(),
            buffer_bytes: default_dynamic_buffer_bytes(),
            dynamic_store_overflow_threshold: default_dynamic_store_overflow_threshold(),
            require_sync_replica: true,
            retry_initial_ms: default_dynamic_retry_initial_ms(),
            retry_max_ms: default_dynamic_retry_max_ms(),
        }
    }
}

impl YTsaurusDynamicWriteConfig {
    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (1..=100_000).contains(&self.transaction_rows),
            "ytsaurus dynamic transaction_rows must be between 1 and 100000"
        );
        anyhow::ensure!(
            (1..=32).contains(&self.transaction_concurrency),
            "ytsaurus dynamic transaction_concurrency must be between 1 and 32"
        );
        anyhow::ensure!(
            self.transaction_timeout_ms > 0,
            "ytsaurus dynamic transaction_timeout_ms must be positive"
        );
        anyhow::ensure!(
            self.buffer_bytes > 0,
            "ytsaurus dynamic buffer_bytes must be positive"
        );
        anyhow::ensure!(
            self.dynamic_store_overflow_threshold.is_finite()
                && (0.0..1.0).contains(&self.dynamic_store_overflow_threshold),
            "ytsaurus dynamic_store_overflow_threshold must be finite and between 0 and 1"
        );
        anyhow::ensure!(
            self.retry_initial_ms > 0,
            "ytsaurus dynamic retry_initial_ms must be positive"
        );
        anyhow::ensure!(
            self.retry_max_ms >= self.retry_initial_ms,
            "ytsaurus dynamic retry_max_ms must be at least retry_initial_ms"
        );
        Ok(())
    }
}

#[derive(Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct YTsaurusSinkConfig {
    #[schemars(
        title = "Table type",
        extend("x-ui" = {
            "control_width": "full",
            "defer_variant_details": true,
            "indent_variant_details": false,
            "order": -100,
            "reveal_rest_on_selection": true
        })
    )]
    pub tables: YTsaurusTableMode,

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
            title = "Primary key semantics",
            description = "For schemas with primary keys, the default stages the snapshot, sorts it by the key and fails on duplicates. Preserve every row keeps the unsorted append semantics.",
            extend("x-ui" = { "section": "advanced" })
        )]
        primary_key_semantics: YTsaurusPrimaryKeySemantics,

        #[serde(default = "default_primary_medium")]
        #[schemars(
            title = "Primary medium",
            description = "YT storage medium used for newly created tables. The default value selects the cluster's default medium.",
            extend("x-ui" = { "section": "advanced" })
        )]
        primary_medium: String,

        #[serde(default)]
        #[schemars(
            title = "Table attributes",
            description = "Additional attributes for every newly created table. Structural attributes have dedicated settings and cannot be overridden here.",
            extend("x-ui" = { "section": "advanced", "widget": "compact_array", "item_label": "attribute" })
        )]
        table_attributes: Vec<YTsaurusYsonEntry>,

        #[serde(default = "default_big_value_policy")]
        #[schemars(
            default = "default_big_value_policy",
            title = "Oversized values",
            description = "Fail preserves every source row. Drop explicitly acknowledges and discards an entire row if a value or the row exceeds YTsaurus storage limits.",
            extend("x-ui" = { "section": "advanced" })
        )]
        big_value_policy: YTsaurusBigValuePolicy,

        #[serde(default = "default_optimize_for")]
        #[schemars(
            default = "default_optimize_for",
            title = "Optimize for",
            description = "Physical layout for newly written static-table chunks",
            extend("x-ui" = { "section": "advanced" })
        )]
        optimize_for: YTsaurusOptimizeFor,

        #[serde(default)]
        #[schemars(
            title = "YT Spec",
            description = "Additional YT table_writer parameters. Explicit entries override Transferia's writer defaults.",
            extend("x-ui" = { "section": "advanced", "widget": "compact_array", "item_label": "parameter" })
        )]
        spec: Vec<YTsaurusYsonEntry>,

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
            title = "Primary key semantics",
            description = "Dynamic tables require one row per primary key.",
            extend("x-ui" = { "section": "advanced" })
        )]
        primary_key_semantics: YTsaurusPrimaryKeySemantics,

        #[serde(default = "default_primary_medium")]
        #[schemars(
            title = "Primary medium",
            description = "YT storage medium used for newly created tables. The default value selects the cluster's default medium.",
            extend("x-ui" = { "section": "advanced" })
        )]
        primary_medium: String,

        #[serde(default)]
        #[schemars(
            title = "Table attributes",
            description = "Additional attributes for every newly created table. Structural attributes have dedicated settings and cannot be overridden here.",
            extend("x-ui" = { "section": "advanced", "widget": "compact_array", "item_label": "attribute" })
        )]
        table_attributes: Vec<YTsaurusYsonEntry>,

        #[serde(default = "default_big_value_policy")]
        #[schemars(
            default = "default_big_value_policy",
            title = "Oversized values",
            description = "Fail preserves every source row. Drop explicitly acknowledges and discards an entire row if a value or the row exceeds YTsaurus storage limits.",
            extend("x-ui" = { "section": "advanced" })
        )]
        big_value_policy: YTsaurusBigValuePolicy,

        #[serde(default = "default_dynamic_snapshot_mode")]
        #[schemars(
            default = "default_dynamic_snapshot_mode",
            title = "Batch snapshot delivery",
            description = "Static staging writes the complete finite snapshot efficiently, verifies primary-key uniqueness, sorts it and converts it into a dynamic table before committing source progress",
            extend("x-ui" = { "delivery_types": ["batch"] })
        )]
        snapshot_mode: YTsaurusDynamicSnapshotMode,

        #[serde(default)]
        #[schemars(
            title = "Atomicity",
            description = "Full provides all-or-nothing tablet transactions. None weakens atomicity explicitly and may expose a partial write if a multi-row transaction fails.",
            extend("x-ui" = { "section": "advanced" })
        )]
        atomicity: YTsaurusAtomicity,

        #[serde(default)]
        #[schemars(
            title = "Tablet cell bundle",
            description = "Optional bundle used when Transferia creates the dynamic table",
            extend("x-ui" = { "section": "advanced" })
        )]
        tablet_cell_bundle: Option<String>,

        #[serde(default = "default_initial_tablet_count")]
        #[schemars(
            title = "Initial tablet count",
            description = "Number of tablets created before the table is mounted. Values above one require an integral first primary-key column so YTsaurus can derive lossless uniform pivot keys.",
            range(min = 1, max = 10_000),
            extend("x-ui" = { "section": "advanced" })
        )]
        initial_tablet_count: usize,

        #[serde(default)]
        #[schemars(
            title = "Table TTL",
            description = "Delete dynamic-table rows after the configured duration. Months are exactly 30 days and years are exactly 365 days. Disabled by default so data is never expired implicitly.",
            range(min = 1),
            extend("x-ui" = { "section": "advanced", "widget": "duration_scale" })
        )]
        table_ttl_ms: Option<u64>,

        #[serde(default)]
        #[schemars(extend("x-ui" = { "widget": "hidden" }))]
        write: YTsaurusDynamicWriteConfig,
    },
}

impl YTsaurusSinkConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        self.connection.validate()?;
        validate_path(self.path())?;
        anyhow::ensure!(
            !self.primary_medium().trim().is_empty(),
            "ytsaurus.primary_medium must not be empty"
        );
        self.parsed_table_attributes()?;
        self.parsed_writer_spec()?;
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
        if let Some(write) = self.dynamic_write() {
            anyhow::ensure!(
                self.connection.trusted_native_rpc_plaintext,
                "ytsaurus dynamic-table writes use plaintext native_rpc; set trusted_native_rpc_plaintext=true to acknowledge that credentials and data will not be encrypted"
            );
            write.validate()?;
            if let Some(bundle) = self.tablet_cell_bundle() {
                anyhow::ensure!(
                    !bundle.trim().is_empty(),
                    "ytsaurus dynamic tablet_cell_bundle must not be empty"
                );
            }
            if let Some(pool) = self.dynamic_snapshot_operation_pool() {
                anyhow::ensure!(
                    !pool.trim().is_empty(),
                    "ytsaurus dynamic snapshot operation_pool must not be empty"
                );
            }
            let initial_tablet_count = self
                .initial_tablet_count()
                .ok_or_else(|| anyhow::anyhow!("dynamic table has no initial tablet count"))?;
            anyhow::ensure!(
                (1..=MAX_TABLET_COUNT).contains(&initial_tablet_count),
                "ytsaurus dynamic initial_tablet_count must be between 1 and {MAX_TABLET_COUNT}"
            );
            if let Some(ttl_ms) = self.dynamic_table_ttl_ms() {
                anyhow::ensure!(ttl_ms > 0, "ytsaurus dynamic table_ttl_ms must be positive");
            }
        }
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
    pub const fn primary_key_semantics(&self) -> YTsaurusPrimaryKeySemantics {
        match &self.tables {
            YTsaurusTableMode::StaticTables {
                primary_key_semantics,
                ..
            }
            | YTsaurusTableMode::DynamicTables {
                primary_key_semantics,
                ..
            } => *primary_key_semantics,
        }
    }

    #[must_use]
    pub fn primary_medium(&self) -> &str {
        match &self.tables {
            YTsaurusTableMode::StaticTables { primary_medium, .. }
            | YTsaurusTableMode::DynamicTables { primary_medium, .. } => primary_medium,
        }
    }

    #[must_use]
    pub fn big_value_policy(&self) -> YTsaurusBigValuePolicy {
        match &self.tables {
            YTsaurusTableMode::StaticTables {
                big_value_policy, ..
            }
            | YTsaurusTableMode::DynamicTables {
                big_value_policy, ..
            } => *big_value_policy,
        }
    }

    fn table_attributes(&self) -> &[YTsaurusYsonEntry] {
        match &self.tables {
            YTsaurusTableMode::StaticTables {
                table_attributes, ..
            }
            | YTsaurusTableMode::DynamicTables {
                table_attributes, ..
            } => table_attributes,
        }
    }

    #[must_use]
    pub const fn static_format(&self) -> Option<YTsaurusWriteFormat> {
        match &self.tables {
            YTsaurusTableMode::StaticTables { format, .. } => Some(*format),
            YTsaurusTableMode::DynamicTables { .. } => None,
        }
    }

    #[must_use]
    pub const fn static_optimize_for(&self) -> Option<YTsaurusOptimizeFor> {
        match &self.tables {
            YTsaurusTableMode::StaticTables { optimize_for, .. } => Some(*optimize_for),
            YTsaurusTableMode::DynamicTables { .. } => None,
        }
    }

    #[must_use]
    pub const fn dynamic_write(&self) -> Option<&YTsaurusDynamicWriteConfig> {
        match &self.tables {
            YTsaurusTableMode::StaticTables { .. } => None,
            YTsaurusTableMode::DynamicTables { write, .. } => Some(write),
        }
    }

    #[must_use]
    pub const fn dynamic_atomicity(&self) -> Option<YTsaurusAtomicity> {
        match &self.tables {
            YTsaurusTableMode::StaticTables { .. } => None,
            YTsaurusTableMode::DynamicTables { atomicity, .. } => Some(*atomicity),
        }
    }

    #[must_use]
    pub fn tablet_cell_bundle(&self) -> Option<&str> {
        match &self.tables {
            YTsaurusTableMode::StaticTables { .. } => None,
            YTsaurusTableMode::DynamicTables {
                tablet_cell_bundle, ..
            } => tablet_cell_bundle.as_deref(),
        }
    }

    #[must_use]
    pub const fn initial_tablet_count(&self) -> Option<usize> {
        match &self.tables {
            YTsaurusTableMode::StaticTables { .. } => None,
            YTsaurusTableMode::DynamicTables {
                initial_tablet_count,
                ..
            } => Some(*initial_tablet_count),
        }
    }

    #[must_use]
    pub const fn dynamic_table_ttl_ms(&self) -> Option<u64> {
        match &self.tables {
            YTsaurusTableMode::StaticTables { .. } => None,
            YTsaurusTableMode::DynamicTables { table_ttl_ms, .. } => *table_ttl_ms,
        }
    }

    #[must_use]
    pub const fn stages_dynamic_snapshots(&self) -> bool {
        matches!(
            self.tables,
            YTsaurusTableMode::DynamicTables {
                snapshot_mode: YTsaurusDynamicSnapshotMode::StaticStaging { .. },
                ..
            }
        )
    }

    #[must_use]
    pub fn dynamic_snapshot_operation_pool(&self) -> Option<&str> {
        match &self.tables {
            YTsaurusTableMode::DynamicTables {
                snapshot_mode: YTsaurusDynamicSnapshotMode::StaticStaging { operation_pool },
                ..
            } => operation_pool.as_deref(),
            YTsaurusTableMode::StaticTables { .. }
            | YTsaurusTableMode::DynamicTables {
                snapshot_mode: YTsaurusDynamicSnapshotMode::Direct,
                ..
            } => None,
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

    pub(super) fn parsed_table_attributes(
        &self,
    ) -> anyhow::Result<BTreeMap<String, serde_json::Value>> {
        let attributes = parse_yson_entries(self.table_attributes(), "YTsaurus table attribute")?;
        for name in attributes.keys() {
            anyhow::ensure!(
                !RESERVED_TABLE_ATTRIBUTES.contains(&name.as_str()),
                "YTsaurus table attribute '{name}' has a dedicated configuration field and cannot be overridden"
            );
        }
        Ok(attributes)
    }

    pub(super) fn parsed_writer_spec(&self) -> anyhow::Result<BTreeMap<String, serde_json::Value>> {
        let entries = match &self.tables {
            YTsaurusTableMode::StaticTables { spec, .. } => spec,
            YTsaurusTableMode::DynamicTables { .. } => return Ok(BTreeMap::new()),
        };
        parse_yson_entries(entries, "YT Spec")
    }
}

fn parse_yson_entries(
    entries: &[YTsaurusYsonEntry],
    subject: &str,
) -> anyhow::Result<BTreeMap<String, serde_json::Value>> {
    let mut values = BTreeMap::new();
    for entry in entries {
        anyhow::ensure!(
            !entry.name.is_empty() && !entry.name.contains('\0'),
            "{subject} parameter names must be non-empty and contain no NUL"
        );
        let value = parse_text_yson(&entry.value).map_err(|error| {
            anyhow::anyhow!(
                "{subject} parameter '{}' has invalid YSON: {error}",
                entry.name
            )
        })?;
        anyhow::ensure!(
            values.insert(entry.name.clone(), value).is_none(),
            "{subject} parameter '{}' is configured more than once",
            entry.name
        );
    }
    Ok(values)
}

fn parse_text_yson(input: &str) -> anyhow::Result<serde_json::Value> {
    let mut parser = TextYsonParser {
        input: input.as_bytes(),
        offset: 0,
    };
    let value = parser.value()?;
    parser.whitespace();
    anyhow::ensure!(parser.offset == parser.input.len(), "unexpected trailing input");
    Ok(value)
}

struct TextYsonParser<'a> {
    input: &'a [u8],
    offset: usize,
}

impl TextYsonParser<'_> {
    fn value(&mut self) -> anyhow::Result<serde_json::Value> {
        self.whitespace();
        match self.peek() {
            Some(b'#') => {
                self.offset += 1;
                Ok(serde_json::Value::Null)
            }
            Some(b'{') => self.map(b'{', b'}').map(serde_json::Value::Object),
            Some(b'[') => self.list(),
            Some(b'<') => {
                let attributes = self.map(b'<', b'>')?;
                let value = self.value()?;
                Ok(serde_json::json!({ "$attributes": attributes, "$value": value }))
            }
            Some(b'"') => Ok(serde_json::Value::String(self.quoted()?)),
            Some(_) => self.scalar(),
            None => anyhow::bail!("value is empty"),
        }
    }

    fn map(
        &mut self,
        opening: u8,
        closing: u8,
    ) -> anyhow::Result<serde_json::Map<String, serde_json::Value>> {
        self.expect(opening)?;
        let mut output = serde_json::Map::new();
        loop {
            self.whitespace();
            if self.take(closing) {
                return Ok(output);
            }
            let key = if self.peek() == Some(b'"') {
                self.quoted()?
            } else {
                self.token(&[b'='])?
            };
            anyhow::ensure!(!key.is_empty(), "map key is empty");
            self.expect(b'=')?;
            let value = self.value()?;
            anyhow::ensure!(output.insert(key.clone(), value).is_none(), "duplicate key '{key}'");
            self.separator_or_end(closing)?;
        }
    }

    fn list(&mut self) -> anyhow::Result<serde_json::Value> {
        self.expect(b'[')?;
        let mut output = Vec::new();
        loop {
            self.whitespace();
            if self.take(b']') {
                return Ok(serde_json::Value::Array(output));
            }
            output.push(self.value()?);
            self.separator_or_end(b']')?;
        }
    }

    fn scalar(&mut self) -> anyhow::Result<serde_json::Value> {
        let token = self.token(&[b';', b',', b']', b'}', b'>'])?;
        match token.as_str() {
            "%true" => Ok(serde_json::Value::Bool(true)),
            "%false" => Ok(serde_json::Value::Bool(false)),
            token if token.ends_with('u') => {
                let value = token[..token.len() - 1].parse::<u64>()?;
                Ok(serde_json::json!({ "$uint64": value.to_string() }))
            }
            "%nan" | "%inf" | "%-inf" => {
                Ok(serde_json::json!({ "$double": token }))
            }
            token if token.starts_with('%') => anyhow::bail!("invalid YSON scalar '{token}'"),
            token => match token.parse::<serde_json::Number>() {
                Ok(number) => Ok(serde_json::Value::Number(number)),
                Err(_) if !token.is_empty() => Ok(serde_json::Value::String(token.to_owned())),
                Err(error) => Err(error.into()),
            },
        }
    }

    fn quoted(&mut self) -> anyhow::Result<String> {
        let start = self.offset;
        self.expect(b'"')?;
        let mut escaped = false;
        while let Some(byte) = self.peek() {
            self.offset += 1;
            if byte == b'"' && !escaped {
                return Ok(serde_json::from_slice(&self.input[start..self.offset])?);
            }
            escaped = byte == b'\\' && !escaped;
            if byte != b'\\' {
                escaped = false;
            }
        }
        anyhow::bail!("unterminated quoted string")
    }

    fn token(&mut self, delimiters: &[u8]) -> anyhow::Result<String> {
        self.whitespace();
        let start = self.offset;
        while let Some(byte) = self.peek() {
            if delimiters.contains(&byte) || byte.is_ascii_whitespace() {
                break;
            }
            self.offset += 1;
        }
        Ok(std::str::from_utf8(&self.input[start..self.offset])?.to_owned())
    }

    fn separator_or_end(&mut self, closing: u8) -> anyhow::Result<()> {
        self.whitespace();
        if self.peek() == Some(closing) {
            return Ok(());
        }
        anyhow::ensure!(self.take(b';') || self.take(b','), "expected a separator");
        Ok(())
    }

    fn expect(&mut self, expected: u8) -> anyhow::Result<()> {
        self.whitespace();
        anyhow::ensure!(self.take(expected), "expected '{}'", char::from(expected));
        Ok(())
    }

    fn take(&mut self, expected: u8) -> bool {
        if self.peek() != Some(expected) {
            return false;
        }
        self.offset += 1;
        true
    }

    fn whitespace(&mut self) {
        while self.peek().is_some_and(|byte| byte.is_ascii_whitespace()) {
            self.offset += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.offset).copied()
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

const fn default_dynamic_transaction_rows() -> usize {
    DEFAULT_DYNAMIC_TRANSACTION_ROWS
}

const fn default_dynamic_transaction_concurrency() -> usize {
    DEFAULT_DYNAMIC_TRANSACTION_CONCURRENCY
}

const fn default_dynamic_transaction_timeout_ms() -> u64 {
    DEFAULT_DYNAMIC_TRANSACTION_TIMEOUT_MS
}

const fn default_dynamic_buffer_bytes() -> usize {
    DEFAULT_DYNAMIC_BUFFER_BYTES
}

const fn default_dynamic_store_overflow_threshold() -> f64 {
    DEFAULT_DYNAMIC_STORE_OVERFLOW_THRESHOLD
}

const fn default_dynamic_retry_initial_ms() -> u64 {
    DEFAULT_DYNAMIC_RETRY_INITIAL_MS
}

const fn default_dynamic_retry_max_ms() -> u64 {
    DEFAULT_DYNAMIC_RETRY_MAX_MS
}

const fn default_initial_tablet_count() -> usize {
    DEFAULT_INITIAL_TABLET_COUNT
}

const fn default_true() -> bool {
    true
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
