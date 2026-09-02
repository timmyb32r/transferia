//! Cross-connector rules. This module contains no connector implementation or
//! connector-specific config types: only neutral capability descriptors and the
//! rules that relate a source pipeline to a sink.

use arrow::datatypes::DataType;
use schemars::JsonSchema;
use serde::Serialize;

use crate::DeliveryType;
use transferia_core::data::system_columns::SystemColumnKind;
use transferia_core::delivery::{DatasetRole, DeliveryDiscovery, DiscoveredDataset};

#[derive(Debug, Clone)]
pub enum EndpointDescriptor {
    Logbroker(SourceDescriptor),
    Kafka(SourceDescriptor),
    MySql(SourceDescriptor),
    Postgres(SourceDescriptor),
    YdbSource(SourceDescriptor),
    YTsaurus(SourceDescriptor),
    ClickHouseSource(SourceDescriptor),
    S3Source(SourceDescriptor),
    IcebergSource(SourceDescriptor),
    DataGenerator(SourceDescriptor),
    MySqlSink,
    PostgresSink,
    YdbSink,
    YTsaurusSink(YTsaurusSinkMode),
    LogbrokerSink(QueueSinkDescriptor),
    KafkaSink(QueueSinkDescriptor),
    ClickHouse,
    S3(S3Descriptor),
    IcebergSink,
    /// Benchmark-only sink which durably stores nothing.
    Discard,
}

impl EndpointDescriptor {
    #[must_use]
    pub const fn source_behavior(&self) -> Option<SourceBehavior> {
        match self {
            Self::Logbroker(source)
            | Self::Kafka(source)
            | Self::MySql(source)
            | Self::Postgres(source)
            | Self::YdbSource(source)
            | Self::YTsaurus(source)
            | Self::ClickHouseSource(source)
            | Self::S3Source(source)
            | Self::IcebergSource(source)
            | Self::DataGenerator(source) => Some(source.behavior),
            Self::MySqlSink
            | Self::PostgresSink
            | Self::YdbSink
            | Self::YTsaurusSink(_)
            | Self::LogbrokerSink(_)
            | Self::KafkaSink(_)
            | Self::ClickHouse
            | Self::S3(_)
            | Self::IcebergSink
            | Self::Discard => None,
        }
    }

    #[must_use]
    pub const fn supports_delivery_type(&self, delivery_type: DeliveryType) -> bool {
        match self {
            Self::Logbroker(source)
            | Self::Kafka(source)
            | Self::MySql(source)
            | Self::Postgres(source)
            | Self::YdbSource(source)
            | Self::YTsaurus(source)
            | Self::ClickHouseSource(source)
            | Self::S3Source(source)
            | Self::IcebergSource(source)
            | Self::DataGenerator(source) => source.delivery_modes.supports(delivery_type),
            Self::MySqlSink
            | Self::PostgresSink
            | Self::YdbSink
            | Self::YTsaurusSink(_)
            | Self::LogbrokerSink(_)
            | Self::KafkaSink(_)
            | Self::ClickHouse
            | Self::S3(_)
            | Self::IcebergSink
            | Self::Discard => false,
        }
    }

    #[must_use]
    pub const fn record_semantics(&self) -> Option<RecordSemantics> {
        match self.source_behavior() {
            Some(SourceBehavior::AppendOnlyRows | SourceBehavior::FiniteAppendOnlyRows) => {
                Some(RecordSemantics::AppendOnly)
            }
            Some(SourceBehavior::ChangelogRows) => Some(RecordSemantics::Changelog),
            Some(SourceBehavior::BenchmarkDiscard) | None => None,
        }
    }

    #[must_use]
    pub const fn accepts_record_semantics(&self, semantics: RecordSemantics) -> bool {
        match semantics {
            RecordSemantics::AppendOnly => matches!(
                self,
                Self::MySqlSink
                    | Self::PostgresSink
                    | Self::YdbSink
                    | Self::YTsaurusSink(_)
                    | Self::LogbrokerSink(_)
                    | Self::KafkaSink(_)
                    | Self::ClickHouse
                    | Self::S3(_)
                    | Self::IcebergSink
                    | Self::Discard
            ),
            // Enable changelog input for a destination only together with its
            // replay-safe operation-aware writer implementation.
            RecordSemantics::Changelog => matches!(
                self,
                Self::MySqlSink
                    | Self::PostgresSink
                    | Self::YdbSink
                    | Self::YTsaurusSink(YTsaurusSinkMode::Dynamic)
                    | Self::ClickHouse
                    | Self::Discard
            ) || matches!(
                self,
                Self::LogbrokerSink(QueueSinkDescriptor {
                    changelog: true
                }) | Self::KafkaSink(QueueSinkDescriptor { changelog: true })
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueSinkDescriptor {
    pub changelog: bool,
}

#[derive(Debug, Clone)]
pub struct SourceDescriptor {
    pub behavior: SourceBehavior,

    pub delivery_modes: SourceDeliveryModes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceDeliveryModes {
    batch: bool,
    stream: bool,
}

impl SourceDeliveryModes {
    pub const BATCH: Self = Self {
        batch: true,
        stream: false,
    };
    pub const STREAM: Self = Self {
        batch: false,
        stream: true,
    };
    pub const BATCH_AND_STREAM: Self = Self {
        batch: true,
        stream: true,
    };

    #[must_use]
    pub const fn supports(self, delivery_type: DeliveryType) -> bool {
        match delivery_type {
            DeliveryType::Batch => self.batch,
            DeliveryType::Stream => self.stream,
            DeliveryType::BatchAndStream => self.batch && self.stream,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceBehavior {
    AppendOnlyRows,
    FiniteAppendOnlyRows,
    ChangelogRows,
    /// Benchmark-only mode which advances source offsets without producing rows.
    BenchmarkDiscard,
}

#[derive(Debug, Clone, Copy, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordSemantics {
    AppendOnly,
    Changelog,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum YTsaurusSinkMode {
    Static,
    Dynamic,
}

#[derive(Debug, Clone)]
pub struct S3Descriptor {
    pub partitioning: S3Partitioning,
    pub record_time_rotation: bool,
    pub wall_clock_rotation: bool,
    pub object_layout_version: u32,
}

#[derive(Debug, Clone)]
pub enum S3Partitioning {
    Source,
    Fields(Vec<String>),
    RecordTime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryGuarantee {
    ExactlyOnce,
    AtLeastOnce,
    NoDurability,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticSeverity {
    Error,
    Info,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DiagnosticCode {
    UnsupportedPipeline,
    UnsupportedRecordSemantics,
    InvalidDeliveryDiscovery,
    MissingSystemColumn,
    SystemColumnsNotProduced,
    UnknownPartitionField,
    NullablePartitionField,
    UnsupportedPartitionFieldType,
    WallClockRotationDisablesExactlyOnce,
    DeterministicS3Commit,
    ClickHouseAtLeastOnce,
    MySqlAtLeastOnce,
    PostgresAtLeastOnce,
    YdbAtLeastOnce,
    YTsaurusAtLeastOnce,
    IcebergAtLeastOnce,
    PqV1AtLeastOnce,
    KafkaAtLeastOnce,
    BenchmarkDiscard,
    BenchmarkSourceDiscard,
}

#[derive(Debug, Clone, Serialize)]
pub struct SemanticsDiagnostic {
    pub code: DiagnosticCode,
    pub severity: DiagnosticSeverity,
    pub config_paths: Vec<String>,
    pub explanation: String,
    pub remediation: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeliverySemanticsReport {
    pub guarantee: DeliveryGuarantee,
    pub diagnostics: Vec<SemanticsDiagnostic>,
}

impl DeliverySemanticsReport {
    pub fn ensure_valid(&self) -> anyhow::Result<()> {
        let errors: Vec<_> = self
            .diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
            .map(|diagnostic| format!("{:?}: {}", diagnostic.code, diagnostic.explanation))
            .collect();
        anyhow::ensure!(
            errors.is_empty(),
            "incompatible source/sink configuration:\n{}",
            errors.join("\n")
        );
        Ok(())
    }
}

#[must_use]
pub fn validate_pipeline(
    source: &EndpointDescriptor,
    sink: &EndpointDescriptor,
    discovery: &DeliveryDiscovery,
    keep_system_columns: bool,
) -> DeliverySemanticsReport {
    if matches!(sink, EndpointDescriptor::Discard) {
        let benchmark_source = source.source_behavior() == Some(SourceBehavior::BenchmarkDiscard);
        return DeliverySemanticsReport {
            guarantee: DeliveryGuarantee::NoDurability,
            diagnostics: vec![if benchmark_source {
                SemanticsDiagnostic {
                    code: DiagnosticCode::BenchmarkDiscard,
                    severity: DiagnosticSeverity::Info,
                    config_paths: vec!["source.parser.benchmark_discard".into(), "sink.discard".into()],
                    explanation: "the benchmark parser and discard sink intentionally acknowledge messages without producing or storing rows".into(),
                    remediation: Some("configure a row-producing parser and durable sink for a real delivery".into()),
                }
            } else {
                SemanticsDiagnostic {
                    code: DiagnosticCode::BenchmarkDiscard,
                    severity: DiagnosticSeverity::Info,
                    config_paths: vec!["sink.discard".into()],
                    explanation: "the discard sink intentionally acknowledges source messages without storing parsed rows".into(),
                    remediation: Some("choose a durable sink when the delivery must preserve data".into()),
                }
            }],
        };
    }
    if source.source_behavior() == Some(SourceBehavior::BenchmarkDiscard) {
        return DeliverySemanticsReport {
            guarantee: DeliveryGuarantee::NoDurability,
            diagnostics: vec![error(
                DiagnosticCode::BenchmarkSourceDiscard,
                &["source", "sink"],
                "the source is configured to discard records before producing rows, so a durable sink would acknowledge data it never stored",
                Some("disable the source benchmark-discard mode, or use the benchmark-only discard sink"),
            )],
        };
    }
    if let Some(record_semantics) = source.record_semantics() {
        if !sink.accepts_record_semantics(record_semantics) {
            return DeliverySemanticsReport {
                guarantee: DeliveryGuarantee::NoDurability,
                diagnostics: vec![error(
                    DiagnosticCode::UnsupportedRecordSemantics,
                    &["source", "sink"],
                    &format!(
                        "the configured sink cannot preserve {record_semantics:?} source records"
                    ),
                    Some(
                        "choose a sink with explicit changelog application support, or use an append-only source",
                    ),
                )],
            };
        }
    }
    if matches!(sink, EndpointDescriptor::ClickHouse) {
        return DeliverySemanticsReport {
            guarantee: DeliveryGuarantee::AtLeastOnce,
            diagnostics: vec![SemanticsDiagnostic {
                code: DiagnosticCode::ClickHouseAtLeastOnce,
                severity: DiagnosticSeverity::Info,
                config_paths: vec!["sink.clickhouse".into()],
                explanation: "ClickHouse INSERT completion precedes source commit, but a retry after an ambiguous INSERT result may duplicate rows".into(),
                remediation: Some("use a ClickHouse-side deduplication strategy if exactly-once final state is required".into()),
            }],
        };
    }
    if matches!(sink, EndpointDescriptor::PostgresSink) {
        return DeliverySemanticsReport {
            guarantee: DeliveryGuarantee::AtLeastOnce,
            diagnostics: vec![SemanticsDiagnostic {
                code: DiagnosticCode::PostgresAtLeastOnce,
                severity: DiagnosticSeverity::Info,
                config_paths: vec!["sink.postgres".into()],
                explanation: "PostgreSQL COPY completion precedes source progress commit, so a retry after an ambiguous COPY result may duplicate rows".into(),
                remediation: Some("include a user-defined idempotency key and enforce it at the destination when duplicate-free final state is required".into()),
            }],
        };
    }
    if matches!(sink, EndpointDescriptor::MySqlSink) {
        return DeliverySemanticsReport {
            guarantee: DeliveryGuarantee::AtLeastOnce,
            diagnostics: vec![SemanticsDiagnostic {
                code: DiagnosticCode::MySqlAtLeastOnce,
                severity: DiagnosticSeverity::Info,
                config_paths: vec!["sink.mysql".into()],
                explanation: "MySQL COMMIT completion precedes source progress commit, so a retry after an ambiguous COMMIT may duplicate rows".into(),
                remediation: Some("include a user-defined idempotency key and enforce it at the destination when duplicate-free final state is required".into()),
            }],
        };
    }
    if matches!(sink, EndpointDescriptor::YdbSink) {
        return DeliverySemanticsReport {
            guarantee: DeliveryGuarantee::AtLeastOnce,
            diagnostics: vec![SemanticsDiagnostic {
                code: DiagnosticCode::YdbAtLeastOnce,
                severity: DiagnosticSeverity::Info,
                config_paths: vec!["sink.ydb".into()],
                explanation: "YDB BulkUpsert completion precedes source progress commit; replay is idempotent only while primary-key values and delivery order remain stable".into(),
                remediation: Some("preserve stable primary keys; a replay then replaces the same YDB rows without duplicating them".into()),
            }],
        };
    }
    if matches!(sink, EndpointDescriptor::YTsaurusSink(_)) {
        return DeliverySemanticsReport {
            guarantee: DeliveryGuarantee::AtLeastOnce,
            diagnostics: vec![SemanticsDiagnostic {
                code: DiagnosticCode::YTsaurusAtLeastOnce,
                severity: DiagnosticSeverity::Info,
                config_paths: vec!["sink.ytsaurus".into()],
                explanation: "YTsaurus append completion precedes source progress commit, so a retry after an ambiguous write may duplicate rows".into(),
                remediation: Some("include a user-defined idempotency key when duplicate-free final state is required".into()),
            }],
        };
    }
    if matches!(sink, EndpointDescriptor::IcebergSink) {
        return DeliverySemanticsReport {
            guarantee: DeliveryGuarantee::AtLeastOnce,
            diagnostics: vec![SemanticsDiagnostic {
                code: DiagnosticCode::IcebergAtLeastOnce,
                severity: DiagnosticSeverity::Info,
                config_paths: vec!["sink.iceberg".into()],
                explanation: "an Iceberg snapshot commit precedes source progress commit, so replay after an ambiguous source commit may append duplicate rows".into(),
                remediation: Some("include a stable row identity and run Iceberg compaction/deduplication when duplicate-free final state is required".into()),
            }],
        };
    }
    if matches!(sink, EndpointDescriptor::LogbrokerSink(_)) {
        return DeliverySemanticsReport { guarantee: DeliveryGuarantee::AtLeastOnce, diagnostics: vec![SemanticsDiagnostic { code: DiagnosticCode::PqV1AtLeastOnce, severity: DiagnosticSeverity::Info, config_paths: vec!["sink.logbroker".into()], explanation: "Logbroker write acknowledgement precedes source progress commit, so an ambiguous retry may produce a duplicate unless the destination deduplicates the configured producer sequence".into(), remediation: None }] };
    }
    if matches!(sink, EndpointDescriptor::KafkaSink(_)) {
        return DeliverySemanticsReport {
            guarantee: DeliveryGuarantee::AtLeastOnce,
            diagnostics: vec![SemanticsDiagnostic {
                code: DiagnosticCode::KafkaAtLeastOnce,
                severity: DiagnosticSeverity::Info,
                config_paths: vec!["sink.kafka".into()],
                explanation: "Kafka acknowledgement precedes source progress commit, so an ambiguous retry may produce a duplicate record".into(),
                remediation: Some("enable downstream idempotency or deduplication when duplicate-free final state is required".into()),
            }],
        };
    }
    let EndpointDescriptor::S3(sink) = sink else {
        return DeliverySemanticsReport {
            guarantee: DeliveryGuarantee::AtLeastOnce,
            diagnostics: vec![error(
                DiagnosticCode::UnsupportedPipeline,
                &["source", "sink"],
                "the configured source/sink pair is not supported",
                None,
            )],
        };
    };

    let mut diagnostics = Vec::new();
    let mains = discovery
        .datasets
        .iter()
        .filter(|dataset| dataset.role == DatasetRole::Main)
        .collect::<Vec<_>>();
    if mains.is_empty() {
        diagnostics.push(error(
            DiagnosticCode::InvalidDeliveryDiscovery,
            &["source.logbroker.parser"],
            "delivery discovery contains no main dataset",
            Some("configure a row-producing parser with one main and one DLQ dataset"),
        ));
    }
    for kind in [
        SystemColumnKind::Topic,
        SystemColumnKind::Partition,
        SystemColumnKind::Offset,
        SystemColumnKind::MessageIndex,
    ] {
        for main in &mains {
            require_system_column(Some(main), kind, &mut diagnostics);
        }
    }
    if keep_system_columns
        && mains
            .iter()
            .any(|dataset| dataset.system_columns.is_empty())
    {
        diagnostics.push(error(
            DiagnosticCode::SystemColumnsNotProduced,
            &["source.logbroker.parser.common.system_columns"],
            "system columns cannot be retained because the parser produces none",
            Some("enable at least one parser system column"),
        ));
    }

    match &sink.partitioning {
        S3Partitioning::Fields(fields) => {
            for field in fields {
                for main in &mains {
                    match main
                        .incoming_schema
                        .columns
                        .iter()
                        .find(|column| column.name == *field)
                {
                    None => diagnostics.push(error(
                        DiagnosticCode::UnknownPartitionField,
                        &[
                            "sink.s3.partitioning.columns",
                            "source.logbroker.parser.json_parser.columns",
                        ],
                        &format!("partition field '{field}' is absent from the parser schema"),
                        Some("add a non-null scalar parser column with this name"),
                    )),
                    Some(column) if column.nullable => diagnostics.push(error(
                        DiagnosticCode::NullablePartitionField,
                        &[
                            "sink.s3.partitioning.columns",
                            "source.logbroker.parser.json_parser.columns",
                        ],
                        &format!("partition field '{field}' is nullable"),
                        Some("make the parser column non-nullable; invalid records will go to DLQ"),
                    )),
                    Some(column) if !supported_partition_type(&column.data_type) => diagnostics
                        .push(error(
                            DiagnosticCode::UnsupportedPartitionFieldType,
                            &[
                                "sink.s3.partitioning.columns",
                                "source.logbroker.parser.json_parser.columns",
                            ],
                            &format!(
                                "partition field '{field}' has unsupported type {:?}",
                                column.data_type
                            ),
                            Some("use string, integer, boolean, date, or timestamp"),
                        )),
                    Some(_) => {}
                }
                }
            }
        }
        S3Partitioning::RecordTime => {
            for main in &mains {
                require_system_column(
                    Some(main),
                    SystemColumnKind::WriteTimestampMs,
                    &mut diagnostics,
                );
            }
        }
        S3Partitioning::Source => {}
    }
    if sink.record_time_rotation {
        for main in &mains {
            require_system_column(
                Some(main),
                SystemColumnKind::WriteTimestampMs,
                &mut diagnostics,
            );
        }
    }

    let guarantee = if !matches!(source, EndpointDescriptor::Logbroker(_)) {
        diagnostics.push(SemanticsDiagnostic {
            code: DiagnosticCode::DeterministicS3Commit,
            severity: DiagnosticSeverity::Info,
            config_paths: vec!["source".into(), "sink.s3".into()],
            explanation: "S3 objects are committed before source progress, but a finite source snapshot can change between retries".into(),
            remediation: Some("run against an immutable source snapshot when stable final contents are required".into()),
        });
        DeliveryGuarantee::AtLeastOnce
    } else if sink.wall_clock_rotation {
        diagnostics.push(SemanticsDiagnostic {
            code: DiagnosticCode::WallClockRotationDisablesExactlyOnce,
            severity: DiagnosticSeverity::Info,
            config_paths: vec!["sink.s3.rotation.wall_clock_interval".into()],
            explanation: "wall-clock object boundaries differ after a restart, so deterministic overwrite cannot prove exactly-once".into(),
            remediation: Some("remove wall_clock_interval and use deterministic row/byte/record-time rotation".into()),
        });
        DeliveryGuarantee::AtLeastOnce
    } else {
        diagnostics.push(SemanticsDiagnostic {
            code: DiagnosticCode::DeterministicS3Commit,
            severity: DiagnosticSeverity::Info,
            config_paths: vec![
                stream_source_path(source, "parser"),
                stream_source_path(source, "topics"),
                stream_source_path(source, "consumer_name"),
                "middlewares".into(),
                "sink.s3.bucket".into(),
                "sink.s3.host".into(),
                "sink.s3.port".into(),
                "sink.s3.region".into(),
                "sink.s3.prefix".into(),
                "sink.s3.object_layout_version".into(),
                "sink.s3.partitioning".into(),
                "sink.s3.rotation".into(),
                "sink.s3.buffering.max_epoch_buffers".into(),
                "sink.s3.buffering.max_epoch_bytes".into(),
            ],
            explanation: "object boundaries and keys are deterministic for fixed transformation and destination configuration; successful overwrite precedes source commit".into(),
            remediation: Some("do not change source identity, parser, middleware, projection, destination identity, S3 prefix, object_layout_version, partitioning, rotation, max_epoch_buffers, or max_epoch_bytes while uncommitted data can replay; keep one logical source cluster per normalized bucket/prefix namespace".into()),
        });
        DeliveryGuarantee::ExactlyOnce
    };
    DeliverySemanticsReport {
        guarantee,
        diagnostics,
    }
}

fn stream_source_path(source: &EndpointDescriptor, field: &str) -> String {
    debug_assert!(matches!(source, EndpointDescriptor::Logbroker(_)));
    format!("source.logbroker.{field}")
}

fn require_system_column(
    dataset: Option<&DiscoveredDataset>,
    kind: SystemColumnKind,
    diagnostics: &mut Vec<SemanticsDiagnostic>,
) {
    if dataset.is_none_or(|dataset| {
        !dataset
            .system_columns
            .iter()
            .any(|column| column.kind == kind)
    }) {
        diagnostics.push(error(
            DiagnosticCode::MissingSystemColumn,
            &["source.logbroker.parser.common.system_columns", "sink.s3"],
            &format!(
                "S3 sink requires parser system column '{}'",
                kind.default_name()
            ),
            Some(&format!(
                "enable {} in parser.common.system_columns",
                config_name(kind)
            )),
        ));
    }
}

const fn config_name(kind: SystemColumnKind) -> &'static str {
    match kind {
        SystemColumnKind::Topic => "topic",
        SystemColumnKind::Partition => "partition",
        SystemColumnKind::Offset => "offset",
        SystemColumnKind::MessageIndex => "message_index",
        SystemColumnKind::WriteTimestampMs => "write_timestamp_ms",
        SystemColumnKind::ChangeOperation => "change_operation",
        SystemColumnKind::ChangedColumns => "changed_columns",
    }
}

const fn supported_partition_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    )
}

fn error(
    code: DiagnosticCode,
    paths: &[&str],
    explanation: &str,
    remediation: Option<&str>,
) -> SemanticsDiagnostic {
    SemanticsDiagnostic {
        code,
        severity: DiagnosticSeverity::Error,
        config_paths: paths.iter().map(ToString::to_string).collect(),
        explanation: explanation.into(),
        remediation: remediation.map(Into::into),
    }
}

#[cfg(test)]
#[path = "tests/semantics.rs"]
mod tests;
