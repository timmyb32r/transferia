//! Cross-provider rules. This module contains no provider implementation or
//! provider-specific config types: only neutral capability descriptors and the
//! rules that relate a source pipeline to a sink.

use arrow::datatypes::DataType;
use serde::Serialize;

use crate::delivery::{DatasetRole, DeliveryDiscovery, DiscoveredDataset};
use crate::types::system_columns::SystemColumnKind;

#[derive(Debug, Clone)]
pub enum EndpointDescriptor {
    PqV1(SourceDescriptor),
    ClickHouse,
    S3(S3Descriptor),
    /// Benchmark-only sink which durably stores nothing.
    Discard,
}

#[derive(Debug, Clone)]
pub struct SourceDescriptor {
    pub behavior: SourceBehavior,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceBehavior {
    ProducesRows,
    /// Benchmark-only mode which advances source offsets without producing rows.
    BenchmarkDiscard,
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
    InvalidDeliveryDiscovery,
    MissingSystemColumn,
    SystemColumnsNotProduced,
    UnknownPartitionField,
    NullablePartitionField,
    UnsupportedPartitionFieldType,
    WallClockRotationDisablesExactlyOnce,
    DeterministicS3Commit,
    ClickHouseAtLeastOnce,
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
        return DeliverySemanticsReport {
            guarantee: DeliveryGuarantee::NoDurability,
            diagnostics: vec![SemanticsDiagnostic {
                code: DiagnosticCode::BenchmarkDiscard,
                severity: DiagnosticSeverity::Info,
                config_paths: vec!["sink.discard".into()],
                explanation: "the discard sink acknowledges and drops every delivery; it is intended only for throughput benchmarks".into(),
                remediation: Some("use clickhouse or s3 for a durable transfer".into()),
            }],
        };
    }
    if matches!(
        source,
        EndpointDescriptor::PqV1(SourceDescriptor {
            behavior: SourceBehavior::BenchmarkDiscard,
            ..
        })
    ) {
        return DeliverySemanticsReport {
            guarantee: DeliveryGuarantee::NoDurability,
            diagnostics: vec![error(
                DiagnosticCode::BenchmarkSourceDiscard,
                &[
                    "source.pqv1.benchmark_discard_before_decompression",
                    "source.pqv1.parser.benchmark_discard",
                    "sink",
                ],
                "the PQv1 source is configured to discard payloads, so a durable sink would acknowledge and commit data it never stored",
                Some("disable benchmark_discard_before_decompression and configure a row-producing parser, or use the benchmark-only discard sink"),
            )],
        };
    }
    if matches!(
        (source, sink),
        (EndpointDescriptor::PqV1(_), EndpointDescriptor::ClickHouse)
    ) {
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
    let (EndpointDescriptor::PqV1(_), EndpointDescriptor::S3(sink)) = (source, sink) else {
        return DeliverySemanticsReport {
            guarantee: DeliveryGuarantee::AtLeastOnce,
            diagnostics: vec![error(
                DiagnosticCode::UnsupportedPipeline,
                &["source", "sink"],
                "supported durable paths are PQv1 to ClickHouse or S3",
                None,
            )],
        };
    };

    let mut diagnostics = Vec::new();
    let main = match discovery.dataset(DatasetRole::Main) {
        Ok(dataset) => Some(dataset),
        Err(discovery_error) => {
            diagnostics.push(error(
                DiagnosticCode::InvalidDeliveryDiscovery,
                &["source.pqv1.parser"],
                &discovery_error.to_string(),
                Some("configure a row-producing parser with one main and one DLQ dataset"),
            ));
            None
        }
    };
    for kind in [
        SystemColumnKind::Topic,
        SystemColumnKind::Partition,
        SystemColumnKind::Offset,
        SystemColumnKind::MessageIndex,
    ] {
        require_system_column(main, kind, &mut diagnostics);
    }
    if keep_system_columns && main.is_some_and(|dataset| dataset.system_columns.is_empty()) {
        diagnostics.push(error(
            DiagnosticCode::SystemColumnsNotProduced,
            &[
                "keep_system_columns_in_sink",
                "source.pqv1.parser.common.system_columns",
            ],
            "system columns cannot be retained because the parser produces none",
            Some("enable at least one parser system column"),
        ));
    }

    match &sink.partitioning {
        S3Partitioning::Fields(fields) => {
            for field in fields {
                match main.and_then(|dataset| {
                    dataset
                        .incoming_schema
                        .columns
                        .iter()
                        .find(|column| column.name == *field)
                }) {
                    None => diagnostics.push(error(
                        DiagnosticCode::UnknownPartitionField,
                        &[
                            "sink.s3.partitioning.columns",
                            "source.pqv1.parser.json_parser.columns",
                        ],
                        &format!("partition field '{field}' is absent from the parser schema"),
                        Some("add a non-null scalar parser column with this name"),
                    )),
                    Some(column) if column.nullable => diagnostics.push(error(
                        DiagnosticCode::NullablePartitionField,
                        &[
                            "sink.s3.partitioning.columns",
                            "source.pqv1.parser.json_parser.columns",
                        ],
                        &format!("partition field '{field}' is nullable"),
                        Some("make the parser column non-nullable; invalid records will go to DLQ"),
                    )),
                    Some(column) if !supported_partition_type(&column.data_type) => diagnostics
                        .push(error(
                            DiagnosticCode::UnsupportedPartitionFieldType,
                            &[
                                "sink.s3.partitioning.columns",
                                "source.pqv1.parser.json_parser.columns",
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
        S3Partitioning::RecordTime => {
            require_system_column(main, SystemColumnKind::WriteTimestampMs, &mut diagnostics);
        }
        S3Partitioning::Source => {}
    }
    if sink.record_time_rotation {
        require_system_column(main, SystemColumnKind::WriteTimestampMs, &mut diagnostics);
    }

    let guarantee = if sink.wall_clock_rotation {
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
                "source.pqv1.parser".into(),
                "source.pqv1.topic_path".into(),
                "source.pqv1.consumer_name".into(),
                "middlewares".into(),
                "keep_system_columns_in_sink".into(),
                "sink.s3.bucket".into(),
                "sink.s3.endpoint".into(),
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

fn require_system_column(
    dataset: Option<&DiscoveredDataset>,
    kind: SystemColumnKind,
    diagnostics: &mut Vec<SemanticsDiagnostic>,
) {
    if dataset.is_none_or(|dataset| !dataset.system_columns.contains(&kind)) {
        diagnostics.push(error(
            DiagnosticCode::MissingSystemColumn,
            &["source.pqv1.parser.common.system_columns", "sink.s3"],
            &format!("S3 sink requires parser system column '{}'", kind.name()),
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
mod tests {
    use super::*;
    use crate::delivery::{DiscoveredDataset, SchemaOrigin};
    use crate::types::schema::{DatasetSchema, SchemaColumn};

    fn source() -> EndpointDescriptor {
        EndpointDescriptor::PqV1(SourceDescriptor {
            behavior: SourceBehavior::ProducesRows,
        })
    }

    fn discovery() -> DeliveryDiscovery {
        DeliveryDiscovery {
            source_name: "topic".into(),
            source_partitions: vec![0],
            schema_origin: SchemaOrigin::ParserProjection,
            keep_system_columns: false,
            datasets: vec![DiscoveredDataset {
                role: DatasetRole::Main,
                name: "events".into(),
                incoming_schema: DatasetSchema::new(vec![SchemaColumn::new(
                    "tenant".into(),
                    DataType::Utf8,
                    false,
                )]),
                stored_schema: DatasetSchema::new(vec![SchemaColumn::new(
                    "tenant".into(),
                    DataType::Utf8,
                    false,
                )]),
                system_columns: vec![
                    SystemColumnKind::Topic,
                    SystemColumnKind::Partition,
                    SystemColumnKind::Offset,
                    SystemColumnKind::MessageIndex,
                    SystemColumnKind::WriteTimestampMs,
                ],
            }],
        }
    }

    fn sink(partitioning: S3Partitioning, wall_clock_rotation: bool) -> EndpointDescriptor {
        EndpointDescriptor::S3(S3Descriptor {
            partitioning,
            record_time_rotation: false,
            wall_clock_rotation,
            object_layout_version: 5,
        })
    }

    #[test]
    fn infers_exactly_once_from_deterministic_settings() {
        let report = validate_pipeline(
            &source(),
            &sink(S3Partitioning::Source, false),
            &discovery(),
            false,
        );
        assert_eq!(report.guarantee, DeliveryGuarantee::ExactlyOnce);
        assert!(report.ensure_valid().is_ok());
        let diagnostic = report
            .diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == DiagnosticCode::DeterministicS3Commit)
            .expect("deterministic S3 diagnostic");
        assert!(diagnostic
            .config_paths
            .iter()
            .any(|path| path == "sink.s3.object_layout_version"));
    }

    #[test]
    fn wall_clock_rotation_is_at_least_once() {
        let report = validate_pipeline(
            &source(),
            &sink(S3Partitioning::Source, true),
            &discovery(),
            false,
        );
        assert_eq!(report.guarantee, DeliveryGuarantee::AtLeastOnce);
        assert!(report.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == DiagnosticCode::WallClockRotationDisablesExactlyOnce
        }));
    }

    #[test]
    fn clickhouse_pipeline_is_supported_as_at_least_once() {
        let report = validate_pipeline(
            &source(),
            &EndpointDescriptor::ClickHouse,
            &discovery(),
            false,
        );
        assert_eq!(report.guarantee, DeliveryGuarantee::AtLeastOnce);
        assert!(report.ensure_valid().is_ok());
        assert!(report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiagnosticCode::ClickHouseAtLeastOnce));
    }

    #[test]
    fn discard_sink_is_explicitly_supported_for_benchmarks() {
        let report =
            validate_pipeline(&source(), &EndpointDescriptor::Discard, &discovery(), false);
        assert_eq!(report.guarantee, DeliveryGuarantee::NoDurability);
        assert!(report.ensure_valid().is_ok());
        assert!(report
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == DiagnosticCode::BenchmarkDiscard));
    }

    #[test]
    fn durable_sinks_reject_a_discarding_source() {
        let mut source_endpoint = source();
        let EndpointDescriptor::PqV1(source) = &mut source_endpoint else {
            unreachable!()
        };
        source.behavior = SourceBehavior::BenchmarkDiscard;

        for sink_endpoint in [
            EndpointDescriptor::ClickHouse,
            sink(S3Partitioning::Source, false),
        ] {
            let report = validate_pipeline(&source_endpoint, &sink_endpoint, &discovery(), false);
            assert_eq!(report.guarantee, DeliveryGuarantee::NoDurability);
            assert!(report.ensure_valid().is_err());
            assert!(report.diagnostics.iter().any(|diagnostic| {
                diagnostic.code == DiagnosticCode::BenchmarkSourceDiscard
                    && diagnostic.severity == DiagnosticSeverity::Error
            }));
        }

        let benchmark = validate_pipeline(
            &source_endpoint,
            &EndpointDescriptor::Discard,
            &discovery(),
            false,
        );
        assert!(benchmark.ensure_valid().is_ok());
    }

    #[test]
    fn record_time_partitioning_requires_source_timestamp_column() {
        let source_endpoint = source();
        let mut discovery = discovery();
        discovery.datasets[0]
            .system_columns
            .retain(|kind| *kind != SystemColumnKind::WriteTimestampMs);
        let report = validate_pipeline(
            &source_endpoint,
            &sink(S3Partitioning::RecordTime, false),
            &discovery,
            false,
        );
        assert!(report.ensure_valid().is_err());
    }
}
