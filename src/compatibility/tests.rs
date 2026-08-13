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
                SystemColumnKind::Topic.into(),
                SystemColumnKind::Partition.into(),
                SystemColumnKind::Offset.into(),
                SystemColumnKind::MessageIndex.into(),
                SystemColumnKind::WriteTimestampMs.into(),
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
    let report = validate_pipeline(&source(), &EndpointDescriptor::Discard, &discovery(), false);
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
