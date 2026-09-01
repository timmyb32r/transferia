use super::*;
use transferia_core::data::schema::{DatasetSchema, SchemaColumn};
use transferia_core::delivery::{DiscoveredDataset, SchemaOrigin};

fn source() -> EndpointDescriptor {
    EndpointDescriptor::Logbroker(SourceDescriptor {
        behavior: SourceBehavior::ProducesRows,
        delivery_modes: SourceDeliveryModes::STREAM,
    })
}

#[test]
fn source_delivery_modes_are_explicit() {
    assert!(source().supports_delivery_type(crate::DeliveryType::Stream));
    assert!(!source().supports_delivery_type(crate::DeliveryType::Batch));
    assert!(!source().supports_delivery_type(crate::DeliveryType::BatchAndStream));
}

fn discovery() -> DeliveryDiscovery {
    DeliveryDiscovery {
        source_name: "topic".into(),
        source_topology: transferia_core::delivery::SourceTopology::StaticPartitions(vec![0]),
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
        performance_advice: Vec::new(),
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
fn ydb_pipeline_reports_primary_key_replay_semantics() {
    let report = validate_pipeline(&source(), &EndpointDescriptor::YdbSink, &discovery(), false);
    assert_eq!(report.guarantee, DeliveryGuarantee::AtLeastOnce);
    assert!(report.ensure_valid().is_ok());
    assert!(report
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == DiagnosticCode::YdbAtLeastOnce));
}

#[test]
fn discard_sink_accepts_a_row_producing_parser() {
    let report = validate_pipeline(&source(), &EndpointDescriptor::Discard, &discovery(), false);
    assert_eq!(report.guarantee, DeliveryGuarantee::NoDurability);
    assert!(report.ensure_valid().is_ok());
    assert!(report.diagnostics.iter().any(|diagnostic| {
        diagnostic.code == DiagnosticCode::BenchmarkDiscard
            && diagnostic.severity == DiagnosticSeverity::Info
    }));
}

#[test]
fn durable_sinks_reject_a_discarding_source() {
    let mut logbroker = source();
    let EndpointDescriptor::Logbroker(source) = &mut logbroker else {
        panic!("test source must be YDB Topic")
    };
    source.behavior = SourceBehavior::BenchmarkDiscard;
    let ytsaurus = EndpointDescriptor::YTsaurus(SourceDescriptor {
        behavior: SourceBehavior::BenchmarkDiscard,
        delivery_modes: SourceDeliveryModes::BATCH,
    });

    for source_endpoint in [&logbroker, &ytsaurus] {
        for sink_endpoint in [
            EndpointDescriptor::ClickHouse,
            sink(S3Partitioning::Source, false),
        ] {
            let report = validate_pipeline(source_endpoint, &sink_endpoint, &discovery(), false);
            assert_eq!(report.guarantee, DeliveryGuarantee::NoDurability);
            assert!(report.ensure_valid().is_err());
            assert!(report.diagnostics.iter().any(|diagnostic| {
                diagnostic.code == DiagnosticCode::BenchmarkSourceDiscard
                    && diagnostic.severity == DiagnosticSeverity::Error
            }));
        }
    }

    let benchmark = validate_pipeline(&ytsaurus, &EndpointDescriptor::Discard, &discovery(), false);
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
