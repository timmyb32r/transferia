//! Connector-neutral runtime component registry.

mod definition;
pub mod durable;
mod registry;
pub mod table_selection;
mod traits;
pub mod tuning;
mod ui_contract;

pub use definition::{ConnectorDefinition, DeliveryMode, EndpointDefinition, MiddlewareDefinition};
pub use registry::Composition;
pub use registry::{
    ComponentRegistration, MiddlewareRegistration,
    Registry, RegistryBuilder, SourcePreview, SourcePreviewMetadata, SourcePreviewMetadataItem, TableSampleLimits,
};
pub use traits::{
    validate_speedtest_build_context, validate_speedtest_discovery, validate_speedtest_prepare,
    ConnectionCheckResult, ConnectionCheckStatus, DatasetPrepare, DynamicOption, DynamicOptions,
    EndpointRole, OptionsRequest, PreparedSourceExecution, SinkBuildContext, SinkConnector,
    SinkPrepare, SinkSpeedtestIsolation, SinkSpeedtestIsolationSafety, SnapshotDatasetRowCount,
    SnapshotRowCountStrategy, SourceBuildContext, SourceConnector, SourceDiscoveryContext,
    SourceExecutionContext, SourceExecutionPhase, SourcePhase, SpeedtestPhysicalTarget,
    SpeedtestUnsupported, TableIdentity,
};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
