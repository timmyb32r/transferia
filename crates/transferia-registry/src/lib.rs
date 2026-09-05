//! Connector-neutral runtime component registry.

mod definition;
pub mod durable;
mod registry;
mod traits;
pub mod tuning;
pub mod table_selection;
mod ui_contract;

pub use definition::{ConnectorDefinition, DeliveryMode, EndpointDefinition, MiddlewareDefinition};
pub use registry::Composition;
pub use registry::{
    ComponentRegistration, MiddlewarePreview, MiddlewarePreviewColumn, MiddlewareRegistration,
    Registry, RegistryBuilder, SourcePreview, SourcePreviewMetadata, SourcePreviewMetadataItem,
};
pub use traits::{
    validate_speedtest_build_context, validate_speedtest_discovery, validate_speedtest_prepare,
    ConnectionCheckResult, ConnectionCheckStatus, DatasetPrepare, DynamicOption, DynamicOptions, TableIdentity,
    EndpointRole, OptionsRequest, PreparedSourceExecution, SinkBuildContext, SinkConnector,
    SinkPrepare, SinkSpeedtestIsolation, SinkSpeedtestIsolationSafety, SnapshotDatasetRowCount,
    SnapshotRowCountStrategy, SourceBuildContext, SourceConnector, SourceDiscoveryContext,
    SourceExecutionContext, SourceExecutionPhase, SourcePhase, SpeedtestPhysicalTarget,
    SpeedtestUnsupported,
};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
