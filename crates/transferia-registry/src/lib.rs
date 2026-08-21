//! Connector-neutral runtime component registry.

mod definition;
pub mod durable;
mod registry;
mod traits;
mod ui_contract;

pub use definition::{DeliveryMode, EndpointDefinition, MiddlewareDefinition, ConnectorDefinition};
pub use registry::Composition;
pub use registry::{
    ComponentRegistration, MiddlewarePreview, MiddlewarePreviewColumn, MiddlewareRegistration,
    Registry, RegistryBuilder, SourcePreview, SourcePreviewMetadata, SourcePreviewMetadataItem,
};
pub use traits::{
    ConnectionCheckResult, ConnectionCheckStatus, DatasetPrepare, DynamicOption, DynamicOptions,
    EndpointRole, OptionsRequest, SinkBuildContext, SinkPrepare, SinkConnector, SourceBuildContext,
    SourceDiscoveryContext, SourceConnector,
};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
