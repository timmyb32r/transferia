//! Connector-neutral runtime component registry.

mod definition;
pub mod durable;
mod registry;
mod traits;
mod ui_contract;

pub use definition::{ConnectorDefinition, DeliveryMode, EndpointDefinition, MiddlewareDefinition};
pub use registry::Composition;
pub use registry::{
    ComponentRegistration, MiddlewarePreview, MiddlewarePreviewColumn, MiddlewareRegistration,
    Registry, RegistryBuilder, SourcePreview, SourcePreviewMetadata, SourcePreviewMetadataItem,
};
pub use traits::{
    ConnectionCheckResult, ConnectionCheckStatus, DatasetPrepare, DynamicOption, DynamicOptions,
    EndpointRole, OptionsRequest, SinkBuildContext, SinkConnector, SinkPrepare, SourceBuildContext,
    SourceConnector, SourceDiscoveryContext,
};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
