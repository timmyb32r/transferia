//! Provider-neutral runtime component registry.

mod definition;
pub mod durable;
mod registry;
mod traits;

pub use definition::{DeliveryMode, EndpointDefinition, MiddlewareDefinition, ProviderDefinition};
pub use registry::Composition;
pub use registry::{
    ComponentRegistration, MiddlewarePreview, MiddlewarePreviewColumn, MiddlewareRegistration,
    Registry, RegistryBuilder,
};
pub use traits::{
    ConnectionCheckResult, ConnectionCheckStatus, DatasetPrepare, DynamicOption, DynamicOptions,
    EndpointRole, OptionsRequest, SinkBuildContext, SinkPrepare, SinkProvider, SourceBuildContext,
    SourceDiscoveryContext, SourceProvider,
};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
