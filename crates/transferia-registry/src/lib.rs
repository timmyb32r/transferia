//! Provider-neutral runtime component registry.

mod definition;
pub mod durable;
mod registry;
mod traits;

pub use definition::{DeliveryMode, EndpointDefinition, ProviderDefinition};
pub use registry::Composition;
pub use registry::{ComponentRegistration, Registry, RegistryBuilder};
pub use traits::{
    ConnectionCheckResult, ConnectionCheckStatus, DatasetPrepare, EndpointRole, SinkBuildContext,
    SinkPrepare, SinkProvider, SourceBuildContext, SourceDiscoveryContext, SourceProvider,
};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
