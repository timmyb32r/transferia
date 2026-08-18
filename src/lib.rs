//! Compatibility-free public composition surface for embedders.
//!
//! Implementation lives in focused workspace crates. This crate intentionally
//! contains no business logic: it is the stable dependency selected by external
//! compile-time compositions.

pub use transferia_core as core;
pub use transferia_delivery as delivery;
pub use transferia_providers::{
    durable, extension, metrics, parsers, providers, schema_registry, serializer,
};
pub use transferia_runtime as runtime_api;

pub mod runtime {
    pub use transferia_runtime::*;

    pub mod local {
        pub use transferia_composition::run;
        pub use transferia_runtime_local::LocalWorkerSupervisor;
    }
}

pub mod server {
    pub use transferia_control_plane::server::*;
}

pub mod middleware {
    pub use transferia_delivery::middleware::*;
}
