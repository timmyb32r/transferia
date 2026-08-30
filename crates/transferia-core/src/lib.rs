extern crate alloc;

pub mod data;
pub mod delivery;
pub mod failure;
pub mod memory;
pub mod sink;
pub mod source;

pub use data::message::{Message, MessageMeta, SourceBatch};
pub use data::record_batch::{compact_record_batch, compact_record_batch_chunks};
pub use data::schema::{DatasetSchema, SchemaColumn};
pub use data::system_columns::{SystemColumn, SystemColumnKind, SystemColumns};
pub use data::table_data::TableData;
pub use delivery::{
    DatasetRole, DeliveryDiscovery, DeliveryDiscoveryRequest, DiscoveredDataset,
    DiscoveredSystemColumn, PerformanceAdvice, PerformanceAdviceSeverity, SchemaOrigin, SinkLimits,
    SinkLimitsDescription, SourceTopology,
};
pub use failure::{DataPlaneFailure, DataPlaneResult, FailureDisposition};
pub use memory::{MemoryReservation, PipelineMemory};
pub use sink::{Sink, SinkBatch, SinkIo};
pub use source::{CommitMarker, CommitMarkerTypeMismatch, Source};
