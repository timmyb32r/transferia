mod actor;
mod config;
mod object_key;
mod partitioning;
pub mod provider;
mod upload;

pub use config::{
    BufferingConfig, ByteSize, DurationValue, PartitionPathChange, PartitioningConfig, RetryConfig,
    RotationConfig, S3CredentialsConfig, S3SinkConfig, UploadConfig,
};
pub use provider::S3SinkProvider;

#[cfg(test)]
mod tests;
