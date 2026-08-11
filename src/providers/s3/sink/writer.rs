//! Compatibility module retained so old internal paths fail at compile time in
//! one obvious place instead of carrying a second sink implementation.

pub use super::actor::S3Sink;
pub use super::config::S3SinkConfig;
