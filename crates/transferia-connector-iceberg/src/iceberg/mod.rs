mod catalog;
mod config;
mod sink;
mod source;
mod storage;

pub use config::{IcebergSinkConfig, IcebergSourceConfig};
pub use sink::check_connection as check_sink_connection;
pub use sink::IcebergSinkConnector;
pub use source::check_connection as check_source_connection;
pub use source::IcebergSourceConnector;

#[cfg(test)]
mod tests;
