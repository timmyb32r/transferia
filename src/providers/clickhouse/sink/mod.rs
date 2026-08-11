mod actor;
mod transport;

#[cfg(test)]
mod tests;

pub use actor::ClickHouseSink;
pub use transport::{InsertError, InsertTransport};

// Keep the sink's configuration discoverable next to its public type.
pub use super::ClickHouseSinkConfig;
