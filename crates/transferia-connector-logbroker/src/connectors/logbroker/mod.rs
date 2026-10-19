mod config;
pub mod pqv1;
pub mod proto;
pub mod sink;
pub mod src_stream;
mod transport;

pub use config::{LogbrokerAuthConfig, LogbrokerDriver};
pub use sink::build_sink_connector;
pub use src_stream::check_connection;
pub use src_stream::check_network_connection;
pub use src_stream::preview_message;
pub use src_stream::{
    build_source_connector, build_source_connector_with_parsers, YdbDriverSourceConnector,
};

/// Both write protocols use the table name verbatim as their producer identity.
/// Validate before opening a session; never shorten or replace an identifier.
fn validate_producer_id(table: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !table.is_empty() && table.len() <= 2048,
        "Logbroker producer identity derived from table name must contain 1..=2048 UTF-8 bytes"
    );
    Ok(())
}
