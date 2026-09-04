mod client;
mod connection;
pub mod sink;
pub mod src_batch;

pub use connection::{
    OpenSearchAuth, OpenSearchConnectionCheckConfig, OpenSearchConnectionConfig,
};

pub(crate) use client::{OpenSearchClient, OpenSearchHttpError, OpenSearchResponse};
pub(crate) use connection::validate_index_name;

#[cfg(test)]
mod tests;
