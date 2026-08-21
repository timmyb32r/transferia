mod config;
mod copy_binary;
mod connector;
mod writer;

pub use config::PostgresSinkConfig;
pub use connector::PostgresSinkConnector;

#[cfg(test)]
mod tests;
