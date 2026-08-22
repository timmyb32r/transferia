mod config;
mod connector;
mod copy_binary;
mod writer;

pub use config::PostgresSinkConfig;
pub use connector::PostgresSinkConnector;

#[cfg(test)]
mod tests;
