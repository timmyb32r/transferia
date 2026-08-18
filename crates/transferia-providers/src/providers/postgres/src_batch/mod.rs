mod config;
mod provider;
mod reader;

pub use config::PostgresSourceConfig;
pub use provider::PostgresSourceProvider;

#[cfg(test)]
mod tests;
