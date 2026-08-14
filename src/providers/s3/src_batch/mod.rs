mod config;
mod provider;
mod reader;

pub use config::S3SourceConfig;
pub use provider::S3SourceProvider;

#[cfg(test)]
mod tests;
