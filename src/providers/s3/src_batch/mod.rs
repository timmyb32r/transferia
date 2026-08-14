mod config;
mod provider;
mod reader;

pub use provider::S3SourceProvider;

#[cfg(test)]
mod tests;
