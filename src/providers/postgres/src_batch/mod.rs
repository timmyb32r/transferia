mod config;
mod provider;
mod reader;

pub use provider::PostgresSourceProvider;

#[cfg(test)]
mod tests;
