mod config;
mod provider;
mod runtime;

pub(crate) use provider::connect;
pub use provider::PostgresSourceProvider;

#[cfg(test)]
mod tests;
