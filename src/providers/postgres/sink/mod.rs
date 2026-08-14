mod config;
mod copy_binary;
mod provider;
mod writer;

pub use config::PostgresSinkConfig;
pub use provider::PostgresSinkProvider;

#[cfg(test)]
mod tests;
