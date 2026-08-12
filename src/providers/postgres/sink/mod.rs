mod config;
mod copy_binary;
mod provider;
mod runtime;

pub use provider::PostgresSinkProvider;

#[cfg(test)]
mod tests;
