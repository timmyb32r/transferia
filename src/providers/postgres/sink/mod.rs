mod config;
mod copy_binary;
mod provider;
mod writer;

pub use provider::PostgresSinkProvider;

#[cfg(test)]
mod tests;
