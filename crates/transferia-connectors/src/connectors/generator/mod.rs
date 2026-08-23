mod connector;
mod preset;
mod source;

pub use connector::{DataGeneratorConfig, DataGeneratorSourceConnector};
pub use preset::DataGeneratorPreset;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
