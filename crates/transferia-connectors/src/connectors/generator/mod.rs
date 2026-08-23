mod connector;
mod preset;
mod source;

pub use connector::{DataGeneratorConfig, DataGeneratorSourceConnector, GenerationAmount};
pub use preset::DataGeneratorPreset;

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
