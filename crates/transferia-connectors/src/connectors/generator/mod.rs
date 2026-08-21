mod connector;
mod source;

pub use connector::{DataGeneratorConfig, DataGeneratorSourceConnector};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
