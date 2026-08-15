mod config;
mod provider;
mod writer;

pub use config::YdbTopicSinkConfig;
pub use provider::build_sink_provider;

#[cfg(test)]
mod tests;
