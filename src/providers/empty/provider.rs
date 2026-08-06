use std::sync::Arc;

use futures_util::future::BoxFuture;
use serde_yaml::Value;

use crate::pipeline::sink::Sink;
use crate::providers::empty::sink::EmptySink;
use crate::providers::traits::SinkProvider;

pub struct EmptySinkProvider {
    sink: Arc<EmptySink>,
}

impl EmptySinkProvider {
    pub fn from_config(_value: Value) -> anyhow::Result<Self> {
        Ok(Self { sink: Arc::new(EmptySink::new()) })
    }
}

impl SinkProvider for EmptySinkProvider {
    fn build_sink<'a>(&'a self) -> BoxFuture<'a, anyhow::Result<Arc<dyn Sink>>> {
        let sink = self.sink.clone();
        Box::pin(async move { Ok(sink as Arc<dyn Sink>) })
    }
}
