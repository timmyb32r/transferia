pub mod datafusion;
pub mod filter;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_yaml::Value;

use transferia_delivery_contracts::middleware::Middleware;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MiddlewareEntry {
    #[serde(flatten)]
    inner: HashMap<String, Value>,
}

impl MiddlewareEntry {
    pub fn kind(&self) -> anyhow::Result<&str> {
        let keys: Vec<&str> = self.inner.keys().map(String::as_str).collect();
        match *keys.as_slice() {
            [single] => Ok(single),
            [] => anyhow::bail!("middleware: no middleware key found"),
            _ => anyhow::bail!("middleware: expected exactly one middleware key, got {keys:?}"),
        }
    }

    pub fn raw(&self) -> anyhow::Result<&Value> {
        let kind = self.kind()?;
        self.inner
            .get(kind)
            .ok_or_else(|| anyhow::anyhow!("middleware key '{kind}' is missing from config"))
    }
}

pub fn build_middleware(name: &str, raw: Value) -> anyhow::Result<Box<dyn Middleware>> {
    match name {
        "datafusion" => {
            #[cfg(feature = "datafusion")]
            {
                let config: datafusion::DataFusionConfig = serde_yaml::from_value(raw)?;
                return Ok(Box::new(datafusion::DataFusionMiddleware::new(config.sql)?));
            }
            #[cfg(not(feature = "datafusion"))]
            anyhow::bail!("DataFusion middleware is not available in this build")
        }
        "filter" => {
            let config: filter::FilterConfig = serde_yaml::from_value(raw)?;
            Ok(Box::new(filter::FilterMiddleware::new(
                config.field,
                config.value,
            )?))
        }
        other => {
            anyhow::bail!("unknown middleware '{other}'; supported middleware: datafusion, filter")
        }
    }
}

#[cfg(test)]
mod tests;
