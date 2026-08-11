pub mod filter;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};

use serde::Deserialize;
use serde_yaml::Value;

use crate::pipeline::middleware::Middleware;

#[derive(Debug, Clone, Deserialize)]
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

type MiddlewareFactory = Arc<dyn Fn(Value) -> anyhow::Result<Box<dyn Middleware>> + Send + Sync>;

static MIDDLEWARE_REGISTRY: LazyLock<Mutex<HashMap<&'static str, MiddlewareFactory>>> =
    LazyLock::new(|| {
        let mut registry: HashMap<&'static str, MiddlewareFactory> = HashMap::new();
        registry.insert(
            "filter",
            Arc::new(|raw| {
                let config: filter::FilterConfig = serde_yaml::from_value(raw)?;
                Ok(
                    Box::new(filter::FilterMiddleware::new(config.field, config.value)?)
                        as Box<dyn Middleware>,
                )
            }),
        );
        Mutex::new(registry)
    });

pub fn register_middleware(name: &'static str, factory: MiddlewareFactory) {
    MIDDLEWARE_REGISTRY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(name, factory);
}

pub fn middleware_names() -> HashSet<&'static str> {
    MIDDLEWARE_REGISTRY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .keys()
        .copied()
        .collect()
}

pub fn build_middleware(name: &str, raw: Value) -> anyhow::Result<Box<dyn Middleware>> {
    let factory = {
        let registry = MIDDLEWARE_REGISTRY
            .lock()
            .map_err(|error| anyhow::anyhow!("middleware registry is poisoned: {error}"))?;
        registry.get(name).cloned().ok_or_else(|| {
            anyhow::anyhow!(
                "Unknown middleware '{}'; registered: {:?}",
                name,
                registry.keys().collect::<Vec<_>>(),
            )
        })?
    };
    factory(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_middleware_from_opaque_entry() -> anyhow::Result<()> {
        let entry: MiddlewareEntry =
            serde_yaml::from_str("filter:\n  field: event_name\n  value: page_view\n")?;
        anyhow::ensure!(entry.kind()? == "filter");
        drop(build_middleware(entry.kind()?, entry.raw()?.clone())?);
        Ok(())
    }

    #[test]
    fn rejects_unknown_middleware() -> anyhow::Result<()> {
        let entry: MiddlewareEntry = serde_yaml::from_str("unknown: {}\n")?;
        anyhow::ensure!(build_middleware(entry.kind()?, entry.raw()?.clone()).is_err());
        Ok(())
    }
}
