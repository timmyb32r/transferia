pub mod json_parser;

use std::collections::{HashMap, HashSet};

use alloc::sync::Arc;
use serde::Deserialize;
use serde_yaml::Value;

use crate::types::exactly_once::ExactlyOnceKey;
use crate::types::message::Message;
use crate::types::table_data::TableData;

pub use crate::parsers::json_parser::ParserWorkspace;

/// Common parser interface. Every parser converts raw [`Message`]s into
/// Arrow [`TableData`] (valid + optional DLQ).
pub trait Parser: Send + Sync {
    fn parse_into(
        &self,
        messages: Vec<Message>,
        partition_id: i64,
        exactly_once_key: Option<ExactlyOnceKey>,
        ws: &mut ParserWorkspace,
    ) -> anyhow::Result<(TableData, Option<TableData>)>;
}

// ---------------------------------------------------------------------------
// ParserEntry — dynamic { kind: { config } } dispatch, like SourceEntry/SinkEntry
// ---------------------------------------------------------------------------

/// Parser config entry: `parser: { <kind>: { ... } }` — exactly one key.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct ParserEntry {
    #[serde(flatten)]
    pub inner: HashMap<String, Value>,
}

impl ParserEntry {
    pub fn kind(&self) -> anyhow::Result<&str> {
        let keys: Vec<&str> = self.inner.keys().map(String::as_str).collect();
        match *keys.as_slice() {
            [single] => Ok(single),
            [] => anyhow::bail!("parser: no parser key found (expected 'json_parser')"),
            _ => anyhow::bail!("parser: expected exactly one parser key, got {keys:?}"),
        }
    }

    pub fn raw(&self) -> anyhow::Result<&Value> {
        let kind = self.kind()?;
        self.inner
            .get(kind)
            .ok_or_else(|| anyhow::anyhow!("parser: parser key '{kind}' missing from config"))
    }
}

// ---------------------------------------------------------------------------
// ParserRegistry — per-process, seeded with json_parser on first access
// ---------------------------------------------------------------------------

/// `Arc` (not `Box`) so a factory can be cloned out of the registry and invoked
/// without holding the registry lock.
type ParserFactory = Arc<
    dyn Fn(Value, Arc<str>, Option<ExactlyOnceKey>) -> anyhow::Result<Arc<dyn Parser>> + Send + Sync,
>;

use std::sync::{LazyLock, Mutex};

static PARSER_REGISTRY: LazyLock<Mutex<HashMap<&'static str, ParserFactory>>> =
    LazyLock::new(|| {
        let mut m: HashMap<&'static str, ParserFactory> = HashMap::new();
        m.insert(
            "json_parser",
            Arc::new(|raw: Value, table: Arc<str>, key: Option<ExactlyOnceKey>| {
                let cfg: crate::parsers::json_parser::JsonParserConfig =
                    serde_yaml::from_value(raw)?;
                Ok(Arc::new(crate::parsers::json_parser::JsonParser::new(
                    &cfg, table, key,
                )?) as Arc<dyn Parser>)
            }),
        );
        Mutex::new(m)
    });

pub fn register_parser(name: &'static str, factory: ParserFactory) {
    PARSER_REGISTRY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(name, factory);
}

pub fn parser_names() -> HashSet<&'static str> {
    PARSER_REGISTRY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .keys()
        .copied()
        .collect()
}

pub fn build_parser(
    name: &str,
    raw: Value,
    table: Arc<str>,
    key: Option<ExactlyOnceKey>,
) -> anyhow::Result<Arc<dyn Parser>> {
    let factory = {
        let registry = PARSER_REGISTRY
            .lock()
            .map_err(|e| anyhow::anyhow!("parser registry is poisoned: {e}"))?;
        registry
            .get(name)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Unknown parser '{}'; registered: {:?}",
                    name,
                    registry.keys().collect::<Vec<_>>(),
                )
            })?
    };
    factory(raw, table, key)
}
