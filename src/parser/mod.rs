pub mod json_parser;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};

use json_parser::{JsonParser, ParserWorkspace};

use crate::config::yaml::SchemaConfig;
use crate::types::exactly_once::ExactlyOnceKey;
use crate::types::message::Message;
use crate::types::table_data::TableData;

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
// ParserRegistry — process-global, seeded with json_parser on first access
// ---------------------------------------------------------------------------

/// `Arc` (not `Box`) so a factory can be cloned out of the registry and invoked
/// without holding the registry lock.
type ParserFactory = Arc<
    dyn Fn(&SchemaConfig, Arc<str>, Option<ExactlyOnceKey>) -> anyhow::Result<Arc<dyn Parser>>
        + Send
        + Sync,
>;

static PARSER_REGISTRY: LazyLock<Mutex<HashMap<&'static str, ParserFactory>>> =
    LazyLock::new(|| {
        let mut m: HashMap<&'static str, ParserFactory> = HashMap::new();
        m.insert(
            "json_parser",
            Arc::new(|schema: &SchemaConfig, table: Arc<str>, key: Option<ExactlyOnceKey>| {
                Ok(Arc::new(JsonParser::new(schema, table, key)?) as Arc<dyn Parser>)
            }),
        );
        Mutex::new(m)
    });

pub fn register_parser(name: &'static str, factory: ParserFactory) {
    // Recover the lock from a possible poison error instead of panicking.
    PARSER_REGISTRY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(name, factory);
}

pub fn parser_names() -> HashSet<&'static str> {
    PARSER_REGISTRY
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .keys()
        .copied()
        .collect()
}

pub fn build_parser(
    name: &str,
    schema: &SchemaConfig,
    table: Arc<str>,
    key: Option<ExactlyOnceKey>,
) -> anyhow::Result<Arc<dyn Parser>> {
    // Scope the lock strictly to the lookup: the factory is cloned out and
    // invoked after the guard is released (avoiding lock contention).
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
    factory(schema, table, key)
}

// The `Parser` impl for `JsonParser` lives in `json_parser.rs` next to the
// implementation (it needs access to private types).
