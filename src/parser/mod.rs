pub mod json_parser;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};

pub use json_parser::{JsonParser, ParserWorkspace};

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

type ParserFactory = Box<
    dyn Fn(&SchemaConfig, Arc<str>, Option<ExactlyOnceKey>) -> anyhow::Result<Arc<dyn Parser>>
        + Send
        + Sync,
>;

static PARSER_REGISTRY: LazyLock<Mutex<HashMap<&'static str, ParserFactory>>> =
    LazyLock::new(|| {
        let mut m: HashMap<&'static str, ParserFactory> = HashMap::new();
        m.insert(
            "json_parser",
            Box::new(|schema: &SchemaConfig, table: Arc<str>, key: Option<ExactlyOnceKey>| {
                Ok(Arc::new(JsonParser::new(schema, table, key)?) as Arc<dyn Parser>)
            }),
        );
        Mutex::new(m)
    });

pub fn register_parser(name: &'static str, factory: ParserFactory) {
    PARSER_REGISTRY.lock().unwrap().insert(name, factory);
}

pub fn parser_names() -> HashSet<&'static str> {
    PARSER_REGISTRY.lock().unwrap().keys().copied().collect()
}

pub fn build_parser(
    name: &str,
    schema: &SchemaConfig,
    table: Arc<str>,
    key: Option<ExactlyOnceKey>,
) -> anyhow::Result<Arc<dyn Parser>> {
    let registry = PARSER_REGISTRY.lock().unwrap();
    let factory = registry.get(name).ok_or_else(|| {
        anyhow::anyhow!(
            "Unknown parser '{}'; registered: {:?}",
            name,
            registry.keys().collect::<Vec<_>>(),
        )
    })?;
    factory(schema, table, key)
}

// Delegate to the inherent method — same signature.
impl Parser for JsonParser {
    fn parse_into(
        &self,
        messages: Vec<Message>,
        partition_id: i64,
        exactly_once_key: Option<ExactlyOnceKey>,
        ws: &mut ParserWorkspace,
    ) -> anyhow::Result<(TableData, Option<TableData>)> {
        JsonParser::parse_into(self, messages, partition_id, exactly_once_key, ws)
    }
}
