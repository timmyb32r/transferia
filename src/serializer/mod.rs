pub mod json_serializer;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
pub use json_serializer::JsonSerializer;

/// Converts Arrow [`RecordBatch`]es into serialized output format.
///
/// The inverse of the parser: takes columnar data and produces byte sequences
/// suitable for writing to sinks (S3 objects, YDS messages).
pub trait Serializer: Send + Sync {
    /// Serialize a single RecordBatch into a `Vec<u8>`.
    /// Each implementation defines its own output format (NDJSON, Parquet, etc.).
    fn serialize_batch(&self, batch: &RecordBatch) -> anyhow::Result<Bytes>;
}

// ---------------------------------------------------------------------------
// SerializerRegistry — process-global, seeded with "json" on first access
// ---------------------------------------------------------------------------

type SerializerFactory = Box<dyn Fn() -> Arc<dyn Serializer> + Send + Sync>;

static SERIALIZER_REGISTRY: LazyLock<Mutex<HashMap<&'static str, SerializerFactory>>> =
    LazyLock::new(|| {
        let mut m: HashMap<&'static str, SerializerFactory> = HashMap::new();
        m.insert("json", Box::new(|| Arc::new(JsonSerializer) as Arc<dyn Serializer>));
        Mutex::new(m)
    });

pub fn register_serializer(name: &'static str, factory: SerializerFactory) {
    SERIALIZER_REGISTRY.lock().unwrap().insert(name, factory);
}

pub fn build_serializer(name: &str) -> anyhow::Result<Arc<dyn Serializer>> {
    let registry = SERIALIZER_REGISTRY.lock().unwrap();
    let factory = registry.get(name).ok_or_else(|| {
        anyhow::anyhow!(
            "Unknown serializer '{}'; registered: {:?}",
            name,
            registry.keys().collect::<Vec<_>>(),
        )
    })?;
    Ok(factory())
}
