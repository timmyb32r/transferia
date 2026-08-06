pub mod json_serializer;

use std::collections::HashMap;
use alloc::sync::Arc;
use std::sync::{LazyLock, Mutex};

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use json_serializer::JsonSerializer;

/// Converts Arrow [`RecordBatch`]es into serialized output format.
///
/// The inverse of the parser: takes columnar data and produces byte sequences
/// suitable for writing to sinks (S3 objects, YDS messages).
pub trait Serializer: Send + Sync {
    /// Serialize a single `RecordBatch` into a `Vec<u8>`.
    /// Each implementation defines its own output format (NDJSON, Parquet, etc.).
    fn serialize_batch(&self, batch: &RecordBatch) -> anyhow::Result<Bytes>;
}

// ---------------------------------------------------------------------------
// SerializerRegistry — process-global, seeded with "json" on first access
// ---------------------------------------------------------------------------

/// `Arc` (not `Box`) so a factory can be cloned out of the registry and invoked
/// without holding the registry lock.
type SerializerFactory = Arc<dyn Fn() -> Arc<dyn Serializer> + Send + Sync>;

static SERIALIZER_REGISTRY: LazyLock<Mutex<HashMap<&'static str, SerializerFactory>>> =
    LazyLock::new(|| {
        let mut m: HashMap<&'static str, SerializerFactory> = HashMap::new();
        m.insert("json", Arc::new(|| Arc::new(JsonSerializer::default()) as Arc<dyn Serializer>));
        Mutex::new(m)
    });

pub fn register_serializer(name: &'static str, factory: SerializerFactory) {
    // Recover the lock from a possible poison error instead of panicking.
    SERIALIZER_REGISTRY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(name, factory);
}

/// Build a JSON serializer with explicit null-column skipping behavior.
/// `skip_nulls=true` elides null-valued keys from output; the default (`false`)
/// emits them as `"col": null`.
#[must_use]
pub fn build_json_serializer(skip_nulls: bool) -> Arc<dyn Serializer> {
    Arc::new(JsonSerializer::new(skip_nulls))
}

pub fn build_serializer(name: &str) -> anyhow::Result<Arc<dyn Serializer>> {
    // Scope the lock strictly to the lookup: the factory is cloned out and
    // invoked after the guard is released (avoiding lock contention).
    let factory = {
        let registry = SERIALIZER_REGISTRY
            .lock()
            .map_err(|e| anyhow::anyhow!("serializer registry is poisoned: {e}"))?;
        registry
            .get(name)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Unknown serializer '{}'; registered: {:?}",
                    name,
                    registry.keys().collect::<Vec<_>>(),
                )
            })?
    };
    Ok(factory())
}
