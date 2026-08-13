use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use transferia::durable::{CompareExchangeResult, DurableContext, DurableStorage, DurableValue};

#[derive(Default)]
struct MemoryDurableStorage {
    values: Mutex<HashMap<String, DurableValue>>,
}

impl DurableStorage for MemoryDurableStorage {
    fn read<'a>(&'a self, key: &'a str) -> BoxFuture<'a, anyhow::Result<Option<DurableValue>>> {
        Box::pin(async move {
            Ok(self
                .values
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(key)
                .cloned())
        })
    }

    fn compare_exchange<'a>(
        &'a self,
        key: &'a str,
        expected_revision: Option<u64>,
        payload: &'a [u8],
    ) -> BoxFuture<'a, anyhow::Result<CompareExchangeResult>> {
        Box::pin(async move {
            let mut values = self
                .values
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let current = values.get(key).cloned();
            if current.as_ref().map(|value| value.revision) != expected_revision {
                return Ok(CompareExchangeResult::Conflict(current));
            }
            let value = DurableValue {
                revision: expected_revision.map_or(0, |revision| revision.saturating_add(1)),
                payload: payload.to_vec(),
            };
            values.insert(key.to_owned(), value.clone());
            drop(values);
            Ok(CompareExchangeResult::Applied(value))
        })
    }
}

pub fn durable_context() -> DurableContext {
    DurableContext {
        delivery_id: Arc::from("integration-test"),
        storage: Arc::new(MemoryDurableStorage::default()),
    }
}
