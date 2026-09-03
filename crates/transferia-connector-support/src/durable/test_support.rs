use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;

use super::{CompareExchangeResult, DurableContext, DurableLease, DurableStorage, DurableValue};

#[derive(Default)]
struct MemoryDurableStorage {
    values: Mutex<HashMap<String, DurableValue>>,
    leases: Arc<Mutex<HashSet<String>>>,
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
            let revision = match expected_revision {
                Some(revision) => revision
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("durable revision overflow"))?,
                None => 0,
            };
            let value = DurableValue {
                revision,
                payload: payload.to_vec(),
            };
            values.insert(key.to_owned(), value.clone());
            drop(values);
            Ok(CompareExchangeResult::Applied(value))
        })
    }

    fn acquire_execution_lease<'a>(
        &'a self,
        key: &'a str,
    ) -> BoxFuture<'a, anyhow::Result<DurableLease>> {
        Box::pin(async move {
            let mut leases = self
                .leases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            anyhow::ensure!(
                leases.insert(key.to_owned()),
                "another execution already owns durable lease '{key}'"
            );
            drop(leases);
            Ok(DurableLease::new(MemoryDurableLease {
                key: key.to_owned(),
                leases: Arc::clone(&self.leases),
            }))
        })
    }
}

struct MemoryDurableLease {
    key: String,
    leases: Arc<Mutex<HashSet<String>>>,
}

impl Drop for MemoryDurableLease {
    fn drop(&mut self) {
        self.leases
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.key);
    }
}

#[must_use]
pub fn context() -> DurableContext {
    DurableContext {
        delivery_id: Arc::from("test-delivery"),
        storage: Arc::new(MemoryDurableStorage::default()),
        resource_storage: Arc::new(MemoryDurableStorage::default()),
    }
}
