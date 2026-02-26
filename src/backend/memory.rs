use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use futures::{stream, Stream};
use crate::Serializable;
use super::{Backend, BackendError, Column, Result};

#[derive(Debug, Clone, Default)]
struct MemoryStore {
    columns: HashMap<Column, HashMap<Vec<u8>, Vec<u8>>>,
}

/// In-memory backend implementation for testing
#[derive(Debug, Clone)]
pub struct MemoryBackend {
    store: Arc<RwLock<MemoryStore>>,
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryBackend {
    /// Create a new empty memory backend
    pub fn new() -> Self {
        Self {
            store: Arc::new(RwLock::new(MemoryStore::default())),
        }
    }
}

impl Backend for MemoryBackend {
    type Error = std::convert::Infallible;
    type RawBytes = Vec<u8>;

    async fn open_column<C: AsRef<[u8]>>(&self, _column: C) -> Result<(), Self::Error> {
        // No-op for memory backend - columns are created on demand
        Ok(())
    }

    async fn write<K: Serializable, V: Serializable>(
        &self,
        column: Column,
        key: K,
        data: V,
    ) -> Result<(), Self::Error> {
        let key_bytes = key.to_bytes().map_err(BackendError::from)?;
        let value_bytes = data.to_bytes().map_err(BackendError::from)?;

        let mut store = self.store.write().unwrap();
        store
            .columns
            .entry(column)
            .or_insert_with(HashMap::new)
            .insert(key_bytes, value_bytes);

        Ok(())
    }

    async fn read<K: Serializable>(
        &self,
        column: Column,
        key: K,
    ) -> Result<Option<Self::RawBytes>, Self::Error> {
        let key_bytes = key.to_bytes().map_err(BackendError::from)?;

        let store = self.store.read().unwrap();
        Ok(store
            .columns
            .get(&column)
            .and_then(|col| col.get(&key_bytes).cloned()))
    }

    async fn iterator_prefix<P: Serializable>(
        &self,
        column: Column,
        prefix: P,
    ) -> Result<impl Stream<Item = Result<(Self::RawBytes, Self::RawBytes), Self::Error>>, Self::Error> {
        let prefix_bytes = prefix.to_bytes().map_err(BackendError::from)?;

        let store = self.store.read().unwrap();
        let entries: Vec<(Vec<u8>, Vec<u8>)> = store
            .columns
            .get(&column)
            .map(|col| {
                col.iter()
                    .filter(|(k, _)| k.starts_with(&prefix_bytes))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(stream::iter(entries.into_iter().map(Ok)))
    }

    async fn delete<K: Serializable>(
        &self,
        column: Column,
        key: K,
    ) -> Result<(), Self::Error> {
        let key_bytes = key.to_bytes().map_err(BackendError::from)?;

        let mut store = self.store.write().unwrap();
        if let Some(col) = store.columns.get_mut(&column) {
            col.remove(&key_bytes);
        }

        Ok(())
    }

    async fn exists<K: Serializable>(
        &self,
        column: Column,
        key: K,
    ) -> Result<bool, Self::Error> {
        let key_bytes = key.to_bytes().map_err(BackendError::from)?;

        let store = self.store.read().unwrap();
        Ok(store
            .columns
            .get(&column)
            .map(|col| col.contains_key(&key_bytes))
            .unwrap_or(false))
    }

    async fn list_keys<'a>(
        &'a self,
        column: Column,
    ) -> Result<impl Stream<Item = Result<Self::RawBytes, Self::Error>> + 'a, Self::Error> {
        let store = self.store.read().unwrap();
        let keys: Vec<Vec<u8>> = store
            .columns
            .get(&column)
            .map(|col| col.keys().cloned().collect())
            .unwrap_or_default();

        Ok(stream::iter(keys.into_iter().map(Ok)))
    }

    async fn clear(&self) -> Result<(), Self::Error> {
        let mut store = self.store.write().unwrap();
        store.columns.clear();
        Ok(())
    }

    async fn flush(&self) -> Result<(), Self::Error> {
        // No-op for memory backend
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::ColumnKind;

    use super::*;

    #[tokio::test]
    async fn test_memory_backend_basic_operations() {
        let backend = MemoryBackend::new();
        let column = Column {
        id: 1,
            kind: ColumnKind::Entity,
        };

        backend.open_column(b"test").await.unwrap();

        // Test write and read
        backend.write(column, &1u32, &42u64).await.unwrap();
        let result = backend.read(column, &1u32).await.unwrap();
        assert!(result.is_some());

        // Test exists
        assert!(backend.exists(column, &1u32).await.unwrap());
        assert!(!backend.exists(column, &2u32).await.unwrap());

        // Test delete
        backend.delete(column, &1u32).await.unwrap();
        assert!(!backend.exists(column, &1u32).await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_backend_clear() {
        let backend = MemoryBackend::new();
        let column = Column {
            id: 1,
            kind: ColumnKind::Entity,
        };

        backend.write(column, &1u32, &100u64).await.unwrap();
        backend.clear().await.unwrap();
        assert!(!backend.exists(column, &1u32).await.unwrap());
    }
}
