use std::{collections::{BTreeMap, HashMap}, fmt::Display};
use futures::{stream, Stream};
use itertools::Either;
use crate::{Serializable, backend::{BackendError, ColumnId}, engine::{IteratorDirection, IteratorMode}};
use super::{Backend, Column};
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
struct MemoryStore {
    columns: HashMap<ColumnId, BTreeMap<Bytes, Bytes>>,
}

/// In-memory backend implementation for testing
#[derive(Debug, Clone)]
pub struct MemoryBackend {
    store: MemoryStore,
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
            store: MemoryStore::default(),
        }
    }
}

/// Helper function to serialize a Serializable value into Bytes for storage
#[inline]
fn serialize_data<V: Serializable, E: Display>(data: V) -> Result<Bytes, BackendError<E>> {
    let bytes = data.to_bytes()?;
    Ok(Bytes::copy_from_slice(&bytes))
}

impl Backend for MemoryBackend {
    type Config = ();
    type Error = std::convert::Infallible;
    type RawBytes = Bytes;

    async fn open(_: Self::Config, columns: &[Column]) -> Result<Self, BackendError<Self::Error>> {
        let mut backend = Self::new();
        for column in columns {
            backend.open_column(column).await?;
        }
        Ok(backend)
    }

    async fn open_column(&mut self, _: &Column) -> Result<(), BackendError<Self::Error>> {
        // No-op for memory backend - columns are created on demand
        Ok(())
    }

    async fn write<K: Serializable, V: Serializable>(
        &mut self,
        column: &Column,
        key: K,
        data: V,
    ) -> Result<(), BackendError<Self::Error>> {
        let key_bytes = serialize_data(key)?;
        let value_bytes = serialize_data(data)?;

        self.store
            .columns
            .entry(column.id())
            .or_default()
            .insert(key_bytes, value_bytes);

        Ok(())
    }

    async fn read<K: Serializable>(
        &self,
        column: &Column,
        key: K,
    ) -> Result<Option<Self::RawBytes>, BackendError<Self::Error>> {
        let key_bytes = serialize_data(key)?;

        Ok(self.store
            .columns
            .get(&column.id())
            .and_then(|col| col.get(&key_bytes).cloned()))
    }

    async fn iterator<'a>(
        &'a self,
        column: &'a Column,
        mode: IteratorMode<'a>,
    ) -> Result<impl Stream<Item = Result<(Self::RawBytes, Self::RawBytes), BackendError<Self::Error>>> + 'a, BackendError<Self::Error>> {
        let entries = self.store
            .columns
            .get(&column.id())
            .into_iter()
            .flat_map(move |col| {
                let (lower, upper, direction) = mode.bounds();
                let range = col.range((lower, upper));
                match direction {
                    IteratorDirection::Forward => Either::Left(range),
                    IteratorDirection::Backward => Either::Right(range.rev()),
                }.into_iter().map(|(k, v)| Ok((k.clone(), v.clone())))
            });

        Ok(stream::iter(entries))
    }

    async fn delete<K: Serializable>(
        &mut self,
        column: &Column,
        key: K,
    ) -> Result<(), BackendError<Self::Error>> {
        let key_bytes = serialize_data(key)?;

        if let Some(col) = self.store.columns.get_mut(&column.id()) {
            col.remove(&key_bytes);
        }

        Ok(())
    }

    async fn exists<K: Serializable>(
        &self,
        column: &Column,
        key: K,
    ) -> Result<bool, BackendError<Self::Error>> {
        let key_bytes = serialize_data(key)?;

        Ok(self.store
            .columns
            .get(&column.id())
            .map_or(false, |col| col.contains_key(&key_bytes))
        )
    }

    async fn clear(&mut self) -> Result<(), BackendError<Self::Error>> {
        self.store.columns.clear();
        Ok(())
    }

    async fn flush(&self) -> Result<(), BackendError<Self::Error>> {
        // No-op for memory backend
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::backend::{ColumnId, ColumnProperties, column::{ColumnInner, ColumnKind}};

    use super::*;

    #[tokio::test]
    async fn test_memory_backend_basic_operations() {
        let mut backend = MemoryBackend::new();
        let column = Arc::new(ColumnInner {
            name: "test_entity".into(),
            id: ColumnId(1),
            kind: ColumnKind::Entity,
            properties: ColumnProperties { prefix_length: Some(4) },
        });

        backend.open_column(&column).await.unwrap();

        // Test write and read
        backend.write(&column, &1u32, &42u64).await.unwrap();
        let result = backend.read(&column, &1u32).await.unwrap();
        assert!(result.is_some());

        // Test exists
        assert!(backend.exists(&column, &1u32).await.unwrap());
        assert!(!backend.exists(&column, &2u32).await.unwrap());

        // Test delete
        backend.delete(&column, &1u32).await.unwrap();
        assert!(!backend.exists(&column, &1u32).await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_backend_clear() {
        let mut backend = MemoryBackend::new();
        let column = Arc::new(ColumnInner {
            name: "test_entity".into(),
            id: ColumnId(1),
            kind: ColumnKind::Entity,
            properties: ColumnProperties { prefix_length: Some(4) },
        });
        backend.write(&column, 1u32, 100u64).await.unwrap();
        backend.clear().await.unwrap();
        assert!(!backend.exists(&column, &1u32).await.unwrap());
    }
}
