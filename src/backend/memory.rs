use std::collections::{BTreeMap, HashMap};
use futures::{stream, Stream};
use itertools::Either;
use crate::{Serializable, backend::BackendError, engine::{IteratorDirection, IteratorMode}};
use super::{Backend, Column};
use bytes::Bytes;

#[derive(Debug, Clone, Default)]
struct MemoryStore {
    columns: HashMap<Column, BTreeMap<Bytes, Bytes>>,
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

impl Backend for MemoryBackend {
    type Error = std::convert::Infallible;
    type RawBytes = Bytes;

    async fn open_column(&self, _: &Column) -> Result<(), BackendError<Self::Error>> {
        // No-op for memory backend - columns are created on demand
        Ok(())
    }

    async fn write<K: Serializable, V: Serializable>(
        &mut self,
        column: &Column,
        key: K,
        data: V,
    ) -> Result<(), BackendError<Self::Error>> {
        let key_bytes = key.to_bytes()
            .map(Bytes::from)
            .map_err(BackendError::from)?;
        let value_bytes = data.to_bytes()
            .map(Bytes::from)
            .map_err(BackendError::from)?;

        self.store
            .columns
            .entry(*column)
            .or_default()
            .insert(key_bytes, value_bytes);

        Ok(())
    }

    async fn read<K: Serializable>(
        &self,
        column: &Column,
        key: K,
    ) -> Result<Option<Self::RawBytes>, BackendError<Self::Error>> {
        let key_bytes = key.to_bytes()
            .map(Bytes::from)
            .map_err(BackendError::from)?;

        Ok(self.store
            .columns
            .get(&column)
            .and_then(|col| col.get(&key_bytes).cloned()))
    }

    async fn iterator<'a>(
        &'a self,
        column: &'a Column,
        mode: IteratorMode<'a>,
    ) -> Result<impl Stream<Item = Result<(Self::RawBytes, Self::RawBytes), BackendError<Self::Error>>> + 'a, BackendError<Self::Error>> {
        let entries = self.store
            .columns
            .get(column)
            .into_iter()
            .flat_map(move |col| {
                match mode {
                    IteratorMode::All(direction) => Either::Left(Either::Left(match direction {
                        IteratorDirection::Forward => Either::Left(col.iter()),
                        IteratorDirection::Backward => Either::Right(col.iter().rev()),
                    })),
                    IteratorMode::Prefix(prefix, direction) => {
                        let prefix = Bytes::copy_from_slice(prefix);
                        let range = col.range(prefix.clone()..);

                        Either::Left(Either::Right(match direction {
                            IteratorDirection::Forward => Either::Left(range),
                            IteratorDirection::Backward => Either::Right(range.rev()),
                        }.into_iter().take_while(move |(k, _)| k.starts_with(&prefix))))
                    },
                    IteratorMode::Range { start, end, direction } => {
                        let start = Bytes::copy_from_slice(start);
                        let end = Bytes::copy_from_slice(end);
                        let range = col.range(start..end);

                        Either::Right(match direction {
                            IteratorDirection::Forward => Either::Left(range),
                            IteratorDirection::Backward => Either::Right(range.rev()),
                        })
                    },
                    IteratorMode::From(start, direction) => {
                        let start = Bytes::copy_from_slice(start);
                        let range = col.range(start..);

                        Either::Right(match direction {
                            IteratorDirection::Forward => Either::Left(range),
                            IteratorDirection::Backward => Either::Right(range.rev()),
                        })
                    },
                }.into_iter().map(|(k, v)| Ok((k.clone(), v.clone())))
            });

        Ok(stream::iter(entries))
    }

    async fn delete<K: Serializable>(
        &mut self,
        column: &Column,
        key: K,
    ) -> Result<(), BackendError<Self::Error>> {
        let key_bytes = key.to_bytes()
            .map(Bytes::from)
            .map_err(BackendError::from)?;

        if let Some(col) = self.store.columns.get_mut(&column) {
            col.remove(&key_bytes);
        }

        Ok(())
    }

    async fn exists<K: Serializable>(
        &self,
        column: &Column,
        key: K,
    ) -> Result<bool, BackendError<Self::Error>> {
        let key_bytes = key.to_bytes()
            .map(Bytes::from)
            .map_err(BackendError::from)?;

        Ok(self.store
            .columns
            .get(&column)
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
    use crate::backend::ColumnKind;

    use super::*;

    #[tokio::test]
    async fn test_memory_backend_basic_operations() {
        let mut backend = MemoryBackend::new();
        let column = Column {
            id: 1,
            kind: ColumnKind::Entity,
        };

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
        let column = Column {
            id: 1,
            kind: ColumnKind::Entity,
        };

        backend.write(&column, &1u32, &100u64).await.unwrap();
        backend.clear().await.unwrap();
        assert!(!backend.exists(&column, &1u32).await.unwrap());
    }
}
