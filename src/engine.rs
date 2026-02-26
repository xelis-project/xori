use dashmap::DashMap;
use futures::{Stream, StreamExt};

use crate::{BackendError, Result, Serializable};
use crate::backend::{Backend, Column, ColumnKind};
use crate::entity::{Entity, EntityHandle, KeyIndexColumn};
use std::sync::Arc;

/// The main Xori database engine wrapped in Arc for use in EntityHandle
pub struct XoriEngine<B: Backend> {
    backend: Arc<B>,
    entity_registry: DashMap<&'static str, Column>,
}

pub struct EntityConfig {
    pub key_indexing: bool,
}

impl<B: Backend> XoriEngine<B> {
    /// Create a new Xori engine with the given backend
    #[inline]
    pub fn new(backend: Arc<B>) -> Arc<Self> {
        Arc::new(Self {
            backend,
            entity_registry: DashMap::new(),
        })
    }

    /// Register an entity type and get a handle for it
    #[inline]
    pub async fn register<E: Entity>(self: &Arc<Self>, config: EntityConfig) -> Result<EntityHandle<E, B>, B::Error> {
        if self.entity_registry.contains_key(E::entity_name()) {
            return Err(BackendError::ColumnAlreadyRegistered);
        }

        self.backend.open_column(E::entity_name()).await?;
        let column = Column {
            id: self.entity_registry.len() as u32,
            kind: ColumnKind::Entity, 
        };

        self.entity_registry.insert(E::entity_name(), column.clone());

        let key_index_column = if config.key_indexing {
            let key_to_id = Column {
                id: self.entity_registry.len() as u32,
                kind: ColumnKind::Index,
            };
            self.backend.open_column(format!("{}:ki", E::entity_name())).await?;

            let id_to_key = Column {
                id: self.entity_registry.len() as u32,
                kind: ColumnKind::Index,
            };
            self.backend.open_column(format!("{}:ik", E::entity_name())).await?;

            Some(KeyIndexColumn {
                key_to_id,
                id_to_key,
            })
        } else {
            None
        };

        Ok(EntityHandle {
            column,
            key_index_column,
            engine: Arc::clone(&self),
            _phantom: std::marker::PhantomData,
        })
    }

    /// Get a handle for a previously registered entity type
    #[inline]
    pub fn entity<E: Entity>(self: &Arc<Self>) -> Option<EntityHandle<E, B>> {
        self.entity_registry
            .get(E::entity_name())
            .map(|column| EntityHandle {
                column: column.value().clone(),
                key_index_column: None,
                engine: Arc::clone(&self),
                _phantom: std::marker::PhantomData,
            })
    }

    /// Check if an entity type is registered
    #[inline]
    pub fn is_registered(&self, entity_type: &str) -> bool {
        self.entity_registry.contains_key(entity_type)
    }
    
    /// Clear all data from the database
    #[inline]
    pub async fn clear(&self) -> Result<(), B::Error> {
        self.backend.clear().await
    }

    /// Read a value from the backend for a given column and key
    #[inline]
    pub async fn read<K: Serializable, V: Serializable>(&self, column: Column, key: K) -> Result<Option<V>, BackendError<B::Error>> {
        self.backend.read(column, key).await
            .and_then(|bytes| bytes
                    .map(|bytes| V::from_bytes(bytes.as_ref())
                        .map_err(BackendError::from)
                    ).transpose()
                )
            .map_err(BackendError::Backend)
    }

    /// Write a value to the backend for a given column and key
    #[inline]
    pub async fn write<K: Serializable, V: Serializable>(&self, column: Column, key: K, value: V) -> Result<(), BackendError<B::Error>> {
        self.backend.write(column, key, value).await
            .map_err(BackendError::Backend)
    }

    /// Iterate over all entries with keys that start with the given prefix
    pub async fn iterator_prefix<'a, P: Serializable + 'a, K: Serializable, V: Serializable>(&'a self, column: Column, prefix: P) -> Result<impl Stream<Item = Result<(K, V), BackendError<B::Error>>> + 'a, BackendError<B::Error>> {
        match self.backend.iterator_prefix(column, prefix).await {
            Ok(stream) => Ok(stream.map(|item| match item {
                Ok((key, value)) => {
                    let key = K::from_bytes(key)?;
                    let value = V::from_bytes(value)?;
                    Ok((key, value))
                },
                Err(e) => Err(BackendError::Backend(e)),
            })),
            Err(e) => Err(BackendError::Backend(e)),
        }
    }

    /// List all keys in a column
    pub async fn list_keys<'a, K: Serializable + 'a>(&'a self, column: Column) -> Result<impl Stream<Item = Result<K, BackendError<B::Error>>> + 'a, BackendError<B::Error>> {
        match self.backend.list_keys(column).await {
            Ok(stream) => Ok(stream.map(|item| match item {
                Ok(key) => K::from_bytes(key).map_err(BackendError::from),
                Err(e) => Err(BackendError::Backend(e)),
            })),
            Err(e) => Err(BackendError::Backend(e)),
        }
    }

    /// Delete a key from the backend for a given column
    pub async fn delete<K: Serializable>(&self, column: Column, key: K) -> Result<(), BackendError<B::Error>> {
        self.backend.delete(column, key).await
            .map_err(BackendError::Backend)
    }
}