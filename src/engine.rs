mod error;

use futures::{Stream, StreamExt};

use crate::builder::EntityInfo;
use crate::{BackendError, EntityReadHandle, Serializable};
use crate::backend::{Backend, Column};
use crate::entity::{Entity, EntityWriteHandle};
use std::collections::HashMap;

pub use error::{XoriError, Result};

/// The main Xori database engine wrapper around the backend and entity registry
pub struct XoriBackend<B: Backend> {
    pub(crate) backend: B,
}

impl<B: Backend> XoriBackend<B> {
    /// Clear all data from the database
    #[inline]
    pub async fn clear(&mut self) -> Result<(), B::Error> {
        self.backend.clear().await
            .map_err(XoriError::Backend)
    }

    /// Read a value from the backend for a given column and key
    #[inline]
    pub async fn read<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(&self, column: &Column, key: K) -> Result<Option<V>, B::Error> {
        self.backend.read(column, key).await
            .and_then(|bytes| bytes
                    .map(|bytes| V::from_bytes(bytes.as_ref())
                        .map_err(BackendError::from)
                    ).transpose()
                )
                .map_err(XoriError::Backend)
    }

    /// Write a value to the backend for a given column and key
    #[inline]
    pub async fn write<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(&mut self, column: &Column, key: K, value: V) -> Result<(), B::Error> {
        self.backend.write(column, key, value).await
            .map_err(XoriError::Backend)
    }

    /// Iterate over all entries with keys that start with the given prefix
    pub async fn iterator_prefix<'a, P: Serializable + Send + Sync + 'a, K: Serializable + Send + Sync, V: Serializable + Send + Sync>(&'a self, column: &'a Column, prefix: P) -> Result<impl Stream<Item = Result<(K, V), B::Error>> + 'a, B::Error> {
        match self.backend.iterator_prefix(column, prefix).await {
            Ok(stream) => Ok(stream.map(|item| match item {
                Ok((key, value)) => {
                    let key = K::from_bytes(key)?;
                    let value = V::from_bytes(value)?;
                    Ok((key, value))
                },
                Err(e) => Err(XoriError::Backend(e)),
            })),
            Err(e) => Err(XoriError::Backend(e)),
        }
    }

    /// List all keys in a column
    pub async fn list_keys<'a, K: Serializable + Send + Sync + 'a>(&'a self, column: &'a Column) -> Result<impl Stream<Item = Result<K, B::Error>> + 'a, B::Error> {
        match self.backend.list_keys(column).await {
            Ok(stream) => Ok(stream.map(|item| match item {
                Ok(key) => K::from_bytes(key).map_err(XoriError::from),
                Err(e) => Err(XoriError::Backend(e)),
            })),
            Err(e) => Err(XoriError::Backend(e)),
        }
    }

    /// Delete a key from the backend for a given column
    pub async fn delete<K: Serializable + Send + Sync>(&mut self, column: &Column, key: K) -> Result<(), B::Error> {
        self.backend.delete(column, key).await
            .map_err(XoriError::Backend)
    }
}

/// The main Xori database engine wrapped in Arc for use in EntityHandle
pub struct XoriEngine<B: Backend> {
    pub(crate) backend: XoriBackend<B>,
    pub(crate) entity_registry: HashMap<&'static str, EntityInfo>,
}

impl<B: Backend> XoriEngine<B> {
    /// Get a handle for a previously registered entity type
    #[inline]
    pub fn entity_handle_read<'a, E: Entity>(&'a self) -> Option<EntityReadHandle<'a, E, B>> {
        self.entity_registry
            .get(E::entity_name())
            .map(|info| EntityReadHandle {
                info,
                backend: &self.backend,
                _phantom: std::marker::PhantomData,
            })
    }

    /// Get a mutable handle for a previously registered entity type
    #[inline]
    pub fn entity_handle_write<'a, E: Entity>(&'a mut self) -> Option<EntityWriteHandle<'a, E, B>> {
        self.entity_registry
            .get(E::entity_name())
            .map(|info| EntityWriteHandle {
                info,
                backend: &mut self.backend,
                _phantom: std::marker::PhantomData,
            })
    }

    /// Get a reference to the backend for advanced operations
    #[inline(always)]
    pub fn backend(&self) -> &XoriBackend<B> {
        &self.backend
    }

    /// Get a mutable reference to the backend for advanced operations
    #[inline(always)]
    pub fn backend_mut(&mut self) -> &mut XoriBackend<B> {
        &mut self.backend
    }
}