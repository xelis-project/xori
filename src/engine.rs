mod error;
mod iterator;

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use futures::{Stream, StreamExt};

use crate::{
    builder::EntityInfo,
    changes::EntryState,
    snapshot::Snapshot,
    BackendError,
    EntityReadHandle,
    Serializable,
    Changes,
    backend::{Backend, Column, ColumnId},
    entity::{Entity, EntityWriteHandle},
};

pub use error::{XoriError, XoriResult};
pub use iterator::{IteratorDirection, IteratorMode};

/// The main Xori database engine wrapper around the backend and entity registry
pub struct XoriBackend<B: Backend> {
    pub(crate) backend: B,
    /// Set of columns that have been registered
    pub(crate) columns: HashMap<ColumnId, Column>,
}

impl<B: Backend> XoriBackend<B> {
    /// Read pending changes before falling back to the backend.
    pub(crate) async fn read_with_changes<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(
        &self,
        changes: &Changes,
        column: &Column,
        key: K,
    ) -> XoriResult<Option<V>, B::Error> {
        if let Some(changes) = changes.column(column) {
            match changes.get(key.to_bytes()?) {
                EntryState::Stored(value) => return Ok(Some(V::from_bytes(value)?)),
                EntryState::Deleted => return Ok(None),
                EntryState::Absent => {},
            }
        }
        self.read(column, key).await
    }

    /// Create a new snapshot of the current state of the engine, including pending changes
    pub fn create_snapshot<'a>(&'a self) -> Snapshot<'a, B> {
        Snapshot::new(self, Changes::default())
    }

    /// Apply a snapshot of changes to the backend, writing all modified entries and deletions
    pub async fn apply_changes(&mut self, changes: Changes) -> XoriResult<(), B::Error> {
        let iterator = changes.columns.into_iter()
            .map(|(column_id, column_changes)| {
                let column = self.columns.get(&column_id)
                    .expect("Column should exist in the registry"); // SAFETY: no new columns can be created outside of the registry
                (column, column_changes)
            });

        self.backend.write_batch(iterator).await
            .map_err(XoriError::Backend)
    }

    /// Clear all data from the database
    #[inline]
    pub async fn clear(&mut self) -> XoriResult<(), B::Error> {
        self.backend.clear().await
            .map_err(XoriError::Backend)
    }

    /// Read a value from the backend for a given column and key
    #[inline]
    pub async fn read<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(&self, column: &Column, key: K) -> XoriResult<Option<V>, B::Error> {
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
    pub async fn write<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(&mut self, column: &Column, key: K, value: V) -> XoriResult<(), B::Error> {
        self.backend.write(column, key, value).await.map_err(XoriError::Backend)
    }

    /// Iterate over all entries with keys that start with the given prefix
    pub async fn iterator<'a, K, V>(&'a self, column: &'a Column, mode: IteratorMode<'a>) -> XoriResult<impl Stream<Item = XoriResult<(K, V), B::Error>> + 'a, B::Error>
    where
        K: Serializable + Send + Sync + 'a,
        V: Serializable + Send + Sync + 'a,
    {
        match self.backend.iterator(column, mode).await {
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

    /// Iterator over all available keys in a column
    #[inline(always)]
    pub async fn iterator_keys<'a, K: Serializable + Send + Sync + 'a>(&'a self, column: &'a Column, mode: IteratorMode<'a>) -> XoriResult<impl Stream<Item = XoriResult<K, B::Error>> + 'a, B::Error> {
        self.iterator::<K, ()>(column, mode).await
            .map(|stream| stream.map(|item| item.map(|(key, _)| key)))
    }

    /// Delete a key from the backend for a given column
    pub async fn delete<K: Serializable + Send + Sync>(&mut self, column: &Column, key: K) -> XoriResult<(), B::Error> {
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

impl<B: Backend> Deref for XoriEngine<B> {
    type Target = XoriBackend<B>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.backend
    }
}

impl<B: Backend> DerefMut for XoriEngine<B> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.backend
    }
}
