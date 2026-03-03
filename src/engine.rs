mod error;
mod iterator;

use bytes::Bytes;
use futures::future::Either;
use futures::{Stream, StreamExt, stream};

use crate::builder::EntityInfo;
use crate::snapshot::EntryState;
use crate::{BackendError, EntityReadHandle, Serializable, Snapshot};
use crate::backend::{Backend, Column};
use crate::entity::{Entity, EntityWriteHandle};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

pub use error::{XoriError, Result};
pub use iterator::{IteratorDirection, IteratorMode};

/// The main Xori database engine wrapper around the backend and entity registry
pub struct XoriBackend<B: Backend> {
    pub(crate) backend: B,
    /// Memory snapshot for pending changes that have not yet been committed to the backend
    pub(crate) snapshot: Option<Snapshot>,
}

impl<B: Backend> XoriBackend<B> {
    /// Create a new snapshot for batching changes
    /// if a snapshot already exists, it will clone it
    #[inline]
    pub fn create_snapshot(&self) -> Snapshot {
        self.snapshot.clone().unwrap_or_default()
    }

    /// Swap the current snapshot with a new one, returning the old snapshot if it exists
    #[inline]
    pub fn swap_snapshot(&mut self, snapshot: Option<Snapshot>) -> Option<Snapshot> {
        std::mem::replace(&mut self.snapshot, snapshot)
    }

    /// Apply a snapshot of changes to the backend, writing all modified entries and deletions
    pub async fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<(), B::Error> {
        for (column, column_snapshot) in snapshot.columns {
            for (key, value) in column_snapshot.entries {
                match value {
                    Some(value) => self.backend.write(&column, key.as_ref(), value.as_ref()).await.map_err(XoriError::Backend)?,
                    None => self.backend.delete(&column, key.as_ref()).await.map_err(XoriError::Backend)?,
                }
            }
        }
        Ok(())
    }

    /// Clear all data from the database
    #[inline]
    pub async fn clear(&mut self) -> Result<(), B::Error> {
        if self.snapshot.is_some() {
            return Err(XoriError::SnapshotActive.into());
        }

        self.backend.clear().await
            .map_err(XoriError::Backend)
    }

    /// Read a value from the backend for a given column and key
    #[inline]
    pub async fn read<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(&self, column: &Column, key: K) -> Result<Option<V>, B::Error> {
        if let Some(snapshot) = self.snapshot.as_ref().and_then(|snapshot| snapshot.column(column)) {
            match snapshot.get(key.to_bytes()?) {
                EntryState::Stored(bytes) => return V::from_bytes(bytes.as_ref()).map(Some).map_err(XoriError::from),
                EntryState::Deleted => return Ok(None),
                EntryState::Absent => {}
            };
        }

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
        match self.snapshot.as_mut().map(|snapshot| snapshot.column_mut(column.clone())) {
            Some(snapshot) => {
                let key = key.to_bytes()?;
                let value = value.to_bytes()?;

                snapshot.insert(Bytes::copy_from_slice(&key), Bytes::copy_from_slice(&value));
                Ok(())
            },
            None => self.backend.write(column, key, value).await
                .map_err(XoriError::Backend)
        }
    }

    /// Iterate over all entries with keys that start with the given prefix
    pub async fn iterator<'a, K, V>(&'a self, column: &'a Column, mode: IteratorMode<'a>) -> Result<impl Stream<Item = Result<(K, V), B::Error>> + 'a, B::Error>
    where
        K: Serializable + Send + Sync + 'a,
        V: Serializable + Send + Sync + 'a,
    {
        match self.snapshot.as_ref().and_then(|snapshot| snapshot.column(column)) {
            Some(snapshot) => {
                let iter = snapshot.iterator(mode).map(|(key, value)| {
                        let key = K::from_bytes(key).map_err(XoriError::from)?;
                        let value = V::from_bytes(value).map_err(XoriError::from)?;
                        Ok((key, value))
                    });

                let stream = stream::iter(iter);

                let backend_stream = self.backend.iterator(column, mode).await?
                    .map(|item| {
                        let (key, value) = item.map_err(XoriError::from)?;
                        if snapshot.contains(&key).unwrap_or(false) {
                            Ok::<Option<(K, V)>, XoriError<B::Error>>(None)
                        } else {
                            let key = K::from_bytes(key).map_err(XoriError::from)?;
                            let value = V::from_bytes(value).map_err(XoriError::from)?;
                            Ok(Some((key, value)))
                        }
                    })
                    .filter_map(|item| futures::future::ready(match item {
                        Ok(Some(kv)) => Some(Ok(kv)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    }));

                Ok(Either::Left(stream.chain(backend_stream)))
            },
            None => match self.backend.iterator(column, mode).await {
                Ok(stream) => Ok(Either::Right(stream.map(|item| match item {
                    Ok((key, value)) => {
                        let key = K::from_bytes(key)?;
                        let value = V::from_bytes(value)?;
                        Ok((key, value))
                    },
                    Err(e) => Err(XoriError::Backend(e)),
                }))),
                Err(e) => Err(XoriError::Backend(e)),
            }
        }
    }

    /// Iterator over all available keys in a column
    #[inline(always)]
    pub async fn iterator_keys<'a, K: Serializable + Send + Sync + 'a>(&'a self, column: &'a Column, mode: IteratorMode<'a>) -> Result<impl Stream<Item = Result<K, B::Error>> + 'a, B::Error> {
        self.iterator::<K, ()>(column, mode).await
            .map(|stream| stream.map(|item| item.map(|(key, _)| key)))
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