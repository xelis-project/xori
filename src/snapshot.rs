use bytes::Bytes;
use futures::{Stream, StreamExt, future::{Either, ready}, stream};

use crate::{Backend, Changes, Column, Serializable, XoriError, XoriResult, changes::EntryState, engine::{IteratorMode, XoriBackend}};

/// Represents a snapshot of the current state of the engine, including pending changes
#[derive(Clone)]
pub struct Snapshot<'a, B: Backend> {
    pub(crate) engine: &'a XoriBackend<B>,
    pub(crate) changes: Changes,
}

impl<'a, B: Backend> Snapshot<'a, B> {
    /// Create a new snapshot with the given engine and pending changes
    pub(crate) fn new(engine: &'a XoriBackend<B>, changes: Changes) -> Self {
        Self {
            engine,
            changes,
        }
    }

    /// Read a value from the backend for a given column and key
    #[inline]
    pub async fn read<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(&self, column: &Column, key: K) -> XoriResult<Option<V>, B::Error> {
        if let Some(column) = self.changes.column(column) {
            match column.get(key.to_bytes()?) {
                EntryState::Stored(bytes) => return V::from_bytes(bytes.as_ref()).map(Some).map_err(XoriError::from),
                EntryState::Deleted => return Ok(None),
                EntryState::Absent => {}
            };
        }

        self.engine.read(column, key).await
    }

    /// Write a value to the backend for a given column and key
    #[inline]
    pub async fn write<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(&mut self, column: &Column, key: K, value: V) -> XoriResult<(), B::Error> {
        let key = key.to_bytes()?;
        let value = value.to_bytes()?;

        self.changes.column_mut(column)
            .insert(
                Bytes::copy_from_slice(&key),
                Bytes::copy_from_slice(&value)
            );

        Ok(())
    }

    /// Iterate over all entries with keys that start with the given prefix
    pub async fn iterator<'b, K, V>(&'b self, column: &'b Column, mode: IteratorMode<'b>) -> XoriResult<impl Stream<Item = XoriResult<(K, V), B::Error>> + 'b, B::Error>
    where
        K: Serializable + Send + Sync + 'b,
        V: Serializable + Send + Sync + 'b,
    {
        match self.changes.column(column) {
            Some(changes) => {
                let iter = changes.iterator(mode).map(|(key, value)| {
                        let key = K::from_bytes(key).map_err(XoriError::from)?;
                        let value = V::from_bytes(value).map_err(XoriError::from)?;
                        Ok((key, value))
                    });

                let stream = stream::iter(iter);

                // use backend iterator directly to prevent deserializing entries that are modified in the snapshot, and filter them out
                let backend_stream = self.engine.backend.iterator(column, mode).await?
                    .map(|item| {
                        let (key, value) = item.map_err(XoriError::from)?;
                        if changes.contains(&key).unwrap_or(false) {
                            Ok::<Option<(K, V)>, XoriError<B::Error>>(None)
                        } else {
                            let key = K::from_bytes(key).map_err(XoriError::from)?;
                            let value = V::from_bytes(value).map_err(XoriError::from)?;
                            Ok(Some((key, value)))
                        }
                    })
                    .filter_map(|item| ready(match item {
                        Ok(Some(kv)) => Some(Ok(kv)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    }));

                Ok(Either::Left(stream.chain(backend_stream)))
            },
            None => self.engine.iterator(column, mode).await.map(Either::Right)
        }
    }
}