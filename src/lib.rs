pub mod backend;
pub mod entity;
pub mod engine;
pub mod serde;
pub mod types;
pub mod builder;
pub mod changes;
pub mod dag;
pub mod snapshot;

pub use backend::{Backend, Column, BackendError, MemoryBackend};
#[cfg(feature = "rocksdb")]
pub use backend::RocksDBBackend;
pub use entity::{Entity, EntityReadHandle};
pub use engine::{XoriEngine, XoriResult, XoriError};
use futures::{Stream, future::Either};
pub use serde::*;
pub use types::*;
pub use builder::{XoriBuilder, EntityConfig};
pub use changes::Changes;
pub use dag::{DagState, DagEntry, DagEntryBuilder, DagKey, DagError, DagResult};

use crate::{engine::IteratorMode, snapshot::Snapshot};

/// A wrapper type that can represent either a mutable reference to the engine or a snapshot, allowing for unified read/write operations
/// In case you plan to use a snapshot or batched changes,
/// you can use this wrapper to abstract over the underlying engine or snapshot and perform read/write operations 
/// without needing to worry about the specific type of engine or snapshot being used.
pub enum EngineWrapper<'a, B: Backend> {
    Engine(&'a mut XoriEngine<B>),
    Snapshot(&'a mut Snapshot<'a, B>),
}

impl<'a, B: Backend> EngineWrapper<'a, B> {
    /// Create a new engine instance from a mutable reference to an XoriEngine
    #[inline]
    pub fn new(engine: &'a mut XoriEngine<B>) -> Self {
        Self::Engine(engine)
    }

    /// Create a new engine instance from a snapshot
    #[inline]
    pub fn from_snapshot(snapshot: &'a mut Snapshot<'a, B>) -> Self {
        Self::Snapshot(snapshot)
    }

    /// Write a value to the backend for a given column and key
    #[inline]
    pub async fn write<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(&mut self, column: &Column, key: K, value: V) -> XoriResult<(), B::Error> {
        match self {
            EngineWrapper::Engine(engine) => engine.write(column, key, value).await,
            EngineWrapper::Snapshot(snapshot) => snapshot.write(column, key, value).await,
        }
    }

    /// Read a value from the backend for a given column and key
    #[inline]
    pub async fn read<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(&self, column: &Column, key: K) -> XoriResult<Option<V>, B::Error> {
        match self {
            EngineWrapper::Engine(engine) => engine.read(column, key).await,
            EngineWrapper::Snapshot(snapshot) => snapshot.read(column, key).await,
        }
    }

    /// Iterate over all entries with keys that start with the given prefix
    #[inline]
    pub async fn iterator<'b, K, V>(&'b self, column: &'b Column, mode: IteratorMode<'b>) -> XoriResult<impl Stream<Item = XoriResult<(K, V), B::Error>> + 'b, B::Error>
    where
        K: Serializable + Send + Sync + 'b,
        V: Serializable + Send + Sync + 'b,
    {
        match self {
            EngineWrapper::Engine(engine) => engine.iterator(column, mode).await.map(Either::Left),
            EngineWrapper::Snapshot(snapshot) => snapshot.iterator(column, mode).await.map(Either::Right),
        }
    }
}