pub mod memory;

use futures::Stream;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use std::fmt::{self, Display};
use crate::{ReaderError, Serializable, WriterError};

pub use memory::MemoryBackend;

/// Represents a database column/namespace for organizing data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Column {
    pub(crate) id: u32,
    pub(crate) kind: ColumnKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnKind {
    Entity,
    Index,
    Other,
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Column({})", self.id)
    }
}

#[derive(Debug, Error)]
pub enum BackendError<B: Display> {
    #[error(transparent)]
    Writer(#[from] WriterError),
    #[error(transparent)]
    Reader(#[from] ReaderError),
    #[error("Backend error: {0}")]
    Backend(B),
}

/// Trait defining the interface for different database backends
pub trait Backend {
    /// Error type specific to the backend implementation
    type Error: Display;

    /// Raw bytes type returned by the backend for deserialization
    type RawBytes: AsRef<[u8]>;

    /// Open a column/namespace for use (e.g., for an entity type)
    fn open_column(&self, column: &Column) -> impl Future<Output = Result<(), BackendError<Self::Error>>> + Send;

    /// Write data to the database
    fn write<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(&mut self, column: &Column, key: K, data: V) -> impl Future<Output = Result<(), BackendError<Self::Error>>> + Send;

    /// Delete a key entirely
    fn delete<K: Serializable + Send + Sync>(&mut self, column: &Column, key: K) -> impl Future<Output = Result<(), BackendError<Self::Error>>> + Send;

    /// Read data at the latest version
    fn read<K: Serializable + Send + Sync>(&self, column: &Column, key: K) -> impl Future<Output = Result<Option<Self::RawBytes>, BackendError<Self::Error>>> + Send;

    /// Get all historical entries for a key (entire history)
    fn iterator_prefix<'a, P: Serializable + Send + 'a>(&'a self, column: &'a Column, prefix: P) -> impl Future<Output = Result<impl Stream<Item = Result<(Self::RawBytes, Self::RawBytes), BackendError<Self::Error>>> + 'a, BackendError<Self::Error>>> + Send + 'a;

    /// Check if a key exists at current version
    fn exists<K: Serializable + Send + Sync>(&self, column: &Column, key: K) -> impl Future<Output = Result<bool, BackendError<Self::Error>>> + Send;

    /// Get all keys in a column
    fn list_keys<'a>(&'a self, column: &'a Column) -> impl Future<Output = Result<impl Stream<Item = Result<Self::RawBytes, BackendError<Self::Error>>> + 'a, BackendError<Self::Error>>> + Send + 'a;

    /// Clear all data (careful operation)
    fn clear(&mut self) -> impl Future<Output = Result<(), BackendError<Self::Error>>> + Send;

    /// Flush any pending writes to the storage medium
    fn flush(&self) -> impl Future<Output = Result<(), BackendError<Self::Error>>> + Send;
}
