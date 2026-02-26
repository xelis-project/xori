pub mod memory;

use futures::Stream;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use thiserror::Error;

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

/// Database errors
#[derive(Error, Debug)]
pub enum BackendError<T: Display> {
    #[error("Column already registered")]
    ColumnAlreadyRegistered,
    #[error("Key not found")]
    KeyNotFound,
    #[error("Column not found")]
    ColumnNotFound,
    #[error("Version not found")]
    VersionNotFound,
    #[error("No version available at or before the specified version")]
    NoVersionAvailable,
    #[error("Serialization error: {0}")]
    ReaderError(#[from] ReaderError),
    #[error("Writer error: {0}")]
    WriterError(#[from] WriterError),
    #[error("Backend error: {0}")]
    Backend(T),
}

pub type Result<T, E> = std::result::Result<T, BackendError<E>>;

/// Trait defining the interface for different database backends
// #[async_trait::async_trait]
pub trait Backend {
    /// Error type specific to the backend implementation
    type Error: Display;

    /// Raw bytes type returned by the backend for deserialization
    type RawBytes: AsRef<[u8]>;

    /// Open a column/namespace for use (e.g., for an entity type)
    fn open_column<C: AsRef<[u8]> + Send>(&self, column: C) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Write data to the database
    fn write<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(&self, column: Column, key: K, data: V) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Read data at the latest version
    fn read<K: Serializable + Send + Sync>(&self, column: Column, key: K) -> impl Future<Output = Result<Option<Self::RawBytes>, Self::Error>> + Send;

    /// Get all historical entries for a key (entire history)
    fn iterator_prefix<P: Serializable + Send>(&self, column: Column, prefix: P) -> impl Future<Output = Result<impl Stream<Item = Result<(Self::RawBytes, Self::RawBytes), Self::Error>>, Self::Error>> + Send;

    /// Delete a key entirely
    fn delete<K: Serializable + Send + Sync>(&self, column: Column, key: K) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Check if a key exists at current version
    fn exists<K: Serializable + Send + Sync>(&self, column: Column, key: K) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// Get all keys in a column
    fn list_keys<'a>(&'a self, column: Column) -> impl Future<Output = Result<impl Stream<Item = Result<Self::RawBytes, Self::Error>> + 'a, Self::Error>> + Send + 'a;

    /// Clear all data (careful operation)
    fn clear(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Flush any pending writes to the storage medium
    fn flush(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
