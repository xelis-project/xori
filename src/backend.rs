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
    async fn open_column<C: AsRef<[u8]>>(&self, column: C) -> Result<(), Self::Error>;

    /// Write data to the database
    async fn write<K: Serializable, V: Serializable>(&self, column: Column, key: K, data: V) -> Result<(), Self::Error>;

    /// Read data at the latest version
    async fn read<K: Serializable>(&self, column: Column, key: K) -> Result<Option<Self::RawBytes>, Self::Error>;

    /// Get all historical entries for a key (entire history)
    async fn iterator_prefix<P: Serializable>(&self, column: Column, prefix: P) -> Result<impl Stream<Item = Result<(Self::RawBytes, Self::RawBytes), Self::Error>>, Self::Error>;

    /// Delete a key entirely
    async fn delete<K: Serializable>(&self, column: Column, key: K) -> Result<(), Self::Error>;

    /// Check if a key exists at current version
    async fn exists<K: Serializable>(&self, column: Column, key: K) -> Result<bool, Self::Error>;

    /// Get all keys in a column
    async fn list_keys<'a>(&'a self, column: Column) -> Result<impl Stream<Item = Result<Self::RawBytes, Self::Error>> + 'a, Self::Error>;

    /// Clear all data (careful operation)
    async fn clear(&self) -> Result<(), Self::Error>;

    /// Flush any pending writes to the storage medium
    async fn flush(&self) -> Result<(), Self::Error>;
}
