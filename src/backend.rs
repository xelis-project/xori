pub mod memory;
pub mod column;

#[cfg(feature = "rocksdb")]
pub mod rocksdb;


use futures::Stream;
use thiserror::Error;
use std::fmt::Display;
use crate::{ReaderError, Serializable, WriterError, engine::IteratorMode};

pub use memory::MemoryBackend;
pub use column::{Column, ColumnId, ColumnKind, ColumnInner, ColumnProperties};
#[cfg(feature = "rocksdb")]
pub use rocksdb::RocksDBBackend;

#[derive(Debug, Error)]
pub enum BackendError<B: Display> {
    #[error(transparent)]
    Writer(#[from] WriterError),
    #[error(transparent)]
    Reader(#[from] ReaderError),
    #[error("Operation not supported by this backend")]
    Unsupported,
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
    fn open_column(&mut self, column: &Column) -> impl Future<Output = Result<(), BackendError<Self::Error>>> + Send;

    /// Write data to the database
    fn write<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(&mut self, column: &Column, key: K, data: V) -> impl Future<Output = Result<(), BackendError<Self::Error>>> + Send;

    /// Delete a key entirely
    fn delete<K: Serializable + Send + Sync>(&mut self, column: &Column, key: K) -> impl Future<Output = Result<(), BackendError<Self::Error>>> + Send;

    /// Read data at the latest version
    fn read<K: Serializable + Send + Sync>(&self, column: &Column, key: K) -> impl Future<Output = Result<Option<Self::RawBytes>, BackendError<Self::Error>>> + Send;

    /// Get all historical entries for a key (entire history)
    fn iterator<'a>(&'a self, column: &'a Column, mode: IteratorMode<'a>) -> impl Future<Output = Result<impl Stream<Item = Result<(Self::RawBytes, Self::RawBytes), BackendError<Self::Error>>> + 'a, BackendError<Self::Error>>> + Send + 'a;

    /// Check if a key exists at current version
    fn exists<K: Serializable + Send + Sync>(&self, column: &Column, key: K) -> impl Future<Output = Result<bool, BackendError<Self::Error>>> + Send;

    /// Clear all data (careful operation)
    fn clear(&mut self) -> impl Future<Output = Result<(), BackendError<Self::Error>>> + Send;

    /// Flush any pending writes to the storage medium
    fn flush(&self) -> impl Future<Output = Result<(), BackendError<Self::Error>>> + Send;
}
