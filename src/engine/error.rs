use std::fmt::Display;

use thiserror::Error;

use crate::{ReaderError, WriterError, backend::{BackendError, ColumnId}};


/// Database errors
#[derive(Error, Debug)]
pub enum XoriError<T: Display> {
    #[error("A snapshot is currently active, cannot perform this operation")]
    SnapshotActive,
    #[error("Column already registered")]
    ColumnAlreadyRegistered,
    #[error("Unknown column: {0}")]
    UnknownColumn(ColumnId),
    #[error("Key not found")]
    KeyNotFound,
    #[error("Column not found")]
    ColumnNotFound,
    #[error("Version not found")]
    VersionNotFound,
    #[error("No version available at or before the specified version")]
    NoVersionAvailable,
    #[error("Backend error: {0}")]
    Backend(#[from] BackendError<T>),
}

impl<T: Display> From<WriterError> for XoriError<T> {
    fn from(err: WriterError) -> Self {
        XoriError::Backend(BackendError::Writer(err))
    }
}

impl<T: Display> From<ReaderError> for XoriError<T> {
    fn from(err: ReaderError) -> Self {
        XoriError::Backend(BackendError::Reader(err))
    }
}

/// Result type for backend operations
pub type XoriResult<T, E> = std::result::Result<T, XoriError<E>>;
