use std::fmt::Display;
use thiserror::Error;

use crate::engine::XoriError;

/// Errors specific to DAG operations
#[derive(Error, Debug)]
pub enum DagError<T: Display> {
    #[error("Entry already exists in the DAG")]
    EntryAlreadyExists,
    #[error("Predecessor entry not found")]
    PredecessorNotFound,
    #[error("Entry not found")]
    EntryNotFound,
    #[error("Backend error: {0}")]
    Backend(XoriError<T>),
}

impl<E: Into<XoriError<T>>, T: Display> From<E> for DagError<T> {
    fn from(error: E) -> Self {
        DagError::Backend(error.into())
    }
}

/// Result type for DAG operations
pub type DagResult<T, E> = std::result::Result<T, DagError<E>>;
