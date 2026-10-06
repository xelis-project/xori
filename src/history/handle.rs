use super::HistoryWriter;
use crate::{Backend, Entity, Serializable, Version, XoriResult};
use std::marker::PhantomData;

/// Typed access to a HistoryWriter. Destructive deletes are deliberately absent:
/// use store_deleted so rollback can restore an earlier version without values
/// in the journal.
pub struct HistoryEntityWriteHandle<'writer, 'engine, E: Entity, B: Backend> {
    pub(super) writer: &'writer mut HistoryWriter<'engine, B>,
    pub(super) entity: PhantomData<E>,
}

impl<E: Entity, B: Backend> HistoryEntityWriteHandle<'_, '_, E, B> {
    pub async fn store<K: Serializable + Send + Sync>(
        &mut self,
        key: K,
        value: E,
    ) -> XoriResult<(), B::Error> {
        self.writer.store(key, value).await
    }

    pub async fn store_deleted<K: Serializable + Send + Sync>(
        &mut self,
        key: K,
    ) -> XoriResult<(), B::Error> {
        self.writer.store_deleted::<E, K>(key).await
    }

    pub async fn last_version<K: Serializable + Send + Sync>(
        &self,
        key: K,
    ) -> XoriResult<Option<Version>, B::Error> {
        self.writer.last_version::<E, K>(key).await
    }

    pub async fn read_at_version<K: Serializable + Send + Sync>(
        &self,
        key: K,
        version: Version,
    ) -> XoriResult<Option<E>, B::Error> {
        self.writer.read_at_version::<E, K>(key, version).await
    }
}
