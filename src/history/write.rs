use super::{
    History, HistoryEntityWriteHandle,
    chunk::{self, chunk_key, prefix},
    entry::EntryRef,
};
use crate::{
    Backend, Changes, Column, Entity, EntityMetadata, KeyIndex, Serializable, VarUint, Version,
    VersionedKey, XoriEngine, XoriError, XoriResult, backend::ColumnId, builder::EntityInfo,
    changes::ColumnChanges,
};
use bytes::Bytes;
use std::{borrow::Cow, marker::PhantomData};

/// Stages versioned writes and their references until `flush(history_key)`.
/// Every store/deletion is recorded in staging order, including repeated keys.
/// All pending data remains in memory; chunking bounds journal value sizes,
/// not the total memory used by an atomic batch.
pub struct HistoryWriter<'a, B: Backend> {
    pub(super) engine: &'a mut XoriEngine<B>,
    pub(super) history: History,
    pub(super) changes: Changes,
    pub(super) entries: Vec<(ColumnId, Bytes, Version)>,
}

impl<'engine, B: Backend> HistoryWriter<'engine, B> {
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The same store/store_deleted shape as an engine entity write handle,
    /// with automatic history registration and staged reads.
    pub fn entity_handle_write<E: Entity>(
        &mut self,
    ) -> Option<HistoryEntityWriteHandle<'_, 'engine, E, B>> {
        self.engine
            .entity_registry
            .contains_key(E::entity_name())
            .then_some(HistoryEntityWriteHandle {
                writer: self,
                entity: PhantomData,
            })
    }

    fn info<E: Entity>(&self) -> XoriResult<&EntityInfo, B::Error> {
        self.engine
            .entity_registry
            .get(E::entity_name())
            .ok_or(XoriError::UnknownEntity(E::entity_name()))
    }

    async fn read_pending<V: Serializable + Send + Sync>(
        &self,
        column: &Column,
        key: &[u8],
    ) -> XoriResult<Option<V>, B::Error> {
        self.engine
            .read_with_changes(&self.changes, column, key)
            .await
    }

    async fn mapped_key<'a>(
        &self,
        info: &EntityInfo,
        key: &'a [u8],
    ) -> XoriResult<Option<Cow<'a, [u8]>>, B::Error> {
        match &info.key_index_column {
            Some(columns) => self
                .read_pending::<KeyIndex>(&columns.key_to_id, key)
                .await?
                .map(|id| {
                    id.to_bytes()
                        .map(|bytes| Cow::Owned(bytes.into_vec()))
                        .map_err(XoriError::from)
                })
                .transpose(),
            None => Ok(Some(Cow::Borrowed(key))),
        }
    }

    /// Store one new entity version, automatically registering its reference.
    pub async fn store<E: Entity, K: Serializable + Send + Sync>(
        &mut self,
        key: K,
        value: E,
    ) -> XoriResult<(), B::Error> {
        self.stage::<E, K>(key, Some(value)).await
    }

    /// Append a versioned deletion, which rollback can undo without a before-image.
    pub async fn store_deleted<E: Entity, K: Serializable + Send + Sync>(
        &mut self,
        key: K,
    ) -> XoriResult<(), B::Error> {
        self.stage::<E, K>(key, None).await
    }

    async fn stage<E: Entity, K: Serializable + Send + Sync>(
        &mut self,
        key: K,
        value: Option<E>,
    ) -> XoriResult<(), B::Error> {
        let info = self.info::<E>()?;
        let column = info.column.id();
        let raw = Bytes::from(key.to_bytes()?.into_vec());
        // Serialize first, then prepare the whole operation locally. Any failure
        // must leave the writer usable without partially staged mappings/data.
        let data = Bytes::from(value.to_bytes()?.into_vec());
        let mut operation = Changes::default();
        let mapped = self
            .prepare_key(info, &raw, value.is_none(), &mut operation)
            .await?;
        let previous = self.read_pending::<Version>(&info.column, &mapped).await?;
        if previous.is_none() && value.is_none() {
            return Err(XoriError::NoVersionAvailable);
        }
        let version = match previous {
            Some(previous) => Version(previous.0.checked_add(1).ok_or(XoriError::IndexExhausted)?),
            None => Version::default(),
        };
        let entry = EntryRef {
            column,
            key: &raw,
            version,
        };
        // Boolean last marker + one-byte entry count + reference.
        if entry.size() > self.history.config.max_chunk_bytes - 2 {
            return Err(XoriError::HistoryEntryTooLarge);
        }
        let versioned = VersionedKey {
            key: mapped.as_ref(),
            version,
        };
        operation
            .column_mut(&info.column)
            .insert(Bytes::from(versioned.to_bytes()?.into_vec()), data);
        operation
            .column_mut(&info.column)
            .insert(mapped, Bytes::from(version.to_bytes()?.into_vec()));
        for (column, changes) in operation.columns {
            self.changes
                .columns
                .entry(column)
                .or_default()
                .entries
                .extend(changes.entries);
        }
        self.entries.push((column, raw, version));
        Ok(())
    }

    /// Keep key allocation local until the complete entity write is validated.
    async fn prepare_key(
        &self,
        info: &EntityInfo,
        raw: &Bytes,
        deleted: bool,
        operation: &mut Changes,
    ) -> XoriResult<Bytes, B::Error> {
        if let Some(mapped) = self.mapped_key(info, raw).await? {
            return Ok(match mapped {
                Cow::Borrowed(_) => raw.clone(),
                Cow::Owned(bytes) => Bytes::from(bytes),
            });
        }
        if deleted {
            return Err(XoriError::NoVersionAvailable);
        }

        let columns = info
            .key_index_column
            .as_ref()
            .expect("Only indexed keys need allocation");
        let mut metadata = self
            .read_pending::<EntityMetadata>(&info.column, &[])
            .await?
            .unwrap_or_default();
        let id = KeyIndex(VarUint(metadata.keys_count));
        metadata.keys_count = metadata
            .keys_count
            .checked_add(1)
            .ok_or(XoriError::IndexExhausted)?;
        let mapped = Bytes::from(id.to_bytes()?.into_vec());
        operation
            .column_mut(&columns.key_to_id)
            .insert(raw.clone(), mapped.clone());
        operation
            .column_mut(&columns.id_to_key)
            .insert(mapped.clone(), raw.clone());
        operation
            .column_mut(&info.column)
            .insert(Bytes::new(), Bytes::from(metadata.to_bytes()?.into_vec()));
        Ok(mapped)
    }

    /// Latest index, including this writer's pending changes.
    pub async fn last_version<E: Entity, K: Serializable + Send + Sync>(
        &self,
        key: K,
    ) -> XoriResult<Option<Version>, B::Error> {
        let info = self.info::<E>()?;
        match self.mapped_key(info, &key.to_bytes()?).await? {
            Some(mapped) => self.read_pending(&info.column, &mapped).await,
            None => Ok(None),
        }
    }

    pub async fn read_at_version<E: Entity, K: Serializable + Send + Sync>(
        &self,
        key: K,
        version: Version,
    ) -> XoriResult<Option<E>, B::Error> {
        let info = self.info::<E>()?;
        let raw = key.to_bytes()?;
        let Some(mapped) = self.mapped_key(info, &raw).await? else {
            return Ok(None);
        };
        let versioned = VersionedKey {
            key: mapped.as_ref(),
            version,
        };
        self.read_pending::<Option<E>>(&info.column, &versioned.to_bytes()?)
            .await?
            .ok_or(XoriError::VersionNotFound)
    }

    /// Commit pending data and chunked references under a unique application key.
    /// Empty batches write one empty chunk. On error the pending batch is retained
    /// for retry; on success the writer can start another batch. This commits a
    /// database batch, not a backend memtable flush or a forced fsync.
    pub async fn flush<K: Serializable + Send + Sync>(
        &mut self,
        key: K,
    ) -> XoriResult<(), B::Error> {
        let prefix = prefix(&key.to_bytes()?);
        let first = chunk_key(&prefix, 0);
        if self
            .engine
            .backend
            .backend
            .exists(&self.history.column, first.as_slice())
            .await?
        {
            return Err(XoriError::HistoryAlreadyExists);
        }
        let entries = self.entries.iter().map(|(column, key, version)| EntryRef {
            column: *column,
            key: key.as_ref(),
            version: *version,
        });
        // Serialize the journal before committing anything. Retain the staged
        // entity changes so serialization/backend failures can be retried.
        let mut journal = ColumnChanges::default();
        for (index, chunk) in chunk::pack(entries, self.history.config.max_chunk_bytes).enumerate()
        {
            journal.insert(
                Bytes::from(chunk_key(&prefix, index as u64)),
                Bytes::from(chunk.to_bytes()?.into_vec()),
            );
        }

        let backend = &mut self.engine.backend;
        let changes = self
            .changes
            .columns
            .iter()
            .map(|(id, changes)| {
                let column = backend
                    .columns
                    .get(id)
                    .expect("Staged columns are registered");
                (column, changes.clone())
            })
            .chain(std::iter::once((&self.history.column, journal)));
        backend.backend.write_batch(changes).await?;

        self.changes = Changes::default();
        self.entries.clear();

        Ok(())
    }
}
