//! Compact, chunked journals of entity versions, grouped by an application key.
//!
//! A writer journals every staged entity version in operation order. `flush(key)` atomically commits
//! both the entity changes and the journal; dropping a writer discards its work.
//! Journal entries contain keys and version indices, never entity values.

mod chunk;
mod entry;
mod handle;
mod rollback;
mod write;

use crate::{
    Backend, Changes, Column, Serializable, XoriBuilder, XoriEngine, XoriError, XoriResult,
    backend::column::ColumnKind,
};
use futures::{Stream, TryStreamExt, stream};
use std::borrow::Cow;

pub use chunk::HistoryChunk;
use chunk::{chunk_key, prefix};
pub use entry::HistoryEntry;
pub use handle::HistoryEntityWriteHandle;
pub use write::HistoryWriter;

/// Maximum serialized value size of a history chunk (including framing).
/// A single reference exceeding this limit is rejected before staging changes.
#[derive(Debug, Clone, Copy)]
pub struct HistoryConfig {
    pub max_chunk_bytes: usize,
}

impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            max_chunk_bytes: 64 * 1024, // 64 KiB
        }
    }
}

/// Registered shared journal column. Register in the same schema order on reopen.
#[derive(Debug, Clone)]
pub struct History {
    column: Column,
    config: HistoryConfig,
}

impl History {
    pub fn register(
        builder: &mut XoriBuilder,
        name: impl Into<Cow<'static, str>>,
        config: HistoryConfig,
    ) -> Self {
        let column = builder.register_column(name, ColumnKind::Other, Default::default());
        Self { column, config }
    }

    pub fn column(&self) -> &Column {
        &self.column
    }

    /// Begin a staged batch. Only writes through this writer are recorded.
    pub fn writer<'a, B: Backend>(
        &self,
        engine: &'a mut XoriEngine<B>,
    ) -> XoriResult<HistoryWriter<'a, B>, B::Error> {
        if self.config.max_chunk_bytes < 2 {
            return Err(XoriError::InvalidHistoryConfig);
        }

        if !engine
            .backend
            .columns
            .get(&self.column.id())
            .is_some_and(|col| col.name() == self.column.name())
        {
            return Err(XoriError::UnknownColumn(self.column.id()));
        }

        Ok(HistoryWriter {
            engine,
            history: self.clone(),
            changes: Changes::default(),
            entries: Vec::new(),
        })
    }

    /// Fetch chunks only when polled, retaining the backend's raw value. Each
    /// returned chunk decodes its entries lazily as an iterator of results.
    /// Missing keys yield an empty stream; a missing continuation is an error.
    pub fn chunks<'a, B: Backend, K: Serializable>(
        &'a self,
        engine: &'a XoriEngine<B>,
        key: K,
    ) -> XoriResult<
        impl Stream<Item = XoriResult<HistoryChunk<B::RawBytes>, B::Error>> + 'a,
        B::Error,
    > {
        let prefix = prefix(&key.to_bytes()?);
        Ok(self.chunks_from_prefix(engine, &prefix))
    }

    /// Decode one reference per poll, fetching the next chunk only after the
    /// current one is exhausted. Dropping the stream does no additional work.
    pub fn entries<'a, B: Backend, K: Serializable>(
        &'a self,
        engine: &'a XoriEngine<B>,
        key: K,
    ) -> XoriResult<impl Stream<Item = XoriResult<HistoryEntry, B::Error>> + 'a, B::Error> {
        Ok(self
            .chunks(engine, key)?
            .map_ok(|chunk| stream::iter(chunk.map(|entry| entry.map_err(XoriError::from))))
            .try_flatten())
    }

    fn chunks_from_prefix<'a, B: Backend>(
        &'a self,
        engine: &'a XoriEngine<B>,
        prefix: &[u8],
    ) -> impl Stream<Item = XoriResult<HistoryChunk<B::RawBytes>, B::Error>> + use<'a, B> {
        stream::try_unfold(
            Some((chunk_key(prefix, 0), 0u64)),
            move |state| async move {
                let Some((mut key, index)) = state else {
                    return Ok(None);
                };
                match engine
                    .backend
                    .backend
                    .read(&self.column, key.as_slice())
                    .await?
                {
                    Some(bytes) => {
                        let chunk = HistoryChunk::new(bytes)?;
                        let next = if chunk.is_last() {
                            None
                        } else {
                            let index = index.checked_add(1).ok_or(XoriError::InvalidHistory)?;
                            let offset = key.len() - 8;
                            key[offset..].copy_from_slice(&index.to_be_bytes());
                            Some((key, index))
                        };
                        Ok(Some((chunk, next)))
                    }
                    None if index == 0 => Ok(None),
                    None => Err(XoriError::InvalidHistory),
                }
            },
        )
    }
}

#[cfg(test)]
mod tests;
