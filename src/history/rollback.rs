use super::{
    History, HistoryChunk,
    chunk::{chunk_key, decode_chunk_key, prefix},
};
use crate::{
    Backend, Changes, Serializable, XoriEngine, XoriError, XoriResult,
    engine::{IteratorDirection, IteratorMode},
};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};

impl History {
    /// Undo a journal in reverse operation order as one atomic batch.
    /// Undo newer history keys first. Returns false for a missing journal.
    /// Key allocation counters remain monotonic; new entity mappings are removed.
    pub async fn rollback<B: Backend, K: Serializable + Send + Sync>(
        &self,
        engine: &mut XoriEngine<B>,
        key: K,
    ) -> XoriResult<bool, B::Error> {
        let prefix = prefix(&key.to_bytes()?);
        let mut changes = Changes::default();
        let mut found = false;
        {
            let chunks = engine
                .backend
                .backend
                .iterator(
                    &self.column,
                    IteratorMode::Prefix(&prefix, IteratorDirection::Backward),
                )
                .await?;
            futures::pin_mut!(chunks);
            let mut expected = None;
            while let Some((key, bytes)) = chunks.try_next().await? {
                let (_, index) = decode_chunk_key(key.as_ref())?;
                let chunk = HistoryChunk::new(bytes)?;

                // The first chunk must terminate the journal; all preceding
                // chunks must be contiguous continuations ending at index zero.
                if found && expected != Some(index) || chunk.is_last() == found {
                    return Err(XoriError::InvalidHistory);
                }

                found = true;
                expected = index.checked_sub(1);
                let entries = chunk.collect::<Result<Vec<_>, _>>()?;
                for entry in entries.into_iter().rev() {
                    entry.rollback(engine, &mut changes).await?;
                }

                changes
                    .column_mut(&self.column)
                    .remove(chunk_key(&prefix, index));
            }
            if expected.is_some() {
                return Err(XoriError::InvalidHistory);
            }
        }

        if found {
            engine.apply_changes(changes).await?;
        }

        Ok(found)
    }

    /// Undo keys whose serialized bytes are >= `first`, newest first.
    /// Each key is an atomic commit, making interrupted rollback resumable.
    /// For topoheights use an order-preserving encoding such as big-endian u64.
    pub async fn rollback_from<B: Backend, K: Serializable + Send + Sync>(
        &self,
        engine: &mut XoriEngine<B>,
        first: K,
    ) -> XoriResult<usize, B::Error> {
        let start = prefix(&first.to_bytes()?);
        let mut count = 0;
        loop {
            let latest = {
                let entries = engine
                    .iterator_keys::<Bytes>(
                        &self.column,
                        IteratorMode::From(&start, IteratorDirection::Backward),
                    )
                    .await?;
                futures::pin_mut!(entries);
                entries.next().await.transpose()?
            };
            let Some(latest) = latest else {
                return Ok(count);
            };
            let (key, _) = decode_chunk_key(&latest)?;
            if !self.rollback(engine, key.as_slice()).await? {
                return Err(XoriError::InvalidHistory);
            }
            count += 1;
        }
    }
}
