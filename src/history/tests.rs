use super::{chunk::decode_chunk_key, *};

use crate::{
    BackendError, Entity, EntityConfig, MemoryBackend, Reader, ReaderError, Version, Writable,
    WriterError,
    backend::ColumnId,
    changes::ColumnChanges,
    engine::{IteratorDirection, IteratorMode},
};

use bytes::Bytes;
use futures::StreamExt;

#[derive(Clone, Debug, PartialEq)]
struct Balance(u64);

impl Entity for Balance {
    fn entity_name() -> &'static str {
        "history_balance"
    }
}

impl Serializable for Balance {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        self.0.write(writer)
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        Ok(Self(u64::read(reader)?))
    }

    fn size(&self) -> usize {
        8
    }
}

fn schema(indexed: bool, limit: usize) -> (XoriBuilder, History) {
    let mut builder = XoriBuilder::new()
        .register_entity::<Balance>(EntityConfig {
            key_indexing: indexed,
            prefix_length: None,
        })
        .register_entity::<u64>(EntityConfig::default());

    let history = History::register(
        &mut builder,
        "history",
        HistoryConfig {
            max_chunk_bytes: limit,
        },
    );

    (builder, history)
}

async fn lifecycle<B: Backend>(engine: &mut XoriEngine<B>, history: &History)
where
    B::Error: std::fmt::Debug,
{
    // Native and journaled writes share the entity format.
    engine
        .entity_handle_write::<Balance>()
        .unwrap()
        .store(1u64, Balance(10))
        .await
        .unwrap();

    {
        let mut writer = history.writer(engine).unwrap();
        writer
            .entity_handle_write::<Balance>()
            .unwrap()
            .store(1u64, Balance(20))
            .await
            .unwrap();
        writer.store(2u64, Balance(30)).await.unwrap();
        writer.store(1u64, 99u64).await.unwrap();

        assert_eq!(
            writer.last_version::<Balance, _>(1u64).await.unwrap(),
            Some(Version(1))
        );

        assert_eq!(
            writer
                .read_at_version::<Balance, _>(1u64, Version(1))
                .await
                .unwrap(),
            Some(Balance(20))
        );
        writer.flush(100u64).await.unwrap();

        assert!(writer.is_empty());
        writer.store_deleted::<Balance, _>(1u64).await.unwrap();
        writer.store(2u64, Balance(50)).await.unwrap();

        // Duplicate history IDs cannot overwrite journal entries or commit data.
        assert!(matches!(
            writer.flush(100u64).await,
            Err(XoriError::HistoryAlreadyExists)
        ));

        assert_eq!(writer.len(), 2);
        writer.flush(101u64).await.unwrap();
    }

    assert_eq!(
        engine
            .entity_handle_read::<Balance>()
            .unwrap()
            .read_at_version(&1u64, Version(2))
            .await
            .unwrap(),
        None
    );

    let chunks: Vec<_> = history
        .chunks(engine, 100u64)
        .unwrap()
        .map(|chunk| {
            chunk?
                .collect::<Result<Vec<_>, _>>()
                .map_err(XoriError::from)
        })
        .try_collect()
        .await
        .unwrap();

    assert_eq!(chunks.iter().map(Vec::len).sum::<usize>(), 3);
    assert!(chunks.len() > 1);
    assert!(matches!(
        history.rollback(engine, 100u64).await,
        Err(XoriError::HistoryConflict)
    ));

    assert_eq!(history.rollback_from(engine, 101u64).await.unwrap(), 1);
    assert_eq!(
        engine
            .entity_handle_read::<Balance>()
            .unwrap()
            .last_version(&1u64)
            .await
            .unwrap(),
        Some(Version(1))
    );

    assert!(history.rollback(engine, 100u64).await.unwrap());
    assert!(!history.rollback(engine, 100u64).await.unwrap());

    let reader = engine.entity_handle_read::<Balance>().unwrap();

    assert_eq!(reader.last_version(&1u64).await.unwrap(), Some(Version(0)));
    assert_eq!(
        reader.read_at_version(&1u64, Version(0)).await.unwrap(),
        Some(Balance(10))
    );

    assert_eq!(reader.last_version(&2u64).await.unwrap(), None);
    assert_eq!(
        engine
            .entity_handle_read::<u64>()
            .unwrap()
            .last_version(&1u64)
            .await
            .unwrap(),
        None
    );

    // Creating the removed key again works, including with key indexing.
    let mut writer = history.writer(engine).unwrap();
    writer.store(2u64, Balance(60)).await.unwrap();

    assert_eq!(
        writer.last_version::<Balance, _>(2u64).await.unwrap(),
        Some(Version(0))
    );
    writer.flush(100u64).await.unwrap();
}

#[tokio::test]
async fn memory_lifecycle_raw_and_indexed() {
    for indexed in [false, true] {
        let (builder, history) = schema(indexed, 16);
        let mut engine = builder.build::<MemoryBackend>(()).await.unwrap();
        lifecycle(&mut engine, &history).await;
    }
}

#[tokio::test]
async fn empty_drop_and_reused_writer() {
    let (builder, history) = schema(true, 16);
    let mut engine = builder.build::<MemoryBackend>(()).await.unwrap();

    {
        let mut writer = history.writer(&mut engine).unwrap();
        writer.store(1u64, Balance(10)).await.unwrap();
    }

    assert_eq!(
        engine
            .entity_handle_read::<Balance>()
            .unwrap()
            .last_version(&1u64)
            .await
            .unwrap(),
        None
    );
    history
        .writer(&mut engine)
        .unwrap()
        .flush(10u64)
        .await
        .unwrap();

    let chunks: Vec<_> = history
        .chunks(&engine, 10u64)
        .unwrap()
        .map(|chunk| {
            chunk?
                .collect::<Result<Vec<_>, _>>()
                .map_err(XoriError::from)
        })
        .try_collect()
        .await
        .unwrap();

    assert_eq!(chunks, vec![vec![]]);
    assert_eq!(history.rollback_from(&mut engine, 0u64).await.unwrap(), 1);
}

#[tokio::test]
async fn size_limits_and_failed_staging_leave_no_changes() {
    let (builder, history) = schema(true, 13); // 2 framing + column(1) + key(9) + version(1)
    let mut engine = builder.build::<MemoryBackend>(()).await.unwrap();
    let mut writer = history.writer(&mut engine).unwrap();

    assert!(matches!(
        writer.store("a key too long".to_owned(), Balance(10)).await,
        Err(XoriError::HistoryEntryTooLarge)
    ));

    assert!(writer.is_empty());
    assert!(writer.changes.columns.is_empty());
    assert!(matches!(
        writer.store_deleted::<Balance, _>(1u64).await,
        Err(XoriError::NoVersionAvailable)
    ));

    assert!(writer.changes.columns.is_empty());
    writer.store(1u64, Balance(10)).await.unwrap();
    writer.store(2u64, Balance(20)).await.unwrap();
    writer.flush(1u64).await.unwrap();
    drop(writer);

    let values: Vec<(Bytes, Bytes)> = engine
        .iterator(
            history.column(),
            IteratorMode::All(IteratorDirection::Forward),
        )
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(values.len(), 2);
    assert!(values.iter().all(|(_, value)| value.len() == 13));
    history.rollback_from(&mut engine, 1u64).await.unwrap();

    // Every mapping and payload is removed; only the monotonic allocator remains.
    let info = engine.entity_registry.get(Balance::entity_name()).unwrap();
    let rows: Vec<(Bytes, Bytes)> = engine
        .backend
        .backend
        .iterator(&info.column, IteratorMode::All(IteratorDirection::Forward))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert!(rows[0].0.is_empty());

    let keys: Vec<u64> = engine
        .entity_handle_read::<Balance>()
        .unwrap()
        .list_keys::<u64>()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert!(keys.is_empty());
}

#[test]
fn history_key_encoding_preserves_order_and_boundaries() {
    let keys: Vec<Vec<u8>> = vec![
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 255],
        vec![1],
        vec![1, 0],
        vec![255],
    ];

    let encoded: Vec<_> = keys.iter().map(|key| chunk_key(&prefix(key), 0)).collect();

    assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));

    for (key, encoded) in keys.iter().zip(&encoded) {
        assert_eq!(decode_chunk_key(encoded).unwrap(), (key.clone(), 0));
    }

    assert!(decode_chunk_key(&[0, 1]).is_err());
}

#[tokio::test]
async fn variable_length_history_keys_do_not_overlap() {
    let (builder, history) = schema(false, 16);
    let mut engine = builder.build::<MemoryBackend>(()).await.unwrap();
    let keys: &[&[u8]] = &[&[], &[0], &[0, 0], &[0, 255], &[1]];

    for key in keys {
        history
            .writer(&mut engine)
            .unwrap()
            .flush(*key)
            .await
            .unwrap();
    }

    assert_eq!(
        history
            .rollback_from(&mut engine, &[0, 0][..])
            .await
            .unwrap(),
        3
    );

    assert!(history.rollback(&mut engine, &[0][..]).await.unwrap());
    assert!(history.rollback(&mut engine, &[][..]).await.unwrap());
}

#[tokio::test]
async fn chunk_count_varint_boundary_and_missing_continuation() {
    let (builder, history) = schema(false, 2785); // 252 * 11 + 2, but not 253 * 11 + 4
    let mut engine = builder.build::<MemoryBackend>(()).await.unwrap();
    let mut writer = history.writer(&mut engine).unwrap();

    for key in 0..254u64 {
        writer.store(key, Balance(key)).await.unwrap();
    }
    writer.flush(1u64).await.unwrap();
    drop(writer);

    let chunks: Vec<_> = history
        .chunks(&engine, 1u64)
        .unwrap()
        .map(|chunk| {
            chunk?
                .collect::<Result<Vec<_>, _>>()
                .map_err(XoriError::from)
        })
        .try_collect()
        .await
        .unwrap();

    assert_eq!(
        chunks.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![252, 2]
    );

    let missing = chunk_key(&prefix(&1u64.to_bytes().unwrap()), 1);
    engine
        .delete(history.column(), missing.as_slice())
        .await
        .unwrap();

    assert!(matches!(
        history.rollback(&mut engine, 1u64).await,
        Err(XoriError::InvalidHistory)
    ));

    // No partial rollback when a later chunk is missing.
    assert_eq!(
        engine
            .entity_handle_read::<Balance>()
            .unwrap()
            .last_version(&0u64)
            .await
            .unwrap(),
        Some(Version(0))
    );
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn rocksdb_lifecycle_and_reopen() {
    use crate::{RocksDBBackend, RocksDBConfig};

    let dir = tempfile::tempdir().unwrap();

    {
        let (builder, history) = schema(true, 16);
        let mut engine = builder
            .build::<RocksDBBackend>(RocksDBConfig::new(dir.path()))
            .await
            .unwrap();
        lifecycle(&mut engine, &history).await;
    }

    let (builder, history) = schema(true, 16);
    let mut engine = builder
        .build::<RocksDBBackend>(RocksDBConfig::new(dir.path()))
        .await
        .unwrap();

    let chunks: Vec<_> = history
        .chunks(&engine, 100u64)
        .unwrap()
        .map(|chunk| {
            chunk?
                .collect::<Result<Vec<_>, _>>()
                .map_err(XoriError::from)
        })
        .try_collect()
        .await
        .unwrap();

    assert_eq!(chunks.iter().map(Vec::len).sum::<usize>(), 1);
    assert_eq!(
        engine
            .entity_handle_read::<Balance>()
            .unwrap()
            .read_at_version(&2u64, Version(0))
            .await
            .unwrap(),
        Some(Balance(60))
    );

    assert_eq!(history.rollback_from(&mut engine, 100u64).await.unwrap(), 1);
}

// Reject an entire batch to exercise retry and rollback error paths without
// relying on a particular disk failure mode.
struct FallibleBackend {
    inner: MemoryBackend,
    reject: std::sync::Arc<std::sync::atomic::AtomicBool>,
    reads: std::sync::atomic::AtomicUsize,
}

impl Backend for FallibleBackend {
    type Config = std::sync::Arc<std::sync::atomic::AtomicBool>;
    type Error = std::convert::Infallible;
    type RawBytes = Bytes;

    async fn open(
        reject: Self::Config,
        columns: &[Column],
    ) -> Result<Self, BackendError<Self::Error>> {
        Ok(Self {
            inner: MemoryBackend::open((), columns).await?,
            reject,
            reads: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    async fn open_column(&mut self, column: &Column) -> Result<(), BackendError<Self::Error>> {
        self.inner.open_column(column).await
    }

    async fn write<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(
        &mut self,
        column: &Column,
        key: K,
        value: V,
    ) -> Result<(), BackendError<Self::Error>> {
        self.inner.write(column, key, value).await
    }

    async fn read<K: Serializable + Send + Sync>(
        &self,
        column: &Column,
        key: K,
    ) -> Result<Option<Bytes>, BackendError<Self::Error>> {
        self.reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.read(column, key).await
    }

    async fn delete<K: Serializable + Send + Sync>(
        &mut self,
        column: &Column,
        key: K,
    ) -> Result<(), BackendError<Self::Error>> {
        self.inner.delete(column, key).await
    }

    async fn exists<K: Serializable + Send + Sync>(
        &self,
        column: &Column,
        key: K,
    ) -> Result<bool, BackendError<Self::Error>> {
        self.inner.exists(column, key).await
    }

    async fn iterator<'a>(
        &'a self,
        column: &'a Column,
        mode: IteratorMode<'a>,
    ) -> Result<
        impl Stream<Item = Result<(Bytes, Bytes), BackendError<Self::Error>>> + 'a,
        BackendError<Self::Error>,
    > {
        self.inner.iterator(column, mode).await
    }

    async fn clear(&mut self) -> Result<(), BackendError<Self::Error>> {
        self.inner.clear().await
    }

    async fn flush(&self) -> Result<(), BackendError<Self::Error>> {
        self.inner.flush().await
    }

    async fn write_batch<'a, I: Iterator<Item = (&'a Column, ColumnChanges)> + Send + 'a>(
        &mut self,
        changes: I,
    ) -> Result<(), BackendError<Self::Error>> {
        if self.reject.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(BackendError::Unsupported);
        }

        self.inner.write_batch(changes).await
    }
}

#[tokio::test]
async fn failed_atomic_commit_and_rollback_are_retryable() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    let reject = Arc::new(AtomicBool::new(true));
    let (builder, history) = schema(true, 16);
    let mut engine = builder
        .build::<FallibleBackend>(reject.clone())
        .await
        .unwrap();

    let mut writer = history.writer(&mut engine).unwrap();
    writer.store(1u64, Balance(10)).await.unwrap();
    writer.store(2u64, Balance(20)).await.unwrap();

    assert!(writer.flush(1u64).await.is_err());
    assert_eq!(writer.len(), 2);
    assert_eq!(
        writer
            .engine
            .entity_handle_read::<Balance>()
            .unwrap()
            .last_version(&1u64)
            .await
            .unwrap(),
        None
    );

    assert!(
        history
            .chunks(writer.engine, 1u64)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    reject.store(false, Ordering::Relaxed);
    writer.flush(1u64).await.unwrap();
    drop(writer);
    reject.store(true, Ordering::Relaxed);

    assert!(history.rollback(&mut engine, 1u64).await.is_err());
    assert_eq!(
        engine
            .entity_handle_read::<Balance>()
            .unwrap()
            .last_version(&1u64)
            .await
            .unwrap(),
        Some(Version(0))
    );

    assert_eq!(
        history
            .chunks(&engine, 1u64)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        2
    );
    reject.store(false, Ordering::Relaxed);

    assert!(history.rollback(&mut engine, 1u64).await.unwrap());
}

#[tokio::test]
async fn invalid_configuration_is_rejected() {
    let (builder, history) = schema(false, 1);
    let mut engine = builder.build::<MemoryBackend>(()).await.unwrap();

    assert!(matches!(
        history.writer(&mut engine),
        Err(XoriError::InvalidHistoryConfig)
    ));
}

#[test]
fn borrowed_chunks_preserve_the_persisted_format() {
    let entry = HistoryEntry {
        column: ColumnId(3),
        key: vec![7, 8],
        version: Version(253),
    };

    let chunks: Vec<_> = chunk::pack(std::iter::once(&entry), 64).collect();

    assert_eq!(chunks.len(), 1);

    let bytes = chunks[0].to_bytes().unwrap();

    assert_eq!(bytes.as_ref(), &[1, 1, 3, 2, 7, 8, 253, 0, 253]);

    let decoded = HistoryChunk::new(bytes).unwrap();

    assert!(decoded.is_last());
    assert_eq!(decoded.collect::<Result<Vec<_>, _>>().unwrap(), vec![entry]);
}

#[tokio::test]
async fn history_reads_only_fetch_chunks_when_polled() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    let (builder, history) = schema(false, 24); // Two 11-byte entries per chunk.
    let mut engine = builder
        .build::<FallibleBackend>(Arc::new(AtomicBool::new(false)))
        .await
        .unwrap();

    let mut writer = history.writer(&mut engine).unwrap();

    for key in 0..5u64 {
        writer.store(key, Balance(key)).await.unwrap();
    }
    writer.flush(7u64).await.unwrap();
    drop(writer);

    let reads = &engine.backend.backend.reads;
    reads.store(0, Ordering::Relaxed);

    {
        let entries = history.entries(&engine, 7u64).unwrap();
        futures::pin_mut!(entries);

        assert_eq!(reads.load(Ordering::Relaxed), 0);
        assert_eq!(
            entries.try_next().await.unwrap().unwrap().key,
            0u64.to_be_bytes()
        );

        assert_eq!(reads.load(Ordering::Relaxed), 1);
        assert_eq!(
            entries.try_next().await.unwrap().unwrap().key,
            1u64.to_be_bytes()
        );

        assert_eq!(reads.load(Ordering::Relaxed), 1);
        assert_eq!(
            entries.try_next().await.unwrap().unwrap().key,
            2u64.to_be_bytes()
        );

        assert_eq!(reads.load(Ordering::Relaxed), 2);

        // Drop midway through a chunk: the final chunk must never be fetched.
    }

    assert_eq!(reads.load(Ordering::Relaxed), 2);

    reads.store(0, Ordering::Relaxed);

    let chunks = history.chunks(&engine, 7u64).unwrap();
    futures::pin_mut!(chunks);

    assert_eq!(reads.load(Ordering::Relaxed), 0);
    drop(chunks.try_next().await.unwrap().unwrap());

    assert_eq!(reads.load(Ordering::Relaxed), 1);

    let entries = history.entries(&engine, 99u64).unwrap();

    assert!(entries.try_collect::<Vec<_>>().await.unwrap().is_empty());
}

#[tokio::test]
async fn late_decode_error_is_lazy_and_rollback_stays_atomic() {
    let (builder, history) = schema(true, 64);
    let mut engine = builder.build::<MemoryBackend>(()).await.unwrap();
    let mut writer = history.writer(&mut engine).unwrap();
    writer.store(1u64, Balance(10)).await.unwrap();
    writer.store(2u64, Balance(20)).await.unwrap();
    writer.flush(1u64).await.unwrap();
    drop(writer);

    let key = chunk_key(&prefix(&1u64.to_bytes().unwrap()), 0);
    let mut damaged = engine
        .backend
        .backend
        .read(history.column(), key.as_slice())
        .await
        .unwrap()
        .unwrap()
        .to_vec();
    damaged.pop(); // Remove only the second entry's version.
    engine
        .write(history.column(), key.as_slice(), damaged.as_slice())
        .await
        .unwrap();

    {
        let chunks = history.chunks(&engine, 1u64).unwrap();
        futures::pin_mut!(chunks);

        let mut chunk = chunks.try_next().await.unwrap().unwrap();

        assert!(chunk.is_last());
        assert_eq!(chunk.next().unwrap().unwrap().key, 1u64.to_be_bytes());
        assert!(chunk.next().unwrap().is_err());
        assert!(chunk.next().is_none());
    }
    {
        let entries = history.entries(&engine, 1u64).unwrap();
        futures::pin_mut!(entries);

        assert!(entries.try_next().await.unwrap().is_some());
        assert!(entries.try_next().await.is_err());
    }

    assert!(history.rollback(&mut engine, 1u64).await.is_err());

    let reader = engine.entity_handle_read::<Balance>().unwrap();

    for key in [1u64, 2] {
        assert_eq!(reader.last_version(&key).await.unwrap(), Some(Version(0)));
    }

    assert!(
        engine
            .backend
            .backend
            .exists(history.column(), key.as_slice())
            .await
            .unwrap()
    );
}

#[test]
fn lazy_chunks_reject_invalid_framing_and_trailing_bytes() {
    for bytes in [
        &[][..],
        &[1],
        &[1, 1],
        &[0, 0],
        &[1, 0, 7],
        &[1, 255, 255, 255, 255, 255, 255, 255, 255, 255],
    ] {
        assert!(HistoryChunk::new(bytes).is_err());
    }

    // Valid entry (column 0, empty key, version 0), followed by garbage.
    let mut chunk = HistoryChunk::new(&[1, 1, 0, 0, 0, 7][..]).unwrap();

    assert!(chunk.next().unwrap().is_err());
    assert!(chunk.next().is_none());

    let mut empty = HistoryChunk::new(&[1, 0][..]).unwrap();

    assert!(empty.is_last());
    assert!(empty.next().is_none());
}

#[test]
fn packing_does_not_serialize_or_scan_future_chunks() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Clone)]
    struct CountedEntry {
        entry: HistoryEntry,
        writes: Arc<AtomicUsize>,
    }

    impl Serializable for CountedEntry {
        fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            self.entry.write(writer)
        }

        fn read(_: &mut Reader) -> Result<Self, ReaderError> {
            Err(ReaderError::NotSerializable)
        }

        fn size(&self) -> usize {
            self.entry.size()
        }
    }

    let visited = Arc::new(AtomicUsize::new(0));
    let writes = Arc::new(AtomicUsize::new(0));
    let entries = (0..100u64).map({
        let visited = visited.clone();
        let writes = writes.clone();
        move |key| {
            visited.fetch_add(1, Ordering::Relaxed);
            CountedEntry {
                entry: HistoryEntry {
                    column: ColumnId(0),
                    key: key.to_be_bytes().to_vec(),
                    version: Version(0),
                },
                writes: writes.clone(),
            }
        }
    });

    let mut chunks = chunk::pack(entries, 13);

    assert_eq!(visited.load(Ordering::Relaxed), 0);

    let first = chunks.next().unwrap();

    assert_eq!(visited.load(Ordering::Relaxed), 2); // One entry plus lookahead.
    assert_eq!(writes.load(Ordering::Relaxed), 0);

    let bytes = first.to_bytes().unwrap();

    assert_eq!(bytes.len(), 13);
    assert_eq!(writes.load(Ordering::Relaxed), 1);
    assert_eq!(visited.load(Ordering::Relaxed), 3); // Serialize the bounded view.
    drop(chunks);

    assert_eq!(writes.load(Ordering::Relaxed), 1);
}

async fn repeated_changes<B: Backend>(engine: &mut XoriEngine<B>, history: &History)
where
    B::Error: std::fmt::Debug,
{
    engine
        .entity_handle_write::<Balance>()
        .unwrap()
        .store(9u64, Balance(5))
        .await
        .unwrap();

    let mut writer = history.writer(engine).unwrap();
    writer.store(9u64, Balance(10)).await.unwrap();
    writer.store(1u64, Balance(20)).await.unwrap();
    writer.store_deleted::<Balance, _>(9u64).await.unwrap();
    writer.store_deleted::<Balance, _>(1u64).await.unwrap();
    writer.store(9u64, Balance(30)).await.unwrap();
    writer.store(1u64, Balance(40)).await.unwrap();

    assert_eq!(writer.len(), 6);
    assert_eq!(
        writer.last_version::<Balance, _>(9u64).await.unwrap(),
        Some(Version(3))
    );

    assert_eq!(
        writer
            .read_at_version::<Balance, _>(9u64, Version(2))
            .await
            .unwrap(),
        None
    );
    writer.flush(7u64).await.unwrap();
    drop(writer);

    let entries: Vec<_> = history
        .entries(engine, 7u64)
        .unwrap()
        .try_collect()
        .await
        .unwrap();

    assert_eq!(
        entries
            .iter()
            .map(|entry| (u64::from_bytes(&entry.key).unwrap(), entry.version))
            .collect::<Vec<_>>(),
        vec![
            (9, Version(1)),
            (1, Version(0)),
            (9, Version(2)),
            (1, Version(1)),
            (9, Version(3)),
            (1, Version(2)),
        ]
    );

    assert!(history.rollback(engine, 7u64).await.unwrap());

    let reader = engine.entity_handle_read::<Balance>().unwrap();

    assert_eq!(reader.last_version(&9u64).await.unwrap(), Some(Version(0)));
    assert_eq!(
        reader.read_at_version(&9u64, Version(0)).await.unwrap(),
        Some(Balance(5))
    );

    assert_eq!(reader.last_version(&1u64).await.unwrap(), None);

    for version in 1..=3 {
        assert!(matches!(
            reader.read_at_version(&9u64, Version(version)).await,
            Err(XoriError::VersionNotFound)
        ));
    }

    // A recreated indexed key must receive a fresh ID and a fresh version chain.
    engine
        .entity_handle_write::<Balance>()
        .unwrap()
        .store(1u64, Balance(50))
        .await
        .unwrap();

    assert_eq!(
        engine
            .entity_handle_read::<Balance>()
            .unwrap()
            .read_at_version(&1u64, Version(0))
            .await
            .unwrap(),
        Some(Balance(50))
    );
}

#[tokio::test]
async fn repeated_changes_rollback_in_reverse_order() {
    for indexed in [false, true] {
        // Exercise repeated keys within one chunk and across chunk boundaries.
        for limit in [16, 64] {
            let (builder, history) = schema(indexed, limit);
            let mut engine = builder.build::<MemoryBackend>(()).await.unwrap();
            repeated_changes(&mut engine, &history).await;
        }
    }
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn rocksdb_repeated_changes_rollback_in_reverse_order() {
    use crate::{RocksDBBackend, RocksDBConfig};

    for indexed in [false, true] {
        for limit in [16, 64] {
            let dir = tempfile::tempdir().unwrap();
            let (builder, history) = schema(indexed, limit);
            let mut engine = builder
                .build::<RocksDBBackend>(RocksDBConfig::new(dir.path()))
                .await
                .unwrap();
            repeated_changes(&mut engine, &history).await;
        }
    }
}

#[tokio::test]
async fn missing_chunks_leave_repeated_changes_untouched() {
    for missing in 0..3 {
        let (builder, history) = schema(true, 16);
        let mut engine = builder.build::<MemoryBackend>(()).await.unwrap();
        let mut writer = history.writer(&mut engine).unwrap();

        for value in 0..3 {
            writer.store(1u64, Balance(value)).await.unwrap();
        }
        writer.flush(7u64).await.unwrap();
        drop(writer);

        let key = chunk_key(&prefix(&7u64.to_bytes().unwrap()), missing);
        engine
            .backend
            .backend
            .delete(history.column(), key.as_slice())
            .await
            .unwrap();

        assert!(matches!(
            history.rollback(&mut engine, 7u64).await,
            Err(XoriError::InvalidHistory)
        ));

        let reader = engine.entity_handle_read::<Balance>().unwrap();

        assert_eq!(reader.last_version(&1u64).await.unwrap(), Some(Version(2)));

        for version in 0..3 {
            assert_eq!(
                reader
                    .read_at_version(&1u64, Version(version))
                    .await
                    .unwrap(),
                Some(Balance(version))
            );
        }
    }
}
