mod error;

use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

pub use error::{DagError, DagResult};
use futures::TryStreamExt;

use crate::{SerializedBytes, Snapshot, XoriBuilder, XoriEngine};
use crate::backend::{Backend, Column, ColumnKind};
use crate::engine::{IteratorDirection, IteratorMode, XoriBackend};
use crate::serde::{Reader, ReaderError, Serializable, Writable, WriterError};


/// A single entry (node) in the DAG.
///
/// Generic over `K`, the user-chosen key type for identifying entries (e.g. block hashes).
/// Each entry stores:
/// - Its predecessors (tips it builds upon)
/// - Its order (for deterministic conflict resolution during traversal)
/// - The changes (diffs) it introduces, organized by column
#[derive(Debug, Clone)]
pub struct DagEntry<K: DagKey> {
    /// Predecessor entries this entry builds upon (parent tips)
    predecessors: Vec<K>,
}

impl<K: DagKey> DagEntry<K> {
    /// Get the predecessors of this entry
    #[inline]
    pub fn predecessors(&self) -> &[K] {
        &self.predecessors
    }
}

/// Trait bounds required for DAG entry keys.
/// The user provides their own key type (e.g. `[u8; 32]` for block hashes).
pub trait DagKey: Serializable + Clone + Eq + Hash + Ord + Debug + Send + Sync {}

// Blanket implementation: any type satisfying the bounds is a DagKey
impl<T> DagKey for T
where
    T: Serializable + Clone + Eq + Hash + Ord + Debug + Send + Sync,
{}

impl<K: DagKey> Serializable for DagEntry<K> {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        self.predecessors.write(writer)
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        let predecessors = Vec::<K>::read(reader)?;
        Ok(DagEntry { predecessors })
    }

    fn size(&self) -> usize {
        self.predecessors.size()
    }
}

/// Result of reading a key from the DAG, as seen from a given set of tips.
/// K is the key at which the value was found (could be one of the tips or an ancestor).
/// V is the value type stored in the DAG (e.g. block header data).
pub enum ReadResult<V, K> {
    Stored(V, K),
    Deleted(K),
    Absent,
}

/// Composite key for storing individual changes in the backend.
///
/// Each change is stored as a separate record keyed by
/// `[entry_key_bytes | column | data_key_bytes]`.
///
/// This allows point-lookups during DAG traversal and prefix iteration
/// for cleanup when removing an entry.
struct DagChangeKey<'a> {
    column: &'a Column,
    entry_key_bytes: &'a [u8],
    data_key_bytes: &'a [u8],
}

impl Serializable for DagChangeKey<'_> {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        self.column.write(writer)?;
        writer.extend_bytes(self.entry_key_bytes);
        writer.extend_bytes(self.data_key_bytes);
        Ok(())
    }

    fn read(_: &mut Reader) -> Result<Self, ReaderError> {
        Err(ReaderError::NotSerializable)
    }

    fn size(&self) -> usize {
        self.entry_key_bytes.len() + self.column.size() + self.data_key_bytes.len()
    }
}

/// The core DAG structure.
///
/// Stores entries in a backend-agnostic way. Each entry is identified by
/// a user-chosen key `K` and contains a set of changes (diffs) plus
/// references to its predecessors.
///
/// The `DagState` is parameterized by:
/// - `K`: The key type for identifying entries (e.g. `[u8; 32]` for block hashes)
/// - `B`: The storage backend (e.g. `MemoryBackend` or a disk-based backend)
pub struct DagState<K: DagKey, B: Backend> {
    /// The backend engine for persisting entries
    engine: XoriEngine<B>,
    /// Column used to store DagEntry metadata (predecessors + order), keyed by K
    entries_column: Column,
    /// Column used to store individual changes, keyed by composite `(K, Column, data_key)`
    changes_column: Column,
    /// Phantom data for the key type
    _phantom: PhantomData<K>,
}

impl<K: DagKey, B: Backend> DagState<K, B> {
    /// Create a new DagState with the given backend.
    pub async fn new(mut builder: XoriBuilder, backend: B) -> DagResult<Self, B::Error> {
        let entries_column = builder.register_column(ColumnKind::Other);
        let changes_column = builder.register_column(ColumnKind::Other);

        let engine = builder.build(backend).await?;

        Ok(Self {
            engine,
            entries_column,
            changes_column,
            _phantom: PhantomData,
        })
    }

    /// Get a reference to the underlying engine for direct access.
    #[inline(always)]
    pub fn engine(&self) -> &XoriEngine<B> {
        &self.engine
    }

    /// Get a mutable reference to the underlying engine for direct access.
    #[inline(always)]
    pub fn engine_mut(&mut self) -> &mut XoriEngine<B> {
        &mut self.engine
     }

    /// Add a new entry to the DAG.
    ///
    /// The entry metadata (predecessors + order) is stored in the entries column,
    /// while each individual change is stored as a separate record in the changes column.
    ///
    /// # Arguments
    /// - `key`: The unique identifier for this entry
    /// - `predecessors`: The predecessor entry keys this entry builds upon
    /// - `order`: Topological order for conflict resolution (higher = more recent)
    /// - `changes`: The changes this entry introduces, organized by column
    ///
    /// # Errors
    /// - `DagError::EntryAlreadyExists` if an entry with this key already exists
    /// - `DagError::PredecessorNotFound` if any predecessor doesn't exist
    pub async fn add_entry(
        &mut self,
        key: K,
        predecessors: Vec<K>,
        snapshot: Snapshot,
    ) -> DagResult<(), B::Error> {
        // Check that entry doesn't already exist
        if self.has_entry(&key).await? {
            return Err(DagError::EntryAlreadyExists);
        }

        // Verify all predecessors exist
        for pred in &predecessors {
            if !self.has_entry(pred).await? {
                return Err(DagError::PredecessorNotFound);
            }
        }

        let entry = DagEntry { predecessors };

        // Write entry metadata
        self.engine
            .write(&self.entries_column, &key, &entry)
            .await?;

        // Write each change individually to the changes column
        let entry_key_bytes = key.to_bytes()?;

        for (column, snapshot) in snapshot.columns {
            for (data_key, value) in snapshot.entries {
                let data_key_bytes = data_key.to_bytes()?;
                let change_key = DagChangeKey {
                    column: &column,
                    entry_key_bytes: entry_key_bytes.as_ref(),
                    data_key_bytes: &data_key_bytes,
                };

                self.engine
                    .write(&self.changes_column, &change_key, value)
                    .await?;
            }
        }

        Ok(())
    }

    /// Check if an entry exists in the DAG
    pub async fn has_entry(&self, key: &K) -> DagResult<bool, B::Error> {
        self.engine
            .read::<_, ()>(&self.entries_column, key)
            .await
            .map(|opt| opt.is_some())
            .map_err(DagError::from)
    }

    /// Get an entry from the DAG by its key
    pub async fn get_entry(&self, key: &K) -> DagResult<Option<DagEntry<K>>, B::Error> {
        self.engine
            .read::<_, DagEntry<K>>(&self.entries_column, key)
            .await
            .map_err(DagError::from)
    }

    /// Remove an entry and all its associated changes from the DAG.
    ///
    /// **Warning**: This does not check whether other entries reference this one
    /// as a predecessor. The caller is responsible for ensuring consistency.
    pub async fn remove_entry(&mut self, key: &K) -> DagResult<(), B::Error> {
        let entry_key_bytes = key.to_bytes()?;

        // Delete all changes for this entry using prefix iteration on entry key bytes
        let mode = IteratorMode::Prefix(entry_key_bytes.as_ref(), IteratorDirection::Forward);
        let keys_to_delete: Vec<SerializedBytes> = self.engine.backend.iterator_keys(&self.changes_column, mode).await?
            .try_collect()
            .await?;

        for change_key_bytes in keys_to_delete {
            self.engine.backend.delete(&self.changes_column, &change_key_bytes).await?;
        }

        // Remove entry metadata
        self.engine
            .delete(&self.entries_column, key)
            .await
            .map_err(DagError::from)
    }

    /// Read a value for a given column and data key, as seen from the given set of tips.
    ///
    /// Traverses the DAG backwards from the tips using local topological order:
    /// - tips are visited in the order provided by `tips`
    /// - for each entry, predecessors are visited in the order stored in that entry
    ///
    /// For each entry visited, a point-lookup is made in the changes column.
    /// The first entry that has a write or deletion for this key wins.
    ///
    /// Returns:
    /// - `EntryState::Stored(Bytes)` if the value was found
    /// - `EntryState::Deleted` if the value was explicitly deleted
    /// - `EntryState::Absent` if no entry in the DAG touched this key
    pub async fn read<'a, DK, V>(
        &self,
        column: &Column,
        data_key: DK,
        tips: &'a [K],
    ) -> DagResult<ReadResult<V, Cow<'a, K>>, B::Error>
    where
        DK: Serializable + Send + Sync,
        V: Serializable + Send + Sync,
    {
        let mut visited = HashSet::new();
        // Stack for deterministic DFS using provided tip/predecessor order.
        // Push in reverse to preserve forward processing order with LIFO pop.
        let mut stack = Vec::new();

        for tip in tips.iter().rev() {
            if self.has_entry(tip).await? {
                stack.push(Cow::Borrowed(tip));
            }
        }

        let data_key_bytes = data_key.to_bytes()?;

        while let Some(key) = stack.pop() {
            if !visited.insert(key.clone()) {
                continue;
            }

            let Some(entry) = self.get_entry(&key).await? else {
                continue;
            };

            // Query the changes column for (entry_key, column, data_key)
            let entry_key_bytes = key.to_bytes()?;

            let change_key = DagChangeKey {
                entry_key_bytes: entry_key_bytes.as_ref(),
                column,
                data_key_bytes: &data_key_bytes,
            };

            let change_value = self.engine
                .read::<_, Option<V>>(&self.changes_column, &change_key)
                .await?;

            if let Some(change) = change_value {
                return match change {
                    Some(v) => Ok(ReadResult::Stored(v, key)),
                    None => Ok(ReadResult::Deleted(key)),
                };
            }

            // Push predecessors into the stack in reverse order so they are
            // popped/visited in stored predecessor order.
            for pred in entry.predecessors.into_iter().rev() {
                if self.has_entry(&pred).await? {
                    stack.push(Cow::Owned(pred));
                }
            }
        }

        Ok(ReadResult::Absent)
    }

    /// Check if a key exists in a column as seen from the given tips.
    pub async fn exists<DK>(
        &self,
        column: &Column,
        data_key: DK,
        tips: &[K],
    ) -> DagResult<bool, B::Error>
    where
        DK: Serializable + Send + Sync,
    {
        match self.read::<DK, ()>(column, data_key, tips).await? {
            ReadResult::Stored(_, _) => Ok(true),
            ReadResult::Deleted(_) => Ok(false),
            ReadResult::Absent => Ok(false),
        }
    }

    /// Get the underlying backend for direct access.
    #[inline]
    pub fn backend(&self) -> &XoriBackend<B> {
        &self.engine
    }

    /// Get a mutable reference to the underlying backend.
    #[inline]
    pub fn backend_mut(&mut self) -> &mut XoriBackend<B> {
        &mut self.engine
    }
}

/// Builder for constructing a `DagEntry` incrementally.
///
/// Accumulates changes (writes and deletions) before committing
/// the entry into the DAG.
pub struct DagEntryBuilder<K: DagKey> {
    predecessors: Vec<K>,
    snapshot: Snapshot,
}

impl<K: DagKey> DagEntryBuilder<K> {
    /// Create a new builder for a DAG entry.
    pub fn new(predecessors: Vec<K>) -> Self {
        Self {
            predecessors,
            snapshot: Snapshot::default(),
        }
    }

    /// Write a key-value pair into this entry's changes for the given column.
    pub fn write<DK, V>(&mut self, column: Column, key: &DK, value: &V) -> Result<(), WriterError>
    where
        DK: Serializable,
        V: Serializable,
    {
        let key = key.to_bytes()?;
        let value = value.to_bytes()?;

        self.snapshot.column_mut(column)
            .insert(key, value);

        Ok(())
    }

    /// Mark a key as deleted in this entry's changes for the given column.
    pub fn delete<DK>(&mut self, column: Column, key: &DK) -> Result<(), WriterError>
    where
        DK: Serializable,
    {
        let key = key.to_bytes()?;

        self.snapshot.column_mut(column)
            .remove(key);

        Ok(())
    }

    /// Consume the builder and commit the entry into the DAG.
    pub async fn commit<B: Backend>(
        self,
        dag: &mut DagState<K, B>,
        key: K,
    ) -> DagResult<(), B::Error> {
        dag.add_entry(key, self.predecessors, self.snapshot)
            .await
    }

    /// Consume the builder and return the changes without committing.
    pub fn build(self) -> (Vec<K>, Snapshot) {
        (self.predecessors, self.snapshot)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use crate::backend::{ColumnKind, MemoryBackend};
    use crate::dag::ReadResult;
    use crate::{DagEntryBuilder, DagState, XoriBuilder};

    async fn create_dag() -> (DagState<u64, MemoryBackend>, crate::Column) {
        let mut builder = XoriBuilder::new();
        let data_column = builder.register_column(ColumnKind::Other);
        let dag = DagState::new(builder, MemoryBackend::new()).await.unwrap();
        (dag, data_column)
    }

    #[track_caller]
    #[inline(always)]
    fn assert_stored_u64(state: ReadResult<u64, Cow<'_, u64>>, expected: u64) {
        match state {
            ReadResult::Stored(value, _) => assert_eq!(value, expected),
            ReadResult::Deleted(_) => panic!("expected stored value {expected}, got Deleted"),
            ReadResult::Absent => panic!("expected stored value {expected}, got Absent"),
        }
    }

    #[tokio::test]
    async fn dag_merge_supports_combined_branch_data() {
        let (mut dag, column) = create_dag().await;

        let mut genesis = DagEntryBuilder::new(vec![]);
        genesis.write(column, &1u8, &10u64).unwrap();
        genesis.commit(&mut dag, 1).await.unwrap();

        let mut left = DagEntryBuilder::new(vec![1]);
        left.write(column, &2u8, &20u64).unwrap();
        left.commit(&mut dag, 2).await.unwrap();

        let mut right = DagEntryBuilder::new(vec![1]);
        right.write(column, &3u8, &30u64).unwrap();
        right.commit(&mut dag, 3).await.unwrap();

        let merge = DagEntryBuilder::new(vec![2, 3]);
        merge.commit(&mut dag, 4).await.unwrap();

        assert_stored_u64(dag.read::<_, u64>(&column, 1u8, &[4]).await.unwrap(), 10);
        assert_stored_u64(dag.read::<_, u64>(&column, 2u8, &[4]).await.unwrap(), 20);
        assert_stored_u64(dag.read::<_, u64>(&column, 3u8, &[4]).await.unwrap(), 30);
    }

    #[tokio::test]
    async fn dag_can_read_old_data_from_old_tips() {
        let (mut dag, column) = create_dag().await;

        let mut genesis = DagEntryBuilder::new(vec![]);
        genesis.write(column, &1u8, &100u64).unwrap();
        genesis.commit(&mut dag, 10).await.unwrap();

        let mut left = DagEntryBuilder::new(vec![10]);
        left.write(column, &2u8, &200u64).unwrap();
        left.commit(&mut dag, 20).await.unwrap();

        let mut right = DagEntryBuilder::new(vec![10]);
        right.write(column, &3u8, &300u64).unwrap();
        right.commit(&mut dag, 30).await.unwrap();

        // Historical view at genesis: only key 1 exists.
        assert_stored_u64(dag.read::<_, u64>(&column, 1u8, &[10]).await.unwrap(), 100);
        assert!(matches!(
            dag.read::<_, u64>(&column, 2u8, &[10]).await.unwrap(),
            ReadResult::Absent
        ));
        assert!(matches!(
            dag.read::<_, u64>(&column, 3u8, &[10]).await.unwrap(),
            ReadResult::Absent
        ));

        // Left branch tip sees genesis + left append.
        assert_stored_u64(dag.read::<_, u64>(&column, 1u8, &[20]).await.unwrap(), 100);
        assert_stored_u64(dag.read::<_, u64>(&column, 2u8, &[20]).await.unwrap(), 200);
        assert!(matches!(
            dag.read::<_, u64>(&column, 3u8, &[20]).await.unwrap(),
            ReadResult::Absent
        ));
    }

    #[tokio::test]
    async fn dag_merge_orders_conflicts_by_predecessor_order() {
        let (mut dag, column) = create_dag().await;

        let shared = 9u8;
        let left_only = 10u8;
        let right_only = 11u8;

        let mut genesis = DagEntryBuilder::new(vec![]);
        genesis.write(column, &shared, &1u64).unwrap();
        genesis.commit(&mut dag, 100).await.unwrap();

        let mut left_1 = DagEntryBuilder::new(vec![100]);
        left_1.write(column, &left_only, &200u64).unwrap();
        left_1.commit(&mut dag, 200).await.unwrap();

        let mut left_2 = DagEntryBuilder::new(vec![200]);
        left_2.write(column, &shared, &400u64).unwrap();
        left_2.commit(&mut dag, 400).await.unwrap();

        let mut right_1 = DagEntryBuilder::new(vec![100]);
        right_1.write(column, &right_only, &300u64).unwrap();
        right_1.commit(&mut dag, 300).await.unwrap();

        let mut right_2 = DagEntryBuilder::new(vec![300]);
        right_2.write(column, &shared, &500u64).unwrap();
        right_2.commit(&mut dag, 500).await.unwrap();

        let merge = DagEntryBuilder::new(vec![400, 500]);
        merge.commit(&mut dag, 600).await.unwrap();


        // Merge sees data from both branches
        assert_stored_u64(dag.read::<_, u64>(&column, &left_only, &[600]).await.unwrap(), 200);
        assert_stored_u64(dag.read::<_, u64>(&column, &right_only, &[600]).await.unwrap(), 300);
        // and for conflicts, predecessor order controls local traversal priority.
        // Merge predecessors are [400, 500], so left branch wins here.
        assert_stored_u64(dag.read::<_, u64>(&column, &shared, &[600]).await.unwrap(), 400);
        // Reversing merge predecessor order flips conflict winner.
        let merge_reversed = DagEntryBuilder::new(vec![500, 400]);
        merge_reversed.commit(&mut dag, 700).await.unwrap();
        assert_stored_u64(dag.read::<_, u64>(&column, &shared, &[700]).await.unwrap(), 500);
        // Each branch tip still sees its own latest conflicting value.
        assert_stored_u64(dag.read::<_, u64>(&column, &shared, &[400]).await.unwrap(), 400);
        assert_stored_u64(dag.read::<_, u64>(&column, &shared, &[500]).await.unwrap(), 500);
    }
}