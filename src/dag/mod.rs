mod error;

use std::borrow::Cow;
use std::collections::{HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

pub use error::{DagError, DagResult};
use futures::{Stream, TryStreamExt, stream};
use indexmap::IndexSet;

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

    /// Get the predecessors of a given entry as a stream.
    pub async fn predecessors<'a>(&'a self, key: &'a K) -> DagResult<impl Stream<Item = DagResult<K, B::Error>> + 'a, B::Error> {
        struct StreamState<K> {
            predecessors: VecDeque<K>,
            visited: HashSet<K>,
        }

        Ok(stream::unfold(
            match self.get_entry(key).await? {
                Some(entry) => Some(StreamState {
                    visited: HashSet::new(),
                    predecessors: VecDeque::from(entry.predecessors),
                }),
                None => None,
            },
            |state| async {
                let mut state = state?;
                let mut predecessor = state.predecessors.pop_front()?;
                while !state.visited.insert(predecessor.clone()) {
                    predecessor = state.predecessors.pop_front()?;
                }

                match self.get_entry(&predecessor).await {
                    Ok(Some(entry)) => {
                        state.predecessors.extend(entry.predecessors);
                        Some((Ok(predecessor), Some(state)))
                    }
                    Ok(None) => Some((Err(DagError::EntryNotFound), Some(state))),
                    Err(e) => Some((Err(DagError::from(e)), Some(state))),
                }
            }
        ))
    }

    /// Get the predecessors of a given entry by level, as a stream of vectors.
    /// Each vector contains the predecessors at the same level (i.e. same distance from the original entry).
    pub async fn predecessors_by_level<'a>(&'a self, key: &'a K) -> DagResult<impl Stream<Item = DagResult<IndexSet<K>, B::Error>> + 'a, B::Error> {
        struct StreamState<K> {
            current_level: IndexSet<K>,
            visited: HashSet<K>,
        }

        Ok(stream::unfold(
            match self.get_entry(key).await? {
                Some(entry) => Some(StreamState {
                    visited: HashSet::new(),
                    current_level: IndexSet::from_iter(entry.predecessors),
                }),
                None => None,
            },
            |state| async {
                let mut state = state?;
                if state.current_level.is_empty() {
                    return None;
                }

                let mut next_level = IndexSet::new();
                for predecessor in state.current_level.iter() {
                    if !state.visited.insert(predecessor.clone()) {
                        continue;
                    }

                    match self.get_entry(predecessor).await {
                        Ok(Some(entry)) => next_level.extend(entry.predecessors),
                        Ok(None) => return Some((Err(DagError::EntryNotFound), Some(state))),
                        Err(e) => return Some((Err(DagError::from(e)), Some(state))),
                    }
                }

                let current_level = std::mem::take(&mut state.current_level);
                state.current_level = next_level;

                Some((Ok(current_level), Some(state)))
            }
        ))
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

    use futures::TryStreamExt;
    use indexmap::IndexSet;

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

        let levels = dag.predecessors_by_level(&4).await.unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let last_level: IndexSet<u64> = IndexSet::from_iter(vec![2u64, 3u64]);
        let first_level: IndexSet<u64> = IndexSet::from_iter(vec![1u64]);
        assert_eq!(levels, vec![last_level, first_level]);
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

    #[tokio::test]
    async fn dag_predecessors_order() {
        let (mut dag, _) = create_dag().await;

        // Build a more complex DAG with multiple branches and merges:
        //       /---> 2 ---> 4 --\
        //      /                   \
        //  1 --                      6 ---> 7
        //      \                    /      /
        //       \---> 3 ---> 5 --/-------/
        //
        // Then add another merge point:
        //     /-----------------------> 9 ---\
        //    /                            \   \
        //   /   /---> 2 ---> 4 -\      /-> 7 ->\
        //  /   /                 \    /         \
        //  1 --                   \-> 6 ------> 8
        //      \                    /          /
        //       \---> 3 ---> 5 ----/----------/
        // from 8: [6, 9, 7, 4, 5, 1, 2, 3]

        // Genesis
        let genesis = DagEntryBuilder::new(vec![]);
        genesis.commit(&mut dag, 1).await.unwrap();

        // First level branches
        let left_1 = DagEntryBuilder::new(vec![1]);
        left_1.commit(&mut dag, 2).await.unwrap();

        let right_1 = DagEntryBuilder::new(vec![1]);
        right_1.commit(&mut dag, 3).await.unwrap();

        let middle = DagEntryBuilder::new(vec![1]);
        middle.commit(&mut dag, 9).await.unwrap();

        // Second level branches
        let left_2 = DagEntryBuilder::new(vec![2]);
        left_2.commit(&mut dag, 4).await.unwrap();

        let right_2 = DagEntryBuilder::new(vec![3]);
        right_2.commit(&mut dag, 5).await.unwrap();

        // First merge: 2 -> 4 and 3 -> 5 merge
        let merge_1 = DagEntryBuilder::new(vec![4, 5]);
        merge_1.commit(&mut dag, 6).await.unwrap();

        // Second merge: 6 and 9 merge
        let merge_2 = DagEntryBuilder::new(vec![6, 9]);
        merge_2.commit(&mut dag, 7).await.unwrap();

        // Third merge with more branches
        let merge_3 = DagEntryBuilder::new(vec![6, 9, 7]);
        merge_3.commit(&mut dag, 8).await.unwrap();

        // Verify predecessors from the most merged node (8)
        // Predecessors are [6, 9, 7]. VecDeque pops from front, so:
        // Pop 6 -> visit 6, add 4,5 to queue -> [9, 7, 4, 5]
        // Pop 9 -> visit 9, add 1 to queue -> [7, 4, 5, 1]
        // Pop 7 -> visit 7, add 6,9 but already visited -> [4, 5, 1]
        // Pop 4 -> visit 4, add 2 to queue -> [5, 1, 2]
        // Pop 5 -> visit 5, add 3 to queue -> [1, 2, 3]
        // Pop 1 -> visit 1, no preds -> [2, 3]
        // Pop 2 -> visit 2, add 1 but already visited -> [3]
        // Pop 3 -> visit 3, add 1 but already visited -> []
        let preds: Vec<u64> = dag.predecessors(&8).await.unwrap()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(preds, vec![6, 9, 7, 4, 5, 1, 2, 3]);

        let preds_by_level: Vec<IndexSet<u64>> = dag.predecessors_by_level(&8).await.unwrap()
            .try_collect()
            .await
            .unwrap();

        let level4: IndexSet<u64> = IndexSet::from_iter(vec![6, 9, 7]);
        let level3: IndexSet<u64> = IndexSet::from_iter(vec![4, 5, 1, 6, 9]);
        let level2: IndexSet<u64> = IndexSet::from_iter(vec![2, 3]);
        let level1: IndexSet<u64> = IndexSet::from_iter(vec![1u64]);
        assert_eq!(preds_by_level, vec![level4, level3, level2, level1]);

        // Verify predecessors from merge_2 (7)
        let preds_7: Vec<u64> = dag.predecessors(&7).await.unwrap()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(preds_7, vec![6, 9, 4, 5, 1, 2, 3]);

        // Verify predecessors from merge_1 (6)
        let preds_6: Vec<u64> = dag.predecessors(&6).await.unwrap()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(preds_6, vec![4, 5, 2, 3, 1]);
    }
}