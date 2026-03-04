use std::collections::{BTreeMap, HashMap, btree_map::Entry};

use bytes::Bytes;
use itertools::Either;

use crate::{Column, backend::ColumnId, engine::{IteratorDirection, IteratorMode}};

/// Represents the state of an entry in a snapshot, which can be stored, deleted, or absent (not modified in the snapshot)
#[derive(Debug)]
pub enum EntryState<T> {
    // Has been added/modified in our snapshot
    Stored(T),
    // Has been deleted in our snapshot
    Deleted,
    // Not present in our snapshot
    // Must fallback on backend
    Absent
}

impl<T: Clone> Clone for EntryState<T> {
    fn clone(&self) -> Self {
        match self {
            EntryState::Stored(value) => EntryState::Stored(value.clone()),
            EntryState::Deleted => EntryState::Deleted,
            EntryState::Absent => EntryState::Absent,
        }
    }
}

/// Represents a snapshot of changes to be applied to the database, organized by column
#[derive(Debug, Clone, Default)]
pub struct ColumnSnapshot {
    pub(crate) entries: BTreeMap<Bytes, Option<Bytes>>,
}

#[derive(Default, Debug, Clone)]
pub struct Snapshot {
    // Maps columns to their modified entries in the snapshot
    // for each column, we have a map of key to either a stored value (if modified/added) or a deletion marker (if deleted)
    pub(crate) columns: HashMap<ColumnId, ColumnSnapshot>,
}

impl Snapshot {
    /// Get a mutable reference to the snapshot for a specific column, creating it if it doesn't exist
    #[inline]
    pub fn column_mut(&mut self, column: &Column) -> &mut ColumnSnapshot {
        self.columns.entry(column.id()).or_default()
    }

    /// Get an immutable reference to the snapshot for a specific column, if it exists
    #[inline]
    pub fn column(&self, column: &Column) -> Option<&ColumnSnapshot> {
        self.columns.get(&column.id())
    }
}

impl ColumnSnapshot {
    /// Get the value for a key in this column snapshot
    pub fn get<'a, K>(&'a self, key: K) -> EntryState<&'a Bytes>
    where
        K: AsRef<[u8]>,
    {
        match self.entries.get(key.as_ref()) {
            Some(Some(value)) => EntryState::Stored(value),
            Some(None) => EntryState::Deleted,
            None => EntryState::Absent,
        }
    }

    /// Set a key to a new value
    /// Returns the previous value if any
    pub fn insert<K, V>(&mut self, key: K, value: V) -> EntryState<Bytes>
    where
        K: Into<Bytes>,
        V: Into<Bytes>,
    {
        match self.entries.insert(key.into(), Some(value.into())) {
            Some(Some(prev)) => EntryState::Stored(prev),
            Some(None) => EntryState::Deleted,
            None => EntryState::Absent,
        }
    }

    /// Remove a key
    /// If bool return true, we must read from disk
    /// Returns the previous value if any
    pub fn remove<K>(&mut self, key: K) -> EntryState<Bytes>
    where
        K: Into<Bytes>,
    {
        match self.entries.entry(key.into()) {
            Entry::Occupied(mut entry) => {
                let value = entry.get_mut().take();
                match value {
                    Some(v) => EntryState::Stored(v),
                    None => EntryState::Deleted,
                }
            },
            Entry::Vacant(v) => {
                v.insert(None);
                EntryState::Absent
            },
        }
    }

    /// Check if key is present in our batch
    /// Return None if key wasn't overwritten yet
    /// Otherwise, return Some(true) if key is present, Some(false) if it was deleted
    #[inline]
    pub fn contains<K>(&self, key: K) -> Option<bool>
    where
        K: AsRef<[u8]>
    {
        self.entries.get(key.as_ref()).map(|v| v.is_some())
    }

    /// Get an iterator over the entries in this column snapshot, yielding (key, value) pairs
    #[inline]
    pub fn iterator<'a>(&'a self, mode: IteratorMode<'a>) -> impl Iterator<Item = (&'a Bytes, &'a Bytes)> + 'a {
        match mode {
            IteratorMode::All(direction) => Either::Left(Either::Left(match direction {
                IteratorDirection::Forward => Either::Left(self.entries.iter()),
                IteratorDirection::Backward => Either::Right(self.entries.iter().rev()),
            })),
            IteratorMode::Prefix(prefix, direction) => {
                let prefix = Bytes::copy_from_slice(prefix);
                let range = self.entries
                    .range(prefix.clone()..);

                Either::Left(Either::Right(match direction {
                    IteratorDirection::Forward => Either::Left(range),
                    IteratorDirection::Backward => Either::Right(range.rev()),
                }.into_iter().take_while(move |(k, _)| k.starts_with(&prefix))))
            },
            IteratorMode::Range { start, end, direction } => {
                let start = Bytes::copy_from_slice(start);
                let end = Bytes::copy_from_slice(end);
                let range = self.entries
                    .range(start..end);

                Either::Right(match direction {
                    IteratorDirection::Forward => Either::Left(range),
                    IteratorDirection::Backward => Either::Right(range.rev()),
                })
            },
            IteratorMode::From(start, direction) => {
                let start = Bytes::copy_from_slice(start);
                let range = self.entries
                    .range(start..);

                Either::Right(match direction {
                    IteratorDirection::Forward => Either::Left(range),
                    IteratorDirection::Backward => Either::Right(range.rev()),
                })
            },
        }.into_iter().filter_map(|(k, v)| match v {
            Some(value) => Some((k, value)),
            None => None,
        })
    }

    /// Get an iterator over the keys in this column snapshot, yielding keys only
    #[inline]
    pub fn iterator_keys<'a>(&'a self, mode: IteratorMode<'a>) -> impl Iterator<Item = &'a Bytes> + 'a {
        self.iterator(mode).map(|(k, _)| k)
    }

    // /// Get an iterator over the entries in this column snapshot that have keys starting with the given prefix
    // #[inline]
    // pub fn iter_prefix<'a, P>(&'a self, prefix: P) -> impl Iterator<Item = (&'a Bytes, Option<&'a Bytes>)> + 'a
    // where
    //     P: Into<Bytes>,
    // {
    //     let prefix_bytes = prefix.into();
    //     self.entries.range(prefix_bytes.clone()..)
    //         .take_while(move |(k, _)| k.starts_with(&prefix_bytes))
    //         .map(|(k, v)| (k, v.as_ref()))
    // }
}

