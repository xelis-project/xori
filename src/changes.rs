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

/// Represents a set of changes to be applied to the database, organized by column
#[derive(Debug, Clone, Default)]
pub struct ColumnChanges {
    pub(crate) entries: BTreeMap<Bytes, Option<Bytes>>,
}

#[derive(Default, Debug, Clone)]
pub struct Changes {
    // Maps columns to their modified entries in the snapshot
    // for each column, we have a map of key to either a stored value (if modified/added) or a deletion marker (if deleted)
    pub(crate) columns: HashMap<ColumnId, ColumnChanges>,
}

impl Changes {
    /// Get a mutable reference to the snapshot for a specific column, creating it if it doesn't exist
    #[inline]
    pub fn column_mut(&mut self, column: &Column) -> &mut ColumnChanges {
        self.columns.entry(column.id()).or_default()
    }

    /// Get an immutable reference to the snapshot for a specific column, if it exists
    #[inline]
    pub fn column(&self, column: &Column) -> Option<&ColumnChanges> {
        self.columns.get(&column.id())
    }
}

impl ColumnChanges {
    /// Pending puts and deletions. `None` represents a deletion.
    pub fn entries(self) -> impl Iterator<Item = (Bytes, Option<Bytes>)> {
        self.entries.into_iter()
    }

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
        let (lower, upper, direction) = mode.bounds();
        let range = self.entries.range((lower, upper));
        match direction {
            IteratorDirection::Forward => Either::Left(range),
            IteratorDirection::Backward => Either::Right(range.rev()),
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
}
