use std::{borrow::{Borrow, Cow}, cmp::Ordering, fmt, hash::{Hash, Hasher}, sync::Arc};
use serde::{Deserialize, Serialize};
use crate::{Reader, ReaderError, Serializable, VarInt, Writable, WriterError};

/// Unique identifier for a column/namespace in the database
#[derive(Debug, PartialOrd, Ord, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ColumnId(pub(crate) u64);

impl fmt::Display for ColumnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ColumnId({})", self.0)
    }
}

/// Represents a column/namespace in the database, used for organizing data (e.g., per entity type)
pub type Column = Arc<ColumnInner>;

/// Represents a database column/namespace for organizing data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInner {
    pub(crate) name: Cow<'static, str>,
    pub(crate) id: ColumnId,
    pub(crate) kind: ColumnKind,
    pub(crate) properties: ColumnProperties,
}

impl Hash for ColumnInner {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl PartialEq for ColumnInner {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Borrow<ColumnId> for ColumnInner {
    fn borrow(&self) -> &ColumnId {
        &self.id
    }
}

impl Eq for ColumnInner {}

impl PartialOrd for ColumnInner {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.id.cmp(&other.id))
    }
}

impl Ord for ColumnInner {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl ColumnInner {
    #[inline]
    pub fn name<'a>(&'a self) -> &'a str {
        &self.name
    }

    #[inline]
    pub fn id(&self) -> ColumnId {
        self.id
    }

    #[inline]
    pub fn kind(&self) -> ColumnKind {
        self.kind
    }

    #[inline]
    pub fn properties(&self) -> &ColumnProperties {
        &self.properties
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum ColumnKind {
    Entity,
    Index,
    Other,
    Unknown,
}

impl Serializable for ColumnId {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        VarInt::from(self.0).write(writer)
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        let id = VarInt::read(reader)?;
        Ok(ColumnId(id.0))
    }

    fn size(&self) -> usize {
        VarInt::from(self.0).size()
    }
}

impl fmt::Display for ColumnInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ColumnId({})", self.id.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnProperties {
    pub prefix_length: Option<usize>,
}