use crate::{
    Backend, Changes, KeyIndex, Reader, ReaderError, Serializable, VarUint, Version, VersionedKey,
    Writable, WriterError, XoriEngine, XoriError, XoriResult, backend::ColumnId,
};
use bytes::Bytes;

/// One new version. `key` is the serialized application key, not an indexed ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryEntry {
    pub column: ColumnId,
    pub key: Vec<u8>,
    pub version: Version,
}

impl Serializable for HistoryEntry {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        self.as_ref().write(writer)
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        let column = ColumnId::read(reader)?;
        let len =
            usize::try_from(VarUint::read(reader)?.0).map_err(|_| ReaderError::UnexpectedValue)?;
        let key = reader.read_bytes_ref(len)?.to_vec();
        let version = Version::read(reader)?;
        Ok(Self {
            column,
            key,
            version,
        })
    }

    fn size(&self) -> usize {
        self.as_ref().size()
    }
}

/// Borrow keys directly from the writer's journal when packing.
#[derive(Clone, Copy)]
pub(super) struct EntryRef<'a> {
    pub column: ColumnId,
    pub key: &'a [u8],
    pub version: Version,
}

impl Serializable for EntryRef<'_> {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        self.column.write(writer)?;
        VarUint(self.key.len() as u64).write(writer)?;
        writer.extend_bytes(self.key)?;
        self.version.write(writer)
    }

    fn read(_: &mut Reader) -> Result<Self, ReaderError> {
        Err(ReaderError::NotSerializable)
    }

    fn size(&self) -> usize {
        self.column.size()
            + VarUint::encoded_size(self.key.len())
            + self.key.len()
            + self.version.size()
    }
}

impl HistoryEntry {
    fn as_ref(&self) -> EntryRef<'_> {
        EntryRef {
            column: self.column,
            key: &self.key,
            version: self.version,
        }
    }

    /// Stage an undo only when this reference is still the entity's head.
    pub(super) async fn rollback<B: Backend>(
        self,
        engine: &XoriEngine<B>,
        changes: &mut Changes,
    ) -> XoriResult<(), B::Error> {
        let column = engine
            .backend
            .columns
            .get(&self.column)
            .ok_or(XoriError::UnknownColumn(self.column))?;
        let info = engine
            .entity_registry
            .get(column.name())
            .filter(|info| info.column.id() == self.column)
            .ok_or(XoriError::UnknownColumn(self.column))?;
        let raw = Bytes::from(self.key);
        let mapped = match &info.key_index_column {
            Some(columns) => {
                let id = engine
                    .read_with_changes::<_, KeyIndex>(changes, &columns.key_to_id, raw.as_ref())
                    .await?
                    .ok_or(XoriError::HistoryConflict)?;
                Bytes::from(id.to_bytes()?.into_vec())
            }
            None => raw.clone(),
        };
        let latest = engine
            .read_with_changes::<_, Version>(changes, &info.column, mapped.as_ref())
            .await?;
        if latest != Some(self.version) {
            return Err(XoriError::HistoryConflict);
        }
        let versioned = VersionedKey {
            key: mapped.as_ref(),
            version: self.version,
        };
        changes
            .column_mut(&info.column)
            .remove(Bytes::from(versioned.to_bytes()?.into_vec()));
        match self.version.previous() {
            Some(previous) => {
                changes
                    .column_mut(&info.column)
                    .insert(mapped, Bytes::from(previous.to_bytes()?.into_vec()));
            }
            None => {
                changes.column_mut(&info.column).remove(mapped.clone());
                if let Some(columns) = &info.key_index_column {
                    changes.column_mut(&columns.key_to_id).remove(raw);
                    changes.column_mut(&columns.id_to_key).remove(mapped);
                }
            }
        }
        Ok(())
    }
}
