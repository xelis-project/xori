use crate::{Reader, ReaderError, Serializable, Writable, WriterError};

/// Key representation for entities, supporting both raw keys and indexed keys
#[derive(Debug, Clone)]
pub enum Key<K: Serializable> {
    Raw(K),
    Indexed(KeyIndex),
}

/// Key index for mapping raw keys to indexed keys
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct KeyIndex(pub(crate) u64);

impl Serializable for KeyIndex {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        self.0.write(writer)
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        let value = u64::read(reader)?;
        Ok(KeyIndex(value))
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

impl<K: Serializable> Serializable for Key<K> {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        match self {
            Key::Raw(key) => key.write(writer),
            Key::Indexed(index) => index.write(writer),
        }
    }

    fn read(_: &mut Reader) -> Result<Self, ReaderError> {
        Err(ReaderError::NotSerializable)
    }

    fn size(&self) -> usize {
        match self {
            Key::Raw(key) => key.size(),
            Key::Indexed(index) => index.size(),
        }
    }
}