use crate::{KeyIndex, Reader, ReaderError, Serializable, Writable, WriterError};


#[derive(Debug, Clone, Default)]
pub struct EntityMetadata {
    pub keys_count: u64,
}

impl EntityMetadata {
    pub fn next_key_index(&mut self) -> KeyIndex {
        let index = KeyIndex(self.keys_count);
        self.keys_count += 1;
        index
    }
}

impl Serializable for EntityMetadata {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        self.keys_count.write(writer)
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        let keys_count = u64::read(reader)?;
        Ok(Self { keys_count })
    }

    fn size(&self) -> usize {
        self.keys_count.size()
    }
}
