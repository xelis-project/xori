mod writer;
mod reader;
mod default;
mod writable;
mod varint;
mod bytes;

use std::any::type_name;

pub use writer::{Writable, WriterError};
pub use reader::{Reader, ReaderError};
pub use writable::WritableBytes;
pub use varint::VarUint;
pub use bytes::SerializedBytes;

/// Trait for types that can be serialized and deserialized by Xori
pub trait Serializable: Sized {
    /// Serialize directly into a buffer or sink, propagating write errors.
    /// On failure the sink may contain a partially serialized value.
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError>;

    /// Serialize to bytes
    fn to_bytes<'a>(&'a self) -> Result<SerializedBytes<'a>, WriterError> {
        let mut buffer = Vec::with_capacity(self.size());
        self.write(&mut buffer)
            .map_err(|error| error.context(format!("serializing {}", type_name::<Self>())))?;
        Ok(SerializedBytes::Owned(buffer.into_boxed_slice()))
    }

    /// Deserialize from bytes
    fn from_bytes<T: AsRef<[u8]>>(bytes: T) -> Result<Self, ReaderError> {
        let mut reader = Reader::new(bytes.as_ref());
        Self::read(&mut reader).map_err(|error| {
            error.context(format!(
                "deserializing {} at byte {}",
                type_name::<Self>(),
                reader.total_read(),
            ))
        })
    }

    /// Read an instance of the type from a reader
    fn read(reader: &mut Reader) -> Result<Self, ReaderError>;

    /// Estimate the size of the serialized entity without actually serializing it
    fn size(&self) -> usize;
}

impl<'a, T: Serializable> Serializable for &'a T {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        (*self).write(writer)
    }

    fn read(_: &mut Reader) -> Result<Self, ReaderError> {
        Err(ReaderError::NotSerializable)
    }

    fn size(&self) -> usize {
        (*self).size()
    }

}
