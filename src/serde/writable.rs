use std::borrow::Cow;

use crate::{Reader, ReaderError, Serializable, Writable, WriterError};

pub struct WritableBytes<T: AsRef<[u8]>>(pub T);

impl<T: AsRef<[u8]>> Serializable for WritableBytes<T> {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        writer.extend_bytes(self.0.as_ref());
        Ok(())
    }

    fn to_bytes<'a>(&'a self) -> Result<Cow<'a, [u8]>, WriterError> {
        Ok(Cow::Borrowed(self.0.as_ref()))
    }

    fn read(_: &mut Reader) -> Result<Self, ReaderError> {
        Err(ReaderError::NotSerializable)
    }

    fn size(&self) -> usize {
        self.0.as_ref().len()
    }
}