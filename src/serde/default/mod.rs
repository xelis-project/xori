use std::borrow::Cow;

use super::*;
use crate::VarInt;

macro_rules! impl_serializable_integer {
    ($($ty:ty => $size:expr),+) => {
        $(
            impl Serializable for $ty {
                fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
                    writer.extend_bytes(&self.to_be_bytes());
                    Ok(())
                }

                fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
                    let bytes = reader.read_bytes_ref($size)?;
                    Ok(<$ty>::from_be_bytes(bytes.try_into().expect(concat!("Failed to read ", stringify!($ty), " from bytes"))))
                }

                fn size(&self) -> usize {
                    $size
                }
            }
        )+
    };
}

impl_serializable_integer!(u16 => 2, u32 => 4, i64 => 8, u64 => 8);

impl Serializable for u8 {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        writer.push(*self);
        Ok(())
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        reader.next_byte()
    }

    fn size(&self) -> usize {
        1
    }
}

impl Serializable for bool {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        (*self as u8).write(writer)
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        match u8::read(reader)? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(ReaderError::UnexpectedValue)
        }
    }

    fn size(&self) -> usize {
        1
    }
}

impl Serializable for () {
    fn write<W: Writable>(&self, _writer: &mut W) -> Result<(), WriterError> {
        Ok(())
    }

    fn read(_reader: &mut Reader) -> Result<Self, ReaderError> {
        Ok(())
    }

    fn size(&self) -> usize {
        0
    }
}

impl<'a, T: Serializable + Clone> Serializable for Cow<'a, T> {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        self.as_ref().write(writer)
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        T::read(reader).map(Cow::Owned)
    }

    fn size(&self) -> usize {
        self.as_ref().size()
    }
}

impl<T: Serializable> Serializable for Option<T> {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        match self {
            Some(value) => {
                1u8.write(writer)?;
                value.write(writer)
            }
            None => 0u8.write(writer),
        }
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        match u8::read(reader)? {
            0 => Ok(None),
            1 => T::read(reader).map(Some),
            _ => Err(ReaderError::UnexpectedValue)
        }
    }

    fn size(&self) -> usize {
        1 + self.as_ref().map_or(0, |value| value.size())
    }
}

impl<T: Serializable> Serializable for Vec<T> {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        // Write length as VarInt
        VarInt(self.len() as u64).write(writer)?;

        // Write each element
        for item in self {
            item.write(writer)?;
        }
        
        Ok(())
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        // Read length as VarInt
        let len = VarInt::read(reader)?.value();

        // Pre-allocate vector
        let mut vec = Vec::with_capacity(len.min(1024) as usize); // Cap allocation for safety

        // Read each element
        for _ in 0..len {
            vec.push(T::read(reader)?);
        }
        
        Ok(vec)
    }

    fn size(&self) -> usize {
        let len_size = VarInt::encoded_size(self.len());
        let items_size: usize = self.iter().map(|item| item.size()).sum();
        len_size + items_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vec_u8_serialization() {
        let vec = vec![1u8, 2, 3, 4, 5];
        let bytes = vec.to_bytes().unwrap();
        let decoded = Vec::<u8>::from_bytes(&bytes).unwrap();
        assert_eq!(vec, decoded);
    }

    #[test]
    fn test_vec_u32_serialization() {
        let vec = vec![100u32, 200, 300, 400];
        let bytes = vec.to_bytes().unwrap();
        let decoded = Vec::<u32>::from_bytes(&bytes).unwrap();
        assert_eq!(vec, decoded);
    }

    #[test]
    fn test_vec_empty() {
        let vec: Vec<u64> = vec![];
        let bytes = vec.to_bytes().unwrap();
        assert_eq!(bytes.len(), 1); // Just the length prefix (0)
        let decoded = Vec::<u64>::from_bytes(&bytes).unwrap();
        assert_eq!(vec, decoded);
    }

    #[test]
    fn test_vec_single_element() {
        let vec = vec![42u64];
        let bytes = vec.to_bytes().unwrap();
        let decoded = Vec::<u64>::from_bytes(&bytes).unwrap();
        assert_eq!(vec, decoded);
    }

    #[test]
    fn test_vec_large_collection() {
        let vec: Vec<u16> = (0..1000).collect();
        let bytes = vec.to_bytes().unwrap();
        let decoded = Vec::<u16>::from_bytes(&bytes).unwrap();
        assert_eq!(vec, decoded);
    }

    #[test]
    fn test_vec_nested() {
        let vec = vec![
            vec![1u32, 2, 3],
            vec![4, 5],
            vec![6, 7, 8, 9],
        ];
        let bytes = vec.to_bytes().unwrap();
        let decoded = Vec::<Vec<u32>>::from_bytes(&bytes).unwrap();
        assert_eq!(vec, decoded);
    }

    #[test]
    fn test_vec_size_calculation() {
        let vec = vec![1u64, 2, 3, 4, 5];
        let size = vec.size();
        let bytes = vec.to_bytes().unwrap();
        assert_eq!(size, bytes.len());
    }

    #[test]
    fn test_vec_option_serialization() {
        let vec = vec![Some(1u32), None, Some(3), None, Some(5)];
        let bytes = vec.to_bytes().unwrap();
        let decoded = Vec::<Option<u32>>::from_bytes(&bytes).unwrap();
        assert_eq!(vec, decoded);
    }

    #[test]
    fn test_vec_variable_size_prefix() {
        // Test that small length uses 1 byte prefix
        let small_vec = vec![1u8; 10];
        let small_bytes = small_vec.to_bytes().unwrap();
        assert_eq!(small_bytes[0], 10); // Direct length encoding
        
        // Test that larger length uses multi-byte prefix
        let large_vec = vec![1u8; 300];
        let large_bytes = large_vec.to_bytes().unwrap();
        assert_eq!(large_bytes[0], 0xFD); // VarInt prefix for u16
    }
}

impl<'a> Serializable for &'a [u8] {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        writer.extend_bytes(self);
        Ok(())
    }

    fn read(_: &mut Reader) -> Result<Self, ReaderError> {
        Err(ReaderError::NotSerializable)
    }

    fn size(&self) -> usize {
        self.len()
    }
}