use super::{Reader, ReaderError, Serializable, Writable, WriterError};

/// Variable-length integer encoding for usize values
/// Uses 1, 2, 4, or 8 bytes depending on the value magnitude
/// 
/// Format:
/// - 0x00-0xFC: value in 1 byte (0-252)
/// - 0xFD + 2 bytes: u16 value (253-65535)
/// - 0xFE + 4 bytes: u32 value (65536-4294967295)
/// - 0xFF + 8 bytes: u64 value (4294967296+)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VarInt(pub usize);

impl VarInt {
    /// Create a new VarInt from a usize value
    #[inline]
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    /// Get the inner usize value
    #[inline]
    pub const fn value(&self) -> usize {
        self.0
    }

    /// Calculate the serialized size without actually serializing
    #[inline]
    pub const fn encoded_size(value: usize) -> usize {
        if value <= 0xFC {
            1
        } else if value <= 0xFFFF {
            3 // 1 byte prefix + 2 bytes
        } else if value <= 0xFFFFFFFF {
            5 // 1 byte prefix + 4 bytes
        } else {
            9 // 1 byte prefix + 8 bytes
        }
    }
}

impl From<usize> for VarInt {
    fn from(value: usize) -> Self {
        Self(value)
    }
}

impl From<VarInt> for usize {
    fn from(varint: VarInt) -> Self {
        varint.0
    }
}

impl Serializable for VarInt {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        let value = self.0;
        
        if value <= 0xFC {
            // Single byte for values 0-252
            writer.push(value as u8);
        } else if value <= 0xFFFF {
            // 0xFD prefix + 2 bytes for u16
            writer.push(0xFD);
            writer.extend_bytes(&(value as u16).to_be_bytes());
        } else if value <= 0xFFFFFFFF {
            // 0xFE prefix + 4 bytes for u32
            writer.push(0xFE);
            writer.extend_bytes(&(value as u32).to_be_bytes());
        } else {
            // 0xFF prefix + 8 bytes for u64
            writer.push(0xFF);
            writer.extend_bytes(&(value as u64).to_be_bytes());
        }
        
        Ok(())
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        let prefix = reader.next_byte()?;
        match prefix {
            0x00..=0xFC => Ok(Self(prefix as usize)),
            0xFD => u16::read(reader).map(|v| Self(v as usize)),
            0xFE => u32::read(reader).map(|v| Self(v as usize)),
            0xFF => u64::read(reader).map(|v| Self(v as usize)),
        }
    }

    fn size(&self) -> usize {
        Self::encoded_size(self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_single_byte() {
        for value in [0, 1, 100, 252] {
            let varint = VarInt(value);
            let bytes = varint.to_bytes().unwrap();
            assert_eq!(bytes.len(), 1);
            
            let decoded = VarInt::from_bytes(&bytes).unwrap();
            assert_eq!(decoded.0, value);
        }
    }

    #[test]
    fn test_varint_two_bytes() {
        for value in [253, 254, 255, 1000, 65535] {
            let varint = VarInt(value);
            let bytes = varint.to_bytes().unwrap();
            assert_eq!(bytes.len(), 3); // prefix + 2 bytes
            
            let decoded = VarInt::from_bytes(&bytes).unwrap();
            assert_eq!(decoded.0, value);
        }
    }

    #[test]
    fn test_varint_four_bytes() {
        for value in [65536, 100000, 1000000, 0xFFFFFFFF] {
            let varint = VarInt(value);
            let bytes = varint.to_bytes().unwrap();
            assert_eq!(bytes.len(), 5); // prefix + 4 bytes
            
            let decoded = VarInt::from_bytes(&bytes).unwrap();
            assert_eq!(decoded.0, value);
        }
    }

    #[test]
    fn test_varint_eight_bytes() {
        for value in [0x100000000usize, 0xFFFFFFFFFFFFFFFF] {
            let varint = VarInt(value);
            let bytes = varint.to_bytes().unwrap();
            assert_eq!(bytes.len(), 9); // prefix + 8 bytes
            
            let decoded = VarInt::from_bytes(&bytes).unwrap();
            assert_eq!(decoded.0, value);
        }
    }

    #[test]
    fn test_varint_encoded_size() {
        assert_eq!(VarInt::encoded_size(0), 1);
        assert_eq!(VarInt::encoded_size(252), 1);
        assert_eq!(VarInt::encoded_size(253), 3);
        assert_eq!(VarInt::encoded_size(65535), 3);
        assert_eq!(VarInt::encoded_size(65536), 5);
        assert_eq!(VarInt::encoded_size(0xFFFFFFFF), 5);
        assert_eq!(VarInt::encoded_size(0x100000000), 9);
    }

    #[test]
    fn test_varint_boundary_values() {
        let boundaries = [0, 252, 253, 255, 256, 65535, 65536, 0xFFFFFFFF, 0x100000000];
        
        for &value in &boundaries {
            let varint = VarInt(value);
            let bytes = varint.to_bytes().unwrap();
            let decoded = VarInt::from_bytes(&bytes).unwrap();
            assert_eq!(decoded.0, value, "Failed at boundary value {}", value);
        }
    }

    #[test]
    fn test_varint_size_matches_serialized() {
        for value in [0, 1, 252, 253, 1000, 65535, 65536, 1000000, 0x100000000usize] {
            let varint = VarInt(value);
            let size = varint.size();
            let bytes = varint.to_bytes().unwrap();
            assert_eq!(size, bytes.len(), "Size mismatch for value {}", value);
        }
    }
}
