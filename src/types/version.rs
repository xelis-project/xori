use serde::{Deserialize, Serialize};

use crate::{Reader, ReaderError, Serializable, VarInt, Writable, WriterError};


/// Version identifier representing a point in history (e.g., topoheight)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub struct Version(pub(crate) u64);

impl Version {
    #[inline(always)]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    #[inline(always)]
    pub fn previous(self) -> Option<Self> {
        self.0.checked_sub(1).map(Version)
    }
}

impl Serializable for Version {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        VarInt(self.0).write(writer)
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        let value = VarInt::read(reader)?;
        Ok(Version(value.0))
    }

    fn size(&self) -> usize {
        VarInt(self.0).size()
    }
}

/// Represents a single value entry with version information
/// Serialize the key first to have the properties of the key preserved in the BTree ordering,
/// then serialize the version to allow for version-based range queries
/// It allows for range queries based on version for a specific key
#[derive(Debug, Clone)]
pub struct VersionedKey<E: Serializable> {
    pub key: E,
    pub version: Version,
}

impl<E: Serializable> Serializable for VersionedKey<E> {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        self.key.write(writer)?;
        self.version.write(writer)
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        let key = E::read(reader)?;
        let version = Version::read(reader)?;
        Ok(Self { version, key })
    }

    fn size(&self) -> usize {
        self.key.size() + self.version.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_varint_serialization_small() {
        // Test small values (single byte)
        let version = Version(100);
        let bytes = version.to_bytes().unwrap();
        
        assert_eq!(bytes.len(), 1);
        assert_eq!(Version::from_bytes(&bytes).unwrap(), version);
    }

    #[test]
    fn test_version_varint_serialization_boundary() {
        // Test boundary values where VarInt encoding changes size
        let test_cases = vec![
            (252, 1),   // Last single-byte value
            (253, 3),   // First two-byte value (with prefix)
            (65535, 3), // Last two-byte value
            (65536, 5), // First four-byte value (with prefix)
            (u32::MAX as u64, 5), // Last four-byte value
            (u32::MAX as u64 + 1, 9), // First eight-byte value (with prefix)
        ];

        for (value, expected_size) in test_cases {
            let version = Version(value);
            let bytes = version.to_bytes().unwrap();
            
            assert_eq!(bytes.len(), expected_size, "Version({}) should serialize to {} bytes", value, expected_size);
            assert_eq!(Version::from_bytes(&bytes).unwrap(), version);
        }
    }

    #[test]
    fn test_version_varint_ordering_preserves_lexicographic_order() {
        // This is the critical test: VarInt encoding should preserve lexicographic ordering
        // across encoding boundary transitions
        
        let versions = vec![
            Version(0),
            Version(1),
            Version(100),
            Version(252),   // Last 1-byte encoding
            Version(253),   // First 3-byte encoding
            Version(254),
            Version(1000),
            Version(65535), // Last 3-byte encoding
            Version(65536), // First 5-byte encoding
            Version(100000),
            Version(u32::MAX as u64), // Last 5-byte encoding
            Version(u32::MAX as u64 + 1), // First 9-byte encoding
            Version(u64::MAX),
        ];

        let mut serialized: Vec<(Version, Vec<u8>)> = versions
            .iter()
            .map(|v| (*v, v.to_bytes().unwrap().into_vec()))
            .collect();

        // Sort by serialized bytes (lexicographic order)
        serialized.sort_by(|a, b| a.1.cmp(&b.1));

        // Verify that lexicographic order matches numeric order
        for i in 0..serialized.len() - 1 {
            let (v1, bytes1) = &serialized[i];
            let (v2, bytes2) = &serialized[i + 1];
            
            assert!(
                v1 < v2,
                "Lexicographic order violation: Version({}) (bytes: {:?}) should be < Version({}) (bytes: {:?})",
                v1.0, bytes1, v2.0, bytes2
            );
        }
    }

    #[test]
    fn test_versioned_entry_range_query_simulation() {
        // Simulate how versioned entries would be stored with different version values
        // This tests if prefix-based range queries would work correctly
        
        #[derive(Debug, Clone, PartialEq)]
        struct TestKey(u32);
        
        impl Serializable for TestKey {
            fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
                self.0.write(writer)
            }
            
            fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
                Ok(TestKey(u32::read(reader)?))
            }
            
            fn size(&self) -> usize {
                4
            }
        }

        let key = TestKey(42);
        
        // Create versioned entries with versions crossing encoding boundaries
        let versions = vec![
            Version(0),
            Version(100),
            Version(252),   // Last 1-byte
            Version(253),   // First 3-byte
            Version(300),
            Version(65535), // Last 3-byte
            Version(65536), // First 5-byte
            Version(100000),
        ];

        let mut entries: Vec<(Vec<u8>, Version)> = versions
            .iter()
            .map(|v| {
                let entry = VersionedKey {
                    version: *v,
                    key: &key,
                };
                (entry.to_bytes().unwrap().into_vec(), *v)
            })
            .collect();

        // Sort entries by their serialized bytes (as they would be in a BTree)
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Verify versions are still in ascending order after serialization sorting
        for i in 0..entries.len() - 1 {
            let v1 = entries[i].1;
            let v2 = entries[i + 1].1;
            
            assert!(
                v1 < v2,
                "Range query order violation: Version({}) should be < Version({})",
                v1.0, v2.0
            );
        }
    }

    #[test]
    fn test_version_prefix_range_compatibility() {
        // Test that versions with the same key prefix maintain correct ordering
        // This is important for iterator_prefix operations
        // Note: VersionedEntry serializes as [version][key], so we're testing
        // that within entries sharing the same version structure, ordering works correctly
        
        #[derive(Debug, Clone)]
        struct Key(u32);
        
        impl Serializable for Key {
            fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
                self.0.write(writer)
            }
            
            fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
                Ok(Key(u32::read(reader)?))
            }
            
            fn size(&self) -> usize {
                4
            }
        }

        let key = Key(1);

        // Create multiple versioned entries with the same key but different versions
        let versions = vec![
            Version(0),
            Version(250),
            Version(252),
            Version(253), // Encoding boundary
            Version(255),
            Version(1000),
            Version(65535),
            Version(65536), // Another encoding boundary
            Version(100000),
        ];

        let mut serialized_entries: Vec<(Vec<u8>, Version)> = versions
            .iter()
            .map(|v| {
                let entry = VersionedKey {
                    version: *v,
                    key: &key,
                };
                (entry.to_bytes().unwrap().into_vec(), *v)
            })
            .collect();

        // Sort by serialized bytes
        serialized_entries.sort_by(|a, b| a.0.cmp(&b.0));

        // Verify ordering is preserved
        let sorted_versions: Vec<Version> = serialized_entries.iter().map(|(_, v)| *v).collect();
        assert_eq!(sorted_versions, versions, "Version ordering should be preserved after serialization and sorting");
    }

    #[test]
    fn test_version_encoding_size_transitions() {
        // Verify the exact byte sizes at encoding boundaries
        assert_eq!(Version(252).size(), 1);
        assert_eq!(Version(253).size(), 3);
        assert_eq!(Version(65535).size(), 3);
        assert_eq!(Version(65536).size(), 5);
        assert_eq!(Version(u32::MAX as u64).size(), 5);
        assert_eq!(Version(u32::MAX as u64 + 1).size(), 9);
    }
}