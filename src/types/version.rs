use serde::{Deserialize, Serialize};

use crate::{Reader, ReaderError, Serializable, Writable, WriterError};


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
        self.0.write(writer)
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        let value = u64::read(reader)?;
        Ok(Version(value))
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

/// Represents a single value entry with version information
#[derive(Debug, Clone)]
pub struct VersionedEntry<E: Serializable> {
    pub version: Version,
    pub data: E,
}

impl<E: Serializable> Serializable for VersionedEntry<E> {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        self.version.write(writer)?;
        self.data.write(writer)
    }

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        let version = Version::read(reader)?;
        let data = E::read(reader)?;
        Ok(Self { version, data })
    }

    fn size(&self) -> usize {
        self.version.size() + self.data.size()
    }
}