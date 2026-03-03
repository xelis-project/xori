use std::{borrow::Borrow, cmp::Ordering, hash::{Hash, Hasher}, ops::Deref};

use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum SerializedBytes<'a> {
    Borrowed(&'a [u8]),
    Owned(Box<[u8]>),
    Shared(Bytes),
}

impl<'a> SerializedBytes<'a> {
    /// Convert the data into a Vec<u8>, consuming self
    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        match self {
            SerializedBytes::Borrowed(bytes) => bytes.to_vec(),
            SerializedBytes::Owned(bytes) => bytes.into_vec(),
            SerializedBytes::Shared(bytes) => bytes.to_vec(),
        }
    }

    /// Convert the data into a Vec<u8> without consuming self
    #[inline]
    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            SerializedBytes::Borrowed(bytes) => bytes.to_vec(),
            SerializedBytes::Owned(bytes) => bytes.to_vec(),
            SerializedBytes::Shared(bytes) => bytes.to_vec(),
        }
    }

    /// Convert the data into a Box<[u8]> without consuming self
    #[inline]
    pub fn to_boxed_slice(&self) -> Box<[u8]> {
        match self {
            SerializedBytes::Borrowed(bytes) => (*bytes).into(),
            SerializedBytes::Owned(bytes) => bytes.clone(),
            SerializedBytes::Shared(bytes) => bytes.as_ref().into(),
        }
    }

    /// Convert the data into a Box<[u8]>, consuming self
    #[inline]
    pub fn into_boxed_slice(self) -> Box<[u8]> {
        match self {
            SerializedBytes::Borrowed(bytes) => bytes.into(),
            SerializedBytes::Owned(bytes) => bytes,
            SerializedBytes::Shared(bytes) => bytes.as_ref().into(),
        }
    }

    /// Convert into a Bytes, consuming self
    #[inline]
    pub fn into_bytes(self) -> Bytes {
        match self {
            SerializedBytes::Borrowed(bytes) => Bytes::copy_from_slice(bytes),
            SerializedBytes::Owned(bytes) => Bytes::copy_from_slice(&bytes),
            SerializedBytes::Shared(bytes) => bytes,
        }
    }

    /// Convert into a Bytes without consuming self
    #[inline]
    pub fn to_bytes(&self) -> Bytes {
        match self {
            SerializedBytes::Borrowed(bytes) => Bytes::copy_from_slice(bytes),
            SerializedBytes::Owned(bytes) => Bytes::copy_from_slice(&bytes),
            SerializedBytes::Shared(bytes) => bytes.clone(),
        }
    }
}

impl<'a> AsRef<[u8]> for SerializedBytes<'a> {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        match self {
            SerializedBytes::Borrowed(bytes) => bytes,
            SerializedBytes::Owned(bytes) => bytes,
            SerializedBytes::Shared(bytes) => bytes.as_ref(),
        }
    }
}

impl<'a> Deref for SerializedBytes<'a> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<'a> From<Vec<u8>> for SerializedBytes<'a> {
    #[inline]
    fn from(vec: Vec<u8>) -> Self {
        SerializedBytes::Owned(vec.into_boxed_slice())
    }
}

impl<'a> From<Bytes> for SerializedBytes<'a> {
    #[inline]
    fn from(bytes: Bytes) -> Self {
        SerializedBytes::Shared(bytes)
    }
}

impl<'a> From<&'a [u8]> for SerializedBytes<'a> {
    #[inline]
    fn from(bytes: &'a [u8]) -> Self {
        SerializedBytes::Borrowed(bytes)
    }
}

impl<'a> Borrow<[u8]> for SerializedBytes<'a> {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

impl Hash for SerializedBytes<'_> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl PartialEq for SerializedBytes<'_> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for SerializedBytes<'_> {}

impl PartialOrd for SerializedBytes<'_> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_ref().partial_cmp(other.as_ref())
    }
}

impl Ord for SerializedBytes<'_> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl<'a> Into<Bytes> for SerializedBytes<'a> {
    #[inline]
    fn into(self) -> Bytes {
        self.into_bytes()
    }
}

impl<'a> Into<Vec<u8>> for SerializedBytes<'a> {
    #[inline]
    fn into(self) -> Vec<u8> {
        self.into_vec()
    }
}

impl<'a> Into<Box<[u8]>> for SerializedBytes<'a> {
    #[inline]
    fn into(self) -> Box<[u8]> {
        self.into_boxed_slice()
    }
}