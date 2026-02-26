use std::borrow::Cow;

use thiserror::Error;

/// Errors that can occur while reading from a byte slice
#[derive(Error, Debug)]
pub enum ReaderError {
    #[error("Unexpected value")]
    UnexpectedValue,
    #[error("Data is not serializable")]
    NotSerializable,
    #[error("Requested {requested} bytes but only {available} available")]
    OutOfBounds {
        requested: usize,
        available: usize,
    },
    #[error("Failed to convert bytes")]
    ErrorTryInto,
    #[error(transparent)]
    Any(#[from] anyhow::Error),
}

/// Reader for deserializing entities from a byte slice
pub struct Reader<'a> {
    data: Cow<'a, [u8]>,
    total: usize,
}

impl<'a> Reader<'a> {
    /// Create a new reader from a byte slice or owned byte vector
    #[inline(always)]
    pub fn new(data: impl Into<Cow<'a, [u8]>>) -> Self {
        Self {
            data: data.into(),
            total: 0
        }
    }

    /// Get all the bytes provided to this reader
    #[inline(always)]
    pub fn bytes<'b>(&'b self) -> &'b [u8] {
        &self.data
    }

    /// Get the bytes that have not yet been read by this reader
    pub fn read_bytes_left(&mut self) -> &[u8] {
        let tmp = self.total;
        self.total = self.data.len();
        &self.data[tmp..]
    }

    /// Get the total number of bytes that have been read so far
    #[inline(always)]
    pub fn total_read(&self) -> usize {
        self.total
    }

    /// Check if there are more bytes left to read
    #[inline(always)]
    pub fn has_more(&self) -> bool {
        self.total < self.data.len()
    }

    /// Get the number of bytes remaining to be read (alias for bytes_remaining)
    #[inline(always)]
    pub fn remaining(&self) -> usize {
        self.data.len() - self.total
    }

    /// Read the next byte from the reader
    pub fn next_byte(&mut self) -> Result<u8, ReaderError> {
        if self.total >= self.data.len() {
            return Err(ReaderError::OutOfBounds {
                requested: 1,
                available: 0,
            });
        }

        let byte = self.data[self.total];
        self.total += 1;
        Ok(byte)
    }

    /// Read a specific number of bytes and return them as a slice
    pub fn read_bytes<T>(&mut self, n: usize) -> Result<T, ReaderError>
    where
        T: for<'b> TryFrom<&'b [u8]>
    {
        if n > self.remaining() {
            return Err(ReaderError::OutOfBounds {
                requested: n,
                available: self.remaining(),
            });
        }

        let result = match self.data[self.total..self.total+n].try_into() {
            Ok(v) => {
                Ok(v)
            },
            Err(_) => Err(ReaderError::ErrorTryInto)
        };

        self.total += n;
        result
    }

    /// Read a specific number of bytes and return them as a slice reference
    pub fn read_bytes_ref(&mut self, n: usize) -> Result<&[u8], ReaderError> {
        if n > self.remaining() {
            return Err(ReaderError::OutOfBounds {
                requested: n,
                available: self.remaining(),
            });
        }

        let bytes = &self.data[self.total..self.total+n];
        self.total += n;
        Ok(bytes)
    }
}