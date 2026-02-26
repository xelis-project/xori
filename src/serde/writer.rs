use thiserror::Error;

/// Errors that can occur during serialization of entities
#[derive(Error, Debug)]
pub enum WriterError {
    #[error(transparent)]
    Any(#[from] anyhow::Error),   
}

/// Trait for writing serialized data to a byte buffer or stream
pub trait Writable {
    /// Write a single byte to the output
    fn push(&mut self, byte: u8) {
        self.extend_bytes(&[byte]);
    }

    /// Extend the current byte buffer with additional bytes
    fn extend_bytes(&mut self, bytes: &[u8]);

    /// Pre-allocate space for additional bytes to optimize writes
    /// Returns true if pre-allocation was successful, false otherwise
    /// This is a hint to the underlying storage to optimize for the expected size
    fn pre_allocate(&mut self, additional: usize) -> bool;
}

impl Writable for Vec<u8> {
    fn extend_bytes(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }

    fn pre_allocate(&mut self, additional: usize) -> bool {
        self.reserve(additional);
        true
    }
}