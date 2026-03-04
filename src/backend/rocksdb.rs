use std::fmt::Display;
use futures::{Stream, stream};
use itertools::Either;
use rocksdb::{DB, Direction, IteratorMode as RocksIteratorMode, Options, SliceTransform};
use crate::{Serializable, SerializedBytes, backend::BackendError, engine::{IteratorDirection, IteratorMode}};
use super::{Backend, Column};

pub type RocksDBError = rocksdb::Error;

/// RocksDB backend implementation for persistent storage
pub struct RocksDBBackend {
    db: DB,
}

impl RocksDBBackend {
    /// Create a new RocksDB backend at the specified path
    /// Uses the default column family for all data
    pub fn new(path: &str) -> Result<Self, RocksDBError> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, path)?;

        Ok(Self {
            db,
        })
    }

    /// Create a new RocksDB backend with custom options
    pub fn with_options(path: &str, mut options: rocksdb::Options) -> Result<Self, RocksDBError> {
        options.create_if_missing(true);
        let db = DB::open(&options, path)?;

        Ok(Self {
            db,
        })
    }
}

/// Helper function to serialize a Serializable value into bytes
#[inline]
fn serialize_data<'a, V: Serializable, E: Display>(data: &'a V) -> Result<SerializedBytes<'a>, BackendError<E>> {
    data.to_bytes()
        .map_err(BackendError::Writer)
}

#[inline]
fn map_direction(dir: IteratorDirection) -> Direction {
    match dir {
        IteratorDirection::Forward => Direction::Forward,
        IteratorDirection::Backward => Direction::Reverse,
    }
}

impl Backend for RocksDBBackend {
    type Error = RocksDBError;
    type RawBytes = Box<[u8]>;

    async fn open_column(&mut self, column: &Column) -> Result<(), BackendError<Self::Error>> {
        let name = column.name();
        if self.db.cf_handle(&name).is_none() {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            if let Some(prefix_len) = column.properties().prefix_length {
                opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_len));
            }
            self.db.create_cf(&name, &opts)
                .map_err(BackendError::Backend)?;
        }

        Ok(())
    }

    async fn write<K: Serializable + Send + Sync, V: Serializable + Send + Sync>(
        &mut self,
        column: &Column,
        key: K,
        data: V,
    ) -> Result<(), BackendError<Self::Error>> {
        let key_bytes = serialize_data(&key)?;
        let value_bytes = serialize_data(&data)?;

        let cf = self.db.cf_handle(column.name())
            .expect("Column family should exist since it is created in open_column");

        self.db.put_cf(cf, &key_bytes, &value_bytes)
            .map_err(BackendError::Backend)
    }

    async fn read<K: Serializable + Send + Sync>(
        &self,
        column: &Column,
        key: K,
    ) -> Result<Option<Self::RawBytes>, BackendError<Self::Error>> {
        let key_bytes = serialize_data(&key)?;

        let cf = self.db.cf_handle(column.name())
            .expect("Column family should exist since it is created in open_column");

        self.db.get_cf(cf, &key_bytes)
            .map(|opt| opt.map(Vec::into_boxed_slice))
            .map_err(BackendError::Backend)
    }

    async fn iterator<'a>(
        &'a self,
        column: &'a Column,
        mode: IteratorMode<'a>,
    ) -> Result<impl Stream<Item = Result<(Self::RawBytes, Self::RawBytes), BackendError<Self::Error>>> + 'a, BackendError<Self::Error>> {
        let cf = self.db.cf_handle(column.name())
            .expect("Column family should exist since it is created in open_column");

        let iter = match mode {
            IteratorMode::All(direction) => {
                let mode = match direction {
                    IteratorDirection::Forward => RocksIteratorMode::Start,
                    IteratorDirection::Backward => RocksIteratorMode::End,
                };
                let iter = self.db.iterator_cf(cf, mode);
                Either::Left(Either::Left(iter.map(|res| res
                    .map_err(BackendError::Backend))))
            },
            IteratorMode::Prefix(prefix, direction) => {
                let prefix_bytes = prefix.to_vec();
                let iter = self.db.iterator_cf(cf, RocksIteratorMode::From(prefix, map_direction(direction)))
                    .take_while(move |res| {
                        match res {
                            Ok((k, _)) => k.starts_with(&prefix_bytes),
                            Err(_) => true,
                        }
                    });
                Either::Left(Either::Right(iter.map(|res| res.map_err(BackendError::Backend))))
            },
            IteratorMode::Range { start, end, direction } => {
                let end_bytes = end.to_vec();
                let iter = self.db.iterator_cf(cf, RocksIteratorMode::From(start, map_direction(direction)))
                    .take_while(move |res| {
                        match res {
                            Ok((k, _)) => k.as_ref() < end_bytes.as_slice(),
                            Err(_) => true,
                        }
                    });
                Either::Right(Either::Left(iter.map(|res| res.map_err(BackendError::Backend))))
            },
            IteratorMode::From(start, direction) => {
                let iter = self.db.iterator_cf(cf, RocksIteratorMode::From(start, map_direction(direction)));
                Either::Right(Either::Right(iter.map(|res| res.map_err(BackendError::Backend))))
            },
        };

        Ok(stream::iter(iter))
    }

    async fn delete<K: Serializable + Send + Sync>(
        &mut self,
        column: &Column,
        key: K,
    ) -> Result<(), BackendError<Self::Error>> {
        let key_bytes = serialize_data(&key)?;

        let cf = self.db.cf_handle(column.name())
            .expect("Column family should exist since it is created in open_column");

        self.db.delete_cf(cf, &key_bytes)
            .map_err(BackendError::Backend)
    }

    async fn exists<K: Serializable + Send + Sync>(
        &self,
        column: &Column,
        key: K,
    ) -> Result<bool, BackendError<Self::Error>> {
        let key_bytes = serialize_data(&key)?;

        let cf = self.db.cf_handle(column.name())
            .expect("Column family should exist since it is created in open_column");

        self.db.get_cf(cf, &key_bytes)
            .map_err(BackendError::Backend)
            .map(|val| val.is_some())
    }

    async fn clear(&mut self) -> Result<(), BackendError<Self::Error>> {
        // Iterate through all keys and delete them
        let iter = self.db.iterator(RocksIteratorMode::Start);
        for res in iter {
            if let Ok((key, _)) = res {
                self.db.delete(&key)
                    .map_err(BackendError::Backend)?;
            }
        }
        
        Ok(())
    }

    async fn flush(&self) -> Result<(), BackendError<Self::Error>> {
        self.db.flush()
            .map_err(BackendError::Backend)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;
    use crate::backend::{ColumnId, ColumnProperties, column::{ColumnInner, ColumnKind}};
    use super::*;

    #[tokio::test]
    async fn test_rocksdb_backend_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let mut backend = RocksDBBackend::new(temp_dir.path().to_str().unwrap()).unwrap();

        let column = Arc::new(ColumnInner {
            name: "entity".into(),
            id: ColumnId(1),
            kind: ColumnKind::Entity,
            properties: ColumnProperties { prefix_length: Some(4) },
        });

        backend.open_column(&column).await.unwrap();

        // Test write and read
        backend.write(&column, &1u32, &42u64).await.unwrap();
        let result = backend.read(&column, &1u32).await.unwrap();
        assert!(result.is_some());

        // Test exists
        assert!(backend.exists(&column, &1u32).await.unwrap());
        assert!(!backend.exists(&column, &2u32).await.unwrap());

        // Test delete
        backend.delete(&column, &1u32).await.unwrap();
        assert!(!backend.exists(&column, &1u32).await.unwrap());
    }

    #[tokio::test]
    async fn test_rocksdb_backend_multiple_columns() {
        let temp_dir = TempDir::new().unwrap();
        let mut backend = RocksDBBackend::new(temp_dir.path().to_str().unwrap()).unwrap();

        let col1 = Arc::new(ColumnInner {
            name: "entity".into(),
            id: ColumnId(1),
            kind: ColumnKind::Entity,
            properties: ColumnProperties { prefix_length: Some(4) },
        });
        let col2 = Arc::new(ColumnInner {
            name: "index".into(),
            id: ColumnId(2),
            kind: ColumnKind::Index,
            properties: ColumnProperties { prefix_length: Some(4) },
        });

        backend.open_column(&col1).await.unwrap();
        backend.open_column(&col2).await.unwrap();

        backend.write(&col1, &1u32, &100u64).await.unwrap();
        backend.write(&col2, &1u32, &200u64).await.unwrap();

        let val1 = backend.read(&col1, &1u32).await.unwrap().unwrap();
        let val2 = backend.read(&col2, &1u32).await.unwrap().unwrap();

        // Verify values are serialized correctly by deserializing them
        let recovered1 = u64::from_bytes(&val1).unwrap();
        let recovered2 = u64::from_bytes(&val2).unwrap();
        
        assert_eq!(recovered1, 100u64);
        assert_eq!(recovered2, 200u64);
    }
}
