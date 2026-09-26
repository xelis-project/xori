use std::{collections::HashSet, fmt::Display, ops::Bound, path::{Path, PathBuf}};
use futures::{Stream, stream};
use rocksdb::{ColumnFamilyDescriptor, DB, IteratorMode as RocksIteratorMode, Options, ReadOptions, SliceTransform};
use crate::{Serializable, SerializedBytes, backend::BackendError, engine::{IteratorDirection, IteratorMode}};
use super::{Backend, Column};

pub type RocksDBError = rocksdb::Error;

/// Database configuration. The engine supplies the column schema at build time.
pub struct RocksDBConfig {
    pub path: PathBuf,
    pub options: Options,
}

impl RocksDBConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into(), options: Options::default() }
    }
}

fn column_options(column: &Column) -> Options {
    let mut options = Options::default();
    if let Some(prefix_len) = column.properties().prefix_length {
        options.set_prefix_extractor(SliceTransform::create_fixed_prefix(prefix_len));
    }
    options
}

/// RocksDB backend implementation for persistent storage
pub struct RocksDBBackend {
    db: DB,
    columns: HashSet<Column>,
}

impl RocksDBBackend {
    /// Create a new RocksDB backend at the specified path
    /// The supplied schema must include all existing named column families.
    pub fn new(path: impl AsRef<Path>, columns: &[Column]) -> Result<Self, RocksDBError> {
        Self::with_options(path, Options::default(), columns)
    }

    /// Create a new RocksDB backend with custom options
    pub fn with_options(path: impl AsRef<Path>, mut options: rocksdb::Options, columns: &[Column]) -> Result<Self, RocksDBError> {
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let descriptors = columns.iter().map(|column|
            ColumnFamilyDescriptor::new(column.name(), column_options(column))
        );
        let db = DB::open_cf_descriptors(&options, path, descriptors)?;

        Ok(Self {
            db,
            // Track only columns successfully opened by this constructor.
            columns: columns.iter().cloned().collect(),
        })
    }
}

/// Helper function to serialize a Serializable value into bytes
#[inline]
fn serialize_data<'a, V: Serializable, E: Display>(data: &'a V) -> Result<SerializedBytes<'a>, BackendError<E>> {
    data.to_bytes()
        .map_err(BackendError::Writer)
}

impl Backend for RocksDBBackend {
    type Config = RocksDBConfig;
    type Error = RocksDBError;
    type RawBytes = Box<[u8]>;

    async fn open(config: Self::Config, columns: &[Column]) -> Result<Self, BackendError<Self::Error>> {
        Self::with_options(config.path, config.options, columns)
            .map_err(BackendError::Backend)
    }

    async fn open_column(&mut self, column: &Column) -> Result<(), BackendError<Self::Error>> {
        let name = column.name();
        if self.db.cf_handle(&name).is_none() {
            let opts = column_options(column);
            self.db.create_cf(&name, &opts)
                .map_err(BackendError::Backend)?;
        }

        // Column is a Arc<ColumnInner>, so we can safely insert it into the HashSet
        self.columns.insert(column.clone());
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

        let (lower, upper, direction) = mode.bounds();
        let mut options = ReadOptions::default();
        // These APIs support arbitrary byte ranges, including ranges spanning
        // multiple configured prefixes and reverse traversal.
        options.set_total_order_seek(true);
        if let Bound::Included(start) = lower {
            options.set_iterate_lower_bound(start.to_vec());
        }
        if let Bound::Excluded(end) = upper {
            options.set_iterate_upper_bound(end.to_vec());
        }
        let mode = match direction {
            IteratorDirection::Forward => RocksIteratorMode::Start,
            IteratorDirection::Backward => RocksIteratorMode::End,
        };
        let iter = self.db.iterator_cf_opt(cf, options, mode)
            .map(|res| res.map_err(BackendError::Backend));

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
        for column in &self.columns {
            let cf = self.db.cf_handle(column.name())
                .expect("Column family should exist since it is created in open_column");

            self.db.flush_cf(cf)
                .map_err(BackendError::Backend)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::TempDir;
    use crate::backend::{ColumnId, ColumnProperties, column::{ColumnInner, ColumnKind}};
    use super::*;

    #[tokio::test]
    async fn engine_build_opens_registered_schema_on_create_and_reopen() {
        use crate::{EntityConfig, Version, XoriBuilder};

        let dir = TempDir::new().unwrap();
        for reopening in [false, true] {
            let mut builder = XoriBuilder::new().register_entity::<u64>(EntityConfig {
                key_indexing: true, prefix_length: None,
            });
            let column = builder.register_column("custom", ColumnKind::Other,
                ColumnProperties { prefix_length: Some(8) });
            let mut engine = builder.build::<RocksDBBackend>(RocksDBConfig::new(dir.path()))
                .await.unwrap();
            if !reopening {
                engine.entity_handle_write::<u64>().unwrap().store(7u64, 42u64).await.unwrap();
                engine.write(&column, 1u64, 99u64).await.unwrap();
            }
            let entity = engine.entity_handle_read::<u64>().unwrap();
            assert_eq!(entity.last_version(&7u64).await.unwrap(), Some(Version::default()));
            assert_eq!(entity.read_at_version(&7u64, Version::default()).await.unwrap(), Some(42));
            assert_eq!(engine.read::<_, u64>(&column, 1u64).await.unwrap(), Some(99));
            engine.backend.backend.flush().await.unwrap();
            let files = engine.backend.backend.db.live_files().unwrap();
            for name in ["iterator_test", "iterator_test_k2i", "iterator_test_i2k", "custom"] {
                assert!(files.iter().any(|file| file.column_family_name == name));
            }
        }
        // A missing schema must be reported rather than opening a partial database.
        assert!(XoriBuilder::new().build::<RocksDBBackend>(RocksDBConfig::new(dir.path()))
            .await.is_err());
    }

    #[tokio::test]
    async fn dag_registers_columns_before_opening_database() {
        use crate::{DagEntryBuilder, DagState, XoriBuilder, dag::ReadResult};

        let dir = TempDir::new().unwrap();
        for reopening in [false, true] {
            let mut builder = XoriBuilder::new();
            let column = builder.register_column("data", ColumnKind::Other, Default::default());
            let mut dag = DagState::<u64, RocksDBBackend>::new(builder, RocksDBConfig::new(dir.path()))
                .await.unwrap();
            if !reopening {
                let mut entry = DagEntryBuilder::new(vec![]);
                entry.write(&column, &1u64, &42u64).unwrap();
                entry.commit(&mut dag, 10u64).await.unwrap();
            }
            assert!(dag.has_entry(&10).await.unwrap());
            assert!(matches!(dag.read::<_, u64>(&column, 1u64, &[10]).await.unwrap(),
                ReadResult::Stored(42, _)));
        }
    }

    #[tokio::test]
    async fn rocksdb_varuint_bounds_after_flush_and_reopen() {
        use crate::backend::tests::{seed_version_boundaries, check_version_boundaries};

        let dir = TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap();
        let columns: Vec<_> = [None, Some(1)].into_iter().enumerate().map(|(id, prefix_length)| {
            Arc::new(ColumnInner {
                name: format!("versions_{id}").into(),
                id: ColumnId(id as u64),
                kind: ColumnKind::Entity,
                properties: ColumnProperties { prefix_length },
            })
        }).collect();
        {
            let mut backend = RocksDBBackend::new(path, &columns).unwrap();
            for column in &columns {
                seed_version_boundaries(&mut backend, column).await;
                check_version_boundaries(&backend, column).await;
            }
            backend.flush().await.unwrap();
            let files = backend.db.live_files().unwrap();
            for column in &columns {
                assert!(files.iter().any(|file| file.column_family_name == column.name()),
                    "flush must persist the named column to an SST file");
                check_version_boundaries(&backend, column).await;
            }
        }
        // Exercise both public constructors without rewriting the persisted data.
        for custom_options in [false, true] {
            let mut backend = if custom_options {
                RocksDBBackend::with_options(path, Options::default(), &columns).unwrap()
            } else {
                RocksDBBackend::new(path, &columns).unwrap()
            };
            for column in &columns {
                backend.open_column(column).await.unwrap();
                check_version_boundaries(&backend, column).await;
            }
        }
    }

    #[tokio::test]
    async fn test_rocksdb_backend_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let mut backend = RocksDBBackend::new(temp_dir.path(), &[]).unwrap();

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
        let mut backend = RocksDBBackend::new(temp_dir.path(), &[]).unwrap();

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
