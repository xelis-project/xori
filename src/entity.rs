mod read;
mod write;

use crate::{
    Backend, Key, KeyIndex, Result, Serializable, Version, backend::Column, engine::XoriBackend
};
pub use read::EntityReadHandle;
pub use write::EntityWriteHandle;

/// Trait for types that can be stored in Xori
pub trait Entity: Send + Sync + Serializable + Clone + 'static {
    /// Type identifier for this entity
    fn entity_name() -> &'static str;
}

/// Search bias for binary search over entity versions
pub enum SearchBias {
    First,
    Lowest,
    Highest,
}

/// Versioned entry for storing entity data along with its version
#[derive(Debug, Clone)]
pub(crate) struct KeyIndexColumn {
    pub(crate) key_to_id: Column,
    pub(crate) id_to_key: Column,
}

/// Get the key for a raw key, if key indexing is enabled
/// Returns None only if key indexing is enabled but the key does not exist
#[inline]
pub(crate) async fn fetch_key_index<K: Serializable + Send + Sync, B: Backend>(backend: &XoriBackend<B>, index: Option<&KeyIndexColumn>, key: K) -> Result<Option<Key<K>>, B::Error> {
    match index {
        Some(index) => backend.read::<_, KeyIndex>(&index.key_to_id, key).await
            .map(|opt| opt.map(Key::Indexed)),
        None => Ok(Some(Key::Raw(key))),
    }
}

/// Get the latest version for the specified mapped key
#[inline(always)]
pub(crate) async fn version<K: Serializable + Send + Sync, B: Backend>(backend: &XoriBackend<B>, column: &Column, key: &K) -> Result<Option<Version>, B::Error> {
    backend.read(column, key).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryBackend, VersionedKey, XoriBuilder, builder::EntityConfig};
    use std::cmp::Ordering;

    #[derive(Debug, Clone, PartialEq)]
    struct TestEntity {
        value: u64,
    }

    impl Entity for TestEntity {
        fn entity_name() -> &'static str {
            "test_entity"
        }
    }

    impl Serializable for TestEntity {
        fn write<W: crate::Writable>(&self, writer: &mut W) -> std::result::Result<(), crate::WriterError> {
            self.value.write(writer)
        }

        fn read(reader: &mut crate::Reader) -> std::result::Result<Self, crate::ReaderError> {
            Ok(TestEntity {
                value: u64::read(reader)?,
            })
        }

        fn size(&self) -> usize {
            self.value.size()
        }
    }

    async fn setup_entity_handle() -> EntityWriteHandle<'static, TestEntity, MemoryBackend> {
        let backend = MemoryBackend::new();
        let engine = XoriBuilder::new()
            .register_entity::<TestEntity>(EntityConfig { key_indexing: true, prefix_length: None })
            .build(backend).await.unwrap();

        let engine = Box::leak(Box::new(engine));
        engine.entity_handle_write::<TestEntity>().unwrap()
    }

    #[tokio::test]
    async fn test_binary_search_empty_history() {
        let handle = setup_entity_handle().await;
        let key = 1u32;
        // Search in empty history
        let result = handle
            .downgrade()
            .binary_search_with_bias(
                &key,
                Version(10),
                |v, _| if v.0 <= 5 { Ordering::Less } else { Ordering::Greater },
                SearchBias::Lowest,
            )
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_binary_search_single_version_match() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;
        // Store a single version
        handle.store(key, TestEntity { value: 100 }).await.unwrap();

        // Search for exact match
        let result = handle
            .downgrade()
            .binary_search_with_bias(
                &key,
                Version(0),
                |_, entity| if entity.unwrap().value == 100 { Ordering::Equal } else { Ordering::Less },
                SearchBias::Lowest,
            )
            .await
            .unwrap();

        assert!(result.is_some());
        let (entity, version) = result.unwrap();
        assert_eq!(entity.unwrap().value, 100);
        assert_eq!(version, Version(0));
    }

    #[tokio::test]
    async fn test_binary_search_multiple_versions_exact_match() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;
        // Store multiple versions
        for i in 0..10 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Search for entity with value 50
        let result = handle
            .downgrade()
            .binary_search_with_bias(
                &key,
                Version(9),
                |_, entity| {
                    let entity = entity.unwrap();
                    if entity.value < 50 {
                        Ordering::Greater
                    } else if entity.value > 50 {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                },
                SearchBias::Lowest,
            )
            .await
            .unwrap();

        assert!(result.is_some());
        let (entity, version) = result.unwrap();
        assert_eq!(entity.unwrap().value, 50);
        assert_eq!(version, Version(5));
    }

    #[tokio::test]
    async fn test_binary_search_lowest_bias_multiple_matches() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;
        // Store versions where multiple have value >= 50
        handle.store(key, TestEntity { value: 10 }).await.unwrap();  // v0
        handle.store(key, TestEntity { value: 20 }).await.unwrap();  // v1
        handle.store(key, TestEntity { value: 30 }).await.unwrap();  // v2
        handle.store(key, TestEntity { value: 50 }).await.unwrap();  // v3
        handle.store(key, TestEntity { value: 50 }).await.unwrap();  // v4
        handle.store(key, TestEntity { value: 50 }).await.unwrap();  // v5
        handle.store(key, TestEntity { value: 70 }).await.unwrap();  // v6

        // Search for value >= 50, should return lowest matching version
        let result = handle
            .downgrade()
            .binary_search_with_bias(
                &key,
                Version(6),
                |_, entity| {
                    let entity = entity.unwrap();
                    if entity.value < 50 {
                        Ordering::Greater
                    } else {
                        Ordering::Equal
                    }
                },
                SearchBias::Lowest,
            )
            .await
            .unwrap();

        assert!(result.is_some());
        let (entity, version) = result.unwrap();
        assert_eq!(entity.unwrap().value, 50);
        assert_eq!(version, Version(3)); // Should find the first occurrence
    }

    #[tokio::test]
    async fn test_binary_search_highest_bias_multiple_matches() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store versions where multiple have value >= 50
        handle.store(key, TestEntity { value: 10 }).await.unwrap();  // v0
        handle.store(key, TestEntity { value: 20 }).await.unwrap();  // v1
        handle.store(key, TestEntity { value: 30 }).await.unwrap();  // v2
        handle.store(key, TestEntity { value: 50 }).await.unwrap();  // v3
        handle.store(key, TestEntity { value: 50 }).await.unwrap();  // v4
        handle.store(key, TestEntity { value: 50 }).await.unwrap();  // v5
        handle.store(key, TestEntity { value: 70 }).await.unwrap();  // v6

        // Search for value == 50, should return highest matching version
        let result = handle
            .downgrade()
            .binary_search_with_bias(
                &key,
                Version(6),
                |_, entity| {
                    let entity = entity.unwrap();
                    if entity.value < 50 {
                        Ordering::Greater
                    } else if entity.value > 50 {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                },
                SearchBias::Highest,
            )
            .await
            .unwrap();

        assert!(result.is_some());
        let (entity, version) = result.unwrap();
        assert_eq!(entity.unwrap().value, 50);
        assert_eq!(version, Version(5)); // Should find the last occurrence of value 50
    }

    #[tokio::test]
    async fn test_binary_search_no_match() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store versions with values 0, 10, 20, ..., 90
        for i in 0..10 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Search for value 55 (doesn't exist)
        let result = handle
            .downgrade()
            .binary_search_with_bias(
                &key,
                Version(9),
                |_, entity| {
                    let entity = entity.unwrap();
                    if entity.value < 55 {
                        Ordering::Greater
                    } else if entity.value > 55 {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                },
                SearchBias::Lowest,
            )
            .await
            .unwrap();

        // Should return None as no exact match exists
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_binary_search_boundary_at_start() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store multiple versions
        for i in 0..5 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Search for first version (value == 0)
        let result = handle
            .downgrade()
            .binary_search_with_bias(
                &key,
                Version(4),
                |_, entity| {
                    let entity = entity.unwrap();
                    if entity.value > 0 {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                },
                SearchBias::Lowest,
            )
            .await
            .unwrap();

        assert!(result.is_some());
        let (entity, version) = result.unwrap();
        assert_eq!(entity.unwrap().value, 0);
        assert_eq!(version, Version(0));
    }

    #[tokio::test]
    async fn test_binary_search_boundary_at_end() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store multiple versions
        for i in 0..5 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Search for last version
        let result = handle
            .downgrade()
            .binary_search_with_bias(
                &key,
                Version(4),
                |_, entity| {
                    let entity = entity.unwrap();
                    if entity.value < 40 {
                        Ordering::Greater
                    } else if entity.value > 40 {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                },
                SearchBias::Highest,
            )
            .await
            .unwrap();

        assert!(result.is_some());
        let (entity, version) = result.unwrap();
        assert_eq!(entity.unwrap().value, 40);
        assert_eq!(version, Version(4));
    }

    #[tokio::test]
    async fn test_binary_search_with_condition() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store versions with increasing values
        for i in 0..20 {
            handle.store(key, TestEntity { value: i }).await.unwrap();
        }

        // Search for first version where value >= 15
        let result = handle
            .downgrade()
            .binary_search_with_bias(
                &key,
                Version(19),
                |_, entity| {
                    let entity = entity.unwrap();
                    if entity.value < 15 {
                        Ordering::Greater // Need to go higher
                    } else {
                        Ordering::Equal // Found a match
                    }
                },
                SearchBias::Lowest,
            )
            .await
            .unwrap();

        assert!(result.is_some());
        let (entity, version) = result.unwrap();
        assert_eq!(entity.unwrap().value, 15);
        assert_eq!(version, Version(15));
    }

    #[tokio::test]
    async fn test_binary_search_all_versions_too_small() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store versions with small values
        for i in 0..5 {
            handle.store(key, TestEntity { value: i }).await.unwrap();
        }

        // Search for value >= 100 (all values are too small)
        let result = handle
            .downgrade()
            .binary_search_with_bias(
                &key,
                Version(4),
                |_, entity| {
                    let entity = entity.unwrap();
                    if entity.value < 100 {
                        Ordering::Greater
                    } else {
                        Ordering::Equal
                    }
                },
                SearchBias::Lowest,
            )
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_binary_search_all_versions_too_large() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store versions with large values
        for i in 10..15 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Search for value <= 50 (all values are too large)
        let result = handle
            .downgrade()
            .binary_search_with_bias(
                &key,
                Version(4),
                |_, entity| {
                    let entity = entity.unwrap();
                    if entity.value > 50 {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                },
                SearchBias::Lowest,
            )
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_binary_search_nonexistent_key() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;
        let nonexistent_key = 999u32;

        // Store some versions for key 1
        for i in 0..5 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Search with a key that doesn't exist
        let result = handle
            .downgrade()
            .binary_search_with_bias(
                &nonexistent_key,
                Version(4),
                |_, entity| if entity.unwrap().value == 20 { Ordering::Equal } else { Ordering::Less },
                SearchBias::Lowest,
            )
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_binary_search_version_based_condition() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store versions
        for i in 0..10 {
            handle.store(key, TestEntity { value: i * 5 }).await.unwrap();
        }

        // Search for version >= 5 (using version in comparison)
        let result = handle
            .downgrade()
            .binary_search_with_bias(
                &key,
                Version(9),
                |v, _| {
                    if v.0 < 5 {
                        Ordering::Greater
                    } else {
                        Ordering::Equal
                    }
                },
                SearchBias::Lowest,
            )
            .await
            .unwrap();

        assert!(result.is_some());
        let (_, version) = result.unwrap();
        assert_eq!(version, Version(5));
    }

    #[tokio::test]
    async fn test_delete_until_version_nonexistent_key() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Try to delete versions for a key that doesn't exist
        let result = handle
            .delete_until_version(&key, Version(5))
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_until_version_empty_history() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store then try to delete from a version that never existed
        handle.store(key, TestEntity { value: 100 }).await.unwrap();
        let result = handle
            .delete_until_version(&key, Version(5))
            .await;

        assert!(result.is_ok());
        // Original version should still be readable
        let entity = handle.downgrade().read_at_version(&key, Version(0)).await.unwrap().unwrap();
        assert_eq!(entity.value, 100);
    }

    #[tokio::test]
    async fn test_delete_until_version_single_version() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store a single version
        handle.store(key, TestEntity { value: 100 }).await.unwrap();
        
        // Deleting with version 0 should keep version 0 (only deletes versions > 0)
        handle.delete_until_version(&key, Version(0)).await.unwrap();

        // Version 0 should still exist
        let result = handle.downgrade().read_at_version(&key, Version(0)).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, 100);
    }

    #[tokio::test]
    async fn test_delete_until_version_middle() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store multiple versions (0, 1, 2, 3, 4)
        for i in 0..5 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Delete versions > 2 (only keep 0, 1, 2)
        handle.delete_until_version(&key, Version(2)).await.unwrap();

        let handle = handle.downgrade();
        // Latest version should now be 2
        let version_opt = handle.last_version(&key).await.unwrap();
        assert_eq!(version_opt, Some(Version(2)));
        
        // Versions 0, 1, 2 should still exist
        for v in 0..=2 {
            let result = handle.read_at_version(&key, Version(v)).await.unwrap();
            assert!(result.is_some());
            assert_eq!(result.unwrap().value, v as u64 * 10);
        }

        // Versions 3, 4 should not exist
        for v in 3..5 {
            assert!(handle.read_at_version(&key, Version(v)).await.is_err());
        }
    }

    #[tokio::test]
    async fn test_delete_until_version_all_versions() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store multiple versions
        for i in 0..5 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Delete versions > some very high version number (nothing gets deleted)
        handle.delete_until_version(&key, Version(10)).await.unwrap();

        let handle = handle.downgrade();
        // All versions should still exist since we're keeping up to version 10
        for v in 0..5 {
            let result = handle.read_at_version(&key, Version(v)).await.unwrap();
            assert!(result.is_some());
        }
    }

    #[tokio::test]
    async fn test_delete_until_version_at_boundary() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store versions 0, 1, 2, 3
        for i in 0..4 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Delete versions > 2 (keep 0, 1, 2)
        handle.delete_until_version(&key, Version(2)).await.unwrap();

        let handle = handle.downgrade();
        // Latest version should be 2
        let version_opt = handle.last_version(&key).await.unwrap();
        assert_eq!(version_opt, Some(Version(2)));
        let entity = handle.read_at_version(&key, Version(2)).await.unwrap().unwrap();
        assert_eq!(entity.value, 20); // Version 2 had value 20

        // Versions 0, 1, 2 should exist
        for v in 0..=2 {
            let result = handle.read_at_version(&key, Version(v)).await.unwrap();
            assert!(result.is_some());
        }

        // Version 3 should not exist
        assert!(handle.read_at_version(&key, Version(3)).await.is_err());
    }

    #[tokio::test]
    async fn test_delete_until_version_beyond_max() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store versions 0, 1, 2
        for i in 0..3 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Delete versions > 10 (nothing happens since max is 2)
        handle.delete_until_version(&key, Version(10)).await.unwrap();

        let handle = handle.downgrade();
        // All versions should still exist
        let version_opt = handle.last_version(&key).await.unwrap();
        assert_eq!(version_opt, Some(Version(2)));
        
        for v in 0..3 {
            let result = handle.read_at_version(&key, Version(v)).await.unwrap();
            assert!(result.is_some());
        }
    }

    #[tokio::test]
    async fn test_delete_until_version_multiple_keys() {
        let mut handle = setup_entity_handle().await;

        // Store versions for multiple keys
        for key in 1u32..=3u32 {
            for i in 0..5 {
                handle.store(key, TestEntity { value: key as u64 * 100 + i }).await.unwrap();
            }
        }

        // Delete versions > 2 from key 2 (keep 0, 1, 2)
        handle.delete_until_version(&2u32, Version(2)).await.unwrap();

        let handle = handle.downgrade();
        // Key 2 should have version 2 as latest
        let version_opt = handle.last_version(&2u32).await.unwrap();
        assert_eq!(version_opt, Some(Version(2)));

        // Key 1 and key 3 should be unaffected (still at version 4)
        let v1_opt = handle.last_version(&1u32).await.unwrap();
        assert_eq!(v1_opt, Some(Version(4)));

        let v3_opt = handle.last_version(&3u32).await.unwrap();
        assert_eq!(v3_opt, Some(Version(4)));
    }

    #[tokio::test]
    async fn test_delete_until_version_with_gaps() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store versions 0, 1, 2, 3, 4
        for i in 0..5 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Delete version 3 specifically
        let versioned_key = VersionedKey {
            version: Version(3),
            key: &key,
        };
        handle.backend.delete(&handle.info.column, &versioned_key).await.unwrap();

        // Now delete versions > 1 (keep 0, 1)
        handle.delete_until_version(&key, Version(1)).await.unwrap();

        let handle = handle.downgrade();
        // Latest version should be 1
        let version_opt = handle.last_version(&key).await.unwrap();
        assert_eq!(version_opt, Some(Version(1)));

        // Versions 0, 1 should exist
        assert!(handle.read_at_version(&key, Version(0)).await.unwrap().is_some());
        assert!(handle.read_at_version(&key, Version(1)).await.unwrap().is_some());

        // Versions 2, 3, 4 should not exist
        assert!(handle.read_at_version(&key, Version(2)).await.is_err());
        assert!(handle.read_at_version(&key, Version(3)).await.is_err());
        assert!(handle.read_at_version(&key, Version(4)).await.is_err());
    }

    #[tokio::test]
    async fn test_delete_until_version_preserves_earlier_versions() {
        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store 10 versions with different values
        for i in 0..10 {
            handle.store(key, TestEntity { value: i * 100 }).await.unwrap();
        }

        // Delete versions > 4 (keep 0-4)
        handle.delete_until_version(&key, Version(4)).await.unwrap();

        let handle = handle.downgrade();
        // Check that earlier versions are preserved
        for v in 0..=4 {
            let entity = handle.read_at_version(&key, Version(v)).await.unwrap().unwrap();
            assert_eq!(entity.value, (v as u64) * 100);
        }

        // Check that deleted versions don't exist
        for v in 5..10 {
            assert!(handle.read_at_version(&key, Version(v)).await.is_err());
        }

        // Latest version should be 4
        let version_opt = handle.last_version(&key).await.unwrap();
        assert_eq!(version_opt, Some(Version(4)));
    }

    #[tokio::test]
    async fn test_list_keys_empty() {
        use futures::StreamExt;

        let handle = setup_entity_handle().await;
        let handle = handle.downgrade();

        // List keys when no data exists
        let stream = handle.list_keys::<u32>().await.unwrap();
        let keys: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(keys.len(), 0);
    }

    #[tokio::test]
    async fn test_list_keys_single_key() {
        use futures::StreamExt;

        let mut handle = setup_entity_handle().await;
        let key = 42u32;

        // Store a single entity
        handle.store(key, TestEntity { value: 100 }).await.unwrap();

        let handle = handle.downgrade();
        let stream = handle.list_keys::<u32>().await.unwrap();
        let keys: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].as_ref().unwrap(), &key);
    }

    #[tokio::test]
    async fn test_list_keys_multiple_keys() {
        use futures::StreamExt;

        let mut handle = setup_entity_handle().await;
        
        // Store entities with different keys
        let test_keys = vec![1u32, 5, 10, 15, 20];
        for &key in &test_keys {
            handle.store(key, TestEntity { value: key as u64 * 10 }).await.unwrap();
        }

        let handle = handle.downgrade();
        let stream = handle.list_keys::<u32>().await.unwrap();
        let mut keys: Vec<u32> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Sort for comparison since order may vary
        keys.sort();

        assert_eq!(keys.len(), test_keys.len());
        assert_eq!(keys, test_keys);
    }

    #[tokio::test]
    async fn test_list_keys_multiple_versions_single_key() {
        use futures::StreamExt;

        let mut handle = setup_entity_handle().await;
        let key = 1u32;

        // Store multiple versions of the same key
        for i in 0..5 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        let handle = handle.downgrade();
        let stream = handle.list_keys::<u32>().await.unwrap();
        let keys: Vec<_> = stream.collect::<Vec<_>>().await;

        // Should still return only one key, not multiple entries for each version
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].as_ref().unwrap(), &key);
    }

    #[tokio::test]
    async fn test_list_keys_after_deletion() {
        use futures::StreamExt;

        let mut handle = setup_entity_handle().await;
        
        // Store multiple entities
        let keys_to_store = vec![1u32, 2, 3, 4, 5];
        for &key in &keys_to_store {
            handle.store(key, TestEntity { value: key as u64 }).await.unwrap();
        }

        // Delete one key completely
        handle.delete(&3u32).await.unwrap();

        let handle = handle.downgrade();
        let stream = handle.list_keys::<u32>().await.unwrap();
        let mut keys: Vec<u32> = stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        keys.sort();

        // Should have 4 keys remaining (1, 2, 4, 5)
        assert_eq!(keys.len(), 4);
        assert_eq!(keys, vec![1, 2, 4, 5]);
    }
}
