use futures::{Stream, stream};

use crate::{
    EntityMetadata,
    Key,
    KeyIndex,
    Serializable,
    Version,
    VersionedEntry,
    XoriEngine,
    backend::{Backend, BackendError, Column, Result}
};
use std::{cmp::Ordering, marker::PhantomData, sync::Arc};

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
#[derive(Debug, Clone, Copy)]
pub(crate) struct KeyIndexColumn {
    pub(crate) key_to_id: Column,
    pub(crate) id_to_key: Column,
}

/// Entity handle for managing a specific entity type with versioning
pub struct EntityHandle<E: Entity, B: Backend> {
    pub(crate) column: Column,
    pub(crate) key_index_column: Option<KeyIndexColumn>,
    pub(crate) engine: Arc<XoriEngine<B>>,
    pub(crate) _phantom: PhantomData<E>,
}

impl<E: Entity, B: Backend> Clone for EntityHandle<E, B> {
    fn clone(&self) -> Self {
        Self {
            column: self.column,
            key_index_column: self.key_index_column,
            engine: Arc::clone(&self.engine),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<E: Entity, B: Backend> EntityHandle<E, B> {
    /// Read the entity metadata
    /// TODO: cache it
    #[inline(always)]
    async fn metadata(&self) -> Result<EntityMetadata, BackendError<B::Error>> {
        self.engine.read(self.column, &()).await
            .map(Option::unwrap_or_default)
    }

    /// Update the entity metadata
    #[inline(always)]
    async fn update_metadata(&self, metadata: &EntityMetadata) -> Result<(), BackendError<B::Error>> {
        self.engine.write(self.column, &(), metadata).await
    }

    /// Map a raw key to either a raw key or an indexed key, depending on whether key indexing is enabled
    async fn get_or_create_key<K: Serializable + Send + Sync>(&self, key: K) -> Result<Key<K>, BackendError<B::Error>> {
        match self.key_index_column {
            Some(index) => {
                let id = self.engine.read::<_, KeyIndex>(index.key_to_id, &key).await
                    .map(|opt| opt.map(Key::Indexed))?;

                match id {
                    Some(key) => Ok(key),
                    None => {
                        let mut metadata = self.metadata().await?;
                        let key_index = metadata.next_key_index();

                        // Store the new key index mapping
                        self.engine.write(index.key_to_id, &key, &key_index).await?;
                        self.engine.write(index.id_to_key, &key_index, &key).await?;

                        self.update_metadata(&metadata).await?;

                        Ok(Key::Indexed(key_index))
                    }

                }
            },
            None => Ok(Key::Raw(key)),
        }
    }

    /// Get the key for a raw key, if key indexing is enabled
    /// Returns None only if key indexing is enabled but the key does not exist
    async fn get_key<K: Serializable + Send + Sync>(&self, key: K) -> Result<Option<Key<K>>, BackendError<B::Error>> {
        match self.key_index_column {
            Some(index) => self.engine.read::<_, KeyIndex>(index.key_to_id, key).await
                .map(|opt| opt.map(Key::Indexed)),
            None => Ok(Some(Key::Raw(key))),
        }
    }

    /// Get the latest version for the specified mapped key
    #[inline(always)]
    async fn version<K: Serializable + Send + Sync>(&self, key: &K) -> Result<Option<Version>, BackendError<B::Error>> {
        self.engine.read(self.column, key).await
    }

    /// Get the latest version for the specified key, using the key index if available
    pub async fn last_version<K: Serializable + Send + Sync>(&self, key: &K) -> Result<Option<Version>, BackendError<B::Error>> {
        match self.get_key(key).await? {
            Some(mapped_key) => self.version(&mapped_key).await,
            None => Ok(None),
        }
    }

    /// Update the latest version for a key
    async fn update_version<K: Serializable + Send + Sync>(&self, key: &K, version: Version) -> Result<(), BackendError<B::Error>> {
        self.engine.write(self.column, key, &version).await
    }

    /// Store a new version of the entity for the given key
    pub async fn store<K: Serializable + Send + Sync>(&self, key: K, value: E) -> Result<(), BackendError<B::Error>> {
        // Map the key if required
        let key = self.get_or_create_key::<K>(key).await?;

        // load the current version and increment it for the new entry
        let version = self.version(&key).await?
            .map(Version::next)
            .unwrap_or_default();

        let versioned = VersionedEntry {
            version,
            data: &key,
        };

        self.engine.write(self.column, &versioned, &value).await?;

        // Update the latest version for the key
        self.update_version(&key, version).await
    }

    /// Delete versions at or above the specified version for the given key
    /// This is used for rolling back to a previous version by deleting all newer versions
    pub async fn delete_until_version<K: Serializable + Send + Sync>(&self, key: &K, version: Version) -> Result<(), BackendError<B::Error>> {
        match self.get_key(key).await? {
            Some(mapped_key) => {
                let Some(last_version) = self.version(&mapped_key).await? else {
                    return Ok(()); // No versions exist, nothing to delete
                };

                if last_version <= version {
                    return Ok(()); // Latest version is already older than the specified version, nothing to delete
                }

                // Iterate over versions starting from the specified version and delete them
                let mut current_version = Some(last_version);
                let mut updated = false;
                while let Some(v) = current_version {
                    if v <= version {
                        // Update the latest version to the last existing version before deletion
                        self.update_version(&mapped_key, v).await?;
                        updated = true;
                        break;
                    }

                    let versioned_key = VersionedEntry {
                        version: v,
                        data: &mapped_key,
                    };

                    self.engine.delete(self.column, &versioned_key).await?;

                    // Move to the next version
                    current_version = v.previous();
                }

                if !updated {
                    // If we deleted all versions, update the latest version to None
                    self.engine.delete(self.column, &mapped_key).await?;
                }

                Ok(())
            },
            None => Ok(()), // If key doesn't exist, nothing to delete
        }
    }

    /// Read a specific version of the entity for the given key
    pub async fn read_at_version<K: Serializable + Send + Sync>(&self, key: &K, version: Version) -> Result<Option<E>, BackendError<B::Error>> {
        Ok(match self.get_key(key).await? {
            Some(mapped_key) => {
                let versioned_key = VersionedEntry {
                    version,
                    data: &mapped_key,
                };

                self.engine.read::<_, E>(self.column, &versioned_key).await?
            },
            None => None,
        })
    }

    /// Get the entire history of versions for a given key as a stream of (value, version) pairs, starting from the latest version and going backwards
    pub async fn history<'a, K: Serializable + Send + Sync>(&'a self, key: &'a K) -> Result<impl Stream<Item = Result<(E, Version), BackendError<B::Error>>> + 'a, BackendError<B::Error>> {
        Ok(stream::unfold(
            match self.get_key(key).await? {
                Some(mapped_key) => self.version(&mapped_key).await?
                    .map(|version| (mapped_key, version)),
                None => None,
            },
            move |state| async move {
                let (key, version) = state?;

                let versioned_key = VersionedEntry {
                    version,
                    data: &key,
                };

                match self.engine.read::<_, E>(self.column, &versioned_key).await {
                    Ok(Some(value)) => {
                        let next_version = version.previous();
                        Some((Ok((value, version)), next_version.map(|v| (key, v))))
                    }
                    Ok(None) => None,
                    Err(e) => Some((Err(e), None)),
                }
            },
        ))
    }

    /// Perform a binary search over the versions for a given key, using the provided comparison function to determine the direction of the search
    pub async fn binary_search_with_bias<K: Serializable + Send + Sync>(
        &self,
        key: &K,
        top_version: Version,
        f: fn(&Version, &E) -> Ordering,
        bias: SearchBias,
    ) -> Result<Option<(E, Version)>, BackendError<B::Error>> {
        let Some(mapped_key) = self.get_key(key).await? else {
            return Ok(None);
        };

        let mut low = 0u64;
        let mut high = top_version.0;
        let mut result = None;

        while low <= high {
            let mid = low + (high - low) / 2;
            let mid_version = Version(mid);

            let versioned_key = VersionedEntry {
                version: mid_version,
                data: &mapped_key,
            };

            // Check if this version exists in storage
            let maybe_value = self.engine.read::<_, E>(self.column, &versioned_key).await?;

            if let Some(value) = maybe_value {
                match f(&mid_version, &value) {
                    Ordering::Equal => {
                        // Found a match, but continue searching based on bias
                        result = Some((value, mid_version));

                        match bias {
                            SearchBias::First => break, // Return the first match found
                            SearchBias::Highest => {
                                // Look for higher matching versions
                                low = mid + 1;
                            }
                            SearchBias::Lowest => {
                                // Look for lower matching versions
                                if mid == 0 {
                                    break;
                                }
                                high = mid - 1;
                            }
                        }
                    }
                    Ordering::Less => {
                        // Current version is less than target, search lower half
                        if mid == 0 {
                            break;
                        }
                        high = mid - 1;
                    }
                    Ordering::Greater => {
                        // Current version is greater than target, search upper half
                        low = mid + 1;
                    }
                }
            } else {
                // Version doesn't exist, try to find the nearest existing version
                // Search lower half as this version is missing
                if mid == 0 {
                    break;
                }
                high = mid - 1;
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryBackend, XoriEngine, engine::EntityConfig};
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

    async fn setup_entity_handle() -> EntityHandle<TestEntity, MemoryBackend> {
        let backend = Arc::new(MemoryBackend::new());
        let engine = XoriEngine::new(backend);
        engine.register::<TestEntity>(EntityConfig { key_indexing: true }).await.unwrap()
    }

    #[tokio::test]
    async fn test_binary_search_empty_history() {
        let handle = setup_entity_handle().await;
        let key = 1u32;
        // Search in empty history
        let result = handle
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
        let handle = setup_entity_handle().await;
        let key = 1u32;
        // Store a single version
        handle.store(key, TestEntity { value: 100 }).await.unwrap();

        // Search for exact match
        let result = handle
            .binary_search_with_bias(
                &key,
                Version(0),
                |_, entity| if entity.value == 100 { Ordering::Equal } else { Ordering::Less },
                SearchBias::Lowest,
            )
            .await
            .unwrap();

        assert!(result.is_some());
        let (entity, version) = result.unwrap();
        assert_eq!(entity.value, 100);
        assert_eq!(version, Version(0));
    }

    #[tokio::test]
    async fn test_binary_search_multiple_versions_exact_match() {
        let handle = setup_entity_handle().await;
        let key = 1u32;
        // Store multiple versions
        for i in 0..10 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Search for entity with value 50
        let result = handle
            .binary_search_with_bias(
                &key,
                Version(9),
                |_, entity| {
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
        assert_eq!(entity.value, 50);
        assert_eq!(version, Version(5));
    }

    #[tokio::test]
    async fn test_binary_search_lowest_bias_multiple_matches() {
        let handle = setup_entity_handle().await;
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
            .binary_search_with_bias(
                &key,
                Version(6),
                |_, entity| {
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
        assert_eq!(entity.value, 50);
        assert_eq!(version, Version(3)); // Should find the first occurrence
    }

    #[tokio::test]
    async fn test_binary_search_highest_bias_multiple_matches() {
        let handle = setup_entity_handle().await;
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
                .binary_search_with_bias(
                    &key,
                    Version(6),
                    |_, entity| {
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
            assert_eq!(entity.value, 50);
            assert_eq!(version, Version(5)); // Should find the last occurrence of value 50
    }

    #[tokio::test]
    async fn test_binary_search_no_match() {
        let handle = setup_entity_handle().await;
            let key = 1u32;

            // Store versions with values 0, 10, 20, ..., 90
            for i in 0..10 {
                handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
            }

            // Search for value 55 (doesn't exist)
            let result = handle
                .binary_search_with_bias(
                    &key,
                    Version(9),
                    |_, entity| {
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
        let handle = setup_entity_handle().await;
            let key = 1u32;

            // Store multiple versions
            for i in 0..5 {
                handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
            }

            // Search for first version (value == 0)
            let result = handle
                .binary_search_with_bias(
                    &key,
                    Version(4),
                    |_, entity| {
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
            assert_eq!(entity.value, 0);
            assert_eq!(version, Version(0));
    }

    #[tokio::test]
    async fn test_binary_search_boundary_at_end() {
        let handle = setup_entity_handle().await;
            let key = 1u32;

            // Store multiple versions
            for i in 0..5 {
                handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
            }

            // Search for last version
            let result = handle
                .binary_search_with_bias(
                    &key,
                    Version(4),
                    |_, entity| {
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
            assert_eq!(entity.value, 40);
            assert_eq!(version, Version(4));
    }

    #[tokio::test]
    async fn test_binary_search_with_condition() {
        let handle = setup_entity_handle().await;
            let key = 1u32;

            // Store versions with increasing values
            for i in 0..20 {
                handle.store(key, TestEntity { value: i }).await.unwrap();
            }

            // Search for first version where value >= 15
            let result = handle
                .binary_search_with_bias(
                    &key,
                    Version(19),
                    |_, entity| {
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
            assert_eq!(entity.value, 15);
            assert_eq!(version, Version(15));
    }

    #[tokio::test]
    async fn test_binary_search_all_versions_too_small() {
        let handle = setup_entity_handle().await;
            let key = 1u32;

            // Store versions with small values
            for i in 0..5 {
                handle.store(key, TestEntity { value: i }).await.unwrap();
            }

            // Search for value >= 100 (all values are too small)
            let result = handle
                .binary_search_with_bias(
                    &key,
                    Version(4),
                    |_, entity| {
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
        let handle = setup_entity_handle().await;
            let key = 1u32;

            // Store versions with large values
            for i in 10..15 {
                handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
            }

            // Search for value <= 50 (all values are too large)
            let result = handle
                .binary_search_with_bias(
                    &key,
                    Version(4),
                    |_, entity| {
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
        let handle = setup_entity_handle().await;
            let key = 1u32;
            let nonexistent_key = 999u32;

            // Store some versions for key 1
            for i in 0..5 {
                handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
            }

            // Search with a key that doesn't exist
            let result = handle
                .binary_search_with_bias(
                    &nonexistent_key,
                    Version(4),
                    |_, entity| if entity.value == 20 { Ordering::Equal } else { Ordering::Less },
                    SearchBias::Lowest,
                )
                .await
                .unwrap();

            assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_binary_search_version_based_condition() {
        let handle = setup_entity_handle().await;
            let key = 1u32;

            // Store versions
            for i in 0..10 {
                handle.store(key, TestEntity { value: i * 5 }).await.unwrap();
            }

            // Search for version >= 5 (using version in comparison)
            let result = handle
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
        let handle = setup_entity_handle().await;
        let key = 1u32;

        // Try to delete versions for a key that doesn't exist
        let result = handle
            .delete_until_version(&key, Version(5))
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_until_version_empty_history() {
        let handle = setup_entity_handle().await;
        let key = 1u32;

        // Store then try to delete from a version that never existed
        handle.store(key, TestEntity { value: 100 }).await.unwrap();
        let result = handle
            .delete_until_version(&key, Version(5))
            .await;

        assert!(result.is_ok());
        // Original version should still be readable
        let entity = handle.read_at_version(&key, Version(0)).await.unwrap().unwrap();
        assert_eq!(entity.value, 100);
    }

    #[tokio::test]
    async fn test_delete_until_version_single_version() {
        let handle = setup_entity_handle().await;
        let key = 1u32;

        // Store a single version
        handle.store(key, TestEntity { value: 100 }).await.unwrap();
        
        // Deleting with version 0 should keep version 0 (only deletes versions > 0)
        handle.delete_until_version(&key, Version(0)).await.unwrap();

        // Version 0 should still exist
        let result = handle.read_at_version(&key, Version(0)).await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, 100);
    }

    #[tokio::test]
    async fn test_delete_until_version_middle() {
        let handle = setup_entity_handle().await;
        let key = 1u32;

        // Store multiple versions (0, 1, 2, 3, 4)
        for i in 0..5 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Delete versions > 2 (only keep 0, 1, 2)
        handle.delete_until_version(&key, Version(2)).await.unwrap();

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
            let result = handle.read_at_version(&key, Version(v)).await.unwrap();
            assert!(result.is_none());
        }
    }

    #[tokio::test]
    async fn test_delete_until_version_all_versions() {
        let handle = setup_entity_handle().await;
        let key = 1u32;

        // Store multiple versions
        for i in 0..5 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Delete versions > some very high version number (nothing gets deleted)
        handle.delete_until_version(&key, Version(10)).await.unwrap();

        // All versions should still exist since we're keeping up to version 10
        for v in 0..5 {
            let result = handle.read_at_version(&key, Version(v)).await.unwrap();
            assert!(result.is_some());
        }
    }

    #[tokio::test]
    async fn test_delete_until_version_at_boundary() {
        let handle = setup_entity_handle().await;
        let key = 1u32;

        // Store versions 0, 1, 2, 3
        for i in 0..4 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Delete versions > 2 (keep 0, 1, 2)
        handle.delete_until_version(&key, Version(2)).await.unwrap();

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
        let v3 = handle.read_at_version(&key, Version(3)).await.unwrap();
        assert!(v3.is_none());
    }

    #[tokio::test]
    async fn test_delete_until_version_beyond_max() {
        let handle = setup_entity_handle().await;
        let key = 1u32;

        // Store versions 0, 1, 2
        for i in 0..3 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Delete versions > 10 (nothing happens since max is 2)
        handle.delete_until_version(&key, Version(10)).await.unwrap();

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
        let handle = setup_entity_handle().await;

        // Store versions for multiple keys
        for key in 1u32..=3u32 {
            for i in 0..5 {
                handle.store(key, TestEntity { value: key as u64 * 100 + i }).await.unwrap();
            }
        }

        // Delete versions > 2 from key 2 (keep 0, 1, 2)
        handle.delete_until_version(&2u32, Version(2)).await.unwrap();

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
        let handle = setup_entity_handle().await;
        let key = 1u32;

        // Store versions 0, 1, 2, 3, 4
        for i in 0..5 {
            handle.store(key, TestEntity { value: i * 10 }).await.unwrap();
        }

        // Delete version 3 specifically
        let versioned_key = VersionedEntry {
            version: Version(3),
            data: &key,
        };
        handle.engine.delete(handle.column, &versioned_key).await.unwrap();

        // Now delete versions > 1 (keep 0, 1)
        handle.delete_until_version(&key, Version(1)).await.unwrap();

        // Latest version should be 1
        let version_opt = handle.last_version(&key).await.unwrap();
        assert_eq!(version_opt, Some(Version(1)));

        // Versions 0, 1 should exist
        assert!(handle.read_at_version(&key, Version(0)).await.unwrap().is_some());
        assert!(handle.read_at_version(&key, Version(1)).await.unwrap().is_some());

        // Versions 2, 3, 4 should not exist
        assert!(handle.read_at_version(&key, Version(2)).await.unwrap().is_none());
        assert!(handle.read_at_version(&key, Version(3)).await.unwrap().is_none());
        assert!(handle.read_at_version(&key, Version(4)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_delete_until_version_preserves_earlier_versions() {
        let handle = setup_entity_handle().await;
        let key = 1u32;

        // Store 10 versions with different values
        for i in 0..10 {
            handle.store(key, TestEntity { value: i * 100 }).await.unwrap();
        }

        // Delete versions > 4 (keep 0-4)
        handle.delete_until_version(&key, Version(4)).await.unwrap();

        // Check that earlier versions are preserved
        for v in 0..=4 {
            let entity = handle.read_at_version(&key, Version(v)).await.unwrap().unwrap();
            assert_eq!(entity.value, (v as u64) * 100);
        }

        // Check that deleted versions don't exist
        for v in 5..10 {
            let entity = handle.read_at_version(&key, Version(v)).await.unwrap();
            assert!(entity.is_none());
        }

        // Latest version should be 4
        let version_opt = handle.last_version(&key).await.unwrap();
        assert_eq!(version_opt, Some(Version(4)));
    }
}
