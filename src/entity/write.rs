use std::marker::PhantomData;

use crate::{
    Backend,
    Entity,
    EntityMetadata,
    EntityReadHandle,
    Key,
    KeyIndex,
    Result,
    Serializable,
    Version,
    VersionedKey,
    XoriError,
    builder::EntityInfo,
    engine::XoriBackend
};

pub struct EntityWriteHandle<'engine, E: Entity, B: Backend> {
    pub(crate) info: &'engine EntityInfo,
    pub(crate) backend: &'engine mut XoriBackend<B>,
    pub(crate) _phantom: PhantomData<E>,
}

impl<'engine, E: Entity, B: Backend> EntityWriteHandle<'engine, E, B> {
    /// Get the key for a raw key, if key indexing is enabled
    /// Returns None only if key indexing is enabled but the key does not exist
    #[inline(always)]
    async fn fetch_key_index<K: Serializable + Send + Sync>(&self, key: K) -> Result<Option<Key<K>>, B::Error> {
        super::fetch_key_index(self.backend, self.info.key_index_column.as_ref(), key).await
    }

    /// Get the latest version for the specified mapped key
    #[inline(always)]
    async fn version<K: Serializable + Send + Sync>(&self, key: &K) -> Result<Option<Version>, B::Error> {
        super::version(self.backend, &self.info.column, key).await
    }

    /// Read the entity metadata
    /// TODO: cache it
    #[inline(always)]
    pub(crate) async fn metadata(&self) -> Result<EntityMetadata, B::Error> {
        self.backend.read(&self.info.column, &()).await
            .map(Option::unwrap_or_default)
    }

    /// Update the entity metadata
    #[inline(always)]
    async fn update_metadata(&mut self, metadata: &EntityMetadata) -> Result<(), B::Error> {
        self.backend.write(&self.info.column, &(), metadata).await
    }

    /// Map a raw key to either a raw key or an indexed key, depending on whether key indexing is enabled
    async fn get_or_create_key<K: Serializable + Send + Sync>(&mut self, key: K) -> Result<Key<K>, B::Error> {
        match self.info.key_index_column.as_ref() {
            Some(index) => {
                let id = self.backend.read::<_, KeyIndex>(&index.key_to_id, &key).await
                    .map(|opt| opt.map(Key::Indexed))?;

                match id {
                    Some(key) => Ok(key),
                    None => {
                        let mut metadata = self.metadata().await?;
                        let key_index = metadata.next_key_index();

                        // Store the new key index mapping
                        self.backend.write(&index.key_to_id, &key, &key_index).await?;
                        self.backend.write(&index.id_to_key, &key_index, &key).await?;

                        self.update_metadata(&metadata).await?;

                        Ok(Key::Indexed(key_index))
                    }

                }
            },
            None => Ok(Key::Raw(key)),
        }
    }

    /// Update the latest version for a key
    async fn update_version<K: Serializable + Send + Sync>(&mut self, key: &K, version: Version) -> Result<(), B::Error> {
        self.backend.write(&self.info.column, key, &version).await
    }

    /// Store a new version of the entity for the given key
    pub async fn store<K: Serializable + Send + Sync>(&mut self, key: K, value: E) -> Result<(), B::Error> {
        // Map the key if required
        let key = self.get_or_create_key(key).await?;

        // load the current version and increment it for the new entry
        let version = self.version(&key).await?
            .map(Version::next)
            .unwrap_or_default();

        let versioned = VersionedKey {
            version,
            key: &key,
        };

        self.backend.write(&self.info.column, &versioned, Some(&value)).await?;

        // Update the latest version for the key
        self.update_version(&key, version).await
    }

    /// Store a deletion entry for the given key, which will be interpreted as a deletion of the latest version
    pub async fn store_deleted<K: Serializable + Send + Sync>(&mut self, key: K) -> Result<(), B::Error> {
        // Map the key if required
        let key = self.get_or_create_key(key).await?;

        // load the current version and increment it for the new entry
        let version = match self.version(&key).await? {
            Some(version) => version.next(),
            None => return Err(XoriError::NoVersionAvailable),
        };

        let versioned = VersionedKey {
            version,
            key: &key,
        };

        self.backend.write(&self.info.column, &versioned, None::<()>).await?;

        // Update the latest version for the key
        self.update_version(&key, version).await
    }

    /// Delete versions at or above the specified version for the given key
    /// This is used for rolling back to a previous version by deleting all newer versions
    pub async fn delete_until_version<K: Serializable + Send + Sync>(&mut self, key: &K, version: Version) -> Result<(), B::Error> {
        match self.fetch_key_index(key).await? {
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

                    let versioned_key = VersionedKey {
                        version: v,
                        key: &mapped_key,
                    };

                    self.backend.delete(&self.info.column, &versioned_key).await?;

                    // Move to the next version
                    current_version = v.previous();
                }

                if !updated {
                    // If we deleted all versions, update the latest version to None
                    self.backend.delete(&self.info.column, &mapped_key).await?;
                }

                Ok(())
            },
            None => Ok(()), // If key doesn't exist, nothing to delete
        }
    }

    pub async fn delete<K: Serializable + Send + Sync>(&mut self, initial_key: K) -> Result<(), B::Error> {
        match self.fetch_key_index(&initial_key).await? {
            Some(mapped_key) => {
                let Some(mut version) = self.version(&mapped_key).await? else {
                    return Ok(()); // No versions exist, nothing to delete
                };

                // Delete the latest version entry for the key
                self.backend.delete(&self.info.column, &mapped_key).await?;

                // Clean up all versions for the key
                while let Some(v) = version.previous() {
                    let versioned_key = VersionedKey {
                        version: v,
                        key: &mapped_key,
                    };

                    self.backend.delete(&self.info.column, &versioned_key).await?;

                    // Move to the next version
                    version = v;
                }

                // Clean up old key mapping if it exists
                if let Some(index) = self.info.key_index_column.as_ref() {
                    self.backend.delete(&index.id_to_key, &mapped_key).await?;
                    self.backend.delete(&index.key_to_id, &initial_key).await?;
                }

                Ok(())
            },
            None => Ok(()), // If key doesn't exist, nothing to delete
        }
    }

    /// Downgrade to a read handle for the same entity type
    #[inline(always)]
    pub fn downgrade(self) -> EntityReadHandle<'engine, E, B> {
        EntityReadHandle {
            info: self.info,
            backend: self.backend,
            _phantom: self._phantom,
        }
    }
}