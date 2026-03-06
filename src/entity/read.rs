use std::{cmp::Ordering, marker::PhantomData};

use futures::{Stream, future::Either, stream};

use crate::{
    Backend, Entity, Key, XoriResult, Serializable, Version, VersionedKey, XoriError, builder::EntityInfo, engine::{IteratorDirection, IteratorMode, XoriBackend}, entity::SearchBias
};

/// Entity handle for managing a specific entity type with versioning
/// This handle provides methods for reading only operations
pub struct EntityReadHandle<'engine, E: Entity, B: Backend> {
    pub(crate) info: &'engine EntityInfo,
    pub(crate) backend: &'engine XoriBackend<B>,
    pub(crate) _phantom: PhantomData<E>,
}

impl<'engine, E: Entity, B: Backend> EntityReadHandle<'engine, E, B> {
    /// Get the key for a raw key, if key indexing is enabled
    /// Returns None only if key indexing is enabled but the key does not exist
    #[inline(always)]
    async fn fetch_key_index<K: Serializable + Send + Sync>(&self, key: K) -> XoriResult<Option<Key<K>>, B::Error> {
        super::fetch_key_index(self.backend, self.info.key_index_column.as_ref(), key).await
    }

    /// Get the latest version for the specified mapped key
    #[inline(always)]
    async fn version<K: Serializable + Send + Sync>(&self, key: &K) -> XoriResult<Option<Version>, B::Error> {
        super::version(self.backend, &self.info.column, key).await
    }

    /// Get the latest version for the specified key, using the key index if available
    pub async fn last_version<K: Serializable + Send + Sync>(&self, key: &K) -> XoriResult<Option<Version>, B::Error> {
        match self.fetch_key_index(key).await? {
            Some(mapped_key) => self.version(&mapped_key).await,
            None => Ok(None),
        }
    }

    /// Read a specific version of the entity for the given key
    pub async fn read_at_version<K: Serializable + Send + Sync>(&self, key: &K, version: Version) -> XoriResult<Option<E>, B::Error> {
        Ok(match self.fetch_key_index(key).await? {
            Some(mapped_key) => {
                let versioned_key = VersionedKey {
                    version,
                    key: &mapped_key,
                };

                self.backend.read::<_, Option<E>>(&self.info.column, &versioned_key).await?
                    .ok_or(XoriError::VersionNotFound)?
            },
            None => None,
        })
    }

    /// Get the entire history of versions for a given key as a stream of (value, version) pairs, starting from the latest version and going backwards
    pub async fn history<'a, K: Serializable + Send + Sync>(&'a self, key: &'a K) -> XoriResult<impl Stream<Item = XoriResult<(Option<E>, Version), B::Error>> + 'a, B::Error> {
        Ok(stream::unfold(
            match self.fetch_key_index(key).await? {
                Some(mapped_key) => self.version(&mapped_key).await?
                    .map(|version| (mapped_key, version)),
                None => None,
            },
            move |state| async move {
                let (key, version) = state?;

                let versioned_key = VersionedKey {
                    version,
                    key: &key,
                };

                match self.backend.read::<_, Option<E>>(&self.info.column, &versioned_key).await {
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
        f: fn(&Version, Option<&E>) -> Ordering,
        bias: SearchBias,
    ) -> XoriResult<Option<(Option<E>, Version)>, B::Error> {
        let Some(mapped_key) = self.fetch_key_index(key).await? else {
            return Ok(None);
        };

        let mut low = 0u64;
        let mut high = top_version.0;
        let mut result = None;

        while low <= high {
            let mid = low + (high - low) / 2;
            let mid_version = Version(mid);

            let versioned_key = VersionedKey {
                version: mid_version,
                key: &mapped_key,
            };

            // Check if this version exists in storage
            let maybe_value = self.backend.read::<_, Option<E>>(&self.info.column, &versioned_key).await?;

            if let Some(value) = maybe_value {
                match f(&mid_version, value.as_ref()) {
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

    /// List all keys in the entity, using the key index if available
    pub async fn list_keys<'a, K: Serializable + Send + Sync + 'a>(&'a self) -> XoriResult<impl Stream<Item = XoriResult<K, B::Error>> + 'a, B::Error> {
        match self.info.key_index_column.as_ref() {
            Some(index_col) => Ok(Either::Left(self.backend.iterator_keys(&index_col.key_to_id, IteratorMode::All(IteratorDirection::Forward)).await?)),
            None => Ok(Either::Right(self.backend.iterator_keys(&self.info.column, IteratorMode::All(IteratorDirection::Forward)).await?)),
        }
    }
}
