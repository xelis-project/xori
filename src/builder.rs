use std::collections::HashMap;

use crate::{Backend, Column, Entity, XoriEngine, backend::ColumnKind, engine::{Result, XoriBackend}, entity::KeyIndexColumn};

#[derive(Clone, Debug)]
pub(crate) struct EntityInfo {
    pub(crate) column: Column,
    pub(crate) key_index_column: Option<KeyIndexColumn>,
}

#[derive(Clone, Debug, Default)]
pub struct XoriBuilder {
    entity_registry: HashMap<&'static str, EntityInfo>,
    column_index: u32,
}

impl XoriBuilder {
    /// Create a new XoriBuilder
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an entity type with the builder
    pub fn register_entity<E: Entity>(mut self, config: EntityConfig) -> Self {
        let column_id = self.column_index;
        self.column_index += 1;

        let key_index_column = if config.key_indexing {
            let key_to_id = Column {
                id: self.column_index,
                kind: ColumnKind::Index,
            };

            self.column_index += 1;

            let id_to_key = Column {
                id: self.column_index,
                kind: ColumnKind::Index,
            };

            self.column_index += 1;
            Some(KeyIndexColumn {
                key_to_id,
                id_to_key,
            })
        } else {
            None
        };

        let prev = self.entity_registry.insert(E::entity_name(), EntityInfo {
            column: Column {
                id: column_id,
                kind: ColumnKind::Entity,
            },
            key_index_column
        });
        assert!(prev.is_none(), "Entity {} is already registered", E::entity_name());

        self
    }

    /// Register a custom column with the builder
    #[inline]
    pub fn register_column(&mut self, kind: ColumnKind) -> Column {
        let column = Column {
            id: self.column_index,
            kind,
        };
        self.column_index += 1;
        column
    }

    /// Build the Xori engine with the given backend
    #[inline]
    pub async fn build<B: Backend>(self, backend: B) -> Result<XoriEngine<B>, B::Error> {
        // Open all columns for registered entities
        for entity in self.entity_registry.values() {
            backend.open_column(&entity.column).await?;
            if let Some(key_index) = entity.key_index_column.as_ref() {
                backend.open_column(&key_index.key_to_id).await?;
                backend.open_column(&key_index.id_to_key).await?;
            }
        }

        Ok(XoriEngine {
            backend: XoriBackend { backend, snapshot: None },
            entity_registry: self.entity_registry,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct EntityConfig {
    pub key_indexing: bool,
}
