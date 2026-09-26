use std::{borrow::Cow, collections::HashMap, sync::Arc};

use crate::{Backend, Column, Entity, XoriEngine, backend::{ColumnId, ColumnProperties, column::{ColumnInner, ColumnKind}}, engine::{XoriResult, XoriBackend}, entity::KeyIndexColumn};

#[derive(Clone, Debug)]
pub(crate) struct EntityInfo {
    pub(crate) column: Column,
    pub(crate) key_index_column: Option<KeyIndexColumn>,
}

#[derive(Clone, Debug, Default)]
pub struct XoriBuilder {
    entity_registry: HashMap<&'static str, EntityInfo>,
    columns: HashMap<ColumnId, Column>,
}

impl XoriBuilder {
    /// Create a new XoriBuilder
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an entity type with the builder
    pub fn register_entity<E: Entity>(mut self, config: EntityConfig) -> Self {
        assert!(!self.entity_registry.contains_key(E::entity_name()), "Entity {} is already registered", E::entity_name());
        let prefix_length = if config.key_indexing {
            // Indexed keys have variable width; no fixed prefix length can
            // isolate the key from its version. Use bounded scans instead.
            None
        } else {
            config.prefix_length
        };

        let column = self.register_column(E::entity_name(), ColumnKind::Entity, ColumnProperties { prefix_length });

        let key_index_column = if config.key_indexing {
            let key_to_id = self.register_column(format!("{}_k2i", E::entity_name()), ColumnKind::Index, Default::default());
            let id_to_key = self.register_column(format!("{}_i2k", E::entity_name()), ColumnKind::Index, Default::default());

            Some(KeyIndexColumn {
                key_to_id,
                id_to_key,
            })
        } else {
            None
        };

        self.entity_registry.insert(E::entity_name(), EntityInfo {
            column,
            key_index_column
        });

        self
    }

    /// Register a custom column with the builder
    #[inline]
    pub fn register_column(&mut self, name: impl Into<Cow<'static, str>>, kind: ColumnKind, properties: ColumnProperties) -> Column {
        let column = Arc::new(ColumnInner {
            name: name.into(),
            id: ColumnId(self.columns.len() as u64),
            kind,
            properties,
        });

        // Ensure column ID uniqueness
        assert!(!self.columns.contains_key(&column.id), "Column ID {} is already in use", column.id.0);
        self.columns.insert(column.id, column.clone());

        column
    }

    /// Open the backend with all registered columns, then build the engine.
    #[inline]
    pub async fn build<B: Backend>(self, config: B::Config) -> XoriResult<XoriEngine<B>, B::Error> {
        let mut columns: Vec<_> = self.columns.values().cloned().collect();
        columns.sort_by_key(|column| column.id());
        let backend = B::open(config, &columns).await?;

        Ok(XoriEngine {
            backend: XoriBackend { backend, columns: self.columns },
            entity_registry: self.entity_registry,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct EntityConfig {
    pub key_indexing: bool,
    pub prefix_length: Option<usize>,
}
