use bytes::Bytes;
use futures::StreamExt;

use super::*;
use crate::{
    Entity, Serializable, VarUint, Version, VersionedKey, XoriBuilder,
    builder::EntityConfig,
    changes::ColumnChanges,
    engine::{IteratorDirection, IteratorMode},
};

impl Entity for u64 {
    fn entity_name() -> &'static str {
        "iterator_test"
    }
}

async fn columns() -> Vec<Column> {
    let mut builder = XoriBuilder::new().register_entity::<u64>(EntityConfig {
        key_indexing: true,
        // Must be ignored when raw keys are replaced by variable-width indices.
        prefix_length: Some(32),
    });
    builder.register_column(
        "fixed_prefix",
        ColumnKind::Other,
        ColumnProperties {
            prefix_length: Some(1),
        },
    );
    let engine = builder.build::<MemoryBackend>(()).await.unwrap();
    let columns: Vec<_> = engine.backend.columns.values().cloned().collect();
    assert!(
        columns
            .iter()
            .filter(|column| column.name() != "fixed_prefix")
            .all(|column| column.properties().prefix_length.is_none())
    );
    columns
}

fn keys() -> Vec<Bytes> {
    let mut keys = vec![
        Bytes::new(),
        Bytes::from_static(&[0x10]),
        Bytes::from_static(&[0x10, 0xff]),
        Bytes::from_static(&[0x10, 0xff, 0]),
        Bytes::from_static(&[0x11]),
        Bytes::from_static(&[0xff]),
        Bytes::from_static(&[0xff, 0xff]),
        Bytes::from_static(&[0xff, 0xff, 0]),
    ];
    // Neighboring indices and versions straddle every encoding-width boundary.
    for index in [
        251,
        252,
        253,
        254,
        65535,
        65536,
        u32::MAX as u64,
        u32::MAX as u64 + 1,
    ] {
        let key = VarUint(index);
        keys.push(Bytes::copy_from_slice(&key.to_bytes().unwrap()));
        for version in [
            0,
            252,
            253,
            255,
            256,
            65535,
            65536,
            u32::MAX as u64,
            u32::MAX as u64 + 1,
            u64::MAX,
        ] {
            keys.push(Bytes::copy_from_slice(
                &VersionedKey {
                    key,
                    version: Version(version),
                }
                .to_bytes()
                .unwrap(),
            ));
        }
    }
    keys.sort();
    keys
}

fn cases() -> Vec<IteratorMode<'static>> {
    let mut cases = Vec::new();
    for direction in [IteratorDirection::Forward, IteratorDirection::Backward] {
        cases.push(IteratorMode::All(direction));
        for prefix in [
            &[][..],
            &[0x10],
            &[0x10, 0xff],
            &[0x12],
            &[0xff],
            &[0xff, 0xff],
            &[0xfc],
            &[0xfd, 0, 0xfd],
            &[0xfd, 0xff, 0xff],
            &[0xfe, 0, 1, 0, 0],
            &[0xff, 0, 0, 0, 1, 0, 0, 0, 0],
        ] {
            cases.push(IteratorMode::Prefix(prefix, direction));
        }
        cases.push(IteratorMode::Range {
            start: &[0x10],
            end: &[0x11],
            direction,
        });
        cases.push(IteratorMode::Range {
            start: &[0x10, 1],
            end: &[0xfd],
            direction,
        });
        cases.push(IteratorMode::Range {
            start: &[0x11],
            end: &[0x11],
            direction,
        });
        cases.push(IteratorMode::From(&[0x10], direction));
        cases.push(IteratorMode::From(&[0x12], direction));
    }
    cases
}

fn expected(keys: &[Bytes], mode: IteratorMode<'_>) -> Vec<Bytes> {
    let (mut result, direction) = match mode {
        IteratorMode::All(direction) => (keys.to_vec(), direction),
        IteratorMode::Prefix(prefix, direction) => (
            keys.iter()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect(),
            direction,
        ),
        IteratorMode::Range {
            start,
            end,
            direction,
        } => (
            keys.iter()
                .filter(|k| k.as_ref() >= start && k.as_ref() < end)
                .cloned()
                .collect(),
            direction,
        ),
        IteratorMode::From(start, direction) => (
            keys.iter()
                .filter(|k| k.as_ref() >= start)
                .cloned()
                .collect(),
            direction,
        ),
    };
    if direction == IteratorDirection::Backward {
        result.reverse();
    }
    result
}

async fn check_backend<B: Backend>(backend: &mut B)
where
    B::Error: std::fmt::Debug,
{
    let keys = keys();
    for column in columns().await {
        backend.open_column(&column).await.unwrap();
        for key in &keys {
            backend.write(&column, key.as_ref(), 42u64).await.unwrap();
        }
        for mode in cases() {
            let stream = backend.iterator(&column, mode).await.unwrap();
            futures::pin_mut!(stream);
            let mut actual = Vec::new();
            while let Some(entry) = stream.next().await {
                actual.push(Bytes::copy_from_slice(entry.unwrap().0.as_ref()));
            }
            assert_eq!(actual, expected(&keys, mode), "{mode:?}");
        }
        // Assert numeric version order directly, independently of sorting bytes.
        let index = VarUint(253);
        let prefix = index.to_bytes().unwrap();
        let stream = backend
            .iterator(
                &column,
                IteratorMode::Prefix(&prefix, IteratorDirection::Forward),
            )
            .await
            .unwrap();
        futures::pin_mut!(stream);
        assert_eq!(
            stream.next().await.unwrap().unwrap().0.as_ref(),
            prefix.as_ref()
        );
        for version in [
            0,
            252,
            253,
            255,
            256,
            65535,
            65536,
            u32::MAX as u64,
            u32::MAX as u64 + 1,
            u64::MAX,
        ] {
            let key = stream.next().await.unwrap().unwrap().0;
            let decoded = VersionedKey::<VarUint>::from_bytes(key.as_ref()).unwrap();
            assert_eq!(decoded.version, Version(version));
        }
        assert!(stream.next().await.is_none());
    }
}

#[tokio::test]
async fn memory_iterator_boundaries() {
    check_backend(&mut MemoryBackend::new()).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn rocksdb_iterator_boundaries() {
    let directory = tempfile::tempdir().unwrap();
    check_backend(&mut RocksDBBackend::new(directory.path(), &[]).unwrap()).await;
}

#[test]
fn snapshot_iterator_boundaries() {
    let keys = keys();
    let mut changes = ColumnChanges::default();
    for key in &keys {
        changes.insert(key.clone(), Bytes::from_static(b"value"));
    }
    for mode in cases() {
        assert_eq!(
            changes.iterator_keys(mode).cloned().collect::<Vec<_>>(),
            expected(&keys, mode),
            "{mode:?}"
        );
    }
    changes.remove(Bytes::from_static(&[0x10, 0xff]));
    let keys: Vec<_> = keys
        .into_iter()
        .filter(|k| k.as_ref() != [0x10, 0xff])
        .collect();
    for mode in cases() {
        assert_eq!(
            changes.iterator_keys(mode).cloned().collect::<Vec<_>>(),
            expected(&keys, mode),
            "{mode:?}"
        );
    }
}

const BOUNDARY_INDICES: &[u64] = &[252, 253, 65535, 65536, u32::MAX as u64, u32::MAX as u64 + 1];
const BOUNDARY_VERSIONS: &[u64] = &[
    0, 251, 252, 253, 254, 65534, 65535, 65536, 65537,
    u32::MAX as u64 - 1, u32::MAX as u64, u32::MAX as u64 + 1,
    u32::MAX as u64 + 2, u64::MAX,
];

pub(super) async fn seed_version_boundaries<B: Backend>(backend: &mut B, column: &Column)
where
    B::Error: std::fmt::Debug,
{
    backend.open_column(column).await.unwrap();
    // Write in reverse order so insertion order cannot satisfy the assertions.
    for &index in BOUNDARY_INDICES.iter().rev() {
        for &version in BOUNDARY_VERSIONS.iter().rev() {
            backend.write(column, VersionedKey {
                key: VarUint(index), version: Version(version),
            }, version).await.unwrap();
        }
    }
}

async fn assert_version_scan<B: Backend>(
    backend: &B, column: &Column, index: u64, mode: IteratorMode<'_>, expected: &[u64],
) where
    B::Error: std::fmt::Debug,
{
    let stream = backend.iterator(column, mode).await.unwrap();
    futures::pin_mut!(stream);
    let mut actual = Vec::new();
    while let Some(entry) = stream.next().await {
        let (key, value) = entry.unwrap();
        let key = VersionedKey::<VarUint>::from_bytes(key).unwrap();
        assert_eq!(key.key.value(), index, "scan leaked into another key: {mode:?}");
        assert_eq!(u64::from_bytes(value).unwrap(), key.version.0);
        actual.push(key.version.0);
    }
    assert_eq!(actual, expected, "index {index}, {mode:?}");
}

pub(super) async fn check_version_boundaries<B: Backend>(backend: &B, column: &Column)
where
    B::Error: std::fmt::Debug,
{
    for &index in BOUNDARY_INDICES {
        let key = VarUint(index);
        let prefix = key.to_bytes().unwrap();
        let next_key = VarUint(index + 1);
        let key_end = next_key.to_bytes().unwrap();
        for direction in [IteratorDirection::Forward, IteratorDirection::Backward] {
            let mut versions = BOUNDARY_VERSIONS.to_vec();
            if direction == IteratorDirection::Backward { versions.reverse(); }
            assert_version_scan(backend, column, index,
                IteratorMode::Prefix(&prefix, direction), &versions).await;

            for boundary in [253, 65536, u32::MAX as u64 + 1] {
                // Include exact boundaries, their neighbors, and an absent lower bound.
                for lower in [boundary - 1, boundary, boundary + 1, boundary + 2] {
                    let start = VersionedKey { key, version: Version(lower) };
                    let start = start.to_bytes().unwrap();
                    let expected: Vec<_> = versions.iter().copied().filter(|&v| v >= lower).collect();
                    assert_version_scan(backend, column, index,
                        IteratorMode::Range { start: &start, end: &key_end, direction },
                        &expected).await;
                }
                // Check exclusion at an upper bound on either side of the transition.
                for upper in [boundary, boundary + 1] {
                    let start = VersionedKey { key, version: Version(boundary - 1) };
                    let end = VersionedKey { key, version: Version(upper) };
                    let start = start.to_bytes().unwrap();
                    let end = end.to_bytes().unwrap();
                    let expected: Vec<_> = versions.iter().copied()
                        .filter(|&v| v >= boundary - 1 && v < upper).collect();
                    assert_version_scan(backend, column, index,
                        IteratorMode::Range { start: &start, end: &end, direction },
                        &expected).await;
                }
            }
        }
    }
}

#[tokio::test]
async fn memory_varuint_version_bounds() {
    let column = columns().await.remove(0);
    let mut backend = MemoryBackend::new();
    seed_version_boundaries(&mut backend, &column).await;
    check_version_boundaries(&backend, &column).await;
}
