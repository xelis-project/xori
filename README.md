# Xori

A high-performance, versioned storage engine for Rust with support for historical data tracking and efficient version queries.

## Overview

Xori is a database abstraction layer that provides versioned entity storage with built-in support for version history, binary search across versions, and pluggable backend implementations. It's designed for applications that need to track changes over time, such as blockchain ledgers, audit systems, or temporal databases.

## Features

- **Versioned Storage**: Every write operation creates a new version, preserving complete history
- **Efficient Version Queries**: Binary search across versions with configurable bias (lowest/highest match)
- **Type-Safe Entities**: Strong typing through Rust's type system with the `Entity` trait
- **Pluggable Backends**: Support for different storage backends through the `Backend` trait
- **Async/Await**: Full async support for non-blocking I/O operations
- **Key Indexing**: Optional key indexing to reduce storage overhead for large keys
- **Custom Serialization**: Built-in serialization framework with support for custom types
- **Stream-Based APIs**: Efficient iteration over large datasets using Rust streams
- **Snapshot**: Create memory snapshots of the database state to allow rollback of the operations
- **DAG State Management**: Manage complex DAG-based data structures with conflict resolution and merging capabilities

## Installation

Add Xori to your `Cargo.toml`:

```toml
[dependencies]
xori = "0.1.0"
```

## Quick Start

### Define an Entity

```rust
use xori::{Entity, Serializable, Readable, Reader, ReaderError, Writable, WriterError};

#[derive(Debug, Clone)]
struct Account {
    balance: u64,
    owner: String,
}

impl Entity for Account {
    fn entity_name() -> &'static str {
        "account"
    }
}

impl Serializable for Account {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        self.balance.write(writer)?;
        self.owner.as_bytes().to_vec().write(writer)
    }

    fn read<R: Readable>(reader: &mut Reader<R>) -> Result<Self, ReaderError> {
        let balance = u64::read(reader)?;
        let owner_bytes = Vec::<u8>::read(reader)?;
        let owner = String::from_utf8(owner_bytes)
            .map_err(|_| ReaderError::UnexpectedValue)?;
        Ok(Account { balance, owner })
    }

    fn size(&self) -> usize {
        self.balance.size() + self.owner.as_bytes().len() + 8
    }
}
```

### Basic Usage

```rust
use xori::{EntityConfig, MemoryBackend, Version, XoriBuilder};

// Register the schema before creating the backend.
let mut engine = XoriBuilder::new()
    .register_entity::<Account>(EntityConfig::default())
    .build::<MemoryBackend>(())
    .await?;

let account_id = 1u64;
engine.entity_handle_write::<Account>().unwrap()
    .store(account_id, Account { balance: 1000, owner: "Alice".into() })
    .await?;

let accounts = engine.entity_handle_read::<Account>().unwrap();
if let Some(version) = accounts.last_version(&account_id).await? {
    let account = accounts.read_at_version(&account_id, version).await?;
}
let initial = accounts.read_at_version(&account_id, Version::default()).await?;
```

For RocksDB, supply its configuration instead of an already-open database:

```rust
use xori::{EntityConfig, RocksDBBackend, RocksDBConfig, XoriBuilder};

let engine = XoriBuilder::new()
    .register_entity::<Account>(EntityConfig::default())
    .build::<RocksDBBackend>(RocksDBConfig::new("./data"))
    .await?;
```

The builder passes all registered columns to `Backend::open(config, columns)`.
RocksDB opens those column families together, using their configured prefix settings.
Reopening requires the complete existing schema; missing column families produce an
error. No column discovery from disk is performed. New registered columns are created
automatically. Keep column registration order stable because column IDs are assigned
in that order.

Custom backends implement `Backend::Config` and `Backend::open` in addition to the
storage operations. `DagState::new(builder, config)` registers its internal columns
before opening the backend as well.

## Core Concepts

### Entities

Entities are the primary data structures stored in Xori. They must implement both the `Entity` and `Serializable` traits:

- `Entity`: Provides metadata about the entity type
- `Serializable`: Defines how the entity is serialized and deserialized

### Versions

Every write operation creates a new version, starting from 0 and incrementing sequentially. Versions are immutable once written, providing a complete audit trail.

### EntityHandle

The `EntityHandle` provides the API for interacting with a specific entity type:

- `store()`: Write a new version
- `read_at_version()`: Read a specific version
- `history()`: Stream all versions in reverse chronological order
- `last_version()`: Get the latest version number
- `binary_search_with_bias()`: Efficiently search for versions matching criteria

### Binary Search

Xori provides efficient binary search across versions with three bias modes:

- `SearchBias::First`: Find the first version matching the criteria
- `SearchBias::Lowest`: Find the first version matching the criteria
- `SearchBias::Highest`: Find the last version matching the criteria

```rust
use std::cmp::Ordering;

// Find first version where balance >= 1000
let result = accounts.binary_search_with_bias(
    &account_id,
    latest_version,
    |_version, account| {
        if account.balance < 1000 {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    },
    SearchBias::Lowest,
).await?;
```

## Serialization

Xori includes a custom serialization framework optimized for versioned storage. Built-in support for:

- Primitive types: `u8`, `u16`, `u32`, `u64`, `i64`, `bool`
- Collections: `Vec<T>`, `Option<T>`
- References: `&T`, `Cow<T>`

### Writing directly to a sink

`Serializable::write` serializes directly into any `Writable` destination without
creating an intermediate byte buffer:

```rust
account.write(&mut sink)?;
```

`Writable::push` and `extend_bytes` return `Result<(), WriterError>`. Custom
serializers must propagate these results with `?`. `WriterError` is a Matriochka
error: sinks can wrap their own error types using `WriterError::new(error)` and
add optional `.context(...)`. Reader errors also use Matriochka for custom causes
and expose `.context(...)`. `from_bytes` adds the type and reader offset to
decoding failures; `to_bytes` adds the type to serialization failures. There are
no I/O-specific error variants.

A failed write may leave a partial value in the destination. Each sink determines
its buffering and commit behavior. `pre_allocate` is optional and returns `false`
by default.

### Reading directly from a source

`Reader<R>` accepts any `Readable` source. Synchronous `std::io::Read` types,
including files, sockets, and cursors, implement `Readable` automatically:

```rust
use xori::{Reader, Serializable};

let mut file = std::fs::File::open("account.bin")?;
let mut reader = Reader::from_source(&mut file);
let account = Account::read(&mut reader)?;
```

`Reader::new(bytes)` and `Serializable::from_bytes(bytes)` still support byte
slices. Slice readers retain `bytes`, `remaining`, `has_more`, `read_bytes_ref`,
and `read_bytes_left`; these borrowed helpers are specific to slice sources.

Custom sources implement `Readable::read(&mut self, buffer: &mut [u8])`, returning
the number of bytes read or a `ReaderError`. Zero means EOF. Custom errors can be
wrapped with Matriochka, without using I/O errors. Partial reads are handled by
`Reader::read_exact`, and source errors include the consumed byte offset.

Fixed-size values read into stack buffers. Strings and keys read into their final
owned buffers. `read_remaining_bytes` now returns an owned `Vec<u8>` and consumes
the source until EOF; use a bounded source for unframed values inside a larger
stream. Custom serializers must adopt the generic `read<R: Readable>` signature.

### Custom Serialization

Implement `Serializable` for custom types:

```rust
impl Serializable for MyType {
    fn write<W: Writable>(&self, writer: &mut W) -> Result<(), WriterError> {
        // Write fields
        self.field1.write(writer)?;
        self.field2.write(writer)?;
        Ok(())
    }

    fn read<R: Readable>(reader: &mut Reader<R>) -> Result<Self, ReaderError> {
        // Read fields
        let field1 = Type1::read(reader)?;
        let field2 = Type2::read(reader)?;
        Ok(MyType { field1, field2 })
    }

    fn size(&self) -> usize {
        self.field1.size() + self.field2.size()
    }
}
```

## Advanced Features

### Version History Streaming

Iterate through all versions efficiently:

```rust
use futures::StreamExt;

let mut history = accounts.history(&account_id).await?;
while let Some(result) = history.next().await {
    let (account, version) = result?;
    println!("Version {:?}: balance = {}", version, account.balance);
}
```

### Key Indexing

For entities with large keys, enable key indexing to reduce storage overhead by mapping keys to compact integer indices.

### Metadata Tracking

Xori automatically tracks entity metadata including the latest version and key index state.

## Performance Considerations

- **Version Lookup**: O(1) for latest version, O(log n) for binary search
- **History Iteration**: Streaming API prevents loading all versions into memory
- **Key Indexing**: Reduces storage for large keys at the cost of an extra lookup
- **Backend Choice**: In-memory for testing, disk-based for persistence

## Architecture

```
┌─────────────────┐
│  Application    │
└────────┬────────┘
         │
┌────────▼────────┐
│  EntityHandle   │  (Type-safe API per entity)
└────────┬────────┘
         │
┌────────▼────────┐
│  XoriEngine     │  (Core engine, entity registry)
└────────┬────────┘
         │
┌────────▼────────┐
│    Backend      │  (Storage abstraction)
└────────┬────────┘
         │
    ┌────▼────┐
    │ Storage │  (Memory, Disk, Network, etc.)
    └─────────┘
```

## Use Cases

- **Blockchain State**: Track account balances and smart contract state over block heights
- **Audit Logs**: Maintain complete history of entity changes
- **Temporal Databases**: Query data as it existed at any point in time
- **Event Sourcing**: Store and replay events with full history
- **Configuration Management**: Track configuration changes with rollback capability

### Chunked history and reorg rollback

Register one shared `History` column before opening the backend. A history writer
stages entity versions and automatically records `(column, key, created_version)`
references. It stores no entity values in the journal. Use one writer batch per
topoheight, with at most one change to each entity key in that batch:

```rust
use xori::{EntityConfig, History, HistoryConfig, MemoryBackend, XoriBuilder};

let mut builder = XoriBuilder::new()
    .register_entity::<Account>(EntityConfig::default());
let history = History::register(&mut builder, "history", HistoryConfig::default());
let mut engine = builder.build::<MemoryBackend>(()).await?;

{
    let mut writer = history.writer(&mut engine)?;
    writer.entity_handle_write::<Account>().unwrap()
        .store(42u64, Account { balance: 100, owner: "Alice".into() }).await?;
    writer.flush(1000u64).await?; // atomically commits data + history
}

// Remove topoheight 1000 and everything above it, newest first.
history.rollback_from(&mut engine, 1000u64).await?;
```

`writer.store(key, entity)` and `writer.store_deleted::<EntityType, _>(key)` are
also available directly. `last_version` and `read_at_version` see pending writes.
Dropping a writer discards unflushed changes. A successful `flush(key)` clears the
pending batch so the writer can be reused; a failed flush retains it for retry.
Existing history keys are rejected. Repeated entity changes within a batch are
recorded in operation order, each with its own version.
An empty flush records an empty history entry.

`HistoryConfig::max_chunk_bytes` defaults to 64 KiB and limits each serialized
history value, including its framing. References are packed into chunks under
`{escaped history key}{chunk index}`. The escaping preserves the serialized key's
ordering and prevents prefix collisions. A single reference too large for the
configured limit is rejected before staging any changes. Chunking bounds stored
history values; the complete pending data batch still lives in memory. References
borrow the staged keys while packing. Flush serializes all journal chunks before
calling `write_batch`, then commits them together with the entity changes. The
staged entity changes are retained for retry if the commit fails. The stored
chunk format is unchanged.

`history.entries(&engine, key)` streams individual references. Creating a stream
does no database reads; each poll decodes one entry, and another chunk is fetched
only after the current one is exhausted. Dropping the stream stops further work.

`history.chunks(&engine, key)` returns lazy `HistoryChunk` iterators rather than
`Vec<HistoryEntry>` lists. Each chunk owns its backend buffer and decodes entries
on demand. Both chunk fetching and entry decoding can fail. To collect a chunk,
use `chunk.collect::<Result<Vec<_>, _>>()`; skipping entries also skips their
validation. For normal entry-by-entry consumption:

```rust
use futures::TryStreamExt;

let entries = history.entries(&engine, 1000u64)?;
futures::pin_mut!(entries);
while let Some(entry) = entries.try_next().await? {
    // Inspect entry.column, entry.key, and entry.version.
}
```

`history.rollback(&mut engine, key)` undoes one complete history key atomically.
It reads chunks and operations in reverse order, decoding at most one chunk
into an entry list at a time. It retains the undo changes until every chunk
has been validated, so a late decoding error cannot cause a partial rollback.
`rollback_from` commits each history key separately and can resume after an
error. Supply history keys in chronological byte order (for example `u64`
topoheights, whose encoding is big endian), and undo newer keys first. Conflicting
latest versions stop rollback before modifying that history key. Newly created
entities lose their latest pointer, payload, and key mappings; allocation counters
remain monotonic.

Only versioned writes through `HistoryWriter` are tracked. Use `store_deleted`
instead of physically deleting versions that may be needed for rollback. Writes
through ordinary engine handles remain untracked; do not mix untracked changes
into state that must be reverted through this journal.

History flush and rollback require `Backend::write_batch`. MemoryBackend and
RocksDBBackend implement atomic cross-column batches. Custom backends implement
`write_batch` over an iterator of `(&Column, ColumnChanges)` pairs and can inspect
puts and deletions through `ColumnChanges::entries()`. All journal serialization
finishes before the batch is submitted, so serialization errors cannot publish
partial changes. Atomic batches require memory proportional to their pending
changes, including the serialized journal chunks.

History `flush` commits a batch; it does not force an SST flush or fsync. RocksDB
uses its normal WAL/write options. Register the same schema, including history,
in the same order on reopen.
