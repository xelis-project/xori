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

## Installation

Add Xori to your `Cargo.toml`:

```toml
[dependencies]
xori = "0.1.0"
```

## Quick Start

### Define an Entity

```rust
use xori::{Entity, Serializable, Reader, ReaderError, Writable, WriterError};

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

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
        let balance = u64::read(reader)?;
        let owner_bytes = Vec::<u8>::read(reader)?;
        let owner = String::from_utf8(owner_bytes)
            .map_err(|_| ReaderError::InvalidValue)?;
        Ok(Account { balance, owner })
    }

    fn size(&self) -> usize {
        self.balance.size() + self.owner.as_bytes().len() + 8
    }
}
```

### Basic Usage

```rust
use xori::{XoriEngine, MemoryBackend};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create engine with memory backend
    let backend = Arc::new(MemoryBackend::new());
    let engine = XoriEngine::new(backend);

    // Register entity type
    let accounts = engine.register::<Account>().await?;

    // Store versioned data
    let account_id = 1u64;
    accounts.store(account_id, Account {
        balance: 1000,
        owner: "Alice".to_string(),
    }).await?;

    // Update creates a new version
    accounts.store(account_id, Account {
        balance: 1500,
        owner: "Alice".to_string(),
    }).await?;

    // Read latest version
    if let Some((account, version)) = accounts.read(&account_id).await? {
        println!("Balance: {} at version {:?}", account.balance, version);
    }

    // Read specific version
    if let Some(account) = accounts.read_at_version(&account_id, Version(0)).await? {
        println!("Initial balance: {}", account.balance);
    }

    Ok(())
}
```

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

    fn read(reader: &mut Reader) -> Result<Self, ReaderError> {
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
