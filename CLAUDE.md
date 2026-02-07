# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Kura is a database management system built from scratch in Kotlin, reimplementing the concepts from Edward Sciore's "Database Design and Implementation" (SimpleDB). This is a learning/experimental project.

## Reference Implementation (SimpleDB)

When implementing new features, refer to the corresponding SimpleDB (Java) code. The package mapping is:

| SimpleDB (Java)              | Kura (Kotlin)                     | Status |
|------------------------------|-----------------------------------|--------|
| `simpledb/file/`             | `com.kura.file/`                  | Done |
| `simpledb/log/`              | `com.kura.log/`                   | Done |
| `simpledb/buffer/`           | `com.kura.buffer/`                | Done |
| `simpledb/tx/`               | `com.kura.transaction/`           | Done |
| `simpledb/record/`           | `com.kura.record/`                | Done |
| `simpledb/metadata/`         | `com.kura.metadata/`              | Done |
| `simpledb/query/`            | `com.kura.query/`                 | Done |
| `simpledb/parse/`            | `com.kura.parse/`                 | Done |
| `simpledb/plan/`             | `com.kura.plan/`                  | Done |
| `simpledb/index/hash/`       | `com.kura.index/hash/`            | Done |
| `simpledb/index/btree/`      | -                                 | Not yet |
| `simpledb/index/planner/`    | -                                 | Not yet |
| `simpledb/index/query/`      | -                                 | Not yet |
| `simpledb/materialize/`      | -                                 | Not yet |
| `simpledb/multibuffer/`      | -                                 | Not yet |
| `simpledb/opt/`              | -                                 | Not yet |
| `simpledb/jdbc/`             | `com.kura.jdbc/`                  | Done |
| `simpledb/server/`           | `com.kura.server/`                | Done |

## Build & Run Commands

```sh
# Build (includes tests)
./gradlew build

# Run all tests
./gradlew test

# Run a single test class
./gradlew test --tests "com.kura.file.FileManagerTest"

# Run a single test method
./gradlew test --tests "com.kura.file.FileManagerTest.should append new block to file"

# Start the server (RMI port 1099)
./gradlew run

# Start the SQL client (requires server running)
./gradlew runClient

# Build client JAR
./gradlew clientJar
```

## Tech Stack

- **Language**: Kotlin 2.0.21 / Java 21
- **Build**: Gradle 8.12.1 (Kotlin DSL)
- **Test**: JUnit 5 + MockK
- **Network**: Java RMI (port 1099)

## Architecture

Layered architecture where each layer depends only on the layers below it. Corresponds to the book's chapter structure.

```
SQL Client (sample/kura_client/)
    ↓
JDBC / Network (jdbc/, jdbc/network/)  ← RMI-based client/server communication
    ↓
Planner (plan/)                         ← Query plan generation and optimization
    ↓
Parser (parse/)                         ← SQL lexing and parsing
    ↓
Query (query/)                          ← Scan implementations (Select, Project, Product)
    ↓
Metadata (metadata/)                    ← Table/view/index/statistics management
    ↓
Record (record/)                        ← Record layout, schema, table scan
    ↓
Index (index/)                          ← HashIndex implementation
    ↓
Transaction (transaction/)              ← ACID guarantees
├── concurrency/                        ← SLock/XLock concurrency control
└── recovery/                           ← WAL-based recovery (LogRecord types)
    ↓
Buffer (buffer/)                        ← Buffer pool management (pin/unpin)
    ↓
Log (log/)                              ← Write-Ahead Logging
    ↓
File (file/)                            ← Block-level disk I/O (BlockId, Page, FileManager)
```

**Entry point**: `com.kura.server.StartServer` → `KuraDB` initializes FileManager, LogManager, BufferManager, MetadataManager, and Planner.

## Key Design Decisions

- **Block size**: 400 bytes (defined in `KuraDB.kt`)
- **Buffer pool**: 8 buffers (defined in `KuraDB.kt`)
- **Log file**: `kura.log`
- **Data directory**: `studentdb/` (excluded via .gitignore)
- **Temp tables**: Files with `temp` prefix are auto-deleted on startup

## Source Layout

All source code under `app/src/main/kotlin/com/kura/`. Tests mirror this structure under `app/src/test/kotlin/com/kura/`.

## Supported SQL

SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, CREATE VIEW, CREATE INDEX
