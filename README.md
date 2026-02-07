# Kura (蔵)

A relational database built from scratch in Kotlin.
Reimplements [SimpleDB](http://www.cs.bc.edu/~sciore/simpledb/) from Edward Sciore's "[Database Design and Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7)" with idiomatic Kotlin.

## Features

- Full stack from block-level disk I/O to SQL query planning
- WAL (Write-Ahead Logging) crash recovery
- SLock/XLock concurrency control
- Hash and B-Tree indexes
- External merge sort and materialization
- Multi-buffer product (chunk-based joins)
- Heuristic query optimization with index utilization
- RMI-based client/server communication with JDBC interface
- 118 source files / ~10,000 lines of Kotlin

## Architecture

```
SQL Client
  ↓
JDBC / Network (RMI)          Client/server communication
  ↓
Planner                        Query plan generation & optimization
├── BasicQueryPlanner            Naive cross-product
├── BetterQueryPlanner           Product ordering optimization
├── HeuristicQueryPlanner        Heuristic optimization + index utilization
├── IndexSelectPlan / IndexJoinPlan
├── MultibufferProductPlan       Chunk-based product
├── SortPlan / MergeJoinPlan     External merge sort & merge join
└── GroupByPlan                  Aggregation & grouping
  ↓
Parser                         SQL lexing & parsing
  ↓
Query                          Scan implementations (Select, Project, Product)
  ↓
Metadata                       Table/view/index/statistics management
  ↓
Record                         Record layout, schema, table scan
  ↓
Index                          Hash index / B-Tree index
  ↓
Transaction                    ACID transactions
├── Concurrency                  SLock/XLock concurrency control
└── Recovery                     WAL-based recovery
  ↓
Buffer                         Buffer pool management (pin/unpin)
  ↓
Log                            Write-Ahead Logging
  ↓
File                           Block-level disk I/O
```

## Tech Stack

| | |
|---|---|
| Language | Kotlin 2.0.21 / Java 21 |
| Build | Gradle 8.12.1 (Kotlin DSL) |
| Test | JUnit 5 + MockK |
| Network | Java RMI (port 1099) |

## Quick Start

### Start the Server

```sh
./gradlew build   # Build + run tests
./gradlew run     # Start server (port 1099)
```

### Connect with SQL Client

```sh
./gradlew runClient
```

```
Connection target [jdbc:kuradb://localhost]>
Connecting to KuraDB: jdbc:kuradb://localhost...
Connection successful

SQL> create table students (sid int, sname varchar(20))
SQL> insert into students (sid, sname) values (1, 'tanaka')
SQL> insert into students (sid, sname) values (2, 'sato')
SQL> select sid, sname from students
SQL> update students set sname = 'suzuki' where sid = 1
SQL> delete from students where sid = 2
```

### Sample Benchmark (1M+ Records)

A standalone program that exercises every layer of Kura without requiring the server:

```sh
./gradlew runSampleSQL
```

Bulk-loads ~3 million rows across 4 tables (dept, emp, project, assignment) and runs 8 SQL queries covering single-table lookups, multi-table products, and three-table joins.

## Supported SQL

```sql
SELECT field1, field2 FROM table1, table2 WHERE predicate
INSERT INTO table (field1, field2) VALUES (val1, val2)
UPDATE table SET field = value WHERE predicate
DELETE FROM table WHERE predicate
CREATE TABLE table (field1 int, field2 varchar(20))
CREATE VIEW view AS select ...
CREATE INDEX index ON table (field)
```

> Field names must be globally unique within a query (`table.field` notation is not supported).

## Project Structure

```
app/src/main/kotlin/
├── com/kura/
│   ├── file/              Block I/O (BlockId, Page, FileManager)
│   ├── log/               Write-Ahead Log (LogManager, LogIterator)
│   ├── buffer/            Buffer pool (BufferManager, Buffer)
│   ├── transaction/       Transactions
│   │   ├── concurrency/   Lock management
│   │   └── recovery/      WAL recovery (LogRecord)
│   ├── record/            Records (Schema, Layout, TableScan, RecordPage)
│   ├── metadata/          Metadata management (table/view/index/statistics)
│   ├── query/             Scans (Select, Project, Product, Constant, Predicate)
│   ├── parse/             SQL parser (Lexer, Parser)
│   ├── plan/              Planners (Basic, Better, Optimized, Table, Select, Project, Product)
│   ├── index/
│   │   ├── hash/          Hash index
│   │   ├── btree/         B-Tree index
│   │   ├── planner/       Index planners (Select, Join, Update)
│   │   └── query/         Index scans
│   ├── materialize/       Materialization, sort, merge join, aggregation
│   ├── multibuffer/       Multi-buffer product (BufferNeeds, ChunkScan)
│   ├── opt/               Heuristic optimization (TablePlanner, HeuristicQueryPlanner)
│   ├── jdbc/              JDBC driver
│   │   └── network/       RMI remote interfaces
│   └── server/            Server startup (KuraDB, StartServer)
└── sample/
    ├── kura_client/       SQL client
    └── sample_sql/        Benchmark (SampleSQL)
```

## Build Commands

```sh
./gradlew build                # Build + all tests
./gradlew test                 # Tests only
./gradlew run                  # Start server
./gradlew runClient            # SQL client
./gradlew runSampleSQL         # 1M-record benchmark
./gradlew clientJar            # Build client JAR
```

## References

- Edward Sciore, "[Database Design and Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7)", Springer, 2nd Edition
- [SimpleDB (original Java implementation)](http://www.cs.bc.edu/~sciore/simpledb/)
