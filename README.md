# 蔵 (Kura)

蔵 is the next evolution of my previous RDBMS project, [お箱](https://github.com/ue-sho/ohako). While "お箱" meant a "box," "蔵" is more like a "warehouse". The goal of this project is to create a simple, yet functional, database management system from scratch in Python using [Codon](https://docs.exaloop.io/codon). The implementation is based on the SimpleDB of the book "[Database Design and Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7)". The original code of SimpleDB is written in Java and can be found [here](http://www.cs.bc.edu/~sciore/simpledb/).

## Setup

1. Install dependencies
    ```bash
    curl -sSf https://rye.astral.sh/get | bash
    /bin/bash -c "$(curl -fsSL https://exaloop.io/install.sh)"
    rye sync
    ```
1. Run the project
    ```bash
    make run

    # or
    make build
    ./kura
    ```

## Features

- [ ] `SELECT`, `INSERT`, `DELETE`, `UPDATE` (Chapter 8)
  - [ ] multiple-row INSERT
- [ ] Join (Chapter 8)
  - [ ] comma-separated join (ex. `SELECT * FROM A, B WHERE A.x = B.y`)
  - [ ] `JOIN` syntax (ex. `SELECT * FROM A JOIN B ON A.x = B.y`) (Exercises 9.10)
  - [ ] index join algorithm (Section 12.6.2)
  - [ ] hash join algorithm
    - there is `HashJoinPlan` but no planner (Exercises 15.17)
  - [ ] merge join algorithm
    - there is `MergeJoinPlan` but no planner
- [ ] Sorting (Chapter 9)
  - there is `SortPlan` but no `ORDER BY` grammar in parser (Exercises 13.15)
- [ ] Aggregation (Chapter 9)
  - there is `GroupByPlan` but no `GROUP BY` grammar in parser (Exercises 13.17)
- [ ] Schema (Chapter 6)
  - [ ] `INT` type
  - [ ] `VARCHAR` type
    - [ ] fixed-length
    - [ ] variable-length (Exercises 6.9)
  - [ ] `NULL` (Exercises 6.13)
  - [ ] `CREATE TABLE`
  - [ ] `DROP TABLE`
  - [ ] `ALTER TABLE`
- [ ] Transactions (Chapter 5)
  - [ ] `COMMIT`, `ROLLBACK`
  - [ ] recovery
  - [ ] concurrency management
    - [ ] serializable
    - [ ] multiversion locking (Section 5.4.6)
    - [ ] read uncommitted, read committed, repeatable read (Section 5.4.7)
- [ ] Indexes (Chapter 12)
  - [ ] `CREATE INDEX`
    - [ ] B-Tree index
    - [ ] Hash index (Section 12.3.2)
  - [ ] `SELECT` with index
  - [ ] `CREATE TABLE` with index (Exercises 12.23)
  - [ ] `DROP INDEX` (Exercises 12.25)
- [ ] Views (Section 7.3)
- [ ] Client (Chapter 11)
  - [ ] embedded client
  - [ ] remote client (Section 11.3)
