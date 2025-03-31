# Kura (蔵) - A Kotlin Database Management System

## Overview

Kura is a database management system built from scratch in Kotlin. This project implements the concepts introduced in the book "[Database Design and Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7)" by Edward Sciore. While the original SimpleDB from the book is implemented in Java and available [here](http://www.cs.bc.edu/~sciore/simpledb/), Kura reimagines it in Kotlin with modern programming practices.

## Getting Started

### Server Setup

#### Building the Server
```sh
./gradlew build
```

#### Running the Server
```sh
# Using the JAR file
java -jar app/build/libs/app.jar

# Or using Gradle
./gradlew run
```

### Client Usage

#### Building the SQL Client
```sh
./gradlew clientJar
```

#### Running the Client
```sh
# Using the JAR file
java -jar app/build/libs/kura-sql-client.jar

# Or using Gradle
./gradlew runClient
```

## SQL Examples

Once connected to the Kura database, you can execute standard SQL commands:

```sql
-- Connection sequence
Connection target [jdbc:kuradb://localhost]>
Connecting to KuraDB: jdbc:kuradb://localhost...
Connection successful

-- Create a table
SQL> create table students (id int, name varchar(20));

-- Insert data
SQL> insert into students (id, name) values (1, 'Tanaka');
SQL> insert into students (id, name) values (2, 'Sato');

-- Query data
SQL> select id, name from students;

-- Update data
SQL> update students set name = 'Suzuki' where id = 1;

-- Delete data
SQL> delete from students where id = 2;
```
