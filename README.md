# go-duckdb

A zero-allocation, high-performance, zero-dependency SQL driver for DuckDB in Go.

[![Go Reference](https://pkg.go.dev/badge/github.com/semihalev/go-duckdb.svg)](https://pkg.go.dev/github.com/semihalev/go-duckdb)
[![Go Report Card](https://goreportcard.com/badge/github.com/semihalev/go-duckdb)](https://goreportcard.com/report/github.com/semihalev/go-duckdb)
[![License](https://img.shields.io/github/license/semihalev/go-duckdb)](https://github.com/semihalev/go-duckdb/blob/main/LICENSE)

## Features

- Zero allocation (minimizes GC pressure)
- High performance with Go 1.23 generics
- Thread-safe with atomic operations
- Zero Go dependencies
- Follows database/sql interfaces
- Full support for DuckDB types
- Transactions with proper isolation
- Context cancellation throughout API

## DuckDB 1.2.1 Compatibility

This driver is compatible with DuckDB 1.2.1. However, there are some limitations:

1. **Prepared statements** don't work correctly with parameter binding in 1.2.1 
   and are currently disabled (direct query execution is used instead)
2. **BLOB handling** is simplified
3. **Named parameters** are not yet supported

These limitations will be addressed in future versions.

## Requirements

- Go 1.23 or higher
- No external dependencies! The DuckDB library is included in the project for all major platforms:

## Installation

Simply install the Go driver:

```bash
go get github.com/semihalev/go-duckdb
```

No need to install DuckDB separately! The driver includes pre-compiled static libraries for:

- Linux (amd64, arm64)
- macOS (amd64, arm64)
- Windows (amd64)

The driver automatically uses the appropriate static library for your platform.

## Usage

```go
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/semihalev/go-duckdb"
)

func main() {
	// Open an in-memory database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER)`)
	if err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec(`INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)`)
	if err != nil {
		log.Fatalf("failed to insert data: %v", err)
	}

	// Query with a prepared statement
	rows, err := db.Query(`SELECT id, name, age FROM users WHERE age > ?`, 20)
	if err != nil {
		log.Fatalf("failed to query data: %v", err)
	}
	defer rows.Close()

	// Process the result
	for rows.Next() {
		var id int
		var name string
		var age int

		err := rows.Scan(&id, &name, &age)
		if err != nil {
			log.Fatalf("failed to scan row: %v", err)
		}

		fmt.Printf("User: %d, %s, %d\n", id, name, age)
	}
}
```

## Connection String

The driver supports the following connection string formats:

- In-memory database: `:memory:`
- File-based database: `/path/to/database.db`

## Benchmarks

Run the benchmarks with:

```bash
make bench
```

## Running Tests

```bash
make test
```

## License

MIT License - see [LICENSE](LICENSE) for more details.
