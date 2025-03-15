/*
Package duckdb provides a zero-allocation, high-performance, zero-dependency SQL driver for DuckDB in Go.

# Features

- Zero allocation (minimizes GC pressure)
- High performance with Go 1.23 generics
- Thread-safe with atomic operations
- Zero Go dependencies  
- Follows database/sql interfaces
- Full support for DuckDB types
- Prepared statements with context support
- Transactions with proper isolation
- Context cancellation throughout API

# Requirements

- Go 1.23 or higher
- DuckDB C library installed on your system

# Usage

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

# Connection String

The driver supports the following connection string formats:

- In-memory database: `:memory:`
- File-based database: `/path/to/database.db`
*/
package duckdb