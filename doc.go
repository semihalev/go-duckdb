/*
Package duckdb provides a high-performance Go driver for DuckDB with both standard SQL and low-level API support.

# Overview

Go-DuckDB is a zero-dependency, high-performance driver for DuckDB, a fast analytical database. It offers two API options:

1. Standard database/sql interface for compatibility with Go's ecosystem
2. Low-level direct API for maximum performance with minimal allocations

The driver is designed for maximum performance with minimal memory overhead, making it ideal for both 
application integration and high-performance analytical workloads.

# Standard SQL API Example

Using the standard database/sql interface:

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

# Low-Level Direct API Example

For maximum performance, use the direct API:

	package main

	import (
		"fmt"
		"log"

		"github.com/semihalev/go-duckdb"
	)

	func main() {
		// Create a new connection
		conn, err := duckdb.NewConnection(":memory:")
		if err != nil {
			log.Fatalf("failed to create connection: %v", err)
		}
		defer conn.Close()

		// Create a table
		_, err = conn.ExecDirect(`CREATE TABLE analytics (
			id INTEGER, 
			category VARCHAR, 
			value DOUBLE, 
			active BOOLEAN
		)`)
		if err != nil {
			log.Fatalf("failed to create table: %v", err)
		}

		// Insert sample data
		_, err = conn.ExecDirect(`INSERT INTO analytics VALUES 
			(1, 'A', 10.5, true),
			(2, 'B', 20.3, false),
			(3, 'A', 15.2, true),
			(4, 'C', 7.8, true),
			(5, 'B', 30.1, false)`)
		if err != nil {
			log.Fatalf("failed to insert data: %v", err)
		}

		// Use the high-performance direct result API
		result, err := conn.QueryDirectResult(`
			SELECT 
				id, 
				category, 
				value, 
				active 
			FROM analytics 
			WHERE value > 10.0
		`)
		if err != nil {
			log.Fatalf("failed to query data: %v", err)
		}
		defer result.Close()

		// Get row count
		rowCount := result.RowCount()
		fmt.Printf("Found %d matching records\n", rowCount)

		// Extract entire columns at once (much faster than row-by-row)
		ids, idNulls, err := result.ExtractInt32Column(0)
		if err != nil {
			log.Fatalf("failed to extract id column: %v", err)
		}

		categories, categoryNulls, err := result.ExtractStringColumn(1)
		if err != nil {
			log.Fatalf("failed to extract category column: %v", err)
		}

		values, valueNulls, err := result.ExtractFloat64Column(2)
		if err != nil {
			log.Fatalf("failed to extract value column: %v", err)
		}

		actives, activeNulls, err := result.ExtractBoolColumn(3)
		if err != nil {
			log.Fatalf("failed to extract active column: %v", err)
		}

		// Process all data at once (vector-oriented processing)
		for i := 0; i < rowCount; i++ {
			// Check for NULL values
			if idNulls[i] || categoryNulls[i] || valueNulls[i] || activeNulls[i] {
				fmt.Printf("Row %d contains NULL values\n", i)
				continue
			}

			fmt.Printf("Row %d: ID=%d, Category=%s, Value=%.2f, Active=%v\n",
				i, ids[i], categories[i], values[i], actives[i])
		}
	}

# Key Features

The driver offers several high-performance features:

1. Zero Allocation Design - Minimizes GC pressure and memory overhead
2. Dual API Support - Standard database/sql interface and low-level direct API
3. Advanced Optimizations:
   - SIMD acceleration with AVX2 and ARM64 NEON
   - Zero-copy column extraction
   - Tiered buffer pools
   - String caching and deduplication
4. High-Performance Features:
   - Batch parameter binding
   - Appender API for bulk data insertion
   - Direct column-wise extraction
   - Memory mapping with zero-copy architecture
5. Full DuckDB Support:
   - All DuckDB data types
   - Transactions with proper isolation
   - Context cancellation
   - Thread-safe operations
6. Zero Go Dependencies - Lightweight and portable

# Connection String

The driver supports the following connection string formats:

- In-memory database: `:memory:`
- File-based database: `/path/to/database.db`
- Read-only mode: `/path/to/database.db?access_mode=READ_ONLY`
- Temporary directory: `/path/to/database.db?temp_directory=/path/to/temp`

# DuckDB Compatibility

This driver is compatible with DuckDB 1.2.1 and provides complete support for all DuckDB data types and features.
*/
package duckdb