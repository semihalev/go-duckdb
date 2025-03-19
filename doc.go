/*
Package duckdb provides low-level, high-performance SQL driver for DuckDB in Go.

This driver implements the database/sql interfaces for DuckDB, a high-performance analytical database.
It is designed to be fast, with minimal allocations, and to support all DuckDB features.

Basic usage:

	import (
		"database/sql"
		_ "github.com/semihalev/go-duckdb"
	)

	func main() {
		// Open an in-memory database
		db, err := sql.Open("duckdb", ":memory:")
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		// Use the database
		rows, err := db.Query("SELECT 1")
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		// Process results
		for rows.Next() {
			var val int
			if err := rows.Scan(&val); err != nil {
				log.Fatal(err)
			}
			fmt.Println(val)
		}
	}

For efficient bulk data loading, use the Appender API:

	import (
		"github.com/semihalev/go-duckdb"
	)

	func main() {
		// Open a connection
		conn, err := duckdb.NewConnection(":memory:")
		if err != nil {
			log.Fatal(err)
		}
		defer conn.Close()

		// Create a table
		_, err = conn.ExecContext(context.Background(), "CREATE TABLE test (id INTEGER, name VARCHAR)", nil)
		if err != nil {
			log.Fatal(err)
		}

		// Create an appender
		appender, err := duckdb.NewAppender(conn, "main", "test")
		if err != nil {
			log.Fatal(err)
		}
		defer appender.Close()

		// Append rows
		for i := 0; i < 1000; i++ {
			if err := appender.AppendRow(i, fmt.Sprintf("Name %d", i)); err != nil {
				log.Fatal(err)
			}
		}

		// Flush the appender
		if err := appender.Flush(); err != nil {
			log.Fatal(err)
		}
	}

The driver is compatible with DuckDB 1.2.1 and provides the following features:

- Zero allocation design to minimize GC pressure
- High performance with atomic operations for thread safety
- Full support for DuckDB data types
- Transaction support with proper isolation levels
- Context cancellation throughout the API
- Efficient bulk data loading with the Appender API
- Prepared statements for query reuse
*/
package duckdb
