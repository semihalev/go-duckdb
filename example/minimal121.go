package main

import (
	"fmt"
	"log"

	"github.com/semihalev/go-duckdb"
)

func main() {
	// Display DuckDB version
	fmt.Printf("Using DuckDB version: %s\n", duckdb.GetDuckDBVersion())
	fmt.Printf("Driver version: %s\n\n", duckdb.DriverVersion)

	// Open an in-memory database
	conn, err := duckdb.NewConnection(":memory:")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer conn.Close()

	// Create a table
	_, err = conn.ExecContext(nil, "CREATE TABLE version_test (id INTEGER, version VARCHAR)", nil)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Insert DuckDB version info
	_, err = conn.ExecContext(nil, "INSERT INTO version_test VALUES (1, ?)", []duckdb.NamedValue{
		{Ordinal: 1, Value: duckdb.GetDuckDBVersion()},
	})
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}

	// Query the data
	rows, err := conn.QueryContext(nil, "SELECT id, version FROM version_test", nil)
	if err != nil {
		log.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	// Process results
	fmt.Println("DuckDB version information:")
	fmt.Println("---------------------------")
	for rows.Next() {
		var id int
		var version string
		if err := rows.Scan(&id, &version); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		fmt.Printf("ID: %d, DuckDB Version: %s\n", id, version)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Error during row iteration: %v", err)
	}

	// Show DuckDB features
	fmt.Println("\nDuckDB 1.2.1 Features:")
	fmt.Println("---------------------")
	features, err := conn.QueryContext(nil, "PRAGMA version", nil)
	if err != nil {
		log.Fatalf("Failed to query version: %v", err)
	}
	defer features.Close()

	for features.Next() {
		var info string
		if err := features.Scan(&info); err != nil {
			log.Fatalf("Failed to scan version info: %v", err)
		}
		fmt.Printf("Version Info: %s\n", info)
	}

	if err := features.Err(); err != nil {
		log.Fatalf("Error during feature iteration: %v", err)
	}
}