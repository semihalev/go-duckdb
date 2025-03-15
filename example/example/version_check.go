package main

import (
	"database/sql"
	"fmt"
	"log"

	duckdb "github.com/semihalev/go-duckdb"
)

func main() {
	// Get and print DuckDB version from the driver
	version := duckdb.GetDuckDBVersion()
	fmt.Printf("DuckDB Version (from driver): %s\n", version)
	fmt.Printf("  Major: %d, Minor: %d, Patch: %d\n", 
		version.Major, version.Minor, version.Patch)
	fmt.Printf("  Is >= 1.2.0: %v\n", version.IsAtLeast120())
	fmt.Printf("  Is >= 1.2.1: %v\n", version.IsAtLeast121())

	// Open an in-memory database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Get the DuckDB version from the database
	var libVersion string
	err = db.QueryRow("SELECT library_version()").Scan(&libVersion)
	if err != nil {
		log.Fatalf("Failed to get version: %v", err)
	}

	fmt.Printf("\nDuckDB Library Version (from SQL): %s\n", libVersion)
	
	// Run a simple query
	rows, err := db.Query("SELECT 1 + 1 AS result")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()
	
	if rows.Next() {
		var result int
		if err := rows.Scan(&result); err != nil {
			log.Fatalf("Failed to scan result: %v", err)
		}
		fmt.Printf("1 + 1 = %d\n", result)
	}
	
	fmt.Println("DuckDB connection successful!")
}