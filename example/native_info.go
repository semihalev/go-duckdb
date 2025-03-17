package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/semihalev/go-duckdb"
)

// This example demonstrates how to check if native optimizations are available
// and shows performance benefits when using them
func main() {
	// Check version and native optimization information
	versionInfo := duckdb.GetVersionInfo()
	fmt.Println("Version Information:")
	fmt.Println(versionInfo)
	fmt.Println()

	// Get detailed information about native optimizations
	nativeInfo := duckdb.GetNativeOptimizationInfo()
	fmt.Println("Native Optimization Information:")
	fmt.Println(nativeInfo)
	fmt.Println()

	// Create a test database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table with some data
	_, err = db.Exec(`
		CREATE TABLE test (id INTEGER, value DOUBLE);
		INSERT INTO test SELECT range, random() FROM range(0, 1000000);
	`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// First, run a query using the standard database/sql interface
	// This will use the optimized implementation but with standard row-by-row access
	fmt.Println("Running benchmark with standard database/sql interface...")
	start := time.Now()

	var sum float64
	rows, err := db.Query("SELECT SUM(value) FROM test")
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	if rows.Next() {
		err = rows.Scan(&sum)
		if err != nil {
			log.Fatalf("Scan failed: %v", err)
		}
	}
	rows.Close()

	fmt.Printf("Standard interface: Sum = %f, took %v\n\n", sum, time.Since(start))

	// Now, use the direct, columnar API for faster access when native optimizations are available
	fmt.Println("Running benchmark with direct columnar API...")
	start = time.Now()

	// Open a direct connection
	conn, err := duckdb.NewConnection(":memory:")
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create the same test table
	_, err = conn.ExecDirect(`
		CREATE TABLE test (id INTEGER, value DOUBLE);
		INSERT INTO test SELECT range, random() FROM range(0, 1000000);
	`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Use the columnar API for optimized extraction
	result, err := conn.QueryColumnar("SELECT SUM(value) FROM test")
	if err != nil {
		log.Fatalf("Columnar query failed: %v", err)
	}

	// Access the value directly from the columnar result
	columnarSum := result.Columns[0].([]float64)[0]

	fmt.Printf("Columnar API: Sum = %f, took %v\n\n", columnarSum, time.Since(start))

	// Performance improvement note
	if nativeInfo.Available {
		fmt.Println("Native optimizations are providing SIMD acceleration for:")
		fmt.Printf("- %s SIMD instructions\n", nativeInfo.SIMDFeatures)
		fmt.Println("- Zero-copy data extraction")
		fmt.Println("- Optimized memory access patterns")
	} else {
		fmt.Println("Native optimizations not available. For better performance:")
		fmt.Println("- Run 'make native-current' to build for your platform")
		fmt.Println("- Or use 'make native-dynamic' to build for all platforms")
		fmt.Println("- Source code is in the 'src' directory")
		fmt.Printf("- Error: %s\n", nativeInfo.Error)
	}
}
