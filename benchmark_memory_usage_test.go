package duckdb

import (
	"database/sql/driver"
	"runtime"
	"strconv"
	"sync"
	"testing"
)

// This benchmark file demonstrates memory usage patterns without the noise
// from string formatting operations, which can obscure the actual memory
// behavior of the database operations.

var (
	// Precomputed strings to eliminate fmt.Sprintf allocations during benchmarks
	precomputedNames = make([]string, 10000)
	nameInitOnce     sync.Once
)

// Initialize precomputed string values to eliminate string formatting during benchmarks
func initPrecomputedStrings() {
	nameInitOnce.Do(func() {
		for i := 0; i < 10000; i++ {
			precomputedNames[i] = "name-" + strconv.Itoa(i)
		}
	})
}

// BenchmarkMemoryUsageComparison compares individual operations vs batch operations
// without the noise from fmt.Sprintf to get a clearer picture of memory usage
func BenchmarkMemoryUsageComparison(b *testing.B) {
	// Initialize our precomputed strings
	initPrecomputedStrings()

	// Get connection to in-memory database
	conn, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to connect to DuckDB: %v", err)
	}
	defer conn.Close()

	// Create a test table
	_, err = conn.ExecDirect(`
		CREATE TABLE memory_test (
			id INTEGER, 
			name VARCHAR, 
			value DOUBLE, 
			flag BOOLEAN
		)
	`)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Run individual operations benchmark
	b.Run("IndividualOperations", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Prepare a statement for insertion
			stmt, err := newStatement(conn, "INSERT INTO memory_test VALUES (?, ?, ?, ?)")
			if err != nil {
				b.Fatalf("Failed to prepare statement: %v", err)
			}

			// Insert 1000 rows individually
			for j := 0; j < 1000; j++ {
				// Use precomputed strings to avoid fmt.Sprintf allocations
				nameIndex := j % len(precomputedNames)

				args := []driver.Value{
					j,
					precomputedNames[nameIndex],
					float64(j) * 1.5,
					j%2 == 0,
				}

				_, err = stmt.Exec(args)
				if err != nil {
					b.Fatalf("Failed to insert row: %v", err)
				}
			}

			stmt.Close()

			// Clean up for next iteration
			_, err = conn.ExecDirect("DELETE FROM memory_test")
			if err != nil {
				b.Fatalf("Failed to clean table: %v", err)
			}
		}
	})

	// Run batch operations benchmark
	b.Run("BatchOperations", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Create parameter sets for batch operation
			batchSize := 1000
			parameterSets := make([][]interface{}, batchSize)

			// Populate parameter sets - reusing precomputed strings
			for j := 0; j < batchSize; j++ {
				nameIndex := j % len(precomputedNames)

				parameterSets[j] = []interface{}{
					j,
					precomputedNames[nameIndex],
					float64(j) * 1.5,
					j%2 == 0,
				}
			}

			// Execute with BatchExec
			_, err := conn.BatchExec("INSERT INTO memory_test VALUES (?, ?, ?, ?)", parameterSets)
			if err != nil {
				b.Fatalf("Failed to execute batch: %v", err)
			}

			// Clean up for next iteration
			_, err = conn.ExecDirect("DELETE FROM memory_test")
			if err != nil {
				b.Fatalf("Failed to clean table: %v", err)
			}
		}
	})
}

// BenchmarkQueryMemoryUsage compares individual vs batch query operations
func BenchmarkQueryMemoryUsage(b *testing.B) {
	// Initialize precomputed strings
	initPrecomputedStrings()

	// Get connection
	conn, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to connect to DuckDB: %v", err)
	}
	defer conn.Close()

	// Create test table and populate with data
	_, err = conn.ExecDirect(`
		CREATE TABLE query_test (
			id INTEGER, 
			name VARCHAR, 
			value DOUBLE, 
			flag BOOLEAN
		)
	`)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data - using batch operations for setup
	batchSize := 10000 // More data for query tests
	parameterSets := make([][]interface{}, batchSize)

	for i := 0; i < batchSize; i++ {
		nameIndex := i % len(precomputedNames)

		parameterSets[i] = []interface{}{
			i,
			precomputedNames[nameIndex],
			float64(i) * 1.5,
			i%2 == 0,
		}
	}

	_, err = conn.BatchExec("INSERT INTO query_test VALUES (?, ?, ?, ?)", parameterSets)
	if err != nil {
		b.Fatalf("Failed to execute batch for setup: %v", err)
	}

	// Individual query benchmark
	b.Run("IndividualQueries", func(b *testing.B) {
		// Prepare statement with parameter for consistent testing
		stmt, err := newStatement(conn, "SELECT * FROM query_test WHERE id > ? LIMIT 1000")
		if err != nil {
			b.Fatalf("Failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Vary the offset to prevent query caching
			startID := i % 9000

			// Execute the query
			rows, err := stmt.Query([]driver.Value{startID})
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}

			// Process all rows to ensure memory usage is comparable
			count := 0
			values := make([]driver.Value, 4)

			for {
				err := rows.Next(values)
				if err != nil {
					break
				}
				count++
			}

			rows.Close()

			if count == 0 {
				b.Fatalf("Expected to retrieve rows")
			}
		}
	})

	// Batch query benchmark
	b.Run("BatchQueries", func(b *testing.B) {
		// Create a batch statement
		batchStmt, err := NewBatchStmt(conn, "SELECT * FROM query_test WHERE id > ? LIMIT 1000", 100)
		if err != nil {
			b.Fatalf("Failed to create batch statement: %v", err)
		}
		defer batchStmt.Close()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Vary the offset to prevent query caching
			startID := i % 9000

			// Execute batch query
			rows, err := batchStmt.QueryBatch(startID)
			if err != nil {
				b.Fatalf("Failed to execute batch query: %v", err)
			}

			// Process all rows to ensure memory usage is comparable
			count := 0
			values := make([]driver.Value, 4)

			for {
				err := rows.Next(values)
				if err != nil {
					break
				}
				count++
			}

			rows.Close()

			if count == 0 {
				b.Fatalf("Expected to retrieve rows")
			}
		}
	})
}

// BenchmarkLargeDataMemoryUsage tests memory behavior with very large datasets
func BenchmarkLargeDataMemoryUsage(b *testing.B) {
	// Skip in short mode
	if testing.Short() {
		b.Skip("Skipping large data benchmark in short mode")
	}

	// Initialize our precomputed strings
	initPrecomputedStrings()

	// Get connection
	conn, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to connect to DuckDB: %v", err)
	}
	defer conn.Close()

	// Create test table
	_, err = conn.ExecDirect(`
		CREATE TABLE large_data_test (
			id INTEGER, 
			name VARCHAR, 
			value DOUBLE, 
			flag BOOLEAN
		)
	`)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Large batch operations
	b.Run("LargeIndividualOperations", func(b *testing.B) {
		// Set a smaller b.N for this intensive benchmark
		if b.N > 3 {
			b.N = 3
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Prepare statement
			stmt, err := newStatement(conn, "INSERT INTO large_data_test VALUES (?, ?, ?, ?)")
			if err != nil {
				b.Fatalf("Failed to prepare statement: %v", err)
			}

			// Insert 10,000 rows individually
			rowCount := 10000
			for j := 0; j < rowCount; j++ {
				nameIndex := j % len(precomputedNames)

				args := []driver.Value{
					j,
					precomputedNames[nameIndex],
					float64(j) * 1.5,
					j%2 == 0,
				}

				_, err = stmt.Exec(args)
				if err != nil {
					b.Fatalf("Failed to insert row: %v", err)
				}
			}

			stmt.Close()

			// Clean up after each iteration
			_, err = conn.ExecDirect("DELETE FROM large_data_test")
			if err != nil {
				b.Fatalf("Failed to clean table: %v", err)
			}
		}
	})

	// Large batch operations
	b.Run("LargeBatchOperations", func(b *testing.B) {
		// Set a smaller b.N for this intensive benchmark
		if b.N > 3 {
			b.N = 3
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Create parameter sets for large batch
			rowCount := 10000
			parameterSets := make([][]interface{}, rowCount)

			// Populate parameter sets
			for j := 0; j < rowCount; j++ {
				nameIndex := j % len(precomputedNames)

				parameterSets[j] = []interface{}{
					j,
					precomputedNames[nameIndex],
					float64(j) * 1.5,
					j%2 == 0,
				}
			}

			// Execute large batch
			_, err := conn.BatchExec("INSERT INTO large_data_test VALUES (?, ?, ?, ?)", parameterSets)
			if err != nil {
				b.Fatalf("Failed to execute large batch: %v", err)
			}

			// Clean up after each iteration
			_, err = conn.ExecDirect("DELETE FROM large_data_test")
			if err != nil {
				b.Fatalf("Failed to clean table: %v", err)
			}
		}
	})
}

// BenchmarkScalingMemoryUsage tests how memory usage scales with increasing batch size
func BenchmarkScalingMemoryUsage(b *testing.B) {
	// Initialize precomputed strings
	initPrecomputedStrings()

	// Get connection
	conn, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to connect to DuckDB: %v", err)
	}
	defer conn.Close()

	// Create test table
	_, err = conn.ExecDirect(`
		CREATE TABLE scaling_test (
			id INTEGER, 
			name VARCHAR, 
			value DOUBLE, 
			flag BOOLEAN
		)
	`)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Helper function to measure memory per operation
	measureMemPerRow := func(rowCount int, batchMode bool) (bytesPerRow int, allocsPerRow float64) {
		var m1, m2 runtime.MemStats

		runtime.GC()
		runtime.ReadMemStats(&m1)

		if batchMode {
			// Batch mode - create a batch and execute it once
			parameterSets := make([][]interface{}, rowCount)

			for j := 0; j < rowCount; j++ {
				nameIndex := j % len(precomputedNames)

				parameterSets[j] = []interface{}{
					j,
					precomputedNames[nameIndex],
					float64(j) * 1.5,
					j%2 == 0,
				}
			}

			_, err = conn.BatchExec("INSERT INTO scaling_test VALUES (?, ?, ?, ?)", parameterSets)
			if err != nil {
				b.Fatalf("Failed to execute batch: %v", err)
			}
		} else {
			// Individual mode - execute row by row
			stmt, err := newStatement(conn, "INSERT INTO scaling_test VALUES (?, ?, ?, ?)")
			if err != nil {
				b.Fatalf("Failed to prepare statement: %v", err)
			}

			for j := 0; j < rowCount; j++ {
				nameIndex := j % len(precomputedNames)

				args := []driver.Value{
					j,
					precomputedNames[nameIndex],
					float64(j) * 1.5,
					j%2 == 0,
				}

				_, err = stmt.Exec(args)
				if err != nil {
					b.Fatalf("Failed to insert row: %v", err)
				}
			}

			stmt.Close()
		}

		runtime.ReadMemStats(&m2)

		// Calculate memory difference
		memDiff := int(m2.TotalAlloc - m1.TotalAlloc)
		allocDiff := float64(m2.Mallocs - m1.Mallocs)

		// Clean up
		_, err = conn.ExecDirect("DELETE FROM scaling_test")
		if err != nil {
			b.Fatalf("Failed to clean table: %v", err)
		}

		return memDiff / rowCount, allocDiff / float64(rowCount)
	}

	// Test with different row counts to see how memory usage scales
	rowCounts := []int{10, 100, 1000, 5000, 10000}

	for _, rowCount := range rowCounts {
		// Skip larger batches in short mode
		if testing.Short() && rowCount > 1000 {
			continue
		}

		b.Run(strconv.Itoa(rowCount)+"_Rows", func(b *testing.B) {
			// Reduce iterations for large row counts
			if rowCount >= 5000 && b.N > 5 {
				b.N = 5
			}

			indivMemPerRow, indivAllocPerRow := measureMemPerRow(rowCount, false)
			batchMemPerRow, batchAllocPerRow := measureMemPerRow(rowCount, true)

			b.Logf("Row Count: %d\n", rowCount)
			b.Logf("Individual: %d bytes/row, %.2f allocs/row\n", indivMemPerRow, indivAllocPerRow)
			b.Logf("Batch:      %d bytes/row, %.2f allocs/row\n", batchMemPerRow, batchAllocPerRow)
			b.Logf("Ratio:      %.2fx bytes, %.2fx allocs\n",
				float64(batchMemPerRow)/float64(indivMemPerRow),
				batchAllocPerRow/indivAllocPerRow)
		})
	}
}
