package duckdb

import (
	"database/sql/driver"
	"fmt"
	"math/rand"
	"testing"
)

// Benchmark for extractColumnBatch
func BenchmarkExtractColumnBatch(b *testing.B) {
	// Skip if short test mode
	if testing.Short() {
		b.Skip("Skipping extraction benchmark in short mode")
	}

	// Get connection
	conn, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Create test tables for different data types
	_, err = conn.Exec("CREATE TABLE int_test (id INTEGER, value INTEGER, name VARCHAR)", nil)
	if err != nil {
		b.Fatalf("Failed to create int table: %v", err)
	}

	_, err = conn.Exec("CREATE TABLE bigint_test (id INTEGER, value BIGINT, name VARCHAR)", nil)
	if err != nil {
		b.Fatalf("Failed to create bigint table: %v", err)
	}

	_, err = conn.Exec("CREATE TABLE double_test (id INTEGER, value DOUBLE, name VARCHAR)", nil)
	if err != nil {
		b.Fatalf("Failed to create double table: %v", err)
	}

	_, err = conn.Exec("CREATE TABLE bool_test (id INTEGER, value BOOLEAN, name VARCHAR)", nil)
	if err != nil {
		b.Fatalf("Failed to create bool table: %v", err)
	}

	// Insert test data - 10,000 rows for each table
	insertStmt1, err := newStatement(conn, "INSERT INTO int_test VALUES (?, ?, ?)")
	if err != nil {
		b.Fatalf("Failed to prepare int insert statement: %v", err)
	}
	defer insertStmt1.Close()

	insertStmt2, err := newStatement(conn, "INSERT INTO bigint_test VALUES (?, ?, ?)")
	if err != nil {
		b.Fatalf("Failed to prepare bigint insert statement: %v", err)
	}
	defer insertStmt2.Close()

	insertStmt3, err := newStatement(conn, "INSERT INTO double_test VALUES (?, ?, ?)")
	if err != nil {
		b.Fatalf("Failed to prepare double insert statement: %v", err)
	}
	defer insertStmt3.Close()

	insertStmt4, err := newStatement(conn, "INSERT INTO bool_test VALUES (?, ?, ?)")
	if err != nil {
		b.Fatalf("Failed to prepare bool insert statement: %v", err)
	}
	defer insertStmt4.Close()

	// Create test data
	for i := 0; i < 10000; i++ {
		// Insert into int_test
		_, err = insertStmt1.Exec([]driver.Value{i, i * 10, fmt.Sprintf("name-%d", i)})
		if err != nil {
			b.Fatalf("Failed to insert int test data: %v", err)
		}

		// Insert into bigint_test
		_, err = insertStmt2.Exec([]driver.Value{i, int64(i) * 1000000000, fmt.Sprintf("name-%d", i)})
		if err != nil {
			b.Fatalf("Failed to insert bigint test data: %v", err)
		}

		// Insert into double_test
		_, err = insertStmt3.Exec([]driver.Value{i, float64(i) * 1.5, fmt.Sprintf("name-%d", i)})
		if err != nil {
			b.Fatalf("Failed to insert double test data: %v", err)
		}

		// Insert into bool_test with some nulls
		var boolVal driver.Value
		if i%10 == 0 {
			boolVal = nil // Some NULL values
		} else {
			boolVal = (i%2 == 0)
		}
		_, err = insertStmt4.Exec([]driver.Value{i, boolVal, fmt.Sprintf("name-%d", i)})
		if err != nil {
			b.Fatalf("Failed to insert bool test data: %v", err)
		}
	}

	// Benchmark INTEGER extraction with different batch sizes
	batchSizes := []int{100, 1000, 5000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("INTEGER_BatchSize=%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Use an offset based on iteration to avoid query result caching
				offset := (i * 97) % 9900 // Ensure we don't exceed row count

				rows, err := conn.QueryBatch(fmt.Sprintf("SELECT id, value, name FROM int_test WHERE id >= %d LIMIT 1000", offset), batchSize)
				if err != nil {
					b.Fatalf("Failed to execute batch query: %v", err)
				}

				count := 0
				for {
					values := make([]driver.Value, 3)
					err := rows.Next(values)
					if err != nil {
						break
					}
					count++
				}
				rows.Close()
			}
		})
	}

	// Benchmark BIGINT extraction with different batch sizes
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BIGINT_BatchSize=%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Use an offset based on iteration to avoid query result caching
				offset := (i * 97) % 9900 // Ensure we don't exceed row count

				rows, err := conn.QueryBatch(fmt.Sprintf("SELECT id, value, name FROM bigint_test WHERE id >= %d LIMIT 1000", offset), batchSize)
				if err != nil {
					b.Fatalf("Failed to execute batch query: %v", err)
				}

				count := 0
				for {
					values := make([]driver.Value, 3)
					err := rows.Next(values)
					if err != nil {
						break
					}
					count++
				}
				rows.Close()
			}
		})
	}

	// Benchmark DOUBLE extraction with different batch sizes
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("DOUBLE_BatchSize=%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Use an offset based on iteration to avoid query result caching
				offset := (i * 97) % 9900 // Ensure we don't exceed row count

				rows, err := conn.QueryBatch(fmt.Sprintf("SELECT id, value, name FROM double_test WHERE id >= %d LIMIT 1000", offset), batchSize)
				if err != nil {
					b.Fatalf("Failed to execute batch query: %v", err)
				}

				count := 0
				for {
					values := make([]driver.Value, 3)
					err := rows.Next(values)
					if err != nil {
						break
					}
					count++
				}
				rows.Close()
			}
		})
	}

	// Benchmark BOOLEAN extraction with different batch sizes
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BOOLEAN_BatchSize=%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Use an offset based on iteration to avoid query result caching
				offset := (i * 97) % 9900 // Ensure we don't exceed row count

				rows, err := conn.QueryBatch(fmt.Sprintf("SELECT id, value, name FROM bool_test WHERE id >= %d LIMIT 1000", offset), batchSize)
				if err != nil {
					b.Fatalf("Failed to execute batch query: %v", err)
				}

				count := 0
				for {
					values := make([]driver.Value, 3)
					err := rows.Next(values)
					if err != nil {
						break
					}
					count++
				}
				rows.Close()
			}
		})
	}

	// Benchmark mixed column types (realistic use case)
	b.Run("MixedTypes", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Use a more complex query with joins and multiple types
			offset := (i * 97) % 9900 // Ensure we don't exceed row count

			query := fmt.Sprintf(`
				SELECT i.id, i.value as int_val, b.value as bool_val, d.value as double_val
				FROM int_test i
				JOIN bool_test b ON i.id = b.id
				JOIN double_test d ON i.id = d.id
				WHERE i.id >= %d
				LIMIT 1000
			`, offset)

			rows, err := conn.QueryBatch(query, 1000)
			if err != nil {
				b.Fatalf("Failed to execute complex batch query: %v", err)
			}

			count := 0
			for {
				values := make([]driver.Value, 4)
				err := rows.Next(values)
				if err != nil {
					break
				}
				count++
			}
			rows.Close()
		}
	})
}

// BenchmarkCompareOptimizations compares the performance of direct row-by-row extraction
// vs the optimized batch extraction
func BenchmarkCompareOptimizations(b *testing.B) {
	// Skip if short test mode
	if testing.Short() {
		b.Skip("Skipping optimization comparison benchmark in short mode")
	}

	// Get connection
	conn, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to get connection: %v", err)
	}
	defer conn.Close()

	// Create a test table with different column types
	_, err = conn.Exec(`
		CREATE TABLE test_data (
			id INTEGER, 
			int_val INTEGER,
			big_val BIGINT,
			double_val DOUBLE,
			bool_val BOOLEAN,
			text_val VARCHAR
		)
	`, nil)
	if err != nil {
		b.Fatalf("Failed to create test table: %v", err)
	}

	// Generate random test data
	r := rand.New(rand.NewSource(42)) // Use fixed seed for reproducibility

	insertStmt, err := newStatement(conn, `
		INSERT INTO test_data 
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		b.Fatalf("Failed to prepare insert statement: %v", err)
	}
	defer insertStmt.Close()

	// Insert 10,000 rows of test data
	for i := 0; i < 10000; i++ {
		// Mix of values with some nulls
		var intVal, bigVal, doubleVal, boolVal, textVal driver.Value

		if i%10 != 0 { // 90% non-null values
			intVal = r.Intn(1000000)
			bigVal = int64(r.Intn(1000000)) * 1000000
			doubleVal = r.Float64() * 1000000
			boolVal = (r.Intn(2) == 1)
			textVal = fmt.Sprintf("text-%d-%d", i, r.Intn(1000))
		}

		_, err = insertStmt.Exec([]driver.Value{
			i,
			intVal,
			bigVal,
			doubleVal,
			boolVal,
			textVal,
		})
		if err != nil {
			b.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Create indexes to ensure query performance is reasonable
	_, err = conn.Exec("CREATE INDEX idx_test_data_id ON test_data(id)", nil)
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}

	// Compare optimized vs row-by-row extraction
	benchmarkSizes := []int{100, 1000, 5000}

	for _, size := range benchmarkSizes {
		// Test with different row limits
		b.Run(fmt.Sprintf("Rows=%d", size), func(b *testing.B) {
			// First benchmark: Standard method (row-by-row extraction)
			b.Run("Standard", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					// Use offset to avoid caching
					offset := (i * 97) % (10000 - size)

					// Execute standard query and fetch results
					rows, err := conn.Query(fmt.Sprintf(`
						SELECT * FROM test_data 
						WHERE id >= %d 
						ORDER BY id 
						LIMIT %d
					`, offset, size), nil)
					if err != nil {
						b.Fatalf("Failed to execute standard query: %v", err)
					}

					// Extract all rows
					count := 0
					for {
						values := make([]driver.Value, 6)
						err := rows.Next(values)
						if err != nil {
							break
						}
						count++
					}
					rows.Close()

					if count != size {
						b.Fatalf("Expected %d rows, got %d", size, count)
					}
				}
			})

			// Second benchmark: Optimized BatchQuery method
			b.Run("BatchOptimized", func(b *testing.B) {
				b.ResetTimer()
				b.ReportAllocs()

				for i := 0; i < b.N; i++ {
					// Use offset to avoid caching
					offset := (i * 97) % (10000 - size)

					// Execute batch query with optimized extraction
					rows, err := conn.QueryBatch(fmt.Sprintf(`
						SELECT * FROM test_data 
						WHERE id >= %d 
						ORDER BY id 
						LIMIT %d
					`, offset, size), 1000)
					if err != nil {
						b.Fatalf("Failed to execute batch query: %v", err)
					}

					// Extract all rows
					count := 0
					for {
						values := make([]driver.Value, 6)
						err := rows.Next(values)
						if err != nil {
							break
						}
						count++
					}
					rows.Close()

					if count != size {
						b.Fatalf("Expected %d rows, got %d", size, count)
					}
				}
			})
		})
	}
}
