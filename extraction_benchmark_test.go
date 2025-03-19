package duckdb

import (
	"database/sql/driver"
	"testing"
)

// BenchmarkExtractInt32ColumnOptimized benchmarks the optimized int32 column extraction
func BenchmarkExtractInt32ColumnOptimized(b *testing.B) {
	// Create a test connection
	db, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to open in-memory database: %v", err)
	}
	defer db.Close()

	// Create a test table with a large number of rows
	_, err = db.Exec(`
		CREATE TABLE bench_extract (
			int32_col INTEGER
		);
		
		INSERT INTO bench_extract 
		SELECT i FROM range(1, 100000) t(i);
	`, nil)
	if err != nil {
		b.Fatalf("Failed to create test data: %v", err)
	}

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Query the data using DirectResult for maximum performance
		result, err := db.QueryDirectResult("SELECT * FROM bench_extract ORDER BY int32_col")
		if err != nil {
			b.Fatalf("Failed to query data: %v", err)
		}

		// Extract using the optimized method
		int32Vals, _, err := result.ExtractInt32Column(0)
		if err != nil {
			b.Fatalf("Failed to extract int32 column: %v", err)
		}

		// Verify we got all the data
		if len(int32Vals) != 99999 {
			b.Fatalf("Expected 99999 values, got %d", len(int32Vals))
		}

		result.Close()
	}
}

// BenchmarkCompareExtractionMethods benchmarks different extraction methods to compare performance
func BenchmarkCompareExtractionMethods(b *testing.B) {
	// Create a test connection
	db, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to open in-memory database: %v", err)
	}
	defer db.Close()

	// Create a test table with a large number of rows
	_, err = db.Exec(`
		CREATE TABLE bench_compare (
			int32_col INTEGER,
			int64_col BIGINT,
			float64_col DOUBLE,
			string_col VARCHAR
		);
		
		INSERT INTO bench_compare 
		SELECT 
			i, 
			i*1000, 
			i*0.5, 
			'str_' || i::VARCHAR
		FROM range(1, 10000) t(i);
	`, nil)
	if err != nil {
		b.Fatalf("Failed to create test data: %v", err)
	}

	// Benchmark row-by-row extraction (standard driver.Rows way)
	b.Run("Row-by-row", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query("SELECT * FROM bench_compare ORDER BY int32_col", nil)
			if err != nil {
				b.Fatalf("Failed to query data: %v", err)
			}

			// Extract row by row
			rowCount := 0
			duckRows := rows.(*FastRows)
			values := make([]driver.Value, 4)

			for {
				// Use Next with values slice parameter
				err := duckRows.Next(values)
				if err != nil {
					break // End of rows
				}
				rowCount++
			}

			if rowCount != 9999 {
				b.Fatalf("Expected 9999 rows, got %d", rowCount)
			}

			rows.Close()
		}
	})

	// Benchmark column-wise extraction using our optimized method with DirectResult
	b.Run("DirectResult optimized", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Use DirectResult for maximum performance
			result, err := db.QueryDirectResult("SELECT * FROM bench_compare ORDER BY int32_col")
			if err != nil {
				b.Fatalf("Failed to query data: %v", err)
			}

			// Extract columns using the optimized methods
			int32Vals, _, err := result.ExtractInt32Column(0)
			if err != nil {
				b.Fatalf("Failed to extract int32 column: %v", err)
			}

			int64Vals, _, err := result.ExtractInt64Column(1)
			if err != nil {
				b.Fatalf("Failed to extract int64 column: %v", err)
			}

			float64Vals, _, err := result.ExtractFloat64Column(2)
			if err != nil {
				b.Fatalf("Failed to extract float64 column: %v", err)
			}

			stringVals, _, err := result.ExtractStringColumn(3)
			if err != nil {
				b.Fatalf("Failed to extract string column: %v", err)
			}

			// Verify we got all the data
			if len(int32Vals) != 9999 || len(int64Vals) != 9999 ||
				len(float64Vals) != 9999 || len(stringVals) != 9999 {
				b.Fatalf("Expected 9999 values for all columns")
			}

			result.Close()
		}
	})

	// Also test with batch rows QueryBatch API
	b.Run("QueryBatch optimized", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Use QueryBatch for direct access to batch rows
			rows, err := db.QueryBatch("SELECT * FROM bench_compare ORDER BY int32_col", 1000)
			if err != nil {
				b.Fatalf("Failed to query data: %v", err)
			}

			// Process all rows
			values := make([]driver.Value, 4)
			rowCount := 0

			for {
				err := rows.Next(values)
				if err != nil {
					break
				}
				rowCount++
			}

			// Verify we got all rows
			if rowCount != 9999 {
				b.Fatalf("Expected 9999 rows, got %d", rowCount)
			}

			rows.Close()
		}
	})
}
