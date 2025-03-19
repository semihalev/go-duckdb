package duckdb

import (
	"runtime"
	"testing"
)

func TestParallelExtraction(t *testing.T) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create test table with multiple columns of the same type for parallel extraction testing
	_, err = conn.ExecDirect(`
        CREATE TABLE parallel_test (
            id1 INTEGER,
            id2 INTEGER,
            id3 INTEGER,
            id4 INTEGER,
            big1 BIGINT,
            big2 BIGINT,
            big3 BIGINT,
            val1 DOUBLE,
            val2 DOUBLE,
            val3 DOUBLE,
            val4 DOUBLE
        )
    `)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert some test data
	_, err = conn.ExecDirect(`
        INSERT INTO parallel_test
        SELECT 
            i AS id1,
            i + 1 AS id2,
            i + 2 AS id3,
            i + 3 AS id4,
            i * 1000 AS big1,
            i * 2000 AS big2,
            i * 3000 AS big3,
            i * 1.1 AS val1,
            i * 2.2 AS val2,
            i * 3.3 AS val3,
            i * 4.4 AS val4
        FROM range(0, 1000) t(i)
    `)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Query data with DirectResult
	result, err := conn.QueryDirectResult("SELECT * FROM parallel_test ORDER BY id1")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer result.Close()

	// Test parallel extraction of int32 columns
	int32Cols, _, err := result.ExtractInt32ColumnsParallel([]int{0, 1, 2, 3})
	if err != nil {
		t.Fatalf("Failed to extract int32 columns in parallel: %v", err)
	}

	// Verify extraction results
	if len(int32Cols) != 4 {
		t.Errorf("Expected 4 int32 columns, got %d", len(int32Cols))
	}

	// Check values in first row across all columns
	expectedFirstRow := []int32{0, 1, 2, 3}
	for i := 0; i < 4; i++ {
		if int32Cols[i][0] != expectedFirstRow[i] {
			t.Errorf("Column %d, row 0: expected %d, got %d", i, expectedFirstRow[i], int32Cols[i][0])
		}
	}

	// Test parallel extraction of int64 columns
	int64Cols, _, err := result.ExtractInt64ColumnsParallel([]int{4, 5, 6})
	if err != nil {
		t.Fatalf("Failed to extract int64 columns in parallel: %v", err)
	}

	// Verify extraction results
	if len(int64Cols) != 3 {
		t.Errorf("Expected 3 int64 columns, got %d", len(int64Cols))
	}

	// Check values in first row across all columns
	expectedFirstRowBig := []int64{0, 0, 0}
	for i := 0; i < 3; i++ {
		if int64Cols[i][0] != expectedFirstRowBig[i] {
			t.Errorf("Column %d, row 0: expected %d, got %d", i, expectedFirstRowBig[i], int64Cols[i][0])
		}
	}

	// Test parallel extraction of float64 columns
	floatCols, _, err := result.ExtractFloat64ColumnsParallel([]int{7, 8, 9, 10})
	if err != nil {
		t.Fatalf("Failed to extract float64 columns in parallel: %v", err)
	}

	// Verify extraction results
	if len(floatCols) != 4 {
		t.Errorf("Expected 4 float64 columns, got %d", len(floatCols))
	}

	// Check values in first row across all columns
	expectedFirstRowFloat := []float64{0, 0, 0, 0}
	for i := 0; i < 4; i++ {
		if floatCols[i][0] != expectedFirstRowFloat[i] {
			t.Errorf("Column %d, row 0: expected %f, got %f", i, expectedFirstRowFloat[i], floatCols[i][0])
		}
	}
}

func BenchmarkParallelExtraction(b *testing.B) {
	b.Skip("Due do fatal error: semasleep on Darwin signal stack")
	// Connect to in-memory database
	conn, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to connect to DuckDB: %v", err)
	}
	defer conn.Close()

	// Create table with multiple columns of same type for parallel extraction benchmarks
	_, err = conn.ExecDirect(`
		CREATE TABLE bench_parallel (
			id1 INTEGER,
			id2 INTEGER,
			id3 INTEGER,
			id4 INTEGER,
			big1 BIGINT,
			big2 BIGINT,
			big3 BIGINT,
			num1 DOUBLE,
			num2 DOUBLE,
			num3 DOUBLE,
			num4 DOUBLE
		);
	`)
	if err != nil {
		b.Fatalf("Failed to create batch test table: %v", err)
	}

	// Generate test data with multiple columns of same type
	_, err = conn.ExecDirect(`
		INSERT INTO bench_parallel
		SELECT 
			i AS id1,
			i + 1 AS id2,
			i + 2 AS id3,
			i + 3 AS id4,
			i * 1000 AS big1,
			i * 2000 AS big2,
			i * 3000 AS big3,
			i * 1.1 AS num1,
			i * 2.2 AS num2,
			i * 3.3 AS num3,
			i * 4.4 AS num4
		FROM range(0, 100000) t(i);
	`)
	if err != nil {
		b.Fatalf("Failed to insert batch test data: %v", err)
	}

	// Record the number of CPU cores for context
	b.Logf("Running benchmark on system with %d CPU cores", runtime.NumCPU())

	// Benchmark sequential int32 columns extraction (using BatchTool)
	b.Run("Int32Batch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Query multiple columns
			result, err := conn.QueryDirectResult("SELECT id1, id2, id3, id4 FROM bench_parallel")
			if err != nil {
				b.Fatalf("Failed to query data: %v", err)
			}

			// Extract columns in batch (sequential)
			cols, _, err := result.ExtractInt32ColumnsBatch([]int{0, 1, 2, 3})
			if err != nil {
				b.Fatalf("Failed to extract columns in batch: %v", err)
			}

			// Verify we got the right amount of data
			if len(cols) != 4 || len(cols[0]) != 100000 {
				b.Fatalf("Expected 4 columns with 100000 rows each")
			}

			result.Close()
		}
	})

	// Benchmark parallel int32 columns extraction
	b.Run("Int32Parallel", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Query multiple columns
			result, err := conn.QueryDirectResult("SELECT id1, id2, id3, id4 FROM bench_parallel")
			if err != nil {
				b.Fatalf("Failed to query data: %v", err)
			}

			// Extract columns in parallel
			cols, _, err := result.ExtractInt32ColumnsParallel([]int{0, 1, 2, 3})
			if err != nil {
				b.Fatalf("Failed to extract columns in parallel: %v", err)
			}

			// Verify we got the right amount of data
			if len(cols) != 4 || len(cols[0]) != 100000 {
				b.Fatalf("Expected 4 columns with 100000 rows each")
			}

			result.Close()
		}
	})

	// Benchmark sequential int64 columns extraction (using BatchTool)
	b.Run("Int64Batch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Query multiple columns
			result, err := conn.QueryDirectResult("SELECT big1, big2, big3 FROM bench_parallel")
			if err != nil {
				b.Fatalf("Failed to query data: %v", err)
			}

			// Extract columns in batch (sequential)
			cols, _, err := result.ExtractInt64ColumnsBatch([]int{0, 1, 2})
			if err != nil {
				b.Fatalf("Failed to extract columns in batch: %v", err)
			}

			// Verify we got the right amount of data
			if len(cols) != 3 || len(cols[0]) != 100000 {
				b.Fatalf("Expected 3 columns with 100000 rows each")
			}

			result.Close()
		}
	})

	// Benchmark parallel int64 columns extraction
	b.Run("Int64Parallel", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Query multiple columns
			result, err := conn.QueryDirectResult("SELECT big1, big2, big3 FROM bench_parallel")
			if err != nil {
				b.Fatalf("Failed to query data: %v", err)
			}

			// Extract columns in parallel
			cols, _, err := result.ExtractInt64ColumnsParallel([]int{0, 1, 2})
			if err != nil {
				b.Fatalf("Failed to extract columns in parallel: %v", err)
			}

			// Verify we got the right amount of data
			if len(cols) != 3 || len(cols[0]) != 100000 {
				b.Fatalf("Expected 3 columns with 100000 rows each")
			}

			result.Close()
		}
	})

	// Benchmark sequential float64 columns extraction (using BatchTool)
	b.Run("Float64Batch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Query multiple columns
			result, err := conn.QueryDirectResult("SELECT num1, num2, num3, num4 FROM bench_parallel")
			if err != nil {
				b.Fatalf("Failed to query data: %v", err)
			}

			// Extract columns in batch (sequential)
			cols, _, err := result.ExtractFloat64ColumnsBatch([]int{0, 1, 2, 3})
			if err != nil {
				b.Fatalf("Failed to extract columns in batch: %v", err)
			}

			// Verify we got the right amount of data
			if len(cols) != 4 || len(cols[0]) != 100000 {
				b.Fatalf("Expected 4 columns with 100000 rows each")
			}

			result.Close()
		}
	})

	// Benchmark parallel float64 columns extraction
	b.Run("Float64Parallel", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Query multiple columns
			result, err := conn.QueryDirectResult("SELECT num1, num2, num3, num4 FROM bench_parallel")
			if err != nil {
				b.Fatalf("Failed to query data: %v", err)
			}

			// Extract columns in parallel
			cols, _, err := result.ExtractFloat64ColumnsParallel([]int{0, 1, 2, 3})
			if err != nil {
				b.Fatalf("Failed to extract columns in parallel: %v", err)
			}

			// Verify we got the right amount of data
			if len(cols) != 4 || len(cols[0]) != 100000 {
				b.Fatalf("Expected 4 columns with 100000 rows each")
			}

			result.Close()
		}
	})

	// Benchmark sequential single-column extraction for reference
	b.Run("SingleColumnExtraction", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Query a single column
			result, err := conn.QueryDirectResult("SELECT id1 FROM bench_parallel")
			if err != nil {
				b.Fatalf("Failed to query data: %v", err)
			}

			// Extract single column
			col, _, err := result.ExtractInt32Column(0)
			if err != nil {
				b.Fatalf("Failed to extract column: %v", err)
			}

			// Verify we got the right amount of data
			if len(col) != 100000 {
				b.Fatalf("Expected 100000 rows")
			}

			result.Close()
		}
	})

	// Benchmark analytics-style workload with many columns
	b.Run("AnalyticsWorkload", func(b *testing.B) {
		// Create a test function that simulates an analytics workload
		// using both sequential and parallel approaches

		// Sequential approach with batch extraction
		b.Run("Sequential", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Query all columns
				result, err := conn.QueryDirectResult("SELECT * FROM bench_parallel")
				if err != nil {
					b.Fatalf("Failed to query data: %v", err)
				}

				// Extract int32 columns
				int32Cols, _, err := result.ExtractInt32ColumnsBatch([]int{0, 1, 2, 3})
				if err != nil {
					b.Fatalf("Failed to extract int32 columns: %v", err)
				}

				// Extract int64 columns
				int64Cols, _, err := result.ExtractInt64ColumnsBatch([]int{4, 5, 6})
				if err != nil {
					b.Fatalf("Failed to extract int64 columns: %v", err)
				}

				// Extract float64 columns
				float64Cols, _, err := result.ExtractFloat64ColumnsBatch([]int{7, 8, 9, 10})
				if err != nil {
					b.Fatalf("Failed to extract float64 columns: %v", err)
				}

				// Do some simple calculations with the data to ensure it's used
				var sum1, sum2, sum3 float64
				for i := 0; i < 100000; i++ {
					// Calculate some summary statistics
					sum1 += float64(int32Cols[0][i] + int32Cols[1][i] + int32Cols[2][i] + int32Cols[3][i])
					sum2 += float64(int64Cols[0][i] + int64Cols[1][i] + int64Cols[2][i])
					sum3 += float64Cols[0][i] + float64Cols[1][i] + float64Cols[2][i] + float64Cols[3][i]
				}

				// Ensure the calculations are used (prevent compiler optimizations)
				if sum1 == 0 && sum2 == 0 && sum3 == 0 {
					b.Fatalf("Unexpected zero sums")
				}

				result.Close()
			}
		})

		// Parallel approach
		b.Run("Parallel", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Query all columns
				result, err := conn.QueryDirectResult("SELECT * FROM bench_parallel")
				if err != nil {
					b.Fatalf("Failed to query data: %v", err)
				}

				// Extract all columns in parallel using goroutines
				type resultSet struct {
					int32Cols   [][]int32
					int64Cols   [][]int64
					float64Cols [][]float64
					err         error
				}

				// Channel to collect results from goroutines
				resultChan := make(chan resultSet, 3)

				// Extract int32 columns in parallel
				go func() {
					cols, _, err := result.ExtractInt32ColumnsParallel([]int{0, 1, 2, 3})
					resultChan <- resultSet{int32Cols: cols, err: err}
				}()

				// Extract int64 columns in parallel
				go func() {
					cols, _, err := result.ExtractInt64ColumnsParallel([]int{4, 5, 6})
					resultChan <- resultSet{int64Cols: cols, err: err}
				}()

				// Extract float64 columns in parallel
				go func() {
					cols, _, err := result.ExtractFloat64ColumnsParallel([]int{7, 8, 9, 10})
					resultChan <- resultSet{float64Cols: cols, err: err}
				}()

				// Collect results
				var int32Cols [][]int32
				var int64Cols [][]int64
				var float64Cols [][]float64

				for i := 0; i < 3; i++ {
					res := <-resultChan
					if res.err != nil {
						b.Fatalf("Failed to extract columns: %v", res.err)
					}

					if res.int32Cols != nil {
						int32Cols = res.int32Cols
					}
					if res.int64Cols != nil {
						int64Cols = res.int64Cols
					}
					if res.float64Cols != nil {
						float64Cols = res.float64Cols
					}
				}

				// Verify we have all data
				if len(int32Cols) != 4 || len(int64Cols) != 3 || len(float64Cols) != 4 {
					b.Fatalf("Missing some columns in the results")
				}

				// Do some simple calculations with the data to ensure it's used
				var sum1, sum2, sum3 float64
				for i := 0; i < 100000; i++ {
					// Calculate some summary statistics
					sum1 += float64(int32Cols[0][i] + int32Cols[1][i] + int32Cols[2][i] + int32Cols[3][i])
					sum2 += float64(int64Cols[0][i] + int64Cols[1][i] + int64Cols[2][i])
					sum3 += float64Cols[0][i] + float64Cols[1][i] + float64Cols[2][i] + float64Cols[3][i]
				}

				// Ensure the calculations are used (prevent compiler optimizations)
				if sum1 == 0 && sum2 == 0 && sum3 == 0 {
					b.Fatalf("Unexpected zero sums")
				}

				result.Close()
			}
		})
	})
}
