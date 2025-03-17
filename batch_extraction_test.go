package duckdb

import (
	"testing"
)

func TestBatchExtraction(t *testing.T) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create test table with multiple columns of the same type for batch testing
	_, err = conn.ExecDirect(`
        CREATE TABLE batch_test (
            id1 INTEGER,
            id2 INTEGER,
            id3 INTEGER,
            big1 BIGINT,
            big2 BIGINT,
            val1 DOUBLE,
            val2 DOUBLE,
            val3 DOUBLE
        )
    `)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert some test data
	_, err = conn.ExecDirect(`
        INSERT INTO batch_test VALUES 
        (1, 10, 100, 1000, 10000, 1.1, 2.2, 3.3),
        (2, 20, 200, 2000, 20000, 4.4, 5.5, 6.6),
        (3, 30, 300, 3000, 30000, 7.7, 8.8, 9.9),
        (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
    `)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Query data with DirectResult
	result, err := conn.QueryDirectResult("SELECT * FROM batch_test ORDER BY id1 NULLS LAST")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer result.Close()

	// Test batch extraction of int32 columns
	int32Cols, int32Nulls, err := result.ExtractInt32ColumnsBatch([]int{0, 1, 2})
	if err != nil {
		t.Fatalf("Failed to extract int32 columns in batch: %v", err)
	}

	// Verify batch extraction results
	if len(int32Cols) != 3 {
		t.Errorf("Expected 3 int32 columns, got %d", len(int32Cols))
	}

	// Check values in first column
	expectedIds1 := []int32{1, 2, 3, 0}
	for i := 0; i < 3; i++ {
		if int32Cols[0][i] != expectedIds1[i] {
			t.Errorf("Expected id1[%d] to be %d, got %d", i, expectedIds1[i], int32Cols[0][i])
		}
		if int32Nulls[0][i] {
			t.Errorf("Expected id1[%d] to be non-null", i)
		}
	}

	// Check values in second column
	expectedIds2 := []int32{10, 20, 30, 0}
	for i := 0; i < 3; i++ {
		if int32Cols[1][i] != expectedIds2[i] {
			t.Errorf("Expected id2[%d] to be %d, got %d", i, expectedIds2[i], int32Cols[1][i])
		}
		if int32Nulls[1][i] {
			t.Errorf("Expected id2[%d] to be non-null", i)
		}
	}

	// Check values in third column
	expectedIds3 := []int32{100, 200, 300, 0}
	for i := 0; i < 3; i++ {
		if int32Cols[2][i] != expectedIds3[i] {
			t.Errorf("Expected id3[%d] to be %d, got %d", i, expectedIds3[i], int32Cols[2][i])
		}
		if int32Nulls[2][i] {
			t.Errorf("Expected id3[%d] to be non-null", i)
		}
	}

	// Check null handling for all columns
	if !int32Nulls[0][3] || !int32Nulls[1][3] || !int32Nulls[2][3] {
		t.Errorf("Expected all columns to have null in row 3")
	}

	// Test batch extraction of int64 columns
	int64Cols, int64Nulls, err := result.ExtractInt64ColumnsBatch([]int{3, 4})
	if err != nil {
		t.Fatalf("Failed to extract int64 columns in batch: %v", err)
	}

	// Verify batch extraction results
	if len(int64Cols) != 2 {
		t.Errorf("Expected 2 int64 columns, got %d", len(int64Cols))
	}

	// Check values
	expectedBig1 := []int64{1000, 2000, 3000, 0}
	expectedBig2 := []int64{10000, 20000, 30000, 0}

	for i := 0; i < 3; i++ {
		if int64Cols[0][i] != expectedBig1[i] {
			t.Errorf("Expected big1[%d] to be %d, got %d", i, expectedBig1[i], int64Cols[0][i])
		}
		if int64Nulls[0][i] {
			t.Errorf("Expected big1[%d] to be non-null", i)
		}

		if int64Cols[1][i] != expectedBig2[i] {
			t.Errorf("Expected big2[%d] to be %d, got %d", i, expectedBig2[i], int64Cols[1][i])
		}
		if int64Nulls[1][i] {
			t.Errorf("Expected big2[%d] to be non-null", i)
		}
	}

	// Test batch extraction of float64 columns
	floatCols, floatNulls, err := result.ExtractFloat64ColumnsBatch([]int{5, 6, 7})
	if err != nil {
		t.Fatalf("Failed to extract float64 columns in batch: %v", err)
	}

	// Verify batch extraction results
	if len(floatCols) != 3 {
		t.Errorf("Expected 3 float64 columns, got %d", len(floatCols))
	}

	// Expected values
	expectedVals := [][]float64{
		{1.1, 4.4, 7.7, 0.0},
		{2.2, 5.5, 8.8, 0.0},
		{3.3, 6.6, 9.9, 0.0},
	}

	// Check values in all float columns
	for col := 0; col < 3; col++ {
		for i := 0; i < 3; i++ {
			if floatCols[col][i] != expectedVals[col][i] {
				t.Errorf("Expected val%d[%d] to be %f, got %f", col+1, i, expectedVals[col][i], floatCols[col][i])
			}
			if floatNulls[col][i] {
				t.Errorf("Expected val%d[%d] to be non-null", col+1, i)
			}
		}

		// Check null handling
		if !floatNulls[col][3] {
			t.Errorf("Expected val%d[3] to be null", col+1)
		}
	}

	// Test batch extraction with invalid column type should fail
	_, _, err = result.ExtractInt32ColumnsBatch([]int{0, 5}) // column 5 is DOUBLE not INTEGER
	if err == nil {
		t.Errorf("Expected error when mixing column types in batch extraction, but got none")
	}
}

func BenchmarkBatchExtraction(b *testing.B) {
	// Connect to in-memory database
	conn, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to connect to DuckDB: %v", err)
	}
	defer conn.Close()

	// Create table with multiple columns of same type for batch extraction benchmarks
	_, err = conn.ExecDirect(`
		CREATE TABLE bench_batch (
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
		INSERT INTO bench_batch
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

	// Benchmark multiple column extraction (using individual functions)
	b.Run("MultiColumnSeparate", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Query multiple columns
			result, err := conn.QueryDirectResult("SELECT id1, id2, id3, id4 FROM bench_batch")
			if err != nil {
				b.Fatalf("Failed to query data: %v", err)
			}

			// Extract each column individually
			ids1, _, err := result.ExtractInt32Column(0)
			if err != nil {
				b.Fatalf("Failed to extract id1 column: %v", err)
			}

			ids2, _, err := result.ExtractInt32Column(1)
			if err != nil {
				b.Fatalf("Failed to extract id2 column: %v", err)
			}

			ids3, _, err := result.ExtractInt32Column(2)
			if err != nil {
				b.Fatalf("Failed to extract id3 column: %v", err)
			}

			ids4, _, err := result.ExtractInt32Column(3)
			if err != nil {
				b.Fatalf("Failed to extract id4 column: %v", err)
			}

			// Verify we got the right amount of data
			if len(ids1) != 100000 || len(ids2) != 100000 || len(ids3) != 100000 || len(ids4) != 100000 {
				b.Fatalf("Expected 100000 values for each column")
			}

			result.Close()
		}
	})

	// Benchmark batch extraction for int32 columns
	b.Run("BatchInt32Extraction", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Query multiple columns of same type
			result, err := conn.QueryDirectResult("SELECT id1, id2, id3, id4 FROM bench_batch")
			if err != nil {
				b.Fatalf("Failed to query data: %v", err)
			}

			// Extract multiple int32 columns in a single batch operation
			columns, _, err := result.ExtractInt32ColumnsBatch([]int{0, 1, 2, 3})
			if err != nil {
				b.Fatalf("Failed to batch extract int32 columns: %v", err)
			}

			// Verify we got the right amount of data
			if len(columns) != 4 {
				b.Fatalf("Expected 4 columns, got %d", len(columns))
			}

			for i := 0; i < 4; i++ {
				if len(columns[i]) != 100000 {
					b.Fatalf("Expected 100000 values in column %d, got %d", i, len(columns[i]))
				}
			}

			result.Close()
		}
	})

	// Benchmark batch extraction for int64 columns
	b.Run("BatchInt64Extraction", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Query multiple columns of same type
			result, err := conn.QueryDirectResult("SELECT big1, big2, big3 FROM bench_batch")
			if err != nil {
				b.Fatalf("Failed to query data: %v", err)
			}

			// Extract multiple int64 columns in a single batch operation
			columns, _, err := result.ExtractInt64ColumnsBatch([]int{0, 1, 2})
			if err != nil {
				b.Fatalf("Failed to batch extract int64 columns: %v", err)
			}

			// Verify we got the right amount of data
			if len(columns) != 3 {
				b.Fatalf("Expected 3 columns, got %d", len(columns))
			}

			for i := 0; i < 3; i++ {
				if len(columns[i]) != 100000 {
					b.Fatalf("Expected 100000 values in column %d, got %d", i, len(columns[i]))
				}
			}

			result.Close()
		}
	})

	// Benchmark batch extraction for float64 columns
	b.Run("BatchFloat64Extraction", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Query multiple columns of same type
			result, err := conn.QueryDirectResult("SELECT num1, num2, num3, num4 FROM bench_batch")
			if err != nil {
				b.Fatalf("Failed to query data: %v", err)
			}

			// Extract multiple float64 columns in a single batch operation
			columns, _, err := result.ExtractFloat64ColumnsBatch([]int{0, 1, 2, 3})
			if err != nil {
				b.Fatalf("Failed to batch extract float64 columns: %v", err)
			}

			// Verify we got the right amount of data
			if len(columns) != 4 {
				b.Fatalf("Expected 4 columns, got %d", len(columns))
			}

			for i := 0; i < 4; i++ {
				if len(columns[i]) != 100000 {
					b.Fatalf("Expected 100000 values in column %d, got %d", i, len(columns[i]))
				}
			}

			result.Close()
		}
	})

	// Benchmark comparison of direct extraction vs. SQL aggregation
	b.Run("ExtractionVsAggregation", func(b *testing.B) {
		// Define a simple aggregation function (sum)
		calcSumCol := func(vals []float64, nulls []bool) float64 {
			var sum float64
			for i, v := range vals {
				if !nulls[i] {
					sum += v
				}
			}
			return sum
		}

		// SQL aggregation function
		sqlAggregation := func(conn *Connection) float64 {
			result, err := conn.QueryDirectResult("SELECT SUM(num1) FROM bench_batch")
			if err != nil {
				b.Fatalf("Failed to calculate sum: %v", err)
				return 0
			}
			defer result.Close()

			sums, nulls, err := result.ExtractFloat64Column(0)
			if err != nil {
				b.Fatalf("Failed to extract sum: %v", err)
				return 0
			}

			if len(sums) > 0 && !nulls[0] {
				return sums[0]
			}
			return 0
		}

		// Benchmark data retrieval + Go aggregation
		b.Run("ColumnExtraction", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				result, err := conn.QueryDirectResult("SELECT num1 FROM bench_batch")
				if err != nil {
					b.Fatalf("Failed to query data: %v", err)
				}

				vals, nulls, err := result.ExtractFloat64Column(0)
				if err != nil {
					b.Fatalf("Failed to extract column: %v", err)
				}

				sum := calcSumCol(vals, nulls)
				if sum == 0 {
					b.Fatalf("Sum should not be zero")
				}

				result.Close()
			}
		})

		// Benchmark SQL aggregation
		b.Run("SQLAggregation", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				sum := sqlAggregation(conn)
				if sum == 0 {
					b.Fatalf("Sum should not be zero")
				}
			}
		})
	})

	// Compare batch operation to individual extractions for complex analytics
	b.Run("AnalyticsWorkload", func(b *testing.B) {
		// Function to calculate statistics with individual extractions
		calcStatsSeparate := func(result *DirectResult) (float64, float64, float64, float64) {
			ids, _, err := result.ExtractInt32Column(0)
			if err != nil {
				b.Fatalf("Failed to extract ids: %v", err)
			}

			nums1, _, err := result.ExtractFloat64Column(1)
			if err != nil {
				b.Fatalf("Failed to extract nums1: %v", err)
			}

			nums2, _, err := result.ExtractFloat64Column(2)
			if err != nil {
				b.Fatalf("Failed to extract nums2: %v", err)
			}

			nums3, _, err := result.ExtractFloat64Column(3)
			if err != nil {
				b.Fatalf("Failed to extract nums3: %v", err)
			}

			// Calculate sum of each column
			var sum1, sum2, sum3, weightedSum float64
			for i := 0; i < len(ids); i++ {
				sum1 += nums1[i]
				sum2 += nums2[i]
				sum3 += nums3[i]
				weightedSum += (nums1[i] * nums2[i] * nums3[i]) / float64(ids[i]+1)
			}

			return sum1, sum2, sum3, weightedSum
		}

		// Function to calculate statistics with batch extraction
		calcStatsBatch := func(result *DirectResult) (float64, float64, float64, float64) {
			ids, _, err := result.ExtractInt32Column(0)
			if err != nil {
				b.Fatalf("Failed to extract ids: %v", err)
			}

			numCols, _, err := result.ExtractFloat64ColumnsBatch([]int{1, 2, 3})
			if err != nil {
				b.Fatalf("Failed to batch extract num columns: %v", err)
			}

			// Calculate sum of each column
			var sum1, sum2, sum3, weightedSum float64
			for i := 0; i < len(ids); i++ {
				sum1 += numCols[0][i]
				sum2 += numCols[1][i]
				sum3 += numCols[2][i]
				weightedSum += (numCols[0][i] * numCols[1][i] * numCols[2][i]) / float64(ids[i]+1)
			}

			return sum1, sum2, sum3, weightedSum
		}

		// Benchmark analytics with separate column extraction
		b.Run("SeparateExtractions", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				result, err := conn.QueryDirectResult("SELECT id1, num1, num2, num3 FROM bench_batch")
				if err != nil {
					b.Fatalf("Failed to query data: %v", err)
				}

				sum1, sum2, sum3, weighted := calcStatsSeparate(result)
				if sum1 == 0 || sum2 == 0 || sum3 == 0 || weighted == 0 {
					b.Fatalf("Calculated sums should not be zero")
				}

				result.Close()
			}
		})

		// Benchmark analytics with batch extraction
		b.Run("BatchExtractions", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				result, err := conn.QueryDirectResult("SELECT id1, num1, num2, num3 FROM bench_batch")
				if err != nil {
					b.Fatalf("Failed to query data: %v", err)
				}

				sum1, sum2, sum3, weighted := calcStatsBatch(result)
				if sum1 == 0 || sum2 == 0 || sum3 == 0 || weighted == 0 {
					b.Fatalf("Calculated sums should not be zero")
				}

				result.Close()
			}
		})
	})
}
