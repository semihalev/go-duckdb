package duckdb

import (
	"math"
	"testing"
)

func calculateInt32Sum(values []int32, nulls []bool) (int64, error) {
	var sum int64
	for i, val := range values {
		if !nulls[i] {
			sum += int64(val)
		}
	}
	return sum, nil
}

func calculateInt64Sum(values []int64, nulls []bool) (int64, error) {
	var sum int64
	for i, val := range values {
		if !nulls[i] {
			sum += val
		}
	}
	return sum, nil
}

func calculateFloat64Sum(values []float64, nulls []bool) (float64, error) {
	var sum float64
	for i, val := range values {
		if !nulls[i] {
			sum += val
		}
	}
	return sum, nil
}

func TestAggregations(t *testing.T) {
	// Create test data
	valuesInt32 := []int32{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	valuesInt64 := []int64{1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000}
	valuesFloat64 := []float64{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9, 10.0}

	// Create null masks (all values non-null)
	nullsInt32 := make([]bool, len(valuesInt32))
	nullsInt64 := make([]bool, len(valuesInt64))
	nullsFloat64 := make([]bool, len(valuesFloat64))

	// Test SUM with Int32 values
	t.Run("Int32Sum", func(t *testing.T) {
		// Expected sum = 10+20+30+40+50+60+70+80+90+100 = 550
		expectedSum := int64(550)

		sum, err := calculateInt32Sum(valuesInt32, nullsInt32)
		if err != nil {
			t.Fatalf("Int32Sum failed: %v", err)
		}

		if sum != expectedSum {
			t.Errorf("Expected sum %d, got %d", expectedSum, sum)
		}

		// Test with some NULL values
		nullsWithNulls := make([]bool, len(nullsInt32))
		// Mark some values as NULL (values at index 1, 3, 5, 7, 9)
		nullsWithNulls[1] = true // 20
		nullsWithNulls[3] = true // 40
		nullsWithNulls[5] = true // 60
		nullsWithNulls[7] = true // 80
		nullsWithNulls[9] = true // 100

		// Expected sum = 10+30+50+70+90 = 250
		expectedSumWithNulls := int64(250)

		sumWithNulls, err := calculateInt32Sum(valuesInt32, nullsWithNulls)
		if err != nil {
			t.Fatalf("Int32Sum with nulls failed: %v", err)
		}

		if sumWithNulls != expectedSumWithNulls {
			t.Errorf("Expected sum with nulls %d, got %d", expectedSumWithNulls, sumWithNulls)
		}
	})

	// Test SUM with Int64 values
	t.Run("Int64Sum", func(t *testing.T) {
		// Expected sum = 1000+2000+3000+4000+5000+6000+7000+8000+9000+10000 = 55000
		expectedSum := int64(55000)

		sum, err := calculateInt64Sum(valuesInt64, nullsInt64)
		if err != nil {
			t.Fatalf("Int64Sum failed: %v", err)
		}

		if sum != expectedSum {
			t.Errorf("Expected sum %d, got %d", expectedSum, sum)
		}

		// Test with some NULL values
		nullsWithNulls := make([]bool, len(nullsInt64))
		// Mark some values as NULL (values at index 1, 3, 5, 7, 9)
		nullsWithNulls[1] = true // 2000
		nullsWithNulls[3] = true // 4000
		nullsWithNulls[5] = true // 6000
		nullsWithNulls[7] = true // 8000
		nullsWithNulls[9] = true // 10000

		// Expected sum = 1000+3000+5000+7000+9000 = 25000
		expectedSumWithNulls := int64(25000)

		sumWithNulls, err := calculateInt64Sum(valuesInt64, nullsWithNulls)
		if err != nil {
			t.Fatalf("Int64Sum with nulls failed: %v", err)
		}

		if sumWithNulls != expectedSumWithNulls {
			t.Errorf("Expected sum with nulls %d, got %d", expectedSumWithNulls, sumWithNulls)
		}
	})

	// Test SUM with Float64 values
	t.Run("Float64Sum", func(t *testing.T) {
		// Expected sum = 1.1+2.2+3.3+4.4+5.5+6.6+7.7+8.8+9.9+10.0 = 59.5
		expectedSum := 59.5

		sum, err := calculateFloat64Sum(valuesFloat64, nullsFloat64)
		if err != nil {
			t.Fatalf("Float64Sum failed: %v", err)
		}

		// Use epsilon comparison for floating point
		if math.Abs(sum-expectedSum) > 1e-10 {
			t.Errorf("Expected sum %f, got %f", expectedSum, sum)
		}

		// Test with some NULL values
		nullsWithNulls := make([]bool, len(nullsFloat64))
		// Mark some values as NULL (values at index 1, 3, 5, 7, 9)
		nullsWithNulls[1] = true // 2.2
		nullsWithNulls[3] = true // 4.4
		nullsWithNulls[5] = true // 6.6
		nullsWithNulls[7] = true // 8.8
		nullsWithNulls[9] = true // 10.0

		// Expected sum = 1.1+3.3+5.5+7.7+9.9 = 27.5
		expectedSumWithNulls := 27.5

		sumWithNulls, err := calculateFloat64Sum(valuesFloat64, nullsWithNulls)
		if err != nil {
			t.Fatalf("Float64Sum with nulls failed: %v", err)
		}

		// Use epsilon comparison for floating point
		if math.Abs(sumWithNulls-expectedSumWithNulls) > 1e-10 {
			t.Errorf("Expected sum with nulls %f, got %f", expectedSumWithNulls, sumWithNulls)
		}
	})
}

func TestDirectResultAggregations(t *testing.T) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create test table with for aggregation testing
	_, err = conn.ExecDirect(`
        CREATE TABLE agg_test (
            id INTEGER,
            val_int INTEGER,
            val_big BIGINT,
            val_float DOUBLE
        )
    `)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	_, err = conn.ExecDirect(`
        INSERT INTO agg_test VALUES 
        (1, 10, 1000, 1.1),
        (2, 20, 2000, 2.2),
        (3, 30, 3000, 3.3),
        (4, 40, 4000, 4.4),
        (5, 50, 5000, 5.5),
        (6, 60, 6000, 6.6),
        (7, 70, 7000, 7.7),
        (8, 80, 8000, 8.8),
        (9, 90, 9000, 9.9),
        (10, 100, 10000, 10.0),
        (11, NULL, NULL, NULL)
    `)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Query data with DirectResult
	result, err := conn.QueryDirectResult("SELECT * FROM agg_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer result.Close()

	// Test SumInt32Column
	t.Run("SumInt32Column", func(t *testing.T) {
		// val_int sum should be 10+20+30+40+50+60+70+80+90+100 = 550
		expectedSum := int64(550)

		sum, err := result.SumInt32Column(1) // val_int is at index 1
		if err != nil {
			t.Fatalf("SumInt32Column failed: %v", err)
		}

		if sum != expectedSum {
			t.Errorf("Expected sum %d, got %d", expectedSum, sum)
		}
	})

	// Test SumInt64Column
	t.Run("SumInt64Column", func(t *testing.T) {
		// val_big sum should be 1000+2000+3000+4000+5000+6000+7000+8000+9000+10000 = 55000
		expectedSum := int64(55000)

		sum, err := result.SumInt64Column(2) // val_big is at index 2
		if err != nil {
			t.Fatalf("SumInt64Column failed: %v", err)
		}

		if sum != expectedSum {
			t.Errorf("Expected sum %d, got %d", expectedSum, sum)
		}
	})

	// Test SumFloat64Column
	t.Run("SumFloat64Column", func(t *testing.T) {
		// val_float sum should be 1.1+2.2+3.3+4.4+5.5+6.6+7.7+8.8+9.9+10.0 = 59.5
		expectedSum := 59.5

		sum, err := result.SumFloat64Column(3) // val_float is at index 3
		if err != nil {
			t.Fatalf("SumFloat64Column failed: %v", err)
		}

		// Use epsilon comparison for floating point
		if math.Abs(sum-expectedSum) > 1e-10 {
			t.Errorf("Expected sum %f, got %f", expectedSum, sum)
		}
	})
}

func TestParallelAggregations(t *testing.T) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create test table with multiple columns of the same type
	_, err = conn.ExecDirect(`
        CREATE TABLE parallel_agg_test (
            id INTEGER,
            val_int1 INTEGER,
            val_int2 INTEGER,
            val_int3 INTEGER,
            val_big1 BIGINT,
            val_big2 BIGINT,
            val_float1 DOUBLE,
            val_float2 DOUBLE,
            val_float3 DOUBLE
        )
    `)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	_, err = conn.ExecDirect(`
        INSERT INTO parallel_agg_test
        SELECT 
            i AS id,
            i * 1 AS val_int1,
            i * 2 AS val_int2,
            i * 3 AS val_int3,
            i * 1000 AS val_big1,
            i * 2000 AS val_big2,
            i * 1.1 AS val_float1,
            i * 2.2 AS val_float2,
            i * 3.3 AS val_float3
        FROM range(1, 101) t(i)
    `)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Query data with DirectResult
	result, err := conn.QueryDirectResult("SELECT * FROM parallel_agg_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer result.Close()

	// Create parallel extractor
	pe := NewParallelExtractor(result)

	// Test SumInt32Columns (multiple columns in parallel)
	t.Run("SumInt32Columns", func(t *testing.T) {
		// Sum multiple int32 columns
		colIndices := []int{1, 2, 3} // val_int1, val_int2, val_int3

		sums, err := pe.SumInt32Columns(colIndices)
		if err != nil {
			t.Fatalf("SumInt32Columns failed: %v", err)
		}

		// Check results
		// For 1..100:
		// sum(i*1) = 5050
		// sum(i*2) = 10100
		// sum(i*3) = 15150
		expectedSums := map[int]int64{
			1: 5050,
			2: 10100,
			3: 15150,
		}

		for colIdx, expectedSum := range expectedSums {
			sum, ok := sums[colIdx]
			if !ok {
				t.Errorf("Missing result for column %d", colIdx)
				continue
			}

			if sum != expectedSum {
				t.Errorf("Column %d: expected sum %d, got %d", colIdx, expectedSum, sum)
			}
		}
	})

	// Test SumInt64Columns
	t.Run("SumInt64Columns", func(t *testing.T) {
		colIndices := []int{4, 5} // val_big1, val_big2

		sums, err := pe.SumInt64Columns(colIndices)
		if err != nil {
			t.Fatalf("SumInt64Columns failed: %v", err)
		}

		// Check results
		// For 1..100:
		// sum(i*1000) = 5050000
		// sum(i*2000) = 10100000
		expectedSums := map[int]int64{
			4: 5050000,
			5: 10100000,
		}

		for colIdx, expectedSum := range expectedSums {
			sum, ok := sums[colIdx]
			if !ok {
				t.Errorf("Missing result for column %d", colIdx)
				continue
			}

			if sum != expectedSum {
				t.Errorf("Column %d: expected sum %d, got %d", colIdx, expectedSum, sum)
			}
		}
	})

	// Test SumFloat64Columns
	t.Run("SumFloat64Columns", func(t *testing.T) {
		colIndices := []int{6, 7, 8} // val_float1, val_float2, val_float3

		sums, err := pe.SumFloat64Columns(colIndices)
		if err != nil {
			t.Fatalf("SumFloat64Columns failed: %v", err)
		}

		// Check results
		// For 1..100:
		// sum(i*1.1) ≈ 5555.0
		// sum(i*2.2) ≈ 11110.0
		// sum(i*3.3) ≈ 16665.0
		expectedSums := map[int]float64{
			6: 5050 * 1.1,
			7: 5050 * 2.2,
			8: 5050 * 3.3,
		}

		for colIdx, expectedSum := range expectedSums {
			sum, ok := sums[colIdx]
			if !ok {
				t.Errorf("Missing result for column %d", colIdx)
				continue
			}

			// Use relative epsilon for larger floating point numbers
			relError := math.Abs((sum - expectedSum) / expectedSum)
			if relError > 1e-10 {
				t.Errorf("Column %d: expected sum %f, got %f (relative error: %e)",
					colIdx, expectedSum, sum, relError)
			}
		}
	})
}
