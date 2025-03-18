package duckdb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"
)

func TestBatchParamBinding(t *testing.T) {
	// Create a direct connection
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create table for testing
	_, err = conn.Query("CREATE TABLE batch_test (id INTEGER, name VARCHAR, value DOUBLE, created TIMESTAMP)", nil)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create a batch of parameter sets
	now := time.Now()
	paramSets := [][]interface{}{
		{1, "Item 1", 10.5, now},
		{2, "Item 2", 20.5, now.Add(time.Hour)},
		{3, "Item 3", 30.5, now.Add(2 * time.Hour)},
		{4, "Item 4", 40.5, now.Add(3 * time.Hour)},
		{5, "Item 5", 50.5, now.Add(4 * time.Hour)},
	}

	// Execute batch
	result, err := conn.BatchExec("INSERT INTO batch_test VALUES (?, ?, ?, ?)", paramSets)
	if err != nil {
		t.Fatalf("failed to execute batch: %v", err)
	}

	// Check affected rows
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	}
	if rowsAffected != 5 {
		t.Errorf("expected 5 rows affected, got %d", rowsAffected)
	}

	// Query to verify data
	_, err = conn.Query("SELECT COUNT(*) FROM batch_test", nil)
	if err != nil {
		t.Fatalf("failed to query data: %v", err)
	}

	// Query to get count
	countRows, err := conn.Query("SELECT COUNT(*) FROM batch_test", nil)
	if err != nil {
		t.Fatalf("failed to query count: %v", err)
	}

	countValues := make([]driver.Value, 1)
	err = countRows.Next(countValues)
	if err != nil {
		t.Fatalf("failed to get count: %v", err)
	}
	countRows.Close()

	var count int
	switch v := countValues[0].(type) {
	case int32:
		count = int(v)
	case int64:
		count = int(v)
	default:
		t.Fatalf("unexpected count type: %T", countValues[0])
	}
	if count != 5 {
		t.Errorf("expected 5 rows, got %d", count)
	}
}

func TestBatchParamBindingWithContext(t *testing.T) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create table for testing with context
	_, err = conn.Query("CREATE TABLE batch_ctx_test (id INTEGER, name VARCHAR, value DOUBLE)", nil)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create a batch with context
	ctx := context.Background()
	paramSets := [][]interface{}{
		{10, "Context 1", 100.5},
		{20, "Context 2", 200.5},
		{30, "Context 3", 300.5},
	}

	// Prepare direct statement
	stmt, err := NewOptimizedBatchStmt(conn, "INSERT INTO batch_ctx_test VALUES (?, ?, ?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Execute with context
	result, err := stmt.ExecBatchContext(ctx, paramSets)
	if err != nil {
		t.Fatalf("failed to execute batch with context: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected != 3 {
		t.Errorf("expected 3 rows affected, got %d", rowsAffected)
	}

	// Verify count
	countRows, err := conn.Query("SELECT COUNT(*) FROM batch_ctx_test", nil)
	if err != nil {
		t.Fatalf("failed to query count: %v", err)
	}

	countValues := make([]driver.Value, 1)
	err = countRows.Next(countValues)
	if err != nil {
		t.Fatalf("failed to get count: %v", err)
	}
	countRows.Close()

	var count int
	switch v := countValues[0].(type) {
	case int32:
		count = int(v)
	case int64:
		count = int(v)
	default:
		t.Fatalf("unexpected count type: %T", countValues[0])
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

func TestDifferentParameterTypesBatch(t *testing.T) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create a comprehensive test table with many types
	_, err = conn.ExecContext(context.Background(), `
	CREATE TABLE type_test (
		col_bool BOOLEAN,
		col_int8 TINYINT,
		col_int16 SMALLINT,
		col_int32 INTEGER,
		col_int64 BIGINT,
		col_uint8 UTINYINT,
		col_uint16 USMALLINT,
		col_uint32 UINTEGER,
		col_uint64 UBIGINT,
		col_float FLOAT,
		col_double DOUBLE,
		col_string VARCHAR,
		col_blob BLOB,
		col_timestamp TIMESTAMP
	)`, nil)
	if err != nil {
		t.Fatalf("failed to create type test table: %v", err)
	}

	// Create test data for all types
	now := time.Now()
	binaryData := []byte{0x01, 0x02, 0x03, 0x04}

	paramSets := [][]interface{}{
		{
			true, int8(1), int16(2), int32(3), int64(4),
			uint8(5), uint16(6), uint32(7), uint64(8),
			float32(9.5), float64(10.5), "string1", binaryData, now,
		},
		{
			false, int8(-1), int16(-2), int32(-3), int64(-4),
			uint8(50), uint16(60), uint32(70), uint64(80),
			float32(90.5), float64(100.5), "string2", binaryData, now.Add(time.Hour),
		},
	}

	// Insert with BatchExec
	result, err := conn.BatchExec(`
	INSERT INTO type_test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, paramSets)
	if err != nil {
		t.Fatalf("failed to batch insert different types: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected != 2 {
		t.Errorf("expected 2 rows affected, got %d", rowsAffected)
	}

	// Query the data to verify
	rows, err := conn.Query("SELECT * FROM type_test", nil)
	if err != nil {
		t.Fatalf("failed to query type test data: %v", err)
	}
	defer rows.Close()

	// Verify we can retrieve the data
	rowCount := 0
	values := make([]driver.Value, 14) // 14 columns

	for {
		err = rows.Next(values)
		if err != nil {
			break // End of rows
		}
		rowCount++
	}

	if rowCount != 2 {
		t.Errorf("expected 2 rows, got %d", rowCount)
	}
}

func TestBatchParamBindingNulls(t *testing.T) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create table with nullable fields
	_, err = conn.Query(`
	CREATE TABLE null_test (
		id INTEGER,
		name VARCHAR,
		value DOUBLE,
		created TIMESTAMP
	)`, nil)
	if err != nil {
		t.Fatalf("failed to create null test table: %v", err)
	}

	// Create parameter sets with NULLs
	now := time.Now()
	paramSets := [][]interface{}{
		{1, "Item 1", 10.5, now},
		{2, nil, 20.5, now.Add(time.Hour)},         // NULL name
		{3, "Item 3", nil, now.Add(2 * time.Hour)}, // NULL value
		{4, "Item 4", 40.5, nil},                   // NULL timestamp
		{5, nil, nil, nil},                         // All NULLs except id
	}

	// Execute batch
	result, err := conn.BatchExec("INSERT INTO null_test VALUES (?, ?, ?, ?)", paramSets)
	if err != nil {
		t.Fatalf("failed to execute batch with NULLs: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected != 5 {
		t.Errorf("expected 5 rows affected, got %d", rowsAffected)
	}

	// Query to verify NULL handling
	rows, err := conn.Query("SELECT COUNT(*) FROM null_test WHERE name IS NULL", nil)
	if err != nil {
		t.Fatalf("failed to query NULL names: %v", err)
	}

	values := make([]driver.Value, 1)
	_ = rows.Next(values)
	rows.Close()

	var nullNameCount int
	switch v := values[0].(type) {
	case int32:
		nullNameCount = int(v)
	case int64:
		nullNameCount = int(v)
	default:
		t.Fatalf("unexpected count type: %T", values[0])
	}
	if nullNameCount != 2 { // We should have 2 NULL names
		t.Errorf("expected 2 NULL names, got %d", nullNameCount)
	}

	// Query for NULL values
	rows, err = conn.Query("SELECT COUNT(*) FROM null_test WHERE value IS NULL", nil)
	if err != nil {
		t.Fatalf("failed to query NULL values: %v", err)
	}

	_ = rows.Next(values)
	rows.Close()

	var nullValueCount int
	switch v := values[0].(type) {
	case int32:
		nullValueCount = int(v)
	case int64:
		nullValueCount = int(v)
	default:
		t.Fatalf("unexpected count type: %T", values[0])
	}
	if nullValueCount != 2 { // We should have 2 NULL values
		t.Errorf("expected 2 NULL values, got %d", nullValueCount)
	}
}

func TestLargeBatch(t *testing.T) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create a table for large batch testing
	_, err = conn.Query("CREATE TABLE large_batch (id INTEGER, name VARCHAR, amount DOUBLE)", nil)
	if err != nil {
		t.Fatalf("failed to create batch table: %v", err)
	}

	// Create a large parameter batch (10,000 rows)
	batchSize := 10000
	paramSets := make([][]interface{}, batchSize)
	for i := 0; i < batchSize; i++ {
		// Use values that exercise different parameter types
		paramSets[i] = []interface{}{
			int64(i),
			fmt.Sprintf("Item %d with some extra text to make it longer", i),
			float64(i) * 1.5,
		}
	}

	// Measure time for batch insert
	start := time.Now()
	result, err := conn.BatchExec("INSERT INTO large_batch VALUES (?, ?, ?)", paramSets)
	if err != nil {
		t.Fatalf("failed to execute large batch: %v", err)
	}
	elapsed := time.Since(start)

	// Verify all rows were inserted
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected != int64(batchSize) {
		t.Errorf("expected %d rows affected, got %d", batchSize, rowsAffected)
	}

	// Query to verify count
	rows, err := conn.Query("SELECT COUNT(*) FROM large_batch", nil)
	if err != nil {
		t.Fatalf("failed to query batch count: %v", err)
	}

	values := make([]driver.Value, 1)
	_ = rows.Next(values)
	rows.Close()

	var count int
	switch v := values[0].(type) {
	case int32:
		count = int(v)
	case int64:
		count = int(v)
	default:
		t.Fatalf("unexpected count type: %T", values[0])
	}
	if count != batchSize {
		t.Errorf("expected %d rows, got %d", batchSize, count)
	}

	t.Logf("Successfully inserted %d rows in large batch in %s", batchSize, elapsed)
}

// TestSmallBatch tests with a small batch size
func TestSmallBatch(t *testing.T) {
	// Using native implementation (no longer forcing fallback as memory issues are fixed)
	// Store original value to be safe in test environment
	originalNativeLibLoaded := nativeLibLoaded
	defer func() {
		nativeLibLoaded = originalNativeLibLoaded
	}()

	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create a table for batch testing
	_, err = conn.Query("CREATE TABLE small_batch (id INTEGER, name VARCHAR)", nil)
	if err != nil {
		t.Fatalf("failed to create batch table: %v", err)
	}

	// Create a small parameter batch (50 rows)
	batchSize := 50
	paramSets := make([][]interface{}, batchSize)
	for i := 0; i < batchSize; i++ {
		// Use int64 values to avoid type conversion issues
		paramSets[i] = []interface{}{int64(i), fmt.Sprintf("Item %d", i)}
	}

	// Measure time for batch insert
	start := time.Now()
	result, err := conn.BatchExec("INSERT INTO small_batch VALUES (?, ?)", paramSets)
	if err != nil {
		t.Fatalf("failed to execute batch: %v", err)
	}
	elapsed := time.Since(start)

	// Verify all rows were inserted
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected != int64(batchSize) {
		t.Errorf("expected %d rows affected, got %d", batchSize, rowsAffected)
	}

	// Query to verify count
	rows, err := conn.Query("SELECT COUNT(*) FROM small_batch", nil)
	if err != nil {
		t.Fatalf("failed to query batch count: %v", err)
	}

	values := make([]driver.Value, 1)
	_ = rows.Next(values)
	rows.Close()

	var count int
	switch v := values[0].(type) {
	case int32:
		count = int(v)
	case int64:
		count = int(v)
	default:
		t.Fatalf("unexpected count type: %T", values[0])
	}
	if count != batchSize {
		t.Errorf("expected %d rows, got %d", batchSize, count)
	}

	t.Logf("Inserted %d rows in batch in %s", batchSize, elapsed)

	// Compare with individual inserts
	sampleSize := 10
	stmt, err := newStatement(conn, "INSERT INTO small_batch VALUES (?, ?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Insert individually with driver values
	start = time.Now()
	for i := batchSize; i < batchSize+sampleSize; i++ {
		// Use int64 values for driver.Value
		args := []driver.Value{int64(i), fmt.Sprintf("Individual Item %d", i)}
		_, err = stmt.Exec(args)
		if err != nil {
			t.Fatalf("failed to execute individual insert: %v", err)
		}
	}
	individualElapsed := time.Since(start)

	t.Logf("Inserted %d rows individually in %s", sampleSize, individualElapsed)
	t.Logf("Batch insertion is approximately %.2f times faster per row",
		(individualElapsed.Seconds()/float64(sampleSize))/(elapsed.Seconds()/float64(batchSize)))

	// We can't run the benchmark directly from a test, but we log the suggestion
	if testing.Verbose() {
		t.Logf("For detailed benchmarks, run: go test -run=NONE -bench=BenchmarkBatchVsIndividual")
	}
}

// TestChunkedBatchFallback tests the fallback chunking implementation
func TestChunkedBatchFallback(t *testing.T) {
	// Save original nativeLibLoaded value and restore it later
	originalNativeLibLoaded := nativeLibLoaded
	nativeLibLoaded = false // Force fallback implementation
	defer func() {
		nativeLibLoaded = originalNativeLibLoaded
	}()

	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create a table for testing
	_, err = conn.Query("CREATE TABLE chunked_batch (id INTEGER, name VARCHAR)", nil)
	if err != nil {
		t.Fatalf("failed to create batch table: %v", err)
	}

	// Create a batch that would exceed the defaultBatchChunkSize
	// We'll set our chunk size to 50 for testing (it's normally 1000)
	const testChunkSize = 50
	// Create a batch with 3.5 chunks (175 rows)
	batchSize := testChunkSize*3 + testChunkSize/2

	// Use temporary chunking size for test
	prevChunkSize := defaultBatchChunkSize
	defaultBatchChunkSize = testChunkSize
	defer func() {
		defaultBatchChunkSize = prevChunkSize
	}()

	// Create parameter sets
	paramSets := make([][]interface{}, batchSize)
	for i := 0; i < batchSize; i++ {
		// Use int64 values to avoid type conversion issues
		paramSets[i] = []interface{}{int64(i), fmt.Sprintf("Chunked Item %d", i)}
	}

	// Execute batch with chunking
	result, err := conn.BatchExec("INSERT INTO chunked_batch VALUES (?, ?)", paramSets)
	if err != nil {
		t.Fatalf("failed to execute chunked batch: %v", err)
	}

	// Verify rows affected
	rowsAffected, _ := result.RowsAffected()
	if rowsAffected != int64(batchSize) {
		t.Errorf("expected %d rows affected, got %d", batchSize, rowsAffected)
	}

	// Query to verify count
	rows, err := conn.Query("SELECT COUNT(*) FROM chunked_batch", nil)
	if err != nil {
		t.Fatalf("failed to query batch count: %v", err)
	}

	values := make([]driver.Value, 1)
	_ = rows.Next(values)
	rows.Close()

	var count int
	switch v := values[0].(type) {
	case int32:
		count = int(v)
	case int64:
		count = int(v)
	default:
		t.Fatalf("unexpected count type: %T", values[0])
	}
	if count != batchSize {
		t.Errorf("expected %d rows, got %d", batchSize, count)
	}

	t.Logf("Successfully inserted %d rows in %d chunks with fallback implementation",
		batchSize, (batchSize+testChunkSize-1)/testChunkSize)
}

// BenchmarkBatchVsIndividual is a comprehensive benchmark comparing batch vs individual inserts
func BenchmarkBatchVsIndividual(b *testing.B) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create a table for benchmarking
	_, err = conn.Query("CREATE TABLE bench_comparison (id INTEGER, name VARCHAR, value DOUBLE)", nil)
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	// Run benchmarks for different batch sizes
	for _, batchSize := range []int{10, 100, 1000} {
		// Pre-generate string values to avoid allocations during benchmark
		maxNeeded := batchSize * 10 // More than enough for multiple iterations
		stringValues := make([]string, maxNeeded)
		for i := 0; i < maxNeeded; i++ {
			stringValues[i] = fmt.Sprintf("name-%d", i)
		}

		// Benchmark individual inserts
		b.Run(fmt.Sprintf("Individual_%d", batchSize), func(b *testing.B) {
			// Prepare a statement for individual inserts
			stmt, err := newStatement(conn, "INSERT INTO bench_comparison VALUES (?, ?, ?)")
			if err != nil {
				b.Fatalf("failed to prepare statement: %v", err)
			}
			defer stmt.Close()

			// Reset the timer after preparation
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Stop timer during parameter preparation
				b.StopTimer()

				// Prepare all values for this iteration
				args := make([][]driver.Value, batchSize)
				for j := 0; j < batchSize; j++ {
					id := i*batchSize + j
					strIdx := id % maxNeeded
					args[j] = []driver.Value{
						int64(id),
						stringValues[strIdx],
						float64(id) * 1.5,
					}
				}

				// Resume timer for actual database operations
				b.StartTimer()

				// Execute individual inserts
				for j := 0; j < batchSize; j++ {
					_, err = stmt.Exec(args[j])
					if err != nil {
						b.Fatalf("failed to execute insert: %v", err)
					}
				}
			}
		})

		// Clear the table between benchmarks
		_, err = conn.Query("DELETE FROM bench_comparison", nil)
		if err != nil {
			b.Fatalf("failed to clear table: %v", err)
		}

		// Benchmark batch inserts
		b.Run(fmt.Sprintf("Batch_%d", batchSize), func(b *testing.B) {
			// Reset timer before benchmark
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Stop timer during parameter preparation
				b.StopTimer()

				// Create the batch of parameters
				paramSets := make([][]interface{}, batchSize)

				for j := 0; j < batchSize; j++ {
					id := i*batchSize + j
					strIdx := id % maxNeeded
					paramSets[j] = []interface{}{
						int64(id),
						stringValues[strIdx],
						float64(id) * 1.5,
					}
				}

				// Resume timer for actual database operations
				b.StartTimer()

				// Execute the batch
				result, err := conn.BatchExec("INSERT INTO bench_comparison VALUES (?, ?, ?)", paramSets)
				if err != nil {
					b.Fatalf("failed to execute batch: %v", err)
				}

				// Verify rows affected
				affected, err := result.RowsAffected()
				if err != nil {
					b.Fatalf("failed to get rows affected: %v", err)
				}

				if affected != int64(batchSize) {
					b.Fatalf("expected %d rows affected, got %d", batchSize, affected)
				}
			}
		})

		// Clear the table between benchmarks
		_, err = conn.Query("DELETE FROM bench_comparison", nil)
		if err != nil {
			b.Fatalf("failed to clear table: %v", err)
		}
	}
}
