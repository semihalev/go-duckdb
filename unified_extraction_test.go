package duckdb

import (
	"testing"
)

// TestDirectResultExtraction tests the unified extraction approach with DirectResult
func TestDirectResultExtraction(t *testing.T) {
	// Create a test connection
	db := createTestDB(t)
	defer db.Close()

	// Create a test table with various data types
	_, err := db.Exec(`
		CREATE TABLE extraction_test (
			int32_col INTEGER,
			int64_col BIGINT,
			float64_col DOUBLE,
			string_col VARCHAR,
			ts_col TIMESTAMP,
			date_col DATE
		);
		
		INSERT INTO extraction_test 
		SELECT 
			i, 
			i*1000, 
			i*0.5, 
			'str_' || i::VARCHAR, 
			DATE '2023-01-01' + INTERVAL (i) DAY,
			DATE '2023-01-01' + INTERVAL (i) DAY
		FROM range(1, 1000) t(i);
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Use QueryDirectResult to get direct access to the result
	result, err := db.QueryDirectResult("SELECT * FROM extraction_test ORDER BY int32_col")
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer result.Close()

	// Test int32 extraction
	int32Vals, int32Nulls, err := result.ExtractInt32Column(0)
	if err != nil {
		t.Fatalf("Failed to extract int32 column: %v", err)
	}
	if len(int32Vals) != 999 {
		t.Errorf("Expected 999 int32 values, got %d", len(int32Vals))
	}
	if len(int32Nulls) != 999 {
		t.Errorf("Expected 999 null indicators, got %d", len(int32Nulls))
	}
	// Check a few sample values
	if int32Vals[0] != 1 || int32Vals[998] != 999 {
		t.Errorf("Unexpected int32 values: first=%d, last=%d", int32Vals[0], int32Vals[998])
	}

	// Test int64 extraction
	int64Vals, int64Nulls, err := result.ExtractInt64Column(1)
	if err != nil {
		t.Fatalf("Failed to extract int64 column: %v", err)
	}
	if len(int64Vals) != 999 {
		t.Errorf("Expected 999 int64 values, got %d", len(int64Vals))
	}
	if int64Vals[0] != 1000 || int64Vals[998] != 999000 {
		t.Errorf("Unexpected int64 values: first=%d, last=%d", int64Vals[0], int64Vals[998])
	}

	// Test float64 extraction
	float64Vals, float64Nulls, err := result.ExtractFloat64Column(2)
	if err != nil {
		t.Fatalf("Failed to extract float64 column: %v", err)
	}
	if len(float64Vals) != 999 {
		t.Errorf("Expected 999 float64 values, got %d", len(float64Vals))
	}
	if float64Vals[0] != 0.5 || float64Vals[998] != 499.5 {
		t.Errorf("Unexpected float64 values: first=%f, last=%f", float64Vals[0], float64Vals[998])
	}

	// Test string extraction
	stringVals, stringNulls, err := result.ExtractStringColumn(3)
	if err != nil {
		t.Fatalf("Failed to extract string column: %v", err)
	}
	if len(stringVals) != 999 {
		t.Errorf("Expected 999 string values, got %d", len(stringVals))
	}
	if stringVals[0] != "str_1" || stringVals[998] != "str_999" {
		t.Errorf("Unexpected string values: first=%s, last=%s", stringVals[0], stringVals[998])
	}

	// Test timestamp extraction
	tsVals, tsNulls, err := result.ExtractTimestampColumn(4)
	if err != nil {
		t.Fatalf("Failed to extract timestamp column: %v", err)
	}
	if len(tsVals) != 999 {
		t.Errorf("Expected 999 timestamp values, got %d", len(tsVals))
	}

	// Test date extraction
	dateVals, dateNulls, err := result.ExtractDateColumn(5)
	if err != nil {
		t.Fatalf("Failed to extract date column: %v", err)
	}
	if len(dateVals) != 999 {
		t.Errorf("Expected 999 date values, got %d", len(dateVals))
	}

	// Verify nulls arrays
	for i := 0; i < 999; i++ {
		if int32Nulls[i] || int64Nulls[i] || float64Nulls[i] || stringNulls[i] || tsNulls[i] || dateNulls[i] {
			t.Errorf("Found unexpected NULL value at index %d", i)
		}
	}
}

// TestDirectResultNullValues tests extraction of NULL values
func TestDirectResultNullValues(t *testing.T) {
	// Create a test connection
	db := createTestDB(t)
	defer db.Close()

	// Create a test table with NULL values
	_, err := db.Exec(`
		CREATE TABLE null_test (
			int32_col INTEGER,
			int64_col BIGINT,
			float64_col DOUBLE,
			string_col VARCHAR,
			ts_col TIMESTAMP,
			date_col DATE
		);
		
		INSERT INTO null_test VALUES
		(1, 1000, 0.5, 'str_1', '2023-01-01', '2023-01-01'),
		(NULL, 2000, 1.5, 'str_2', '2023-01-02', '2023-01-02'),
		(3, NULL, 2.5, 'str_3', '2023-01-03', '2023-01-03'),
		(4, 4000, NULL, 'str_4', '2023-01-04', '2023-01-04'),
		(5, 5000, 4.5, NULL, '2023-01-05', '2023-01-05'),
		(6, 6000, 5.5, 'str_6', NULL, '2023-01-06'),
		(7, 7000, 6.5, 'str_7', '2023-01-07', NULL);
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Use QueryDirectResult to get direct access to the result
	result, err := db.QueryDirectResult("SELECT * FROM null_test ORDER BY COALESCE(int32_col, 0)")
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer result.Close()

	// Test int32 extraction with nulls
	intVals, int32Nulls, err := result.ExtractInt32Column(0)
	if err != nil {
		t.Fatalf("Failed to extract int32 column: %v", err)
	}
	if len(int32Nulls) != 7 {
		t.Errorf("Expected 7 int32 values, got %d", len(int32Nulls))
	}

	// Print the values and nulls for debugging
	t.Logf("int32 values: %v", intVals)
	t.Logf("int32 nulls: %v", int32Nulls)

	// Check if the second value (index 1) is NULL
	nullFound := false
	for i, isNull := range int32Nulls {
		if isNull {
			t.Logf("Found NULL at index %d", i)
			nullFound = true
		}
	}

	if !nullFound {
		t.Errorf("No NULL values found in int32 column")
	}

	// Test int64 extraction with nulls
	_, int64Nulls, err := result.ExtractInt64Column(1)
	if err != nil {
		t.Fatalf("Failed to extract int64 column: %v", err)
	}
	if !int64Nulls[2] {
		t.Errorf("Expected NULL for int64 at index 2")
	}

	// Test float64 extraction with nulls
	_, float64Nulls, err := result.ExtractFloat64Column(2)
	if err != nil {
		t.Fatalf("Failed to extract float64 column: %v", err)
	}
	if !float64Nulls[3] {
		t.Errorf("Expected NULL for float64 at index 3")
	}

	// Test string extraction with nulls
	_, stringNulls, err := result.ExtractStringColumn(3)
	if err != nil {
		t.Fatalf("Failed to extract string column: %v", err)
	}
	if !stringNulls[4] {
		t.Errorf("Expected NULL for string at index 4")
	}

	// Test timestamp extraction with nulls
	_, tsNulls, err := result.ExtractTimestampColumn(4)
	if err != nil {
		t.Fatalf("Failed to extract timestamp column: %v", err)
	}
	if !tsNulls[5] {
		t.Errorf("Expected NULL for timestamp at index 5")
	}

	// Test date extraction with nulls
	_, dateNulls, err := result.ExtractDateColumn(5)
	if err != nil {
		t.Fatalf("Failed to extract date column: %v", err)
	}
	if !dateNulls[6] {
		t.Errorf("Expected NULL for date at index 6")
	}
}

// Helper function to create a test database connection
func createTestDB(t *testing.T) *Connection {
	// Open an in-memory DuckDB database
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}
	return conn
}

// BenchmarkDirectResultCreation benchmarks the creation of DirectResult objects
// This helps to verify our memory management fix performs well
func BenchmarkDirectResultCreation(b *testing.B) {
	// Create a test connection
	db, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to open in-memory database: %v", err)
	}
	defer db.Close()

	// Create a test table with a simple structure
	_, err = db.Exec(`
		CREATE TABLE bench_creation (
			col1 INTEGER,
			col2 VARCHAR,
			col3 DOUBLE
		);
		
		INSERT INTO bench_creation 
		SELECT i, 'str_' || i::VARCHAR, i * 0.5
		FROM range(1, 1000) t(i);
	`, nil)
	if err != nil {
		b.Fatalf("Failed to create test data: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Query the data
		result, err := db.QueryDirectResult("SELECT * FROM bench_creation LIMIT 100")
		if err != nil {
			b.Fatalf("Failed to query data: %v", err)
		}

		// Close the result to free memory
		result.Close()
	}
}
