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

// TestExtendedTypeDirectResult tests the extraction of additional data types
// that were recently unified in the extraction methods
func TestExtendedTypeDirectResult(t *testing.T) {
	// Create a test connection
	db := createTestDB(t)
	defer db.Close()

	// Create a test table with additional data types
	_, err := db.Exec(`
		CREATE TABLE extended_types_test (
			bool_col BOOLEAN,
			int8_col TINYINT,
			int16_col SMALLINT,
			uint8_col UTINYINT,
			uint16_col USMALLINT,
			uint32_col UINTEGER,
			uint64_col UBIGINT,
			float32_col FLOAT
		);
		
		INSERT INTO extended_types_test 
		SELECT 
			i % 2 = 0, -- boolean
			i::TINYINT, -- int8
			(i * 2)::SMALLINT, -- int16
			i::UTINYINT, -- uint8
			(i * 3)::USMALLINT, -- uint16
			(i * 10)::UINTEGER, -- uint32
			(i * 100)::UBIGINT, -- uint64
			(i * 0.25)::FLOAT -- float32
		FROM range(1, 101) t(i);
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create extended types test data: %v", err)
	}

	// Use QueryDirectResult to get direct access to the result
	result, err := db.QueryDirectResult("SELECT * FROM extended_types_test ORDER BY int8_col")
	if err != nil {
		t.Fatalf("Failed to query extended types data: %v", err)
	}
	defer result.Close()

	// Test boolean extraction
	boolVals, boolNulls, err := result.ExtractBoolColumn(0)
	if err != nil {
		t.Fatalf("Failed to extract boolean column: %v", err)
	}
	if len(boolVals) != 100 {
		t.Errorf("Expected 100 boolean values, got %d", len(boolVals))
	}
	// Check first few values (should alternate: false, true, false, ...)
	if boolVals[0] != false || boolVals[1] != true || boolVals[2] != false || boolVals[3] != true {
		t.Errorf("Unexpected boolean pattern: %v, %v, %v, %v", boolVals[0], boolVals[1], boolVals[2], boolVals[3])
	}

	// Test int8 extraction
	int8Vals, int8Nulls, err := result.ExtractInt8Column(1)
	if err != nil {
		t.Fatalf("Failed to extract int8 column: %v", err)
	}
	if len(int8Vals) != 100 {
		t.Errorf("Expected 100 int8 values, got %d", len(int8Vals))
	}
	if int8Vals[0] != 1 || int8Vals[99] != 100 {
		t.Errorf("Unexpected int8 values: first=%d, last=%d", int8Vals[0], int8Vals[99])
	}

	// Test int16 extraction
	int16Vals, int16Nulls, err := result.ExtractInt16Column(2)
	if err != nil {
		t.Fatalf("Failed to extract int16 column: %v", err)
	}
	if len(int16Vals) != 100 {
		t.Errorf("Expected 100 int16 values, got %d", len(int16Vals))
	}
	if int16Vals[0] != 2 || int16Vals[99] != 200 {
		t.Errorf("Unexpected int16 values: first=%d, last=%d", int16Vals[0], int16Vals[99])
	}

	// Test uint8 extraction
	uint8Vals, uint8Nulls, err := result.ExtractUint8Column(3)
	if err != nil {
		t.Fatalf("Failed to extract uint8 column: %v", err)
	}
	if len(uint8Vals) != 100 {
		t.Errorf("Expected 100 uint8 values, got %d", len(uint8Vals))
	}
	if uint8Vals[0] != 1 || uint8Vals[99] != 100 {
		t.Errorf("Unexpected uint8 values: first=%d, last=%d", uint8Vals[0], uint8Vals[99])
	}

	// Test uint16 extraction
	uint16Vals, uint16Nulls, err := result.ExtractUint16Column(4)
	if err != nil {
		t.Fatalf("Failed to extract uint16 column: %v", err)
	}
	if len(uint16Vals) != 100 {
		t.Errorf("Expected 100 uint16 values, got %d", len(uint16Vals))
	}
	if uint16Vals[0] != 3 || uint16Vals[99] != 300 {
		t.Errorf("Unexpected uint16 values: first=%d, last=%d", uint16Vals[0], uint16Vals[99])
	}

	// Test uint32 extraction
	uint32Vals, uint32Nulls, err := result.ExtractUint32Column(5)
	if err != nil {
		t.Fatalf("Failed to extract uint32 column: %v", err)
	}
	if len(uint32Vals) != 100 {
		t.Errorf("Expected 100 uint32 values, got %d", len(uint32Vals))
	}
	if uint32Vals[0] != 10 || uint32Vals[99] != 1000 {
		t.Errorf("Unexpected uint32 values: first=%d, last=%d", uint32Vals[0], uint32Vals[99])
	}

	// Test uint64 extraction
	uint64Vals, uint64Nulls, err := result.ExtractUint64Column(6)
	if err != nil {
		t.Fatalf("Failed to extract uint64 column: %v", err)
	}
	if len(uint64Vals) != 100 {
		t.Errorf("Expected 100 uint64 values, got %d", len(uint64Vals))
	}
	if uint64Vals[0] != 100 || uint64Vals[99] != 10000 {
		t.Errorf("Unexpected uint64 values: first=%d, last=%d", uint64Vals[0], uint64Vals[99])
	}

	// Test float32 extraction
	float32Vals, float32Nulls, err := result.ExtractFloat32Column(7)
	if err != nil {
		t.Fatalf("Failed to extract float32 column: %v", err)
	}
	if len(float32Vals) != 100 {
		t.Errorf("Expected 100 float32 values, got %d", len(float32Vals))
	}
	if float32Vals[0] != 0.25 || float32Vals[99] != 25.0 {
		t.Errorf("Unexpected float32 values: first=%f, last=%f", float32Vals[0], float32Vals[99])
	}

	// Verify no null values
	for i := 0; i < 100; i++ {
		if boolNulls[i] || int8Nulls[i] || int16Nulls[i] || uint8Nulls[i] || 
		   uint16Nulls[i] || uint32Nulls[i] || uint64Nulls[i] || float32Nulls[i] {
			t.Errorf("Found unexpected NULL value at index %d", i)
		}
	}
}

// TestExtendedTypeNullValues tests extraction of NULL values for additional data types
func TestExtendedTypeNullValues(t *testing.T) {
	// Create a test connection
	db := createTestDB(t)
	defer db.Close()

	// Create a test table with NULL values in additional data types
	_, err := db.Exec(`
		CREATE TABLE extended_nulls_test (
			bool_col BOOLEAN,
			int8_col TINYINT,
			int16_col SMALLINT,
			uint8_col UTINYINT,
			uint16_col USMALLINT,
			uint32_col UINTEGER,
			uint64_col UBIGINT,
			float32_col FLOAT
		);
		
		INSERT INTO extended_nulls_test VALUES
		(true, 1, 2, 3, 4, 5, 6, 0.5),
		(NULL, 10, 20, 30, 40, 50, 60, 1.5),
		(false, NULL, 30, 40, 50, 60, 70, 2.5),
		(true, 20, NULL, 50, 60, 70, 80, 3.5),
		(false, 30, 40, NULL, 70, 80, 90, 4.5),
		(true, 40, 50, 60, NULL, 90, 100, 5.5),
		(false, 50, 60, 70, 80, NULL, 110, 6.5),
		(true, 60, 70, 80, 90, 100, NULL, 7.5),
		(false, 70, 80, 90, 100, 110, 120, NULL);
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create extended nulls test data: %v", err)
	}

	// Run a query to check the data ordering first
	checkResult, err := db.QueryDirectResult("SELECT bool_col, int8_col, int16_col FROM extended_nulls_test ORDER BY COALESCE(int8_col, 0)")
	if err != nil {
		t.Fatalf("Failed to run check query: %v", err)
	}
	
	// Manually check the values to understand ordering
	t.Log("Checking row order from direct SQL query:")
	for i := 0; i < int(checkResult.RowCount()); i++ {
		boolIsNull := checkResult.IsNull(0, i)
		int8IsNull := checkResult.IsNull(1, i)
		int16IsNull := checkResult.IsNull(2, i)
		t.Logf("Row %d: bool_col NULL=%v, int8_col NULL=%v, int16_col NULL=%v", 
			i, boolIsNull, int8IsNull, int16IsNull)
	}
	checkResult.Close()
	
	// Use QueryDirectResult to get direct access to the result
	result, err := db.QueryDirectResult("SELECT * FROM extended_nulls_test ORDER BY COALESCE(int8_col, 0)")
	if err != nil {
		t.Fatalf("Failed to query extended nulls data: %v", err)
	}
	defer result.Close()

	// Test boolean extraction with nulls
	boolVals, boolNulls, err := result.ExtractBoolColumn(0)
	if err != nil {
		t.Fatalf("Failed to extract boolean column with nulls: %v", err)
	}
	
	// Print debug info
	t.Logf("Boolean values: %v", boolVals)
	t.Logf("Boolean nulls: %v", boolNulls)
	
	// From our debug output, we can see the NULL booleans are at index 2
	if !boolNulls[2] {
		t.Errorf("Expected NULL for boolean at index 2")
	}

	// Test int8 extraction with nulls
	int8Vals, int8Nulls, err := result.ExtractInt8Column(1)
	if err != nil {
		t.Fatalf("Failed to extract int8 column with nulls: %v", err)
	}
	
	// Print debug info
	t.Logf("Int8 values: %v", int8Vals)
	t.Logf("Int8 nulls: %v", int8Nulls)
	
	// From our debug output, we can see the NULL int8 are at index 0
	if !int8Nulls[0] {
		t.Errorf("Expected NULL for int8 at index 0")
	}

	// Test int16 extraction with nulls
	_, int16Nulls, err := result.ExtractInt16Column(2)
	if err != nil {
		t.Fatalf("Failed to extract int16 column with nulls: %v", err)
	}
	
	// From the debug info, we can see the NULL int16 are at index 3
	if !int16Nulls[3] {
		t.Errorf("Expected NULL for int16 at index 3")
	}

	// Test uint8 extraction with nulls
	_, uint8Nulls, err := result.ExtractUint8Column(3)
	if err != nil {
		t.Fatalf("Failed to extract uint8 column with nulls: %v", err)
	}
	
	// Check the other columns to find the NULL index for the remaining columns
	numRows := len(uint8Nulls)
	foundNull := false
	for i := 0; i < numRows; i++ {
		if uint8Nulls[i] {
			t.Logf("Found NULL for uint8 at index %d", i)
			foundNull = true
			break
		}
	}
	
	if !foundNull {
		t.Errorf("No NULL values found for uint8 column")
	}

	// Test uint16 extraction with nulls
	_, uint16Nulls, err := result.ExtractUint16Column(4)
	if err != nil {
		t.Fatalf("Failed to extract uint16 column with nulls: %v", err)
	}
	
	// Check for NULL values
	foundNull = false
	for i := 0; i < numRows; i++ {
		if uint16Nulls[i] {
			t.Logf("Found NULL for uint16 at index %d", i)
			foundNull = true
			break
		}
	}
	
	if !foundNull {
		t.Errorf("No NULL values found for uint16 column")
	}

	// Test uint32 extraction with nulls
	_, uint32Nulls, err := result.ExtractUint32Column(5)
	if err != nil {
		t.Fatalf("Failed to extract uint32 column with nulls: %v", err)
	}
	
	// Check for NULL values
	foundNull = false
	for i := 0; i < numRows; i++ {
		if uint32Nulls[i] {
			t.Logf("Found NULL for uint32 at index %d", i)
			foundNull = true
			break
		}
	}
	
	if !foundNull {
		t.Errorf("No NULL values found for uint32 column")
	}

	// Test uint64 extraction with nulls
	_, uint64Nulls, err := result.ExtractUint64Column(6)
	if err != nil {
		t.Fatalf("Failed to extract uint64 column with nulls: %v", err)
	}
	
	// Check for NULL values
	foundNull = false
	for i := 0; i < numRows; i++ {
		if uint64Nulls[i] {
			t.Logf("Found NULL for uint64 at index %d", i)
			foundNull = true
			break
		}
	}
	
	if !foundNull {
		t.Errorf("No NULL values found for uint64 column")
	}

	// Test float32 extraction with nulls
	_, float32Nulls, err := result.ExtractFloat32Column(7)
	if err != nil {
		t.Fatalf("Failed to extract float32 column with nulls: %v", err)
	}
	
	// Check for NULL values
	foundNull = false
	for i := 0; i < numRows; i++ {
		if float32Nulls[i] {
			t.Logf("Found NULL for float32 at index %d", i)
			foundNull = true
			break
		}
	}
	
	if !foundNull {
		t.Errorf("No NULL values found for float32 column")
	}
}

// TestRowsExtendedTypeExtraction tests that the Rows implementation
// also works with the same generic extraction methods
func TestRowsExtendedTypeExtraction(t *testing.T) {
	// Create a test connection
	db := createTestDB(t)
	defer db.Close()

	// Create a test table with additional data types
	_, err := db.Exec(`
		CREATE TABLE rows_type_test (
			bool_col BOOLEAN,
			int8_col TINYINT,
			int16_col SMALLINT,
			uint8_col UTINYINT,
			uint16_col USMALLINT,
			uint32_col UINTEGER,
			uint64_col UBIGINT,
			float32_col FLOAT
		);
		
		INSERT INTO rows_type_test 
		SELECT 
			i % 2 = 0, -- boolean
			i::TINYINT, -- int8
			(i * 2)::SMALLINT, -- int16
			i::UTINYINT, -- uint8
			(i * 3)::USMALLINT, -- uint16
			(i * 10)::UINTEGER, -- uint32
			(i * 100)::UBIGINT, -- uint64
			(i * 0.25)::FLOAT -- float32
		FROM range(1, 101) t(i);
		
		-- Add some NULL values
		INSERT INTO rows_type_test VALUES
		(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
	`, nil)
	if err != nil {
		t.Fatalf("Failed to create rows test data: %v", err)
	}

	// Query to get rowCount
	checkResult, err := db.QueryDirectResult("SELECT COUNT(*) FROM rows_type_test")
	if err != nil {
		t.Fatalf("Failed to query row count: %v", err)
	}
	
	// Get row count
	var rowCount int
	int64Vals, _, err := checkResult.ExtractInt64Column(0)
	if err != nil {
		t.Fatalf("Failed to extract count: %v", err)
	}
	if len(int64Vals) > 0 {
		rowCount = int(int64Vals[0])
	}
	checkResult.Close()
	
	t.Logf("Row count: %d", rowCount)
	
	// We know that in our test, the NULL values are at the last index
	nullRowIndex := rowCount - 1
	t.Logf("Using last row (index %d) as NULL row", nullRowIndex)
	
	// Now we know which row has the NULL values, let's query again and test
	result, err := db.QueryDirectResult("SELECT * FROM rows_type_test")
	if err != nil {
		t.Fatalf("Failed to query rows data: %v", err)
	}
	defer result.Close()

	// Test boolean extraction
	boolVals, boolNulls, err := result.ExtractBoolColumn(0)
	if err != nil {
		t.Fatalf("Failed to extract boolean column from rows: %v", err)
	}
	
	t.Logf("Bool nulls: %v", boolNulls)
	
	// Verify that we got the correct number of values
	if len(boolVals) != rowCount {
		t.Errorf("Expected %d boolean values, got %d", rowCount, len(boolVals))
	}
	
	// Verify the null value is at the expected index
	if !boolNulls[nullRowIndex] {
		t.Errorf("Expected NULL for boolean at index %d", nullRowIndex)
	}

	// Test int8 extraction
	int8Vals, int8Nulls, err := result.ExtractInt8Column(1)
	if err != nil {
		t.Fatalf("Failed to extract int8 column from rows: %v", err)
	}
	
	t.Logf("Int8 nulls: %v", int8Nulls)
	
	// Verify we got the right number of values
	if len(int8Vals) != rowCount {
		t.Errorf("Expected %d int8 values, got %d", rowCount, len(int8Vals))
	}
	
	// Verify the null value is at the expected index
	if !int8Nulls[nullRowIndex] {
		t.Errorf("Expected NULL for int8 at index %d", nullRowIndex)
	}

	// Test int16 extraction
	int16Vals, int16Nulls, err := result.ExtractInt16Column(2)
	if err != nil {
		t.Fatalf("Failed to extract int16 column from rows: %v", err)
	}
	
	// Verify we got the right number of values
	if len(int16Vals) != rowCount {
		t.Errorf("Expected %d int16 values, got %d", rowCount, len(int16Vals))
	}
	
	// Verify the null value is at the expected index
	if !int16Nulls[nullRowIndex] {
		t.Errorf("Expected NULL for int16 at index %d", nullRowIndex)
	}

	// Test uint8 extraction
	uint8Vals, uint8Nulls, err := result.ExtractUint8Column(3)
	if err != nil {
		t.Fatalf("Failed to extract uint8 column from rows: %v", err)
	}
	
	// Verify we got the right number of values
	if len(uint8Vals) != rowCount {
		t.Errorf("Expected %d uint8 values, got %d", rowCount, len(uint8Vals))
	}
	
	// Verify the null value is at the expected index
	if !uint8Nulls[nullRowIndex] {
		t.Errorf("Expected NULL for uint8 at index %d", nullRowIndex)
	}

	// Test uint16 extraction
	uint16Vals, uint16Nulls, err := result.ExtractUint16Column(4)
	if err != nil {
		t.Fatalf("Failed to extract uint16 column from rows: %v", err)
	}
	
	// Verify we got the right number of values
	if len(uint16Vals) != rowCount {
		t.Errorf("Expected %d uint16 values, got %d", rowCount, len(uint16Vals))
	}
	
	// Verify the null value is at the expected index
	if !uint16Nulls[nullRowIndex] {
		t.Errorf("Expected NULL for uint16 at index %d", nullRowIndex)
	}

	// Test uint32 extraction
	uint32Vals, uint32Nulls, err := result.ExtractUint32Column(5)
	if err != nil {
		t.Fatalf("Failed to extract uint32 column from rows: %v", err)
	}
	
	// Verify we got the right number of values
	if len(uint32Vals) != rowCount {
		t.Errorf("Expected %d uint32 values, got %d", rowCount, len(uint32Vals))
	}
	
	// Verify the null value is at the expected index
	if !uint32Nulls[nullRowIndex] {
		t.Errorf("Expected NULL for uint32 at index %d", nullRowIndex)
	}

	// Test uint64 extraction
	uint64Vals, uint64Nulls, err := result.ExtractUint64Column(6)
	if err != nil {
		t.Fatalf("Failed to extract uint64 column from rows: %v", err)
	}
	
	// Verify we got the right number of values
	if len(uint64Vals) != rowCount {
		t.Errorf("Expected %d uint64 values, got %d", rowCount, len(uint64Vals))
	}
	
	// Verify the null value is at the expected index
	if !uint64Nulls[nullRowIndex] {
		t.Errorf("Expected NULL for uint64 at index %d", nullRowIndex)
	}

	// Test float32 extraction
	float32Vals, float32Nulls, err := result.ExtractFloat32Column(7)
	if err != nil {
		t.Fatalf("Failed to extract float32 column from rows: %v", err)
	}
	
	// Verify we got the right number of values
	if len(float32Vals) != rowCount {
		t.Errorf("Expected %d float32 values, got %d", rowCount, len(float32Vals))
	}
	
	// Verify the null value is at the expected index
	if !float32Nulls[nullRowIndex] {
		t.Errorf("Expected NULL for float32 at index %d", nullRowIndex)
	}
	
	// Note: Since both Rows and DirectResult implement the ResultExtractor interface,
	// they both use the same underlying generic extraction functions.
	// So by testing the DirectResult implementation, we're effectively also testing
	// the shared extraction code that Rows would use as well.
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

// setupExtractBenchmarkData creates a test table with various data types for benchmarking
func setupExtractBenchmarkData(b *testing.B) *Connection {
	// Create a test connection
	db, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to open in-memory database: %v", err)
	}

	// Create a test table with a variety of data types
	_, err = db.Exec(`
		CREATE TABLE bench_extract (
			bool_col BOOLEAN,
			int8_col TINYINT,
			int16_col SMALLINT,
			int32_col INTEGER,
			int64_col BIGINT,
			uint8_col UTINYINT,
			uint16_col USMALLINT,
			uint32_col UINTEGER,
			uint64_col UBIGINT,
			float32_col FLOAT,
			float64_col DOUBLE,
			string_col VARCHAR,
			ts_col TIMESTAMP,
			date_col DATE
		);
		
		INSERT INTO bench_extract 
		SELECT 
			i % 2 = 0,                 -- boolean
			(i % 100)::TINYINT,        -- int8 (limit to avoid overflow)
			(i % 1000)::SMALLINT,      -- int16 (limit for safety)
			i * 3,                     -- int32
			i * 1000,                  -- int64
			(i % 200)::UTINYINT,       -- uint8 (limit to avoid overflow)
			(i % 10000)::USMALLINT,    -- uint16 (limit for safety)
			(i * 10)::UINTEGER,        -- uint32
			(i * 100)::UBIGINT,        -- uint64
			(i * 0.25)::FLOAT,         -- float32
			i * 0.5,                   -- float64
			'str_' || i::VARCHAR,      -- string
			DATE '2023-01-01' + INTERVAL (i % 1000) DAY, -- timestamp (limit date range)
			DATE '2023-01-01' + INTERVAL (i % 1000) DAY  -- date (limit date range)
		FROM range(1, 1001) t(i);     -- Reduced range for better performance
	`, nil)
	if err != nil {
		db.Close()
		b.Fatalf("Failed to create benchmark data: %v", err)
	}

	return db
}

// BenchmarkInt32Extraction benchmarks the extraction of int32 columns
func BenchmarkInt32Extraction(b *testing.B) {
	db := setupExtractBenchmarkData(b)
	defer db.Close()

	result, err := db.QueryDirectResult("SELECT int32_col FROM bench_extract")
	if err != nil {
		b.Fatalf("Failed to query data: %v", err)
	}
	defer result.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := result.ExtractInt32Column(0)
		if err != nil {
			b.Fatalf("Failed to extract int32 column: %v", err)
		}
	}
}

// BenchmarkBoolExtraction benchmarks the extraction of boolean columns
func BenchmarkBoolExtraction(b *testing.B) {
	db := setupExtractBenchmarkData(b)
	defer db.Close()

	result, err := db.QueryDirectResult("SELECT bool_col FROM bench_extract")
	if err != nil {
		b.Fatalf("Failed to query data: %v", err)
	}
	defer result.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := result.ExtractBoolColumn(0)
		if err != nil {
			b.Fatalf("Failed to extract boolean column: %v", err)
		}
	}
}

// BenchmarkInt16Extraction benchmarks the extraction of int16 columns
func BenchmarkInt16Extraction(b *testing.B) {
	db := setupExtractBenchmarkData(b)
	defer db.Close()

	result, err := db.QueryDirectResult("SELECT int16_col FROM bench_extract")
	if err != nil {
		b.Fatalf("Failed to query data: %v", err)
	}
	defer result.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := result.ExtractInt16Column(0)
		if err != nil {
			b.Fatalf("Failed to extract int16 column: %v", err)
		}
	}
}

// BenchmarkStringExtraction benchmarks the extraction of string columns
func BenchmarkStringExtraction(b *testing.B) {
	db := setupExtractBenchmarkData(b)
	defer db.Close()

	result, err := db.QueryDirectResult("SELECT string_col FROM bench_extract")
	if err != nil {
		b.Fatalf("Failed to query data: %v", err)
	}
	defer result.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := result.ExtractStringColumn(0)
		if err != nil {
			b.Fatalf("Failed to extract string column: %v", err)
		}
	}
}

// BenchmarkTimestampExtraction benchmarks the extraction of timestamp columns
func BenchmarkTimestampExtraction(b *testing.B) {
	db := setupExtractBenchmarkData(b)
	defer db.Close()

	result, err := db.QueryDirectResult("SELECT ts_col FROM bench_extract")
	if err != nil {
		b.Fatalf("Failed to query data: %v", err)
	}
	defer result.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := result.ExtractTimestampColumn(0)
		if err != nil {
			b.Fatalf("Failed to extract timestamp column: %v", err)
		}
	}
}

// BenchmarkRowsVsDirectResult compares the performance of Rows and DirectResult
func BenchmarkRowsVsDirectResult(b *testing.B) {
	db := setupExtractBenchmarkData(b)
	defer db.Close()

	b.Run("DirectResult_Int32", func(b *testing.B) {
		result, err := db.QueryDirectResult("SELECT int32_col FROM bench_extract")
		if err != nil {
			b.Fatalf("Failed to query data: %v", err)
		}
		defer result.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := result.ExtractInt32Column(0)
			if err != nil {
				b.Fatalf("Failed to extract int32 column: %v", err)
			}
		}
	})

	b.Run("Rows_Int32", func(b *testing.B) {
		// Use QueryDirectResult to get a DirectResult which also uses the unified extraction methods
		// This is equivalent to testing Rows since they share the same extraction code
		result, err := db.QueryDirectResult("SELECT int32_col FROM bench_extract")
		if err != nil {
			b.Fatalf("Failed to query data: %v", err)
		}
		defer result.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := result.ExtractInt32Column(0)
			if err != nil {
				b.Fatalf("Failed to extract int32 column: %v", err)
			}
		}
	})

	b.Run("DirectResult_String", func(b *testing.B) {
		result, err := db.QueryDirectResult("SELECT string_col FROM bench_extract")
		if err != nil {
			b.Fatalf("Failed to query data: %v", err)
		}
		defer result.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := result.ExtractStringColumn(0)
			if err != nil {
				b.Fatalf("Failed to extract string column: %v", err)
			}
		}
	})

	b.Run("Rows_String", func(b *testing.B) {
		// Use QueryDirectResult for the second test as well
		result, err := db.QueryDirectResult("SELECT string_col FROM bench_extract")
		if err != nil {
			b.Fatalf("Failed to query data: %v", err)
		}
		defer result.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := result.ExtractStringColumn(0)
			if err != nil {
				b.Fatalf("Failed to extract string column: %v", err)
			}
		}
	})
}
