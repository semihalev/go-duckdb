package duckdb

import (
	"database/sql/driver"
	"fmt"
	"testing"
)

func TestMMapQuery(t *testing.T) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create test table
	_, err = conn.FastExec(`
		CREATE TABLE mmap_test (
			id INTEGER, 
			name VARCHAR, 
			value DOUBLE, 
			flag BOOLEAN
		);
		
		INSERT INTO mmap_test 
		SELECT 
			i AS id, 
			'name_' || i AS name, 
			i * 1.5 AS value, 
			i % 2 = 0 AS flag
		FROM range(1, 1001) t(i);
	`)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Execute memory-mapped query
	rows, err := conn.QueryMMap("SELECT * FROM mmap_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to execute memory-mapped query: %v", err)
	}
	defer rows.Close()

	// Check column names
	cols := rows.Columns()
	expectedCols := []string{"id", "name", "value", "flag"}
	if len(cols) != len(expectedCols) {
		t.Fatalf("Expected %d columns, got %d", len(expectedCols), len(cols))
	}
	for i, col := range cols {
		if col != expectedCols[i] {
			t.Fatalf("Expected column %d to be %s, got %s", i, expectedCols[i], col)
		}
	}

	// Check column types
	scanTypes := make([]interface{}, len(cols))
	for i := range scanTypes {
		scanType := rows.ColumnTypeScanType(i)
		if scanType == nil {
			t.Fatalf("Expected scan type for column %d, got nil", i)
		}
	}

	// Read all rows and verify data
	var rowCount int
	values := make([]driver.Value, 4)

	for {
		err := rows.Next(values)
		if err != nil {
			break
		}

		rowCount++
		id := values[0].(int32)
		name := values[1].(string)
		value := values[2].(float64)
		flag := values[3].(bool)

		// Verify data
		expectedID := int32(rowCount)
		expectedName := fmt.Sprintf("name_%d", rowCount)
		expectedValue := float64(rowCount) * 1.5
		expectedFlag := rowCount%2 == 0

		if id != expectedID {
			t.Fatalf("Row %d: Expected id %d, got %d", rowCount, expectedID, id)
		}
		if name != expectedName {
			t.Fatalf("Row %d: Expected name %s, got %s", rowCount, expectedName, name)
		}
		if value != expectedValue {
			t.Fatalf("Row %d: Expected value %f, got %f", rowCount, expectedValue, value)
		}
		if flag != expectedFlag {
			t.Fatalf("Row %d: Expected flag %v, got %v", rowCount, expectedFlag, flag)
		}
	}

	if rowCount != 1000 {
		t.Fatalf("Expected 1000 rows, got %d", rowCount)
	}
}