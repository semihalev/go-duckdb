package duckdb

import (
	"database/sql/driver"
	"testing"
)

// TestSimpleStringScan tests a basic string scan with our optimized implementation
func TestSimpleStringScan(t *testing.T) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer conn.Close()

	// Create a simple test table
	_, err = conn.FastExec("CREATE TABLE string_test (s VARCHAR)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	_, err = conn.FastExec("INSERT INTO string_test VALUES ('test_string_1'), ('test_string_2')")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Query the data
	rows, err := conn.FastQuery("SELECT s FROM string_test")
	if err != nil {
		t.Fatalf("failed to query data: %v", err)
	}
	defer rows.Close()

	count := 0
	for {
		values := make([]driver.Value, 1)
		err := rows.Next(values)
		if err != nil {
			break
		}

		// Verify we can read the string
		str, ok := values[0].(string)
		if !ok {
			t.Fatalf("expected string, got %T", values[0])
		}

		t.Logf("Read string: %s", str)
		count++
	}

	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}
}