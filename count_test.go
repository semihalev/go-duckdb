package duckdb

import (
	"database/sql/driver"
	"testing"
)

func TestCountReturnType(t *testing.T) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create test table
	_, err = conn.FastExec("CREATE TABLE count_test (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert a row
	_, err = conn.FastExec("INSERT INTO count_test VALUES (1, 'test')")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Query the count
	result, err := conn.FastQuery("SELECT COUNT(*) FROM count_test")
	if err != nil {
		t.Fatalf("Failed to query count: %v", err)
	}
	defer result.Close()

	values := make([]driver.Value, 1)
	err = result.Next(values)
	if err != nil {
		t.Fatalf("Failed to get count: %v", err)
	}

	// Print the actual type and value
	t.Logf("COUNT(*) returned type: %T, value: %v", values[0], values[0])
}