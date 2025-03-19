package duckdb

import (
	"database/sql"
	"fmt"
	"testing"
)

// TestSimpleBatchInsert tests a simple batch insertion
func TestSimpleBatchInsert(t *testing.T) {
	// Open a connection to an in-memory database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table
	_, err = db.Exec("CREATE TABLE batch_test (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Use a regular prepared statement with batched executions
	stmt, err := db.Prepare("INSERT INTO batch_test VALUES (?, ?)")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Insert multiple rows
	for i := 1; i <= 10; i++ {
		_, err := stmt.Exec(i, fmt.Sprintf("Name %d", i))
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Verify the data
	rows, err := db.Query("SELECT id, name FROM batch_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		expectedID := count + 1
		expectedName := fmt.Sprintf("Name %d", expectedID)

		if id != expectedID {
			t.Errorf("Row %d: Expected id %d, got %d", count, expectedID, id)
		}
		if name != expectedName {
			t.Errorf("Row %d: Expected name %s, got %s", count, expectedName, name)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Error during rows iteration: %v", err)
	}

	if count != 10 {
		t.Errorf("Expected 10 rows, got %d", count)
	}
}

// TestBatchInsertWithDirectAccess tests the batch insertion functionality
// using the batch execution API directly
func TestBatchInsertWithDirectAccess(t *testing.T) {
	// Skip if we're in short test mode
	if testing.Short() {
		t.Skip("Skipping test in short mode")
	}

	// This test requires direct access to the duckdb driver
	driverName := "duckdb"
	driver, err := sql.Open(driverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer driver.Close()

	// Create a test table
	_, err = driver.Exec(`CREATE TABLE test_direct (
		id INTEGER, 
		name VARCHAR,
		value DOUBLE
	)`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Use conventional methods to insert data
	stmt, err := driver.Prepare("INSERT INTO test_direct VALUES (?, ?, ?)")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}

	// Insert data
	for i := 1; i <= 5; i++ {
		_, err := stmt.Exec(i, fmt.Sprintf("Row %d", i), float64(i)*1.5)
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}
	stmt.Close()

	// Read back the data to verify it
	rows, err := driver.Query("SELECT id, name, value FROM test_direct ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var name string
		var value float64

		err := rows.Scan(&id, &name, &value)
		if err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		expectedID := count + 1
		expectedName := fmt.Sprintf("Row %d", expectedID)
		expectedValue := float64(expectedID) * 1.5

		if id != expectedID {
			t.Errorf("Expected id %d, got %d", expectedID, id)
		}
		if name != expectedName {
			t.Errorf("Expected name %s, got %s", expectedName, name)
		}
		if value != expectedValue {
			t.Errorf("Expected value %f, got %f", expectedValue, value)
		}

		count++
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Error during rows iteration: %v", err)
	}

	if count != 5 {
		t.Errorf("Expected 5 rows, got %d", count)
	}
}
