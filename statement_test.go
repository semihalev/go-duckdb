package duckdb

import (
	"context"
	"database/sql/driver"
	"testing"
)

// TestEmptyBlobBinding specifically tests our fix for empty BLOB binding
func TestEmptyBlobBinding(t *testing.T) {
	// Open a direct connection to DuckDB for low-level testing
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create a test table with a BLOB column
	_, err = conn.FastExec("CREATE TABLE blob_test (id INTEGER, data BLOB)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test inserting an empty BLOB directly via a BLOB literal in SQL
	_, err = conn.FastExec("INSERT INTO blob_test VALUES (1, BLOB '')")
	if err != nil {
		t.Fatalf("Failed to insert empty BLOB literal: %v", err)
	}

	// Test inserting NULL BLOB
	_, err = conn.FastExec("INSERT INTO blob_test VALUES (2, NULL)")
	if err != nil {
		t.Fatalf("Failed to insert NULL BLOB: %v", err)
	}

	// Test inserting non-empty BLOB
	_, err = conn.FastExec("INSERT INTO blob_test VALUES (3, BLOB '\\x01\\x02\\x03\\x04\\x05')")
	if err != nil {
		t.Fatalf("Failed to insert non-empty BLOB literal: %v", err)
	}

	// Now check that we can read all the values back
	rows, err := conn.FastQuery("SELECT id, data FROM blob_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	// Count rows to make sure all inserts succeeded
	count := 0
	values := make([]driver.Value, 2)
	for rows.Next(values) == nil {
		count++
		
		id, ok := values[0].(int32)
		if !ok {
			t.Errorf("Expected int32 for id, got %T", values[0])
			continue
		}

		t.Logf("Row %d: data type %T, value: %v", id, values[1], values[1])

		// For empty BLOBs, check if we get the correct value
		if id == 1 {
			// In DuckDB, an empty BLOB is represented either as an empty slice or empty string
			switch v := values[1].(type) {
			case []byte:
				if len(v) != 0 {
					t.Errorf("Expected empty []byte for empty BLOB, got length %d", len(v))
				}
			case string:
				if len(v) != 0 {
					t.Errorf("Expected empty string for empty BLOB, got length %d", len(v))
				}
			default:
				t.Errorf("Expected []byte or string for empty BLOB, got %T", values[1])
			}
		}

		// For NULL BLOBs, we should get nil
		if id == 2 {
			if values[1] != nil {
				t.Errorf("Expected nil for NULL BLOB, got %T: %v", values[1], values[1])
			}
		}

		// For non-empty BLOBs, check the content
		if id == 3 {
			switch v := values[1].(type) {
			case []byte:
				if len(v) != 5 {
					t.Errorf("Expected []byte of length 5 for non-empty BLOB, got length %d", len(v))
				}
			case string:
				// DuckDB formats BLOBs as hex strings like "\x01\x02\x03\x04\x05"
				expected := "\\x01\\x02\\x03\\x04\\x05"
				if v != expected {
					t.Errorf("Expected BLOB hex string '%s', got '%s'", expected, v)
				}
			default:
				t.Errorf("Expected []byte or string for non-empty BLOB, got %T", values[1])
			}
		}
	}

	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}
}

// TestEmptyBlobBindingWithParameters tests the binding of empty BLOBs using parameters
func TestEmptyBlobBindingWithParameters(t *testing.T) {
	// Open a direct connection to DuckDB for low-level testing
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create a test table with a BLOB column
	_, err = conn.FastExec("CREATE TABLE blob_test_params (id INTEGER, data BLOB)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Prepare statement with parameters
	stmt, err := conn.FastPrepare("INSERT INTO blob_test_params VALUES (?, ?)")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	ctx := context.Background()

	// Test binding an empty BLOB
	emptyBlob := make([]byte, 0)
	_, err = stmt.ExecContext(ctx, []driver.NamedValue{
		{Ordinal: 1, Value: 1},
		{Ordinal: 2, Value: emptyBlob},
	})
	if err != nil {
		t.Fatalf("Failed to bind empty BLOB: %v", err)
	}

	// Test binding NULL
	_, err = stmt.ExecContext(ctx, []driver.NamedValue{
		{Ordinal: 1, Value: 2},
		{Ordinal: 2, Value: nil},
	})
	if err != nil {
		t.Fatalf("Failed to bind NULL BLOB: %v", err)
	}

	// Test binding non-empty BLOB
	nonEmptyBlob := []byte{1, 2, 3, 4, 5}
	_, err = stmt.ExecContext(ctx, []driver.NamedValue{
		{Ordinal: 1, Value: 3},
		{Ordinal: 2, Value: nonEmptyBlob},
	})
	if err != nil {
		t.Fatalf("Failed to bind non-empty BLOB: %v", err)
	}

	// Now check that we can read all the values back
	rows, err := conn.FastQuery("SELECT id, data FROM blob_test_params ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	// Count rows to make sure all inserts succeeded
	count := 0
	values := make([]driver.Value, 2)
	for rows.Next(values) == nil {
		count++
		
		id, ok := values[0].(int32)
		if !ok {
			t.Errorf("Expected int32 for id, got %T", values[0])
			continue
		}

		t.Logf("Row %d: data type %T, value: %v", id, values[1], values[1])

		// For empty BLOBs, check if we get the correct value
		if id == 1 {
			// In DuckDB, an empty BLOB is represented either as an empty slice or empty string
			switch v := values[1].(type) {
			case []byte:
				if len(v) != 0 {
					t.Errorf("Expected empty []byte for empty BLOB, got length %d", len(v))
				}
			case string:
				if len(v) != 0 {
					t.Errorf("Expected empty string for empty BLOB, got length %d", len(v))
				}
			default:
				t.Errorf("Expected []byte or string for empty BLOB, got %T", values[1])
			}
		}

		// For NULL BLOBs, we should get nil
		if id == 2 {
			if values[1] != nil {
				t.Errorf("Expected nil for NULL BLOB, got %T: %v", values[1], values[1])
			}
		}

		// For non-empty BLOBs, check the content
		if id == 3 {
			switch v := values[1].(type) {
			case []byte:
				if len(v) != 5 {
					t.Errorf("Expected []byte of length 5 for non-empty BLOB, got length %d", len(v))
				}
			case string:
				// DuckDB formats BLOBs as hex strings like "\x01\x02\x03\x04\x05"
				expected := "\\x01\\x02\\x03\\x04\\x05"
				if v != expected {
					t.Errorf("Expected BLOB hex string '%s', got '%s'", expected, v)
				}
			default:
				t.Errorf("Expected []byte or string for non-empty BLOB, got %T", values[1])
			}
		}
	}

	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}
}