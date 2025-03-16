package duckdb

import (
	"database/sql"
	"testing"
)

// TestFastRowsCloseInScan verifies that closing rows during scan doesn't cause a panic
func TestFastRowsCloseInScan(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table with multiple string columns
	_, err = db.Exec(`CREATE TABLE scan_close_test (
		id INTEGER,
		s1 VARCHAR,
		s2 VARCHAR,
		s3 VARCHAR
	)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert 100 rows
	stmt, err := db.Prepare(`INSERT INTO scan_close_test VALUES (?, ?, ?, ?)`)
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}

	for i := 0; i < 100; i++ {
		_, err := stmt.Exec(i, "string1", "string2", "string3")
		if err != nil {
			stmt.Close()
			t.Fatalf("failed to insert row: %v", err)
		}
	}
	stmt.Close()

	// Test case 1: Normal row iteration with close at the end
	t.Run("NormalClose", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM scan_close_test")
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}
		defer rows.Close() // This should work fine

		var id int
		var s1, s2, s3 string
		count := 0

		for rows.Next() {
			if err := rows.Scan(&id, &s1, &s2, &s3); err != nil {
				t.Fatalf("failed to scan: %v", err)
			}
			count++
		}

		if count != 100 {
			t.Fatalf("expected 100 rows, got %d", count)
		}
	})

	// Test case 2: Close during row iteration (simulating error handling)
	t.Run("CloseInIteration", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM scan_close_test")
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		var id int
		var s1, s2, s3 string
		count := 0

		// Process only 50 rows then close in the middle
		for rows.Next() {
			if err := rows.Scan(&id, &s1, &s2, &s3); err != nil {
				rows.Close() // This would previously cause a panic
				t.Fatalf("failed to scan: %v", err)
			}
			
			count++
			if count == 50 {
				// Close in middle of iteration
				rows.Close()
				break
			}
		}

		if count != 50 {
			t.Fatalf("expected 50 rows, got %d", count)
		}
	})

	// Test case 3: Call Close() multiple times (which should be safe)
	t.Run("MultipleClose", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM scan_close_test LIMIT 10")
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		var id int
		var s1, s2, s3 string
		
		// Scan one row
		if rows.Next() {
			if err := rows.Scan(&id, &s1, &s2, &s3); err != nil {
				t.Fatalf("failed to scan: %v", err)
			}
		}

		// Close multiple times
		rows.Close()
		rows.Close() // Second close should be safe
		rows.Close() // Third close should be safe
	})

	// Test case 4: Close immediately after query without scanning
	t.Run("CloseWithoutScan", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM scan_close_test")
		if err != nil {
			t.Fatalf("failed to query: %v", err)
		}

		// Close immediately without scanning
		rows.Close()
	})
}