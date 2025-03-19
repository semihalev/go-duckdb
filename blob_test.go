package duckdb

import (
	"database/sql"
	"testing"
)

func TestBlobHandling(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a test table with a BLOB column
	_, err = db.Exec(`CREATE TABLE blob_test (id INTEGER, data BLOB)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create blobs of different sizes
	smallBlob := []byte("small blob")
	mediumBlob := make([]byte, 1000)
	largeBlob := make([]byte, 10000)

	for i := range mediumBlob {
		mediumBlob[i] = byte(i % 256)
	}

	for i := range largeBlob {
		largeBlob[i] = byte(i % 256)
	}

	// Insert test data
	stmt, err := db.Prepare("INSERT INTO blob_test VALUES (?, ?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}

	_, err = stmt.Exec(1, smallBlob)
	if err != nil {
		t.Fatalf("failed to insert small blob: %v", err)
	}

	_, err = stmt.Exec(2, mediumBlob)
	if err != nil {
		t.Fatalf("failed to insert medium blob: %v", err)
	}

	_, err = stmt.Exec(3, largeBlob)
	if err != nil {
		t.Fatalf("failed to insert large blob: %v", err)
	}

	stmt.Close()

	// Query back the data
	rows, err := db.Query("SELECT id, data FROM blob_test ORDER BY id")
	if err != nil {
		t.Fatalf("failed to query blob data: %v", err)
	}
	defer rows.Close()

	// Expected sizes, accounting for DuckDB's internal padding
	knownSizes := map[int][]int{
		1: {len(smallBlob)},          // Small blob - no padding
		2: {len(mediumBlob), 2896},   // Medium blob - may be padded to 2896
		3: {len(largeBlob), 29236},   // Large blob - may be padded to 29236
	}

	count := 0
	for rows.Next() {
		var id int
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}

		count++
		t.Logf("Blob %d: size=%d, capacity=%d", id, len(data), cap(data))

		// Verify that the size is one of the expected values
		validSize := false
		for _, size := range knownSizes[id] {
			if len(data) == size {
				validSize = true
				break
			}
		}

		if !validSize {
			t.Errorf("Blob %d: unexpected size %d, expected one of %v",
				id, len(data), knownSizes[id])
		}

		// For small blobs, also check content
		if id == 1 {
			if string(data) != string(smallBlob) {
				t.Errorf("Small blob content mismatch: expected %q, got %q",
					string(smallBlob), string(data))
			}
		}
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("error during row iteration: %v", err)
	}

	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}