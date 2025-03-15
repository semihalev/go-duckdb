package duckdb

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestDriverBasics(t *testing.T) {
	// Connect to an in-memory database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Test connection
	err = db.Ping()
	if err != nil {
		t.Fatalf("ping failed: %v", err)
	}

	// Create a table
	_, err = db.Exec(`CREATE TABLE test (id INTEGER, name VARCHAR, active BOOLEAN, created TIMESTAMP)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Test transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Insert data with prepared statement
	now := time.Now()
	stmt, err := tx.Prepare("INSERT INTO test (id, name, active, created) VALUES (?, ?, ?, ?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}

	_, err = stmt.Exec(1, "Test 1", true, now)
	if err != nil {
		t.Fatalf("failed to execute prepared statement: %v", err)
	}

	_, err = stmt.Exec(2, "Test 2", false, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("failed to execute prepared statement: %v", err)
	}

	err = stmt.Close()
	if err != nil {
		t.Fatalf("failed to close statement: %v", err)
	}

	// Commit transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	// Query the data
	rows, err := db.Query("SELECT id, name, active, created FROM test ORDER BY id")
	if err != nil {
		t.Fatalf("failed to query data: %v", err)
	}

	// Check column names
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("failed to get columns: %v", err)
	}

	expectedCols := []string{"id", "name", "active", "created"}
	if len(cols) != len(expectedCols) {
		t.Fatalf("expected %d columns, got %d", len(expectedCols), len(cols))
	}

	for i, col := range cols {
		if col != expectedCols[i] {
			t.Fatalf("expected column %s at position %d, got %s", expectedCols[i], i, col)
		}
	}

	// Check the data
	counter := 0
	for rows.Next() {
		counter++
		var id int
		var name string
		var active bool
		var created time.Time

		err := rows.Scan(&id, &name, &active, &created)
		if err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}

		if id != counter {
			t.Fatalf("expected id %d, got %d", counter, id)
		}

		expectedName := "Test " + string('0'+counter)
		if name != expectedName {
			t.Fatalf("expected name %s, got %s", expectedName, name)
		}

		expectedActive := counter == 1
		if active != expectedActive {
			t.Fatalf("expected active %v, got %v", expectedActive, active)
		}
	}

	if counter != 2 {
		t.Fatalf("expected 2 rows, got %d", counter)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("failed to close rows: %v", err)
	}

	// Test NULL values
	_, err = db.Exec("INSERT INTO test (id, name, active, created) VALUES (3, NULL, NULL, NULL)")
	if err != nil {
		t.Fatalf("failed to insert NULL values: %v", err)
	}

	var id int
	var name sql.NullString
	var active sql.NullBool
	var created sql.NullTime

	err = db.QueryRow("SELECT id, name, active, created FROM test WHERE id = 3").Scan(&id, &name, &active, &created)
	if err != nil {
		t.Fatalf("failed to query NULL values: %v", err)
	}

	if id != 3 {
		t.Fatalf("expected id 3, got %d", id)
	}

	if name.Valid {
		t.Fatalf("expected NULL name, got %s", name.String)
	}

	if active.Valid {
		t.Fatalf("expected NULL active, got %v", active.Bool)
	}

	if created.Valid {
		t.Fatalf("expected NULL created, got %v", created.Time)
	}
}

func TestRollback(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec("CREATE TABLE rollback_test (id INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Insert data
	_, err = tx.Exec("INSERT INTO rollback_test VALUES (1), (2), (3)")
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Rollback
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Check no data was inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM rollback_test").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query count: %v", err)
	}

	if count != 0 {
		t.Fatalf("expected 0 rows after rollback, got %d", count)
	}
}

func TestBlobData(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with BLOB
	_, err = db.Exec("CREATE TABLE blob_test (id INTEGER, data BLOB)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Test blob data
	testBlob := []byte{0x01, 0x02, 0x03, 0x04, 0xFF, 0xFE, 0xFD, 0xFC}

	// Insert blob
	_, err = db.Exec("INSERT INTO blob_test (id, data) VALUES (?, ?)", 1, testBlob)
	if err != nil {
		t.Fatalf("failed to insert blob: %v", err)
	}

	// Read blob back
	var id int
	var data []byte

	err = db.QueryRow("SELECT id, data FROM blob_test WHERE id = 1").Scan(&id, &data)
	if err != nil {
		t.Fatalf("failed to query blob: %v", err)
	}

	if id != 1 {
		t.Fatalf("expected id 1, got %d", id)
	}

	if len(data) != len(testBlob) {
		t.Fatalf("blob length mismatch: expected %d, got %d", len(testBlob), len(data))
	}

	for i := 0; i < len(testBlob); i++ {
		if data[i] != testBlob[i] {
			t.Fatalf("blob data mismatch at position %d: expected %d, got %d", i, testBlob[i], data[i])
		}
	}
}

func TestQueryerContext(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table with data
	_, err = db.Exec(`CREATE TABLE query_context_test (id INTEGER, value VARCHAR)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert test data
	_, err = db.Exec(`INSERT INTO query_context_test VALUES (1, 'one'), (2, 'two'), (3, 'three')`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Test with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// This should return a context canceled error
	_, err = db.QueryContext(ctx, "SELECT * FROM query_context_test")
	if err == nil {
		t.Fatal("expected context canceled error, got nil")
	}
	if err != context.Canceled {
		t.Fatalf("expected context canceled error, got %v", err)
	}

	// Test successful query with parameters
	ctx = context.Background()
	rows, err := db.QueryContext(ctx, "SELECT id, value FROM query_context_test WHERE id > ?", 1)
	if err != nil {
		t.Fatalf("failed to query with context: %v", err)
	}
	defer rows.Close()

	// Check results
	var count int
	for rows.Next() {
		var id int
		var value string
		err := rows.Scan(&id, &value)
		if err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		count++

		if id <= 1 {
			t.Fatalf("expected id > 1, got %d", id)
		}
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration failed: %v", err)
	}

	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}
}

func TestExecerContext(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`CREATE TABLE exec_context_test (id INTEGER, value VARCHAR)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Test with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// This should return a context canceled error
	_, err = db.ExecContext(ctx, "INSERT INTO exec_context_test VALUES (1, 'test')")
	if err == nil {
		t.Fatal("expected context canceled error, got nil")
	}
	if err != context.Canceled {
		t.Fatalf("expected context canceled error, got %v", err)
	}

	// Test successful exec with parameters
	ctx = context.Background()
	result, err := db.ExecContext(ctx, "INSERT INTO exec_context_test VALUES (?, ?), (?, ?)", 1, "one", 2, "two")
	if err != nil {
		t.Fatalf("failed to exec with context: %v", err)
	}

	// Check rows affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	}

	if rowsAffected != 2 {
		t.Fatalf("expected 2 rows affected, got %d", rowsAffected)
	}

	// Verify the data was inserted correctly
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM exec_context_test").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}

	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}
}

func TestPrepareContext(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`CREATE TABLE prepare_context_test (id INTEGER, value VARCHAR)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Test with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// This should return a context canceled error
	_, err = db.PrepareContext(ctx, "SELECT * FROM prepare_context_test")
	if err == nil {
		t.Fatal("expected context canceled error, got nil")
	}
	if err != context.Canceled {
		t.Fatalf("expected context canceled error, got %v", err)
	}

	// Test successful prepare with context
	ctx = context.Background()
	stmt, err := db.PrepareContext(ctx, "INSERT INTO prepare_context_test VALUES (?, ?)")
	if err != nil {
		t.Fatalf("failed to prepare with context: %v", err)
	}
	defer stmt.Close()

	// Use the prepared statement
	result, err := stmt.Exec(1, "test")
	if err != nil {
		t.Fatalf("failed to execute prepared statement: %v", err)
	}

	// Check rows affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	}

	if rowsAffected != 1 {
		t.Fatalf("expected 1 row affected, got %d", rowsAffected)
	}

	// Verify the data was inserted correctly
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM prepare_context_test").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}
}
