package duckdb

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestDuckDBVersion(t *testing.T) {
	// Get the version information
	version := GetDuckDBVersion()
	
	// Log the detected version
	t.Logf("Detected DuckDB version: %s (major=%d, minor=%d, patch=%d)", 
		version.String(), version.Major, version.Minor, version.Patch)
	
	// Verify that the version string contains expected information
	if version.VersionStr == "" {
		t.Errorf("Version string should not be empty")
	}
	
	// Test IsAtLeast120 method
	isAtLeast120 := version.IsAtLeast120()
	t.Logf("Is at least 1.2.0: %v", isAtLeast120)
	
	// Basic version comparison tests
	if version.AtLeast(0, 0, 0) != true {
		t.Errorf("Version should be at least 0.0.0")
	}
	
	if version.AtLeast(999, 999, 999) != false {
		t.Errorf("Version should not be at least 999.999.999")
	}
}

func TestDuckDBExtensionLoading(t *testing.T) {
	// Skip if not using DuckDB 1.2.0+
	version := GetDuckDBVersion()
	if !version.IsAtLeast120() {
		t.Skip("Skipping test for DuckDB < 1.2.0")
	}

	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Try to load the built-in json extension which should be available in 1.2.0+
	_, err = db.Exec("INSTALL json; LOAD json;")
	if err != nil {
		// Don't fail the test if the extension isn't available - it could be a compilation option
		if strings.Contains(err.Error(), "No extension found") {
			t.Logf("JSON extension not available, skipping extension test")
			return
		}
		t.Fatalf("failed to load json extension: %v", err)
	}

	// Test parsing a simple JSON
	rows, err := db.Query("SELECT json_extract('{\"a\": 1}', '$.a') as value")
	if err != nil {
		t.Fatalf("failed to execute json query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatalf("expected a row")
	}

	var value int
	err = rows.Scan(&value)
	if err != nil {
		t.Fatalf("failed to scan value: %v", err)
	}

	if value != 1 {
		t.Fatalf("expected 1, got %d", value)
	}
}

// Test DuckDB 1.2.0 specific appender functionality
func TestAppenderCompat(t *testing.T) {
	// This test checks that appenders work correctly regardless of DuckDB version
	
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()
	
	// First, get the database connection
	sqlDB := db.Driver().Open("testing_appender_compat")
	conn, ok := sqlDB.(*conn)
	if !ok {
		t.Fatalf("unexpected driver connection type: %T", sqlDB)
	}
	defer conn.Close()
	
	// Create a test table
	_, err = db.Exec("CREATE TABLE appender_test (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	
	// Create an appender for the table
	appender, err := NewAppender(conn, "main", "appender_test")
	if err != nil {
		t.Fatalf("failed to create appender: %v", err)
	}
	
	// Append some rows
	err = appender.AppendRow(1, "Alice")
	if err != nil {
		t.Fatalf("failed to append row: %v", err)
	}
	
	err = appender.AppendRow(2, "Bob")
	if err != nil {
		t.Fatalf("failed to append row: %v", err)
	}
	
	// Flush the appender
	err = appender.Flush()
	if err != nil {
		t.Fatalf("failed to flush appender: %v", err)
	}
	
	// Close the appender
	err = appender.Close()
	if err != nil {
		t.Fatalf("failed to close appender: %v", err)
	}
	
	// Verify the data was inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM appender_test").Scan(&count)
	if err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	
	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}
}

// Test context cancellation with DuckDB 1.2.0
func TestContextCancellationCompat(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()
	
	// Create a table with data
	_, err = db.Exec(`CREATE TABLE cancel_test (id INTEGER)`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}
	
	// Insert test data - a lot of rows to make the query take time
	_, err = db.Exec(`INSERT INTO cancel_test SELECT * FROM range(1, 1000000)`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}
	
	// Test that cancellation works
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Cancel the query after a short time
		cancel()
	}()
	
	// Try to do a heavy query that should get cancelled
	_, err = db.QueryContext(ctx, "SELECT * FROM cancel_test ORDER BY random()")
	if err == nil {
		t.Fatal("expected query to be cancelled")
	}
	
	// The error could be context.Canceled or a DuckDB-specific error about interruption
	if err != context.Canceled && !strings.Contains(strings.ToLower(err.Error()), "cancel") && 
	   !strings.Contains(strings.ToLower(err.Error()), "interrupt") {
		t.Fatalf("unexpected error: %v", err)
	}
}