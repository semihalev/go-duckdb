package duckdb

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"
)

func TestNativeLibraryStatus(t *testing.T) {
	// Get native optimization info
	info := GetNativeOptimizationInfo()

	// Log the information for debugging
	t.Logf("Native library status: %v", info.String())

	// We don't expect this to fail, just informational
	if info.Available {
		t.Logf("Native library loaded from: %s", info.Path)
		t.Logf("SIMD features: %s", info.SIMDFeatures)
	} else {
		t.Logf("Native library not available: %s", info.Error)
	}
}

func TestFallbackExtractColumnsWithoutNativeLibrary(t *testing.T) {
	// Force fallback mode for this test
	origNativeLibLoaded := nativeLibLoaded
	nativeLibLoaded = false
	defer func() {
		nativeLibLoaded = origNativeLibLoaded
	}()

	// Create an in-memory database
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create a test table with different data types
	_, err = conn.Exec("CREATE TABLE test (int_col INTEGER, str_col VARCHAR, bool_col BOOLEAN, float_col DOUBLE)", nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some test data
	_, err = conn.Exec("INSERT INTO test VALUES (42, 'hello', true, 3.14)", nil)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Query the data
	rows, err := conn.Query("SELECT * FROM test", nil)
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	defer rows.Close()

	// Read the values
	values := make([]driver.Value, 4)
	err = rows.Next(values)
	if err != nil {
		t.Fatalf("Failed to read row: %v", err)
	}

	// Verify the values
	if values[0].(int32) != 42 {
		t.Errorf("Expected int_col=42, got %v", values[0])
	}
	if values[1].(string) != "hello" {
		t.Errorf("Expected str_col='hello', got %v", values[1])
	}
	if values[2].(bool) != true {
		t.Errorf("Expected bool_col=true, got %v", values[2])
	}
	if values[3].(float64) != 3.14 {
		t.Errorf("Expected float_col=3.14, got %v", values[3])
	}
}

// Add specific test for batch parameter binding
func TestBatchParamBindingWithFallback(t *testing.T) {
	// Force fallback mode for this test
	origNativeLibLoaded := nativeLibLoaded
	nativeLibLoaded = false
	defer func() {
		nativeLibLoaded = origNativeLibLoaded
	}()

	// Create a direct connection
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create table for testing
	_, err = conn.Query("CREATE TABLE batch_test (id INTEGER, name VARCHAR, value DOUBLE, created TIMESTAMP)", nil)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create a batch of parameter sets
	now := time.Now()
	paramSets := [][]interface{}{
		{1, "Item 1", 10.5, now},
		{2, "Item 2", 20.5, now.Add(time.Hour)},
		{3, "Item 3", 30.5, now.Add(2 * time.Hour)},
		{4, "Item 4", 40.5, now.Add(3 * time.Hour)},
		{5, "Item 5", 50.5, now.Add(4 * time.Hour)},
	}

	// Execute batch
	result, err := conn.BatchExec("INSERT INTO batch_test VALUES (?, ?, ?, ?)", paramSets)
	if err != nil {
		t.Fatalf("failed to execute batch: %v", err)
	}

	// Check affected rows
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	}
	if rowsAffected != 5 {
		t.Errorf("expected 5 rows affected, got %d", rowsAffected)
	}

	// Query to verify data
	countRows, err := conn.Query("SELECT COUNT(*) FROM batch_test", nil)
	if err != nil {
		t.Fatalf("failed to query count: %v", err)
	}

	countValues := make([]driver.Value, 1)
	err = countRows.Next(countValues)
	if err != nil {
		t.Fatalf("failed to get count: %v", err)
	}
	countRows.Close()

	var count int
	switch v := countValues[0].(type) {
	case int32:
		count = int(v)
	case int64:
		count = int(v)
	default:
		t.Fatalf("unexpected count type: %T", countValues[0])
	}
	if count != 5 {
		t.Errorf("expected 5 rows, got %d", count)
	}
}

// Test batch param binding with native library (if available)
func TestBatchParamBindingWithNative(t *testing.T) {
	// Only run this test if native library is available
	if !NativeOptimizationsAvailable() {
		t.Skip("Native library not available, skipping test")
	}

	// Force native library to be false for this test to ensure it uses the fallback
	origNativeLibLoaded := nativeLibLoaded
	nativeLibLoaded = false
	defer func() {
		nativeLibLoaded = origNativeLibLoaded
	}()

	// Create a direct connection
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create table for testing
	_, err = conn.Query("CREATE TABLE batch_test_native (id INTEGER, name VARCHAR, value DOUBLE, created TIMESTAMP)", nil)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create a batch of parameter sets
	now := time.Now()
	paramSets := [][]interface{}{
		{1, "Native 1", 10.5, now},
		{2, "Native 2", 20.5, now.Add(time.Hour)},
		{3, "Native 3", 30.5, now.Add(2 * time.Hour)},
	}

	// Execute batch
	result, err := conn.BatchExec("INSERT INTO batch_test_native VALUES (?, ?, ?, ?)", paramSets)
	if err != nil {
		t.Fatalf("failed to execute batch: %v", err)
	}

	// Check affected rows
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("failed to get rows affected: %v", err)
	}
	if rowsAffected != 3 {
		t.Errorf("expected 3 rows affected, got %d", rowsAffected)
	}

	// Prepare direct statement
	stmt, err := NewOptimizedBatchStmt(conn, "INSERT INTO batch_test_native VALUES (?, ?, ?, ?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Execute with context
	moreParams := [][]interface{}{
		{10, "Context 1", 100.5, now.Add(5 * time.Hour)},
		{20, "Context 2", 200.5, now.Add(6 * time.Hour)},
	}

	result, err = stmt.ExecBatchContext(context.Background(), moreParams)
	if err != nil {
		t.Fatalf("failed to execute batch with context: %v", err)
	}

	// Check total rows
	countRows, err := conn.Query("SELECT COUNT(*) FROM batch_test_native", nil)
	if err != nil {
		t.Fatalf("failed to query count: %v", err)
	}

	countValues := make([]driver.Value, 1)
	err = countRows.Next(countValues)
	if err != nil {
		t.Fatalf("failed to get count: %v", err)
	}
	countRows.Close()

	var count int
	switch v := countValues[0].(type) {
	case int32:
		count = int(v)
	case int64:
		count = int(v)
	default:
		t.Fatalf("unexpected count type: %T", countValues[0])
	}
	if count != 5 {
		t.Errorf("expected 5 rows total, got %d", count)
	}
}
