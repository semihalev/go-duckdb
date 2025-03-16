package duckdb

import (
	"context"
	"database/sql/driver"
	"sync"
	"testing"
)

// TestConcurrentScan tests specifically if our fix for the row.Scan issues
// works in a concurrent context
func TestConcurrentScan(t *testing.T) {
	// Open a direct connection to DuckDB for low-level testing
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()
	
	// Create a simple test table
	_, err = conn.FastExec("CREATE TABLE scan_test (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Insert test data
	_, err = conn.FastExec("INSERT INTO scan_test SELECT range as id, 'name_' || range as name FROM range(1000)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}
	
	// Function to run a query and scan rows, closing immediately after a scan
	doQueryWithImmediateClose := func(queryID int) error {
		// Run query for a subset of rows
		rows, err := conn.FastQuery("SELECT id, name FROM scan_test WHERE id % 10 = " + string(rune(48+queryID%10)) + " LIMIT 5")
		if err != nil {
			return err
		}
		
		// We'll close immediately after scanning a row
		defer rows.Close()
		
		// Process only one row
		values := make([]driver.Value, 2)
		if rows.Next(values) != nil {
			return err
		}
		
		// Access the values to make sure they're valid
		idVal, ok1 := values[0].(int32)
		_, ok2 := values[1].(string)
		if !ok1 || !ok2 || idVal < 0 {
			t.Logf("Type assertion failed: %T, %T", values[0], values[1])
		}
		
		return nil
	}
	
	// Run many concurrent queries that close immediately after scanning
	t.Run("ImmediateClose", func(t *testing.T) {
		var wg sync.WaitGroup
		errCount := 0
		
		// Run 50 concurrent queries
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 10; j++ { // 10 queries per goroutine
					if err := doQueryWithImmediateClose(id); err != nil {
						t.Logf("Query failed: %v", err)
						errCount++
					}
				}
			}(i)
		}
		
		wg.Wait()
		if errCount > 0 {
			t.Errorf("%d queries failed", errCount)
		}
	})
	
	// Test cancellation during scan
	t.Run("CancelDuringScan", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		
		var wg sync.WaitGroup
		wg.Add(1)
		
		go func() {
			defer wg.Done()
			
			// Start a query
			stmt, err := conn.PrepareContext(ctx, "SELECT * FROM scan_test")
			if err != nil {
				t.Errorf("Failed to prepare: %v", err)
				return
			}
			defer stmt.Close()
			
			// Execute the query
			rows, err := stmt.Query([]driver.Value{})
			if err != nil {
				t.Errorf("Failed to query: %v", err)
				return
			}
			defer rows.Close()
			
			// Start scanning rows
			values := make([]driver.Value, 2)
			for i := 0; i < 10; i++ {
				if err := rows.Next(values); err != nil {
					// Expected after cancellation
					return
				}
			}
		}()
		
		// Cancel the context during query execution
		cancel()
		
		// Wait for the query to complete or be cancelled
		wg.Wait()
	})
}