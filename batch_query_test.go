package duckdb

import (
	"database/sql/driver"
	"fmt"
	"testing"
)

func TestBatchQuery(t *testing.T) {
	// Skip if short test mode
	if testing.Short() {
		t.Skip("Skipping batch query test in short mode")
	}
	
	// Use memory database with shared schema
	dbname := ":memory:"
	
	// Get direct connection first
	dconn, err := NewConnection(dbname)
	if err != nil {
		t.Fatalf("Failed to get direct connection: %v", err)
	}
	defer dconn.Close()
	
	// Create a test table
	_, err = dconn.Exec("CREATE TABLE batch_test (id INTEGER, name VARCHAR, value DOUBLE, flag BOOLEAN)", nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	
	// Create a batch statement for inserting
	insertStmt, err := newStatement(dconn, "INSERT INTO batch_test VALUES (?, ?, ?, ?)")
	if err != nil {
		t.Fatalf("Failed to prepare insert statement: %v", err)
	}
	defer insertStmt.Close()
	
	// Insert test data - 1000 rows
	for i := 0; i < 1000; i++ {
		args := []driver.Value{
			i, fmt.Sprintf("name-%d", i), float64(i) * 1.5, i%2 == 0,
		}
		
		_, err = insertStmt.Exec(args)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}
	
	// Test batch query
	t.Run("BatchQuery", func(t *testing.T) {
		// Execute the batch query
		rows, err := dconn.QueryBatch("SELECT * FROM batch_test ORDER BY id LIMIT 100", 10)
		if err != nil {
			t.Fatalf("Failed to execute batch query: %v", err)
		}
		defer rows.Close()
		
		// Check column names
		columns := rows.Columns()
		expectedColumns := []string{"id", "name", "value", "flag"}
		if len(columns) != len(expectedColumns) {
			t.Fatalf("Expected %d columns, got %d", len(expectedColumns), len(columns))
		}
		for i, col := range columns {
			if col != expectedColumns[i] {
				t.Fatalf("Expected column %d to be %s, got %s", i, expectedColumns[i], col)
			}
		}
		
		// Read rows - we should get all 100 rows
		rowCount := 0
		for {
			values := make([]driver.Value, 4)
			err := rows.Next(values)
			if err != nil {
				break
			}
			
			// Verify row data
			id := values[0].(int32)
			name := values[1].(string)
			value := values[2].(float64)
			flag := values[3].(bool)
			
			if name != fmt.Sprintf("name-%d", id) {
				t.Fatalf("Expected name to be name-%d, got %s", id, name)
			}
			if value != float64(id)*1.5 {
				t.Fatalf("Expected value to be %f, got %f", float64(id)*1.5, value)
			}
			if flag != (id%2 == 0) {
				t.Fatalf("Expected flag to be %v, got %v", id%2 == 0, flag)
			}
			
			rowCount++
		}
		
		if rowCount != 100 {
			t.Fatalf("Expected 100 rows, got %d", rowCount)
		}
	})
	
	// Test batch statement
	t.Run("BatchStatement", func(t *testing.T) {
		// Create a batch statement
		stmt, err := NewBatchStmt(dconn, "SELECT * FROM batch_test WHERE id > ? ORDER BY id LIMIT ?", 25)
		if err != nil {
			t.Fatalf("Failed to create batch statement: %v", err)
		}
		defer stmt.Close()
		
		// Execute with parameters
		rows, err := stmt.QueryBatch(500, 100)
		if err != nil {
			t.Fatalf("Failed to execute batch statement: %v", err)
		}
		defer rows.Close()
		
		// Read rows - we should get all 100 rows
		rowCount := 0
		lastID := 0
		for {
			values := make([]driver.Value, 4)
			err := rows.Next(values)
			if err != nil {
				break
			}
			
			// Verify row data
			id := int(values[0].(int32))
			
			// Check that id is greater than 500 and rows are in order
			if id <= 500 {
				t.Fatalf("Expected id to be > 500, got %d", id)
			}
			if id <= lastID && rowCount > 0 {
				t.Fatalf("Rows not in order: previous id %d, current id %d", lastID, id)
			}
			
			lastID = id
			rowCount++
		}
		
		if rowCount != 100 {
			t.Fatalf("Expected 100 rows, got %d", rowCount)
		}
	})
}

func BenchmarkStandardQuery(b *testing.B) {
	// Get direct connection for dedicated SQL operations 
	conn, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to get direct connection: %v", err)
	}
	defer conn.Close()
	
	// Create table
	_, err = conn.Exec("CREATE TABLE bench_test (id INTEGER, name VARCHAR, value DOUBLE, flag BOOLEAN)", nil)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}
	
	// Prepare insert statement
	insertStmt, err := newStatement(conn, "INSERT INTO bench_test VALUES (?, ?, ?, ?)")
	if err != nil {
		b.Fatalf("Failed to prepare statement: %v", err)
	}
	
	for i := 0; i < 10000; i++ {
		args := []driver.Value{
			i, fmt.Sprintf("name-%d", i), float64(i)*1.5, i%2 == 0,
		}
		_, err = insertStmt.Exec(args)
		if err != nil {
			b.Fatalf("Failed to insert test data: %v", err)
		}
	}
	insertStmt.Close()

	// Standard query benchmark using normal driver query
	b.Run("Standard", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			// Use offset based on iteration to avoid query result caching
			queryStr := fmt.Sprintf("SELECT * FROM bench_test WHERE id > %d ORDER BY id LIMIT 1000", i%9000)
			
			queryRows, err := conn.Query(queryStr, nil)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}
			
			count := 0
			values := make([]driver.Value, 4)
			
			for {
				err := queryRows.Next(values)
				if err != nil {
					break
				}
				count++
			}
			queryRows.Close()
		}
	})
}

func BenchmarkBatchQuery(b *testing.B) {
	// Use memory database
	dbname := ":memory:"
	
	// Get direct connection 
	conn, err := NewConnection(dbname)
	if err != nil {
		b.Fatalf("Failed to get direct connection: %v", err)
	}
	defer conn.Close()
	
	// Create table and insert test data - 10,000 rows
	_, err = conn.Exec("CREATE TABLE bench_test (id INTEGER, name VARCHAR, value DOUBLE, flag BOOLEAN)", nil)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}
	
	// Prepare insert statement
	insertStmt, err := newStatement(conn, "INSERT INTO bench_test VALUES (?, ?, ?, ?)")
	if err != nil {
		b.Fatalf("Failed to prepare statement: %v", err)
	}
	
	for i := 0; i < 10000; i++ {
		args := []driver.Value{
			i, fmt.Sprintf("name-%d", i), float64(i)*1.5, i%2 == 0,
		}
		_, err = insertStmt.Exec(args)
		if err != nil {
			b.Fatalf("Failed to insert test data: %v", err)
		}
	}
	insertStmt.Close()
	
	// Test different batch sizes
	batchSizes := []int{10, 100, 1000}
	
	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize=%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				// Use offset based on iteration to avoid query result caching
				rows, err := conn.QueryBatch(fmt.Sprintf("SELECT * FROM bench_test WHERE id > %d ORDER BY id LIMIT 1000", i%9000), batchSize)
				if err != nil {
					b.Fatalf("Failed to execute batch query: %v", err)
				}
				
				count := 0
				for {
					values := make([]driver.Value, 4)
					err := rows.Next(values)
					if err != nil {
						break
					}
					count++
				}
				rows.Close()
			}
		})
	}
	
	// Benchmark batch prepared statement
	batchStmt, err := NewBatchStmt(conn, "SELECT * FROM bench_test WHERE id > ? ORDER BY id LIMIT 1000", 1000)
	if err != nil {
		b.Fatalf("Failed to create batch statement: %v", err)
	}
	defer batchStmt.Close()
	
	b.Run("BatchPrepared", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		
		for i := 0; i < b.N; i++ {
			// Use offset based on iteration to avoid query result caching
			rows, err := batchStmt.QueryBatch(i % 9000)
			if err != nil {
				b.Fatalf("Failed to execute batch statement: %v", err)
			}
			
			count := 0
			for {
				values := make([]driver.Value, 4)
				err := rows.Next(values)
				if err != nil {
					break
				}
				count++
			}
			rows.Close()
		}
	})
}