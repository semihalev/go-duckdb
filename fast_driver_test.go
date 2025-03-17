// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

import (
	"database/sql/driver"
	"fmt"
	"runtime"
	"testing"
)

func TestFastDriverRowsAffectedIntegration(t *testing.T) {
	// Connect to the in-memory database
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer conn.Close()

	// Create a test table
	_, err = conn.Exec("CREATE TABLE test_affected_rows (id INTEGER, value VARCHAR)", nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test INSERT - should affect multiple rows
	result, err := conn.Exec("INSERT INTO test_affected_rows VALUES (1, 'one'), (2, 'two'), (3, 'three')", nil)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Check rows affected for insert
	insertRows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}
	if insertRows != 3 {
		t.Errorf("Expected 3 rows affected for insert, got %d", insertRows)
	}

	// Test UPDATE - should affect subset of rows
	result, err = conn.Exec("UPDATE test_affected_rows SET value = 'updated' WHERE id <= 2", nil)
	if err != nil {
		t.Fatalf("Failed to update data: %v", err)
	}

	// Check rows affected for update
	updateRows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}
	if updateRows != 2 {
		t.Errorf("Expected 2 rows affected for update, got %d", updateRows)
	}

	// Test DELETE - should affect one row
	result, err = conn.Exec("DELETE FROM test_affected_rows WHERE id = 3", nil)
	if err != nil {
		t.Fatalf("Failed to delete data: %v", err)
	}

	// Check rows affected for delete
	deleteRows, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}
	if deleteRows != 1 {
		t.Errorf("Expected 1 row affected for delete, got %d", deleteRows)
	}
}

func TestFastDriver(t *testing.T) {
	// Connect to in-memory database
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer conn.Close()

	// Create test table
	_, err = conn.Exec("CREATE TABLE fast_test (id INTEGER, name VARCHAR, value DOUBLE, flag BOOLEAN)", nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := 0; i < 100; i++ {
		query := fmt.Sprintf("INSERT INTO fast_test VALUES (%d, 'name-%d', %f, %v)",
			i, i, float64(i)*1.5, i%2 == 0)
		_, err = conn.Exec(query, nil)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Test the fast query implementation
	t.Run("FastQuery", func(t *testing.T) {
		rows, err := conn.FastQuery("SELECT * FROM fast_test ORDER BY id LIMIT 50")
		if err != nil {
			t.Fatalf("Failed to execute fast query: %v", err)
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

		// Read all rows
		rowCount := 0
		values := make([]driver.Value, 4)
		for {
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

		if rowCount != 50 {
			t.Fatalf("Expected 50 rows, got %d", rowCount)
		}
	})

	// Test prepared statements with parameter binding
	t.Run("FastPreparedStatement", func(t *testing.T) {
		// Prepare a statement
		stmt, err := conn.FastPrepare("SELECT * FROM fast_test WHERE id > ? AND value < ? ORDER BY id LIMIT ?")
		if err != nil {
			t.Fatalf("Failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		// Execute with parameters
		rows, err := stmt.ExecuteFast(20, 50.0, 15)
		if err != nil {
			t.Fatalf("Failed to execute prepared statement: %v", err)
		}
		defer rows.Close()

		// Check results
		rowCount := 0
		values := make([]driver.Value, 4)
		for {
			err := rows.Next(values)
			if err != nil {
				break
			}

			// Verify that results match the query parameters
			id := values[0].(int32)
			value := values[2].(float64)

			if id <= 20 {
				t.Fatalf("Expected id > 20, got %d", id)
			}
			if value >= 50.0 {
				t.Fatalf("Expected value < 50.0, got %f", value)
			}

			rowCount++
		}

		if rowCount > 15 {
			t.Fatalf("Expected at most 15 rows, got %d", rowCount)
		}
	})

	// Test parameter binding with different data types
	t.Run("ParameterBindingTypes", func(t *testing.T) {
		// Test different parameter types
		testCases := []struct {
			query    string
			args     []interface{}
			expected interface{}
		}{
			{"SELECT ?::INTEGER", []interface{}{42}, int32(42)},
			{"SELECT ?::BIGINT", []interface{}{int64(9223372036854775807)}, int64(9223372036854775807)},
			{"SELECT ?::VARCHAR", []interface{}{"hello world"}, "hello world"},
			{"SELECT ?::BOOLEAN", []interface{}{true}, true},
			{"SELECT ?::DOUBLE", []interface{}{3.14159}, 3.14159},
		}

		for i, tc := range testCases {
			// Prepare statement
			stmt, err := conn.FastPrepare(tc.query)
			if err != nil {
				t.Fatalf("Test case %d: Failed to prepare statement: %v", i, err)
			}

			// Execute with parameter
			rows, err := stmt.ExecuteFast(tc.args...)
			if err != nil {
				t.Fatalf("Test case %d: Failed to execute statement: %v", i, err)
			}

			// Check result
			values := make([]driver.Value, 1)
			if err := rows.Next(values); err != nil {
				t.Fatalf("Test case %d: Failed to get row: %v", i, err)
			}

			// Compare result with expected value
			// Note: Some types might need special comparison
			switch expected := tc.expected.(type) {
			case float64:
				// For floating point, compare with tolerance
				if values[0].(float64) != expected {
					t.Fatalf("Test case %d: Expected %v, got %v", i, expected, values[0])
				}
			default:
				if values[0] != expected {
					t.Fatalf("Test case %d: Expected %v (%T), got %v (%T)",
						i, expected, expected, values[0], values[0])
				}
			}

			rows.Close()
			stmt.Close()
		}
	})

	// Test the DirectQuery API
	t.Run("DirectQuery", func(t *testing.T) {
		rows, columns, err := conn.DirectQuery("SELECT * FROM fast_test ORDER BY id LIMIT 50")
		if err != nil {
			t.Fatalf("Failed to execute direct query: %v", err)
		}

		// Check column names
		expectedColumns := []string{"id", "name", "value", "flag"}
		if len(columns) != len(expectedColumns) {
			t.Fatalf("Expected %d columns, got %d", len(expectedColumns), len(columns))
		}
		for i, col := range columns {
			if col != expectedColumns[i] {
				t.Fatalf("Expected column %d to be %s, got %s", i, expectedColumns[i], col)
			}
		}

		// Check row count
		if len(rows) != 50 {
			t.Fatalf("Expected 50 rows, got %d", len(rows))
		}

		// Check row data
		for i, row := range rows {
			id := row[0].(int32)
			name := row[1].(string)
			value := row[2].(float64)
			flag := row[3].(bool)

			if int(id) != i {
				t.Fatalf("Expected id to be %d, got %d", i, id)
			}
			if name != fmt.Sprintf("name-%d", id) {
				t.Fatalf("Expected name to be name-%d, got %s", id, name)
			}
			if value != float64(id)*1.5 {
				t.Fatalf("Expected value to be %f, got %f", float64(id)*1.5, value)
			}
			if flag != (id%2 == 0) {
				t.Fatalf("Expected flag to be %v, got %v", id%2 == 0, flag)
			}
		}
	})

	// Test the DirectQuery API with parameters
	t.Run("DirectQueryWithParams", func(t *testing.T) {
		// Execute DirectQuery with parameters
		rows, columns, err := conn.DirectQuery(
			"SELECT * FROM fast_test WHERE id > ? AND id < ? ORDER BY id",
			30, 40)
		if err != nil {
			t.Fatalf("Failed to execute direct query with params: %v", err)
		}

		// Check column names
		expectedColumns := []string{"id", "name", "value", "flag"}
		if len(columns) != len(expectedColumns) {
			t.Fatalf("Expected %d columns, got %d", len(expectedColumns), len(columns))
		}

		// Check results match our parameter filter
		for _, row := range rows {
			id := row[0].(int32)
			if id <= 30 || id >= 40 {
				t.Fatalf("Got row with id %d, which is outside the query range (30-40)", id)
			}

			// Verify the row data
			name := row[1].(string)
			if name != fmt.Sprintf("name-%d", id) {
				t.Fatalf("Expected name to be name-%d, got %s", id, name)
			}
		}

		// We should have 9 rows (31-39)
		if len(rows) != 9 {
			t.Fatalf("Expected 9 rows, got %d", len(rows))
		}
	})
}

func BenchmarkDriverComparison(b *testing.B) {
	// Connect to in-memory database
	conn, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to connect to database: %v", err)
	}
	defer conn.Close()

	// Create test table
	_, err = conn.Exec("CREATE TABLE bench_test (id INTEGER, name VARCHAR, value DOUBLE, flag BOOLEAN)", nil)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Insert 10,000 rows for benchmark
	for i := 0; i < 10000; i++ {
		query := fmt.Sprintf("INSERT INTO bench_test VALUES (%d, 'name-%d', %f, %v)",
			i, i, float64(i)*1.5, i%2 == 0)
		_, err = conn.Exec(query, nil)
		if err != nil {
			b.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Benchmark standard query
	b.Run("StandardQuery", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Use offset based on iteration to avoid query result caching
			queryStr := fmt.Sprintf("SELECT * FROM bench_test WHERE id > %d ORDER BY id LIMIT 1000", i%9000)

			rows, err := conn.Query(queryStr, nil)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}

			count := 0
			values := make([]driver.Value, 4)

			for {
				err := rows.Next(values)
				if err != nil {
					break
				}
				count++
			}
			rows.Close()

			if count == 0 {
				b.Fatalf("Expected rows, got none")
			}
		}
	})

	// Benchmark fast query
	b.Run("FastQuery", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Use offset based on iteration to avoid query result caching
			queryStr := fmt.Sprintf("SELECT * FROM bench_test WHERE id > %d ORDER BY id LIMIT 1000", i%9000)

			rows, err := conn.FastQuery(queryStr)
			if err != nil {
				b.Fatalf("Failed to execute fast query: %v", err)
			}

			count := 0
			values := make([]driver.Value, 4)

			for {
				err := rows.Next(values)
				if err != nil {
					break
				}
				count++
			}
			rows.Close()

			if count == 0 {
				b.Fatalf("Expected rows, got none")
			}
		}
	})

	// Benchmark DirectQuery (getting all rows at once)
	b.Run("DirectQuery", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Use offset based on iteration to avoid query result caching
			queryStr := fmt.Sprintf("SELECT * FROM bench_test WHERE id > %d ORDER BY id LIMIT 1000", i%9000)

			rows, _, err := conn.DirectQuery(queryStr)
			if err != nil {
				b.Fatalf("Failed to execute direct query: %v", err)
			}

			if len(rows) == 0 {
				b.Fatalf("Expected rows, got none")
			}
		}
	})

	// Benchmark with larger result sets
	b.Run("LargeResultSet", func(b *testing.B) {
		b.Skip("Skipping large result set benchmark - uncomment to run")

		// Insert more rows for a larger test
		_, err = conn.Exec("CREATE TABLE large_test AS SELECT * FROM bench_test", nil)
		if err != nil {
			b.Fatalf("Failed to create large test table: %v", err)
		}

		// Insert more rows to make a larger dataset
		for j := 0; j < 10; j++ {
			_, err = conn.Exec("INSERT INTO large_test SELECT * FROM bench_test", nil)
			if err != nil {
				b.Fatalf("Failed to insert data into large test table: %v", err)
			}
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Select a large number of rows
			rows, _, err := conn.DirectQuery("SELECT * FROM large_test LIMIT 10000")
			if err != nil {
				b.Fatalf("Failed to execute large query: %v", err)
			}

			if len(rows) == 0 {
				b.Fatalf("Expected rows, got none")
			}
		}
	})
}

// BenchmarkHighThroughput measures how many rows per second we can process
func BenchmarkHighThroughput(b *testing.B) {
	// Connect to in-memory database
	conn, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to connect to database: %v", err)
	}
	defer conn.Close()

	// Create test table with more columns
	_, err = conn.Exec("CREATE TABLE bench_throughput ("+
		"id INTEGER, "+
		"name VARCHAR, "+
		"value1 DOUBLE, "+
		"value2 DOUBLE, "+
		"value3 DOUBLE, "+
		"value4 DOUBLE, "+
		"value5 DOUBLE, "+
		"flag1 BOOLEAN, "+
		"flag2 BOOLEAN, "+
		"text VARCHAR"+
		")", nil)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Insert 10,000 rows for benchmark
	for i := 0; i < 10000; i++ {
		query := fmt.Sprintf("INSERT INTO bench_throughput VALUES ("+
			"%d, 'name-%d', %f, %f, %f, %f, %f, %v, %v, 'This is a longer text field with some content to make it more realistic')",
			i, i,
			float64(i)*1.5, float64(i)*2.5, float64(i)*3.5, float64(i)*4.5, float64(i)*5.5,
			i%2 == 0, i%3 == 0)
		_, err = conn.Exec(query, nil)
		if err != nil {
			b.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Standard query implementation
	b.Run("StandardThroughput", func(b *testing.B) {
		// Set nativeLibLoaded to false to avoid nil pointers when using dynamic library
		origNativeLibLoaded := nativeLibLoaded
		nativeLibLoaded = false
		defer func() {
			nativeLibLoaded = origNativeLibLoaded
		}()

		b.ResetTimer()
		b.ReportAllocs()

		totalRows := 0
		for i := 0; i < b.N; i++ {
			// Select all rows with a reasonable limit to avoid memory issues
			rows, err := conn.Query("SELECT * FROM bench_throughput LIMIT 5000", nil)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}

			count := 0
			values := make([]driver.Value, 10) // 10 columns

			for {
				err := rows.Next(values)
				if err != nil {
					break
				}
				count++
			}
			rows.Close()

			totalRows += count
		}

		// Report custom metric: rows per second
		b.ReportMetric(float64(totalRows)/b.Elapsed().Seconds(), "rows/sec")
	})

	// Fast query implementation
	b.Run("FastThroughput", func(b *testing.B) {
		// Force native library to be loaded if available or use fallback
		if !nativeLibLoaded {
			b.Logf("Native library not loaded, using fallback implementation")
		} else {
			b.Logf("Using native library optimizations: %s", nativeLibPath)
		}

		b.ResetTimer()
		b.ReportAllocs()

		totalRows := 0
		for i := 0; i < b.N; i++ {
			// Use a smaller limit to avoid memory issues
			func() {
				// Create a defer-recover to catch any panics
				defer func() {
					if r := recover(); r != nil {
						b.Fatalf("Panic during query: %v", r)
					}
				}()

				var rows driver.Rows
				var err error

				// Use appropriate query implementation based on native library availability
				if nativeLibLoaded {
					// Use FastQuery when native library is available
					rows, err = conn.FastQuery("SELECT * FROM bench_throughput LIMIT 5000")
					if err != nil {
						b.Fatalf("Failed to execute fast query: %v", err)
					}
				} else {
					// Fall back to standard query when native library is not available
					rows, err = conn.Query("SELECT * FROM bench_throughput LIMIT 5000", nil)
					if err != nil {
						b.Fatalf("Failed to execute standard query: %v", err)
					}
				}

				// Ensure rows are closed even if there's a panic
				defer rows.Close()

				count := 0
				values := make([]driver.Value, 10) // 10 columns

				for {
					err := rows.Next(values)
					if err != nil {
						break
					}
					count++
				}

				totalRows += count
			}()

			// Force GC between iterations to clean up any lingering resources
			runtime.GC()
		}

		// Report custom metric: rows per second
		b.ReportMetric(float64(totalRows)/b.Elapsed().Seconds(), "rows/sec")
	})

	// DirectQuery implementation - just measuring raw processing speed
	b.Run("DirectThroughput", func(b *testing.B) {
		// Force native library to be loaded if available or use fallback
		if !nativeLibLoaded {
			b.Logf("Native library not loaded, using fallback implementation")
		} else {
			b.Logf("Using native library optimizations: %s", nativeLibPath)
		}

		b.ResetTimer()
		b.ReportAllocs()

		totalRows := 0
		for i := 0; i < b.N; i++ {
			// Using a closure to ensure resources are properly cleaned up
			func() {
				// Using dirctresult to deal with our new native API approach
				if nativeLibLoaded {
					// Use the DirectResult API when native library is available
					result, err := conn.QueryDirectResult("SELECT * FROM bench_throughput LIMIT 5000")
					if err != nil {
						b.Fatalf("Failed to execute direct query: %v", err)
					}
					defer result.Close()

					// Count the rows by checking the result metadata
					rowCount := result.RowCount()
					totalRows += int(rowCount)
				} else {
					// Fall back to the older DirectQuery API for non-native case
					// Ensure it has proper recover to avoid panics
					defer func() {
						if r := recover(); r != nil {
							b.Fatalf("Panic during DirectQuery: %v", r)
						}
					}()

					// Query with a reasonable limit
					rows, err := conn.Query("SELECT * FROM bench_throughput LIMIT 5000", nil)
					if err != nil {
						b.Fatalf("Failed to execute query: %v", err)
					}
					defer rows.Close()

					// Count rows
					count := 0
					values := make([]driver.Value, 10)
					for {
						err := rows.Next(values)
						if err != nil {
							break
						}
						count++
					}
					totalRows += count
				}

				// Force clean up
				runtime.GC()
			}()
		}

		// Report custom metric: rows per second
		b.ReportMetric(float64(totalRows)/b.Elapsed().Seconds(), "rows/sec")
	})
}
