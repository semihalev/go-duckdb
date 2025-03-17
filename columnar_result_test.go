package duckdb

import (
	"database/sql"
	"testing"
	"time"
)

func TestColumnarResult(t *testing.T) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create test table
	_, err = conn.ExecDirect(`
		CREATE TABLE columnar_test (
			id INTEGER, 
			name VARCHAR, 
			value DOUBLE, 
			flag BOOLEAN,
			created_at TIMESTAMP,
			birth_date DATE
		);
		
		INSERT INTO columnar_test 
		SELECT 
			i AS id, 
			'name_' || i AS name, 
			i * 1.5 AS value,
			i % 2 = 0 AS flag,
			TIMESTAMP '2022-01-01 12:00:00' + INTERVAL (i*10) MINUTE AS created_at,
			DATE '1990-01-01' + INTERVAL (i) DAY AS birth_date
		FROM range(1, 101) t(i);
	`)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Execute query and get columnar result
	result, err := conn.QueryColumnar("SELECT * FROM columnar_test ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to execute columnar query: %v", err)
	}

	// Verify metadata
	if result.RowCount != 100 {
		t.Errorf("Expected 100 rows, got %d", result.RowCount)
	}

	if result.ColumnCount != 6 {
		t.Errorf("Expected 6 columns, got %d", result.ColumnCount)
	}

	expectedNames := []string{"id", "name", "value", "flag", "created_at", "birth_date"}
	for i, name := range expectedNames {
		if result.ColumnNames[i] != name {
			t.Errorf("Expected column %d to be %s, got %s", i, name, result.ColumnNames[i])
		}
	}

	// Test numeric columns
	ids, nulls, err := result.GetInt32Column(0)
	if err != nil {
		t.Fatalf("Failed to get integer column: %v", err)
	}

	if len(ids) != 100 || len(nulls) != 100 {
		t.Errorf("Expected 100 values, got %d values and %d nulls", len(ids), len(nulls))
	}

	if ids[0] != 1 || ids[99] != 100 {
		t.Errorf("Unexpected integer values: first=%d, last=%d", ids[0], ids[99])
	}

	// Test timestamp column
	timestamps, nulls, err := result.GetTimestampColumn(4)
	if err != nil {
		t.Fatalf("Failed to get timestamp column: %v", err)
	}

	if len(timestamps) != 100 || len(nulls) != 100 {
		t.Errorf("Expected 100 timestamps, got %d values and %d nulls", len(timestamps), len(nulls))
	}

	// Verify first and last timestamp are in ascending order
	if timestamps[0].After(timestamps[99]) {
		t.Errorf("Timestamps not in ascending order: first=%v, last=%v",
			timestamps[0], timestamps[99])
	}

	// Test date column
	dates, nulls, err := result.GetDateColumn(5)
	if err != nil {
		t.Fatalf("Failed to get date column: %v", err)
	}

	if len(dates) != 100 || len(nulls) != 100 {
		t.Errorf("Expected 100 dates, got %d values and %d nulls", len(dates), len(nulls))
	}

	// Verify first and last date are in ascending order
	if dates[0].After(dates[99]) {
		t.Errorf("Dates not in ascending order: first=%v, last=%v",
			dates[0], dates[99])
	}
}

func BenchmarkColumnarVsTraditional(b *testing.B) {
	// Connect to in-memory database
	conn, err := NewConnection(":memory:")
	if err != nil {
		b.Fatalf("Failed to connect to DuckDB: %v", err)
	}
	defer conn.Close()

	// Also create a standard sql.DB connection for comparison
	sqlDB, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer sqlDB.Close()

	// Create a test table with various data types and 100,000 rows
	_, err = conn.ExecDirect(`
		CREATE TABLE columnar_bench (
			id INTEGER, 
			name VARCHAR, 
			value DOUBLE, 
			flag BOOLEAN,
			created_at TIMESTAMP,
			birth_date DATE
		);
		
		INSERT INTO columnar_bench 
		SELECT 
			i AS id, 
			'name_' || (i % 1000) AS name, 
			i * 1.5 AS value,
			i % 2 = 0 AS flag,
			TIMESTAMP '2022-01-01 12:00:00' + INTERVAL (i % 1000) HOUR AS created_at,
			DATE '1990-01-01' + INTERVAL (i % 10000) DAY AS birth_date
		FROM range(0, 100000) t(i);
	`)
	if err != nil {
		b.Fatalf("Failed to create test data: %v", err)
	}

	// Copy the same data to the sql.DB connection
	_, err = sqlDB.Exec(`
		CREATE TABLE columnar_bench (
			id INTEGER, 
			name VARCHAR, 
			value DOUBLE, 
			flag BOOLEAN,
			created_at TIMESTAMP,
			birth_date DATE
		);
		
		INSERT INTO columnar_bench 
		SELECT 
			i AS id, 
			'name_' || (i % 1000) AS name, 
			i * 1.5 AS value,
			i % 2 = 0 AS flag,
			TIMESTAMP '2022-01-01 12:00:00' + INTERVAL (i % 1000) HOUR AS created_at,
			DATE '1990-01-01' + INTERVAL (i % 10000) DAY AS birth_date
		FROM range(0, 100000) t(i);
	`)
	if err != nil {
		b.Fatalf("Failed to create test data in sql.DB: %v", err)
	}

	// Benchmark traditional row-by-row processing
	b.Run("TraditionalRows", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			rows, err := sqlDB.Query("SELECT * FROM columnar_bench LIMIT 10000")
			if err != nil {
				b.Fatalf("Failed to query: %v", err)
			}

			var (
				id        int
				name      string
				value     float64
				flag      bool
				createdAt time.Time
				birthDate time.Time
				count     int
			)

			for rows.Next() {
				err := rows.Scan(&id, &name, &value, &flag, &createdAt, &birthDate)
				if err != nil {
					b.Fatalf("Failed to scan row: %v", err)
				}
				count++
			}
			rows.Close()

			if count != 10000 {
				b.Fatalf("Expected 10000 rows, got %d", count)
			}
		}
	})

	// Benchmark our new columnar extraction
	b.Run("ColumnarExtraction", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			result, err := conn.QueryColumnar("SELECT * FROM columnar_bench LIMIT 10000")
			if err != nil {
				b.Fatalf("Failed to query columnar: %v", err)
			}

			// Access the columns to make sure we're doing equivalent work
			ids, _, err := result.GetInt32Column(0)
			if err != nil {
				b.Fatalf("Failed to get integer column: %v", err)
			}

			values, _, err := result.GetFloat64Column(2)
			if err != nil {
				b.Fatalf("Failed to get float column: %v", err)
			}

			flags, _, err := result.GetBoolColumn(3)
			if err != nil {
				b.Fatalf("Failed to get boolean column: %v", err)
			}

			timestamps, _, err := result.GetTimestampColumn(4)
			if err != nil {
				b.Fatalf("Failed to get timestamp column: %v", err)
			}

			dates, _, err := result.GetDateColumn(5)
			if err != nil {
				b.Fatalf("Failed to get date column: %v", err)
			}

			if len(ids) != 10000 || len(values) != 10000 || len(flags) != 10000 ||
				len(timestamps) != 10000 || len(dates) != 10000 {
				b.Fatalf("Did not get expected 10000 values for all columns")
			}
		}
	})

	// Benchmark our direct result extraction for comparison
	b.Run("DirectResult", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			result, err := conn.QueryDirectResult("SELECT * FROM columnar_bench LIMIT 10000")
			if err != nil {
				b.Fatalf("Failed to query: %v", err)
			}

			// Extract the columns to make sure we're doing equivalent work
			ids, _, err := result.ExtractInt32Column(0)
			if err != nil {
				b.Fatalf("Failed to extract int32 column: %v", err)
			}

			values, _, err := result.ExtractFloat64Column(2)
			if err != nil {
				b.Fatalf("Failed to extract float64 column: %v", err)
			}

			flags, _, err := result.ExtractBoolColumn(3)
			if err != nil {
				b.Fatalf("Failed to extract bool column: %v", err)
			}

			timestamps, _, err := result.ExtractTimestampColumn(4)
			if err != nil {
				b.Fatalf("Failed to extract timestamp column: %v", err)
			}

			dates, _, err := result.ExtractDateColumn(5)
			if err != nil {
				b.Fatalf("Failed to extract date column: %v", err)
			}

			result.Close()

			if len(ids) != 10000 || len(values) != 10000 || len(flags) != 10000 ||
				len(timestamps) != 10000 || len(dates) != 10000 {
				b.Fatalf("Did not get expected 10000 values for all columns")
			}
		}
	})
}
