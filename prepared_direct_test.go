package duckdb

import (
	"database/sql"
	"testing"
	"time"
)

func TestPreparedDirect(t *testing.T) {
	conn, err := NewConnection(":memory:")
	if err != nil {
		t.Fatalf("Failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create test table
	_, err = conn.ExecDirect(`
		CREATE TABLE prepared_test (
			id INTEGER, 
			name VARCHAR, 
			value DOUBLE, 
			flag BOOLEAN,
			created_at TIMESTAMP
		);
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Test insert with prepared statement
	stmt, err := conn.PrepareDirectResult("INSERT INTO prepared_test VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Verify parameter count
	paramCount := stmt.ParameterCount()
	if paramCount != 5 {
		t.Errorf("Expected 5 parameters, got %d", paramCount)
	}

	// Insert test data
	now := time.Now()
	err = stmt.Bind(1, "Test 1", 1.23, true, now)
	if err != nil {
		t.Fatalf("Failed to bind parameters: %v", err)
	}

	result, err := stmt.Execute()
	if err != nil {
		t.Fatalf("Failed to execute prepared statement: %v", err)
	}
	result.Close()

	// Insert another row
	err = stmt.Bind(2, "Test 2", 4.56, false, now.Add(time.Hour))
	if err != nil {
		t.Fatalf("Failed to bind parameters for second row: %v", err)
	}

	result, err = stmt.Execute()
	if err != nil {
		t.Fatalf("Failed to execute prepared statement for second row: %v", err)
	}
	result.Close()

	// Query the data with prepared statement
	queryStmt, err := conn.PrepareDirectResult("SELECT * FROM prepared_test WHERE id = ?")
	if err != nil {
		t.Fatalf("Failed to prepare query statement: %v", err)
	}
	defer queryStmt.Close()

	// Query first row
	err = queryStmt.Bind(1)
	if err != nil {
		t.Fatalf("Failed to bind parameter for query: %v", err)
	}

	result, err = queryStmt.Execute()
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer result.Close()

	// Verify row count
	if result.RowCount() != 1 {
		t.Errorf("Expected 1 row, got %d", result.RowCount())
	}

	// Extract data
	ids, _, err := result.ExtractInt32Column(0)
	if err != nil {
		t.Fatalf("Failed to extract id column: %v", err)
	}
	if ids[0] != 1 {
		t.Errorf("Expected id=1, got %d", ids[0])
	}

	names, _, err := result.ExtractStringColumn(1)
	if err != nil {
		t.Fatalf("Failed to extract name column: %v", err)
	}
	if names[0] != "Test 1" {
		t.Errorf("Expected name='Test 1', got '%s'", names[0])
	}

	// Test columnar result
	err = queryStmt.Bind(2)
	if err != nil {
		t.Fatalf("Failed to bind parameter for second query: %v", err)
	}

	columnarResult, err := queryStmt.ExecuteColumnar()
	if err != nil {
		t.Fatalf("Failed to execute columnar query: %v", err)
	}

	// Verify columnar result
	if columnarResult.RowCount != 1 {
		t.Errorf("Expected 1 row in columnar result, got %d", columnarResult.RowCount)
	}

	// Check id
	colIdx, err := columnarResult.GetColumnByName("id")
	if err != nil {
		t.Fatalf("Failed to get column index: %v", err)
	}

	colIds, _, err := columnarResult.GetInt32Column(colIdx)
	if err != nil {
		t.Fatalf("Failed to get int32 column: %v", err)
	}

	if colIds[0] != 2 {
		t.Errorf("Expected id=2 in columnar result, got %d", colIds[0])
	}
}

func BenchmarkPreparedDirect(b *testing.B) {
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

	// Create test tables
	_, err = conn.ExecDirect(`
		CREATE TABLE prepared_bench (
			id INTEGER, 
			name VARCHAR, 
			value DOUBLE,
			created_at TIMESTAMP
		);
	`)
	if err != nil {
		b.Fatalf("Failed to create test table: %v", err)
	}

	_, err = sqlDB.Exec(`
		CREATE TABLE prepared_bench (
			id INTEGER, 
			name VARCHAR, 
			value DOUBLE,
			created_at TIMESTAMP
		);
	`)
	if err != nil {
		b.Fatalf("Failed to create test table in sql.DB: %v", err)
	}

	// Insert 10,000 rows for testing
	insertStmt, err := conn.PrepareDirectResult("INSERT INTO prepared_bench VALUES (?, ?, ?, ?)")
	if err != nil {
		b.Fatalf("Failed to prepare insert statement: %v", err)
	}

	stdInsertStmt, err := sqlDB.Prepare("INSERT INTO prepared_bench VALUES (?, ?, ?, ?)")
	if err != nil {
		b.Fatalf("Failed to prepare standard insert statement: %v", err)
	}

	// Insert data into both test tables
	now := time.Now()
	for i := 0; i < 10000; i++ {
		// For the native connection
		err = insertStmt.Bind(i, "name_"+string(rune(i%26+65)), float64(i)*1.5, now.Add(time.Duration(i)*time.Minute))
		if err != nil {
			b.Fatalf("Failed to bind parameters: %v", err)
		}
		result, err := insertStmt.Execute()
		if err != nil {
			b.Fatalf("Failed to execute insert: %v", err)
		}
		result.Close()

		// For the standard connection - convert timestamp to string for compatibility
		ts := now.Add(time.Duration(i) * time.Minute).Format("2006-01-02 15:04:05")
		_, err = stdInsertStmt.Exec(i, "name_"+string(rune(i%26+65)), float64(i)*1.5, ts)
		if err != nil {
			b.Fatalf("Failed to execute standard insert: %v", err)
		}
	}

	insertStmt.Close()
	stdInsertStmt.Close()

	// Benchmark standard prepared statement
	b.Run("StandardPrepared", func(b *testing.B) {
		stmt, err := sqlDB.Prepare("SELECT * FROM prepared_bench WHERE id > ? AND id < ? LIMIT 1000")
		if err != nil {
			b.Fatalf("Failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Vary the parameters a bit
			lowerBound := i % 8000
			upperBound := lowerBound + 2000

			rows, err := stmt.Query(lowerBound, upperBound)
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}

			var count int
			var id int
			var name string
			var value float64
			var createdAt time.Time

			// Process results row by row
			for rows.Next() {
				err := rows.Scan(&id, &name, &value, &createdAt)
				if err != nil {
					b.Fatalf("Failed to scan row: %v", err)
				}
				count++
			}
			rows.Close()

			if err := rows.Err(); err != nil {
				b.Fatalf("Error during iteration: %v", err)
			}

			if count == 0 {
				b.Fatalf("No rows returned")
			}
		}
	})

	// Benchmark direct prepared statement with native Result
	b.Run("DirectPrepared", func(b *testing.B) {
		stmt, err := conn.PrepareDirectResult("SELECT * FROM prepared_bench WHERE id > ? AND id < ? LIMIT 1000")
		if err != nil {
			b.Fatalf("Failed to prepare direct statement: %v", err)
		}
		defer stmt.Close()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Vary the parameters a bit
			lowerBound := i % 8000
			upperBound := lowerBound + 2000

			err = stmt.Bind(lowerBound, upperBound)
			if err != nil {
				b.Fatalf("Failed to bind parameters: %v", err)
			}

			result, err := stmt.Execute()
			if err != nil {
				b.Fatalf("Failed to execute query: %v", err)
			}

			// Extract columns directly
			ids, _, err := result.ExtractInt32Column(0)
			if err != nil {
				b.Fatalf("Failed to extract ID column: %v", err)
			}

			values, _, err := result.ExtractFloat64Column(2)
			if err != nil {
				b.Fatalf("Failed to extract Value column: %v", err)
			}

			result.Close()

			if len(ids) == 0 || len(values) == 0 {
				b.Fatalf("No data extracted")
			}
		}
	})

	// Benchmark direct prepared statement with columnar result
	b.Run("ColumnarPrepared", func(b *testing.B) {
		stmt, err := conn.PrepareDirectResult("SELECT * FROM prepared_bench WHERE id > ? AND id < ? LIMIT 1000")
		if err != nil {
			b.Fatalf("Failed to prepare direct statement: %v", err)
		}
		defer stmt.Close()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Vary the parameters a bit
			lowerBound := i % 8000
			upperBound := lowerBound + 2000

			err = stmt.Bind(lowerBound, upperBound)
			if err != nil {
				b.Fatalf("Failed to bind parameters: %v", err)
			}

			result, err := stmt.ExecuteColumnar()
			if err != nil {
				b.Fatalf("Failed to execute columnar query: %v", err)
			}

			// Access columns directly
			ids, _, err := result.GetInt32Column(0)
			if err != nil {
				b.Fatalf("Failed to get ID column: %v", err)
			}

			values, _, err := result.GetFloat64Column(2)
			if err != nil {
				b.Fatalf("Failed to get Value column: %v", err)
			}

			if len(ids) == 0 || len(values) == 0 {
				b.Fatalf("No data extracted")
			}
		}
	})

	// Create a table with multiple columns of the same type for batch testing
	_, err = conn.ExecDirect(`
		CREATE TABLE prepared_batch (
			id1 INTEGER,
			id2 INTEGER,
			id3 INTEGER,
			num1 DOUBLE,
			num2 DOUBLE,
			num3 DOUBLE
		);
	`)
	if err != nil {
		b.Fatalf("Failed to create batch test table: %v", err)
	}

	// Insert test data
	_, err = conn.ExecDirect(`
		INSERT INTO prepared_batch
		SELECT 
			i AS id1,
			i + 1 AS id2,
			i + 2 AS id3,
			i * 1.1 AS num1,
			i * 2.2 AS num2,
			i * 3.3 AS num3
		FROM range(0, 10000) t(i);
	`)
	if err != nil {
		b.Fatalf("Failed to insert batch test data: %v", err)
	}

	// Benchmark prepared statement with batch extraction for analytical workloads
	b.Run("PreparedBatchExtraction", func(b *testing.B) {
		// Prepare a statement that returns multiple columns of the same type
		stmt, err := conn.PrepareDirectResult("SELECT id1, id2, id3, num1, num2, num3 FROM prepared_batch WHERE id1 > ? AND id1 < ? LIMIT 1000")
		if err != nil {
			b.Fatalf("Failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Vary the parameters a bit
			lowerBound := i % 8000
			upperBound := lowerBound + 2000

			err = stmt.Bind(lowerBound, upperBound)
			if err != nil {
				b.Fatalf("Failed to bind parameters: %v", err)
			}

			directResult, err := stmt.Execute()
			if err != nil {
				b.Fatalf("Failed to execute prepared statement: %v", err)
			}

			// Extract integer columns in batch
			intCols, intNulls, err := directResult.ExtractInt32ColumnsBatch([]int{0, 1, 2})
			if err != nil {
				b.Fatalf("Failed to batch extract int columns: %v", err)
			}

			// Extract float columns in batch
			floatCols, floatNulls, err := directResult.ExtractFloat64ColumnsBatch([]int{3, 4, 5})
			if err != nil {
				b.Fatalf("Failed to batch extract float columns: %v", err)
			}

			// Perform a simple analytics computation (weighted sum)
			var weightedSum float64
			for row := 0; row < len(intCols[0]); row++ {
				if !intNulls[0][row] && !floatNulls[0][row] {
					weightedSum += float64(intCols[0][row]) * floatCols[0][row]
				}
			}

			directResult.Close()

			if weightedSum == 0 || len(intCols[0]) == 0 || len(floatCols[0]) == 0 {
				b.Fatalf("Invalid results from batch extraction")
			}
		}
	})

	// Benchmark comparison of columnar vs batch extraction for prepared statements
	b.Run("ColumnarVsBatchAnalytics", func(b *testing.B) {
		// Standard columnar approach
		b.Run("StandardColumnar", func(b *testing.B) {
			stmt, err := conn.PrepareDirectResult("SELECT id1, id2, id3, num1, num2, num3 FROM prepared_batch WHERE id1 > ? AND id1 < ? LIMIT 1000")
			if err != nil {
				b.Fatalf("Failed to prepare statement: %v", err)
			}
			defer stmt.Close()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				lowerBound := i % 8000
				upperBound := lowerBound + 2000

				err = stmt.Bind(lowerBound, upperBound)
				if err != nil {
					b.Fatalf("Failed to bind parameters: %v", err)
				}

				result, err := stmt.ExecuteColumnar()
				if err != nil {
					b.Fatalf("Failed to execute columnar query: %v", err)
				}

				// Get each column separately through standard columnar API
				id1, _, err := result.GetInt32Column(0)
				if err != nil {
					b.Fatalf("Failed to get id1: %v", err)
				}

				id2, _, err := result.GetInt32Column(1)
				if err != nil {
					b.Fatalf("Failed to get id2: %v", err)
				}

				id3, _, err := result.GetInt32Column(2)
				if err != nil {
					b.Fatalf("Failed to get id3: %v", err)
				}

				num1, _, err := result.GetFloat64Column(3)
				if err != nil {
					b.Fatalf("Failed to get num1: %v", err)
				}

				num2, _, err := result.GetFloat64Column(4)
				if err != nil {
					b.Fatalf("Failed to get num2: %v", err)
				}

				num3, _, err := result.GetFloat64Column(5)
				if err != nil {
					b.Fatalf("Failed to get num3: %v", err)
				}

				// Analytical calculation
				var sum1, sum2, sum3 float64
				for j := 0; j < len(id1); j++ {
					sum1 += float64(id1[j]) * num1[j]
					sum2 += float64(id2[j]) * num2[j]
					sum3 += float64(id3[j]) * num3[j]
				}

				if sum1 == 0 || sum2 == 0 || sum3 == 0 {
					b.Fatalf("Invalid results from computation")
				}
			}
		})

		// Direct batch extraction approach
		b.Run("OptimizedBatch", func(b *testing.B) {
			stmt, err := conn.PrepareDirectResult("SELECT id1, id2, id3, num1, num2, num3 FROM prepared_batch WHERE id1 > ? AND id1 < ? LIMIT 1000")
			if err != nil {
				b.Fatalf("Failed to prepare statement: %v", err)
			}
			defer stmt.Close()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				lowerBound := i % 8000
				upperBound := lowerBound + 2000

				err = stmt.Bind(lowerBound, upperBound)
				if err != nil {
					b.Fatalf("Failed to bind parameters: %v", err)
				}

				directResult, err := stmt.Execute()
				if err != nil {
					b.Fatalf("Failed to execute direct query: %v", err)
				}

				// Extract all integer columns in a single batch operation
				idCols, idNulls, err := directResult.ExtractInt32ColumnsBatch([]int{0, 1, 2})
				if err != nil {
					b.Fatalf("Failed to batch extract int columns: %v", err)
				}

				// Extract all float columns in a single batch operation
				numCols, numNulls, err := directResult.ExtractFloat64ColumnsBatch([]int{3, 4, 5})
				if err != nil {
					b.Fatalf("Failed to batch extract float columns: %v", err)
				}

				// Analytical calculation
				var sum1, sum2, sum3 float64
				for j := 0; j < len(idCols[0]); j++ {
					if !idNulls[0][j] && !numNulls[0][j] {
						sum1 += float64(idCols[0][j]) * numCols[0][j]
						sum2 += float64(idCols[1][j]) * numCols[1][j]
						sum3 += float64(idCols[2][j]) * numCols[2][j]
					}
				}

				directResult.Close()

				if sum1 == 0 || sum2 == 0 || sum3 == 0 {
					b.Fatalf("Invalid results from computation")
				}
			}
		})
	})
}
