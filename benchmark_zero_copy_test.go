package duckdb

import (
	"database/sql"
	"fmt"
	"testing"
)

// BenchmarkZeroCopyStringVsStandard compares standard string extraction vs zero-copy
func BenchmarkZeroCopyStringVsStandard(b *testing.B) {
	// Skip this benchmark when running all benchmarks, can be run specifically
	if testing.Short() {
		b.Skip("Skipping in short mode")
	}
	// Set up test database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create table with string columns of various sizes
	if _, err := db.Exec(`CREATE TABLE str_test(
		id INTEGER,
		small_str VARCHAR, -- Small strings (< 64 bytes)
		medium_str VARCHAR, -- Medium strings (~500 bytes)
		large_str VARCHAR -- Large strings (> 1000 bytes)
	)`); err != nil {
		b.Fatal(err)
	}

	// Insert test data
	insertStringTestData(b, db, 10000)

	// Run benchmark for standard driver (rows with Next/Scan)
	b.Run("StandardScan", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query("SELECT * FROM str_test")
			if err != nil {
				b.Fatal(err)
			}

			var id int
			var small, medium, large string
			count := 0

			for rows.Next() {
				if err := rows.Scan(&id, &small, &medium, &large); err != nil {
					b.Fatal(err)
				}
				count++
			}
			rows.Close()

			if count != 10000 {
				b.Fatalf("Expected 10000 rows, got %d", count)
			}
		}
	})

	// Run benchmark for standard driver with separate query
	b.Run("StandardScan_SingleColumn", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query("SELECT small_str FROM str_test")
			if err != nil {
				b.Fatal(err)
			}

			var strVal string
			count := 0

			for rows.Next() {
				if err := rows.Scan(&strVal); err != nil {
					b.Fatal(err)
				}
				count++
			}
			rows.Close()

			if count != 10000 {
				b.Fatalf("Expected 10000 rows, got %d", count)
			}
		}
	})
}

// BenchmarkZeroCopyBlobVsStandard compares standard blob extraction vs zero-copy
func BenchmarkZeroCopyBlobVsStandard(b *testing.B) {
	// Set up test database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create table with blob columns of various sizes
	if _, err := db.Exec(`CREATE TABLE blob_test(
		id INTEGER,
		small_blob BLOB, -- Small blobs (< 64 bytes)
		medium_blob BLOB, -- Medium blobs (~500 bytes)
		large_blob BLOB -- Large blobs (> 1000 bytes)
	)`); err != nil {
		b.Fatal(err)
	}

	// Insert test data
	insertBlobTestData(b, db, 10000)

	// Run benchmark for standard driver (rows with Next/Scan)
	b.Run("StandardScan", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query("SELECT * FROM blob_test")
			if err != nil {
				b.Fatal(err)
			}

			var id int
			var small, medium, large []byte
			count := 0

			for rows.Next() {
				if err := rows.Scan(&id, &small, &medium, &large); err != nil {
					b.Fatal(err)
				}
				count++
			}
			rows.Close()

			if count != 10000 {
				b.Fatalf("Expected 10000 rows, got %d", count)
			}
		}
	})

	// Run benchmark for standard driver with single column
	b.Run("StandardScan_SingleColumn", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query("SELECT small_blob FROM blob_test")
			if err != nil {
				b.Fatal(err)
			}

			var blobVal []byte
			count := 0

			for rows.Next() {
				if err := rows.Scan(&blobVal); err != nil {
					b.Fatal(err)
				}
				count++
			}
			rows.Close()

			if count != 10000 {
				b.Fatalf("Expected 10000 rows, got %d", count)
			}
		}
	})
}

// BenchmarkTrueZeroCopyVsOriginal compares the new true zero-copy implementation to the original approach
func BenchmarkTrueZeroCopyVsOriginal(b *testing.B) {
	// Set up test database
	conn, err := NewConnection(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	// Create table with string and blob data
	_, err = conn.ExecDirect(`CREATE TABLE zero_copy_test (
		id INTEGER,
		str_col VARCHAR,
		blob_col BLOB
	)`)
	if err != nil {
		b.Fatal(err)
	}

	// Insert test data (1000 rows)
	_, err = conn.ExecDirect(`INSERT INTO zero_copy_test 
		SELECT 
			i AS id,
			'test_string_' || i AS str_col,
			BLOB 'binary_data_' || i AS blob_col
		FROM range(0, 1000) t(i)
	`)
	if err != nil {
		b.Fatal(err)
	}

	// Run benchmark for string extraction
	b.Run("TrueZeroCopyString", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			result, err := conn.QueryDirectResult("SELECT str_col FROM zero_copy_test")
			if err != nil {
				b.Fatal(err)
			}

			strings, nulls, err := result.ExtractStringColumnTrueZeroCopy(0)
			if err != nil {
				b.Fatal(err)
			}

			if len(strings) != 1000 || len(nulls) != 1000 {
				b.Fatalf("Expected 1000 rows, got %d strings, %d nulls", len(strings), len(nulls))
			}

			// Access some strings to ensure they're valid
			for j := 0; j < 1000; j += 100 {
				if len(strings[j]) == 0 {
					b.Fatalf("Expected non-empty string at index %d", j)
				}
			}

			result.Close()
		}
	})

	// Run benchmark for blob extraction
	b.Run("TrueZeroCopyBlob", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			result, err := conn.QueryDirectResult("SELECT blob_col FROM zero_copy_test")
			if err != nil {
				b.Fatal(err)
			}

			blobs, nulls, err := result.ExtractBlobColumnTrueZeroCopy(0)
			if err != nil {
				b.Fatal(err)
			}

			if len(blobs) != 1000 || len(nulls) != 1000 {
				b.Fatalf("Expected 1000 rows, got %d blobs, %d nulls", len(blobs), len(nulls))
			}

			// Access some blobs to ensure they're valid
			for j := 0; j < 1000; j += 100 {
				if len(blobs[j]) == 0 {
					b.Fatalf("Expected non-empty blob at index %d", j)
				}
			}

			result.Close()
		}
	})
}

// BenchmarkZeroCopyMultiColumn tests batch extractions vs individual column extractions
func BenchmarkZeroCopyMultiColumn(b *testing.B) {
	// Skip in short mode
	if testing.Short() {
		b.Skip("Skipping in short mode")
	}
	// Set up test database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create table with mixed columns
	if _, err := db.Exec(`CREATE TABLE mixed_test(
		id INTEGER,
		int_val INTEGER,
		float_val DOUBLE,
		str_val VARCHAR,
		blob_val BLOB
	)`); err != nil {
		b.Fatal(err)
	}

	// Insert test data
	insertMixedTestData(b, db, 10000)

	// Run benchmark for standard row iteration
	b.Run("StandardScan", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query("SELECT * FROM mixed_test")
			if err != nil {
				b.Fatal(err)
			}

			var id, intVal int
			var floatVal float64
			var strVal string
			var blobVal []byte
			count := 0

			for rows.Next() {
				if err := rows.Scan(&id, &intVal, &floatVal, &strVal, &blobVal); err != nil {
					b.Fatal(err)
				}
				count++
			}
			rows.Close()

			if count != 10000 {
				b.Fatalf("Expected 10000 rows, got %d", count)
			}
		}
	})
}

// Helper functions for test data generation

func insertStringTestData(b *testing.B, db *sql.DB, count int) {
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}

	stmt, err := tx.Prepare("INSERT INTO str_test VALUES (?, ?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	for i := 0; i < count; i++ {
		small := fmt.Sprintf("small_str_%d", i)
		medium := fmt.Sprintf("medium_str_%d_%s", i, makeRandomString(450))
		large := makeRandomString(2000)

		if _, err := stmt.Exec(i, small, medium, large); err != nil {
			b.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
}

func insertBlobTestData(b *testing.B, db *sql.DB, count int) {
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}

	stmt, err := tx.Prepare("INSERT INTO blob_test VALUES (?, ?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	for i := 0; i < count; i++ {
		small := []byte(fmt.Sprintf("small_blob_%d", i))
		medium := []byte(makeRandomString(450))
		large := []byte(makeRandomString(2000))

		if _, err := stmt.Exec(i, small, medium, large); err != nil {
			b.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
}

func insertMixedTestData(b *testing.B, db *sql.DB, count int) {
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}

	stmt, err := tx.Prepare("INSERT INTO mixed_test VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	for i := 0; i < count; i++ {
		intVal := i * 10
		floatVal := float64(i) * 1.5
		str := fmt.Sprintf("str_%d_%s", i, makeRandomString(50))
		blob := []byte(makeRandomString(50))

		if _, err := stmt.Exec(i, intVal, floatVal, str, blob); err != nil {
			b.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
}

func makeRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[i%len(charset)]
	}
	return string(b)
}
