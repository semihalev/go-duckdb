package duckdb

import (
	"database/sql"
	"math/rand"
	"testing"
)

func BenchmarkInsert(b *testing.B) {
	b.ReportAllocs() // Report memory allocations
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`CREATE TABLE bench_insert (id INTEGER, value INTEGER)`)
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	// Prepare statement
	stmt, err := db.Prepare("INSERT INTO bench_insert (id, value) VALUES (?, ?)")
	if err != nil {
		b.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = stmt.Exec(i, i*10)
		if err != nil {
			b.Fatalf("failed to execute statement: %v", err)
		}
	}
}

func BenchmarkQuery(b *testing.B) {
	b.ReportAllocs() // Report memory allocations
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table and fill with data
	_, err = db.Exec(`CREATE TABLE bench_query (id INTEGER, value INTEGER)`)
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	// Insert 1000 rows
	tx, err := db.Begin()
	if err != nil {
		b.Fatalf("failed to begin transaction: %v", err)
	}

	stmt, err := tx.Prepare("INSERT INTO bench_query (id, value) VALUES (?, ?)")
	if err != nil {
		b.Fatalf("failed to prepare statement: %v", err)
	}

	for i := 0; i < 1000; i++ {
		_, err = stmt.Exec(i, i*10)
		if err != nil {
			b.Fatalf("failed to execute statement: %v", err)
		}
	}

	err = stmt.Close()
	if err != nil {
		b.Fatalf("failed to close statement: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		b.Fatalf("failed to commit transaction: %v", err)
	}

	// Prepare query statement
	queryStmt, err := db.Prepare("SELECT id, value FROM bench_query WHERE id = ?")
	if err != nil {
		b.Fatalf("failed to prepare query statement: %v", err)
	}
	defer queryStmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var id, value int
		err = queryStmt.QueryRow(i % 1000).Scan(&id, &value)
		if err != nil {
			b.Fatalf("failed to query row: %v", err)
		}

		if id != i%1000 || value != (i%1000)*10 {
			b.Fatalf("unexpected values: id=%d, value=%d", id, value)
		}
	}
}

func BenchmarkParallelQuery(b *testing.B) {
	b.ReportAllocs() // Report memory allocations
	
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table and fill with data
	_, err = db.Exec(`CREATE TABLE bench_parallel (id INTEGER, value INTEGER)`)
	if err != nil {
		b.Fatalf("failed to create table: %v", err)
	}

	// Insert 1000 rows
	tx, err := db.Begin()
	if err != nil {
		b.Fatalf("failed to begin transaction: %v", err)
	}

	stmt, err := tx.Prepare("INSERT INTO bench_parallel (id, value) VALUES (?, ?)")
	if err != nil {
		b.Fatalf("failed to prepare statement: %v", err)
	}

	for i := 0; i < 1000; i++ {
		_, err = stmt.Exec(i, i*10)
		if err != nil {
			b.Fatalf("failed to execute statement: %v", err)
		}
	}

	err = stmt.Close()
	if err != nil {
		b.Fatalf("failed to close statement: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		b.Fatalf("failed to commit transaction: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Prepare query statement - each goroutine gets its own statement
		queryStmt, err := db.Prepare("SELECT id, value FROM bench_parallel WHERE id = ?")
		if err != nil {
			b.Fatalf("failed to prepare query statement: %v", err)
		}
		defer queryStmt.Close()

		var id, value int
		for pb.Next() {
			// Use a random ID from 0-999
			queryID := rand.Intn(1000)
			
			err = queryStmt.QueryRow(queryID).Scan(&id, &value)
			if err != nil {
				b.Fatalf("failed to query row: %v", err)
			}

			if id != queryID || value != queryID*10 {
				b.Fatalf("unexpected values: id=%d, value=%d", id, value)
			}
		}
	})
}
