package main

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log"
	"time"

	"github.com/semihalev/go-duckdb"
)

func main() {
	// Open a connection
	conn, err := duckdb.NewConnection(":memory:")
	if err != nil {
		log.Fatalf("failed to open connection: %v", err)
	}
	defer conn.Close()

	// Create a table with various data types
	_, err = conn.ExecContext(context.Background(), `
		CREATE TABLE test_data (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			active BOOLEAN,
			score DOUBLE,
			created_at TIMESTAMP,
			data BLOB
		)
	`, nil)
	if err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	// Use the appender API for fast bulk inserts
	fmt.Println("Using appender for bulk inserts...")
	appender, err := duckdb.NewAppender(conn, "main", "test_data")
	if err != nil {
		log.Fatalf("failed to create appender: %v", err)
	}

	// Insert 10,000 rows using the appender
	startTime := time.Now()
	rowCount := 10000

	for i := 0; i < rowCount; i++ {
		// Create some test data
		id := i
		name := fmt.Sprintf("User %d", i)
		active := i%2 == 0
		score := float64(i) / 100.0
		createdAt := time.Now().Add(-time.Duration(i) * time.Hour)
		data := []byte(fmt.Sprintf("Data for row %d", i))

		// Append the row
		err := appender.AppendRow(id, name, active, score, createdAt, data)
		if err != nil {
			log.Fatalf("failed to append row: %v", err)
		}
	}

	// Flush the appender to ensure all data is written
	if err := appender.Flush(); err != nil {
		log.Fatalf("failed to flush appender: %v", err)
	}

	// Close the appender
	if err := appender.Close(); err != nil {
		log.Fatalf("failed to close appender: %v", err)
	}

	duration := time.Since(startTime)
	fmt.Printf("Inserted %d rows in %v (%.2f rows/sec)\n", 
		rowCount, duration, float64(rowCount)/duration.Seconds())

	// Query the data using a transaction
	fmt.Println("\nQuerying data with transaction...")
	ctx := context.Background()
	tx, err := conn.BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		log.Fatalf("failed to begin transaction: %v", err)
	}

	// Execute a complex query within the transaction
	rows, err := conn.QueryContext(ctx, `
		SELECT 
			id, 
			name, 
			active, 
			score, 
			created_at, 
			data
		FROM test_data
		WHERE active = true AND score > ?
		ORDER BY score DESC
		LIMIT 5
	`, []duckdb.NamedValue{{Ordinal: 1, Value: 50.0}})
	if err != nil {
		tx.Rollback()
		log.Fatalf("failed to query data: %v", err)
	}
	defer rows.Close()

	// Process the results
	fmt.Println("Top 5 active users with score > 50:")
	fmt.Println("----------------------------------")
	for rows.Next() {
		var (
			id        int
			name      string
			active    bool
			score     float64
			createdAt time.Time
			data      []byte
		)

		if err := rows.Scan(&id, &name, &active, &score, &createdAt, &data); err != nil {
			log.Fatalf("failed to scan row: %v", err)
		}

		fmt.Printf("ID: %d, Name: %s, Active: %v, Score: %.2f, Created: %s, Data length: %d bytes\n",
			id, name, active, score, createdAt.Format(time.RFC3339), len(data))
	}

	// Check for errors from iterating over rows
	if err = rows.Err(); err != nil {
		tx.Rollback()
		log.Fatalf("error during row iteration: %v", err)
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Fatalf("failed to commit transaction: %v", err)
	}

	// Print version information
	fmt.Printf("\nDuckDB Version: %s\n", duckdb.GetDuckDBVersion())
	fmt.Printf("Driver Version: %s\n", duckdb.DriverVersion)
}