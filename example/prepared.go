package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/semihalev/go-duckdb"
)

func main() {
	// Open an in-memory database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table with various data types
	_, err = db.Exec(`
		CREATE TABLE products (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			price DOUBLE,
			in_stock BOOLEAN,
			description VARCHAR,
			created_at TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	// Prepare an insert statement
	stmt, err := db.Prepare(`
		INSERT INTO products (id, name, price, in_stock, description, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		log.Fatalf("failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Insert sample products
	products := []struct {
		id          int
		name        string
		price       float64
		inStock     bool
		description string
		createdAt   time.Time
	}{
		{1, "Laptop", 999.99, true, "High-performance laptop with SSD", time.Now().Add(-24 * time.Hour)},
		{2, "Smartphone", 499.99, true, "Latest smartphone model", time.Now().Add(-48 * time.Hour)},
		{3, "Headphones", 79.99, true, "Noise-cancelling wireless headphones", time.Now().Add(-72 * time.Hour)},
		{4, "Tablet", 349.99, false, "10-inch tablet with retina display", time.Now().Add(-96 * time.Hour)},
		{5, "Smartwatch", 199.99, true, "Fitness tracking smartwatch", time.Now().Add(-120 * time.Hour)},
	}

	// Insert products using prepared statement
	for _, p := range products {
		_, err := stmt.Exec(p.id, p.name, p.price, p.inStock, p.description, p.createdAt)
		if err != nil {
			log.Fatalf("failed to insert product: %v", err)
		}
	}

	// Prepare a query statement
	queryStmt, err := db.Prepare(`
		SELECT id, name, price, in_stock, description, created_at
		FROM products
		WHERE price < ? AND in_stock = ?
		ORDER BY price DESC
	`)
	if err != nil {
		log.Fatalf("failed to prepare query: %v", err)
	}
	defer queryStmt.Close()

	// Execute the prepared query
	rows, err := queryStmt.Query(500.0, true)
	if err != nil {
		log.Fatalf("failed to execute query: %v", err)
	}
	defer rows.Close()

	// Process the results
	fmt.Println("Products under $500 that are in stock:")
	fmt.Println("--------------------------------------")
	for rows.Next() {
		var (
			id          int
			name        string
			price       float64
			inStock     bool
			description string
			createdAt   time.Time
		)

		if err := rows.Scan(&id, &name, &price, &inStock, &description, &createdAt); err != nil {
			log.Fatalf("failed to scan row: %v", err)
		}

		fmt.Printf("ID: %d\nName: %s\nPrice: $%.2f\nIn Stock: %v\nDescription: %s\nCreated: %s\n\n",
			id, name, price, inStock, description, createdAt.Format(time.RFC3339))
	}

	// Check for errors from iterating over rows
	if err = rows.Err(); err != nil {
		log.Fatalf("error during row iteration: %v", err)
	}

	// Query with transaction
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("failed to begin transaction: %v", err)
	}

	// Using transaction for a prepared statement
	txStmt, err := tx.Prepare(`SELECT COUNT(*) FROM products WHERE price > ?`)
	if err != nil {
		tx.Rollback()
		log.Fatalf("failed to prepare statement in transaction: %v", err)
	}
	defer txStmt.Close()

	// Execute query
	var count int
	err = txStmt.QueryRow(200.0).Scan(&count)
	if err != nil {
		tx.Rollback()
		log.Fatalf("failed to execute query in transaction: %v", err)
	}

	fmt.Printf("Number of products over $200: %d\n", count)

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		log.Fatalf("failed to commit transaction: %v", err)
	}
}