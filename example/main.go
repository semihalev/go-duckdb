package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/semihalev/go-duckdb"
)

func main() {
	// Open an in-memory database
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create a table
	_, err = db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR, age INTEGER)`)
	if err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	// Insert some data
	_, err = db.Exec(`INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)`)
	if err != nil {
		log.Fatalf("failed to insert data: %v", err)
	}

	// Query with a prepared statement
	rows, err := db.Query(`SELECT id, name, age FROM users WHERE age > ? ORDER BY age ASC`, 20)
	if err != nil {
		log.Fatalf("failed to query data: %v", err)
	}
	defer rows.Close()

	// Process the result
	fmt.Println("ID\tName\tAge")
	fmt.Println("--\t----\t---")
	for rows.Next() {
		var id int
		var name string
		var age int

		err := rows.Scan(&id, &name, &age)
		if err != nil {
			log.Fatalf("failed to scan row: %v", err)
		}

		fmt.Printf("%d\t%s\t%d\n", id, name, age)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("error during rows iteration: %v", err)
	}

	// Transaction example
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("failed to begin transaction: %v", err)
	}

	// Execute statements in transaction
	_, err = tx.Exec("INSERT INTO users (id, name, age) VALUES (4, 'Dave', 40)")
	if err != nil {
		tx.Rollback()
		log.Fatalf("failed to insert in transaction: %v", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		log.Fatalf("failed to commit transaction: %v", err)
	}

	// Verify the transaction
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	if err != nil {
		log.Fatalf("failed to count users: %v", err)
	}

	fmt.Printf("\nTotal users after transaction: %d\n", count)
}
