package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/semihalev/go-duckdb"
)

func main() {
	// Print the DuckDB version
	fmt.Printf("DuckDB version: %s\n\n", "DuckDB")

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

	// Insert data
	_, err = db.Exec(`INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25)`)
	if err != nil {
		log.Fatalf("failed to insert data: %v", err)
	}

	// Query with a prepared statement
	rows, err := db.Query(`SELECT id, name, age FROM users WHERE age > ?`, 20)
	if err != nil {
		log.Fatalf("failed to query data: %v", err)
	}
	defer rows.Close()

	// Process the result
	fmt.Println("ID | Name | Age")
	fmt.Println("----------------")
	for rows.Next() {
		var id int
		var name string
		var age int

		err := rows.Scan(&id, &name, &age)
		if err != nil {
			log.Fatalf("failed to scan row: %v", err)
		}

		fmt.Printf("%2d | %4s | %3d\n", id, name, age)
	}

	// Check for errors from iterating over rows
	if err = rows.Err(); err != nil {
		log.Fatalf("error during row iteration: %v", err)
	}
}