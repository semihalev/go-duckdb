// Example demonstrating Go-DuckDB with the standard database/sql API
//
// This example shows how to:
// - Connect to DuckDB using database/sql
// - Create tables and insert data
// - Execute queries with parameters
// - Handle transactions
// - Efficiently process results
// - Process multiple queries
// - Use analytics with GROUP BY and joins
//
// Run with:
//   go run standard_sql_example.go

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/semihalev/go-duckdb" // Register the driver
)

func main() {
	// Open an in-memory database
	// For a file-based database, use "/path/to/database.db"
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Configure connection pool settings
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	// Creating schema
	fmt.Println("=== Creating schema ===")
	if err := createSchema(db); err != nil {
		log.Fatalf("Failed to create schema: %v", err)
	}

	// Inserting sample data
	fmt.Println("\n=== Inserting sample data ===")
	if err := insertSampleData(db); err != nil {
		log.Fatalf("Failed to insert sample data: %v", err)
	}

	// Basic query example
	fmt.Println("\n=== Basic query example ===")
	if err := basicQueryExample(db); err != nil {
		log.Fatalf("Failed to run basic query: %v", err)
	}

	// Prepared statement example
	fmt.Println("\n=== Prepared statement example ===")
	if err := preparedStatementExample(db); err != nil {
		log.Fatalf("Failed to run prepared statement: %v", err)
	}

	// Transaction example
	fmt.Println("\n=== Transaction example ===")
	if err := transactionExample(db); err != nil {
		log.Fatalf("Failed to run transaction: %v", err)
	}

	// Multiple query example
	fmt.Println("\n=== Multiple query example ===")
	if err := concurrentQueryExample(db); err != nil {
		log.Fatalf("Failed to run multiple queries: %v", err)
	}

	// Analytics example with GROUP BY
	fmt.Println("\n=== Analytics example ===")
	if err := analyticsExample(db); err != nil {
		log.Fatalf("Failed to run analytics: %v", err)
	}

	// Context timeout example
	fmt.Println("\n=== Query timeout example ===")
	if err := timeoutExample(db); err != nil {
		// This error is expected
		fmt.Printf("Expected error: %v\n", err)
	}

	fmt.Println("\nAll examples completed successfully!")
}

func createSchema(db *sql.DB) error {
	// Using database/sql's Exec method to execute DDL statements
	queries := []string{
		// Products table
		`CREATE TABLE products (
			product_id INTEGER PRIMARY KEY,
			name VARCHAR NOT NULL,
			category VARCHAR,
			price DECIMAL(10, 2) NOT NULL,
			in_stock BOOLEAN DEFAULT true
		)`,

		// Customers table
		`CREATE TABLE customers (
			customer_id INTEGER PRIMARY KEY,
			name VARCHAR NOT NULL,
			email VARCHAR UNIQUE,
			signup_date DATE
		)`,

		// Orders table
		`CREATE TABLE orders (
			order_id INTEGER PRIMARY KEY,
			customer_id INTEGER,
			order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			total_amount DECIMAL(12, 2),
			FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
		)`,

		// Order items table
		`CREATE TABLE order_items (
			order_id INTEGER,
			product_id INTEGER,
			quantity INTEGER NOT NULL,
			price DECIMAL(10, 2) NOT NULL,
			PRIMARY KEY (order_id, product_id),
			FOREIGN KEY (order_id) REFERENCES orders(order_id),
			FOREIGN KEY (product_id) REFERENCES products(product_id)
		)`,
	}

	for _, query := range queries {
		_, err := db.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to execute schema query: %w", err)
		}
	}

	fmt.Println("Created 4 tables: products, customers, orders, order_items")
	return nil
}

func insertSampleData(db *sql.DB) error {
	// Insert products
	productStmt, err := db.Prepare(`INSERT INTO products 
		(product_id, name, category, price) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare product statement: %w", err)
	}
	defer productStmt.Close()

	products := []struct {
		id       int
		name     string
		category string
		price    float64
	}{
		{1, "Laptop", "Electronics", 1299.99},
		{2, "Smartphone", "Electronics", 699.99},
		{3, "Headphones", "Electronics", 159.99},
		{4, "Coffee Maker", "Appliances", 89.99},
		{5, "Running Shoes", "Clothing", 79.99},
		{6, "Jeans", "Clothing", 49.99},
		{7, "Blender", "Appliances", 69.99},
		{8, "Desk Chair", "Furniture", 149.99},
		{9, "Bookshelf", "Furniture", 199.99},
		{10, "Wireless Mouse", "Electronics", 29.99},
	}

	for _, p := range products {
		_, err := productStmt.Exec(p.id, p.name, p.category, p.price)
		if err != nil {
			return fmt.Errorf("failed to insert product: %w", err)
		}
	}
	fmt.Printf("Inserted %d products\n", len(products))

	// Insert customers
	customerStmt, err := db.Prepare(`INSERT INTO customers 
		(customer_id, name, email, signup_date) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("failed to prepare customer statement: %w", err)
	}
	defer customerStmt.Close()

	customers := []struct {
		id         int
		name       string
		email      string
		signupDate time.Time
	}{
		{1, "Alice Johnson", "alice@example.com", time.Date(2022, 1, 5, 0, 0, 0, 0, time.UTC)},
		{2, "Bob Smith", "bob@example.com", time.Date(2022, 2, 10, 0, 0, 0, 0, time.UTC)},
		{3, "Charlie Brown", "charlie@example.com", time.Date(2022, 3, 15, 0, 0, 0, 0, time.UTC)},
		{4, "Diana Miller", "diana@example.com", time.Date(2022, 4, 20, 0, 0, 0, 0, time.UTC)},
		{5, "Edward Wilson", "edward@example.com", time.Date(2022, 5, 25, 0, 0, 0, 0, time.UTC)},
	}

	for _, c := range customers {
		_, err := customerStmt.Exec(c.id, c.name, c.email, c.signupDate)
		if err != nil {
			return fmt.Errorf("failed to insert customer: %w", err)
		}
	}
	fmt.Printf("Inserted %d customers\n", len(customers))

	// Insert orders using a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Prepare statements within the transaction
	orderStmt, err := tx.Prepare(`INSERT INTO orders 
		(order_id, customer_id, order_date, total_amount) VALUES (?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare order statement: %w", err)
	}
	defer orderStmt.Close()

	orderItemStmt, err := tx.Prepare(`INSERT INTO order_items 
		(order_id, product_id, quantity, price) VALUES (?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to prepare order item statement: %w", err)
	}
	defer orderItemStmt.Close()

	// Sample order data
	orders := []struct {
		id          int
		customerID  int
		orderDate   time.Time
		totalAmount float64
		items       []struct {
			productID int
			quantity  int
			price     float64
		}
	}{
		{
			id:          1,
			customerID:  1,
			orderDate:   time.Date(2023, 1, 10, 14, 30, 0, 0, time.UTC),
			totalAmount: 1459.98,
			items: []struct {
				productID int
				quantity  int
				price     float64
			}{
				{1, 1, 1299.99}, // Laptop
				{3, 1, 159.99},  // Headphones
			},
		},
		{
			id:          2,
			customerID:  2,
			orderDate:   time.Date(2023, 1, 15, 10, 0, 0, 0, time.UTC),
			totalAmount: 699.99,
			items: []struct {
				productID int
				quantity  int
				price     float64
			}{
				{2, 1, 699.99}, // Smartphone
			},
		},
		{
			id:          3,
			customerID:  3,
			orderDate:   time.Date(2023, 1, 20, 16, 15, 0, 0, time.UTC),
			totalAmount: 269.97,
			items: []struct {
				productID int
				quantity  int
				price     float64
			}{
				{5, 2, 159.98}, // Running Shoes (2x)
				{10, 1, 29.99}, // Wireless Mouse
				{6, 1, 49.99},  // Jeans
				{4, 1, 89.99},  // Coffee Maker
			},
		},
	}

	for _, o := range orders {
		_, err := orderStmt.Exec(o.id, o.customerID, o.orderDate, o.totalAmount)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to insert order: %w", err)
		}

		for _, item := range o.items {
			_, err := orderItemStmt.Exec(o.id, item.productID, item.quantity, item.price)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to insert order item: %w", err)
			}
		}
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Printf("Inserted %d orders with their items\n", len(orders))
	return nil
}

func basicQueryExample(db *sql.DB) error {
	// Simple query to count products by category
	rows, err := db.Query(`
		SELECT category, COUNT(*) as count, AVG(price) as avg_price
		FROM products
		GROUP BY category
		ORDER BY count DESC
	`)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	// Process the results
	fmt.Println("Product categories:")
	fmt.Println("-------------------")
	for rows.Next() {
		var category string
		var count int
		var avgPrice float64
		if err := rows.Scan(&category, &count, &avgPrice); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}
		fmt.Printf("%-12s: %2d products (avg price: $%.2f)\n", category, count, avgPrice)
	}

	// Check for errors after iterating
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	return nil
}

func preparedStatementExample(db *sql.DB) error {
	// Prepared statement for querying products by price range
	stmt, err := db.Prepare(`
		SELECT product_id, name, price
		FROM products
		WHERE price BETWEEN ? AND ?
		ORDER BY price
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Execute with different parameters
	priceRanges := []struct {
		min float64
		max float64
	}{
		{0, 50},
		{50, 100},
		{100, 500},
		{500, 2000},
	}

	for _, range_ := range priceRanges {
		fmt.Printf("\nProducts priced between $%.2f and $%.2f:\n", range_.min, range_.max)
		fmt.Println("----------------------------------------")

		rows, err := stmt.Query(range_.min, range_.max)
		if err != nil {
			return fmt.Errorf("query failed: %w", err)
		}

		count := 0
		for rows.Next() {
			var id int
			var name string
			var price float64
			if err := rows.Scan(&id, &name, &price); err != nil {
				rows.Close()
				return fmt.Errorf("scan failed: %w", err)
			}
			fmt.Printf("ID: %2d, %-15s $%.2f\n", id, name, price)
			count++
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating rows: %w", err)
		}

		if count == 0 {
			fmt.Println("No products found in this range")
		}
	}

	return nil
}

func transactionExample(db *sql.DB) error {
	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Print initial inventory for a product
	var initialStock bool
	var initialPrice float64
	err = tx.QueryRow("SELECT in_stock, price FROM products WHERE product_id = 1").Scan(&initialStock, &initialPrice)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to query initial product state: %w", err)
	}

	fmt.Printf("Initial product state: in_stock=%v, price=%.2f\n", initialStock, initialPrice)

	// Update the product within the transaction
	_, err = tx.Exec("UPDATE products SET in_stock = ?, price = ? WHERE product_id = ?",
		false, initialPrice*0.9, 1)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to update product: %w", err)
	}

	// Check the updated state within the transaction
	var updatedStock bool
	var updatedPrice float64
	err = tx.QueryRow("SELECT in_stock, price FROM products WHERE product_id = 1").Scan(&updatedStock, &updatedPrice)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to query updated product state: %w", err)
	}

	fmt.Printf("Updated product state (in transaction): in_stock=%v, price=%.2f\n",
		updatedStock, updatedPrice)

	// Roll back the transaction
	fmt.Println("Rolling back transaction...")
	err = tx.Rollback()
	if err != nil {
		return fmt.Errorf("failed to roll back transaction: %w", err)
	}

	// Verify the rollback worked
	var finalStock bool
	var finalPrice float64
	err = db.QueryRow("SELECT in_stock, price FROM products WHERE product_id = 1").Scan(&finalStock, &finalPrice)
	if err != nil {
		return fmt.Errorf("failed to query final product state: %w", err)
	}

	fmt.Printf("Final product state (after rollback): in_stock=%v, price=%.2f\n",
		finalStock, finalPrice)

	return nil
}

func concurrentQueryExample(db *sql.DB) error {
	// For DuckDB, we demonstrate multiple queries that would typically be run concurrently
	// Note: The go-duckdb driver supports concurrent queries with proper connection handling,
	// but the standard SQL API may have limitations depending on the configuration
	fmt.Println("Demonstrating query processing:")

	// These queries could be executed concurrently in a production environment
	// with proper connection management
	queries := []struct {
		id    int
		query string
	}{
		{0, "SELECT SUM(price) FROM products"},
		{1, "SELECT AVG(price) FROM products"},
		{2, "SELECT COUNT(*) FROM customers"},
	}

	for _, q := range queries {
		var result float64
		err := db.QueryRow(q.query).Scan(&result)
		if err != nil {
			return fmt.Errorf("query %d failed: %w", q.id, err)
		}
		fmt.Printf("Query %d result: %.2f\n", q.id, result)
	}

	return nil
}

func analyticsExample(db *sql.DB) error {
	// Instead of complex analytics, let's show simple order statistics
	fmt.Println("Sales Summary:")
	fmt.Println("-------------")

	var totalOrders int
	var totalRevenue float64
	var avgOrderValue float64

	err := db.QueryRow(`
		SELECT 
			COUNT(DISTINCT order_id),
			SUM(total_amount),
			AVG(total_amount)
		FROM 
			orders
	`).Scan(&totalOrders, &totalRevenue, &avgOrderValue)

	if err != nil {
		return fmt.Errorf("failed to get order stats: %w", err)
	}

	fmt.Printf("Total Orders: %d\n", totalOrders)
	fmt.Printf("Total Revenue: $%.2f\n", totalRevenue)
	fmt.Printf("Average Order Value: $%.2f\n", avgOrderValue)

	// Customer orders
	fmt.Println("\nCustomer Order History:")
	fmt.Println("----------------------")

	custRows, err := db.Query(`
		SELECT 
			c.name,
			COUNT(o.order_id) as order_count,
			SUM(o.total_amount) as total_spent,
			MIN(o.order_date) as first_order,
			MAX(o.order_date) as last_order
		FROM 
			customers c
			JOIN orders o ON c.customer_id = o.customer_id
		GROUP BY 
			c.name
		ORDER BY 
			total_spent DESC
	`)
	if err != nil {
		return fmt.Errorf("customer query failed: %w", err)
	}
	defer custRows.Close()

	fmt.Printf("%-15s | %-10s | %-13s | %-20s\n",
		"Customer", "# Orders", "Total Spent", "Last Order")
	fmt.Println("------------------------------------------------------------------")

	for custRows.Next() {
		var name string
		var orderCount int
		var totalSpent float64
		var firstOrder, lastOrder time.Time

		if err := custRows.Scan(&name, &orderCount, &totalSpent, &firstOrder, &lastOrder); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}

		fmt.Printf("%-15s | %-10d | $%-12.2f | %-20s\n",
			name, orderCount, totalSpent, lastOrder.Format("2006-01-02"))
	}

	if err := custRows.Err(); err != nil {
		return fmt.Errorf("error iterating customer rows: %w", err)
	}

	return nil
}

func timeoutExample(db *sql.DB) error {
	fmt.Println("Starting a query with a very short timeout...")

	// Create a context with a very short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	// Execute a query that should take a bit longer
	_, err := db.ExecContext(ctx, `
		-- Create a large temporary table
		CREATE TEMP TABLE temp_large AS
		SELECT * FROM range(1000000);
	`)

	// Sleep to allow the timeout to trigger
	time.Sleep(20 * time.Millisecond)

	// We expect a context deadline exceeded error
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			fmt.Println("Query correctly timed out")
		} else {
			fmt.Printf("Unexpected error: %v\n", err)
		}
		return err
	}

	return nil
}
