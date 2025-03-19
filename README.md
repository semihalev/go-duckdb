<p align="center">
  <img src="duckdb_logo.svg" alt="Go-DuckDB" width="600">
</p>

<p align="center">
  <a href="https://goreportcard.com/report/github.com/semihalev/go-duckdb"><img src="https://goreportcard.com/badge/github.com/semihalev/go-duckdb?style=flat-square" alt="Go Report Card"></a>
  <a href="https://pkg.go.dev/github.com/semihalev/go-duckdb"><img src="https://img.shields.io/badge/go.dev-reference-007d9c?style=flat-square" alt="go.dev reference"></a>
  <a href="https://github.com/semihalev/go-duckdb/releases"><img src="https://img.shields.io/github/v/release/semihalev/go-duckdb?style=flat-square" alt="GitHub release"></a>
  <a href="https://github.com/semihalev/go-duckdb/blob/main/LICENSE"><img src="https://img.shields.io/github/license/semihalev/go-duckdb?style=flat-square" alt="License"></a>
</p>

# Go-DuckDB

A high-performance Go driver for DuckDB with both standard SQL and low-level API support.

## Features

- **Zero Allocation Design** - Minimizes GC pressure and memory overhead
- **Dual API Support**:
  - Standard database/sql interface
  - Low-level direct API for maximum performance
- **Advanced Optimizations**:
  - Vectorized batch operations
  - Zero-copy column extraction
  - Tiered buffer pools
  - String caching and deduplication
- **High-Performance Features**:
  - Batch parameter binding
  - Appender API for bulk data insertion
  - Direct column-wise extraction
  - Memory mapping with zero-copy architecture
- **Concurrency and Thread Safety**:
  - Fully thread-safe with atomic operations
  - Safe concurrent query execution from multiple goroutines
  - Connection pooling with proper resource management
  - Lock-free result fetching for parallel data processing
- **Full DuckDB Support**:
  - All DuckDB data types
  - Transactions with proper isolation
  - Context cancellation
  - Thread-safe operations with full concurrent query support
- **Zero Go Dependencies** - Lightweight and portable

## Installation

```bash
go get github.com/semihalev/go-duckdb
```

No need to install DuckDB separately! The driver includes pre-compiled static libraries for:

- Linux (amd64, arm64)
- macOS (amd64, arm64)
- Windows (amd64, arm64)

## Example: Standard SQL API

```go
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
	for rows.Next() {
		var id int
		var name string
		var age int

		err := rows.Scan(&id, &name, &age)
		if err != nil {
			log.Fatalf("failed to scan row: %v", err)
		}

		fmt.Printf("User: %d, %s, %d\n", id, name, age)
	}
}
```

## Example: Low-Level Direct API

```go
package main

import (
	"fmt"
	"log"

	"github.com/semihalev/go-duckdb"
)

func main() {
	// Create a new connection
	conn, err := duckdb.NewConnection(":memory:")
	if err != nil {
		log.Fatalf("failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create a table
	_, err = conn.ExecDirect(`CREATE TABLE analytics (
		id INTEGER, 
		category VARCHAR, 
		value DOUBLE, 
		active BOOLEAN
	)`)
	if err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	// Insert sample data
	_, err = conn.ExecDirect(`INSERT INTO analytics VALUES 
		(1, 'A', 10.5, true),
		(2, 'B', 20.3, false),
		(3, 'A', 15.2, true),
		(4, 'C', 7.8, true),
		(5, 'B', 30.1, false)`)
	if err != nil {
		log.Fatalf("failed to insert data: %v", err)
	}

	// Use the high-performance direct result API
	result, err := conn.QueryDirectResult(`
		SELECT 
			id, 
			category, 
			value, 
			active 
		FROM analytics 
		WHERE value > 10.0
	`)
	if err != nil {
		log.Fatalf("failed to query data: %v", err)
	}
	defer result.Close()

	// Get row count
	rowCount := result.RowCount()
	fmt.Printf("Found %d matching records\n", rowCount)

	// Extract entire columns at once (much faster than row-by-row)
	ids, idNulls, err := result.ExtractInt32Column(0)
	if err != nil {
		log.Fatalf("failed to extract id column: %v", err)
	}

	categories, categoryNulls, err := result.ExtractStringColumn(1)
	if err != nil {
		log.Fatalf("failed to extract category column: %v", err)
	}

	values, valueNulls, err := result.ExtractFloat64Column(2)
	if err != nil {
		log.Fatalf("failed to extract value column: %v", err)
	}

	actives, activeNulls, err := result.ExtractBoolColumn(3)
	if err != nil {
		log.Fatalf("failed to extract active column: %v", err)
	}

	// Process all data at once (vector-oriented processing)
	for i := 0; i < rowCount; i++ {
		// Check for NULL values (though our example has none)
		if idNulls[i] || categoryNulls[i] || valueNulls[i] || activeNulls[i] {
			fmt.Printf("Row %d contains NULL values\n", i)
			continue
		}

		fmt.Printf("Row %d: ID=%d, Category=%s, Value=%.2f, Active=%v\n",
			i, ids[i], categories[i], values[i], actives[i])
	}
}
```

## Concurrency and Thread Safety

Go-DuckDB is fully thread-safe and designed for concurrent usage. Unlike other DuckDB drivers, it provides robust support for concurrent queries across multiple goroutines:

```go
package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/semihalev/go-duckdb"
)

func main() {
	// Create a shared connection
	conn, err := duckdb.NewConnection(":memory:")
	if err != nil {
		log.Fatalf("failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create a table
	_, err = conn.ExecDirect(`CREATE TABLE data (id INTEGER, value DOUBLE)`)
	if err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	// Insert sample data
	_, err = conn.ExecDirect(`INSERT INTO data SELECT range, random() FROM range(1000)`)
	if err != nil {
		log.Fatalf("failed to insert data: %v", err)
	}

	// Run multiple queries concurrently
	var wg sync.WaitGroup
	numQueries := 10

	// Shared access to results
	var mu sync.Mutex
	results := make(map[int]float64)

	for i := 0; i < numQueries; i++ {
		wg.Add(1)
		go func(queryID int) {
			defer wg.Done()

			// Each goroutine can safely execute queries on the shared connection
			result, err := conn.QueryDirectResult(fmt.Sprintf(`
				SELECT AVG(value) FROM data WHERE id %% %d = 0
			`, queryID+1))
			if err != nil {
				log.Printf("Query %d failed: %v", queryID, err)
				return
			}
			defer result.Close()

			// Extract results
			avgValues, nulls, err := result.ExtractFloat64Column(0)
			if err != nil {
				log.Printf("Failed to extract results for query %d: %v", queryID, err)
				return
			}

			// Store results safely
			if len(avgValues) > 0 && !nulls[0] {
				mu.Lock()
				results[queryID] = avgValues[0]
				mu.Unlock()
			}
		}(i)
	}

	// Wait for all queries to complete
	wg.Wait()

	// Display results
	fmt.Println("Concurrent query results:")
	for i := 0; i < numQueries; i++ {
		fmt.Printf("Query %d: %.6f\n", i, results[i])
	}
}
```

Key thread-safety features:

- **Thread-safe Connection Handling**: All connection operations use atomic operations and proper synchronization
- **Concurrent Query Execution**: Multiple goroutines can execute queries on the same connection simultaneously
- **Resource Management**: All resources are properly tracked and released, even during concurrent operations
- **Query Cancellation**: Any goroutine can safely cancel long-running queries with context
- **Connection Pooling**: The standard SQL API efficiently manages connection pools for web applications
- **Lock-free Result Extraction**: Column extraction is designed for parallel processing of results

## High-Performance Appender API

For extremely efficient bulk data insertion, use the Appender API:

```go
package main

import (
	"fmt"
	"log"
	"time"
	
	"github.com/semihalev/go-duckdb"
)

func main() {
	// Create a new connection
	conn, err := duckdb.NewConnection(":memory:")
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer conn.Close()
	
	// Create a table
	_, err = conn.ExecDirect("CREATE TABLE events (id INTEGER, name VARCHAR, value DOUBLE, active BOOLEAN, timestamp TIMESTAMP)")
	if err != nil {
		log.Fatalf("failed to create table: %v", err)
	}
	
	// Create an appender for the table
	appender, err := duckdb.NewAppender(conn, "", "events")
	if err != nil {
		log.Fatalf("failed to create appender: %v", err)
	}
	defer appender.Close()
	
	// Start timing
	startTime := time.Now()
	
	// Insert 1 million rows using the appender
	for i := 0; i < 1000000; i++ {
		err := appender.AppendRow(
			i,                                    // id (INTEGER)
			fmt.Sprintf("Event-%d", i%100),       // name (VARCHAR)
			float64(i) * 0.5,                     // value (DOUBLE)
			i%2 == 0,                             // active (BOOLEAN) 
			time.Now().Add(time.Duration(i)*time.Second), // timestamp (TIMESTAMP)
		)
		if err != nil {
			log.Fatalf("failed to append row: %v", err)
		}
	}
	
	// Flush the appender to ensure all data is written
	if err := appender.Flush(); err != nil {
		log.Fatalf("failed to flush appender: %v", err)
	}
	
	// Calculate elapsed time
	elapsed := time.Since(startTime)
	
	// Verify the inserted rows
	result, err := conn.QueryDirectResult("SELECT COUNT(*) FROM events")
	if err != nil {
		log.Fatalf("failed to query count: %v", err)
	}
	defer result.Close()
	
	// Extract the count value
	counts, _, err := result.ExtractInt64Column(0)
	if err != nil {
		log.Fatalf("failed to extract count: %v", err)
	}
	
	fmt.Printf("Inserted %d rows in %.2f seconds (%.2f rows/second)\n", 
		counts[0], elapsed.Seconds(), float64(counts[0])/elapsed.Seconds())
}
```

## Parallel Data Processing Example

Go-DuckDB excels at parallel data processing with its column-oriented design:

```go
package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/semihalev/go-duckdb"
)

func main() {
	// Create a connection
	conn, err := duckdb.NewConnection(":memory:")
	if err != nil {
		log.Fatalf("failed to create connection: %v", err)
	}
	defer conn.Close()

	// Create and populate a large table
	_, err = conn.ExecDirect(`CREATE TABLE data AS 
		SELECT range as id, random() as value1, random() as value2 
		FROM range(10000000)`)
	if err != nil {
		log.Fatalf("failed to create data: %v", err)
	}

	// Query the data
	result, err := conn.QueryDirectResult(`
		SELECT id, value1, value2 
		FROM data 
		WHERE id % 100 = 0
	`)
	if err != nil {
		log.Fatalf("failed to query: %v", err)
	}
	defer result.Close()

	// Extract all columns at once
	ids, _, err := result.ExtractInt64Column(0)
	if err != nil {
		log.Fatalf("failed to extract ids: %v", err)
	}

	values1, _, err := result.ExtractFloat64Column(1)
	if err != nil {
		log.Fatalf("failed to extract values1: %v", err)
	}

	values2, _, err := result.ExtractFloat64Column(2)
	if err != nil {
		log.Fatalf("failed to extract values2: %v", err)
	}

	// Process data in parallel
	rowCount := len(ids)
	numWorkers := 4
	chunkSize := rowCount / numWorkers
	var wg sync.WaitGroup
	results := make([]float64, numWorkers)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			// Calculate start/end indices for this worker
			start := workerID * chunkSize
			end := start + chunkSize
			if workerID == numWorkers-1 {
				end = rowCount // Last worker takes remaining rows
			}
			
			// Process this worker's portion of the data
			sum := 0.0
			for i := start; i < end; i++ {
				// Complex calculation with the data
				sum += values1[i] * values2[i] * float64(ids[i])
			}
			
			// Store this worker's result
			results[workerID] = sum
		}(w)
	}

	// Wait for all workers to complete
	wg.Wait()

	// Combine results
	finalResult := 0.0
	for _, r := range results {
		finalResult += r
	}

	fmt.Printf("Processed %d rows in parallel with %d workers\n", rowCount, numWorkers)
	fmt.Printf("Final result: %.2f\n", finalResult)
}
```

## Advanced Usage

For more examples and advanced usage, check out the [Wiki](https://github.com/semihalev/go-duckdb/wiki).

## Connection String Format

The driver supports the following connection string formats:

- **In-memory database**: `:memory:`
- **File-based database**: `/path/to/database.db`
- **Read-only mode**: `/path/to/database.db?access_mode=READ_ONLY`
- **Temporary directory**: `/path/to/database.db?temp_directory=/path/to/temp`
- **Thread control**: `/path/to/database.db?threads=4`
- **Memory limits**: `/path/to/database.db?memory_limit=4GB`

## DuckDB Compatibility

This driver is compatible with DuckDB 1.2.1. See the [Wiki](https://github.com/semihalev/go-duckdb/wiki/Compatibility) for details on compatibility and limitations.

## Benchmarks

```
# Standard SQL API vs. Low-Level Direct API
BenchmarkStandardSQLQuery-8      12241  98412 ns/op   5624 B/op   124 allocs/op
BenchmarkDirectResultQuery-8     50112  23854 ns/op   1248 B/op    23 allocs/op

# Row-by-Row vs. Column Extraction
BenchmarkRowByRowScan-8          10000 119842 ns/op   8840 B/op   210 allocs/op
BenchmarkColumnExtraction-8      42315  28421 ns/op   2432 B/op    37 allocs/op

# Appender vs. Batch Insert vs. Regular Insert
BenchmarkAppenderInsert-8       200000   5984 ns/op    320 B/op     4 allocs/op
BenchmarkBatchInsert-8           50000  24812 ns/op   1824 B/op    32 allocs/op
BenchmarkRegularInsert-8          5000 234521 ns/op  12480 B/op   284 allocs/op

# Concurrent Query Execution
BenchmarkConcurrentQueries-8     32184  37254 ns/op   3210 B/op    48 allocs/op
```

Run the benchmarks with: `go test -bench=. -benchmem`

## Performance Optimization

This driver incorporates several advanced optimizations:

- **Zero-Copy Architecture**: Direct memory access minimizes data copying
- **Column-Wise Processing**: Extract and process entire columns at once
- **String Deduplication**: Cross-query string interning reduces allocations
- **Buffer Pooling**: Reuse memory buffers to minimize GC pressure
- **Batch Processing**: Reduce CGO boundary crossings with batch operations
- **Lock-Free Concurrency**: Optimized for parallel execution without contention

For more details, see the [Wiki](https://github.com/semihalev/go-duckdb/wiki/Performance).

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

MIT License - see [LICENSE](LICENSE) for details.