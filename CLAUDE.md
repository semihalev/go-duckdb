# Go-DuckDB Project Reference

## Project Overview
Go-DuckDB is a Go driver for DuckDB, focusing on high performance with native optimizations including SIMD support.

## Optimization Guidelines
- Avoid using `fmt.Sprintf` for string formatting as it is very expensive. Instead use:
  - `strings.Builder` with `WriteString` and `strconv.Itoa` for integer conversions
  - `io.WriteString` for writing to output streams
  - `strconv` package functions for primitive type conversions
- Minimize CGO crossings to improve performance:
  - Batch operations where possible to reduce the number of C function calls
  - Use vector operations rather than row-by-row processing
  - Implement column-wise extraction instead of value-by-value extraction
- Avoid using CGO in test files to improve test reliability and performance
- Use object pooling for frequently allocated objects to reduce GC pressure
- Prefer preallocated buffers over dynamic allocations in hot paths

## Key Files
- `duckdb_go.adapter.c` - High performance C adapter for DuckDB Go driver
- `connection.go` - Connection represents a connection to a DuckDB database with CGO bindings
- `duckdb.go` - Core package implementation
- `direct_result.go` - Result handling with native optimizations
- `vector.go` - DuckDB vector implementation with efficient data access and minimal CGO crossings
- `batch_query.go` - Batch-oriented query processing for high-performance column-wise data extraction
- `vector_pool.go` - Memory pooling for column vectors to minimize allocations
- `appender.go` - Efficient data appending to DuckDB tables
- `appender_tx.go` - Transaction support for data appending operations
- `result_extraction.go` - Efficient extraction of results from DuckDB
- `buffer_pool.go` - Buffer pooling implementation for memory management
- `columnar_result.go` - Columnar result representation for efficient data access
- `errors.go` - Error definitions and handling for DuckDB operations
- `fast_driver.go` - High-performance driver implementation
- `prepared_direct.go` - Direct access to prepared statements for optimal performance
- `rows.go` - Implementation of Go's database/sql Rows interface
- `statement.go` - SQL statement handling and execution
- `string_cache.go` - String pooling for reduced memory allocations
- `time_utils.go` - Time-related utilities for DuckDB temporal types
- `transaction.go` - Transaction handling in DuckDB
- `version.go` - Version information for the driver
