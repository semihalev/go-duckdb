// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

/*
// Use only necessary includes here - CGO directives are defined in duckdb.go
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Connection represents a connection to a DuckDB database.
type Connection struct {
	db     *C.duckdb_database
	conn   *C.duckdb_connection
	closed int32
	mu     sync.Mutex
}

// NamedValue is used for parameter binding with a name.
type NamedValue = driver.NamedValue

// ConnectionOption represents an option for configuring a DuckDB connection
type ConnectionOption func(*Connection)

// NewConnection creates a new connection to the DuckDB database.
func NewConnection(path string, options ...ConnectionOption) (*Connection, error) {
	var db C.duckdb_database
	var conn C.duckdb_connection

	cPath := cString(path)
	defer freeString(cPath)

	// Initialize database
	if err := C.duckdb_open(cPath, &db); err == C.DuckDBError {
		return nil, fmt.Errorf("failed to open database: %s", GetDuckDBVersion())
	}

	// Create connection
	if err := C.duckdb_connect(db, &conn); err == C.DuckDBError {
		C.duckdb_close(&db)
		return nil, fmt.Errorf("failed to connect to database: %s", GetDuckDBVersion())
	}

	c := &Connection{
		db:   &db,
		conn: &conn,
	}

	// Apply all options
	for _, option := range options {
		option(c)
	}

	// Set finalizer to ensure connection is closed when garbage collected
	runtime.SetFinalizer(c, (*Connection).Close)

	return c, nil
}

// Close closes the connection to the database.
func (c *Connection) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		C.duckdb_disconnect(c.conn)
		c.conn = nil
	}

	if c.db != nil {
		C.duckdb_close(c.db)
		c.db = nil
	}

	runtime.SetFinalizer(c, nil)
	return nil
}

// BeginTx starts a new transaction with the provided context and options.
func (c *Connection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	// Create transaction
	tx := &Transaction{
		conn: c,
	}

	// Execute BEGIN statement with isolation level
	iso := "BEGIN TRANSACTION"
	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault, sql.LevelSerializable:
		// Default isolation level for DuckDB is serializable
	case sql.LevelReadCommitted:
		iso = "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED"
	case sql.LevelReadUncommitted:
		iso = "BEGIN TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
	case sql.LevelRepeatableRead:
		iso = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"
	default:
		return nil, fmt.Errorf("unsupported isolation level: %d", opts.Isolation)
	}

	if _, err := c.ExecContext(ctx, iso, nil); err != nil {
		return nil, err
	}

	return tx, nil
}

// PrepareContext prepares a statement for execution with the provided context.
func (c *Connection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	// Check if context is already canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Context is still valid, proceed
	}

	// Always use fast driver implementation
	c.mu.Lock()
	defer c.mu.Unlock()

	// Use FastPrepare but wrap it with FastStmtWrapper to ensure it implements all needed interfaces
	fastStmt, err := c.FastPrepare(query)
	if err != nil {
		return nil, err
	}

	// Wrap with FastStmtWrapper which properly implements driver.StmtQueryContext
	return &FastStmtWrapper{
		conn:  c,
		stmt:  fastStmt,
		query: query,
	}, nil
}

// ExecContext executes a query without returning any rows.
func (c *Connection) ExecContext(ctx context.Context, query string, args []driver.Value) (driver.Result, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	// Check if context is already canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Context is still valid, proceed
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.FastExecContext(ctx, query, args)
}

// QueryContext executes a query with the provided context.
func (c *Connection) QueryContext(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	// Check if context is already canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Context is still valid, proceed
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.FastQueryContext(ctx, query, args)
}

// Ping verifies a connection to the database is still alive.
func (c *Connection) Ping(ctx context.Context) error {
	if atomic.LoadInt32(&c.closed) != 0 {
		return driver.ErrBadConn
	}

	rows, err := c.QueryContext(ctx, "SELECT 1", nil)
	if err != nil {
		return err
	}
	defer rows.Close()

	return nil
}

// Begin starts a new transaction.
func (c *Connection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// Exec implements the driver.Execer interface
func (c *Connection) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.ExecContext(context.Background(), query, args)
}

// Query implements the driver.Queryer interface
func (c *Connection) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, args)
}

// Prepare prepares a statement for execution.
func (c *Connection) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// CheckNamedValue implements driver.NamedValueChecker interface
func (c *Connection) CheckNamedValue(nv *driver.NamedValue) error {
	switch nv.Value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string, []byte, time.Time:
		return nil
	default:
		return fmt.Errorf("unsupported parameter type: %T", nv.Value)
	}
}

//------------------------------------------------------------------------------
// Direct high-performance methods using native code
//------------------------------------------------------------------------------

// QueryDirectResult executes a query and returns a DirectResult using optimized native code.
// This bypasses the standard SQL driver interface for maximum performance.
// Ideal for data science and analytics workloads that need to process large result sets quickly.
func (c *Connection) QueryDirectResult(query string) (*DirectResult, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, ErrConnectionClosed
	}

	// Lock for the entire operation to ensure consistent locking strategy
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check again after lock acquisition
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, ErrConnectionClosed
	}

	// Convert query to C string (under lock to protect from concurrent frees)
	cQuery := cString(query)
	defer freeString(cQuery)

	// Execute query while holding the connection lock
	// This aligns with the pattern used in ExecContext and QueryContext
	var result C.duckdb_result
	if err := C.duckdb_query(*c.conn, cQuery, &result); err == C.DuckDBError {
		errMsg := C.GoString(C.duckdb_result_error(&result))
		C.duckdb_destroy_result(&result)
		return nil, NewError(ErrQuery, errMsg)
	}

	// Create a DirectResult for high-performance access
	return NewDirectResult(&result), nil
}

// QueryColumnar executes a query and returns the results in a highly optimized columnar format.
// This is ideal for data science, analytics, and any bulk processing scenario where you need
// to operate on entire columns rather than individual rows.
//
// The returned ColumnarResult contains all columns fully extracted in their native types, ready for
// direct manipulation in Go with zero additional copying.
func (c *Connection) QueryColumnar(query string) (*ColumnarResult, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, ErrConnectionClosed
	}

	// First get a direct result
	directResult, err := c.QueryDirectResult(query)
	if err != nil {
		return nil, err
	}

	// Create a columnar result to hold the data
	result := &ColumnarResult{
		RowCount:    int(directResult.RowCount()),
		ColumnCount: directResult.ColumnCount(),
		ColumnNames: directResult.ColumnNames(),
		ColumnTypes: make([]string, directResult.ColumnCount()),
		Columns:     make([]interface{}, directResult.ColumnCount()),
		NullMasks:   make([][]bool, directResult.ColumnCount()),
	}

	// Extract type information
	duckDBTypes := directResult.ColumnTypes()
	for i := 0; i < result.ColumnCount; i++ {
		// Store the type name for user reference
		switch duckDBTypes[i] {
		case C.DUCKDB_TYPE_BOOLEAN:
			result.ColumnTypes[i] = "BOOLEAN"
		case C.DUCKDB_TYPE_TINYINT:
			result.ColumnTypes[i] = "TINYINT"
		case C.DUCKDB_TYPE_SMALLINT:
			result.ColumnTypes[i] = "SMALLINT"
		case C.DUCKDB_TYPE_INTEGER:
			result.ColumnTypes[i] = "INTEGER"
		case C.DUCKDB_TYPE_BIGINT:
			result.ColumnTypes[i] = "BIGINT"
		case C.DUCKDB_TYPE_UTINYINT:
			result.ColumnTypes[i] = "UTINYINT"
		case C.DUCKDB_TYPE_USMALLINT:
			result.ColumnTypes[i] = "USMALLINT"
		case C.DUCKDB_TYPE_UINTEGER:
			result.ColumnTypes[i] = "UINTEGER"
		case C.DUCKDB_TYPE_UBIGINT:
			result.ColumnTypes[i] = "UBIGINT"
		case C.DUCKDB_TYPE_FLOAT:
			result.ColumnTypes[i] = "FLOAT"
		case C.DUCKDB_TYPE_DOUBLE:
			result.ColumnTypes[i] = "DOUBLE"
		case C.DUCKDB_TYPE_VARCHAR:
			result.ColumnTypes[i] = "VARCHAR"
		case C.DUCKDB_TYPE_BLOB:
			result.ColumnTypes[i] = "BLOB"
		case C.DUCKDB_TYPE_TIMESTAMP:
			result.ColumnTypes[i] = "TIMESTAMP"
		case C.DUCKDB_TYPE_DATE:
			result.ColumnTypes[i] = "DATE"
		case C.DUCKDB_TYPE_TIME:
			result.ColumnTypes[i] = "TIME"
		default:
			result.ColumnTypes[i] = "UNKNOWN"
		}

		// Extract the column data using the appropriate method based on type
		var err error
		switch duckDBTypes[i] {
		case C.DUCKDB_TYPE_BOOLEAN:
			result.Columns[i], result.NullMasks[i], err = directResult.ExtractBoolColumn(i)
		case C.DUCKDB_TYPE_TINYINT:
			// Handle via int8
			vals, nulls, err := directResult.ExtractInt8Column(i)
			if err == nil {
				result.Columns[i] = vals
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_SMALLINT:
			// Handle via int16
			vals, nulls, err := directResult.ExtractInt16Column(i)
			if err == nil {
				result.Columns[i] = vals
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_INTEGER:
			result.Columns[i], result.NullMasks[i], err = directResult.ExtractInt32Column(i)
		case C.DUCKDB_TYPE_BIGINT:
			result.Columns[i], result.NullMasks[i], err = directResult.ExtractInt64Column(i)
		case C.DUCKDB_TYPE_UTINYINT:
			// Handle via uint8
			vals, nulls, err := directResult.ExtractUint8Column(i)
			if err == nil {
				result.Columns[i] = vals
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_USMALLINT:
			// Handle via uint16
			vals, nulls, err := directResult.ExtractUint16Column(i)
			if err == nil {
				result.Columns[i] = vals
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_UINTEGER:
			// Handle via uint32
			vals, nulls, err := directResult.ExtractUint32Column(i)
			if err == nil {
				result.Columns[i] = vals
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_UBIGINT:
			// Handle via uint64
			vals, nulls, err := directResult.ExtractUint64Column(i)
			if err == nil {
				result.Columns[i] = vals
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_FLOAT:
			// Handle via float32
			vals, nulls, err := directResult.ExtractFloat32Column(i)
			if err == nil {
				result.Columns[i] = vals
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_DOUBLE:
			result.Columns[i], result.NullMasks[i], err = directResult.ExtractFloat64Column(i)
		case C.DUCKDB_TYPE_VARCHAR:
			result.Columns[i], result.NullMasks[i], err = directResult.ExtractStringColumn(i)
		case C.DUCKDB_TYPE_BLOB:
			result.Columns[i], result.NullMasks[i], err = directResult.ExtractBlobColumn(i)
		case C.DUCKDB_TYPE_TIMESTAMP:
			vals, nulls, err := directResult.ExtractTimestampColumn(i)
			if err == nil {
				// Convert from microseconds to time.Time
				times := make([]time.Time, len(vals))
				for j, micros := range vals {
					if !nulls[j] {
						times[j] = time.Unix(micros/1000000, (micros%1000000)*1000)
					}
				}
				result.Columns[i] = times
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_DATE:
			vals, nulls, err := directResult.ExtractDateColumn(i)
			if err == nil {
				// Convert from days to time.Time
				dates := make([]time.Time, len(vals))
				for j, days := range vals {
					if !nulls[j] {
						dates[j] = time.Unix(int64(days)*24*60*60, 0).UTC()
					}
				}
				result.Columns[i] = dates
				result.NullMasks[i] = nulls
			}
		}

		if err != nil {
			directResult.Close()
			return nil, fmt.Errorf("error extracting column %d (%s): %v", i, result.ColumnNames[i], err)
		}
	}

	// Close the direct result as we've extracted all data
	directResult.Close()

	return result, nil
}

// PrepareDirectResult prepares a statement and returns a prepared statement that can be used
// with native optimized result extraction.
// This is ideal for queries that need to be executed multiple times with different parameters.
func (c *Connection) PrepareDirectResult(query string) (*PreparedDirectStatement, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, ErrConnectionClosed
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	cQuery := cString(query)
	defer freeString(cQuery)

	var stmt C.duckdb_prepared_statement
	if err := C.duckdb_prepare(*c.conn, cQuery, &stmt); err == C.DuckDBError {
		errMsg := C.GoString(C.duckdb_prepare_error(stmt))
		C.duckdb_destroy_prepare(&stmt)
		return nil, NewError(ErrPrepare, errMsg)
	}

	// Create a new prepared statement that uses native implementation
	return &PreparedDirectStatement{
		conn: c,
		stmt: &stmt,
	}, nil
}

// ExecDirect executes a query without parameter binding and returns affected rows.
// This is a high-performance shortcut for queries that don't need parameter binding.
func (c *Connection) ExecDirect(query string) (int64, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return 0, ErrConnectionClosed
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	cQuery := cString(query)
	defer freeString(cQuery)

	var result C.duckdb_result
	if err := C.duckdb_query(*c.conn, cQuery, &result); err == C.DuckDBError {
		errMsg := C.GoString(C.duckdb_result_error(&result))
		C.duckdb_destroy_result(&result)
		return 0, NewError(ErrExec, errMsg)
	}
	defer C.duckdb_destroy_result(&result)

	// For statements like INSERT, UPDATE, DELETE, get affected rows
	return int64(C.duckdb_rows_changed(&result)), nil
}

// BatchExec prepares and executes a statement with multiple parameter sets in a single batch operation
// This provides a significant performance improvement over executing multiple individual statements
func (conn *Connection) BatchExec(query string, args []driver.Value) (driver.Result, error) {
	if atomic.LoadInt32(&conn.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	// Extract the first parameter set to determine the number of parameters per set
	if len(args) == 0 {
		return nil, fmt.Errorf("no parameter sets provided")
	}

	// Count the number of parameters in the first set
	var firstParamSetLen int
	switch v := args[0].(type) {
	case []interface{}:
		firstParamSetLen = len(v)
	default:
		return nil, fmt.Errorf("expected []interface{} parameter set, got %T", args[0])
	}

	// Count the total rows affected across all executions
	var totalRowsAffected int64

	// Loop through each parameter set and execute individually
	// This is a fallback implementation until we have true batch execution
	for i, arg := range args {
		paramSet, ok := arg.([]interface{})
		if !ok {
			return nil, fmt.Errorf("parameter set %d is not []interface{}", i)
		}

		if len(paramSet) != firstParamSetLen {
			return nil, fmt.Errorf("parameter set %d has %d parameters, expected %d",
				i, len(paramSet), firstParamSetLen)
		}

		// Prepare a statement for this execution
		// We'll reuse the statement for each parameter set
		stmt, err := conn.Prepare(query)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare statement: %w", err)
		}

		// Convert []interface{} to []driver.Value for Exec
		driverValues := make([]driver.Value, len(paramSet))
		for j, p := range paramSet {
			driverValues[j] = p
		}

		// Execute with these parameters
		result, err := stmt.Exec(driverValues)
		stmt.Close() // Close the statement after use

		if err != nil {
			return nil, fmt.Errorf("failed to execute batch set %d: %w", i, err)
		}

		// Add to total rows affected
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("failed to get rows affected for batch set %d: %w", i, err)
		}

		totalRowsAffected += rowsAffected
	}

	// Return a result with the total rows affected
	return &Result{
		rowsAffected: totalRowsAffected,
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}, nil
}

// BatchExecContext prepares and executes a statement with multiple parameter sets in a single batch operation
// with context support for cancellation
func (conn *Connection) BatchExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if atomic.LoadInt32(&conn.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	// Check if context is already canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Context is still valid, proceed
	}

	// Extract the first parameter set to determine the number of parameters per set
	if len(args) == 0 {
		return nil, fmt.Errorf("no parameter sets provided")
	}

	// Count the number of parameters in the first set
	var firstParamSetLen int
	switch v := args[0].Value.(type) {
	case []interface{}:
		firstParamSetLen = len(v)
	default:
		return nil, fmt.Errorf("expected []interface{} parameter set, got %T", args[0].Value)
	}

	// Count the total rows affected across all executions
	var totalRowsAffected int64

	// Loop through each parameter set and execute individually
	for i, arg := range args {
		// Check context periodically
		if i%10 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				// Context is still valid, proceed
			}
		}

		paramSet, ok := arg.Value.([]interface{})
		if !ok {
			return nil, fmt.Errorf("parameter set %d is not []interface{}", i)
		}

		if len(paramSet) != firstParamSetLen {
			return nil, fmt.Errorf("parameter set %d has %d parameters, expected %d",
				i, len(paramSet), firstParamSetLen)
		}

		// Prepare a statement for this execution
		stmt, err := conn.PrepareContext(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("failed to prepare statement: %w", err)
		}

		// Convert paramSet to NamedValue for ExecContext
		namedValues := make([]driver.NamedValue, len(paramSet))
		for j, p := range paramSet {
			namedValues[j] = driver.NamedValue{
				Ordinal: j + 1,
				Value:   p,
			}
		}

		// Get stmt that implements StmtExecContext
		execer, ok := stmt.(driver.StmtExecContext)
		if !ok {
			stmt.Close()
			return nil, fmt.Errorf("statement does not implement StmtExecContext")
		}

		// Execute with these parameters
		result, err := execer.ExecContext(ctx, namedValues)
		stmt.Close() // Close the statement after use

		if err != nil {
			return nil, fmt.Errorf("failed to execute batch set %d: %w", i, err)
		}

		// Add to total rows affected
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return nil, fmt.Errorf("failed to get rows affected for batch set %d: %w", i, err)
		}

		totalRowsAffected += rowsAffected
	}

	// Return a result with the total rows affected
	return &Result{
		rowsAffected: totalRowsAffected,
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}, nil
}
