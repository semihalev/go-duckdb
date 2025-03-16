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

// NewConnection creates a new connection to the DuckDB database.
func NewConnection(path string) (*Connection, error) {
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

	c.mu.Lock()
	defer c.mu.Unlock()

	stmt, err := newStatement(c, query)
	if err != nil {
		return nil, err
	}

	return stmt, nil
}

// ExecContext executes a query without returning any rows.
func (c *Connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Use the fast exec implementation for better performance
	// We still need a version that tracks rows affected which the standard API doesn't provide
	
	// Prepare the query string
	cQuery := cString(query)
	defer freeString(cQuery)

	// Check if we have parameters - if so, use prepared statement
	if len(args) > 0 {
		// Use a prepared statement
		var stmt C.duckdb_prepared_statement
		if err := C.duckdb_prepare(*c.conn, cQuery, &stmt); err == C.DuckDBError {
			return nil, fmt.Errorf("failed to prepare statement: %s", goString(C.duckdb_prepare_error(stmt)))
		}
		defer C.duckdb_destroy_prepare(&stmt)

		// Bind parameters
		if err := bindParameters(&stmt, args); err != nil {
			return nil, err
		}

		// Execute statement
		var result C.duckdb_result
		if err := C.duckdb_execute_prepared(stmt, &result); err == C.DuckDBError {
			return nil, fmt.Errorf("failed to execute statement: %s", goString(C.duckdb_result_error(&result)))
		}
		defer C.duckdb_destroy_result(&result)

		// Get affected rows
		rowsAffected := C.duckdb_rows_changed(&result)

		return &Result{
			rowsAffected: int64(rowsAffected),
			lastInsertID: 0, // DuckDB doesn't support last insert ID
		}, nil
	} else {
		// Direct query execution
		var result C.duckdb_result
		if err := C.duckdb_query(*c.conn, cQuery, &result); err == C.DuckDBError {
			return nil, fmt.Errorf("failed to execute query: %s", goString(C.duckdb_result_error(&result)))
		}
		defer C.duckdb_destroy_result(&result)

		// Get affected rows
		rowsAffected := C.duckdb_rows_changed(&result)

		return &Result{
			rowsAffected: int64(rowsAffected),
			lastInsertID: 0, // DuckDB doesn't support last insert ID
		}, nil
	}
}

// QueryContext executes a query with the provided context.
// It uses buffer pooling to minimize allocations.
func (c *Connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Prepare the query string
	cQuery := cString(query)
	defer freeString(cQuery)

	// Get a result set wrapper from the pool
	// The wrapper comes pre-allocated and ready to use
	wrapper := globalBufferPool.GetResultSetWrapper()

	// Check if we have parameters - if so, use prepared statement
	if len(args) > 0 {
		// Use a prepared statement
		var stmt C.duckdb_prepared_statement
		if err := C.duckdb_prepare(*c.conn, cQuery, &stmt); err == C.DuckDBError {
			globalBufferPool.PutResultSetWrapper(wrapper) // Return wrapper to pool
			return nil, fmt.Errorf("failed to prepare statement: %s", goString(C.duckdb_prepare_error(stmt)))
		}

		// Bind parameters
		if err := bindParameters(&stmt, args); err != nil {
			C.duckdb_destroy_prepare(&stmt)
			globalBufferPool.PutResultSetWrapper(wrapper)
			return nil, err
		}

		// Execute statement with pooled result - pass by address since it's a struct now
		if err := C.duckdb_execute_prepared(stmt, &wrapper.result); err == C.DuckDBError {
			C.duckdb_destroy_prepare(&stmt)

			// Get error message before returning wrapper to pool
			errorMsg := goString(C.duckdb_result_error(&wrapper.result))
			globalBufferPool.PutResultSetWrapper(wrapper)

			return nil, fmt.Errorf("failed to execute statement: %s", errorMsg)
		}

		// Clean up the prepared statement as we don't need it anymore
		C.duckdb_destroy_prepare(&stmt)

		// Create rows with the result wrapper
		// The Rows object will be responsible for returning the wrapper to the pool
		return newRowsWithWrapper(wrapper), nil
	} else {
		// Direct query execution with pooled result - pass by address since it's a struct now
		if err := C.duckdb_query(*c.conn, cQuery, &wrapper.result); err == C.DuckDBError {
			// Get error message before returning wrapper to pool
			errorMsg := goString(C.duckdb_result_error(&wrapper.result))
			globalBufferPool.PutResultSetWrapper(wrapper)

			return nil, fmt.Errorf("failed to execute query: %s", errorMsg)
		}

		// Create rows with the pooled wrapper
		// The Rows object will be responsible for returning the wrapper to the pool
		return newRowsWithWrapper(wrapper), nil
	}
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
	return c.ExecContext(context.Background(), query, valuesToNamedValues(args))
}

// Query implements the driver.Queryer interface
func (c *Connection) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, valuesToNamedValues(args))
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

// Helper function to convert driver.Value slice to driver.NamedValue slice
func valuesToNamedValues(args []driver.Value) []driver.NamedValue {
	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}
	return namedArgs
}
