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
	"unsafe"
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

	// Always use fast driver implementation
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.FastPrepare(query)
}

// ExecContext executes a query without returning any rows.
func (c *Connection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// We need to check if we're using dynamic library with fallback mode
	// and avoid using the fast driver in that case
	if nativeLibLoaded {
		// Use fast driver for optimized performance
		return c.FastExecContext(ctx, query, args)
	} else {
		// Use standard, pure Go implementation for fallback mode
		// This path is taken when the dynamic library is not available
		var result C.duckdb_result

		// If we have parameters, we need to use a prepared statement
		if len(args) > 0 {
			// Prepare the query
			cQuery := cString(query)
			defer freeString(cQuery)

			var stmt C.duckdb_prepared_statement
			if err := C.duckdb_prepare(*c.conn, cQuery, &stmt); err == C.DuckDBError {
				return nil, fmt.Errorf("failed to prepare statement: %s", goString(C.duckdb_prepare_error(stmt)))
			}
			defer C.duckdb_destroy_prepare(&stmt)

			// Bind parameters
			for i, arg := range args {
				idx := C.idx_t(i + 1) // Parameters are 1-indexed in DuckDB
				if err := bindValue(stmt, idx, arg.Value); err != nil {
					return nil, err
				}
			}

			// Execute statement
			if err := C.duckdb_execute_prepared(stmt, &result); err == C.DuckDBError {
				errMsg := goString(C.duckdb_result_error(&result))
				C.duckdb_destroy_result(&result)
				return nil, fmt.Errorf("failed to execute statement: %s", errMsg)
			}
		} else {
			// Direct query without parameters
			cQuery := cString(query)
			defer freeString(cQuery)

			if err := C.duckdb_query(*c.conn, cQuery, &result); err == C.DuckDBError {
				errMsg := goString(C.duckdb_result_error(&result))
				C.duckdb_destroy_result(&result)
				return nil, fmt.Errorf("failed to execute query: %s", errMsg)
			}
		}

		// Extract affected rows
		rowsAffected := int64(C.duckdb_rows_changed(&result))
		C.duckdb_destroy_result(&result)

		return &Result{
			rowsAffected: rowsAffected,
			lastInsertID: 0, // DuckDB doesn't support last insert ID
		}, nil
	}
}

// bindValue binds a value to a prepared statement.
// This is a helper function for the fallback implementation.
func bindValue(stmt C.duckdb_prepared_statement, idx C.idx_t, value interface{}) error {
	if value == nil {
		if err := C.duckdb_bind_null(stmt, idx); err == C.DuckDBError {
			return fmt.Errorf("failed to bind NULL parameter at index %d", idx)
		}
		return nil
	}

	// Bind based on value type
	switch v := value.(type) {
	case bool:
		val := C.int8_t(0)
		if v {
			val = C.int8_t(1)
		}
		if err := C.duckdb_bind_int8(stmt, idx, val); err == C.DuckDBError {
			return fmt.Errorf("failed to bind boolean parameter at index %d", idx)
		}

	case int8:
		if err := C.duckdb_bind_int8(stmt, idx, C.int8_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to bind int8 parameter at index %d", idx)
		}

	case int16:
		if err := C.duckdb_bind_int16(stmt, idx, C.int16_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to bind int16 parameter at index %d", idx)
		}

	case int32:
		if err := C.duckdb_bind_int32(stmt, idx, C.int32_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to bind int32 parameter at index %d", idx)
		}

	case int:
		if err := C.duckdb_bind_int64(stmt, idx, C.int64_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to bind int parameter at index %d", idx)
		}

	case int64:
		if err := C.duckdb_bind_int64(stmt, idx, C.int64_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to bind int64 parameter at index %d", idx)
		}

	case uint8:
		if err := C.duckdb_bind_uint8(stmt, idx, C.uint8_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to bind uint8 parameter at index %d", idx)
		}

	case uint16:
		if err := C.duckdb_bind_uint16(stmt, idx, C.uint16_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to bind uint16 parameter at index %d", idx)
		}

	case uint32:
		if err := C.duckdb_bind_uint32(stmt, idx, C.uint32_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to bind uint32 parameter at index %d", idx)
		}

	case uint:
		if err := C.duckdb_bind_uint64(stmt, idx, C.uint64_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to bind uint parameter at index %d", idx)
		}

	case uint64:
		if err := C.duckdb_bind_uint64(stmt, idx, C.uint64_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to bind uint64 parameter at index %d", idx)
		}

	case float32:
		if err := C.duckdb_bind_float(stmt, idx, C.float(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to bind float32 parameter at index %d", idx)
		}

	case float64:
		if err := C.duckdb_bind_double(stmt, idx, C.double(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to bind float64 parameter at index %d", idx)
		}

	case string:
		cStr := cString(v)
		defer freeString(cStr)
		if err := C.duckdb_bind_varchar(stmt, idx, cStr); err == C.DuckDBError {
			return fmt.Errorf("failed to bind string parameter at index %d", idx)
		}

	case []byte:
		if len(v) == 0 {
			if err := C.duckdb_bind_blob(stmt, idx, nil, C.idx_t(0)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind empty blob parameter at index %d", idx)
			}
		} else {
			if err := C.duckdb_bind_blob(stmt, idx, unsafe.Pointer(&v[0]), C.idx_t(len(v))); err == C.DuckDBError {
				return fmt.Errorf("failed to bind blob parameter at index %d", idx)
			}
		}

	case time.Time:
		// Convert to DuckDB timestamp (microseconds since 1970-01-01)
		micros := v.Unix()*1000000 + int64(v.Nanosecond())/1000
		if err := C.duckdb_bind_timestamp(stmt, idx, C.duckdb_timestamp{micros: C.int64_t(micros)}); err == C.DuckDBError {
			return fmt.Errorf("failed to bind timestamp parameter at index %d", idx)
		}

	default:
		return fmt.Errorf("unsupported parameter type %T at index %d", v, idx)
	}

	return nil
}

// QueryContext executes a query with the provided context.
func (c *Connection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if atomic.LoadInt32(&c.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// We need to check if we're using dynamic library with fallback mode
	// and avoid using the fast driver in that case
	if nativeLibLoaded {
		// Use fast driver for optimized performance
		return c.FastQueryContext(ctx, query, args)
	} else {
		// Use standard, pure Go implementation for fallback mode
		// This path is taken when the dynamic library is not available
		var result C.duckdb_result

		// If we have parameters, we need to use a prepared statement
		if len(args) > 0 {
			// Prepare the query
			cQuery := cString(query)
			defer freeString(cQuery)

			var stmt C.duckdb_prepared_statement
			if err := C.duckdb_prepare(*c.conn, cQuery, &stmt); err == C.DuckDBError {
				return nil, fmt.Errorf("failed to prepare statement: %s", goString(C.duckdb_prepare_error(stmt)))
			}
			defer C.duckdb_destroy_prepare(&stmt)

			// Bind parameters
			for i, arg := range args {
				idx := C.idx_t(i + 1) // Parameters are 1-indexed in DuckDB
				if err := bindValue(stmt, idx, arg.Value); err != nil {
					return nil, err
				}
			}

			// Execute statement
			if err := C.duckdb_execute_prepared(stmt, &result); err == C.DuckDBError {
				errMsg := goString(C.duckdb_result_error(&result))
				C.duckdb_destroy_result(&result)
				return nil, fmt.Errorf("failed to execute statement: %s", errMsg)
			}
		} else {
			// Direct query without parameters
			cQuery := cString(query)
			defer freeString(cQuery)

			if err := C.duckdb_query(*c.conn, cQuery, &result); err == C.DuckDBError {
				errMsg := goString(C.duckdb_result_error(&result))
				C.duckdb_destroy_result(&result)
				return nil, fmt.Errorf("failed to execute query: %s", errMsg)
			}
		}

		// Create rows from result
		return newRows(&result), nil
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

	// Only lock during the preparation and parameter binding phase
	c.mu.Lock()
	if atomic.LoadInt32(&c.closed) != 0 {
		c.mu.Unlock()
		return nil, ErrConnectionClosed
	}

	// Convert query to C string (under lock to protect from concurrent frees)
	cQuery := cString(query)
	c.mu.Unlock() // Release lock before the actual query execution

	defer freeString(cQuery)

	// Execute query without holding the connection lock
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
func (conn *Connection) BatchExec(query string, parameterSets [][]interface{}) (driver.Result, error) {
	if atomic.LoadInt32(&conn.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	// Prepare the batch statement
	stmt, err := NewOptimizedBatchStmt(conn, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	// Execute the batch
	return stmt.ExecBatch(parameterSets)
}
