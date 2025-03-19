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
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Statement represents a DuckDB prepared statement.
type Statement struct {
	conn   *Connection
	stmt   *C.duckdb_prepared_statement
	query  string
	params int
	closed int32
	mu     sync.Mutex
}

// newStatement creates a new DuckDB prepared statement.
func newStatement(conn *Connection, query string) (*Statement, error) {
	cQuery := cString(query)
	defer freeString(cQuery)

	var stmt C.duckdb_prepared_statement

	if err := C.duckdb_prepare(*conn.conn, cQuery, &stmt); err == C.DuckDBError {
		// Get error message safely - the error is still available even with an invalid stmt
		errorMsg := goString(C.duckdb_prepare_error(stmt))
		// Clean up the statement handle even if preparation failed
		C.duckdb_destroy_prepare(&stmt)
		return nil, fmt.Errorf("failed to prepare statement: %s", errorMsg)
	}

	// Get parameter count
	paramCount := int(C.duckdb_nparams(stmt))

	return &Statement{
		conn:   conn,
		stmt:   &stmt,
		query:  query,
		params: paramCount,
	}, nil
}

// Close closes the statement.
func (s *Statement) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stmt != nil {
		C.duckdb_destroy_prepare(s.stmt)
		s.stmt = nil
	}

	return nil
}

// NumInput returns the number of placeholder parameters.
func (s *Statement) NumInput() int {
	return s.params
}

// ExecContext executes a query that doesn't return rows, with context support.
func (s *Statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if atomic.LoadInt32(&s.closed) != 0 {
		return nil, errors.New("statement is closed")
	}

	if atomic.LoadInt32(&s.conn.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	// Check if context is already canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Context is still valid, proceed
	}

	// Acquire lock, but don't hold it during the entire operation
	s.mu.Lock()

	// First check - is the statement still valid?
	if s.stmt == nil {
		s.mu.Unlock()
		return nil, errors.New("statement is closed")
	}

	// Make a safe copy of the stmt pointer
	stmtPtr := s.stmt
	s.mu.Unlock()

	// Bind parameters without holding the mutex
	if err := bindParameters(stmtPtr, args); err != nil {
		return nil, err
	}

	// Check context again before executing
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Proceed with execution
	}

	// Execute statement without holding the mutex
	var result C.duckdb_result
	if err := C.duckdb_execute_prepared(*stmtPtr, &result); err == C.DuckDBError {
		return nil, fmt.Errorf("failed to execute statement: %s", goString(C.duckdb_result_error(&result)))
	}
	defer C.duckdb_destroy_result(&result)

	// Get affected rows
	rowsAffected := C.duckdb_rows_changed(&result)

	return &Result{
		rowsAffected: int64(rowsAffected),
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}, nil
}

// Exec executes a query that doesn't return rows.
// Deprecated: Use ExecContext instead.
func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	if atomic.LoadInt32(&s.closed) != 0 {
		return nil, errors.New("statement is closed")
	}

	if atomic.LoadInt32(&s.conn.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	// Acquire lock, but don't hold it during the entire operation
	s.mu.Lock()

	// First check - is the statement still valid?
	if s.stmt == nil {
		s.mu.Unlock()
		return nil, errors.New("statement is closed")
	}

	// Make a safe copy of the stmt pointer
	stmtPtr := s.stmt
	s.mu.Unlock()

	// Get named arguments buffer from pool to reduce allocations
	namedArgs := globalBufferPool.GetNamedArgsBuffer(len(args))
	defer globalBufferPool.PutNamedArgsBuffer(namedArgs) // Return to pool when done

	// Convert to named values for binding
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}

	// Bind parameters without holding the mutex
	if err := bindParameters(stmtPtr, namedArgs); err != nil {
		return nil, err
	}

	// Execute statement without holding the mutex
	var result C.duckdb_result
	if err := C.duckdb_execute_prepared(*stmtPtr, &result); err == C.DuckDBError {
		return nil, fmt.Errorf("failed to execute statement: %s", goString(C.duckdb_result_error(&result)))
	}
	defer C.duckdb_destroy_result(&result)

	// Get affected rows
	rowsAffected := C.duckdb_rows_changed(&result)

	return &Result{
		rowsAffected: int64(rowsAffected),
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}, nil
}

// QueryContext executes a query that may return rows with context support.
func (s *Statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if atomic.LoadInt32(&s.closed) != 0 {
		return nil, errors.New("statement is closed")
	}

	if atomic.LoadInt32(&s.conn.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	// Check if context is already canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Context is still valid, proceed
	}

	// Acquire lock, but don't hold it during the entire operation
	s.mu.Lock()

	// First check - is the statement still valid?
	if s.stmt == nil {
		s.mu.Unlock()
		return nil, errors.New("statement is closed")
	}

	// Make a safe copy of the stmt pointer
	stmtPtr := s.stmt
	s.mu.Unlock()

	// Bind parameters without holding the mutex
	if err := bindParameters(stmtPtr, args); err != nil {
		return nil, err
	}

	// Check context again before executing
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Proceed with execution
	}

	// Get a result set wrapper from the pool
	wrapper := globalBufferPool.GetResultSetWrapper()

	// Execute statement with pooled result - pass by address since it's a struct now
	if err := C.duckdb_execute_prepared(*stmtPtr, &wrapper.result); err == C.DuckDBError {
		// Get error message before returning wrapper to pool
		errorMsg := goString(C.duckdb_result_error(&wrapper.result))
		globalBufferPool.PutResultSetWrapper(wrapper)

		return nil, fmt.Errorf("failed to execute statement: %s", errorMsg)
	}

	// Create rows with the pooled wrapper
	// The Rows object will be responsible for returning the wrapper to the pool
	return newRowsWithWrapper(wrapper), nil
}

// Query executes a query that may return rows.
// Deprecated: Use QueryContext instead.
func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	if atomic.LoadInt32(&s.closed) != 0 {
		return nil, errors.New("statement is closed")
	}

	if atomic.LoadInt32(&s.conn.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	// Acquire lock, but don't hold it during the entire operation
	s.mu.Lock()

	// First check - is the statement still valid?
	if s.stmt == nil {
		s.mu.Unlock()
		return nil, errors.New("statement is closed")
	}

	// Make a safe copy of the stmt pointer
	stmtPtr := s.stmt
	s.mu.Unlock()

	// Get named arguments buffer from pool to reduce allocations
	namedArgs := globalBufferPool.GetNamedArgsBuffer(len(args))
	defer globalBufferPool.PutNamedArgsBuffer(namedArgs) // Return to pool when done

	// Convert to named values for binding
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}

	// Bind parameters without holding the mutex
	if err := bindParameters(stmtPtr, namedArgs); err != nil {
		return nil, err
	}

	// Get a result set wrapper from the pool
	// The wrapper comes pre-allocated and ready to use
	wrapper := globalBufferPool.GetResultSetWrapper()

	// Execute statement with pooled result - pass by address since it's a struct now
	if err := C.duckdb_execute_prepared(*stmtPtr, &wrapper.result); err == C.DuckDBError {
		// Get error message before returning wrapper to pool
		errorMsg := goString(C.duckdb_result_error(&wrapper.result))
		globalBufferPool.PutResultSetWrapper(wrapper)

		return nil, fmt.Errorf("failed to execute statement: %s", errorMsg)
	}

	// Create rows with the pooled wrapper
	// The Rows object will be responsible for returning the wrapper to the pool
	return newRowsWithWrapper(wrapper), nil
}

// Helper function to bind parameters to a prepared statement
func bindParameters(stmt *C.duckdb_prepared_statement, args []driver.NamedValue) error {
	// Use panic recovery to ensure proper cleanup even if there's a panic during binding
	defer func() {
		if r := recover(); r != nil {
			// A panic occurred during binding
			// Log or handle as needed - at minimum, don't let the panic propagate
			// If there's a logging framework, this would be a good place to log the error
		}
	}()

	for i, arg := range args {
		idx := C.idx_t(i + 1) // Parameters are 1-indexed in DuckDB

		if arg.Value == nil {
			if err := C.duckdb_bind_null(*stmt, idx); err == C.DuckDBError {
				return fmt.Errorf("failed to bind NULL parameter at index %d", i)
			}
			continue
		}

		// Bind based on type
		switch v := arg.Value.(type) {
		case bool:
			// Use int8_t for boolean binding since C bool is challenging with CGo
			// DuckDB API will treat non-zero as true and zero as false
			if err := C.duckdb_bind_int8(*stmt, idx, boolToInt8(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind boolean parameter at index %d", i)
			}

		case int, int8, int16, int32:
			val := reflect.ValueOf(v).Int()
			if err := C.duckdb_bind_int32(*stmt, idx, C.int32_t(val)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind int32 parameter at index %d", i)
			}

		case int64:
			if err := C.duckdb_bind_int64(*stmt, idx, C.int64_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind int64 parameter at index %d", i)
			}

		case uint, uint8, uint16, uint32:
			val := reflect.ValueOf(v).Uint()
			if err := C.duckdb_bind_uint32(*stmt, idx, C.uint32_t(val)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind uint32 parameter at index %d", i)
			}

		case uint64:
			if err := C.duckdb_bind_uint64(*stmt, idx, C.uint64_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind uint64 parameter at index %d", i)
			}

		case float32:
			if err := C.duckdb_bind_float(*stmt, idx, C.float(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind float parameter at index %d", i)
			}

		case float64:
			if err := C.duckdb_bind_double(*stmt, idx, C.double(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind double parameter at index %d", i)
			}

		case string:
			cStr := cString(v)
			// Use individual defer for each string to prevent potential memory leaks
			// if later bindings cause a panic
			defer freeString(cStr)
			if err := C.duckdb_bind_varchar(*stmt, idx, cStr); err == C.DuckDBError {
				return fmt.Errorf("failed to bind string parameter at index %d", i)
			}

		case []byte:
			if len(v) == 0 {
				// For empty blobs, pass nil pointer with size 0
				// This is safer than using a temporary slice
				if err := C.duckdb_bind_blob(*stmt, idx, nil, C.idx_t(0)); err == C.DuckDBError {
					return fmt.Errorf("failed to bind empty blob parameter at index %d", i)
				}
			} else {
				// Safely get pointer to the first byte in the slice
				dataPtr := unsafe.Pointer(&v[0])

				if err := C.duckdb_bind_blob(*stmt, idx, dataPtr, C.idx_t(len(v))); err == C.DuckDBError {
					return fmt.Errorf("failed to bind blob parameter at index %d", i)
				}
			}

		case time.Time:
			// Convert to DuckDB timestamp (microseconds since 1970-01-01)
			micros := v.Unix()*1000000 + int64(v.Nanosecond())/1000
			ts := C.duckdb_timestamp{micros: C.int64_t(micros)}
			if err := C.duckdb_bind_timestamp(*stmt, idx, ts); err == C.DuckDBError {
				return fmt.Errorf("failed to bind timestamp parameter at index %d", i)
			}

		default:
			return fmt.Errorf("unsupported parameter type %T at index %d", v, i)
		}
	}

	return nil
}
