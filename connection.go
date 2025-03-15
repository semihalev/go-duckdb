package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"
)

/*
// Platform-specific linker flags to use the embedded static library
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/lib/linux/amd64 -lduckdb -lm -lpthread -lstdc++
#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/lib/linux/arm64 -lduckdb -lm -lpthread -lstdc++
#cgo darwin,amd64 LDFLAGS: -L${SRCDIR}/lib/darwin/amd64 -lduckdb -lm -lpthread -lstdc++
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/lib/darwin/arm64 -lduckdb -lm -lpthread -lstdc++
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/lib/windows/amd64 -lduckdb
// Include paths for the embedded headers
#cgo CFLAGS: -I${SRCDIR}/include
// Standard includes
#include <stdlib.h>
#include <duckdb.h>

// Version detection - for compatibility with DuckDB v1.2.0
#ifndef DUCKDB_VERSION_MAJOR
#define DUCKDB_VERSION_MAJOR 0
#endif

#ifndef DUCKDB_VERSION_MINOR
#define DUCKDB_VERSION_MINOR 0
#endif

#ifndef DUCKDB_VERSION_PATCH
#define DUCKDB_VERSION_PATCH 0
#endif

// Check if DuckDB version is at least 1.2.0
static inline int duckdb_version_at_least_1_2_0() {
    return (DUCKDB_VERSION_MAJOR > 1) || 
           (DUCKDB_VERSION_MAJOR == 1 && DUCKDB_VERSION_MINOR >= 2);
}
*/
import "C"

// conn implements database/sql/driver.Conn interface.
// It also implements driver.ExecerContext, driver.QueryerContext,
// and driver.ConnPrepareContext for more efficient operations.
type conn struct {
	db     *C.duckdb_database
	conn   *C.duckdb_connection
	closed atomic.Bool
	mu     sync.RWMutex
}

func newConnection(dsn string) (*conn, error) {
	cdsn := C.CString(dsn)
	defer C.free(unsafe.Pointer(cdsn))

	var db *C.duckdb_database
	var conn *C.duckdb_connection

	// Open the database
	if rc := C.duckdb_open(cdsn, &db); rc != C.DuckDBSuccess {
		err := errors.New("failed to open database")
		return nil, err
	}

	// Connect to the database
	if rc := C.duckdb_connect(db, &conn); rc != C.DuckDBSuccess {
		C.duckdb_close(&db)
		err := errors.New("failed to connect to database")
		return nil, err
	}

	// Initialize connection with atomic fields
	c := &conn{
		db:   db,
		conn: conn,
	}
	
	// Apply configuration specific to DuckDB 1.2.0+
	version := GetDuckDBVersion()
	if version.IsAtLeast120() {
		// In DuckDB 1.2.0+, set default settings for better compatibility and performance
		execSettings := []struct {
			setting string
			value   string
		}{
			// Enable standard compliant NULL handling for better SQL compatibility
			{"sql_standard_null_handling", "true"},
			// Ensure proper preserving of inserted timestamps
			{"preserve_insertion_order", "true"},
		}
		
		for _, setting := range execSettings {
			query := fmt.Sprintf("SET %s=%s", setting.setting, setting.value)
			cQuery := C.CString(query)
			var result *C.duckdb_result
			C.duckdb_query(conn, cQuery, result)
			C.free(unsafe.Pointer(cQuery))
		}
	}
	
	return c, nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext prepares a statement for execution with context
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.prepare(query)
}

func (c *conn) prepare(query string) (*stmt, error) {
	cquery := C.CString(query)
	defer C.free(unsafe.Pointer(cquery))

	var s *C.duckdb_prepared_statement

	if rc := C.duckdb_prepare(c.conn, cquery, &s); rc != C.DuckDBSuccess {
		err := c.lastError()
		return nil, err
	}

	statement := &stmt{
		conn:      c,
		stmt:      s,
		paramMap:  make(map[string]int),
	}
	
	// Initialize paramCount
	statement.paramCount = int(C.duckdb_nparams(s))
	
	// Try to extract named parameters if any
	// We need to parse the query string ourselves since DuckDB C API doesn't expose parameter names
	statement.extractNamedParams(query)
	
	return statement, nil
}

func (c *conn) Close() error {
	// Atomically check and set closed flag
	if c.closed.Swap(true) {
		return nil // Already closed
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	C.duckdb_disconnect(&c.conn)
	C.duckdb_close(&c.db)

	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts a new transaction with the given context and options.
// It implements the driver.ConnBeginTx interface.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}

	// Check for context cancellation first
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Continue with execution
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	tx := &tx{conn: c}

	// Set isolation level if specified
	if opts.Isolation != driver.IsolationLevel(0) {
		// DuckDB only supports serializable isolation level
		if opts.Isolation > driver.IsolationLevel(2) {
			return nil, errors.New("isolation level not supported")
		}
	}

	// Start transaction
	if _, err := c.ExecContext(ctx, "BEGIN TRANSACTION", nil); err != nil {
		return nil, err
	}

	return tx, nil
}

// ExecContext implements the driver.ExecerContext interface.
// This allows the driver to execute queries directly without preparing a statement.
// It also supports named parameters through the NamedValueChecker interface.
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}

	// Check for context cancellation first for early exit
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Continue with execution
	}

	c.mu.RLock()
	s, err := c.prepare(query)
	c.mu.RUnlock()
	
	if err != nil {
		return nil, err
	}
	defer s.Close()

	// Convert args to values using generics
	dargs, err := namedValueToValue[driver.NamedValue, driver.Value](args)
	if err != nil {
		return nil, err
	}

	return s.execContext(ctx, dargs)
}

// lastError provides a thread-safe way to get the last error from the connection
func (c *conn) lastError() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	error := C.duckdb_connection_error(c.conn)
	defer C.free(unsafe.Pointer(error))

	return errors.New(C.GoString(error))
}

// Convert driver.NamedValue to driver.Value using generics for zero allocation
func namedValueToValue[T driver.NamedValue, U driver.Value](named []T) ([]U, error) {
	// Use unsafe techniques to avoid allocations
	namedHeader := (*reflect.SliceHeader)(unsafe.Pointer(&named))
	
	// Create a result slice that reuses the same backing array
	result := unsafe.Slice((*U)(unsafe.Pointer(namedHeader.Data)), namedHeader.Len)
	
	// We still need to extract the Value field from each NamedValue
	// but we avoid allocating a new slice
	for i, nv := range named {
		result[i] = U(nv.Value)
	}
	
	return result, nil
}

// QueryContext implements the driver.QueryerContext interface.
// This allows the driver to execute queries directly without preparing a statement.
// It also supports named parameters through the NamedValueChecker interface.
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.closed.Load() {
		return nil, driver.ErrBadConn
	}

	// Check for context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Continue with execution
	}

	c.mu.RLock()
	s, err := c.prepare(query)
	c.mu.RUnlock()
	
	if err != nil {
		return nil, err
	}
	
	// Unlike ExecContext, we don't want to defer s.Close() here because the rows will need
	// access to the statement. The statement will be closed when the rows are closed.
	
	// Convert args to values using generics
	dargs, err := namedValueToValue[driver.NamedValue, driver.Value](args)
	if err != nil {
		s.Close() // Close the statement if the query fails
		return nil, err
	}

	rows, err := s.queryContext(ctx, dargs)
	if err != nil {
		s.Close() // Close the statement if the query fails
		return nil, err
	}
	
	// The statement is owned by the rows and will be closed when the rows are closed
	// We need to cast to our internal rows type to set the proper flag
	rowsImpl := rows.(*rows)
	rowsImpl.stmtClosed = true
	
	return rowsImpl, nil
}
