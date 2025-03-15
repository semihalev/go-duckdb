package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"
)

/*
#cgo LDFLAGS: -lduckdb
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

// conn implements database/sql/driver.Conn interface.
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

	if rc := C.duckdb_open(cdsn, &db); rc != C.DuckDBSuccess {
		err := errors.New("failed to open database")
		return nil, err
	}

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
	
	return c, nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	if c.closed.Load() {
		return nil, driver.ErrBadConn
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

	return &stmt{
		conn: c,
		stmt: s,
	}, nil
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

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}

	// Check for context cancellation first for early exit
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Continue with execution
	}

	s, err := c.prepare(query)
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
