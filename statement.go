package duckdb

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

// stmt implements database/sql/driver.Stmt interface.
type stmt struct {
	conn *conn
	stmt *C.duckdb_prepared_statement

	colCount int
	result   *C.duckdb_result
	closed   atomic.Bool
	mu       sync.RWMutex
}

func (s *stmt) Close() error {
	// Atomically check and set closed flag
	if s.closed.Swap(true) {
		return nil // Already closed
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.result != nil {
		C.duckdb_destroy_result(s.result)
		s.result = nil
	}

	C.duckdb_destroy_prepare(&s.stmt)
	return nil
}

func (s *stmt) NumInput() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int(C.duckdb_nparams(s.stmt))
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.execContext(context.Background(), args)
}

func (s *stmt) execContext(ctx context.Context, args []driver.Value) (driver.Result, error) {
	if s.closed.Load() {
		return nil, driver.ErrBadConn
	}

	// Check for context cancellation first
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Continue with execution
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(args) > 0 {
		if err := s.bindArgs(args); err != nil {
			return nil, err
		}
	}

	if s.result != nil {
		C.duckdb_destroy_result(s.result)
		s.result = nil
	}

	result := C.duckdb_result{}

	if rc := C.duckdb_execute_prepared(s.stmt, &result); rc != C.DuckDBSuccess {
		err := s.conn.lastError()
		return nil, err
	}

	s.result = &result
	s.colCount = int(C.duckdb_column_count(&result))

	// DuckDB doesn't have concept of affected rows for all operations
	// But we can get row count for operations that return rows
	rowCount := int64(C.duckdb_row_count(&result))

	return &result_{
		rowsAffected: rowCount,
	}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.queryContext(context.Background(), args)
}

func (s *stmt) queryContext(ctx context.Context, args []driver.Value) (driver.Rows, error) {
	if s.closed.Load() {
		return nil, driver.ErrBadConn
	}

	// Check for context cancellation first
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Continue with execution
	}

	_, err := s.execContext(ctx, args)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return &rows{
		stmt:     s,
		colCount: s.colCount,
		rowCount: int(C.duckdb_row_count(s.result)),
		rowIdx:   0,
	}, nil
}

func (s *stmt) bindArgs(args []driver.Value) error {
	expected := s.NumInput()
	if len(args) != expected {
		return errors.New("wrong argument count: expected " + strconv.Itoa(expected) + ", got " + strconv.Itoa(len(args)))
	}

	for i, arg := range args {
		idx := C.idx_t(i + 1) // DuckDB parameters are 1-indexed

		var rc C.duckdb_state

		switch v := arg.(type) {
		case nil:
			rc = C.duckdb_bind_null(s.stmt, idx)
		case bool:
			boolVal := C.bool(v)
			rc = C.duckdb_bind_boolean(s.stmt, idx, boolVal)
		case int64:
			rc = C.duckdb_bind_int64(s.stmt, idx, C.int64_t(v))
		case int:
			rc = C.duckdb_bind_int64(s.stmt, idx, C.int64_t(v))
		case int32:
			rc = C.duckdb_bind_int32(s.stmt, idx, C.int32_t(v))
		case float64:
			rc = C.duckdb_bind_double(s.stmt, idx, C.double(v))
		case string:
			cstr := C.CString(v)
			defer C.free(unsafe.Pointer(cstr))
			rc = C.duckdb_bind_varchar(s.stmt, idx, cstr)
		case []byte:
			if len(v) == 0 {
				rc = C.duckdb_bind_null(s.stmt, idx)
			} else {
				rc = C.duckdb_bind_blob(s.stmt, idx, unsafe.Pointer(&v[0]), C.idx_t(len(v)))
			}
		case time.Time:
			timestamp := C.duckdb_timestamp{
				micros: C.int64_t(v.UnixNano() / 1000),
			}
			rc = C.duckdb_bind_timestamp(s.stmt, idx, timestamp)
		default:
			return errors.New("unsupported type: " + reflect.TypeOf(arg).String())
		}

		if rc != C.DuckDBSuccess {
			return s.conn.lastError()
		}
	}

	return nil
}

// result_ implements database/sql/driver.Result interface.
type result_ struct {
	rowsAffected int64
}

func (r *result_) LastInsertId() (int64, error) {
	// DuckDB doesn't support last insert ID concept
	return 0, errors.New("LastInsertId is not supported by DuckDB")
}

func (r *result_) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// See rows.go for the implementation of the rows type
