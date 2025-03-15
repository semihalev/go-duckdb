// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

/*
#cgo CFLAGS: -I${SRCDIR}/include
#cgo darwin,amd64 LDFLAGS: -L${SRCDIR}/lib/darwin/amd64 -lduckdb -lstdc++
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/lib/darwin/arm64 -lduckdb -lstdc++
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/lib/linux/amd64 -lduckdb -lstdc++
#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/lib/linux/arm64 -lduckdb -lstdc++
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/lib/windows/amd64 -lduckdb -lstdc++
#cgo windows,arm64 LDFLAGS: -L${SRCDIR}/lib/windows/arm64 -lduckdb -lstdc++

#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
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
	conn    *Connection
	stmt    *C.duckdb_prepared_statement
	query   string
	params  int
	closed  int32
	mu      sync.Mutex
}

// newStatement creates a new DuckDB prepared statement.
func newStatement(conn *Connection, query string) (*Statement, error) {
	cQuery := cString(query)
	defer freeString(cQuery)

	var stmt C.duckdb_prepared_statement
	
	if err := C.duckdb_prepare(*conn.conn, cQuery, &stmt); err == C.DuckDBError {
		return nil, fmt.Errorf("failed to prepare statement: %s", goString(C.duckdb_prepare_error(stmt)))
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

// Exec executes a query that doesn't return rows.
func (s *Statement) Exec(args []driver.Value) (driver.Result, error) {
	if atomic.LoadInt32(&s.closed) != 0 {
		return nil, errors.New("statement is closed")
	}

	if atomic.LoadInt32(&s.conn.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	s.mu.Lock()
	defer s.mu.Unlock()

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

	// Bind parameters
	if err := bindParameters(s.stmt, namedArgs); err != nil {
		return nil, err
	}

	// Execute statement
	var result C.duckdb_result
	if err := C.duckdb_execute_prepared(*s.stmt, &result); err == C.DuckDBError {
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

// Query executes a query that may return rows.
func (s *Statement) Query(args []driver.Value) (driver.Rows, error) {
	if atomic.LoadInt32(&s.closed) != 0 {
		return nil, errors.New("statement is closed")
	}

	if atomic.LoadInt32(&s.conn.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	
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

	// Bind parameters
	if err := bindParameters(s.stmt, namedArgs); err != nil {
		return nil, err
	}

	// Execute statement
	var result C.duckdb_result
	if err := C.duckdb_execute_prepared(*s.stmt, &result); err == C.DuckDBError {
		return nil, fmt.Errorf("failed to execute statement: %s", goString(C.duckdb_result_error(&result)))
	}

	// Create rows
	return newRows(&result), nil
}

// Helper function to bind parameters to a prepared statement
func bindParameters(stmt *C.duckdb_prepared_statement, args []driver.NamedValue) error {
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
			defer freeString(cStr)
			if err := C.duckdb_bind_varchar(*stmt, idx, cStr); err == C.DuckDBError {
				return fmt.Errorf("failed to bind string parameter at index %d", i)
			}

		case []byte:
			if len(v) == 0 {
				if err := C.duckdb_bind_blob(*stmt, idx, unsafe.Pointer(&[]byte{0}[0]), C.idx_t(0)); err == C.DuckDBError {
					return fmt.Errorf("failed to bind empty blob parameter at index %d", i)
				}
			} else {
				if err := C.duckdb_bind_blob(*stmt, idx, unsafe.Pointer(&v[0]), C.idx_t(len(v))); err == C.DuckDBError {
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