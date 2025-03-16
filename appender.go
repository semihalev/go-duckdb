// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

/*
// Use only necessary includes here - CGO directives are defined in duckdb.go
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// Appender is used for efficiently appending data to a table.
type Appender struct {
	conn        *Connection
	appender    *C.duckdb_appender
	columnCount int
	closed      int32
	mu          sync.Mutex
}

// NewAppender creates a new appender for the given table.
func NewAppender(conn *Connection, schema, table string) (*Appender, error) {
	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}

	// Create appender
	var appender C.duckdb_appender

	cSchema := cString(schema)
	defer freeString(cSchema)

	cTable := cString(table)
	defer freeString(cTable)

	if err := C.duckdb_appender_create(*conn.conn, cSchema, cTable, &appender); err == C.DuckDBError {
		return nil, fmt.Errorf("failed to create appender: %s", goString(C.duckdb_appender_error(appender)))
	}

	// Get column count by executing a query
	var result C.duckdb_result
	query := fmt.Sprintf("SELECT * FROM %s.%s LIMIT 0", schema, table)
	cQuery := cString(query)
	defer freeString(cQuery)

	if err := C.duckdb_query(*conn.conn, cQuery, &result); err == C.DuckDBError {
		C.duckdb_appender_destroy(&appender)
		return nil, fmt.Errorf("failed to query table: %s", goString(C.duckdb_result_error(&result)))
	}
	defer C.duckdb_destroy_result(&result)

	// Get column count
	columnCount := int(C.duckdb_column_count(&result))

	a := &Appender{
		conn:        conn,
		appender:    &appender,
		columnCount: columnCount,
	}

	// Set finalizer to ensure appender is closed when garbage collected
	runtime.SetFinalizer(a, (*Appender).Close)

	return a, nil
}

// AppendRow appends a row of values to the table.
func (a *Appender) AppendRow(values ...interface{}) error {
	if atomic.LoadInt32(&a.closed) != 0 {
		return fmt.Errorf("appender is closed")
	}

	if len(values) != a.columnCount {
		return fmt.Errorf("wrong number of values: got %d, expected %d", len(values), a.columnCount)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Append each value
	for i, value := range values {
		if err := a.appendValue(value); err != nil {
			return fmt.Errorf("failed to append value at index %d: %v", i, err)
		}
	}

	// End row
	if err := C.duckdb_appender_end_row(*a.appender); err == C.DuckDBError {
		return fmt.Errorf("failed to end row: %s", goString(C.duckdb_appender_error(*a.appender)))
	}

	return nil
}

// appendValue appends a single value to the row.
func (a *Appender) appendValue(value interface{}) error {
	if value == nil {
		if err := C.duckdb_append_null(*a.appender); err == C.DuckDBError {
			return fmt.Errorf("failed to append NULL: %s", goString(C.duckdb_appender_error(*a.appender)))
		}
		return nil
	}

	// Append based on type
	switch v := value.(type) {
	case bool:
		// Convert bool to int8 (0 or 1) for DuckDB
		var boolVal C.int8_t
		if v {
			boolVal = C.int8_t(1)
		}
		if err := C.duckdb_append_int8(*a.appender, boolVal); err == C.DuckDBError {
			return fmt.Errorf("failed to append boolean: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	case int8:
		if err := C.duckdb_append_int8(*a.appender, C.int8_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to append int8: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	case int16:
		if err := C.duckdb_append_int16(*a.appender, C.int16_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to append int16: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	case int32:
		if err := C.duckdb_append_int32(*a.appender, C.int32_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to append int32: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	case int:
		if err := C.duckdb_append_int64(*a.appender, C.int64_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to append int: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	case int64:
		if err := C.duckdb_append_int64(*a.appender, C.int64_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to append int64: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	case uint8:
		if err := C.duckdb_append_uint8(*a.appender, C.uint8_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to append uint8: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	case uint16:
		if err := C.duckdb_append_uint16(*a.appender, C.uint16_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to append uint16: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	case uint32:
		if err := C.duckdb_append_uint32(*a.appender, C.uint32_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to append uint32: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	case uint:
		if err := C.duckdb_append_uint64(*a.appender, C.uint64_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to append uint: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	case uint64:
		if err := C.duckdb_append_uint64(*a.appender, C.uint64_t(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to append uint64: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	case float32:
		if err := C.duckdb_append_float(*a.appender, C.float(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to append float32: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	case float64:
		if err := C.duckdb_append_double(*a.appender, C.double(v)); err == C.DuckDBError {
			return fmt.Errorf("failed to append float64: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	case string:
		cStr := cString(v)
		defer freeString(cStr)
		if err := C.duckdb_append_varchar(*a.appender, cStr); err == C.DuckDBError {
			return fmt.Errorf("failed to append string: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	case []byte:
		if len(v) == 0 {
			// Empty blob - use a placeholder buffer
			buffer := make([]byte, 1)
			if err := C.duckdb_append_blob(*a.appender, unsafe.Pointer(&buffer[0]), 0); err == C.DuckDBError {
				return fmt.Errorf("failed to append empty blob: %s", goString(C.duckdb_appender_error(*a.appender)))
			}
		} else {
			if err := C.duckdb_append_blob(*a.appender, unsafe.Pointer(&v[0]), C.idx_t(len(v))); err == C.DuckDBError {
				return fmt.Errorf("failed to append blob: %s", goString(C.duckdb_appender_error(*a.appender)))
			}
		}

	case time.Time:
		// Convert to DuckDB timestamp (microseconds since 1970-01-01)
		micros := v.Unix()*1000000 + int64(v.Nanosecond())/1000
		ts := C.duckdb_timestamp{micros: C.int64_t(micros)}
		if err := C.duckdb_append_timestamp(*a.appender, ts); err == C.DuckDBError {
			return fmt.Errorf("failed to append timestamp: %s", goString(C.duckdb_appender_error(*a.appender)))
		}

	default:
		// Try to convert to string for unsupported types
		return fmt.Errorf("unsupported type for appender: %T", v)
	}

	return nil
}

// Flush flushes the appender to the database.
func (a *Appender) Flush() error {
	if atomic.LoadInt32(&a.closed) != 0 {
		return fmt.Errorf("appender is closed")
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if err := C.duckdb_appender_flush(*a.appender); err == C.DuckDBError {
		return fmt.Errorf("failed to flush appender: %s", goString(C.duckdb_appender_error(*a.appender)))
	}

	return nil
}

// Close closes the appender.
func (a *Appender) Close() error {
	if !atomic.CompareAndSwapInt32(&a.closed, 0, 1) {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.appender != nil {
		C.duckdb_appender_destroy(a.appender)
		a.appender = nil
	}

	runtime.SetFinalizer(a, nil)
	return nil
}
