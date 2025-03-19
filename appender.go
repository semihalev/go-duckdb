// Package duckdb provides low-level, high-performance SQL driver for DuckDB in Go.
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

	// Get column count directly from the appender
	columnCount := int(C.duckdb_appender_column_count(appender))

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

// AppendRows efficiently appends multiple rows to the table with a single lock acquisition.
// This significantly improves performance when inserting large numbers of rows by reducing lock contention.
func (a *Appender) AppendRows(rows [][]interface{}) error {
	if atomic.LoadInt32(&a.closed) != 0 {
		return fmt.Errorf("appender is closed")
	}

	if len(rows) == 0 {
		return nil
	}

	// Acquire lock once for all rows
	a.mu.Lock()
	defer a.mu.Unlock()

	// Process all rows under a single lock
	var appendError error
	for rowIdx, row := range rows {
		// Validate row length
		if len(row) != a.columnCount {
			appendError = fmt.Errorf("row %d has wrong number of values: got %d, expected %d",
				rowIdx, len(row), a.columnCount)
			break
		}

		// Append each value in the row
		for colIdx, value := range row {
			if err := a.appendValue(value); err != nil {
				appendError = fmt.Errorf("failed to append value at row %d, column %d: %v",
					rowIdx, colIdx, err)
				break
			}
		}

		if appendError != nil {
			break
		}

		// End this row
		if err := C.duckdb_appender_end_row(*a.appender); err == C.DuckDBError {
			appendError = fmt.Errorf("failed to end row %d: %s",
				rowIdx, goString(C.duckdb_appender_error(*a.appender)))
			break
		}
	}

	// If we encountered an error, we should try to flush what we've done so far
	if appendError != nil {
		// Try to flush but don't overwrite the original error
		_ = C.duckdb_appender_flush(*a.appender)
		return appendError
	}

	return nil
}

// AppendBatch is an optimized method for appending large batches of homogeneous data.
// This method minimizes CGO boundary crossings by using a more direct approach for each data type.
// It's ideal for data science and analytics workloads where you have arrays of primitive types.
func (a *Appender) AppendBatch(columns []interface{}) error {
	if atomic.LoadInt32(&a.closed) != 0 {
		return fmt.Errorf("appender is closed")
	}

	if len(columns) != a.columnCount {
		return fmt.Errorf("wrong number of columns: got %d, expected %d", len(columns), a.columnCount)
	}

	// Determine batch size from first column
	var batchSize int
	switch col := columns[0].(type) {
	case []bool:
		batchSize = len(col)
	case []int8:
		batchSize = len(col)
	case []int16:
		batchSize = len(col)
	case []int32:
		batchSize = len(col)
	case []int:
		batchSize = len(col)
	case []int64:
		batchSize = len(col)
	case []uint8:
		batchSize = len(col)
	case []uint16:
		batchSize = len(col)
	case []uint32:
		batchSize = len(col)
	case []uint:
		batchSize = len(col)
	case []uint64:
		batchSize = len(col)
	case []float32:
		batchSize = len(col)
	case []float64:
		batchSize = len(col)
	case []string:
		batchSize = len(col)
	case [][]byte:
		batchSize = len(col)
	case []time.Time:
		batchSize = len(col)
	default:
		return fmt.Errorf("unsupported column type %T for batch append", columns[0])
	}

	// Make sure all columns have the same length
	for i, col := range columns {
		var colLen int
		switch v := col.(type) {
		case []bool:
			colLen = len(v)
		case []int8:
			colLen = len(v)
		case []int16:
			colLen = len(v)
		case []int32:
			colLen = len(v)
		case []int:
			colLen = len(v)
		case []int64:
			colLen = len(v)
		case []uint8:
			colLen = len(v)
		case []uint16:
			colLen = len(v)
		case []uint32:
			colLen = len(v)
		case []uint:
			colLen = len(v)
		case []uint64:
			colLen = len(v)
		case []float32:
			colLen = len(v)
		case []float64:
			colLen = len(v)
		case []string:
			colLen = len(v)
		case [][]byte:
			colLen = len(v)
		case []time.Time:
			colLen = len(v)
		default:
			return fmt.Errorf("unsupported column type %T at index %d for batch append", col, i)
		}

		if colLen != batchSize {
			return fmt.Errorf("column length mismatch: column %d has length %d, expected %d", i, colLen, batchSize)
		}
	}

	// Acquire lock once for entire batch
	a.mu.Lock()
	defer a.mu.Unlock()

	// Process the batch row by row
	for rowIdx := 0; rowIdx < batchSize; rowIdx++ {
		// Append each value in this row
		for colIdx, col := range columns {
			var err error

			// Extract value from slice based on column type
			switch v := col.(type) {
			case []bool:
				err = a.appendBooleanDirect(v[rowIdx])
			case []int8:
				err = a.appendInt8Direct(v[rowIdx])
			case []int16:
				err = a.appendInt16Direct(v[rowIdx])
			case []int32:
				err = a.appendInt32Direct(v[rowIdx])
			case []int:
				err = a.appendInt64Direct(int64(v[rowIdx]))
			case []int64:
				err = a.appendInt64Direct(v[rowIdx])
			case []uint8:
				err = a.appendUint8Direct(v[rowIdx])
			case []uint16:
				err = a.appendUint16Direct(v[rowIdx])
			case []uint32:
				err = a.appendUint32Direct(v[rowIdx])
			case []uint:
				err = a.appendUint64Direct(uint64(v[rowIdx]))
			case []uint64:
				err = a.appendUint64Direct(v[rowIdx])
			case []float32:
				err = a.appendFloat32Direct(v[rowIdx])
			case []float64:
				err = a.appendFloat64Direct(v[rowIdx])
			case []string:
				err = a.appendStringDirect(v[rowIdx])
			case [][]byte:
				err = a.appendBlobDirect(v[rowIdx])
			case []time.Time:
				err = a.appendTimeDirect(v[rowIdx])
			}

			if err != nil {
				return fmt.Errorf("failed to append value at row %d, column %d: %v", rowIdx, colIdx, err)
			}
		}

		// End this row
		if err := C.duckdb_appender_end_row(*a.appender); err == C.DuckDBError {
			return fmt.Errorf("failed to end row %d: %s", rowIdx, goString(C.duckdb_appender_error(*a.appender)))
		}
	}

	return nil
}

// Direct appending methods to minimize overhead for batch operations

func (a *Appender) appendBooleanDirect(value bool) error {
	var val C.bool
	if value {
		val = true
	}
	if err := C.duckdb_append_bool(*a.appender, val); err == C.DuckDBError {
		return fmt.Errorf("failed to append boolean: %s", goString(C.duckdb_appender_error(*a.appender)))
	}
	return nil
}

func (a *Appender) appendInt8Direct(value int8) error {
	if err := C.duckdb_append_int8(*a.appender, C.int8_t(value)); err == C.DuckDBError {
		return fmt.Errorf("failed to append int8: %s", goString(C.duckdb_appender_error(*a.appender)))
	}
	return nil
}

func (a *Appender) appendInt16Direct(value int16) error {
	if err := C.duckdb_append_int16(*a.appender, C.int16_t(value)); err == C.DuckDBError {
		return fmt.Errorf("failed to append int16: %s", goString(C.duckdb_appender_error(*a.appender)))
	}
	return nil
}

func (a *Appender) appendInt32Direct(value int32) error {
	if err := C.duckdb_append_int32(*a.appender, C.int32_t(value)); err == C.DuckDBError {
		return fmt.Errorf("failed to append int32: %s", goString(C.duckdb_appender_error(*a.appender)))
	}
	return nil
}

func (a *Appender) appendInt64Direct(value int64) error {
	if err := C.duckdb_append_int64(*a.appender, C.int64_t(value)); err == C.DuckDBError {
		return fmt.Errorf("failed to append int64: %s", goString(C.duckdb_appender_error(*a.appender)))
	}
	return nil
}

func (a *Appender) appendUint8Direct(value uint8) error {
	if err := C.duckdb_append_uint8(*a.appender, C.uint8_t(value)); err == C.DuckDBError {
		return fmt.Errorf("failed to append uint8: %s", goString(C.duckdb_appender_error(*a.appender)))
	}
	return nil
}

func (a *Appender) appendUint16Direct(value uint16) error {
	if err := C.duckdb_append_uint16(*a.appender, C.uint16_t(value)); err == C.DuckDBError {
		return fmt.Errorf("failed to append uint16: %s", goString(C.duckdb_appender_error(*a.appender)))
	}
	return nil
}

func (a *Appender) appendUint32Direct(value uint32) error {
	if err := C.duckdb_append_uint32(*a.appender, C.uint32_t(value)); err == C.DuckDBError {
		return fmt.Errorf("failed to append uint32: %s", goString(C.duckdb_appender_error(*a.appender)))
	}
	return nil
}

func (a *Appender) appendUint64Direct(value uint64) error {
	if err := C.duckdb_append_uint64(*a.appender, C.uint64_t(value)); err == C.DuckDBError {
		return fmt.Errorf("failed to append uint64: %s", goString(C.duckdb_appender_error(*a.appender)))
	}
	return nil
}

func (a *Appender) appendFloat32Direct(value float32) error {
	if err := C.duckdb_append_float(*a.appender, C.float(value)); err == C.DuckDBError {
		return fmt.Errorf("failed to append float32: %s", goString(C.duckdb_appender_error(*a.appender)))
	}
	return nil
}

func (a *Appender) appendFloat64Direct(value float64) error {
	if err := C.duckdb_append_double(*a.appender, C.double(value)); err == C.DuckDBError {
		return fmt.Errorf("failed to append float64: %s", goString(C.duckdb_appender_error(*a.appender)))
	}
	return nil
}

func (a *Appender) appendStringDirect(value string) error {
	cStr := cString(value)
	defer freeString(cStr)
	if err := C.duckdb_append_varchar(*a.appender, cStr); err == C.DuckDBError {
		return fmt.Errorf("failed to append string: %s", goString(C.duckdb_appender_error(*a.appender)))
	}
	return nil
}

func (a *Appender) appendBlobDirect(value []byte) error {
	if len(value) == 0 {
		// Empty blob - use a placeholder buffer
		buffer := make([]byte, 1)
		if err := C.duckdb_append_blob(*a.appender, unsafe.Pointer(&buffer[0]), 0); err == C.DuckDBError {
			return fmt.Errorf("failed to append empty blob: %s", goString(C.duckdb_appender_error(*a.appender)))
		}
	} else {
		if err := C.duckdb_append_blob(*a.appender, unsafe.Pointer(&value[0]), C.idx_t(len(value))); err == C.DuckDBError {
			return fmt.Errorf("failed to append blob: %s", goString(C.duckdb_appender_error(*a.appender)))
		}
	}
	return nil
}

func (a *Appender) appendTimeDirect(value time.Time) error {
	// Convert to DuckDB timestamp (microseconds since 1970-01-01)
	// Explicitly convert to UTC to ensure consistent behavior
	utcTime := value.UTC()
	// Calculate microseconds since epoch
	micros := utcTime.Unix()*1000000 + int64(utcTime.Nanosecond())/1000
	// Create a DuckDB timestamp from microseconds
	ts := C.duckdb_timestamp{micros: C.int64_t(micros)}
	if err := C.duckdb_append_timestamp(*a.appender, ts); err == C.DuckDBError {
		return fmt.Errorf("failed to append timestamp: %s", goString(C.duckdb_appender_error(*a.appender)))
	}
	return nil
}

// appendValue appends a single value to the row.
func (a *Appender) appendValue(value interface{}) error {
	// Handle nil values (SQL NULL)
	if value == nil {
		// Use DuckDB's dedicated NULL append function
		if err := C.duckdb_append_null(*a.appender); err == C.DuckDBError {
			return fmt.Errorf("failed to append NULL: %s", goString(C.duckdb_appender_error(*a.appender)))
		}
		return nil
	}

	// Append based on type
	switch v := value.(type) {
	case bool:
		// Use the DuckDB API's boolean append function directly
		if err := C.duckdb_append_bool(*a.appender, C.bool(v)); err == C.DuckDBError {
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
		// Explicitly convert to UTC to ensure consistent behavior
		utcTime := v.UTC()
		// Calculate microseconds since epoch
		micros := utcTime.Unix()*1000000 + int64(utcTime.Nanosecond())/1000
		// Create a DuckDB timestamp from microseconds
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
