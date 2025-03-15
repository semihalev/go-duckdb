package duckdb

import (
	"database/sql/driver"
	"io"
	"sync/atomic"
	"unsafe"
)

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

// rows implements database/sql/driver.Rows interface.
type rows struct {
	stmt     *stmt
	colCount int
	rowCount int
	rowIdx   int
	closed   atomic.Bool
}

func (r *rows) Columns() []string {
	if r.closed.Load() || r.stmt.result == nil {
		return nil
	}

	// Lock the statement while we read column information
	r.stmt.mu.RLock()
	defer r.stmt.mu.RUnlock()

	cols := make([]string, r.colCount)

	for i := 0; i < r.colCount; i++ {
		name := C.duckdb_column_name(r.stmt.result, C.idx_t(i))
		cols[i] = C.GoString(name)
	}

	return cols
}

func (r *rows) Close() error {
	// Atomically mark as closed to prevent concurrent access
	if r.closed.Swap(true) {
		return nil // Already closed
	}

	// Note: we don't destroy the result here since it's owned by the stmt
	// and will be destroyed when the stmt is closed
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.closed.Load() {
		return io.EOF
	}

	if r.rowIdx >= r.rowCount {
		return io.EOF
	}

	// Lock the statement while we read data from the result
	r.stmt.mu.RLock()
	defer r.stmt.mu.RUnlock()

	// Ensure the result is still valid
	if r.stmt.closed.Load() || r.stmt.result == nil {
		return driver.ErrBadConn
	}

	for i := 0; i < r.colCount; i++ {
		colIdx := C.idx_t(i)
		rowIdx := C.idx_t(r.rowIdx)

		// Check for NULL value
		if C.duckdb_value_is_null(r.stmt.result, colIdx, rowIdx) == C.bool(true) {
			dest[i] = nil
			continue
		}

		// Get column type
		colType := C.duckdb_column_type(r.stmt.result, colIdx)

		switch colType {
		case C.DUCKDB_TYPE_BOOLEAN:
			boolVal := C.duckdb_value_boolean(r.stmt.result, colIdx, rowIdx)
			dest[i] = bool(boolVal)
		case C.DUCKDB_TYPE_TINYINT, C.DUCKDB_TYPE_SMALLINT, C.DUCKDB_TYPE_INTEGER:
			intVal := C.duckdb_value_int32(r.stmt.result, colIdx, rowIdx)
			dest[i] = int32(intVal)
		case C.DUCKDB_TYPE_BIGINT:
			bigIntVal := C.duckdb_value_int64(r.stmt.result, colIdx, rowIdx)
			dest[i] = int64(bigIntVal)
		case C.DUCKDB_TYPE_FLOAT, C.DUCKDB_TYPE_DOUBLE:
			doubleVal := C.duckdb_value_double(r.stmt.result, colIdx, rowIdx)
			dest[i] = float64(doubleVal)
		case C.DUCKDB_TYPE_VARCHAR:
			strVal := C.duckdb_value_string(r.stmt.result, colIdx, rowIdx)
			// Create a Go string without allocating a copy using unsafe
			// This is safe because we copy the string data immediately
			str := C.GoString(strVal)
			C.free(unsafe.Pointer(strVal))
			dest[i] = str
		case C.DUCKDB_TYPE_BLOB:
			var dataSize C.idx_t
			blob := C.duckdb_value_blob(r.stmt.result, colIdx, rowIdx, &dataSize)
			blobBytes := C.GoBytes(unsafe.Pointer(blob), C.int(dataSize))
			dest[i] = blobBytes
		case C.DUCKDB_TYPE_TIMESTAMP:
			timestampVal := C.duckdb_value_timestamp(r.stmt.result, colIdx, rowIdx)
			// Convert microseconds to time.Time
			micros := int64(timestampVal.micros)
			dest[i] = timeFromMicros(micros)
		default:
			// For types we don't specifically handle, get as string
			strVal := C.duckdb_value_string(r.stmt.result, colIdx, rowIdx)
			str := C.GoString(strVal)
			C.free(unsafe.Pointer(strVal))
			dest[i] = str
		}
	}

	r.rowIdx++
	return nil
}
