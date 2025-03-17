// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

/*
#include <stdlib.h>
#include <stdbool.h>
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"io"
	"time"
	"unsafe"
)

// BasicRows is a simplified implementation of driver.Rows for fallback mode
type BasicRows struct {
	result      *C.duckdb_result
	rowIdx      C.idx_t
	columnCount C.idx_t
	rowCount    C.idx_t
	columnNames []string
	closed      bool
}

// newRows creates a new basic rows instance for fallback mode
func newRows(result *C.duckdb_result) *BasicRows {
	columnCount := C.duckdb_column_count(result)
	rowCount := C.duckdb_row_count(result)

	// Fetch column names
	columnNames := make([]string, columnCount)
	for i := C.idx_t(0); i < columnCount; i++ {
		columnNames[i] = C.GoString(C.duckdb_column_name(result, i))
	}

	return &BasicRows{
		result:      result,
		rowIdx:      0,
		columnCount: columnCount,
		rowCount:    rowCount,
		columnNames: columnNames,
	}
}

// Columns returns the names of the columns
func (r *BasicRows) Columns() []string {
	return r.columnNames
}

// Close closes the rows iterator
func (r *BasicRows) Close() error {
	if r.closed {
		return nil
	}

	if r.result != nil {
		C.duckdb_destroy_result(r.result)
		r.result = nil
	}

	r.closed = true
	return nil
}

// Next moves to the next row and returns data in dest
func (r *BasicRows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}

	if r.rowIdx >= r.rowCount {
		return io.EOF
	}

	// Extract values for the current row
	for i := C.idx_t(0); i < r.columnCount && i < C.idx_t(len(dest)); i++ {
		// First check if NULL
		if cBoolToGo(C.duckdb_value_is_null(r.result, i, r.rowIdx)) {
			dest[i] = nil
			continue
		}

		// Extract based on column type
		columnType := C.duckdb_column_type(r.result, i)
		switch columnType {
		case C.DUCKDB_TYPE_BOOLEAN:
			boolVal := C.duckdb_value_boolean(r.result, i, r.rowIdx)
			dest[i] = cBoolToGo(boolVal)

		case C.DUCKDB_TYPE_TINYINT:
			dest[i] = int8(C.duckdb_value_int8(r.result, i, r.rowIdx))

		case C.DUCKDB_TYPE_SMALLINT:
			dest[i] = int16(C.duckdb_value_int16(r.result, i, r.rowIdx))

		case C.DUCKDB_TYPE_INTEGER:
			dest[i] = int32(C.duckdb_value_int32(r.result, i, r.rowIdx))

		case C.DUCKDB_TYPE_BIGINT:
			dest[i] = int64(C.duckdb_value_int64(r.result, i, r.rowIdx))

		case C.DUCKDB_TYPE_UTINYINT:
			dest[i] = uint8(C.duckdb_value_uint8(r.result, i, r.rowIdx))

		case C.DUCKDB_TYPE_USMALLINT:
			dest[i] = uint16(C.duckdb_value_uint16(r.result, i, r.rowIdx))

		case C.DUCKDB_TYPE_UINTEGER:
			dest[i] = uint32(C.duckdb_value_uint32(r.result, i, r.rowIdx))

		case C.DUCKDB_TYPE_UBIGINT:
			dest[i] = uint64(C.duckdb_value_uint64(r.result, i, r.rowIdx))

		case C.DUCKDB_TYPE_FLOAT:
			dest[i] = float32(C.duckdb_value_float(r.result, i, r.rowIdx))

		case C.DUCKDB_TYPE_DOUBLE:
			dest[i] = float64(C.duckdb_value_double(r.result, i, r.rowIdx))

		case C.DUCKDB_TYPE_VARCHAR:
			cstr := C.duckdb_value_varchar(r.result, i, r.rowIdx)
			dest[i] = C.GoString(cstr)
			C.duckdb_free(unsafe.Pointer(cstr))

		case C.DUCKDB_TYPE_BLOB:
			var blob C.duckdb_blob
			blob = C.duckdb_value_blob(r.result, i, r.rowIdx)
			if blob.data != nil && blob.size > 0 {
				bytes := C.GoBytes(unsafe.Pointer(blob.data), C.int(blob.size))
				dest[i] = bytes
				C.duckdb_free(blob.data)
			} else {
				dest[i] = []byte{}
			}

		case C.DUCKDB_TYPE_TIMESTAMP, C.DUCKDB_TYPE_TIMESTAMP_S, C.DUCKDB_TYPE_TIMESTAMP_MS, C.DUCKDB_TYPE_TIMESTAMP_NS:
			// Extract DuckDB timestamp (microseconds since 1970-01-01)
			ts := C.duckdb_value_timestamp(r.result, i, r.rowIdx)
			micros := int64(ts.micros)
			// Use Go-native time.Time
			dest[i] = microsToDuckDBTime(micros)

		case C.DUCKDB_TYPE_DATE:
			// Extract DuckDB date (days since 1970-01-01)
			date := C.duckdb_value_date(r.result, i, r.rowIdx)
			days := int64(date.days)
			// Convert days to seconds and create a time.Time
			dest[i] = time.Unix(days*24*60*60, 0).UTC()

		case C.DUCKDB_TYPE_TIME:
			// Extract DuckDB time (microseconds since 00:00:00)
			timeVal := C.duckdb_value_time(r.result, i, r.rowIdx)
			micros := int64(timeVal.micros)
			seconds := micros / 1000000
			nanos := (micros % 1000000) * 1000

			hour := seconds / 3600
			minute := (seconds % 3600) / 60
			second := seconds % 60

			// Using UTC date with just the time component
			dest[i] = time.Date(1970, 1, 1, int(hour), int(minute), int(second), int(nanos), time.UTC)

		default:
			// For other types, convert to string as a fallback
			cstr := C.duckdb_value_varchar(r.result, i, r.rowIdx)
			dest[i] = C.GoString(cstr)
			C.duckdb_free(unsafe.Pointer(cstr))
		}
	}

	r.rowIdx++
	return nil
}
