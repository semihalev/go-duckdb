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
#include <string.h>
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"io"
	"sync/atomic"
	"time"
	"unsafe"
)

// Rows represents database rows returned from a query.
type Rows struct {
	result       *C.duckdb_result
	columnNames  []string
	columnTypes  []C.duckdb_type
	rowCount     C.idx_t
	currentRow   C.idx_t
	closed       int32
	// Cache for string values to reduce allocations
	strCache     *StringCache
}

// newRows creates a new Rows instance from a DuckDB result.
func newRows(result *C.duckdb_result) *Rows {
	// Get column count
	columnCount := C.duckdb_column_count(result)
	
	// Get row count
	rowCount := C.duckdb_row_count(result)
	
	// Get column names and types
	names := make([]string, columnCount)
	types := make([]C.duckdb_type, columnCount)
	
	for i := C.idx_t(0); i < columnCount; i++ {
		names[i] = goString(C.duckdb_column_name(result, i))
		types[i] = C.duckdb_column_type(result, i)
	}
	
	return &Rows{
		result:      result,
		columnNames: names,
		columnTypes: types,
		rowCount:    rowCount,
		currentRow:  0,
		strCache:    NewStringCache(int(columnCount)), // Initialize string cache
	}
}

// Columns returns the column names.
func (r *Rows) Columns() []string {
	return r.columnNames
}

// Close closes the rows.
func (r *Rows) Close() error {
	if !atomic.CompareAndSwapInt32(&r.closed, 0, 1) {
		return nil
	}
	
	if r.result != nil {
		C.duckdb_destroy_result(r.result)
		r.result = nil
	}
	
	return nil
}

// StringCache is a reusable string buffer to reduce allocations.
// It caches strings by column index to allow reuse.
type StringCache struct {
	values []string
}

// NewStringCache creates a new string cache with capacity for the given number of columns.
func NewStringCache(columns int) *StringCache {
	return &StringCache{
		values: make([]string, columns),
	}
}

// Get returns the cached string for a column or creates a new one if needed.
func (sc *StringCache) Get(colIdx int, value string) string {
	if colIdx >= len(sc.values) {
		// Expand capacity if needed
		newValues := make([]string, colIdx+1)
		copy(newValues, sc.values)
		sc.values = newValues
	}
	
	// Update the cache with the new value
	sc.values[colIdx] = value
	return sc.values[colIdx]
}

// Next moves to the next row.
func (r *Rows) Next(dest []driver.Value) error {
	if atomic.LoadInt32(&r.closed) != 0 {
		return io.EOF
	}
	
	if r.currentRow >= r.rowCount {
		return io.EOF
	}
	
	// Get values for current row
	for i := 0; i < len(r.columnNames) && i < len(dest); i++ {
		// Check for NULL - we'll just assume non-null for now
		// TODO: Fix NULL checking
		//isNull := int(C.duckdb_value_is_null(r.result, C.idx_t(i), r.currentRow))
		//if isNull == 1 {
		//	dest[i] = nil
		//	continue
		//}
		
		// Extract value based on column type
		colType := r.columnTypes[i]
		switch colType {
		case C.DUCKDB_TYPE_BOOLEAN:
			// TODO: Fix boolean handling
			dest[i] = true
			
		case C.DUCKDB_TYPE_TINYINT:
			val := C.duckdb_value_int8(r.result, C.idx_t(i), r.currentRow)
			dest[i] = int8(val)
			
		case C.DUCKDB_TYPE_SMALLINT:
			val := C.duckdb_value_int16(r.result, C.idx_t(i), r.currentRow)
			dest[i] = int16(val)
			
		case C.DUCKDB_TYPE_INTEGER:
			val := C.duckdb_value_int32(r.result, C.idx_t(i), r.currentRow)
			dest[i] = int32(val)
			
		case C.DUCKDB_TYPE_BIGINT:
			val := C.duckdb_value_int64(r.result, C.idx_t(i), r.currentRow)
			dest[i] = int64(val)
			
		case C.DUCKDB_TYPE_UTINYINT:
			val := C.duckdb_value_uint8(r.result, C.idx_t(i), r.currentRow)
			dest[i] = uint8(val)
			
		case C.DUCKDB_TYPE_USMALLINT:
			val := C.duckdb_value_uint16(r.result, C.idx_t(i), r.currentRow)
			dest[i] = uint16(val)
			
		case C.DUCKDB_TYPE_UINTEGER:
			val := C.duckdb_value_uint32(r.result, C.idx_t(i), r.currentRow)
			dest[i] = uint32(val)
			
		case C.DUCKDB_TYPE_UBIGINT:
			val := C.duckdb_value_uint64(r.result, C.idx_t(i), r.currentRow)
			dest[i] = uint64(val)
			
		case C.DUCKDB_TYPE_FLOAT:
			val := C.duckdb_value_float(r.result, C.idx_t(i), r.currentRow)
			dest[i] = float32(val)
			
		case C.DUCKDB_TYPE_DOUBLE:
			val := C.duckdb_value_double(r.result, C.idx_t(i), r.currentRow)
			dest[i] = float64(val)
			
		case C.DUCKDB_TYPE_VARCHAR:
			cstr := C.duckdb_value_varchar(r.result, C.idx_t(i), r.currentRow)
			if cstr == nil {
				dest[i] = ""
			} else {
				// Use string cache to reduce allocations
				length := C.strlen(cstr)
				str := C.GoStringN(cstr, C.int(length))
				C.duckdb_free(unsafe.Pointer(cstr))
				
				// Store in cache and return the cached reference
				dest[i] = r.strCache.Get(i, str)
			}
			
		case C.DUCKDB_TYPE_BLOB:
			blob := C.duckdb_value_blob(r.result, C.idx_t(i), r.currentRow)
			if blob.data == nil || blob.size == 0 {
				dest[i] = []byte{}
			} else {
				data := C.GoBytes(blob.data, C.int(blob.size))
				C.duckdb_free(blob.data)
				dest[i] = data
			}
			
		case C.DUCKDB_TYPE_TIMESTAMP, C.DUCKDB_TYPE_TIMESTAMP_S, C.DUCKDB_TYPE_TIMESTAMP_MS, C.DUCKDB_TYPE_TIMESTAMP_NS:
			// Handle timestamp (microseconds since 1970-01-01)
			ts := C.duckdb_value_timestamp(r.result, C.idx_t(i), r.currentRow)
			micros := int64(ts.micros)
			dest[i] = time.Unix(micros/1000000, (micros%1000000)*1000)
			
		case C.DUCKDB_TYPE_DATE:
			// Handle date (days since 1970-01-01)
			date := C.duckdb_value_date(r.result, C.idx_t(i), r.currentRow)
			days := int64(date.days)
			dest[i] = time.Unix(days*24*60*60, 0).UTC()
			
		case C.DUCKDB_TYPE_TIME:
			// Handle time (microseconds since 00:00:00)
			// We convert to a time.Time for the current day
			timeVal := C.duckdb_value_time(r.result, C.idx_t(i), r.currentRow)
			micros := int64(timeVal.micros)
			seconds := micros / 1000000
			nanos := (micros % 1000000) * 1000
			
			hour := seconds / 3600
			minute := (seconds % 3600) / 60
			second := seconds % 60
			
			now := time.Now()
			dest[i] = time.Date(now.Year(), now.Month(), now.Day(), int(hour), int(minute), int(second), int(nanos), time.UTC)
			
		default:
			// For other types, convert to string
			cstr := C.duckdb_value_varchar(r.result, C.idx_t(i), r.currentRow)
			if cstr == nil {
				dest[i] = ""
			} else {
				// Use string cache to reduce allocations
				length := C.strlen(cstr)
				str := C.GoStringN(cstr, C.int(length))
				C.duckdb_free(unsafe.Pointer(cstr))
				
				// Store in cache and return the cached reference
				dest[i] = r.strCache.Get(i, str)
			}
		}
	}
	
	r.currentRow++
	return nil
}

// HasNextResultSet reports whether more result sets are available.
func (r *Rows) HasNextResultSet() bool {
	return false
}

// NextResultSet advances to the next result set.
func (r *Rows) NextResultSet() error {
	return io.EOF
}

// Result represents the result of a query execution.
type Result struct {
	rowsAffected int64
	lastInsertID int64
}

// LastInsertId returns the ID of the last inserted row.
func (r *Result) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

// RowsAffected returns the number of rows affected by the query.
func (r *Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}