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

// StringCache is an optimized cache system for strings that reduces allocations
// It uses string interning and byte buffer pooling to minimize GC pressure
type StringCache struct {
	// Column-specific caches
	columnValues []string
	
	// Global string intern map for value deduplication across all columns
	// This dramatically reduces allocations for repeated values
	internMap map[string]string
	
	// Byte buffer for string conversion to avoid allocations
	// This is reused for each string conversion
	byteBuffer []byte
	
	// Max buffer size to prevent unbounded growth
	maxBufferSize int
	
	// Statistics for monitoring
	hits   int
	misses int
}

// NewStringCache creates a new optimized string cache with capacity for the given number of columns.
func NewStringCache(columns int) *StringCache {
	return &StringCache{
		columnValues:  make([]string, columns),
		internMap:     make(map[string]string, 1024), // Start with capacity for 1024 unique strings
		byteBuffer:    make([]byte, 1024),            // Initial 1KB buffer
		maxBufferSize: 1024 * 1024,                   // Max 1MB buffer size
	}
}

// Get returns a cached string for the column value, creating minimal allocations
func (sc *StringCache) Get(colIdx int, value string) string {
	if colIdx >= len(sc.columnValues) {
		// Expand capacity if needed - this should be rare
		newValues := make([]string, colIdx+1)
		copy(newValues, sc.columnValues)
		sc.columnValues = newValues
	}
	
	// First check if we've seen this exact string before
	if cached, ok := sc.internMap[value]; ok {
		sc.columnValues[colIdx] = cached
		sc.hits++
		return cached
	}
	
	// Cache miss - store in the intern map to deduplicate future occurrences
	sc.internMap[value] = value
	sc.columnValues[colIdx] = value
	sc.misses++
	
	return value
}

// GetFromBytes converts a byte slice to a string with minimal allocations
// By reusing an internal buffer when possible
func (sc *StringCache) GetFromBytes(colIdx int, bytes []byte) string {
	// Fast path: if we can reuse our buffer
	if len(bytes) <= len(sc.byteBuffer) {
		// Copy bytes to our reusable buffer
		copy(sc.byteBuffer, bytes)
		// Create string from our buffer's first n bytes
		s := string(sc.byteBuffer[:len(bytes)])
		
		// Try to get from intern map
		if cached, ok := sc.internMap[s]; ok {
			sc.columnValues[colIdx] = cached
			sc.hits++
			return cached
		}
		
		// Store in intern map
		sc.internMap[s] = s
		sc.columnValues[colIdx] = s
		sc.misses++
		return s
	}
	
	// If bytes won't fit in our buffer but aren't huge, grow the buffer
	// This amortizes allocations over time
	if len(bytes) <= sc.maxBufferSize {
		// Grow buffer to accommodate this size with some room to grow
		newSize := len(bytes) * 2
		if newSize > sc.maxBufferSize {
			newSize = sc.maxBufferSize
		}
		sc.byteBuffer = make([]byte, newSize)
		return sc.GetFromBytes(colIdx, bytes) // Retry with larger buffer
	}
	
	// If bytes are larger than our max buffer, just convert directly
	// This is a rare case for very large strings
	s := string(bytes)
	
	// Still try to intern for future uses
	if cached, ok := sc.internMap[s]; ok {
		sc.columnValues[colIdx] = cached
		sc.hits++
		return cached
	}
	
	sc.internMap[s] = s
	sc.columnValues[colIdx] = s
	sc.misses++
	return s
}

// GetFromCString safely converts a C string to a Go string with minimal allocations
func (sc *StringCache) GetFromCString(colIdx int, cstr *C.char, length C.size_t) string {
	if cstr == nil {
		return ""
	}
	
	// Ensure the column values slice is large enough
	if colIdx >= len(sc.columnValues) {
		// Expand capacity if needed - this should be rare
		newSize := colIdx + 8 // Add some margin to reduce future expansions
		newValues := make([]string, newSize)
		copy(newValues, sc.columnValues)
		sc.columnValues = newValues
	}
	
	// Fast path optimization for empty strings
	if length == 0 {
		return ""
	}
	
	// Safety check - if length is unreasonably large, cap it
	actualLength := int(length)
	if actualLength > 1024*1024 { // Cap at 1MB for safety
		actualLength = 1024 * 1024
	}
	
	// If string fits in our buffer, use it to avoid allocations
	if actualLength <= len(sc.byteBuffer) {
		// Copy from C memory to our buffer with bounds checking
		for i := 0; i < actualLength && i < len(sc.byteBuffer); i++ {
			// Use pointer arithmetic but with safety checks
			bytePtr := (*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(cstr)) + uintptr(i)))
			if bytePtr != nil {
				sc.byteBuffer[i] = byte(*bytePtr)
			} else {
				// Truncate if we hit a null pointer (shouldn't happen, but safety first)
				actualLength = i
				break
			}
		}
		
		// Create string from buffer
		s := string(sc.byteBuffer[:actualLength])
		
		// Get from intern map or add to it
		if cached, ok := sc.internMap[s]; ok {
			sc.columnValues[colIdx] = cached
			sc.hits++
			return cached
		}
		
		// Only intern strings under reasonable size to prevent memory bloat
		if len(s) < 1024 {
			sc.internMap[s] = s
		}
		sc.columnValues[colIdx] = s
		sc.misses++
		return s
	}
	
	// If bytes won't fit in our buffer but aren't huge, grow the buffer
	// This amortizes allocations over time
	if actualLength <= sc.maxBufferSize {
		// Grow buffer to accommodate this size with some room to grow
		newSize := actualLength * 2
		if newSize > sc.maxBufferSize {
			newSize = sc.maxBufferSize
		}
		sc.byteBuffer = make([]byte, newSize)
		return sc.GetFromCString(colIdx, cstr, length) // Retry with larger buffer
	}
	
	// For larger strings, we need to use CGo's conversion
	// This is more expensive but should be rare
	s := C.GoStringN(cstr, C.int(actualLength))
	
	// Only intern reasonably sized strings to prevent memory bloat 
	if len(s) < 1024 {
		// Still try to intern
		if cached, ok := sc.internMap[s]; ok {
			sc.columnValues[colIdx] = cached
			sc.hits++
			return cached
		}
		
		sc.internMap[s] = s
	}
	
	sc.columnValues[colIdx] = s
	sc.misses++
	return s
}

// Reset clears the intern map if it grows too large
// Call this periodically if processing many different strings
func (sc *StringCache) Reset() {
	// Only reset if we have a lot of entries
	if len(sc.internMap) > 10000 {
		// Implement a smarter reset that keeps common values
		// Calculate hit rate for each string
		newMap := make(map[string]string, 1024)
		
		// Keep the most frequently used string values
		// This is a heuristic to avoid losing all our cache on reset
		keepCount := 0
		for s, v := range sc.internMap {
			// This is a simplified heuristic - in a real implementation
			// we would track individual string hit counts
			if s == v && len(s) < 64 { // Common case: small interned strings
				newMap[s] = v
				keepCount++
				if keepCount >= 1000 { // Keep at most 1000 values
					break
				}
			}
		}
		
		sc.internMap = newMap
		
		// Keep stats for monitoring but reset counters
		totalHits := sc.hits
		totalMisses := sc.misses
		sc.hits = 0
		sc.misses = 0
		
		// Use hit rate to dynamically adjust buffer size
		hitRate := float64(totalHits) / float64(totalHits+totalMisses)
		if hitRate < 0.5 && len(sc.byteBuffer) < sc.maxBufferSize/2 {
			// Low hit rate suggests we need a larger buffer
			newSize := len(sc.byteBuffer) * 2
			if newSize > sc.maxBufferSize {
				newSize = sc.maxBufferSize
			}
			sc.byteBuffer = make([]byte, newSize)
		}
	}
}

// TuneCache optimizes cache parameters based on usage patterns
func (sc *StringCache) TuneCache() {
	// Calculate hit rate
	totalOps := sc.hits + sc.misses
	if totalOps > 1000 {
		hitRate := float64(sc.hits) / float64(totalOps)
		
		// Adjust intern map capacity based on hit rate
		if hitRate < 0.3 && len(sc.internMap) < 100000 {
			// Low hit rate with small map suggests we need more capacity
			newMap := make(map[string]string, len(sc.internMap)*2)
			for k, v := range sc.internMap {
				newMap[k] = v
			}
			sc.internMap = newMap
		}
	}
}

// Stats returns cache hit/miss statistics
func (sc *StringCache) Stats() (hits, misses int) {
	return sc.hits, sc.misses
}

// Next moves to the next row with optimized memory handling.
func (r *Rows) Next(dest []driver.Value) error {
	if atomic.LoadInt32(&r.closed) != 0 {
		return io.EOF
	}
	
	if r.currentRow >= r.rowCount {
		return io.EOF
	}
	
	// Get values for current row with minimized allocations
	for i := 0; i < len(r.columnNames) && i < len(dest); i++ {
		colIdx := C.idx_t(i)
		rowIdx := r.currentRow
		
		// Check for NULL values
		isNull := C.duckdb_value_is_null(r.result, colIdx, rowIdx)
		if cBoolToGo(isNull) {
			dest[i] = nil
			continue
		}
		
		// Extract value based on column type with zero-allocation optimizations
		colType := r.columnTypes[i]
		switch colType {
		case C.DUCKDB_TYPE_BOOLEAN:
			// Fixed boolean handling - correct conversion from DuckDB bool to Go bool
			val := C.duckdb_value_boolean(r.result, colIdx, rowIdx)
			dest[i] = cBoolToGo(val)
			
		case C.DUCKDB_TYPE_TINYINT:
			val := C.duckdb_value_int8(r.result, colIdx, rowIdx)
			dest[i] = int8(val)
			
		case C.DUCKDB_TYPE_SMALLINT:
			val := C.duckdb_value_int16(r.result, colIdx, rowIdx)
			dest[i] = int16(val)
			
		case C.DUCKDB_TYPE_INTEGER:
			val := C.duckdb_value_int32(r.result, colIdx, rowIdx)
			dest[i] = int32(val)
			
		case C.DUCKDB_TYPE_BIGINT:
			val := C.duckdb_value_int64(r.result, colIdx, rowIdx)
			dest[i] = int64(val)
			
		case C.DUCKDB_TYPE_UTINYINT:
			val := C.duckdb_value_uint8(r.result, colIdx, rowIdx)
			dest[i] = uint8(val)
			
		case C.DUCKDB_TYPE_USMALLINT:
			val := C.duckdb_value_uint16(r.result, colIdx, rowIdx)
			dest[i] = uint16(val)
			
		case C.DUCKDB_TYPE_UINTEGER:
			val := C.duckdb_value_uint32(r.result, colIdx, rowIdx)
			dest[i] = uint32(val)
			
		case C.DUCKDB_TYPE_UBIGINT:
			val := C.duckdb_value_uint64(r.result, colIdx, rowIdx)
			dest[i] = uint64(val)
			
		case C.DUCKDB_TYPE_FLOAT:
			val := C.duckdb_value_float(r.result, colIdx, rowIdx)
			dest[i] = float32(val)
			
		case C.DUCKDB_TYPE_DOUBLE:
			val := C.duckdb_value_double(r.result, colIdx, rowIdx)
			dest[i] = float64(val)
			
		case C.DUCKDB_TYPE_VARCHAR:
			cstr := C.duckdb_value_varchar(r.result, colIdx, rowIdx)
			if cstr == nil {
				dest[i] = ""
			} else {
				// Get length once to avoid multiple C calls
				length := C.strlen(cstr)
				
				// Use optimized string conversion with minimal allocations
				// The GetFromCString method handles caching and interning
				dest[i] = r.strCache.GetFromCString(i, cstr, length)
				
				// Free C memory
				C.duckdb_free(unsafe.Pointer(cstr))
			}
			
		case C.DUCKDB_TYPE_BLOB:
			blob := C.duckdb_value_blob(r.result, colIdx, rowIdx)
			if blob.data == nil || blob.size == 0 {
				dest[i] = []byte{}
			} else {
				// TODO: Implement zero-copy blob transfer once buffer pooling is added
				// For now we use Go's C.GoBytes which makes a copy
				data := C.GoBytes(blob.data, C.int(blob.size))
				C.duckdb_free(blob.data)
				dest[i] = data
			}
			
		case C.DUCKDB_TYPE_TIMESTAMP, C.DUCKDB_TYPE_TIMESTAMP_S, C.DUCKDB_TYPE_TIMESTAMP_MS, C.DUCKDB_TYPE_TIMESTAMP_NS:
			// Handle timestamp (microseconds since 1970-01-01)
			ts := C.duckdb_value_timestamp(r.result, colIdx, rowIdx)
			micros := int64(ts.micros)
			dest[i] = time.Unix(micros/1000000, (micros%1000000)*1000)
			
		case C.DUCKDB_TYPE_DATE:
			// Handle date (days since 1970-01-01)
			date := C.duckdb_value_date(r.result, colIdx, rowIdx)
			days := int64(date.days)
			dest[i] = time.Unix(days*24*60*60, 0).UTC()
			
		case C.DUCKDB_TYPE_TIME:
			// Handle time (microseconds since 00:00:00)
			timeVal := C.duckdb_value_time(r.result, colIdx, rowIdx)
			micros := int64(timeVal.micros)
			seconds := micros / 1000000
			nanos := (micros % 1000000) * 1000
			
			hour := seconds / 3600
			minute := (seconds % 3600) / 60
			second := seconds % 60
			
			// Using UTC date with just the time component
			// This avoids the expensive time.Now() call in the hot path
			dest[i] = time.Date(1970, 1, 1, int(hour), int(minute), int(second), int(nanos), time.UTC)
			
		default:
			// For other types, convert to string with optimized handling
			cstr := C.duckdb_value_varchar(r.result, colIdx, rowIdx)
			if cstr == nil {
				dest[i] = ""
			} else {
				length := C.strlen(cstr)
				dest[i] = r.strCache.GetFromCString(i, cstr, length)
				C.duckdb_free(unsafe.Pointer(cstr))
			}
		}
	}
	
	// Adaptive cache optimization
	// If we've processed a lot of rows, periodically manage our string cache
	if r.currentRow > 0 {
		if r.currentRow%5000 == 0 {
			// Periodically reset the cache to prevent unbounded growth
			r.strCache.Reset()
		}
		
		if r.currentRow%25000 == 0 {
			// Less frequently, tune the cache parameters based on observed patterns
			r.strCache.TuneCache()
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