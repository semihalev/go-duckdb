// Package duckdb provides low-level, high-performance SQL driver for DuckDB in Go.
package duckdb

/*
// Use only necessary includes here - CGO directives are defined in duckdb.go
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
	result      *C.duckdb_result
	columnNames []string
	columnTypes []C.duckdb_type
	rowCount    C.idx_t
	currentRow  C.idx_t
	closed      int32
	// Cache for string values to reduce allocations
	strCache StringCacher
	// BLOB buffers to reduce allocations for BLOB data
	// Maps column index to buffer for reuse during iteration
	blobBuffers map[int][]byte
	// Column vectors for pre-allocated storage
	// Maps column index to vector for reuse
	columnVectors map[int]*SafeColumnVector
	// Result wrapper for pooled results
	resultWrapper *ResultSetWrapper
	// Flags to track which buffers came from the pool
	fromPool struct {
		strCache      bool
		columnNames   bool
		columnTypes   bool
		blobBuffers   bool
		columnVectors bool
		resultWrapper bool
	}
}

// newRowsWithWrapper creates a new Rows instance from a pooled result wrapper.
// It uses comprehensive buffer pooling to minimize allocations.
func newRowsWithWrapper(wrapper *ResultSetWrapper) *Rows {
	if wrapper == nil || !wrapper.isAllocated {
		// This should never happen, but let's avoid panic
		return &Rows{
			result:      nil,
			rowCount:    0,
			currentRow:  0,
			blobBuffers: make(map[int][]byte),
		}
	}

	// Get column count - use address of wrapper.result
	columnCount := C.duckdb_column_count(&wrapper.result)
	colCountInt := int(columnCount)

	// Get row count - use address of wrapper.result
	rowCount := C.duckdb_row_count(&wrapper.result)

	// Create a new rows instance with pooled resources
	rows := &Rows{
		result:        &wrapper.result, // Store a pointer to the result inside the wrapper
		resultWrapper: wrapper,         // Store the wrapper for later return to pool
		rowCount:      rowCount,
		currentRow:    0,
		blobBuffers:   make(map[int][]byte),            // Initialize blob buffers map
		columnVectors: make(map[int]*SafeColumnVector), // Initialize column vectors map
	}

	// Mark everything as coming from pool for proper cleanup
	rows.fromPool.resultWrapper = true
	rows.fromPool.blobBuffers = true
	rows.fromPool.columnVectors = true

	// Get column names buffer from pool
	rows.columnNames = globalBufferPool.GetColumnNamesBuffer(colCountInt)
	rows.fromPool.columnNames = true

	// Get column types buffer from pool
	rows.columnTypes = globalBufferPool.GetColumnTypesBuffer(colCountInt)
	rows.fromPool.columnTypes = true

	// Get string cache from pool
	rows.strCache = globalBufferPool.GetStringCache(colCountInt)
	rows.fromPool.strCache = true

	// Populate column names and types with shared string map for maximum reuse
	for i := C.idx_t(0); i < columnCount; i++ {
		iInt := int(i)

		// Get column name - use the shared string map for these since they're commonly repeated
		cname := C.duckdb_column_name(&wrapper.result, i)
		colName := goString(cname)

		// Use shared string map for column names which are often repeated
		rows.columnNames[iInt] = globalBufferPool.GetSharedString(colName)

		// Store column type
		rows.columnTypes[iInt] = C.duckdb_column_type(&wrapper.result, i)
	}

	return rows
}

// Columns returns the column names.
func (r *Rows) Columns() []string {
	return r.columnNames
}

// GetColumnVector retrieves or creates an optimized vector for the column
// Uses type-specific vector pools for better performance
func (r *Rows) GetColumnVector(colIdx int, rowCapacity int) *SafeColumnVector {
	// First check if we already have a vector for this column
	if vec, ok := r.columnVectors[colIdx]; ok && vec != nil {
		// Check if it's large enough
		if vec.Capacity >= rowCapacity {
			return vec
		}
	}

	// Need to create or get a new vector - use the type-specific pools
	var vector *SafeColumnVector
	colType := r.columnTypes[colIdx]

	// Choose the appropriate vector type based on column type
	switch colType {
	case C.DUCKDB_TYPE_INTEGER:
		vector = globalBufferPool.GetInt32Vector(rowCapacity)

	case C.DUCKDB_TYPE_BIGINT:
		vector = globalBufferPool.GetInt64Vector(rowCapacity)

	case C.DUCKDB_TYPE_DOUBLE:
		vector = globalBufferPool.GetFloat64Vector(rowCapacity)

	default:
		// For other types, allocate a general vector with appropriate size
		elementSize := 8 // Default to 8 bytes
		if colType == C.DUCKDB_TYPE_BOOLEAN || colType == C.DUCKDB_TYPE_TINYINT {
			elementSize = 1
		} else if colType == C.DUCKDB_TYPE_SMALLINT || colType == C.DUCKDB_TYPE_USMALLINT {
			elementSize = 2
		} else if colType == C.DUCKDB_TYPE_INTEGER || colType == C.DUCKDB_TYPE_UINTEGER || colType == C.DUCKDB_TYPE_FLOAT {
			elementSize = 4
		}

		vector = AllocSafeVector(rowCapacity, elementSize, int(colType))
	}

	// Store the vector for future use
	r.columnVectors[colIdx] = vector

	return vector
}

// Close closes the rows and returns resources to the buffer pool.
func (r *Rows) Close() error {
	if !atomic.CompareAndSwapInt32(&r.closed, 0, 1) {
		return nil
	}

	// Return buffers to the pool if they came from there
	if r.fromPool.strCache && r.strCache != nil {
		globalBufferPool.PutStringCache(r.strCache)
		r.strCache = nil
	}

	if r.fromPool.columnNames {
		globalBufferPool.PutColumnNamesBuffer(r.columnNames)
		r.columnNames = nil
	}

	if r.fromPool.columnTypes {
		globalBufferPool.PutColumnTypesBuffer(r.columnTypes)
		r.columnTypes = nil
	}

	// Return BLOB buffers to the pool
	if r.fromPool.blobBuffers && r.blobBuffers != nil {
		for _, buf := range r.blobBuffers {
			globalBufferPool.PutBlobBuffer(buf)
		}
		r.blobBuffers = nil
	}

	// Reset column vectors for reuse
	if r.fromPool.columnVectors && r.columnVectors != nil {
		for _, vec := range r.columnVectors {
			if vec != nil {
				vec.Reset()
			}
		}
		r.columnVectors = nil
	}

	// If we have a result wrapper, return it to the pool
	// This will handle freeing the C resources
	if r.fromPool.resultWrapper && r.resultWrapper != nil {
		globalBufferPool.PutResultSetWrapper(r.resultWrapper)
		r.resultWrapper = nil
		r.result = nil
	} else if r.result != nil {
		// If we don't have a wrapper but have a result, free it directly
		C.duckdb_destroy_result(r.result)
		r.result = nil
	}

	return nil
}

// Common empty string instance to avoid allocations for empty strings
var emptyString = ""

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

		// Check for NULL values using the safe approach
		if r.result == nil {
			dest[i] = nil
			continue
		}

		// Get a pointer to the result structure
		var resultPtr *C.duckdb_result
		if r.resultWrapper != nil {
			// Use the address of the result in the wrapper
			resultPtr = &r.resultWrapper.result
		} else {
			// Use the existing pointer
			resultPtr = r.result
		}

		// Check for NULL values
		isNull := C.duckdb_value_is_null(resultPtr, colIdx, rowIdx)
		if cBoolToGo(isNull) {
			dest[i] = nil
			continue
		}

		// Extract value based on column type with native optimizations
		colType := r.columnTypes[i]
		switch colType {
		case C.DUCKDB_TYPE_BOOLEAN:
			// Direct access to boolean value
			val := C.duckdb_value_boolean(resultPtr, colIdx, rowIdx)
			dest[i] = cBoolToGo(val)

		case C.DUCKDB_TYPE_TINYINT:
			val := C.duckdb_value_int8(resultPtr, colIdx, rowIdx)
			dest[i] = int8(val)

		case C.DUCKDB_TYPE_SMALLINT:
			val := C.duckdb_value_int16(resultPtr, colIdx, rowIdx)
			dest[i] = int16(val)

		case C.DUCKDB_TYPE_INTEGER:
			val := C.duckdb_value_int32(resultPtr, colIdx, rowIdx)
			dest[i] = int32(val)

		case C.DUCKDB_TYPE_BIGINT:
			val := C.duckdb_value_int64(resultPtr, colIdx, rowIdx)
			dest[i] = int64(val)

		case C.DUCKDB_TYPE_UTINYINT:
			val := C.duckdb_value_uint8(resultPtr, colIdx, rowIdx)
			dest[i] = uint8(val)

		case C.DUCKDB_TYPE_USMALLINT:
			val := C.duckdb_value_uint16(resultPtr, colIdx, rowIdx)
			dest[i] = uint16(val)

		case C.DUCKDB_TYPE_UINTEGER:
			val := C.duckdb_value_uint32(resultPtr, colIdx, rowIdx)
			dest[i] = uint32(val)

		case C.DUCKDB_TYPE_UBIGINT:
			val := C.duckdb_value_uint64(resultPtr, colIdx, rowIdx)
			dest[i] = uint64(val)

		case C.DUCKDB_TYPE_FLOAT:
			val := C.duckdb_value_float(resultPtr, colIdx, rowIdx)
			dest[i] = float32(val)

		case C.DUCKDB_TYPE_DOUBLE:
			val := C.duckdb_value_double(resultPtr, colIdx, rowIdx)
			dest[i] = float64(val)

		case C.DUCKDB_TYPE_VARCHAR:
			cstr := C.duckdb_value_varchar(resultPtr, colIdx, rowIdx)
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
			blob := C.duckdb_value_blob(resultPtr, colIdx, rowIdx)
			if blob.data == nil || blob.size == 0 {
				dest[i] = []byte{} // Empty BLOB
			} else {
				blobSize := int(blob.size)

				// Get or reuse a buffer for this column
				var buffer []byte
				var ok bool

				// Try to reuse the existing buffer for this column if possible
				if buffer, ok = r.blobBuffers[i]; ok && len(buffer) >= blobSize {
					// We can reuse the existing buffer
					buffer = buffer[:blobSize]
				} else {
					// Get a new buffer from the pool
					buffer = globalBufferPool.GetBlobBuffer(blobSize)
					r.blobBuffers[i] = buffer
				}

				// Copy the blob data safely
				if blobSize > 0 {
					// Use memcpy directly for efficiency
					C.memcpy(unsafe.Pointer(&buffer[0]), unsafe.Pointer(blob.data), C.size_t(blobSize))
				}

				// Free the DuckDB blob data immediately
				C.duckdb_free(blob.data)

				// Set the buffer as the result
				dest[i] = buffer
			}

		case C.DUCKDB_TYPE_TIMESTAMP, C.DUCKDB_TYPE_TIMESTAMP_S, C.DUCKDB_TYPE_TIMESTAMP_MS, C.DUCKDB_TYPE_TIMESTAMP_NS:
			// Use the direct access for timestamps (no string conversion)
			// With our optimized native implementation
			ts := C.duckdb_value_timestamp(resultPtr, colIdx, rowIdx)
			micros := int64(ts.micros)
			dest[i] = time.Unix(micros/1000000, (micros%1000000)*1000)

		case C.DUCKDB_TYPE_DATE:
			// Use the direct access for dates (no string conversion)
			// With our optimized native implementation
			date := C.duckdb_value_date(resultPtr, colIdx, rowIdx)
			days := int64(date.days)
			dest[i] = time.Unix(days*24*60*60, 0).UTC()

		case C.DUCKDB_TYPE_TIME:
			// Direct time access
			timeVal := C.duckdb_value_time(resultPtr, colIdx, rowIdx)
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
			cstr := C.duckdb_value_varchar(resultPtr, colIdx, rowIdx)
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
	if r.currentRow > 0 && r.currentRow%10000 == 0 {
		// Periodically reset the cache to prevent unbounded growth
		r.strCache.Reset()
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

// These helper methods enable batch extraction of entire columns at once
// for improved performance with large datasets

// ExtractStringColumn extracts an entire string column
func (r *Rows) ExtractStringColumn(colIdx int) ([]string, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractStringColumnGeneric(r, colIdx)
}

// ExtractBlobColumn extracts an entire blob column
func (r *Rows) ExtractBlobColumn(colIdx int) ([][]byte, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractBlobColumnGeneric(r, colIdx)
}

// Implement Result interface methods
func (r *Rows) GetResultPointer() *C.duckdb_result {
	if r.resultWrapper != nil {
		return &r.resultWrapper.result
	}
	return r.result
}

func (r *Rows) ColumnCount() int {
	return len(r.columnNames)
}

func (r *Rows) RowCount() int64 {
	return int64(r.rowCount)
}

func (r *Rows) ColumnType(colIdx int) C.duckdb_type {
	if colIdx < 0 || colIdx >= len(r.columnTypes) {
		return C.DUCKDB_TYPE_INVALID
	}
	return r.columnTypes[colIdx]
}

func (r *Rows) IsNull(colIdx, rowIdx int) bool {
	var resultPtr *C.duckdb_result
	if r.resultWrapper != nil {
		resultPtr = &r.resultWrapper.result
	} else {
		resultPtr = r.result
	}
	isNull := C.duckdb_value_is_null(resultPtr, C.idx_t(colIdx), C.idx_t(rowIdx))
	return cBoolToGo(isNull)
}

// ExtractInt32Column extracts an entire int32 column using optimized block-based extraction
// Returns the values and null indicators
func (r *Rows) ExtractInt32Column(colIdx int) ([]int32, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractInt32ColumnGeneric(r, colIdx)
}

// ExtractInt64Column extracts an entire int64 column using optimized batch-based extraction
func (r *Rows) ExtractInt64Column(colIdx int) ([]int64, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractInt64ColumnGeneric(r, colIdx)
}

// ExtractFloat64Column extracts an entire float64 column using optimized batch-based extraction
func (r *Rows) ExtractFloat64Column(colIdx int) ([]float64, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractFloat64ColumnGeneric(r, colIdx)
}

// ExtractTimestampColumn extracts an entire timestamp column as microseconds since epoch
func (r *Rows) ExtractTimestampColumn(colIdx int) ([]int64, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractTimestampColumnGeneric(r, colIdx)
}

// ExtractDateColumn extracts an entire date column as days since epoch
func (r *Rows) ExtractDateColumn(colIdx int) ([]int32, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractDateColumnGeneric(r, colIdx)
}

// ExtractBoolColumn extracts an entire boolean column
func (r *Rows) ExtractBoolColumn(colIdx int) ([]bool, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation for boolean columns
	return r.ExtractBool8Column(colIdx)
}

// ExtractBool8Column extracts an entire boolean column as []bool (not compressed)
func (r *Rows) ExtractBool8Column(colIdx int) ([]bool, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractBoolColumnGeneric(r, colIdx)
}

// ExtractInt8Column extracts an entire int8 column
func (r *Rows) ExtractInt8Column(colIdx int) ([]int8, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractInt8ColumnGeneric(r, colIdx)
}

// ExtractInt16Column extracts an entire int16 column
func (r *Rows) ExtractInt16Column(colIdx int) ([]int16, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractInt16ColumnGeneric(r, colIdx)
}

// ExtractUint8Column extracts an entire uint8 column
func (r *Rows) ExtractUint8Column(colIdx int) ([]uint8, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractUint8ColumnGeneric(r, colIdx)
}

// ExtractUint16Column extracts an entire uint16 column
func (r *Rows) ExtractUint16Column(colIdx int) ([]uint16, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractUint16ColumnGeneric(r, colIdx)
}

// ExtractUint32Column extracts an entire uint32 column
func (r *Rows) ExtractUint32Column(colIdx int) ([]uint32, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractUint32ColumnGeneric(r, colIdx)
}

// ExtractUint64Column extracts an entire uint64 column
func (r *Rows) ExtractUint64Column(colIdx int) ([]uint64, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractUint64ColumnGeneric(r, colIdx)
}

// ExtractFloat32Column extracts an entire float32 column
func (r *Rows) ExtractFloat32Column(colIdx int) ([]float32, []bool, error) {
	if atomic.LoadInt32(&r.closed) != 0 {
		return nil, nil, io.ErrClosedPipe
	}

	// Use the unified implementation
	return ExtractFloat32ColumnGeneric(r, colIdx)
}

// QueryResult represents the result of a query execution.
type QueryResult struct {
	rowsAffected int64
	lastInsertID int64
}

// LastInsertId returns the ID of the last inserted row.
func (r *QueryResult) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

// RowsAffected returns the number of rows affected by the query.
func (r *QueryResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}
