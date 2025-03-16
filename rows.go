// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
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

// newRows creates a new Rows instance from a DuckDB result.
// It uses buffer pooling to minimize allocations, but doesn't use a result wrapper.
func newRows(result *C.duckdb_result) *Rows {
	// Get column count
	columnCount := C.duckdb_column_count(result)
	colCountInt := int(columnCount)

	// Get row count
	rowCount := C.duckdb_row_count(result)

	// Create a new rows instance with pooled buffers
	rows := &Rows{
		result:        result,
		rowCount:      rowCount,
		currentRow:    0,
		blobBuffers:   make(map[int][]byte),            // Initialize blob buffers map
		columnVectors: make(map[int]*SafeColumnVector), // Initialize column vectors map
	}
	rows.fromPool.blobBuffers = true   // Mark as coming from pool for cleanup
	rows.fromPool.columnVectors = true // Mark as coming from pool for cleanup

	// Get column names buffer from pool
	rows.columnNames = globalBufferPool.GetColumnNamesBuffer(colCountInt)
	rows.fromPool.columnNames = true

	// Get column types buffer from pool
	rows.columnTypes = globalBufferPool.GetColumnTypesBuffer(colCountInt)
	rows.fromPool.columnTypes = true

	// Get string cache from pool
	rows.strCache = globalBufferPool.GetStringCache(colCountInt)
	rows.fromPool.strCache = true

	// Populate column names and types
	for i := C.idx_t(0); i < columnCount; i++ {
		iInt := int(i)
		rows.columnNames[iInt] = goString(C.duckdb_column_name(result, i))
		rows.columnTypes[iInt] = C.duckdb_column_type(result, i)
	}

	return rows
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

// StringCache is an optimized cache system for strings that reduces allocations
// It uses string interning and byte buffer pooling to minimize GC pressure
// It implements the StringCacher interface
type StringCache struct {
	// Column-specific caches for quick access by column index
	columnValues []string

	// Local string intern map for value deduplication across all columns
	// This dramatically reduces allocations for repeated values within a query
	internMap map[string]string

	// Multiple byte buffers for string conversion to avoid allocations and contention
	// Using [][]byte allows us to reuse multiple buffers for concurrent column access
	buffers     [][]byte
	bufferIndex int // Current buffer index for round-robin selection

	// Max buffer size to prevent unbounded growth
	maxBufferSize int

	// String column vectors for reuse
	// For most common string sizes
	stringVectors map[int]*SafeColumnVector

	// Statistics for monitoring
	hits   int
	misses int

	// Flag indicating whether this cache uses the global string map
	useSharedStringMap bool
}

// NewStringCache creates a new optimized string cache with capacity for the given number of columns.
func NewStringCache(columns int) *StringCache {
	// Create multiple buffers for string conversion to reduce contention
	bufferCount := 4 // Use 4 buffers by default for reasonable concurrency
	buffers := make([][]byte, bufferCount)

	// Initialize each buffer to a reasonable starting size
	for i := range buffers {
		buffers[i] = make([]byte, 1024) // Initial 1KB buffer
	}

	return &StringCache{
		columnValues:       make([]string, columns),
		internMap:          make(map[string]string, 1024), // Start with capacity for 1024 unique strings
		buffers:            buffers,
		maxBufferSize:      4 * 1024 * 1024,                 // Max 4MB buffer size
		stringVectors:      make(map[int]*SafeColumnVector), // For string reuse
		useSharedStringMap: true,                            // Enable shared string map by default
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

	// First check if we've seen this exact string before in our local cache
	if cached, ok := sc.internMap[value]; ok {
		sc.columnValues[colIdx] = cached
		sc.hits++
		return cached
	}

	var finalString string

	// If shared string map is enabled, try to get or add to the global map
	if sc.useSharedStringMap && len(value) < 1024 {
		finalString = globalBufferPool.GetSharedString(value)
	} else {
		// Otherwise use our local map only
		finalString = value
		// Store in the local intern map to deduplicate future occurrences
		sc.internMap[value] = value
	}

	sc.columnValues[colIdx] = finalString
	sc.misses++

	return finalString
}

// GetFromBytes converts a byte slice to a string with minimal allocations
// By reusing an internal buffer when possible
func (sc *StringCache) GetFromBytes(colIdx int, bytes []byte) string {
	// Ensure the column values slice is large enough
	if colIdx >= len(sc.columnValues) {
		// Expand capacity if needed - this should be rare
		newSize := colIdx + 8 // Add some margin to reduce future expansions
		newValues := make([]string, newSize)
		copy(newValues, sc.columnValues)
		sc.columnValues = newValues
	}

	// Get the current buffer in round-robin fashion
	sc.bufferIndex = (sc.bufferIndex + 1) % len(sc.buffers)
	currentBuffer := sc.buffers[sc.bufferIndex]

	// Fast path: if we can reuse our buffer
	if len(bytes) <= len(currentBuffer) {
		// Copy bytes to our reusable buffer
		copy(currentBuffer, bytes)
		// Create string from our buffer's first n bytes
		s := string(currentBuffer[:len(bytes)])

		// Try to get from local intern map first
		if cached, ok := sc.internMap[s]; ok {
			sc.columnValues[colIdx] = cached
			sc.hits++
			return cached
		}

		var finalString string

		// If shared string map is enabled, try to get or add to the global map
		if sc.useSharedStringMap && len(s) < 1024 {
			finalString = globalBufferPool.GetSharedString(s)
		} else {
			// Otherwise use our local map only
			finalString = s
			// Store in the local intern map to deduplicate future occurrences
			sc.internMap[s] = s
		}

		sc.columnValues[colIdx] = finalString
		sc.misses++
		return finalString
	}

	// If bytes won't fit in our current buffer
	if len(bytes) <= sc.maxBufferSize {
		// Get a buffer from the pool or create a new one
		newBuffer := globalBufferPool.GetStringBuffer(len(bytes))

		// Copy bytes to the buffer
		copy(newBuffer, bytes)

		// Create string from buffer
		s := string(newBuffer[:len(bytes)])

		// Try to get from local intern map first
		if cached, ok := sc.internMap[s]; ok {
			// Return the buffer to the pool before returning
			globalBufferPool.PutStringBuffer(newBuffer)

			sc.columnValues[colIdx] = cached
			sc.hits++
			return cached
		}

		var finalString string

		// If shared string map is enabled, try to get or add to the global map
		if sc.useSharedStringMap && len(s) < 1024 {
			finalString = globalBufferPool.GetSharedString(s)
		} else {
			// Otherwise use our local map only
			finalString = s
			// Store in the local intern map to deduplicate future occurrences
			sc.internMap[s] = s
		}

		// Return the buffer to the pool
		globalBufferPool.PutStringBuffer(newBuffer)

		sc.columnValues[colIdx] = finalString
		sc.misses++
		return finalString
	}

	// For very large strings, just convert directly
	s := string(bytes)

	// Try to get from local intern map first
	if cached, ok := sc.internMap[s]; ok {
		sc.columnValues[colIdx] = cached
		sc.hits++
		return cached
	}

	var finalString string

	// Only use shared map for reasonably sized strings to prevent memory bloat
	if sc.useSharedStringMap && len(s) < 1024 {
		finalString = globalBufferPool.GetSharedString(s)
	} else {
		finalString = s
		// Only intern reasonably sized strings locally
		if len(s) < 4096 {
			sc.internMap[s] = s
		}
	}

	sc.columnValues[colIdx] = finalString
	sc.misses++
	return finalString
}

// Common empty string instance to avoid allocations for empty strings
var emptyString = ""

// GetFromCString safely converts a C string to a Go string with minimal allocations
func (sc *StringCache) GetFromCString(colIdx int, cstr *C.char, length C.size_t) string {
	if cstr == nil {
		return emptyString
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
		return emptyString
	}

	// Safety check - if length is unreasonably large, cap it
	actualLength := int(length)
	if actualLength > 1024*1024 { // Cap at 1MB for safety
		actualLength = 1024 * 1024
	}

	// Get the current buffer in round-robin fashion to reduce contention
	sc.bufferIndex = (sc.bufferIndex + 1) % len(sc.buffers)
	currentBuffer := sc.buffers[sc.bufferIndex]

	// If string fits in our current buffer, use it to avoid allocations
	if actualLength <= len(currentBuffer) {
		// Copy from C memory to our buffer with bounds checking
		for i := 0; i < actualLength && i < len(currentBuffer); i++ {
			// Use pointer arithmetic but with safety checks
			bytePtr := (*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(cstr)) + uintptr(i)))
			if bytePtr != nil {
				currentBuffer[i] = byte(*bytePtr)
			} else {
				// Truncate if we hit a null pointer (shouldn't happen, but safety first)
				actualLength = i
				break
			}
		}

		// Create string from buffer
		s := string(currentBuffer[:actualLength])

		// First try to get from our local cache
		if cached, ok := sc.internMap[s]; ok {
			sc.columnValues[colIdx] = cached
			sc.hits++
			return cached
		}

		var finalString string

		// If shared string map is enabled, try to get or add to the global map
		if sc.useSharedStringMap && len(s) < 1024 {
			finalString = globalBufferPool.GetSharedString(s)
		} else {
			// Otherwise use our local map only
			finalString = s
			// Only intern strings under reasonable size to prevent memory bloat
			if len(s) < 1024 {
				sc.internMap[s] = s
			}
		}

		sc.columnValues[colIdx] = finalString
		sc.misses++
		return finalString
	}

	// If bytes won't fit in our current buffer but aren't huge
	if actualLength <= sc.maxBufferSize {
		// Get a buffer from the pool instead of growing our internal one
		// This is more efficient for occasional large strings
		buffer := globalBufferPool.GetStringBuffer(actualLength)

		// Copy from C memory to our buffer with bounds checking
		for i := 0; i < actualLength && i < len(buffer); i++ {
			// Use pointer arithmetic but with safety checks
			bytePtr := (*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(cstr)) + uintptr(i)))
			if bytePtr != nil {
				buffer[i] = byte(*bytePtr)
			} else {
				// Truncate if we hit a null pointer (shouldn't happen, but safety first)
				actualLength = i
				break
			}
		}

		// Create string from buffer
		s := string(buffer[:actualLength])

		// First try to get from our local cache
		if cached, ok := sc.internMap[s]; ok {
			// Return the buffer to the pool before returning the result
			globalBufferPool.PutStringBuffer(buffer)

			sc.columnValues[colIdx] = cached
			sc.hits++
			return cached
		}

		var finalString string

		// If shared string map is enabled, try to get or add to the global map
		if sc.useSharedStringMap && len(s) < 1024 {
			finalString = globalBufferPool.GetSharedString(s)
		} else {
			// Otherwise use our local map only
			finalString = s
			// Only intern strings under reasonable size to prevent memory bloat
			if len(s) < 1024 {
				sc.internMap[s] = s
			}
		}

		// Return the buffer to the pool
		globalBufferPool.PutStringBuffer(buffer)

		sc.columnValues[colIdx] = finalString
		sc.misses++
		return finalString
	}

	// For larger strings, we need to use CGo's conversion
	// This is more expensive but should be rare for very large strings
	s := C.GoStringN(cstr, C.int(actualLength))

	// Try to get from local intern map first for large strings too
	if cached, ok := sc.internMap[s]; ok {
		sc.columnValues[colIdx] = cached
		sc.hits++
		return cached
	}

	var finalString string

	// Only use shared map for reasonably sized strings to prevent memory bloat
	if sc.useSharedStringMap && len(s) < 1024 {
		finalString = globalBufferPool.GetSharedString(s)
	} else {
		finalString = s
		// Only intern reasonably sized strings locally
		if len(s) < 1024 {
			sc.internMap[s] = s
		}
	}

	sc.columnValues[colIdx] = finalString
	sc.misses++
	return finalString
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

		// Reset string vectors if present
		for _, vec := range sc.stringVectors {
			if vec != nil {
				vec.Reset()
			}
		}

		// Keep stats for monitoring but reset counters
		totalHits := sc.hits
		totalMisses := sc.misses
		sc.hits = 0
		sc.misses = 0

		// Trigger a global cleanup to prevent unbounded growth
		go globalBufferPool.PeriodicCleanup()

		// Use hit rate to dynamically adjust buffer sizes
		hitRate := float64(totalHits) / float64(totalHits+totalMisses)
		if hitRate < 0.3 { // Lower threshold to make growth more aggressive
			// If hit rate is low, we might need larger buffers
			for i := range sc.buffers {
				// Get a buffer size that matches our hit rate
				currentSize := len(sc.buffers[i])
				if currentSize < sc.maxBufferSize/2 {
					// Grow buffer to twice its current size
					newSize := currentSize * 2
					if newSize > sc.maxBufferSize {
						newSize = sc.maxBufferSize
					}
					sc.buffers[i] = make([]byte, newSize)
				}
			}
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

		// Dynamically adjust the number of buffers based on hit rate
		// If hit rate is very low, we might have contention on buffers
		if hitRate < 0.2 && len(sc.buffers) < 16 {
			// Double the number of buffers to reduce contention
			newBufferCount := len(sc.buffers) * 2
			if newBufferCount > 16 {
				newBufferCount = 16 // Cap at 16 buffers to prevent excessive memory usage
			}

			// Create new set of buffers
			newBuffers := make([][]byte, newBufferCount)

			// Copy existing buffers
			copy(newBuffers, sc.buffers)

			// Initialize new buffers
			for i := len(sc.buffers); i < newBufferCount; i++ {
				// Start with the same size as the first buffer
				newBuffers[i] = make([]byte, len(sc.buffers[0]))
			}

			sc.buffers = newBuffers
		}
	}
}

// Stats returns cache hit/miss statistics
func (sc *StringCache) Stats() (hits, misses int) {
	return sc.hits, sc.misses
}

// GetDedupString returns a deduplicated string from the cache
// It uses the shared string map for maximum deduplication across all strings
func (sc *StringCache) GetDedupString(value string) string {
	// Simple length check to avoid map lookups for empty strings
	if value == "" {
		return ""
	}

	// First try to get from our local cache
	if cached, ok := sc.internMap[value]; ok {
		sc.hits++
		return cached
	}

	var finalString string

	// If shared string map is enabled, try to get or add to the global map
	if sc.useSharedStringMap && len(value) < 1024 {
		finalString = globalBufferPool.GetSharedString(value)
	} else {
		// Otherwise use our local map only
		finalString = value
		// Only intern strings under reasonable size to prevent memory bloat
		if len(value) < 1024 {
			sc.internMap[value] = value
		}
	}

	sc.misses++
	return finalString
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

		// Extract value based on column type with zero-allocation optimizations
		colType := r.columnTypes[i]
		switch colType {
		case C.DUCKDB_TYPE_BOOLEAN:
			// Fixed boolean handling - correct conversion from DuckDB bool to Go bool
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
			// Handle timestamp (microseconds since 1970-01-01)
			ts := C.duckdb_value_timestamp(resultPtr, colIdx, rowIdx)
			micros := int64(ts.micros)
			dest[i] = time.Unix(micros/1000000, (micros%1000000)*1000)

		case C.DUCKDB_TYPE_DATE:
			// Handle date (days since 1970-01-01)
			date := C.duckdb_value_date(resultPtr, colIdx, rowIdx)
			days := int64(date.days)
			dest[i] = time.Unix(days*24*60*60, 0).UTC()

		case C.DUCKDB_TYPE_TIME:
			// Handle time (microseconds since 00:00:00)
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
