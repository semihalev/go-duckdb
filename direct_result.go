// Package duckdb provides low-level, high-performance SQL driver for DuckDB in Go.
package duckdb

/*
// Use only necessary includes here - CGO directives are defined in duckdb.go
#include <stdlib.h>
#include <string.h>
#include <duckdb.h>
#include "duckdb_go_adapter.h"
*/
import "C"

import (
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

// DirectResult provides direct high-performance access to a DuckDB result set
// It bypasses the standard SQL driver interfaces to enable more efficient data access
type DirectResult struct {
	result      *C.duckdb_result
	rowCount    int64
	columnCount int
	columnNames []string
	columnTypes []C.duckdb_type
	closed      bool
	mu          sync.RWMutex
}

// NewDirectResult creates a new DirectResult from a DuckDB result
func NewDirectResult(result *C.duckdb_result) *DirectResult {
	if result == nil {
		return nil
	}

	// Get column and row counts
	columnCount := int(C.duckdb_column_count(result))
	rowCount := int64(C.duckdb_row_count(result))

	// Get column names and types
	columnNames := make([]string, columnCount)
	columnTypes := make([]C.duckdb_type, columnCount)

	for i := 0; i < columnCount; i++ {
		// Get column name - according to docs, these are managed by DuckDB
		// and will be freed automatically when duckdb_destroy_result is called
		cName := C.duckdb_column_name(result, C.idx_t(i))
		columnNames[i] = C.GoString(cName)
		// DO NOT free cName - DuckDB owns this memory

		// Get column type
		columnTypes[i] = C.duckdb_column_type(result, C.idx_t(i))
	}

	// Create and return the result
	dr := &DirectResult{
		result:      result,
		rowCount:    rowCount,
		columnCount: columnCount,
		columnNames: columnNames,
		columnTypes: columnTypes,
	}

	// Set finalizer to ensure result is cleaned up
	runtime.SetFinalizer(dr, (*DirectResult).Close)

	return dr
}

// Close frees the resources allocated for the result
func (dr *DirectResult) Close() error {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	if dr.closed {
		return nil
	}

	if dr.result != nil {
		C.duckdb_destroy_result(dr.result)
		dr.result = nil
	}

	dr.closed = true
	runtime.SetFinalizer(dr, nil)

	return nil
}

// RowCount returns the number of rows in the result set
func (dr *DirectResult) RowCount() int64 {
	return dr.rowCount
}

// ColumnCount returns the number of columns in the result set
func (dr *DirectResult) ColumnCount() int {
	return dr.columnCount
}

// ColumnNames returns the names of all columns in the result set
func (dr *DirectResult) ColumnNames() []string {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	return dr.columnNames
}

// ColumnTypes returns the DuckDB types of all columns in the result set
func (dr *DirectResult) ColumnTypes() []C.duckdb_type {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	return dr.columnTypes
}

// ExtractInt32Column extracts an int32 column using optimized native code
// Returns the values and null mask
func (dr *DirectResult) ExtractInt32Column(colIdx int) ([]int32, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Use the shared implementation from result_extraction.go
	return ExtractInt32ColumnGeneric(dr, colIdx)
}

// The ColumnCount and RowCount methods are already implemented above

// ColumnType implements the Result interface
func (dr *DirectResult) ColumnType(colIdx int) C.duckdb_type {
	if colIdx < 0 || colIdx >= dr.columnCount {
		return C.DUCKDB_TYPE_INVALID
	}
	return dr.columnTypes[colIdx]
}

// GetResultPointer implements the Result interface
func (dr *DirectResult) GetResultPointer() *C.duckdb_result {
	return dr.result
}

// IsNull implements the Result interface
func (dr *DirectResult) IsNull(colIdx, rowIdx int) bool {
	if colIdx < 0 || colIdx >= dr.columnCount || rowIdx < 0 || rowIdx >= int(dr.rowCount) {
		return true // Out of bounds is treated as NULL
	}
	return bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(rowIdx)))
}

// ExtractInt64Column extracts an int64 column using optimized native code
func (dr *DirectResult) ExtractInt64Column(colIdx int) ([]int64, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Use the shared implementation from result_extraction.go
	return ExtractInt64ColumnGeneric(dr, colIdx)
}

// ExtractFloat64Column extracts a float64 column using optimized native code
func (dr *DirectResult) ExtractFloat64Column(colIdx int) ([]float64, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Use the shared implementation from result_extraction.go
	return ExtractFloat64ColumnGeneric(dr, colIdx)
}

// ExtractStringColumn extracts a string column
func (dr *DirectResult) ExtractStringColumn(colIdx int) ([]string, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Use the shared implementation from result_extraction.go
	return ExtractStringColumnGeneric(dr, colIdx)
}

// ExtractStringColumnTrueZeroCopy extracts a string column with true zero-copy memory sharing
func (dr *DirectResult) ExtractStringColumnTrueZeroCopy(colIdx int) ([]string, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_VARCHAR {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]string, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	// This processes data in chunks rather than row-by-row
	const blockSize = 16 // Smaller block size for string operations due to copying overhead
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process block of null values first
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			nulls[pos] = bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				cStr := C.duckdb_value_varchar(dr.result, C.idx_t(colIdx), C.idx_t(pos))
				if cStr != nil {
					values[pos] = C.GoString(cStr)
					C.duckdb_free(unsafe.Pointer(cStr))
				} else {
					values[pos] = ""
				}
			}
		}
	}

	return values, nulls, nil
}

// ExtractStringColumnZeroCopy is deprecated, use ExtractStringColumnTrueZeroCopy directly
// Deprecated: Use ExtractStringColumnTrueZeroCopy instead for better performance
func (dr *DirectResult) ExtractStringColumnZeroCopy(colIdx int) ([]string, []bool, error) {
	return dr.ExtractStringColumnTrueZeroCopy(colIdx)
}

// ExtractBlobColumnTrueZeroCopy extracts a BLOB column with true zero-copy memory sharing
func (dr *DirectResult) ExtractBlobColumnTrueZeroCopy(colIdx int) ([][]byte, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_BLOB {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	blobs := make([][]byte, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	// This processes data in chunks rather than row-by-row
	const blockSize = 16 // Smaller block size for blob operations due to copying overhead
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process block of null values first
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			nulls[pos] = bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				blob := C.duckdb_value_blob(dr.result, C.idx_t(colIdx), C.idx_t(pos))
				if blob.data != nil && blob.size > 0 {
					// Ensure we make a copy of the data to avoid it being freed
					blobCopy := make([]byte, blob.size)
					C.memcpy(unsafe.Pointer(&blobCopy[0]), unsafe.Pointer(blob.data), C.size_t(blob.size))
					blobs[pos] = blobCopy
				} else {
					blobs[pos] = []byte{}
				}
			} else {
				blobs[pos] = []byte{}
			}
		}
	}

	return blobs, nulls, nil
}

// ExtractBlobColumnZeroCopy extracts a BLOB column with optimized memory management
// Now uses true zero-copy approach from ExtractBlobColumnTrueZeroCopy.
func (dr *DirectResult) ExtractBlobColumnZeroCopy(colIdx int) ([][]byte, []bool, error) {
	return dr.ExtractBlobColumnTrueZeroCopy(colIdx)
}

// ExtractBlobColumn extracts a BLOB column
// Returns a slice of byte slices and a null mask
func (dr *DirectResult) ExtractBlobColumn(colIdx int) ([][]byte, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Use the shared implementation from result_extraction.go
	return ExtractBlobColumnGeneric(dr, colIdx)
}

// ExtractStringColumnsBatch extracts multiple string columns in a single CGO call,
// significantly reducing overhead for result sets with many string columns.
func (dr *DirectResult) ExtractStringColumnsBatch(colIndices []int) ([][]string, [][]bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Validate column indices
	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= dr.columnCount {
			return nil, nil, ErrInvalidColumnIndex
		}

		// Check column type
		if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_VARCHAR {
			return nil, nil, fmt.Errorf("column %d is not a VARCHAR", colIdx)
		}
	}

	// Create output slices
	rowCount := int(dr.rowCount)
	numCols := len(colIndices)
	values := make([][]string, numCols)
	nulls := make([][]bool, numCols)

	// Initialize each column's values and nulls
	for i := range colIndices {
		values[i] = make([]string, rowCount)
		nulls[i] = make([]bool, rowCount)
	}

	// Process by block to reduce CGO boundary crossings
	const blockSize = 32
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process each column for this block
		for colIdx, dbColIdx := range colIndices {
			cColIdx := C.idx_t(dbColIdx)

			// Process block of null values first
			for i := 0; i < blockCount; i++ {
				pos := startRow + i
				rowIdx := C.idx_t(pos)
				nulls[colIdx][pos] = bool(C.duckdb_value_is_null(dr.result, cColIdx, rowIdx))
			}

			// Process block of actual values (only for non-null entries)
			for i := 0; i < blockCount; i++ {
				pos := startRow + i
				if !nulls[colIdx][pos] {
					rowIdx := C.idx_t(pos)
					cStr := C.duckdb_value_varchar(dr.result, cColIdx, rowIdx)
					if cStr != nil {
						values[colIdx][pos] = C.GoString(cStr)
						C.duckdb_free(unsafe.Pointer(cStr))
					} else {
						values[colIdx][pos] = ""
					}
				}
			}
		}
	}

	return values, nulls, nil
}

// ExtractBlobColumnsBatch extracts multiple BLOB columns in a single CGO call,
// significantly reducing overhead for result sets with many BLOB columns.
func (dr *DirectResult) ExtractBlobColumnsBatch(colIndices []int) ([][][]byte, [][]bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Validate column indices
	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= dr.columnCount {
			return nil, nil, ErrInvalidColumnIndex
		}

		// Check column type
		if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_BLOB {
			return nil, nil, fmt.Errorf("column %d is not a BLOB", colIdx)
		}
	}

	// Create output slices
	rowCount := int(dr.rowCount)
	numCols := len(colIndices)
	values := make([][][]byte, numCols)
	nulls := make([][]bool, numCols)

	// Initialize each column's values and nulls
	for i := range colIndices {
		values[i] = make([][]byte, rowCount)
		nulls[i] = make([]bool, rowCount)
	}

	// Process by block to reduce CGO boundary crossings
	const blockSize = 16 // Smaller block for BLOBs due to copy overhead
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process each column for this block
		for colIdx, dbColIdx := range colIndices {
			cColIdx := C.idx_t(dbColIdx)

			// Process block of null values first
			for i := 0; i < blockCount; i++ {
				pos := startRow + i
				rowIdx := C.idx_t(pos)
				nulls[colIdx][pos] = bool(C.duckdb_value_is_null(dr.result, cColIdx, rowIdx))
			}

			// Process block of actual values (only for non-null entries)
			for i := 0; i < blockCount; i++ {
				pos := startRow + i
				if !nulls[colIdx][pos] {
					rowIdx := C.idx_t(pos)
					blob := C.duckdb_value_blob(dr.result, cColIdx, rowIdx)
					if blob.data != nil && blob.size > 0 {
						// Make a copy of the blob data
						blobCopy := make([]byte, blob.size)
						C.memcpy(unsafe.Pointer(&blobCopy[0]), unsafe.Pointer(blob.data), C.size_t(blob.size))
						values[colIdx][pos] = blobCopy
					} else {
						values[colIdx][pos] = []byte{}
					}
				} else {
					values[colIdx][pos] = []byte{}
				}
			}
		}
	}

	return values, nulls, nil
}

// ExtractInt32ColumnsBatch extracts multiple int32 columns in a single optimized operation
func (dr *DirectResult) ExtractInt32ColumnsBatch(colIndices []int) ([][]int32, [][]bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Validate column indices
	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= dr.columnCount {
			return nil, nil, ErrInvalidColumnIndex
		}

		// Check column type
		if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_INTEGER {
			return nil, nil, fmt.Errorf("column %d is not an INTEGER", colIdx)
		}
	}

	// Create output slices
	rowCount := int(dr.rowCount)
	numCols := len(colIndices)
	values := make([][]int32, numCols)
	nulls := make([][]bool, numCols)

	// Initialize each column's values and nulls
	for i := range colIndices {
		values[i] = make([]int32, rowCount)
		nulls[i] = make([]bool, rowCount)
	}

	// Process by block to reduce CGO boundary crossings
	const blockSize = 64
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process each column for this block
		for colIdx, dbColIdx := range colIndices {
			cColIdx := C.idx_t(dbColIdx)

			// Process block of null values first
			for i := 0; i < blockCount; i++ {
				pos := startRow + i
				rowIdx := C.idx_t(pos)
				nulls[colIdx][pos] = bool(C.duckdb_value_is_null(dr.result, cColIdx, rowIdx))
			}

			// Process block of actual values (only for non-null entries)
			for i := 0; i < blockCount; i++ {
				pos := startRow + i
				if !nulls[colIdx][pos] {
					rowIdx := C.idx_t(pos)
					values[colIdx][pos] = int32(C.duckdb_value_int32(dr.result, cColIdx, rowIdx))
				}
			}
		}
	}

	return values, nulls, nil
}

// ExtractInt64ColumnsBatch extracts multiple int64 columns in a single optimized operation
func (dr *DirectResult) ExtractInt64ColumnsBatch(colIndices []int) ([][]int64, [][]bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Validate column indices
	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= dr.columnCount {
			return nil, nil, ErrInvalidColumnIndex
		}

		// Check column type
		if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_BIGINT {
			return nil, nil, fmt.Errorf("column %d is not a BIGINT", colIdx)
		}
	}

	// Create output slices
	rowCount := int(dr.rowCount)
	numCols := len(colIndices)
	values := make([][]int64, numCols)
	nulls := make([][]bool, numCols)

	// Initialize each column's values and nulls
	for i := range colIndices {
		values[i] = make([]int64, rowCount)
		nulls[i] = make([]bool, rowCount)
	}

	// Process by block to reduce CGO boundary crossings
	const blockSize = 64
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process each column for this block
		for colIdx, dbColIdx := range colIndices {
			cColIdx := C.idx_t(dbColIdx)

			// Process block of null values first
			for i := 0; i < blockCount; i++ {
				pos := startRow + i
				rowIdx := C.idx_t(pos)
				nulls[colIdx][pos] = bool(C.duckdb_value_is_null(dr.result, cColIdx, rowIdx))
			}

			// Process block of actual values (only for non-null entries)
			for i := 0; i < blockCount; i++ {
				pos := startRow + i
				if !nulls[colIdx][pos] {
					rowIdx := C.idx_t(pos)
					values[colIdx][pos] = int64(C.duckdb_value_int64(dr.result, cColIdx, rowIdx))
				}
			}
		}
	}

	return values, nulls, nil
}

// ExtractFloat64ColumnsBatch extracts multiple float64 columns in a single optimized operation
func (dr *DirectResult) ExtractFloat64ColumnsBatch(colIndices []int) ([][]float64, [][]bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Validate column indices
	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= dr.columnCount {
			return nil, nil, ErrInvalidColumnIndex
		}

		// Check column type
		if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_DOUBLE {
			return nil, nil, fmt.Errorf("column %d is not a DOUBLE", colIdx)
		}
	}

	// Create output slices
	rowCount := int(dr.rowCount)
	numCols := len(colIndices)
	values := make([][]float64, numCols)
	nulls := make([][]bool, numCols)

	// Initialize each column's values and nulls
	for i := range colIndices {
		values[i] = make([]float64, rowCount)
		nulls[i] = make([]bool, rowCount)
	}

	// Process by block to reduce CGO boundary crossings
	const blockSize = 64
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process each column for this block
		for colIdx, dbColIdx := range colIndices {
			cColIdx := C.idx_t(dbColIdx)

			// Process block of null values first
			for i := 0; i < blockCount; i++ {
				pos := startRow + i
				rowIdx := C.idx_t(pos)
				nulls[colIdx][pos] = bool(C.duckdb_value_is_null(dr.result, cColIdx, rowIdx))
			}

			// Process block of actual values (only for non-null entries)
			for i := 0; i < blockCount; i++ {
				pos := startRow + i
				if !nulls[colIdx][pos] {
					rowIdx := C.idx_t(pos)
					values[colIdx][pos] = float64(C.duckdb_value_double(dr.result, cColIdx, rowIdx))
				}
			}
		}
	}

	return values, nulls, nil
}

// GetValue retrieves a single value from the result set
// Returns the value, a boolean indicating if it was NULL, and any error
func (dr *DirectResult) GetValue(colIdx int, rowIdx int) (interface{}, bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, false, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, false, ErrInvalidColumnIndex
	}

	if rowIdx < 0 || int64(rowIdx) >= dr.rowCount {
		return nil, false, ErrInvalidRowIndex
	}

	// Check if value is NULL
	cColIdx := C.idx_t(colIdx)
	cRowIdx := C.idx_t(rowIdx)
	if bool(C.duckdb_value_is_null(dr.result, cColIdx, cRowIdx)) {
		return nil, true, nil
	}

	// Get value based on column type
	switch dr.columnTypes[colIdx] {
	case C.DUCKDB_TYPE_BOOLEAN:
		value := bool(C.duckdb_value_boolean(dr.result, cColIdx, cRowIdx))
		return value, false, nil

	case C.DUCKDB_TYPE_TINYINT:
		value := int8(C.duckdb_value_int8(dr.result, cColIdx, cRowIdx))
		return value, false, nil

	case C.DUCKDB_TYPE_SMALLINT:
		value := int16(C.duckdb_value_int16(dr.result, cColIdx, cRowIdx))
		return value, false, nil

	case C.DUCKDB_TYPE_INTEGER:
		value := int32(C.duckdb_value_int32(dr.result, cColIdx, cRowIdx))
		return value, false, nil

	case C.DUCKDB_TYPE_BIGINT:
		value := int64(C.duckdb_value_int64(dr.result, cColIdx, cRowIdx))
		return value, false, nil

	case C.DUCKDB_TYPE_UTINYINT:
		value := uint8(C.duckdb_value_uint8(dr.result, cColIdx, cRowIdx))
		return value, false, nil

	case C.DUCKDB_TYPE_USMALLINT:
		value := uint16(C.duckdb_value_uint16(dr.result, cColIdx, cRowIdx))
		return value, false, nil

	case C.DUCKDB_TYPE_UINTEGER:
		value := uint32(C.duckdb_value_uint32(dr.result, cColIdx, cRowIdx))
		return value, false, nil

	case C.DUCKDB_TYPE_UBIGINT:
		value := uint64(C.duckdb_value_uint64(dr.result, cColIdx, cRowIdx))
		return value, false, nil

	case C.DUCKDB_TYPE_FLOAT:
		value := float32(C.duckdb_value_float(dr.result, cColIdx, cRowIdx))
		return value, false, nil

	case C.DUCKDB_TYPE_DOUBLE:
		value := float64(C.duckdb_value_double(dr.result, cColIdx, cRowIdx))
		return value, false, nil

	case C.DUCKDB_TYPE_VARCHAR:
		cStr := C.duckdb_value_varchar(dr.result, cColIdx, cRowIdx)
		if cStr == nil {
			return "", false, nil
		}
		defer C.duckdb_free(unsafe.Pointer(cStr))
		return C.GoString(cStr), false, nil

	case C.DUCKDB_TYPE_BLOB:
		blob := C.duckdb_value_blob(dr.result, cColIdx, cRowIdx)
		if blob.data == nil {
			return []byte{}, false, nil
		}
		blobCopy := make([]byte, blob.size)
		C.memcpy(unsafe.Pointer(&blobCopy[0]), unsafe.Pointer(blob.data), C.size_t(blob.size))
		return blobCopy, false, nil

	case C.DUCKDB_TYPE_TIMESTAMP:
		// Get timestamp in microseconds
		ts := C.duckdb_value_timestamp(dr.result, cColIdx, cRowIdx)
		// Convert to Go time.Time (microseconds to seconds and nanoseconds)
		micros := int64(ts.micros)
		return time.Unix(micros/1000000, (micros%1000000)*1000), false, nil

	case C.DUCKDB_TYPE_DATE:
		// Get date in days since epoch
		date := C.duckdb_value_date(dr.result, cColIdx, cRowIdx)
		// Convert to Go time.Time (days to seconds)
		days := int32(date.days)
		return time.Unix(int64(days)*24*60*60, 0).UTC(), false, nil

	case C.DUCKDB_TYPE_TIME:
		// Get time in microseconds
		time := C.duckdb_value_time(dr.result, cColIdx, cRowIdx)
		// Return as microseconds past midnight
		return int64(time.micros), false, nil

	default:
		return nil, false, fmt.Errorf("unsupported column type: %d", dr.columnTypes[colIdx])
	}
}

// ExtractBoolColumn extracts a boolean column
func (dr *DirectResult) ExtractBoolColumn(colIdx int) ([]bool, []bool, error) {
	return dr.ExtractBool8Column(colIdx)
}

// ExtractBool8Column extracts a boolean column as []bool8 (not compressed)
func (dr *DirectResult) ExtractBool8Column(colIdx int) ([]bool, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_BOOLEAN {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]bool, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	// This processes data in chunks rather than row-by-row
	const blockSize = 64
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process block of null values first
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			nulls[pos] = bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = bool(C.duckdb_value_boolean(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractInt8Column extracts an int8 column
func (dr *DirectResult) ExtractInt8Column(colIdx int) ([]int8, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_TINYINT {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]int8, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process block of null values first
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			nulls[pos] = bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = int8(C.duckdb_value_int8(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractInt16Column extracts an int16 column
func (dr *DirectResult) ExtractInt16Column(colIdx int) ([]int16, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_SMALLINT {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]int16, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process block of null values first
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			nulls[pos] = bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = int16(C.duckdb_value_int16(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractUint8Column extracts a uint8 column
func (dr *DirectResult) ExtractUint8Column(colIdx int) ([]uint8, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_UTINYINT {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]uint8, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process block of null values first
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			nulls[pos] = bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = uint8(C.duckdb_value_uint8(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractUint16Column extracts a uint16 column
func (dr *DirectResult) ExtractUint16Column(colIdx int) ([]uint16, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_USMALLINT {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]uint16, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process block of null values first
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			nulls[pos] = bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = uint16(C.duckdb_value_uint16(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractUint32Column extracts a uint32 column
func (dr *DirectResult) ExtractUint32Column(colIdx int) ([]uint32, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_UINTEGER {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]uint32, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process block of null values first
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			nulls[pos] = bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = uint32(C.duckdb_value_uint32(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractUint64Column extracts a uint64 column
func (dr *DirectResult) ExtractUint64Column(colIdx int) ([]uint64, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_UBIGINT {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]uint64, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process block of null values first
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			nulls[pos] = bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = uint64(C.duckdb_value_uint64(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractFloat32Column extracts a float32 column
func (dr *DirectResult) ExtractFloat32Column(colIdx int) ([]float32, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_FLOAT {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]float32, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	for startRow := 0; startRow < rowCount; startRow += blockSize {
		// Calculate end of current block (handles final partial block)
		endRow := startRow + blockSize
		if endRow > rowCount {
			endRow = rowCount
		}
		blockCount := endRow - startRow

		// Process block of null values first
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			nulls[pos] = bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = float32(C.duckdb_value_float(dr.result, C.idx_t(colIdx), C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractTimestampColumn extracts a timestamp column
func (dr *DirectResult) ExtractTimestampColumn(colIdx int) ([]int64, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Use the shared implementation from result_extraction.go
	return ExtractTimestampColumnGeneric(dr, colIdx)
}

// ExtractDateColumn extracts a date column
func (dr *DirectResult) ExtractDateColumn(colIdx int) ([]int32, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Use the unified implementation
	return ExtractDateColumnGeneric(dr, colIdx)
}
