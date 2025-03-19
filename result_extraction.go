package duckdb

/*
#include <stdlib.h>
#include <string.h>
#include <duckdb.h>
#include "duckdb_go_adapter.h"
*/
import "C"

import (
	"fmt"
	"time"
	"unsafe"
)

// ResultExtractor provides a unified interface for extracting column data
// from different DuckDB result types.
type ResultExtractor interface {
	// ExtractTypedColumn extracts a column of values with the given type
	ExtractTypedColumn(colIdx, startRow, rowCount int) (*ColumnVector, error)

	// ExtractInt32Column extracts a column of int32 values
	ExtractInt32Column(colIdx int) ([]int32, []bool, error)

	// ExtractInt64Column extracts a column of int64 values
	ExtractInt64Column(colIdx int) ([]int64, []bool, error)

	// ExtractFloat64Column extracts a column of float64 values
	ExtractFloat64Column(colIdx int) ([]float64, []bool, error)

	// ExtractStringColumn extracts a column of string values
	ExtractStringColumn(colIdx int) ([]string, []bool, error)

	// ExtractBlobColumn extracts a column of []byte values
	ExtractBlobColumn(colIdx int) ([][]byte, []bool, error)

	// ExtractTimestampColumn extracts a column of timestamp values
	ExtractTimestampColumn(colIdx int) ([]int64, []bool, error)

	// ExtractTimeColumn extracts a column of timestamp values as Go time.Time
	ExtractTimeColumn(colIdx int) ([]time.Time, []bool, error)

	// ExtractDateColumn extracts a column of date values as days since epoch
	ExtractDateColumn(colIdx int) ([]int32, []bool, error)
}

// Result is the common interface for any DuckDB result
type Result interface {
	ColumnCount() int
	RowCount() int64
	ColumnType(colIdx int) C.duckdb_type
	GetResultPointer() *C.duckdb_result
	IsNull(colIdx, rowIdx int) bool
	Close() error
}

// ExtractInt32ColumnGeneric is a generic implementation for extracting int32 columns
// This implementation can be shared between DirectResult and Rows
func ExtractInt32ColumnGeneric(result Result, colIdx int) ([]int32, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_INTEGER {
		return nil, nil, fmt.Errorf("column %d is not an INTEGER", colIdx)
	}

	// Create output slices
	values := make([]int32, rowCount)
	nulls := make([]bool, rowCount)

	// Extract using optimized batch extraction
	vector, err := ExtractColumnBatchTyped(resultPtr, colIdx, 0, rowCount)
	if err != nil {
		// Fall back to block-based extraction if optimized method fails
		const blockSize = 64
		cColIdx := C.idx_t(colIdx)

		for blockStart := 0; blockStart < rowCount; blockStart += blockSize {
			blockEnd := blockStart + blockSize
			if blockEnd > rowCount {
				blockEnd = rowCount
			}

			// Extract nulls
			for i := blockStart; i < blockEnd; i++ {
				nulls[i] = bool(C.duckdb_value_is_null(resultPtr, cColIdx, C.idx_t(i)))
			}

			// Extract values
			for i := blockStart; i < blockEnd; i++ {
				if !nulls[i] {
					values[i] = int32(C.duckdb_value_int32(resultPtr, cColIdx, C.idx_t(i)))
				}
			}
		}
	} else {
		// Copy data from optimized vector
		if vector.int32Data != nil {
			copy(values, vector.int32Data[:rowCount])
		}
		if vector.nullMap != nil {
			copy(nulls, vector.nullMap[:rowCount])
		}

		// Return vector to pool
		PutPooledColumnVector(vector)
	}

	return values, nulls, nil
}

// ExtractInt64ColumnGeneric is a generic implementation for extracting int64 columns
func ExtractInt64ColumnGeneric(result Result, colIdx int) ([]int64, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_BIGINT {
		return nil, nil, fmt.Errorf("column %d is not a BIGINT", colIdx)
	}

	// Create output slices
	values := make([]int64, rowCount)
	nulls := make([]bool, rowCount)

	// Extract using optimized batch extraction
	vector, err := ExtractColumnBatchTyped(resultPtr, colIdx, 0, rowCount)
	if err != nil {
		// Fall back to block-based extraction if optimized method fails
		const blockSize = 64
		cColIdx := C.idx_t(colIdx)

		for blockStart := 0; blockStart < rowCount; blockStart += blockSize {
			blockEnd := blockStart + blockSize
			if blockEnd > rowCount {
				blockEnd = rowCount
			}

			// Extract nulls
			for i := blockStart; i < blockEnd; i++ {
				nulls[i] = bool(C.duckdb_value_is_null(resultPtr, cColIdx, C.idx_t(i)))
			}

			// Extract values
			for i := blockStart; i < blockEnd; i++ {
				if !nulls[i] {
					values[i] = int64(C.duckdb_value_int64(resultPtr, cColIdx, C.idx_t(i)))
				}
			}
		}
	} else {
		// Copy data from optimized vector
		if vector.int64Data != nil {
			copy(values, vector.int64Data[:rowCount])
		}
		if vector.nullMap != nil {
			copy(nulls, vector.nullMap[:rowCount])
		}

		// Return vector to pool
		PutPooledColumnVector(vector)
	}

	return values, nulls, nil
}

// ExtractFloat64ColumnGeneric is a generic implementation for extracting float64 columns
func ExtractFloat64ColumnGeneric(result Result, colIdx int) ([]float64, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_DOUBLE {
		return nil, nil, fmt.Errorf("column %d is not a DOUBLE", colIdx)
	}

	// Create output slices
	values := make([]float64, rowCount)
	nulls := make([]bool, rowCount)

	// Extract using optimized batch extraction
	vector, err := ExtractColumnBatchTyped(resultPtr, colIdx, 0, rowCount)
	if err != nil {
		// Fall back to block-based extraction if optimized method fails
		const blockSize = 64
		cColIdx := C.idx_t(colIdx)

		for blockStart := 0; blockStart < rowCount; blockStart += blockSize {
			blockEnd := blockStart + blockSize
			if blockEnd > rowCount {
				blockEnd = rowCount
			}

			// Extract nulls
			for i := blockStart; i < blockEnd; i++ {
				nulls[i] = bool(C.duckdb_value_is_null(resultPtr, cColIdx, C.idx_t(i)))
			}

			// Extract values
			for i := blockStart; i < blockEnd; i++ {
				if !nulls[i] {
					values[i] = float64(C.duckdb_value_double(resultPtr, cColIdx, C.idx_t(i)))
				}
			}
		}
	} else {
		// Copy data from optimized vector
		if vector.float64Data != nil {
			copy(values, vector.float64Data[:rowCount])
		}
		if vector.nullMap != nil {
			copy(nulls, vector.nullMap[:rowCount])
		}

		// Return vector to pool
		PutPooledColumnVector(vector)
	}

	return values, nulls, nil
}

// ExtractStringColumnGeneric is a generic implementation for extracting string columns
func ExtractStringColumnGeneric(result Result, colIdx int) ([]string, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_VARCHAR {
		return nil, nil, fmt.Errorf("column %d is not a VARCHAR", colIdx)
	}

	// Create output slices
	values := make([]string, rowCount)
	nulls := make([]bool, rowCount)

	// Extract using optimized batch extraction
	vector, err := ExtractColumnBatchTyped(resultPtr, colIdx, 0, rowCount)
	if err != nil {
		// Fall back to block-based extraction if optimized method fails
		const blockSize = 16 // Smaller block size for strings due to higher overhead
		cColIdx := C.idx_t(colIdx)

		for blockStart := 0; blockStart < rowCount; blockStart += blockSize {
			blockEnd := blockStart + blockSize
			if blockEnd > rowCount {
				blockEnd = rowCount
			}

			// Process block
			for i := blockStart; i < blockEnd; i++ {
				rowIdx := C.idx_t(i)
				isNull := bool(C.duckdb_value_is_null(resultPtr, cColIdx, rowIdx))
				nulls[i] = isNull

				if !isNull {
					// Get string value
					cStr := C.duckdb_value_varchar(resultPtr, cColIdx, rowIdx)
					if cStr != nil {
						values[i] = C.GoString(cStr)
						C.duckdb_free(unsafe.Pointer(cStr))
					} else {
						values[i] = ""
					}
				} else {
					values[i] = ""
				}
			}
		}
	} else {
		// Copy data from optimized vector
		if vector.stringData != nil {
			copy(values, vector.stringData[:rowCount])
		}
		if vector.nullMap != nil {
			copy(nulls, vector.nullMap[:rowCount])
		}

		// Return vector to pool
		PutPooledColumnVector(vector)
	}

	return values, nulls, nil
}

// ExtractBlobColumnGeneric is a generic implementation for extracting blob columns
func ExtractBlobColumnGeneric(result Result, colIdx int) ([][]byte, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_BLOB {
		return nil, nil, fmt.Errorf("column %d is not a BLOB", colIdx)
	}

	// Create output slices
	values := make([][]byte, rowCount)
	nulls := make([]bool, rowCount)

	// Extract using optimized batch extraction
	vector, err := ExtractColumnBatchTyped(resultPtr, colIdx, 0, rowCount)
	if err != nil {
		// Fall back to block-based extraction if optimized method fails
		const blockSize = 16 // Smaller block size for blobs due to higher overhead
		cColIdx := C.idx_t(colIdx)

		for blockStart := 0; blockStart < rowCount; blockStart += blockSize {
			blockEnd := blockStart + blockSize
			if blockEnd > rowCount {
				blockEnd = rowCount
			}

			// Process block
			for i := blockStart; i < blockEnd; i++ {
				rowIdx := C.idx_t(i)
				isNull := bool(C.duckdb_value_is_null(resultPtr, cColIdx, rowIdx))
				nulls[i] = isNull

				if !isNull {
					// Get blob value
					cBlob := C.duckdb_value_blob(resultPtr, cColIdx, rowIdx)
					if cBlob.data != nil && cBlob.size > 0 {
						// Allocate Go memory for the blob
						blob := make([]byte, cBlob.size)
						// Copy blob data to Go memory
						C.memcpy(unsafe.Pointer(&blob[0]), unsafe.Pointer(cBlob.data), C.size_t(cBlob.size))
						values[i] = blob
					} else {
						values[i] = []byte{}
					}
				} else {
					values[i] = []byte{}
				}
			}
		}
	} else {
		// Copy data from optimized vector
		if vector.blobData != nil {
			// Deep copy each blob
			for i := 0; i < rowCount; i++ {
				if i < len(vector.blobData) && vector.blobData[i] != nil {
					blob := make([]byte, len(vector.blobData[i]))
					copy(blob, vector.blobData[i])
					values[i] = blob
				} else {
					values[i] = []byte{}
				}
			}
		}
		if vector.nullMap != nil {
			copy(nulls, vector.nullMap[:rowCount])
		}

		// Return vector to pool
		PutPooledColumnVector(vector)
	}

	return values, nulls, nil
}

// ExtractTimestampColumnGeneric is a generic implementation for extracting timestamp columns
func ExtractTimestampColumnGeneric(result Result, colIdx int) ([]int64, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_TIMESTAMP {
		return nil, nil, fmt.Errorf("column %d is not a TIMESTAMP", colIdx)
	}

	// Create output slices
	values := make([]int64, rowCount)
	nulls := make([]bool, rowCount)

	// Extract using optimized batch extraction
	vector, err := ExtractColumnBatchTyped(resultPtr, colIdx, 0, rowCount)
	if err != nil {
		// Fall back to block-based extraction if optimized method fails
		const blockSize = 64
		cColIdx := C.idx_t(colIdx)

		for blockStart := 0; blockStart < rowCount; blockStart += blockSize {
			blockEnd := blockStart + blockSize
			if blockEnd > rowCount {
				blockEnd = rowCount
			}

			// Extract nulls
			for i := blockStart; i < blockEnd; i++ {
				nulls[i] = bool(C.duckdb_value_is_null(resultPtr, cColIdx, C.idx_t(i)))
			}

			// Extract values
			for i := blockStart; i < blockEnd; i++ {
				if !nulls[i] {
					ts := C.duckdb_value_timestamp(resultPtr, cColIdx, C.idx_t(i))
					// Extract microseconds
					values[i] = int64(ts.micros)
				}
			}
		}
	} else {
		// Copy data from optimized vector
		if vector.timestampData != nil {
			copy(values, vector.timestampData[:rowCount])
		}
		if vector.nullMap != nil {
			copy(nulls, vector.nullMap[:rowCount])
		}

		// Return vector to pool
		PutPooledColumnVector(vector)
	}

	return values, nulls, nil
}

// ExtractDateColumnGeneric is a generic implementation for extracting date columns
func ExtractDateColumnGeneric(result Result, colIdx int) ([]int32, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_DATE {
		return nil, nil, fmt.Errorf("column %d is not a DATE", colIdx)
	}

	// Create output slices
	values := make([]int32, rowCount)
	nulls := make([]bool, rowCount)

	// Extract using optimized batch extraction
	vector, err := ExtractColumnBatchTyped(resultPtr, colIdx, 0, rowCount)
	if err != nil {
		// Fall back to block-based extraction if optimized method fails
		const blockSize = 64
		cColIdx := C.idx_t(colIdx)

		for blockStart := 0; blockStart < rowCount; blockStart += blockSize {
			blockEnd := blockStart + blockSize
			if blockEnd > rowCount {
				blockEnd = rowCount
			}

			// Extract nulls
			for i := blockStart; i < blockEnd; i++ {
				nulls[i] = result.IsNull(colIdx, i)
			}

			// Extract values
			for i := blockStart; i < blockEnd; i++ {
				if !nulls[i] {
					date := C.duckdb_value_date(resultPtr, cColIdx, C.idx_t(i))
					values[i] = int32(date.days)
				}
			}
		}
	} else {
		// Copy data from optimized vector
		if vector.int32Data != nil {
			copy(values, vector.int32Data[:rowCount])
		}
		if vector.nullMap != nil {
			copy(nulls, vector.nullMap[:rowCount])
		}

		// Return vector to pool
		PutPooledColumnVector(vector)
	}

	return values, nulls, nil
}

// ExtractBoolColumnGeneric is a generic implementation for extracting boolean columns
func ExtractBoolColumnGeneric(result Result, colIdx int) ([]bool, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_BOOLEAN {
		return nil, nil, fmt.Errorf("column %d is not a BOOLEAN", colIdx)
	}

	// Create output slices
	values := make([]bool, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	cColIdx := C.idx_t(colIdx)

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
			nulls[pos] = result.IsNull(colIdx, pos)
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = bool(C.duckdb_value_boolean(resultPtr, cColIdx, C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractInt8ColumnGeneric is a generic implementation for extracting int8 columns
func ExtractInt8ColumnGeneric(result Result, colIdx int) ([]int8, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_TINYINT {
		return nil, nil, fmt.Errorf("column %d is not a TINYINT", colIdx)
	}

	// Create output slices
	values := make([]int8, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	cColIdx := C.idx_t(colIdx)

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
			nulls[pos] = result.IsNull(colIdx, pos)
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = int8(C.duckdb_value_int8(resultPtr, cColIdx, C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractInt16ColumnGeneric is a generic implementation for extracting int16 columns
func ExtractInt16ColumnGeneric(result Result, colIdx int) ([]int16, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_SMALLINT {
		return nil, nil, fmt.Errorf("column %d is not a SMALLINT", colIdx)
	}

	// Create output slices
	values := make([]int16, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	cColIdx := C.idx_t(colIdx)

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
			nulls[pos] = result.IsNull(colIdx, pos)
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = int16(C.duckdb_value_int16(resultPtr, cColIdx, C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractUint8ColumnGeneric is a generic implementation for extracting uint8 columns
func ExtractUint8ColumnGeneric(result Result, colIdx int) ([]uint8, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_UTINYINT {
		return nil, nil, fmt.Errorf("column %d is not a UTINYINT", colIdx)
	}

	// Create output slices
	values := make([]uint8, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	cColIdx := C.idx_t(colIdx)

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
			nulls[pos] = result.IsNull(colIdx, pos)
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = uint8(C.duckdb_value_uint8(resultPtr, cColIdx, C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractUint16ColumnGeneric is a generic implementation for extracting uint16 columns
func ExtractUint16ColumnGeneric(result Result, colIdx int) ([]uint16, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_USMALLINT {
		return nil, nil, fmt.Errorf("column %d is not a USMALLINT", colIdx)
	}

	// Create output slices
	values := make([]uint16, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	cColIdx := C.idx_t(colIdx)

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
			nulls[pos] = result.IsNull(colIdx, pos)
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = uint16(C.duckdb_value_uint16(resultPtr, cColIdx, C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractUint32ColumnGeneric is a generic implementation for extracting uint32 columns
func ExtractUint32ColumnGeneric(result Result, colIdx int) ([]uint32, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_UINTEGER {
		return nil, nil, fmt.Errorf("column %d is not a UINTEGER", colIdx)
	}

	// Create output slices
	values := make([]uint32, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	cColIdx := C.idx_t(colIdx)

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
			nulls[pos] = result.IsNull(colIdx, pos)
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = uint32(C.duckdb_value_uint32(resultPtr, cColIdx, C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractUint64ColumnGeneric is a generic implementation for extracting uint64 columns
func ExtractUint64ColumnGeneric(result Result, colIdx int) ([]uint64, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_UBIGINT {
		return nil, nil, fmt.Errorf("column %d is not a UBIGINT", colIdx)
	}

	// Create output slices
	values := make([]uint64, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	cColIdx := C.idx_t(colIdx)

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
			nulls[pos] = result.IsNull(colIdx, pos)
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = uint64(C.duckdb_value_uint64(resultPtr, cColIdx, C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ExtractFloat32ColumnGeneric is a generic implementation for extracting float32 columns
func ExtractFloat32ColumnGeneric(result Result, colIdx int) ([]float32, []bool, error) {
	// Get result information
	resultPtr := result.GetResultPointer()
	rowCount := int(result.RowCount())

	// Validate column index
	if colIdx < 0 || colIdx >= result.ColumnCount() {
		return nil, nil, fmt.Errorf("column index out of range: %d", colIdx)
	}

	// Validate column type
	if result.ColumnType(colIdx) != C.DUCKDB_TYPE_FLOAT {
		return nil, nil, fmt.Errorf("column %d is not a FLOAT", colIdx)
	}

	// Create output slices
	values := make([]float32, rowCount)
	nulls := make([]bool, rowCount)

	// Use block-based extraction to reduce CGO boundary crossings
	const blockSize = 64
	cColIdx := C.idx_t(colIdx)

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
			nulls[pos] = result.IsNull(colIdx, pos)
		}

		// Process block of actual values (only for non-null entries)
		for i := 0; i < blockCount; i++ {
			pos := startRow + i
			if !nulls[pos] {
				values[pos] = float32(C.duckdb_value_float(resultPtr, cColIdx, C.idx_t(pos)))
			}
		}
	}

	return values, nulls, nil
}

// ConvertTimestampToTime converts DuckDB timestamp (microseconds since epoch) to Go time.Time
func ConvertTimestampToTime(micros int64) time.Time {
	return time.Unix(micros/1000000, (micros%1000000)*1000)
}

// ConvertTimestampsToTime converts a slice of DuckDB timestamps to Go time.Time values
func ConvertTimestampsToTime(timestamps []int64, nulls []bool) []time.Time {
	if len(timestamps) == 0 {
		return []time.Time{}
	}

	times := make([]time.Time, len(timestamps))
	for i, micros := range timestamps {
		if i < len(nulls) && !nulls[i] {
			times[i] = ConvertTimestampToTime(micros)
		}
	}

	return times
}
