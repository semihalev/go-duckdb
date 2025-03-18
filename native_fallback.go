// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

/*
#cgo CFLAGS: -I${SRCDIR}/include
#include <stdlib.h>
#include <duckdb.h>

// Helper functions that use the direct C API
int duckdb_c_value_is_null(duckdb_result* result, idx_t col, idx_t row) {
    return duckdb_value_is_null(result, col, row);
}

int32_t duckdb_c_value_int32(duckdb_result* result, idx_t col, idx_t row) {
    return duckdb_value_int32(result, col, row);
}

int64_t duckdb_c_value_int64(duckdb_result* result, idx_t col, idx_t row) {
    return duckdb_value_int64(result, col, row);
}

double duckdb_c_value_double(duckdb_result* result, idx_t col, idx_t row) {
    return duckdb_value_double(result, col, row);
}

int8_t duckdb_c_value_boolean(duckdb_result* result, idx_t col, idx_t row) {
    return duckdb_value_boolean(result, col, row);
}

char* duckdb_c_value_varchar(duckdb_result* result, idx_t col, idx_t row) {
    return duckdb_value_varchar(result, col, row);
}
*/
import "C"
import "unsafe"

// This file contains pure Go fallback implementations for when the native
// library is not available. These implementations don't use SIMD optimizations,
// but they provide the same functionality through direct CGO calls to the DuckDB API.

// FallbackExtractInt32Column implements a pure Go version of ExtractInt32Column
func FallbackExtractInt32Column(result uintptr, colIdx int, outBuffer []int32, nullMask []bool, startRow, rowCount int) {
	// Don't process anything if parameters are invalid
	if result == 0 || len(outBuffer) < rowCount || len(nullMask) < rowCount {
		return
	}

	// Process each row
	for i := 0; i < rowCount; i++ {
		rowIdx := startRow + i

		// Check for null
		isNull := duckdbValueIsNull(result, colIdx, rowIdx)
		nullMask[i] = isNull

		if !isNull {
			// Extract value
			outBuffer[i] = duckdbValueInt32(result, colIdx, rowIdx)
		} else {
			outBuffer[i] = 0
		}
	}
}

// FallbackExtractInt64Column implements a pure Go version of ExtractInt64Column
func FallbackExtractInt64Column(result uintptr, colIdx int, outBuffer []int64, nullMask []bool, startRow, rowCount int) {
	// Don't process anything if parameters are invalid
	if result == 0 || len(outBuffer) < rowCount || len(nullMask) < rowCount {
		return
	}

	// Process each row
	for i := 0; i < rowCount; i++ {
		rowIdx := startRow + i

		// Check for null
		isNull := duckdbValueIsNull(result, colIdx, rowIdx)
		nullMask[i] = isNull

		if !isNull {
			// Extract value
			outBuffer[i] = duckdbValueInt64(result, colIdx, rowIdx)
		} else {
			outBuffer[i] = 0
		}
	}
}

// FallbackExtractFloat64Column implements a pure Go version of ExtractFloat64Column
func FallbackExtractFloat64Column(result uintptr, colIdx int, outBuffer []float64, nullMask []bool, startRow, rowCount int) {
	// Don't process anything if parameters are invalid
	if result == 0 || len(outBuffer) < rowCount || len(nullMask) < rowCount {
		return
	}

	// Process each row
	for i := 0; i < rowCount; i++ {
		rowIdx := startRow + i

		// Check for null
		isNull := duckdbValueIsNull(result, colIdx, rowIdx)
		nullMask[i] = isNull

		if !isNull {
			// Extract value
			outBuffer[i] = duckdbValueFloat64(result, colIdx, rowIdx)
		} else {
			outBuffer[i] = 0
		}
	}
}

// FallbackExtractBoolColumn implements a pure Go version of ExtractBoolColumn
func FallbackExtractBoolColumn(result uintptr, colIdx int, outBuffer []bool, nullMask []bool, startRow, rowCount int) {
	// Don't process anything if parameters are invalid
	if result == 0 || len(outBuffer) < rowCount || len(nullMask) < rowCount {
		return
	}

	// Process each row
	for i := 0; i < rowCount; i++ {
		rowIdx := startRow + i

		// Check for null
		isNull := duckdbValueIsNull(result, colIdx, rowIdx)
		nullMask[i] = isNull

		if !isNull {
			// Extract value
			outBuffer[i] = duckdbValueBool(result, colIdx, rowIdx)
		} else {
			outBuffer[i] = false
		}
	}
}

// FallbackExtractStringColumnPtrs implements a pure Go version of ExtractStringColumnPtrs
//
// Note on memory management:
// This function internally calls duckdb_value_varchar which allocates memory
// that needs to be freed with duckdb_free. The caller MUST call duckdb_free on 
// each non-nil pointer in outPtrs after use to prevent memory leaks.
// 
// Typically this is done in the higher-level extraction function, for example:
//   1. Results.Release() for ZeroCopyResult
//   2. StringTable.Release() for GlobalStringTable
//   3. Explicit C.duckdb_free() calls in non-zero-copy extractors
func FallbackExtractStringColumnPtrs(result uintptr, colIdx int, outPtrs []*byte, outLens []int32, nullMask []bool, startRow, rowCount int) {
	// Don't process anything if parameters are invalid
	if result == 0 || len(outPtrs) < rowCount || len(outLens) < rowCount || len(nullMask) < rowCount {
		return
	}

	// Process each row
	for i := 0; i < rowCount; i++ {
		rowIdx := startRow + i

		// Check for null
		isNull := duckdbValueIsNull(result, colIdx, rowIdx)
		nullMask[i] = isNull

		if !isNull {
			// Extract string value
			strVal := duckdbValueVarchar(result, colIdx, rowIdx)
			if strVal != nil {
				outPtrs[i] = strVal
				// Calculate length with safety bounds
				outLens[i] = int32(cStrLen(strVal))
			} else {
				outPtrs[i] = nil
				outLens[i] = 0
			}
		} else {
			outPtrs[i] = nil
			outLens[i] = 0
		}
	}
}

// Direct access to DuckDB result data through CGO
// These functions are used when the native optimized library is not available

// duckdbValueIsNull checks if a value in the result set is NULL
func duckdbValueIsNull(result uintptr, col, row int) bool {
	if result == 0 {
		// Safety check - result should never be nil
		return true
	}

	// Convert uintptr to *C.duckdb_result
	cResult := (*C.duckdb_result)(unsafe.Pointer(result))

	// Call the C API function
	isNull := C.duckdb_c_value_is_null(cResult, C.idx_t(col), C.idx_t(row))
	return isNull != 0
}

// duckdbValueInt32 retrieves an int32 value from the result set
func duckdbValueInt32(result uintptr, col, row int) int32 {
	if result == 0 {
		// Return a sensible default if result is nil
		return 0
	}

	// Convert uintptr to *C.duckdb_result
	cResult := (*C.duckdb_result)(unsafe.Pointer(result))

	// Check if value is NULL first
	if duckdbValueIsNull(result, col, row) {
		return 0
	}

	// Call the C API function
	return int32(C.duckdb_c_value_int32(cResult, C.idx_t(col), C.idx_t(row)))
}

// duckdbValueInt64 retrieves an int64 value from the result set
func duckdbValueInt64(result uintptr, col, row int) int64 {
	if result == 0 {
		// Return a sensible default if result is nil
		return 0
	}

	// Convert uintptr to *C.duckdb_result
	cResult := (*C.duckdb_result)(unsafe.Pointer(result))

	// Check if value is NULL first
	if duckdbValueIsNull(result, col, row) {
		return 0
	}

	// Call the C API function
	return int64(C.duckdb_c_value_int64(cResult, C.idx_t(col), C.idx_t(row)))
}

// duckdbValueFloat64 retrieves a float64 value from the result set
func duckdbValueFloat64(result uintptr, col, row int) float64 {
	if result == 0 {
		// Return a sensible default if result is nil
		return 0
	}

	// Convert uintptr to *C.duckdb_result
	cResult := (*C.duckdb_result)(unsafe.Pointer(result))

	// Check if value is NULL first
	if duckdbValueIsNull(result, col, row) {
		return 0
	}

	// Call the C API function
	return float64(C.duckdb_c_value_double(cResult, C.idx_t(col), C.idx_t(row)))
}

// duckdbValueBool retrieves a boolean value from the result set
func duckdbValueBool(result uintptr, col, row int) bool {
	if result == 0 {
		// Return a sensible default if result is nil
		return false
	}

	// Convert uintptr to *C.duckdb_result
	cResult := (*C.duckdb_result)(unsafe.Pointer(result))

	// Check if value is NULL first
	if duckdbValueIsNull(result, col, row) {
		return false
	}

	// Call the C API function
	return C.duckdb_c_value_boolean(cResult, C.idx_t(col), C.idx_t(row)) != 0
}

// duckdbValueVarchar retrieves a string value from the result set
// IMPORTANT: The memory allocated by this function must be freed using C.duckdb_free
// or it will leak. The caller is responsible for ensuring this happens.
func duckdbValueVarchar(result uintptr, col, row int) *byte {
	if result == 0 {
		// Return nil if result is nil
		return nil
	}

	// Convert uintptr to *C.duckdb_result
	cResult := (*C.duckdb_result)(unsafe.Pointer(result))

	// Check if value is NULL first
	if duckdbValueIsNull(result, col, row) {
		return nil
	}

	// Call the C API function and convert to Go pointer
	cStr := C.duckdb_c_value_varchar(cResult, C.idx_t(col), C.idx_t(row))
	if cStr == nil {
		return nil
	}

	// Return as byte pointer - CRITICAL: caller MUST free this memory with C.duckdb_free after use
	return (*byte)(unsafe.Pointer(cStr))
}

// cStrLen calculates the length of a C string (null-terminated)
// with a maximum length limit to prevent buffer overruns
func cStrLen(s *byte) int {
	// Calculate C string length (until null terminator)
	if s == nil {
		return 0
	}

	p := unsafe.Pointer(s)
	count := 0
	// Add a safety limit to prevent potential buffer overruns
	// Most database strings shouldn't exceed this reasonable limit
	const maxLen = 1 << 20 // 1 MB maximum string length
	
	for count < maxLen {
		if *(*byte)(p) == 0 {
			break
		}
		count++
		p = unsafe.Pointer(uintptr(p) + 1)
	}

	// If we hit the limit without finding a null terminator,
	// it's likely an invalid string or memory corruption
	if count == maxLen {
		// Return a safe length, logging could be added here in the future
		return count
	}

	return count
}
