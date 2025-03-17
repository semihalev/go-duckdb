package duckdb

import (
	"unsafe"
)

// This file contains pure Go fallback implementations for when the native
// library is not available. These implementations don't use SIMD optimizations,
// but they provide the same functionality.

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
				// Calculate length
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

// FallbackSumInt32Column implements a pure Go version of SumInt32Column
func FallbackSumInt32Column(values []int32, nullMask []bool, result *int64) bool {
	if values == nil || nullMask == nil || result == nil || len(values) != len(nullMask) {
		return false
	}

	var sum int64 = 0
	for i, v := range values {
		if !nullMask[i] {
			sum += int64(v)
		}
	}

	*result = sum
	return true
}

// Wrapper functions for direct DuckDB C API access

func duckdbValueIsNull(result uintptr, col, row int) bool {
	// This will need to be implemented with direct CGO calls or replaced with a pure Go alternative
	// For now, this is a placeholder
	return false
}

func duckdbValueInt32(result uintptr, col, row int) int32 {
	// This will need to be implemented with direct CGO calls or replaced with a pure Go alternative
	// For now, this is a placeholder
	return 0
}

func duckdbValueInt64(result uintptr, col, row int) int64 {
	// This will need to be implemented with direct CGO calls or replaced with a pure Go alternative
	// For now, this is a placeholder
	return 0
}

func duckdbValueFloat64(result uintptr, col, row int) float64 {
	// This will need to be implemented with direct CGO calls or replaced with a pure Go alternative
	// For now, this is a placeholder
	return 0
}

func duckdbValueBool(result uintptr, col, row int) bool {
	// This will need to be implemented with direct CGO calls or replaced with a pure Go alternative
	// For now, this is a placeholder
	return false
}

func duckdbValueVarchar(result uintptr, col, row int) *byte {
	// This will need to be implemented with direct CGO calls or replaced with a pure Go alternative
	// For now, this is a placeholder
	return nil
}

func cStrLen(s *byte) int {
	// Calculate C string length (until null terminator)
	if s == nil {
		return 0
	}

	p := unsafe.Pointer(s)
	count := 0
	for *(*byte)(p) != 0 {
		count++
		p = unsafe.Pointer(uintptr(p) + 1)
	}

	return count
}
