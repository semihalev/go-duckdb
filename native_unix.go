//go:build !windows
// +build !windows

package duckdb

import (
	"errors"
	"unsafe"

	"github.com/ebitengine/purego"
)

// Load a dynamic library on Unix systems using purego
func loadDynamicLibrary(path string) (unsafe.Pointer, error) {
	handle, err := purego.Dlopen(path, purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return nil, err
	}
	return unsafe.Pointer(handle), nil
}

// Close the library
func closeLibrary(handle unsafe.Pointer) {
	if handle != nil {
		purego.Dlclose(uintptr(handle))
	}
}

// Get a symbol from the library
func getSymbol(handle unsafe.Pointer, name string) (unsafe.Pointer, error) {
	if handle == nil {
		return nil, errors.New("invalid library handle")
	}

	sym, err := purego.Dlsym(uintptr(handle), name)
	if err != nil {
		return nil, err
	}

	return unsafe.Pointer(sym), nil
}

// ExtractInt32Column calls the native function to extract int32 column data
func ExtractInt32Column(result uintptr, colIdx int, outBuffer []int32, nullMask []bool, startRow, rowCount int) {
	if !nativeLibLoaded || funcExtractInt32Column == nil {
		return
	}

	purego.SyscallN(uintptr(funcExtractInt32Column),
		result,
		uintptr(colIdx),
		uintptr(unsafe.Pointer(&outBuffer[0])),
		uintptr(unsafe.Pointer(&nullMask[0])),
		uintptr(startRow),
		uintptr(rowCount))
}

// ExtractInt64Column calls the native function to extract int64 column data
func ExtractInt64Column(result uintptr, colIdx int, outBuffer []int64, nullMask []bool, startRow, rowCount int) {
	if !nativeLibLoaded || funcExtractInt64Column == nil {
		return
	}

	purego.SyscallN(uintptr(funcExtractInt64Column),
		result,
		uintptr(colIdx),
		uintptr(unsafe.Pointer(&outBuffer[0])),
		uintptr(unsafe.Pointer(&nullMask[0])),
		uintptr(startRow),
		uintptr(rowCount))
}

// ExtractFloat64Column calls the native function to extract float64 column data
func ExtractFloat64Column(result uintptr, colIdx int, outBuffer []float64, nullMask []bool, startRow, rowCount int) {
	if !nativeLibLoaded || funcExtractFloat64Column == nil {
		return
	}

	purego.SyscallN(uintptr(funcExtractFloat64Column),
		result,
		uintptr(colIdx),
		uintptr(unsafe.Pointer(&outBuffer[0])),
		uintptr(unsafe.Pointer(&nullMask[0])),
		uintptr(startRow),
		uintptr(rowCount))
}

// ExtractBoolColumn calls the native function to extract boolean column data
func ExtractBoolColumn(result uintptr, colIdx int, outBuffer []bool, nullMask []bool, startRow, rowCount int) {
	if !nativeLibLoaded || funcExtractBoolColumn == nil {
		return
	}

	purego.SyscallN(uintptr(funcExtractBoolColumn),
		result,
		uintptr(colIdx),
		uintptr(unsafe.Pointer(&outBuffer[0])),
		uintptr(unsafe.Pointer(&nullMask[0])),
		uintptr(startRow),
		uintptr(rowCount))
}

// ExtractStringColumnPtrs calls the native function to extract string column pointers and lengths
func ExtractStringColumnPtrs(result uintptr, colIdx int, outPtrs []*byte, outLens []int32, nullMask []bool, startRow, rowCount int) {
	if !nativeLibLoaded || funcExtractStringColumn == nil {
		return
	}

	purego.SyscallN(uintptr(funcExtractStringColumn),
		result,
		uintptr(colIdx),
		uintptr(unsafe.Pointer(&outPtrs[0])),
		uintptr(unsafe.Pointer(&outLens[0])),
		uintptr(unsafe.Pointer(&nullMask[0])),
		uintptr(startRow),
		uintptr(rowCount))
}
