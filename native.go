// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"
)

// Library loader
var (
	nativeLibOnce    sync.Once
	nativeLibLoaded  bool
	nativeLibError   error
	nativeLibPath    string
	nativeLibHandler unsafe.Pointer
)

// Dynamically loaded native function pointers
var (
	funcExtractInt32Column     unsafe.Pointer
	funcExtractInt64Column     unsafe.Pointer
	funcExtractFloat64Column   unsafe.Pointer
	funcExtractStringColumn    unsafe.Pointer
	funcExtractBoolColumn      unsafe.Pointer
	funcExtractTimestampColumn unsafe.Pointer
	funcExtractDateColumn      unsafe.Pointer
	funcExtractBlobColumn      unsafe.Pointer
	funcFilterInt32ColumnGt    unsafe.Pointer
	funcExtractRowBatch        unsafe.Pointer

	// Reserved for future optimizations
)

// NativeOptimizationsAvailable returns true if native SIMD optimizations are available
func NativeOptimizationsAvailable() bool {
	loadNativeLibrary()
	return nativeLibLoaded
}

// GetNativeLibraryError returns any error that occurred during native library loading
func GetNativeLibraryError() error {
	loadNativeLibrary()
	return nativeLibError
}

// Attempts to load the native library
func loadNativeLibrary() {
	nativeLibOnce.Do(func() {
		// First, try to find a path to the native library
		nativeLibPath = findNativeLibraryPath()
		if nativeLibPath == "" {
			nativeLibError = errors.New("native optimization library not found")
			return
		}

		// Load the dynamic library
		handler, err := loadDynamicLibrary(nativeLibPath)
		if err != nil {
			nativeLibError = fmt.Errorf("failed to load native library: %v", err)
			return
		}
		nativeLibHandler = handler

		// Load all function pointers
		if !loadNativeFunctions() {
			closeLibrary(nativeLibHandler)
			nativeLibError = errors.New("failed to load one or more native functions")
			return
		}

		// Successfully loaded library and functions
		nativeLibLoaded = true
	})
}

// Find the path to the native library based on runtime OS and architecture
func findNativeLibraryPath() string {
	// Get the directory containing the current executable
	execPath, err := os.Executable()
	if err != nil {
		return ""
	}
	execDir := filepath.Dir(execPath)

	// Get the module directory (where go-duckdb is installed)
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	moduleDir := filepath.Dir(thisFile)

	// Determine library name based on OS
	var libName string
	switch runtime.GOOS {
	case "windows":
		libName = "duckdbnative.dll"
	case "darwin":
		libName = "libduckdbnative.dylib"
	case "linux":
		libName = "libduckdbnative.so"
	default:
		return ""
	}

	// OS and architecture specific path within the library
	osArchPath := filepath.Join("lib", runtime.GOOS, runtime.GOARCH, libName)
	srcPath := filepath.Join("src", libName)

	// Check several possible locations for the library
	searchPaths := []string{
		// Current directory
		filepath.Join(".", libName),
		// Executable directory
		filepath.Join(execDir, libName),
		// Module directory
		filepath.Join(moduleDir, libName),
		// Source directory in module
		filepath.Join(moduleDir, srcPath),
		// OS/arch-specific in module directory
		filepath.Join(moduleDir, osArchPath),
		// GO path
		filepath.Join(os.Getenv("GOPATH"), "pkg", "mod", "github.com", "semihalev", "go-duckdb", osArchPath),
		filepath.Join(os.Getenv("GOPATH"), "pkg", "mod", "github.com", "semihalev", "go-duckdb", srcPath),
	}

	// Try all paths
	for _, path := range searchPaths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}

	return ""
}

// Load all native function pointers from the library
func loadNativeFunctions() bool {
	var err error

	funcExtractInt32Column, err = getSymbol(nativeLibHandler, "extract_int32_column")
	if err != nil {
		return false
	}

	funcExtractInt64Column, err = getSymbol(nativeLibHandler, "extract_int64_column")
	if err != nil {
		return false
	}

	funcExtractFloat64Column, err = getSymbol(nativeLibHandler, "extract_float64_column")
	if err != nil {
		return false
	}

	funcExtractStringColumn, err = getSymbol(nativeLibHandler, "extract_string_column_ptrs")
	if err != nil {
		return false
	}

	funcExtractBoolColumn, err = getSymbol(nativeLibHandler, "extract_bool_column")
	if err != nil {
		return false
	}

	funcExtractTimestampColumn, err = getSymbol(nativeLibHandler, "extract_timestamp_column")
	if err != nil {
		return false
	}

	funcExtractDateColumn, err = getSymbol(nativeLibHandler, "extract_date_column")
	if err != nil {
		return false
	}

	funcExtractBlobColumn, err = getSymbol(nativeLibHandler, "extract_blob_column")
	if err != nil {
		return false
	}

	funcFilterInt32ColumnGt, err = getSymbol(nativeLibHandler, "filter_int32_column_gt")
	if err != nil {
		return false
	}

	funcExtractRowBatch, err = getSymbol(nativeLibHandler, "extract_row_batch")
	if err != nil {
		return false
	}

	// Reserved for future optimizations

	return true
}
