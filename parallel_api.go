package duckdb

/*
#include <stdlib.h>
#include <stdbool.h>
#include <duckdb.h>
#include "include/duckdb_native.h"
*/
import "C"
import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"time"
	"unsafe"
)

// ParallelExtractor provides methods to extract multiple columns of the same type
// with parallel processing of the extracted data.
type ParallelExtractor struct {
	dr *DirectResult
}

// NewParallelExtractor creates a new ParallelExtractor for the given DirectResult
func NewParallelExtractor(dr *DirectResult) *ParallelExtractor {
	return &ParallelExtractor{dr: dr}
}

// ProcessInt32Columns extracts multiple int32 columns and processes them in parallel
// with the given processor function.
func (pe *ParallelExtractor) ProcessInt32Columns(colIndices []int, processor func(colIdx int, values []int32, nulls []bool) error) error {
	// Validate parameters
	if len(colIndices) == 0 {
		return fmt.Errorf("no columns specified for parallel processing")
	}

	// First extract all columns (sequentially)
	var columns [][]int32
	var nullMasks [][]bool

	for _, colIdx := range colIndices {
		values, nulls, err := pe.dr.ExtractInt32Column(colIdx)
		if err != nil {
			return fmt.Errorf("failed to extract column %d: %w", colIdx, err)
		}

		columns = append(columns, values)
		nullMasks = append(nullMasks, nulls)
	}

	// Then process the extracted data in parallel
	var wg sync.WaitGroup
	
	// We don't need a buffered error channel with our errOnce approach
	// Initialize it for consistent code structure but don't use it
	_ = make(chan error, len(colIndices)+1)
	
	// Create a context with cancellation to ensure goroutines can be stopped
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure context is cancelled when we're done
	
	// Keep track of the first error
	var processingErr error
	var errOnce sync.Once

	for i, colIdx := range colIndices {
		wg.Add(1)

		// Local variables to avoid race conditions in closure
		localI := i
		localColIdx := colIdx

		go func() {
			defer wg.Done()
			
			// Check if we should stop
			select {
			case <-ctx.Done():
				return
			default:
				// Continue processing
			}

			// Process the column data
			if err := processor(localColIdx, columns[localI], nullMasks[localI]); err != nil {
				errOnce.Do(func() {
					processingErr = fmt.Errorf("error processing column %d: %w", localColIdx, err)
					cancel() // Signal other goroutines to stop
				})
			}
		}()
	}

	// Wait for all processing to complete
	wg.Wait()

	// Return any error that occurred during processing
	if processingErr != nil {
		return processingErr
	}

	return nil
}

// ProcessInt64Columns extracts multiple int64 columns and processes them in parallel
// with the given processor function.
func (pe *ParallelExtractor) ProcessInt64Columns(colIndices []int, processor func(colIdx int, values []int64, nulls []bool) error) error {
	// Validate parameters
	if len(colIndices) == 0 {
		return fmt.Errorf("no columns specified for parallel processing")
	}

	// First extract all columns (sequentially)
	var columns [][]int64
	var nullMasks [][]bool

	for _, colIdx := range colIndices {
		values, nulls, err := pe.dr.ExtractInt64Column(colIdx)
		if err != nil {
			return fmt.Errorf("failed to extract column %d: %w", colIdx, err)
		}

		columns = append(columns, values)
		nullMasks = append(nullMasks, nulls)
	}

	// Then process the extracted data in parallel
	var wg sync.WaitGroup
	
	// We don't need a buffered error channel with our errOnce approach
	// Initialize it for consistent code structure but don't use it
	_ = make(chan error, len(colIndices)+1)
	
	// Create a context with cancellation to ensure goroutines can be stopped
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure context is cancelled when we're done
	
	// Keep track of the first error
	var processingErr error
	var errOnce sync.Once

	for i, colIdx := range colIndices {
		wg.Add(1)

		// Local variables to avoid race conditions in closure
		localI := i
		localColIdx := colIdx

		go func() {
			defer wg.Done()
			
			// Check if we should stop
			select {
			case <-ctx.Done():
				return
			default:
				// Continue processing
			}

			// Process the column data
			if err := processor(localColIdx, columns[localI], nullMasks[localI]); err != nil {
				errOnce.Do(func() {
					processingErr = fmt.Errorf("error processing column %d: %w", localColIdx, err)
					cancel() // Signal other goroutines to stop
				})
			}
		}()
	}

	// Wait for all processing to complete
	wg.Wait()

	// Return any error that occurred during processing
	if processingErr != nil {
		return processingErr
	}

	return nil
}

// ProcessFloat64Columns extracts multiple float64 columns and processes them in parallel
// with the given processor function.
func (pe *ParallelExtractor) ProcessFloat64Columns(colIndices []int, processor func(colIdx int, values []float64, nulls []bool) error) error {
	// Validate parameters
	if len(colIndices) == 0 {
		return fmt.Errorf("no columns specified for parallel processing")
	}

	// First extract all columns (sequentially)
	var columns [][]float64
	var nullMasks [][]bool

	for _, colIdx := range colIndices {
		values, nulls, err := pe.dr.ExtractFloat64Column(colIdx)
		if err != nil {
			return fmt.Errorf("failed to extract column %d: %w", colIdx, err)
		}

		columns = append(columns, values)
		nullMasks = append(nullMasks, nulls)
	}

	// Then process the extracted data in parallel
	var wg sync.WaitGroup
	
	// We don't need a buffered error channel with our errOnce approach
	// Initialize it for consistent code structure but don't use it
	_ = make(chan error, len(colIndices)+1)
	
	// Create a context with cancellation to ensure goroutines can be stopped
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure context is cancelled when we're done
	
	// Keep track of the first error
	var processingErr error
	var errOnce sync.Once

	for i, colIdx := range colIndices {
		wg.Add(1)

		// Local variables to avoid race conditions in closure
		localI := i
		localColIdx := colIdx

		go func() {
			defer wg.Done()
			
			// Check if we should stop
			select {
			case <-ctx.Done():
				return
			default:
				// Continue processing
			}

			// Process the column data
			if err := processor(localColIdx, columns[localI], nullMasks[localI]); err != nil {
				errOnce.Do(func() {
					processingErr = fmt.Errorf("error processing column %d: %w", localColIdx, err)
					cancel() // Signal other goroutines to stop
				})
			}
		}()
	}

	// Wait for all processing to complete
	wg.Wait()

	// Return any error that occurred during processing
	if processingErr != nil {
		return processingErr
	}

	return nil
}

// ProcessParallel is a more generic function that can be used with custom extraction
// and processing functions for any type of column.
func (pe *ParallelExtractor) ProcessParallel(
	colIndices []int,
	extractFn func(colIdx int) (interface{}, []bool, error),
	processFn func(colIdx int, values interface{}, nulls []bool) error,
) error {
	// Validate parameters
	if len(colIndices) == 0 {
		return fmt.Errorf("no columns specified for parallel processing")
	}

	// First extract all columns (sequentially)
	values := make([]interface{}, len(colIndices))
	nullMasks := make([][]bool, len(colIndices))

	for i, colIdx := range colIndices {
		val, nulls, err := extractFn(colIdx)
		if err != nil {
			return fmt.Errorf("failed to extract column %d: %w", colIdx, err)
		}

		values[i] = val
		nullMasks[i] = nulls
	}

	// Then process the extracted data in parallel
	var wg sync.WaitGroup
	
	// We don't need a buffered error channel with our errOnce approach
	// Initialize it for consistent code structure but don't use it
	_ = make(chan error, len(colIndices)+1)
	
	// Create a context with cancellation to ensure goroutines can be stopped
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure context is cancelled when we're done
	
	// Keep track of the first error
	var processingErr error
	var errOnce sync.Once

	for i, colIdx := range colIndices {
		wg.Add(1)

		// Local variables to avoid race conditions in closure
		localI := i
		localColIdx := colIdx

		go func() {
			defer wg.Done()
			
			// Check if we should stop
			select {
			case <-ctx.Done():
				return
			default:
				// Continue processing
			}

			// Process the column data with proper error handling
			if err := processFn(localColIdx, values[localI], nullMasks[localI]); err != nil {
				errOnce.Do(func() {
					processingErr = fmt.Errorf("error processing column %d: %w", localColIdx, err)
					cancel() // Signal other goroutines to stop
				})
			}
		}()
	}

	// Wait for all processing to complete
	wg.Wait()

	// Return any error that occurred during processing
	if processingErr != nil {
		return processingErr
	}

	return nil
}

// extractChunk extracts a chunk of data for a single column in a thread-safe manner
func (pe *ParallelExtractor) extractChunk(colIdx int, startRow, endRow int) (interface{}, []bool, error) {
	dr := pe.dr
	colType := dr.ColumnTypes()[colIdx]

	// Safety check to ensure we don't go past the end of the result set
	totalRows := int(dr.RowCount())
	if startRow >= totalRows {
		return nil, nil, fmt.Errorf("start row %d is beyond result set size %d", startRow, totalRows)
	}

	// Ensure endRow doesn't exceed total rows
	if endRow > totalRows {
		endRow = totalRows
	}

	// Calculate actual row count for this chunk
	rowCount := endRow - startRow
	if rowCount <= 0 {
		return nil, nil, fmt.Errorf("invalid row range: %d to %d", startRow, endRow)
	}

	// Determine column type and use the appropriate extraction method
	switch int(colType) {
	case 4: // INTEGER type (DUCKDB_TYPE_INTEGER = 4)
		values, nulls, err := extractInt32ColumnThreadSafe(dr, colIdx, startRow, rowCount)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to extract int32 column %d: %w", colIdx, err)
		}
		return values, nulls, nil

	case 5: // BIGINT type (DUCKDB_TYPE_BIGINT = 5)
		values, nulls, err := extractInt64ColumnThreadSafe(dr, colIdx, startRow, rowCount)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to extract int64 column %d: %w", colIdx, err)
		}
		return values, nulls, nil

	case 11: // DOUBLE type (DUCKDB_TYPE_DOUBLE = 11)
		values, nulls, err := extractFloat64ColumnThreadSafe(dr, colIdx, startRow, rowCount)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to extract float64 column %d: %w", colIdx, err)
		}
		return values, nulls, nil

	default:
		// Get type name for better error reporting
		typeName := "unknown"
		if colType >= 0 && colType <= 30 {
			// Map some common types to strings for better error messages
			// Match the actual DUCKDB_TYPE_* enum values from duckdb.h
			typeNames := []string{
				"INVALID", "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
				"UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "FLOAT", "DOUBLE",
				"TIMESTAMP", "DATE", "TIME", "INTERVAL", "HUGEINT", "VARCHAR",
				"BLOB", "DECIMAL", "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS",
				"ENUM", "LIST", "STRUCT", "MAP", "UUID", "JSON",
			}
			if int(colType) < len(typeNames) {
				typeName = typeNames[colType]
			}
		}
		return nil, nil, fmt.Errorf("unsupported column type %v (%s) for column %d", colType, typeName, colIdx)
	}
}

// extractInt32ColumnThreadSafe extracts an int32 column directly for a specific range in a thread-safe manner
// Uses specialized C function to avoid CGO thread issues with row-by-row extraction
func extractInt32ColumnThreadSafe(dr *DirectResult, colIdx int, startRow, rowCount int) ([]int32, []bool, error) {
	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_INTEGER {
		return nil, nil, ErrIncompatibleType
	}

	// Additional safety checks
	totalRows := int(dr.RowCount())
	if startRow < 0 || startRow >= totalRows {
		return nil, nil, fmt.Errorf("invalid start row: %d (total rows: %d)", startRow, totalRows)
	}

	if rowCount <= 0 {
		return nil, nil, fmt.Errorf("invalid row count: %d", rowCount)
	}

	// Make sure we don't read past the end of the result
	if startRow+rowCount > totalRows {
		rowCount = totalRows - startRow
	}

	// Create buffers for this chunk
	values := make([]int32, rowCount)
	nulls := make([]bool, rowCount)

	// Use a separate mutex to ensure safety during extraction
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Get pointers to Go slices
	valuesPtr := (*reflect.SliceHeader)(unsafe.Pointer(&values)).Data
	nullsPtr := (*reflect.SliceHeader)(unsafe.Pointer(&nulls)).Data

	// Call optimized C function with proper offset
	C.extract_int32_column(
		dr.result,
		C.idx_t(colIdx),
		(*C.int32_t)(unsafe.Pointer(valuesPtr)),
		(*C.bool)(unsafe.Pointer(nullsPtr)),
		C.idx_t(startRow),
		C.idx_t(rowCount),
	)

	return values, nulls, nil
}

// extractInt64ColumnThreadSafe extracts an int64 column directly for a specific range in a thread-safe manner
// Uses specialized C function to avoid CGO thread issues with row-by-row extraction
func extractInt64ColumnThreadSafe(dr *DirectResult, colIdx int, startRow, rowCount int) ([]int64, []bool, error) {
	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_BIGINT {
		return nil, nil, ErrIncompatibleType
	}

	// Additional safety checks
	totalRows := int(dr.RowCount())
	if startRow < 0 || startRow >= totalRows {
		return nil, nil, fmt.Errorf("invalid start row: %d (total rows: %d)", startRow, totalRows)
	}

	if rowCount <= 0 {
		return nil, nil, fmt.Errorf("invalid row count: %d", rowCount)
	}

	// Make sure we don't read past the end of the result
	if startRow+rowCount > totalRows {
		rowCount = totalRows - startRow
	}

	// Create buffers for this chunk
	values := make([]int64, rowCount)
	nulls := make([]bool, rowCount)

	// Use a separate mutex to ensure safety during extraction
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Get pointers to Go slices
	valuesPtr := (*reflect.SliceHeader)(unsafe.Pointer(&values)).Data
	nullsPtr := (*reflect.SliceHeader)(unsafe.Pointer(&nulls)).Data

	// Call optimized C function with proper offset
	C.extract_int64_column(
		dr.result,
		C.idx_t(colIdx),
		(*C.int64_t)(unsafe.Pointer(valuesPtr)),
		(*C.bool)(unsafe.Pointer(nullsPtr)),
		C.idx_t(startRow),
		C.idx_t(rowCount),
	)

	return values, nulls, nil
}

// extractFloat64ColumnThreadSafe extracts a float64 column directly for a specific range in a thread-safe manner
// Uses specialized C function to avoid CGO thread issues with row-by-row extraction
func extractFloat64ColumnThreadSafe(dr *DirectResult, colIdx int, startRow, rowCount int) ([]float64, []bool, error) {
	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_DOUBLE {
		return nil, nil, ErrIncompatibleType
	}

	// Additional safety checks
	totalRows := int(dr.RowCount())
	if startRow < 0 || startRow >= totalRows {
		return nil, nil, fmt.Errorf("invalid start row: %d (total rows: %d)", startRow, totalRows)
	}

	if rowCount <= 0 {
		return nil, nil, fmt.Errorf("invalid row count: %d", rowCount)
	}

	// Make sure we don't read past the end of the result
	if startRow+rowCount > totalRows {
		rowCount = totalRows - startRow
	}

	// Create buffers for this chunk
	values := make([]float64, rowCount)
	nulls := make([]bool, rowCount)

	// Use a separate mutex to ensure safety during extraction
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Get pointers to Go slices
	valuesPtr := (*reflect.SliceHeader)(unsafe.Pointer(&values)).Data
	nullsPtr := (*reflect.SliceHeader)(unsafe.Pointer(&nulls)).Data

	// Call optimized C function with proper offset
	C.extract_float64_column(
		dr.result,
		C.idx_t(colIdx),
		(*C.double)(unsafe.Pointer(valuesPtr)),
		(*C.bool)(unsafe.Pointer(nullsPtr)),
		C.idx_t(startRow),
		C.idx_t(rowCount),
	)

	return values, nulls, nil
}

// ProcessChunked processes large datasets in chunks for better memory efficiency
// The processor function is called for each chunk of data
func (pe *ParallelExtractor) ProcessChunked(
	colIndices []int,
	chunkSize int,
	processor func(chunkIdx int, colData map[int]interface{}, nullMasks map[int][]bool) error,
) error {
	if len(colIndices) == 0 {
		return fmt.Errorf("no columns specified for chunked processing")
	}

	if chunkSize <= 0 {
		return fmt.Errorf("invalid chunk size: %d", chunkSize)
	}

	// Validate column indices before processing
	dr := pe.dr
	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= dr.columnCount {
			return fmt.Errorf("invalid column index: %d (should be between 0 and %d)",
				colIdx, dr.columnCount-1)
		}
	}

	// Get row count with proper locking
	dr.mu.RLock()
	if dr.closed {
		dr.mu.RUnlock()
		return ErrResultClosed
	}
	rowCount := int(dr.RowCount())
	dr.mu.RUnlock()

	if rowCount == 0 {
		// No data to process
		return nil
	}

	// Calculate number of chunks
	numChunks := (rowCount + chunkSize - 1) / chunkSize

	// Cap the number of parallel goroutines to avoid overwhelming the system
	maxParallel := runtime.NumCPU()
	if maxParallel > 4 {
		maxParallel = 4 // Limit to 4 parallel goroutines for stability
	}
	sem := make(chan struct{}, maxParallel)

	var wg sync.WaitGroup
	
	// Buffered error channel is no longer needed since we use errOnce and processingErr
	// But we'll create it and ignore it for code consistency and potential debugging
	_ = make(chan error, numChunks+1)
	
	// Create a safe way to terminate early if an error occurs
	// Use a parent context with timeout to prevent any possibility of leaked goroutines
	parentCtx, parentCancel := context.WithTimeout(context.Background(), 10*time.Minute)
	ctx, cancel := context.WithCancel(parentCtx)
	
	// Always clean up contexts when we're done
	defer func() {
		cancel()
		parentCancel()
		
		// Drain error channel to prevent goroutine leaks
		for len(errChan) > 0 {
			<-errChan
		}
	}()

	// Create a mutex to protect shared data
	var processMu sync.Mutex
	processedCount := 0

	// Ensure we track any errors that occur
	var processingErr error
	var errOnce sync.Once

	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		wg.Add(1)

		// Local variables to avoid race conditions in closure
		localChunkIdx := chunkIdx

		go func() {
			// Use defer for cleanup to ensure it happens even in case of panic
			defer wg.Done()
			
			// Acquire semaphore slot
			select {
			case sem <- struct{}{}:
				// Successfully acquired semaphore
				defer func() { <-sem }() // Always release semaphore
			case <-ctx.Done():
				// Context was cancelled, exit early
				return
			}

			// Check if we've been cancelled before doing any work
			select {
			case <-ctx.Done():
				// Another goroutine encountered an error, stop processing
				return
			default:
				// Continue processing
			}

			// Catch any panics to avoid crashing the whole program
			defer func() {
				if r := recover(); r != nil {
					// Record the first error only
					errOnce.Do(func() {
						processingErr = fmt.Errorf("panic in chunk %d: %v", localChunkIdx, r)
						// Signal other goroutines to stop
						cancel()
					})
				}
			}()

			// Calculate chunk boundaries
			startRow := localChunkIdx * chunkSize
			endRow := (localChunkIdx + 1) * chunkSize
			if endRow > rowCount {
				endRow = rowCount
			}

			// Skip this chunk if startRow is beyond the end of data
			if startRow >= rowCount {
				return
			}

			// Make sure we have a valid chunk size
			actualChunkSize := endRow - startRow
			if actualChunkSize <= 0 {
				return
			}

			// Extract data for this chunk - using thread-safe extraction
			colData := make(map[int]interface{})
			nullMasks := make(map[int][]bool)

			// Extract each column for this chunk with proper error handling
			for _, colIdx := range colIndices {
				// Check for cancellation between column extractions
				select {
				case <-ctx.Done():
					return
				default:
					// Continue processing
				}

				values, nulls, err := pe.extractChunk(colIdx, startRow, endRow)
				if err != nil {
					// Record the first error only
					errOnce.Do(func() {
						processingErr = fmt.Errorf("failed to extract chunk %d, column %d: %w",
							localChunkIdx, colIdx, err)
						cancel() // Signal other goroutines to stop
					})
					return
				}

				// Verify the extracted data is valid
				if values == nil || nulls == nil {
					errOnce.Do(func() {
						processingErr = fmt.Errorf("nil values or nulls for chunk %d, column %d",
							localChunkIdx, colIdx)
						cancel()
					})
					return
				}

				colData[colIdx] = values
				nullMasks[colIdx] = nulls
			}

			// Track the number of chunks successfully processed
			processMu.Lock()
			processedCount++
			processMu.Unlock()

			// Process the chunk
			if err := processor(localChunkIdx, colData, nullMasks); err != nil {
				errOnce.Do(func() {
					processingErr = fmt.Errorf("error processing chunk %d: %w", localChunkIdx, err)
					cancel() // Signal other goroutines to stop
				})
			}
		}()
	}

	// Wait for all chunks to be processed
	wg.Wait()

	// Return any error that occurred during processing
	if processingErr != nil {
		return processingErr
	}

	return nil
}
