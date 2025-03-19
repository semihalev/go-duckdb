package duckdb

/*
#include <stdlib.h>
#include <stdbool.h>
#include <duckdb.h>
*/
import "C"
import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
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
	errChan := make(chan error, len(colIndices))

	// Create a context with timeout to prevent hanging forever
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for i, colIdx := range colIndices {
		wg.Add(1)

		// Local variables to avoid race conditions in closure
		localI := i
		localColIdx := colIdx

		go func() {
			defer wg.Done()

			// Check if we should terminate early
			select {
			case <-ctx.Done():
				return
			default:
				// Continue processing
			}

			// Process the column data
			if err := processor(localColIdx, columns[localI], nullMasks[localI]); err != nil {
				errChan <- fmt.Errorf("error processing column %d: %w", localColIdx, err)
				cancel() // Signal other goroutines to stop on error
			}
		}()
	}

	// Wait for all processing to complete
	wg.Wait()

	// Check for errors - collect all errors instead of just the first one
	var errors []error

drain:
	for {
		select {
		case err := <-errChan:
			errors = append(errors, err)
		default:
			break drain
		}
	}

	// If we have errors, return a combined error message
	if len(errors) > 0 {
		return fmt.Errorf("%d errors occurred during parallel processing. First error: %v", len(errors), errors[0])
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
	errChan := make(chan error, len(colIndices))

	// Create a context with timeout to prevent hanging forever
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for i, colIdx := range colIndices {
		wg.Add(1)

		// Local variables to avoid race conditions in closure
		localI := i
		localColIdx := colIdx

		go func() {
			defer wg.Done()

			// Check if we should terminate early
			select {
			case <-ctx.Done():
				return
			default:
				// Continue processing
			}

			// Process the column data
			if err := processor(localColIdx, columns[localI], nullMasks[localI]); err != nil {
				errChan <- fmt.Errorf("error processing column %d: %w", localColIdx, err)
				cancel() // Signal other goroutines to stop on error
			}
		}()
	}

	// Wait for all processing to complete
	wg.Wait()

	// Check for errors - collect all errors instead of just the first one
	var errors []error

drain:
	for {
		select {
		case err := <-errChan:
			errors = append(errors, err)
		default:
			break drain
		}
	}

	// If we have errors, return a combined error message
	if len(errors) > 0 {
		return fmt.Errorf("%d errors occurred during parallel processing. First error: %v", len(errors), errors[0])
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
	errChan := make(chan error, len(colIndices))

	// Create a context with timeout to prevent hanging forever
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for i, colIdx := range colIndices {
		wg.Add(1)

		// Local variables to avoid race conditions in closure
		localI := i
		localColIdx := colIdx

		go func() {
			defer wg.Done()

			// Check if we should terminate early
			select {
			case <-ctx.Done():
				return
			default:
				// Continue processing
			}

			// Process the column data
			if err := processor(localColIdx, columns[localI], nullMasks[localI]); err != nil {
				errChan <- fmt.Errorf("error processing column %d: %w", localColIdx, err)
				cancel() // Signal other goroutines to stop on error
			}
		}()
	}

	// Wait for all processing to complete
	wg.Wait()

	// Check for errors - collect all errors instead of just the first one
	var errors []error

drain:
	for {
		select {
		case err := <-errChan:
			errors = append(errors, err)
		default:
			break drain
		}
	}

	// If we have errors, return a combined error message
	if len(errors) > 0 {
		return fmt.Errorf("%d errors occurred during parallel processing. First error: %v", len(errors), errors[0])
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
	errChan := make(chan error, len(colIndices))

	// Create a context with timeout to prevent hanging forever
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for i, colIdx := range colIndices {
		wg.Add(1)

		// Local variables to avoid race conditions in closure
		localI := i
		localColIdx := colIdx

		go func() {
			defer wg.Done()

			// Check if we should terminate early
			select {
			case <-ctx.Done():
				return
			default:
				// Continue processing
			}

			// Process the column data
			if err := processFn(localColIdx, values[localI], nullMasks[localI]); err != nil {
				errChan <- fmt.Errorf("error processing column %d: %w", localColIdx, err)
				cancel() // Signal other goroutines to stop on error
			}
		}()
	}

	// Wait for all processing to complete
	wg.Wait()

	// Check for errors - collect all errors instead of just the first one
	var errors []error

drain:
	for {
		select {
		case err := <-errChan:
			errors = append(errors, err)
		default:
			break drain
		}
	}

	// If we have errors, return a combined error message
	if len(errors) > 0 {
		return fmt.Errorf("%d errors occurred during parallel processing. First error: %v", len(errors), errors[0])
	}

	return nil
}

// extractChunk extracts a chunk of data for a single column in a thread-safe manner
func (pe *ParallelExtractor) extractChunk(colIdx int, startRow, endRow int) (interface{}, []bool, error) {
	dr := pe.dr

	// Acquire a read lock before accessing any DirectResult data
	dr.mu.RLock()

	// Check if result is closed
	if dr.closed {
		dr.mu.RUnlock()
		return nil, nil, ErrResultClosed
	}

	// Get the column type while holding the lock
	colType := dr.ColumnTypes()[colIdx]

	// Get total rows while still holding the lock
	totalRows := int(dr.RowCount())

	// We got all the info we need for validation, can release the lock
	dr.mu.RUnlock()

	// Safety check to ensure we don't go past the end of the result set
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
// Uses block-based approach to minimize CGO overhead
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
	
	// Define block size for processing to reduce CGO boundary crossings
	const blockSize = 64
	
	// Convert to C index
	cColIdx := C.idx_t(colIdx)
	
	// Process the data in blocks to minimize CGO boundary crossings
	for blockStart := 0; blockStart < rowCount; blockStart += blockSize {
		// Calculate actual block size (might be smaller at the end)
		blockEnd := blockStart + blockSize
		if blockEnd > rowCount {
			blockEnd = rowCount
		}
		actualBlockSize := blockEnd - blockStart
		
		// Extract null values for this block
		for i := 0; i < actualBlockSize; i++ {
			rowIdx := C.idx_t(startRow + blockStart + i)
			isNull := C.duckdb_value_is_null(dr.result, cColIdx, rowIdx)
			nulls[blockStart+i] = cBoolToGo(isNull)
		}
		
		// Extract non-null values
		for i := 0; i < actualBlockSize; i++ {
			if !nulls[blockStart+i] {
				rowIdx := C.idx_t(startRow + blockStart + i)
				val := C.duckdb_value_int32(dr.result, cColIdx, rowIdx)
				values[blockStart+i] = int32(val)
			}
		}
	}

	return values, nulls, nil
}

// extractInt64ColumnThreadSafe extracts an int64 column directly for a specific range in a thread-safe manner
// Uses block-based approach to minimize CGO overhead
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
	
	// Define block size for processing to reduce CGO boundary crossings
	const blockSize = 64
	
	// Convert to C index
	cColIdx := C.idx_t(colIdx)
	
	// Process the data in blocks to minimize CGO boundary crossings
	for blockStart := 0; blockStart < rowCount; blockStart += blockSize {
		// Calculate actual block size (might be smaller at the end)
		blockEnd := blockStart + blockSize
		if blockEnd > rowCount {
			blockEnd = rowCount
		}
		actualBlockSize := blockEnd - blockStart
		
		// Extract null values for this block
		for i := 0; i < actualBlockSize; i++ {
			rowIdx := C.idx_t(startRow + blockStart + i)
			isNull := C.duckdb_value_is_null(dr.result, cColIdx, rowIdx)
			nulls[blockStart+i] = cBoolToGo(isNull)
		}
		
		// Extract non-null values
		for i := 0; i < actualBlockSize; i++ {
			if !nulls[blockStart+i] {
				rowIdx := C.idx_t(startRow + blockStart + i)
				val := C.duckdb_value_int64(dr.result, cColIdx, rowIdx)
				values[blockStart+i] = int64(val)
			}
		}
	}

	return values, nulls, nil
}

// extractFloat64ColumnThreadSafe extracts a float64 column directly for a specific range in a thread-safe manner
// Uses block-based approach to minimize CGO overhead
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
	
	// Define block size for processing to reduce CGO boundary crossings
	const blockSize = 64
	
	// Convert to C index
	cColIdx := C.idx_t(colIdx)
	
	// Process the data in blocks to minimize CGO boundary crossings
	for blockStart := 0; blockStart < rowCount; blockStart += blockSize {
		// Calculate actual block size (might be smaller at the end)
		blockEnd := blockStart + blockSize
		if blockEnd > rowCount {
			blockEnd = rowCount
		}
		actualBlockSize := blockEnd - blockStart
		
		// Extract null values for this block
		for i := 0; i < actualBlockSize; i++ {
			rowIdx := C.idx_t(startRow + blockStart + i)
			isNull := C.duckdb_value_is_null(dr.result, cColIdx, rowIdx)
			nulls[blockStart+i] = cBoolToGo(isNull)
		}
		
		// Extract non-null values
		for i := 0; i < actualBlockSize; i++ {
			if !nulls[blockStart+i] {
				rowIdx := C.idx_t(startRow + blockStart + i)
				val := C.duckdb_value_double(dr.result, cColIdx, rowIdx)
				values[blockStart+i] = float64(val)
			}
		}
	}

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
	errChan := make(chan error, numChunks)

	// Create a safe way to terminate early if an error occurs
	// Add a timeout of 5 minutes to prevent hanging forever
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Create a mutex to protect shared data
	var processMu sync.Mutex
	// This tracks our progress - useful for debugging but not currently used in return value
	processedCount := 0

	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		wg.Add(1)

		// Local variables to avoid race conditions in closure
		localChunkIdx := chunkIdx

		go func() {
			// Acquire semaphore slot
			sem <- struct{}{}
			defer func() {
				// Release semaphore slot, even if we panic
				<-sem
				wg.Done()
			}()

			// Check if we've been cancelled
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
					errChan <- fmt.Errorf("panic in chunk %d: %v", localChunkIdx, r)
					cancel() // Signal other goroutines to stop
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
					errChan <- fmt.Errorf("failed to extract chunk %d, column %d: %w",
						localChunkIdx, colIdx, err)
					cancel() // Signal other goroutines to stop
					return
				}

				// Verify the extracted data is valid
				if values == nil || nulls == nil {
					errChan <- fmt.Errorf("nil values or nulls for chunk %d, column %d",
						localChunkIdx, colIdx)
					cancel()
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
				errChan <- fmt.Errorf("error processing chunk %d: %w", localChunkIdx, err)
				cancel() // Signal other goroutines to stop
			}
		}()
	}

	// Wait for all chunks to be processed
	wg.Wait()

	// Check for errors - collect all errors instead of just the first one
	var errors []error

drain:
	for {
		select {
		case err := <-errChan:
			errors = append(errors, err)
		default:
			break drain
		}
	}

	// If we have errors, wrap them all in a combined error message
	if len(errors) > 0 {
		return fmt.Errorf("%d errors occurred during parallel processing. First error: %v", len(errors), errors[0])
	}

	return nil
}
