package duckdb

/*
#include <stdlib.h>
#include <string.h>
#include <duckdb.h>
*/
import "C"

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

// BatchSize determines how many rows to process in a single batch
// This is a key parameter that can be tuned for performance
const DefaultBatchSize = 1000

// Default block size for vectorized processing
// This affects memory usage vs. performance tradeoff
const DefaultBlockSize = 64

// Error types
var (
	ErrNoRowsAvailable   = errors.New("no rows available in current batch")
	ErrInvalidRowIndex   = errors.New("invalid row index")
)

// BatchQuery represents a batch-oriented query result
// It processes data in column-wise batches for much higher performance
type BatchQuery struct {
	result      *C.duckdb_result
	columnCount int
	rowCount    int64
	columnNames []string
	columnTypes []C.duckdb_type
	currentRow  int64
	batchSize   int
	closed      bool
	resultOwned bool // Whether this query owns the result and should free it

	// Mutex to protect concurrent access to shared state
	mu sync.RWMutex

	// Reusable vectors to avoid allocations
	vectors []*ColumnVector

	// Current batch state
	currentBatch   int64 // Current batch number
	batchRowsRead  int   // Rows read in the current batch
	batchAvailable int   // Total rows available in the current batch

	// Temporary buffer for conversions
	buffer []byte
	
	// Optimization flag to use unified extraction when possible
	useUnifiedExtraction bool
}

// FetchNextBatchOptimized fetches the next batch of rows using the optimized extraction method
// This reduces CGO overhead by extracting all columns in a unified way
func (bq *BatchQuery) FetchNextBatchOptimized() error {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	
	if bq.closed {
		return ErrResultClosed
	}
	
	// Calculate how many rows to fetch
	batchStart := bq.currentRow
	rowsLeft := bq.rowCount - batchStart
	if rowsLeft <= 0 {
		// No more rows to fetch
		bq.batchAvailable = 0
		return io.EOF
	}
	
	// Limit batch size to available rows
	batchSize := int64(bq.batchSize)
	if rowsLeft < batchSize {
		batchSize = rowsLeft
	}
	
	// Reset previous batch vectors if they exist
	for i := 0; i < bq.columnCount; i++ {
		if bq.vectors[i] != nil {
			PutPooledColumnVector(bq.vectors[i])
			bq.vectors[i] = nil
		}
	}
	
	// Extract all columns with optimized method
	for i := 0; i < bq.columnCount; i++ {
		vector, err := ExtractColumnBatchTyped(bq.result, i, int(batchStart), int(batchSize))
		if err != nil {
			return fmt.Errorf("failed to extract column %d: %w", i, err)
		}
		bq.vectors[i] = vector
	}
	
	// Update batch state
	bq.currentBatch++
	bq.currentRow += batchSize
	bq.batchAvailable = int(batchSize)
	bq.batchRowsRead = 0
	
	return nil
}

// GetColumnVector returns a column vector for the specified column
// The vector provides type-specific access to column data
func (bq *BatchQuery) GetColumnVector(colIdx int) (*ColumnVector, error) {
	bq.mu.RLock()
	defer bq.mu.RUnlock()
	
	if bq.closed {
		return nil, ErrResultClosed
	}
	
	if colIdx < 0 || colIdx >= bq.columnCount {
		return nil, ErrInvalidColumnIndex
	}
	
	if bq.batchAvailable <= 0 {
		return nil, ErrNoRowsAvailable
	}
	
	// Return the pre-extracted vector
	return bq.vectors[colIdx], nil
}

// ExtractAllColumns is a convenience method to get all column vectors at once
// This is useful when you want to process entire rows or do columnar operations
func (bq *BatchQuery) ExtractAllColumns() ([]*ColumnVector, error) {
	bq.mu.RLock()
	defer bq.mu.RUnlock()
	
	if bq.closed {
		return nil, ErrResultClosed
	}
	
	if bq.batchAvailable <= 0 {
		return nil, ErrNoRowsAvailable
	}
	
	// Make a copy of the vectors to return
	vectors := make([]*ColumnVector, bq.columnCount)
	for i := 0; i < bq.columnCount; i++ {
		vectors[i] = bq.vectors[i]
	}
	
	return vectors, nil
}

// ColumnVector represents a type-specific vector of column values
// Using column vectors allows much more efficient processing than row-wise access
type ColumnVector struct {
	// Common properties
	columnType C.duckdb_type
	nullMap    []bool // Which values are NULL
	length     int    // Number of values in the vector
	capacity   int    // Capacity of the vector

	// Type-specific storage - we only populate the relevant field
	// This design avoids unnecessary allocations for unused types
	boolData      []bool
	int8Data      []int8
	int16Data     []int16
	int32Data     []int32
	int64Data     []int64
	uint8Data     []uint8
	uint16Data    []uint16
	uint32Data    []uint32
	uint64Data    []uint64
	float32Data   []float32
	float64Data   []float64
	stringData    []string
	blobData      [][]byte
	timestampData []int64       // For timestamp types (microseconds since epoch)
	timeData      []interface{} // For other time-related types
}

// FetchNextBatch fetches the next batch of rows
// This method uses the optimized extraction when enabled
func (bq *BatchQuery) FetchNextBatch() error {
	if bq.useUnifiedExtraction {
		return bq.FetchNextBatchOptimized()
	}
	
	// Fall back to the original implementation
	// This is kept for backward compatibility
	// The implementation would go here
	return fmt.Errorf("original batch fetching not implemented, use FetchNextBatchOptimized instead")
}

// GetValue returns a typed value from the current batch
// This provides a high-level interface to access values by column and row index
func (bq *BatchQuery) GetValue(colIdx, rowIdx int) (interface{}, bool, error) {
	if colIdx < 0 || colIdx >= bq.columnCount {
		return nil, false, ErrInvalidColumnIndex
	}
	
	if rowIdx < 0 || rowIdx >= bq.batchAvailable {
		return nil, false, ErrInvalidRowIndex
	}
	
	vector := bq.vectors[colIdx]
	if vector == nil {
		return nil, false, fmt.Errorf("column vector not available")
	}
	
	// Check if value is NULL
	if rowIdx >= len(vector.nullMap) || vector.nullMap[rowIdx] {
		return nil, true, nil
	}
	
	// Return the value based on type
	switch vector.columnType {
	case C.DUCKDB_TYPE_BOOLEAN:
		if rowIdx < len(vector.boolData) {
			return vector.boolData[rowIdx], false, nil
		}
	case C.DUCKDB_TYPE_TINYINT:
		if rowIdx < len(vector.int8Data) {
			return vector.int8Data[rowIdx], false, nil
		}
	case C.DUCKDB_TYPE_SMALLINT:
		if rowIdx < len(vector.int16Data) {
			return vector.int16Data[rowIdx], false, nil
		}
	case C.DUCKDB_TYPE_INTEGER:
		if rowIdx < len(vector.int32Data) {
			return vector.int32Data[rowIdx], false, nil
		}
	case C.DUCKDB_TYPE_BIGINT:
		if rowIdx < len(vector.int64Data) {
			return vector.int64Data[rowIdx], false, nil
		}
	case C.DUCKDB_TYPE_FLOAT:
		if rowIdx < len(vector.float32Data) {
			return vector.float32Data[rowIdx], false, nil
		}
	case C.DUCKDB_TYPE_DOUBLE:
		if rowIdx < len(vector.float64Data) {
			return vector.float64Data[rowIdx], false, nil
		}
	case C.DUCKDB_TYPE_VARCHAR:
		if rowIdx < len(vector.stringData) {
			return vector.stringData[rowIdx], false, nil
		}
	case C.DUCKDB_TYPE_BLOB:
		if rowIdx < len(vector.blobData) {
			return vector.blobData[rowIdx], false, nil
		}
	case C.DUCKDB_TYPE_TIMESTAMP:
		if rowIdx < len(vector.timestampData) {
			// Convert timestamp to Go time.Time
			micros := vector.timestampData[rowIdx]
			return time.Unix(micros/1000000, (micros%1000000)*1000), false, nil
		}
	}
	
	return nil, false, fmt.Errorf("unsupported column type: %d", vector.columnType)
}

// NewBatchQuery creates a new batch-oriented query from a DuckDB result
func NewBatchQuery(result *C.duckdb_result, batchSize int) *BatchQuery {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	// Get column and row counts
	columnCount := int(C.duckdb_column_count(result))
	rowCount := int64(C.duckdb_row_count(result))

	// Initialize batch query
	bq := &BatchQuery{
		result:              result,
		columnCount:         columnCount,
		rowCount:            rowCount,
		columnNames:         make([]string, columnCount),
		columnTypes:         make([]C.duckdb_type, columnCount),
		batchSize:           batchSize,
		vectors:             make([]*ColumnVector, columnCount),
		buffer:              make([]byte, 4096), // Reusable buffer for string conversions
		useUnifiedExtraction: true,              // Use optimized extraction by default
		resultOwned: true,               // By default, we own the result
	}

	// Get column names and types
	for i := 0; i < columnCount; i++ {
		colIdx := C.idx_t(i)
		bq.columnNames[i] = C.GoString(C.duckdb_column_name(result, colIdx))
		bq.columnTypes[i] = C.duckdb_column_type(result, colIdx)

		// Initialize column vector based on type
		bq.vectors[i] = bq.createColumnVector(bq.columnTypes[i], batchSize)
	}

	// Prepare first batch
	bq.fetchNextBatch()

	return bq
}

// createColumnVector creates a type-specific column vector with the given capacity
// Uses the global column vector pool to minimize allocations
func (bq *BatchQuery) createColumnVector(colType C.duckdb_type, capacity int) *ColumnVector {
	// Get the vector from the pool instead of allocating a new one
	// This significantly reduces memory allocations and GC pressure
	return GetPooledColumnVector(colType, capacity)
}

// Columns returns the names of the columns in the result set
func (bq *BatchQuery) Columns() []string {
	return bq.columnNames
}

// ColumnTypeScanType returns the Go type that can be used to scan values from this column
func (bq *BatchQuery) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= bq.columnCount {
		return nil
	}

	switch bq.columnTypes[index] {
	case C.DUCKDB_TYPE_BOOLEAN:
		return reflect.TypeOf(bool(false))
	case C.DUCKDB_TYPE_TINYINT:
		return reflect.TypeOf(int8(0))
	case C.DUCKDB_TYPE_SMALLINT:
		return reflect.TypeOf(int16(0))
	case C.DUCKDB_TYPE_INTEGER:
		return reflect.TypeOf(int32(0))
	case C.DUCKDB_TYPE_BIGINT:
		return reflect.TypeOf(int64(0))
	case C.DUCKDB_TYPE_UTINYINT:
		return reflect.TypeOf(uint8(0))
	case C.DUCKDB_TYPE_USMALLINT:
		return reflect.TypeOf(uint16(0))
	case C.DUCKDB_TYPE_UINTEGER:
		return reflect.TypeOf(uint32(0))
	case C.DUCKDB_TYPE_UBIGINT:
		return reflect.TypeOf(uint64(0))
	case C.DUCKDB_TYPE_FLOAT:
		return reflect.TypeOf(float32(0))
	case C.DUCKDB_TYPE_DOUBLE:
		return reflect.TypeOf(float64(0))
	case C.DUCKDB_TYPE_VARCHAR:
		return reflect.TypeOf(string(""))
	case C.DUCKDB_TYPE_BLOB:
		return reflect.TypeOf([]byte{})
	default:
		// For other types, return interface{}
		return reflect.TypeOf((*interface{})(nil)).Elem()
	}
}

// Close closes the query and releases associated resources
func (bq *BatchQuery) Close() error {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	if bq.closed {
		return nil
	}

	// Free DuckDB result if we own it
	if bq.resultOwned && bq.result != nil {
		C.duckdb_destroy_result(bq.result)
		bq.result = nil
	}

	// Return column vectors to the pool for reuse
	if bq.vectors != nil {
		for _, vector := range bq.vectors {
			if vector != nil {
				// Put back in the pool rather than just dropping the reference
				PutPooledColumnVector(vector)
			}
		}
	}

	// Help GC by clearing references
	bq.vectors = nil
	bq.buffer = nil
	bq.closed = true

	return nil
}

// fetchNextBatch fetches the next batch of rows from the result
// This is where the core batch processing happens
func (bq *BatchQuery) fetchNextBatch() bool {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	// Check if the query was closed
	if bq.closed {
		return false
	}

	// Check if we're at the end of results
	if bq.currentRow >= bq.rowCount {
		bq.batchAvailable = 0
		return false
	}

	// Calculate how many rows to fetch in this batch
	batchStart := bq.currentRow
	rowsRemaining := bq.rowCount - batchStart
	batchSize := bq.batchSize
	if rowsRemaining < int64(batchSize) {
		batchSize = int(rowsRemaining)
	}

	// Reset batch state
	bq.batchRowsRead = 0
	bq.batchAvailable = batchSize

	// Extract data in column-wise fashion (much more efficient than row-wise)
	for colIdx := 0; colIdx < bq.columnCount; colIdx++ {
		if colIdx >= len(bq.vectors) || bq.vectors[colIdx] == nil {
			continue // Skip if vector is nil (should never happen, but protect against panic)
		}

		vector := bq.vectors[colIdx]
		vector.length = batchSize

		// Use the new Vector implementation for data extraction
		// This avoids redundant CGO boundary crossings for each individual value
		bq.extractColumnBatchWithNativeVector(colIdx, int(batchStart), batchSize, vector)
	}

	// Update current batch
	bq.currentBatch++
	bq.currentRow += int64(batchSize)

	return true
}

// extractColumnBatchWithNativeVector extracts a batch of values using Vector
// This implementation uses the new Vector type to reduce CGO overhead
func (bq *BatchQuery) extractColumnBatchWithNativeVector(colIdx int, startRow int, batchSize int, cv *ColumnVector) error {
	// Create a native vector directly from the result
	// This significantly reduces CGO overhead by avoiding per-value crossings
	nativeVector := NewVectorFromResult(bq.result, colIdx, startRow, batchSize)
	if nativeVector == nil {
		// Fall back to the older implementation if there's an issue
		bq.extractColumnBatch(colIdx, startRow, batchSize, cv)
		return nil
	}

	// Copy data from native vector's pooled column vector to our column vector
	// Both are using the same type of storage, so we can copy efficiently

	// First, copy the null map
	if nativeVector.pooledVector != nil && len(nativeVector.pooledVector.nullMap) == len(cv.nullMap) {
		copy(cv.nullMap, nativeVector.pooledVector.nullMap)
	}

	// Now, copy the type-specific data based on column type
	switch bq.columnTypes[colIdx] {
	case C.DUCKDB_TYPE_BOOLEAN:
		if len(nativeVector.pooledVector.boolData) > 0 && len(cv.boolData) > 0 {
			copy(cv.boolData, nativeVector.pooledVector.boolData)
		}

	case C.DUCKDB_TYPE_TINYINT:
		if len(nativeVector.pooledVector.int8Data) > 0 && len(cv.int8Data) > 0 {
			copy(cv.int8Data, nativeVector.pooledVector.int8Data)
		}

	case C.DUCKDB_TYPE_SMALLINT:
		if len(nativeVector.pooledVector.int16Data) > 0 && len(cv.int16Data) > 0 {
			copy(cv.int16Data, nativeVector.pooledVector.int16Data)
		}

	case C.DUCKDB_TYPE_INTEGER:
		if len(nativeVector.pooledVector.int32Data) > 0 && len(cv.int32Data) > 0 {
			copy(cv.int32Data, nativeVector.pooledVector.int32Data)
		}

	case C.DUCKDB_TYPE_BIGINT:
		if len(nativeVector.pooledVector.int64Data) > 0 && len(cv.int64Data) > 0 {
			copy(cv.int64Data, nativeVector.pooledVector.int64Data)
		}

	case C.DUCKDB_TYPE_UTINYINT:
		if len(nativeVector.pooledVector.uint8Data) > 0 && len(cv.uint8Data) > 0 {
			copy(cv.uint8Data, nativeVector.pooledVector.uint8Data)
		}

	case C.DUCKDB_TYPE_USMALLINT:
		if len(nativeVector.pooledVector.uint16Data) > 0 && len(cv.uint16Data) > 0 {
			copy(cv.uint16Data, nativeVector.pooledVector.uint16Data)
		}

	case C.DUCKDB_TYPE_UINTEGER:
		if len(nativeVector.pooledVector.uint32Data) > 0 && len(cv.uint32Data) > 0 {
			copy(cv.uint32Data, nativeVector.pooledVector.uint32Data)
		}

	case C.DUCKDB_TYPE_UBIGINT:
		if len(nativeVector.pooledVector.uint64Data) > 0 && len(cv.uint64Data) > 0 {
			copy(cv.uint64Data, nativeVector.pooledVector.uint64Data)
		}

	case C.DUCKDB_TYPE_FLOAT:
		if len(nativeVector.pooledVector.float32Data) > 0 && len(cv.float32Data) > 0 {
			copy(cv.float32Data, nativeVector.pooledVector.float32Data)
		}

	case C.DUCKDB_TYPE_DOUBLE:
		if len(nativeVector.pooledVector.float64Data) > 0 && len(cv.float64Data) > 0 {
			copy(cv.float64Data, nativeVector.pooledVector.float64Data)
		}

	case C.DUCKDB_TYPE_VARCHAR:
		if len(nativeVector.pooledVector.stringData) > 0 && len(cv.stringData) > 0 {
			copy(cv.stringData, nativeVector.pooledVector.stringData)
		}

	case C.DUCKDB_TYPE_BLOB:
		if len(nativeVector.pooledVector.blobData) > 0 && len(cv.blobData) > 0 {
			copy(cv.blobData, nativeVector.pooledVector.blobData)
		}

	default:
		// For complex types or any other types, fall back to the timeData array
		if len(nativeVector.pooledVector.timeData) > 0 && len(cv.timeData) > 0 {
			copy(cv.timeData, nativeVector.pooledVector.timeData)
		}
	}

	// Clean up the native vector - this will return the pooled vector to its pool
	PutVector(nativeVector)

	return nil
}

// extractColumnBatch extracts a batch of values for a specific column
// This is optimized to minimize CGO boundary crossings using block-based extraction
func (bq *BatchQuery) extractColumnBatch(colIdx int, startRow int, batchSize int, vector *ColumnVector) {
	// Get DuckDB C types ready
	cColIdx := C.idx_t(colIdx)
	cStartRow := C.idx_t(startRow)
	colType := bq.columnTypes[colIdx]

	// Define block size for processing to reduce CGO boundary crossings
	// This value should be tuned based on actual workload characteristics
	const blockSize = 64

	// Process the data in blocks to minimize CGO boundary crossings
	for blockStart := 0; blockStart < batchSize; blockStart += blockSize {
		// Calculate actual block size (might be smaller at the end)
		blockEnd := blockStart + blockSize
		if blockEnd > batchSize {
			blockEnd = batchSize
		}
		actualBlockSize := blockEnd - blockStart

		// Extract null values for this block
		for i := 0; i < actualBlockSize; i++ {
			rowIdx := cStartRow + C.idx_t(blockStart+i)
			isNull := C.duckdb_value_is_null(bq.result, cColIdx, rowIdx)
			vector.nullMap[blockStart+i] = cBoolToGo(isNull)
		}

		// Extract non-null values based on column type
		switch colType {
		case C.DUCKDB_TYPE_BOOLEAN:
			// Extract boolean values for this block
			for i := 0; i < actualBlockSize; i++ {
				if !vector.nullMap[blockStart+i] {
					rowIdx := cStartRow + C.idx_t(blockStart+i)
					val := C.duckdb_value_boolean(bq.result, cColIdx, rowIdx)
					vector.boolData[blockStart+i] = cBoolToGo(val)
				}
			}

		case C.DUCKDB_TYPE_TINYINT:
			// Extract int8 values for this block
			for i := 0; i < actualBlockSize; i++ {
				if !vector.nullMap[blockStart+i] {
					rowIdx := cStartRow + C.idx_t(blockStart+i)
					val := C.duckdb_value_int8(bq.result, cColIdx, rowIdx)
					vector.int8Data[blockStart+i] = int8(val)
				}
			}

		case C.DUCKDB_TYPE_SMALLINT:
			// Extract int16 values for this block
			for i := 0; i < actualBlockSize; i++ {
				if !vector.nullMap[blockStart+i] {
					rowIdx := cStartRow + C.idx_t(blockStart+i)
					val := C.duckdb_value_int16(bq.result, cColIdx, rowIdx)
					vector.int16Data[blockStart+i] = int16(val)
				}
			}

		case C.DUCKDB_TYPE_INTEGER:
			// Extract int32 values for this block
			if len(vector.int32Data) > 0 {
				// Count non-null values for optimized processing
				nonNullCount := 0
				for i := 0; i < actualBlockSize; i++ {
					if !vector.nullMap[blockStart+i] {
						nonNullCount++
					}
				}

				// Only extract if we have non-null values
				if nonNullCount > 0 {
					// Extract non-null values efficiently
					for i := 0; i < actualBlockSize; i++ {
						if !vector.nullMap[blockStart+i] {
							rowIdx := cStartRow + C.idx_t(blockStart+i)
							val := C.duckdb_value_int32(bq.result, cColIdx, rowIdx)
							vector.int32Data[blockStart+i] = int32(val)
						}
					}
				}
			}

		case C.DUCKDB_TYPE_BIGINT:
			// Extract int64 values for this block
			if len(vector.int64Data) > 0 {
				// Count non-null values for optimized processing
				nonNullCount := 0
				for i := 0; i < actualBlockSize; i++ {
					if !vector.nullMap[blockStart+i] {
						nonNullCount++
					}
				}

				// Only extract if we have non-null values
				if nonNullCount > 0 {
					// Extract non-null values efficiently
					for i := 0; i < actualBlockSize; i++ {
						if !vector.nullMap[blockStart+i] {
							rowIdx := cStartRow + C.idx_t(blockStart+i)
							val := C.duckdb_value_int64(bq.result, cColIdx, rowIdx)
							vector.int64Data[blockStart+i] = int64(val)
						}
					}
				}
			}

		case C.DUCKDB_TYPE_FLOAT:
			// Extract float32 values for this block
			for i := 0; i < actualBlockSize; i++ {
				if !vector.nullMap[blockStart+i] {
					rowIdx := cStartRow + C.idx_t(blockStart+i)
					val := C.duckdb_value_float(bq.result, cColIdx, rowIdx)
					vector.float32Data[blockStart+i] = float32(val)
				}
			}

		case C.DUCKDB_TYPE_DOUBLE:
			// Extract float64 values for this block
			for i := 0; i < actualBlockSize; i++ {
				if !vector.nullMap[blockStart+i] {
					rowIdx := cStartRow + C.idx_t(blockStart+i)
					val := C.duckdb_value_double(bq.result, cColIdx, rowIdx)
					vector.float64Data[blockStart+i] = float64(val)
				}
			}

		case C.DUCKDB_TYPE_VARCHAR:
			// Extract string values for this block
			for i := 0; i < actualBlockSize; i++ {
				if !vector.nullMap[blockStart+i] {
					rowIdx := cStartRow + C.idx_t(blockStart+i)
					cstr := C.duckdb_value_varchar(bq.result, cColIdx, rowIdx)
					if cstr != nil {
						vector.stringData[blockStart+i] = C.GoString(cstr)
						C.duckdb_free(unsafe.Pointer(cstr))
					} else {
						vector.stringData[blockStart+i] = ""
					}
				}
			}

		case C.DUCKDB_TYPE_BLOB:
			// Extract blob values for this block
			for i := 0; i < actualBlockSize; i++ {
				if !vector.nullMap[blockStart+i] {
					rowIdx := cStartRow + C.idx_t(blockStart+i)
					blob := C.duckdb_value_blob(bq.result, cColIdx, rowIdx)

					// Handle blob data safely to prevent memory leaks
					if blob.data != nil {
						// Store the pointer locally to avoid race conditions
						blobData := blob.data

						if blob.size > 0 {
							size := int(blob.size)
							// Allocate a new buffer for this blob
							buffer := make([]byte, size)
							// Copy blob data safely
							C.memcpy(unsafe.Pointer(&buffer[0]), unsafe.Pointer(blobData), C.size_t(size))
							vector.blobData[blockStart+i] = buffer
						} else {
							vector.blobData[blockStart+i] = []byte{}
						}

						// Free the C memory after safely copying it
						C.duckdb_free(blobData)
					} else {
						vector.blobData[blockStart+i] = []byte{}
					}
				}
			}
		default:
			// For other types, convert to string for now
			for i := 0; i < actualBlockSize; i++ {
				if !vector.nullMap[blockStart+i] {
					rowIdx := cStartRow + C.idx_t(blockStart+i)
					cstr := C.duckdb_value_varchar(bq.result, cColIdx, rowIdx)
					if cstr != nil {
						vector.timeData[blockStart+i] = C.GoString(cstr)
						C.duckdb_free(unsafe.Pointer(cstr))
					} else {
						vector.timeData[blockStart+i] = ""
					}
				}
			}
		}
	}
}

// BatchRows implements database/sql's Rows interface but with batch processing
type BatchRows struct {
	query       *BatchQuery
	columnCount int

	// Current position
	rowInBatch int

	// Driver value buffer for reuse
	valueBuffer []driver.Value
}

// NewBatchRows creates a new BatchRows from a BatchQuery
func NewBatchRows(query *BatchQuery) *BatchRows {
	return &BatchRows{
		query:       query,
		columnCount: query.columnCount,
		valueBuffer: make([]driver.Value, query.columnCount),
	}
}

// Columns returns the names of the columns
func (br *BatchRows) Columns() []string {
	if br.query == nil {
		return nil
	}
	return br.query.Columns()
}

// Close closes the rows iterator
func (br *BatchRows) Close() error {
	// Free our resources but don't close the underlying query
	// The query has its own lifecycle and might be shared
	br.query = nil
	br.valueBuffer = nil
	return nil
}

// ColumnTypeScanType returns the Go type that can be used to scan values from this column
func (br *BatchRows) ColumnTypeScanType(index int) reflect.Type {
	if br.query == nil {
		return nil
	}
	return br.query.ColumnTypeScanType(index)
}

// Next moves to the next row
// This is where the key batch optimization happens - we only fetch new batches
// when we've exhausted the current one
func (br *BatchRows) Next(dest []driver.Value) error {
	// Get a safe reference to the query to avoid nil pointer issues if Close is called concurrently
	query := br.query
	if query == nil {
		return io.EOF
	}

	// Using query reference to ensure thread safety
	// This doesn't prevent the query from being closed, but prevents nil dereference

	// Check if we need a new batch
	if br.rowInBatch >= query.batchAvailable {
		// Try to fetch the next batch
		if !query.fetchNextBatch() {
			return io.EOF
		}
		br.rowInBatch = 0
	}

	// Get a read lock for accessing query data
	query.mu.RLock()
	defer query.mu.RUnlock()

	// Safety check - query may have been closed while we were waiting for the lock
	if query.closed {
		return io.EOF
	}

	// Get row data from current batch
	for i := 0; i < br.columnCount && i < len(dest); i++ {
		// Safety check for vectors
		if i >= len(query.vectors) || query.vectors[i] == nil {
			dest[i] = nil
			continue
		}
		vector := query.vectors[i]

		// Safety check for row index
		if br.rowInBatch >= len(vector.nullMap) {
			dest[i] = nil
			continue
		}

		// Check for NULL
		if vector.nullMap[br.rowInBatch] {
			dest[i] = nil
			continue
		}

		// Extract value based on column type
		if i < len(query.columnTypes) {
			switch query.columnTypes[i] {
			case C.DUCKDB_TYPE_BOOLEAN:
				if br.rowInBatch < len(vector.boolData) {
					dest[i] = vector.boolData[br.rowInBatch]
				}
			case C.DUCKDB_TYPE_TINYINT:
				if br.rowInBatch < len(vector.int8Data) {
					dest[i] = vector.int8Data[br.rowInBatch]
				}
			case C.DUCKDB_TYPE_SMALLINT:
				if br.rowInBatch < len(vector.int16Data) {
					dest[i] = vector.int16Data[br.rowInBatch]
				}
			case C.DUCKDB_TYPE_INTEGER:
				if br.rowInBatch < len(vector.int32Data) {
					dest[i] = vector.int32Data[br.rowInBatch]
				}
			case C.DUCKDB_TYPE_BIGINT:
				if br.rowInBatch < len(vector.int64Data) {
					dest[i] = vector.int64Data[br.rowInBatch]
				}
			case C.DUCKDB_TYPE_UTINYINT:
				if br.rowInBatch < len(vector.uint8Data) {
					dest[i] = vector.uint8Data[br.rowInBatch]
				}
			case C.DUCKDB_TYPE_USMALLINT:
				if br.rowInBatch < len(vector.uint16Data) {
					dest[i] = vector.uint16Data[br.rowInBatch]
				}
			case C.DUCKDB_TYPE_UINTEGER:
				if br.rowInBatch < len(vector.uint32Data) {
					dest[i] = vector.uint32Data[br.rowInBatch]
				}
			case C.DUCKDB_TYPE_UBIGINT:
				if br.rowInBatch < len(vector.uint64Data) {
					dest[i] = vector.uint64Data[br.rowInBatch]
				}
			case C.DUCKDB_TYPE_FLOAT:
				if br.rowInBatch < len(vector.float32Data) {
					dest[i] = vector.float32Data[br.rowInBatch]
				}
			case C.DUCKDB_TYPE_DOUBLE:
				if br.rowInBatch < len(vector.float64Data) {
					dest[i] = vector.float64Data[br.rowInBatch]
				}
			case C.DUCKDB_TYPE_VARCHAR:
				if br.rowInBatch < len(vector.stringData) {
					dest[i] = vector.stringData[br.rowInBatch]
				}
			case C.DUCKDB_TYPE_BLOB:
				if br.rowInBatch < len(vector.blobData) {
					dest[i] = vector.blobData[br.rowInBatch]
				}
			default:
				if br.rowInBatch < len(vector.timeData) {
					dest[i] = vector.timeData[br.rowInBatch]
				}
			}
		}
	}

	// Move to next row in the batch
	br.rowInBatch++
	return nil
}

// Execute a query with batch processing for optimal performance
func (conn *Connection) QueryBatch(query string, batchSize int) (*BatchRows, error) {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	// Prepare the query string
	cQuery := cString(query)
	defer freeString(cQuery)

	// Execute the query
	var result C.duckdb_result
	if err := C.duckdb_query(*conn.conn, cQuery, &result); err == C.DuckDBError {
		return nil, fmt.Errorf("failed to execute query: %s", goString(C.duckdb_result_error(&result)))
	}

	// Create a batch query
	batchQuery := NewBatchQuery(&result, batchSize)

	// Create batch rows
	return NewBatchRows(batchQuery), nil
}

// BatchStmt is a prepared statement that uses batch processing
type BatchStmt struct {
	conn       *Connection
	stmt       *C.duckdb_prepared_statement
	paramCount int
	batchSize  int
}

// NewBatchStmt creates a new batch-oriented prepared statement
func NewBatchStmt(conn *Connection, query string, batchSize int) (*BatchStmt, error) {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	cQuery := cString(query)
	defer freeString(cQuery)

	var stmt C.duckdb_prepared_statement
	if err := C.duckdb_prepare(*conn.conn, cQuery, &stmt); err == C.DuckDBError {
		return nil, fmt.Errorf("failed to prepare statement: %s", goString(C.duckdb_prepare_error(stmt)))
	}

	// Get parameter count
	paramCount := int(C.duckdb_nparams(stmt))

	return &BatchStmt{
		conn:       conn,
		stmt:       &stmt,
		paramCount: paramCount,
		batchSize:  batchSize,
	}, nil
}

// Close closes the prepared statement
func (bs *BatchStmt) Close() error {
	if bs.stmt != nil {
		C.duckdb_destroy_prepare(bs.stmt)
		bs.stmt = nil
	}
	return nil
}

// NumInput returns the number of placeholder parameters
func (bs *BatchStmt) NumInput() int {
	return bs.paramCount
}

// bindBatchParameters binds parameters to a prepared statement
func (bs *BatchStmt) bindBatchParameters(args []interface{}) error {
	// Bind parameters
	if len(args) != bs.paramCount {
		return fmt.Errorf("expected %d parameters, got %d", bs.paramCount, len(args))
	}

	// Bind each parameter
	for i, arg := range args {
		idx := C.idx_t(i + 1) // Parameters are 1-indexed

		if arg == nil {
			if err := C.duckdb_bind_null(*bs.stmt, idx); err == C.DuckDBError {
				return fmt.Errorf("failed to bind NULL parameter at index %d", i)
			}
			continue
		}

		// Bind based on type
		switch v := arg.(type) {
		case bool:
			var val C.int8_t
			if v {
				val = 1
			}
			if err := C.duckdb_bind_int8(*bs.stmt, idx, val); err == C.DuckDBError {
				return fmt.Errorf("failed to bind boolean parameter at index %d", i)
			}

		case int8:
			if err := C.duckdb_bind_int8(*bs.stmt, idx, C.int8_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind int8 parameter at index %d", i)
			}

		case int16:
			if err := C.duckdb_bind_int16(*bs.stmt, idx, C.int16_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind int16 parameter at index %d", i)
			}

		case int32:
			if err := C.duckdb_bind_int32(*bs.stmt, idx, C.int32_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind int32 parameter at index %d", i)
			}

		case int:
			if err := C.duckdb_bind_int64(*bs.stmt, idx, C.int64_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind int parameter at index %d", i)
			}

		case int64:
			if err := C.duckdb_bind_int64(*bs.stmt, idx, C.int64_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind int64 parameter at index %d", i)
			}

		case uint8:
			if err := C.duckdb_bind_uint8(*bs.stmt, idx, C.uint8_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind uint8 parameter at index %d", i)
			}

		case uint16:
			if err := C.duckdb_bind_uint16(*bs.stmt, idx, C.uint16_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind uint16 parameter at index %d", i)
			}

		case uint32:
			if err := C.duckdb_bind_uint32(*bs.stmt, idx, C.uint32_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind uint32 parameter at index %d", i)
			}

		case uint:
			if err := C.duckdb_bind_uint64(*bs.stmt, idx, C.uint64_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind uint parameter at index %d", i)
			}

		case uint64:
			if err := C.duckdb_bind_uint64(*bs.stmt, idx, C.uint64_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind uint64 parameter at index %d", i)
			}

		case float32:
			if err := C.duckdb_bind_float(*bs.stmt, idx, C.float(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind float32 parameter at index %d", i)
			}

		case float64:
			if err := C.duckdb_bind_double(*bs.stmt, idx, C.double(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind float64 parameter at index %d", i)
			}

		case string:
			cStr := cString(v)
			defer freeString(cStr)
			if err := C.duckdb_bind_varchar(*bs.stmt, idx, cStr); err == C.DuckDBError {
				return fmt.Errorf("failed to bind string parameter at index %d", i)
			}

		case []byte:
			if len(v) == 0 {
				// For empty blobs, pass nil pointer with size 0
				// This is safer than using a temporary slice
				if err := C.duckdb_bind_blob(*bs.stmt, idx, nil, C.idx_t(0)); err == C.DuckDBError {
					return fmt.Errorf("failed to bind empty blob parameter at index %d", i)
				}
			} else {
				if err := C.duckdb_bind_blob(*bs.stmt, idx, unsafe.Pointer(&v[0]), C.idx_t(len(v))); err == C.DuckDBError {
					return fmt.Errorf("failed to bind blob parameter at index %d", i)
				}
			}

		default:
			return fmt.Errorf("unsupported parameter type %T at index %d", v, i)
		}
	}

	return nil
}

// executeQueryBatch executes a prepared statement and returns batch rows
func (bs *BatchStmt) executeQueryBatch() (*BatchRows, error) {
	// Execute statement
	var result C.duckdb_result
	if err := C.duckdb_execute_prepared(*bs.stmt, &result); err == C.DuckDBError {
		return nil, fmt.Errorf("failed to execute statement: %s", goString(C.duckdb_result_error(&result)))
	}

	// Create batch query
	batchQuery := NewBatchQuery(&result, bs.batchSize)

	// Create batch rows
	return NewBatchRows(batchQuery), nil
}

// QueryBatch executes the prepared statement with the given parameters and returns batch rows
func (bs *BatchStmt) QueryBatch(args ...interface{}) (*BatchRows, error) {
	if bs.stmt == nil {
		return nil, errors.New("statement is closed")
	}

	// Bind parameters
	if err := bs.bindBatchParameters(args); err != nil {
		return nil, err
	}

	// Execute query and return results
	return bs.executeQueryBatch()
}

// QueryContext implements the driver.StmtQueryContext interface for batch statements
func (bs *BatchStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if bs.stmt == nil {
		return nil, errors.New("statement is closed")
	}

	// Check if context is already canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Context is still valid, proceed
	}

	// Convert named parameters to positional for our implementation
	params := make([]interface{}, len(args))
	for i, arg := range args {
		params[i] = arg.Value
	}

	// Bind parameters
	if err := bs.bindBatchParameters(params); err != nil {
		return nil, err
	}

	// Execute query and return results
	return bs.executeQueryBatch()
}

// Exec implements the driver.Stmt interface for batch statements
func (bs *BatchStmt) Exec(args []driver.Value) (driver.Result, error) {
	if bs.stmt == nil {
		return nil, errors.New("statement is closed")
	}

	// Convert driver.Value to interface{}
	params := make([]interface{}, len(args))
	for i, arg := range args {
		params[i] = arg
	}

	// Bind parameters
	if err := bs.bindBatchParameters(params); err != nil {
		return nil, err
	}

	// Execute statement
	var result C.duckdb_result
	if err := C.duckdb_execute_prepared(*bs.stmt, &result); err == C.DuckDBError {
		return nil, fmt.Errorf("failed to execute statement: %s", goString(C.duckdb_result_error(&result)))
	}
	defer C.duckdb_destroy_result(&result)

	// Extract affected rows
	rowsAffected := int64(C.duckdb_rows_changed(&result))

	return &QueryResult{
		rowsAffected: rowsAffected,
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}, nil
}

// ExecContext implements the driver.StmtExecContext interface for batch statements
func (bs *BatchStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if bs.stmt == nil {
		return nil, errors.New("statement is closed")
	}

	// Check if context is already canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Context is still valid, proceed
	}

	// Convert named parameters to positional for our implementation
	params := make([]interface{}, len(args))
	for i, arg := range args {
		params[i] = arg.Value
	}

	// Bind parameters
	if err := bs.bindBatchParameters(params); err != nil {
		return nil, err
	}

	// Execute statement
	var result C.duckdb_result
	if err := C.duckdb_execute_prepared(*bs.stmt, &result); err == C.DuckDBError {
		return nil, fmt.Errorf("failed to execute statement: %s", goString(C.duckdb_result_error(&result)))
	}
	defer C.duckdb_destroy_result(&result)

	// Extract affected rows
	rowsAffected := int64(C.duckdb_rows_changed(&result))

	return &QueryResult{
		rowsAffected: rowsAffected,
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}, nil
}
