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
	"unsafe"
)

// BatchSize determines how many rows to process in a single batch
// This is a key parameter that can be tuned for performance
const DefaultBatchSize = 1000

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

	// Reusable vectors to avoid allocations
	vectors []*ColumnVector

	// Current batch state
	currentBatch   int64 // Current batch number
	batchRowsRead  int   // Rows read in the current batch
	batchAvailable int   // Total rows available in the current batch

	// Temporary buffer for conversions
	buffer []byte
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
	boolData    []bool
	int8Data    []int8
	int16Data   []int16
	int32Data   []int32
	int64Data   []int64
	uint8Data   []uint8
	uint16Data  []uint16
	uint32Data  []uint32
	uint64Data  []uint64
	float32Data []float32
	float64Data []float64
	stringData  []string
	blobData    [][]byte
	timeData    []interface{} // For timestamp, date, time
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
		result:      result,
		columnCount: columnCount,
		rowCount:    rowCount,
		columnNames: make([]string, columnCount),
		columnTypes: make([]C.duckdb_type, columnCount),
		batchSize:   batchSize,
		vectors:     make([]*ColumnVector, columnCount),
		buffer:      make([]byte, 4096), // Reusable buffer for string conversions
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
func (bq *BatchQuery) createColumnVector(colType C.duckdb_type, capacity int) *ColumnVector {
	cv := &ColumnVector{
		columnType: colType,
		nullMap:    make([]bool, capacity),
		capacity:   capacity,
	}

	// Allocate type-specific storage based on column type
	switch colType {
	case C.DUCKDB_TYPE_BOOLEAN:
		cv.boolData = make([]bool, capacity)
	case C.DUCKDB_TYPE_TINYINT:
		cv.int8Data = make([]int8, capacity)
	case C.DUCKDB_TYPE_SMALLINT:
		cv.int16Data = make([]int16, capacity)
	case C.DUCKDB_TYPE_INTEGER:
		cv.int32Data = make([]int32, capacity)
	case C.DUCKDB_TYPE_BIGINT:
		cv.int64Data = make([]int64, capacity)
	case C.DUCKDB_TYPE_UTINYINT:
		cv.uint8Data = make([]uint8, capacity)
	case C.DUCKDB_TYPE_USMALLINT:
		cv.uint16Data = make([]uint16, capacity)
	case C.DUCKDB_TYPE_UINTEGER:
		cv.uint32Data = make([]uint32, capacity)
	case C.DUCKDB_TYPE_UBIGINT:
		cv.uint64Data = make([]uint64, capacity)
	case C.DUCKDB_TYPE_FLOAT:
		cv.float32Data = make([]float32, capacity)
	case C.DUCKDB_TYPE_DOUBLE:
		cv.float64Data = make([]float64, capacity)
	case C.DUCKDB_TYPE_VARCHAR:
		cv.stringData = make([]string, capacity)
	case C.DUCKDB_TYPE_BLOB:
		cv.blobData = make([][]byte, capacity)
	default:
		// For complex types (timestamps, etc.), use a generic slice
		cv.timeData = make([]interface{}, capacity)
	}

	return cv
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
	if bq.closed {
		return nil
	}

	// Free DuckDB result if we own it
	if bq.resultOwned && bq.result != nil {
		C.duckdb_destroy_result(bq.result)
		bq.result = nil
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
		vector := bq.vectors[colIdx]
		vector.length = batchSize

		// Function call is outside the loop to minimize CGO boundary crossings
		bq.extractColumnBatch(colIdx, int(batchStart), batchSize, vector)
	}

	// Update current batch
	bq.currentBatch++
	bq.currentRow += int64(batchSize)

	return true
}

// extractColumnBatch extracts a batch of values for a specific column
// This is optimized to minimize CGO boundary crossings
func (bq *BatchQuery) extractColumnBatch(colIdx int, startRow int, batchSize int, vector *ColumnVector) {
	// Get DuckDB C types ready
	cColIdx := C.idx_t(colIdx)
	cStartRow := C.idx_t(startRow)
	colType := bq.columnTypes[colIdx]

	// First pass: extract null values for the entire batch
	// This is done separately to minimize CGO boundary crossings
	for i := 0; i < batchSize; i++ {
		rowIdx := cStartRow + C.idx_t(i)
		isNull := C.duckdb_value_is_null(bq.result, cColIdx, rowIdx)
		vector.nullMap[i] = cBoolToGo(isNull)
	}

	// Second pass: extract non-null values based on column type
	// We extract an entire column at once to minimize CGO boundaries
	switch colType {
	case C.DUCKDB_TYPE_BOOLEAN:
		// Extract all boolean values in the batch
		for i := 0; i < batchSize; i++ {
			if !vector.nullMap[i] {
				rowIdx := cStartRow + C.idx_t(i)
				val := C.duckdb_value_boolean(bq.result, cColIdx, rowIdx)
				vector.boolData[i] = cBoolToGo(val)
			}
		}

	case C.DUCKDB_TYPE_TINYINT:
		// Extract all int8 values in the batch
		for i := 0; i < batchSize; i++ {
			if !vector.nullMap[i] {
				rowIdx := cStartRow + C.idx_t(i)
				val := C.duckdb_value_int8(bq.result, cColIdx, rowIdx)
				vector.int8Data[i] = int8(val)
			}
		}

	case C.DUCKDB_TYPE_SMALLINT:
		// Extract all int16 values in the batch
		for i := 0; i < batchSize; i++ {
			if !vector.nullMap[i] {
				rowIdx := cStartRow + C.idx_t(i)
				val := C.duckdb_value_int16(bq.result, cColIdx, rowIdx)
				vector.int16Data[i] = int16(val)
			}
		}

	case C.DUCKDB_TYPE_INTEGER:
		// Extract all int32 values in the batch
		for i := 0; i < batchSize; i++ {
			if !vector.nullMap[i] {
				rowIdx := cStartRow + C.idx_t(i)
				val := C.duckdb_value_int32(bq.result, cColIdx, rowIdx)
				vector.int32Data[i] = int32(val)
			}
		}

	case C.DUCKDB_TYPE_BIGINT:
		// Extract all int64 values in the batch
		for i := 0; i < batchSize; i++ {
			if !vector.nullMap[i] {
				rowIdx := cStartRow + C.idx_t(i)
				val := C.duckdb_value_int64(bq.result, cColIdx, rowIdx)
				vector.int64Data[i] = int64(val)
			}
		}

	case C.DUCKDB_TYPE_FLOAT:
		// Extract all float32 values in the batch
		for i := 0; i < batchSize; i++ {
			if !vector.nullMap[i] {
				rowIdx := cStartRow + C.idx_t(i)
				val := C.duckdb_value_float(bq.result, cColIdx, rowIdx)
				vector.float32Data[i] = float32(val)
			}
		}

	case C.DUCKDB_TYPE_DOUBLE:
		// Extract all float64 values in the batch
		for i := 0; i < batchSize; i++ {
			if !vector.nullMap[i] {
				rowIdx := cStartRow + C.idx_t(i)
				val := C.duckdb_value_double(bq.result, cColIdx, rowIdx)
				vector.float64Data[i] = float64(val)
			}
		}

	case C.DUCKDB_TYPE_VARCHAR:
		// Extract all string values in the batch
		for i := 0; i < batchSize; i++ {
			if !vector.nullMap[i] {
				rowIdx := cStartRow + C.idx_t(i)
				cstr := C.duckdb_value_varchar(bq.result, cColIdx, rowIdx)
				if cstr != nil {
					vector.stringData[i] = C.GoString(cstr)
					C.duckdb_free(unsafe.Pointer(cstr))
				} else {
					vector.stringData[i] = ""
				}
			}
		}

	case C.DUCKDB_TYPE_BLOB:
		// Extract all blob values in the batch
		for i := 0; i < batchSize; i++ {
			if !vector.nullMap[i] {
				rowIdx := cStartRow + C.idx_t(i)
				blob := C.duckdb_value_blob(bq.result, cColIdx, rowIdx)
				if blob.data != nil && blob.size > 0 {
					size := int(blob.size)
					// Allocate a new buffer for this blob
					buffer := make([]byte, size)
					// Copy blob data
					C.memcpy(unsafe.Pointer(&buffer[0]), unsafe.Pointer(blob.data), C.size_t(size))
					vector.blobData[i] = buffer
					// Free DuckDB blob memory
					C.duckdb_free(blob.data)
				} else {
					vector.blobData[i] = []byte{}
				}
			}
		}

	default:
		// For other types, convert to string for now
		// This can be optimized further for specific types
		for i := 0; i < batchSize; i++ {
			if !vector.nullMap[i] {
				rowIdx := cStartRow + C.idx_t(i)
				cstr := C.duckdb_value_varchar(bq.result, cColIdx, rowIdx)
				if cstr != nil {
					vector.timeData[i] = C.GoString(cstr)
					C.duckdb_free(unsafe.Pointer(cstr))
				} else {
					vector.timeData[i] = ""
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
	if br.query == nil {
		return io.EOF
	}

	// Check if we need a new batch
	if br.rowInBatch >= br.query.batchAvailable {
		// Try to fetch the next batch
		if !br.query.fetchNextBatch() {
			return io.EOF
		}
		br.rowInBatch = 0
	}

	// Get row data from current batch
	for i := 0; i < br.columnCount && i < len(dest); i++ {
		vector := br.query.vectors[i]

		// Check for NULL
		if vector.nullMap[br.rowInBatch] {
			dest[i] = nil
			continue
		}

		// Extract value based on column type
		switch br.query.columnTypes[i] {
		case C.DUCKDB_TYPE_BOOLEAN:
			dest[i] = vector.boolData[br.rowInBatch]
		case C.DUCKDB_TYPE_TINYINT:
			dest[i] = vector.int8Data[br.rowInBatch]
		case C.DUCKDB_TYPE_SMALLINT:
			dest[i] = vector.int16Data[br.rowInBatch]
		case C.DUCKDB_TYPE_INTEGER:
			dest[i] = vector.int32Data[br.rowInBatch]
		case C.DUCKDB_TYPE_BIGINT:
			dest[i] = vector.int64Data[br.rowInBatch]
		case C.DUCKDB_TYPE_UTINYINT:
			dest[i] = vector.uint8Data[br.rowInBatch]
		case C.DUCKDB_TYPE_USMALLINT:
			dest[i] = vector.uint16Data[br.rowInBatch]
		case C.DUCKDB_TYPE_UINTEGER:
			dest[i] = vector.uint32Data[br.rowInBatch]
		case C.DUCKDB_TYPE_UBIGINT:
			dest[i] = vector.uint64Data[br.rowInBatch]
		case C.DUCKDB_TYPE_FLOAT:
			dest[i] = vector.float32Data[br.rowInBatch]
		case C.DUCKDB_TYPE_DOUBLE:
			dest[i] = vector.float64Data[br.rowInBatch]
		case C.DUCKDB_TYPE_VARCHAR:
			dest[i] = vector.stringData[br.rowInBatch]
		case C.DUCKDB_TYPE_BLOB:
			dest[i] = vector.blobData[br.rowInBatch]
		default:
			dest[i] = vector.timeData[br.rowInBatch]
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
				if err := C.duckdb_bind_blob(*bs.stmt, idx, unsafe.Pointer(&[]byte{0}[0]), C.idx_t(0)); err == C.DuckDBError {
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

	// Convert named parameters to positional for our implementation
	params := make([]interface{}, len(args))
	for i, arg := range args {
		params[i] = arg.Value
	}

	// TODO: Respect context cancellation in the future

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

	return &Result{
		rowsAffected: rowsAffected,
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}, nil
}

// ExecContext implements the driver.StmtExecContext interface for batch statements
func (bs *BatchStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if bs.stmt == nil {
		return nil, errors.New("statement is closed")
	}

	// Convert named parameters to positional for our implementation
	params := make([]interface{}, len(args))
	for i, arg := range args {
		params[i] = arg.Value
	}

	// TODO: Respect context cancellation in the future

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

	return &Result{
		rowsAffected: rowsAffected,
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}, nil
}
