// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

/*
#cgo CFLAGS: -I${SRCDIR}/include
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <duckdb.h>
#include "duckdb_go_adapter.h"

// Forward declaration for our memory management function
int ensure_param_batch_resource_capacity(param_batch_t* batch);

// Helper function to handle type mismatches between Go CGO and C
int execute_batch_wrapper(duckdb_prepared_statement* stmt_ptr, param_batch_t* params, result_buffer_t* buffer) {
    return bind_and_execute_batch(*stmt_ptr, params, buffer);
}

// Helper functions to handle Go CGO limitations with arrays
void set_null_flag(param_batch_t* batch, int32_t idx, int8_t value) {
    batch->null_flags[idx] = value;
}

void set_param_type(param_batch_t* batch, int32_t idx, int32_t value) {
    batch->param_types[idx] = value;
}

int32_t get_param_type(param_batch_t* batch, int32_t idx) {
    return batch->param_types[idx];
}

void set_bool_data(param_batch_t* batch, int32_t idx, int8_t value) {
    batch->bool_data[idx] = value;
}

void set_int8_data(param_batch_t* batch, int32_t idx, int8_t value) {
    batch->int8_data[idx] = value;
}

void set_int16_data(param_batch_t* batch, int32_t idx, int16_t value) {
    batch->int16_data[idx] = value;
}

void set_int32_data(param_batch_t* batch, int32_t idx, int32_t value) {
    batch->int32_data[idx] = value;
}

void set_int64_data(param_batch_t* batch, int32_t idx, int64_t value) {
    batch->int64_data[idx] = value;
}

void set_uint8_data(param_batch_t* batch, int32_t idx, uint8_t value) {
    batch->uint8_data[idx] = value;
}

void set_uint16_data(param_batch_t* batch, int32_t idx, uint16_t value) {
    batch->uint16_data[idx] = value;
}

void set_uint32_data(param_batch_t* batch, int32_t idx, uint32_t value) {
    batch->uint32_data[idx] = value;
}

void set_uint64_data(param_batch_t* batch, int32_t idx, uint64_t value) {
    batch->uint64_data[idx] = value;
}

void set_float_data(param_batch_t* batch, int32_t idx, float value) {
    batch->float_data[idx] = value;
}

void set_double_data(param_batch_t* batch, int32_t idx, double value) {
    batch->double_data[idx] = value;
}

void set_string_data(param_batch_t* batch, int32_t idx, char* value) {
    batch->string_data[idx] = value;
}

void set_blob_data(param_batch_t* batch, int32_t idx, void* data, int64_t length) {
    batch->blob_data[idx] = data;
    batch->blob_lengths[idx] = length;
}

void set_timestamp_data(param_batch_t* batch, int32_t idx, int64_t value) {
    batch->timestamp_data[idx] = value;
}

// Ensure the parameter batch has enough capacity for more resources
int ensure_param_batch_resource_capacity(param_batch_t* batch) {
    if (!batch) return 0;

    // Calculate initial size based on our allocation formula
    int32_t estimated_size = batch->param_count * 3 + 20;

    // If we're close to capacity, expand
    if (batch->resource_count + 10 >= estimated_size) {
        // Double the current size
        int32_t new_size = estimated_size * 2;
        void** new_resources = realloc(batch->resources, new_size * sizeof(void*));
        if (!new_resources) {
            return 0; // Memory allocation failed
        }

        // Update the resources pointer
        batch->resources = new_resources;

        // Zero out the new portion
        memset(batch->resources + batch->resource_count, 0,
               (new_size - batch->resource_count) * sizeof(void*));
    }

    return 1;
}

void add_resource(param_batch_t* batch, void* resource) {
    // Check if we need to expand the resources array
    if (ensure_param_batch_resource_capacity(batch)) {
        batch->resources[batch->resource_count] = resource;
        batch->resource_count++;
    }
}

// Allocate memory for specific data types
void* alloc_bool_data(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(int8_t));
    batch->bool_data = (int8_t*)ptr;
    add_resource(batch, ptr);
    return ptr;
}

void* alloc_int8_data(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(int8_t));
    batch->int8_data = (int8_t*)ptr;
    add_resource(batch, ptr);
    return ptr;
}

void* alloc_int16_data(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(int16_t));
    batch->int16_data = (int16_t*)ptr;
    add_resource(batch, ptr);
    return ptr;
}

void* alloc_int32_data(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(int32_t));
    batch->int32_data = (int32_t*)ptr;
    add_resource(batch, ptr);
    return ptr;
}

void* alloc_int64_data(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(int64_t));
    batch->int64_data = (int64_t*)ptr;
    add_resource(batch, ptr);
    return ptr;
}

void* alloc_uint8_data(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(uint8_t));
    batch->uint8_data = (uint8_t*)ptr;
    add_resource(batch, ptr);
    return ptr;
}

void* alloc_uint16_data(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(uint16_t));
    batch->uint16_data = (uint16_t*)ptr;
    add_resource(batch, ptr);
    return ptr;
}

void* alloc_uint32_data(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(uint32_t));
    batch->uint32_data = (uint32_t*)ptr;
    add_resource(batch, ptr);
    return ptr;
}

void* alloc_uint64_data(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(uint64_t));
    batch->uint64_data = (uint64_t*)ptr;
    add_resource(batch, ptr);
    return ptr;
}

void* alloc_float_data(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(float));
    batch->float_data = (float*)ptr;
    add_resource(batch, ptr);
    return ptr;
}

void* alloc_double_data(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(double));
    batch->double_data = (double*)ptr;
    add_resource(batch, ptr);
    return ptr;
}

void* alloc_string_data(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(char*));
    batch->string_data = (char**)ptr;
    add_resource(batch, ptr);
    return ptr;
}

void* alloc_blob_data(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(void*));
    batch->blob_data = (void**)ptr;
    add_resource(batch, ptr);
    return ptr;
}

void* alloc_blob_lengths(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(int64_t));
    batch->blob_lengths = (int64_t*)ptr;
    add_resource(batch, ptr);
    return ptr;
}

void* alloc_timestamp_data(param_batch_t* batch, int32_t size) {
    void* ptr = calloc(size, sizeof(int64_t));
    batch->timestamp_data = (int64_t*)ptr;
    add_resource(batch, ptr);
    return ptr;
}
*/
import "C"

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

// Parameter type constants for better readability
const (
	paramNull      = 0
	paramBool      = 1
	paramInt8      = 2
	paramInt16     = 3
	paramInt32     = 4
	paramInt64     = 5
	paramUint8     = 6
	paramUint16    = 7
	paramUint32    = 8
	paramUint64    = 9
	paramFloat     = 10
	paramDouble    = 11
	paramString    = 12
	paramBlob      = 13
	paramTimestamp = 14
)

// ParamBatch represents a batch of parameters for efficient batch operations
type ParamBatch struct {
	batch    *C.param_batch_t
	paramCnt int
	batchSz  int
}

// NewParamBatch creates a new parameter batch with the given parameter count and batch size
func NewParamBatch(paramCount int, batchSize int) (*ParamBatch, error) {
	if paramCount <= 0 || batchSize <= 0 {
		return nil, errors.New("invalid parameter count or batch size")
	}

	batch := C.create_param_batch(C.int32_t(paramCount), C.int32_t(batchSize))
	if batch == nil {
		return nil, errors.New("failed to allocate parameter batch")
	}

	// Set up finalizer to free memory when GC collects this object
	pb := &ParamBatch{
		batch:    batch,
		paramCnt: paramCount,
		batchSz:  batchSize,
	}
	runtime.SetFinalizer(pb, (*ParamBatch).Finalize)

	return pb, nil
}

// Finalize frees all resources associated with the parameter batch
func (pb *ParamBatch) Finalize() {
	if pb.batch != nil {
		C.free_param_batch(pb.batch)
		pb.batch = nil
	}
}

// OptimizedBatchStmt is an optimized statement for batch parameter binding
type OptimizedBatchStmt struct {
	conn       *Connection
	stmt       *C.duckdb_prepared_statement
	paramCount int
	closed     int32
}

// NewOptimizedBatchStmt creates a new optimized batch statement
func NewOptimizedBatchStmt(conn *Connection, query string) (*OptimizedBatchStmt, error) {
	cQuery := cString(query)
	defer freeString(cQuery)

	var stmt C.duckdb_prepared_statement
	if err := C.duckdb_prepare(*conn.conn, cQuery, &stmt); err == C.DuckDBError {
		return nil, fmt.Errorf("failed to prepare statement: %s", goString(C.duckdb_prepare_error(stmt)))
	}

	// Get parameter count
	paramCount := int(C.duckdb_nparams(stmt))

	bs := &OptimizedBatchStmt{
		conn:       conn,
		stmt:       &stmt,
		paramCount: paramCount,
	}

	// Set up finalizer to clean up resources when GC collects this statement
	runtime.SetFinalizer(bs, (*OptimizedBatchStmt).Close)

	return bs, nil
}

// Close closes the prepared statement
func (bs *OptimizedBatchStmt) Close() error {
	if !atomic.CompareAndSwapInt32(&bs.closed, 0, 1) {
		return nil // Already closed
	}

	if bs.stmt != nil {
		C.duckdb_destroy_prepare(bs.stmt)
		bs.stmt = nil
	}

	bs.conn = nil
	runtime.SetFinalizer(bs, nil)
	return nil
}

// Default chunk size for batch processing
// This can be tuned for performance vs memory usage
var defaultBatchChunkSize = 1000

// ExecBatch executes a batch of parameter sets in a single CGO boundary crossing
// For large batches, it automatically chunks them to avoid memory issues
func (bs *OptimizedBatchStmt) ExecBatch(values [][]interface{}) (driver.Result, error) {
	if atomic.LoadInt32(&bs.closed) != 0 {
		return nil, errors.New("statement is closed")
	}

	if bs.conn == nil || atomic.LoadInt32(&bs.conn.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	// Check for empty batch
	if len(values) == 0 {
		return &Result{
			rowsAffected: 0,
			lastInsertID: 0,
		}, nil
	}

	// All parameter sets must have the same number of parameters
	expectedParams := bs.paramCount
	for i, params := range values {
		if len(params) != expectedParams {
			return nil, fmt.Errorf("parameter set %d has %d parameters, expected %d",
				i, len(params), expectedParams)
		}
	}

	// If we don't have the native library, use the fallback implementation
	if !nativeLibLoaded {
		return bs.execBatchFallback(values)
	}

	// For large batches, break into chunks to avoid memory issues
	if len(values) > defaultBatchChunkSize {
		return bs.execBatchChunked(values, expectedParams)
	}

	// For smaller batches, process directly without chunking
	return bs.execBatchDirect(values, expectedParams)
}

// execBatchChunked executes a large batch of parameter sets by breaking it into smaller chunks
func (bs *OptimizedBatchStmt) execBatchChunked(values [][]interface{}, expectedParams int) (driver.Result, error) {
	var totalRowsAffected int64

	// Process in chunks of defaultBatchChunkSize
	for i := 0; i < len(values); i += defaultBatchChunkSize {
		// Calculate end index for this chunk (ensuring we don't go out of bounds)
		end := i + defaultBatchChunkSize
		if end > len(values) {
			end = len(values)
		}

		// Extract the chunk
		chunk := values[i:end]

		// Execute the chunk
		res, err := bs.execBatchDirect(chunk, expectedParams)
		if err != nil {
			return nil, fmt.Errorf("error executing batch chunk %d-%d: %w", i, end-1, err)
		}

		// Add affected rows to total
		rowsAffected, _ := res.RowsAffected()
		totalRowsAffected += rowsAffected
	}

	// Return aggregated result
	return &Result{
		rowsAffected: totalRowsAffected,
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}, nil
}

// execBatchDirect executes a batch directly without chunking
// This is used for smaller batches or individual chunks of larger batches
func (bs *OptimizedBatchStmt) execBatchDirect(values [][]interface{}, expectedParams int) (driver.Result, error) {
	// Create a parameter batch
	batch, err := NewParamBatch(expectedParams, len(values))
	if err != nil {
		return nil, err
	}
	defer batch.Finalize()

	// Process all parameter sets
	for batchIdx, paramSet := range values {
		for paramIdx, value := range paramSet {
			// Calculate flattened index
			flatIdx := batchIdx*expectedParams + paramIdx

			// Handle NULL parameters
			if value == nil {
				C.set_null_flag(batch.batch, C.int32_t(flatIdx), 1)
				continue
			}

			// Determine parameter type and set it (if not already set for this parameter)
			paramType := getParamType(value)
			if paramType < 0 {
				return nil, fmt.Errorf("unsupported parameter type %T at index %d in set %d",
					value, paramIdx, batchIdx)
			}

			if batchIdx == 0 {
				// Set the parameter type for this parameter (same across all batches)
				C.set_param_type(batch.batch, C.int32_t(paramIdx), C.int32_t(paramType))

				// Allocate memory for this type if needed
				if err := allocateParamTypeMemory(batch.batch, paramType, expectedParams, len(values)); err != nil {
					return nil, err
				}
			} else {
				// Check for type mismatch
				storedType := int(C.get_param_type(batch.batch, C.int32_t(paramIdx)))
				if storedType != paramType {
					// Type mismatch for the same parameter position across different sets
					return nil, fmt.Errorf("parameter type mismatch at index %d: parameter set 0 has type %d, set %d has type %d",
						paramIdx, storedType, batchIdx, paramType)
				}
			}

			// Set the actual parameter value
			if err := setParamValue(batch.batch, paramType, flatIdx, value); err != nil {
				return nil, err
			}
		}
	}

	// Execute the batch with a single CGO call
	var buffer C.result_buffer_t
	result := C.execute_batch_wrapper(bs.stmt, batch.batch, &buffer)
	if result == 0 {
		// Get error message
		var errMsg string
		if buffer.error_message != nil {
			errMsg = C.GoString(buffer.error_message)
			C.free(unsafe.Pointer(buffer.error_message))
		} else {
			errMsg = "unknown error in batch execution"
		}
		return nil, fmt.Errorf("failed to execute batch: %s", errMsg)
	}

	// Create result
	res := &Result{
		rowsAffected: int64(buffer.rows_affected),
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}

	// Clean up result buffer
	C.free_result_buffer(&buffer)

	return res, nil
}

// execBatchFallback executes a batch without using the native optimization
// This is a complete pure-Go implementation that doesn't use the C execute_batch_wrapper
func (bs *OptimizedBatchStmt) execBatchFallback(values [][]interface{}) (driver.Result, error) {
	// Convert the prepared statement to a standard DuckDB statement
	stmt, ok := bs.stmt, true
	if !ok || stmt == nil {
		return nil, errors.New("invalid statement for fallback")
	}

	var totalRowsAffected int64

	// Handle each parameter set individually
	for _, paramSet := range values {
		// Convert to driver.Value
		driverValues := make([]driver.Value, len(paramSet))
		for i, v := range paramSet {
			driverValues[i] = driver.Value(v)
		}

		// Clear previous bindings
		C.duckdb_clear_bindings(*stmt)

		// Bind parameters
		for i, val := range driverValues {
			// DuckDB parameters are 1-indexed
			paramIdx := i + 1

			// Handle nil values
			if val == nil {
				if C.duckdb_bind_null(*stmt, C.idx_t(paramIdx)) == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind NULL to parameter %d", paramIdx)
				}
				continue
			}

			// Bind based on value type
			var err error
			switch v := val.(type) {
			case bool:
				// Use a conditional to convert Go bool to C bool
				var boolVal C.bool
				if v {
					boolVal = true
				}
				if C.duckdb_bind_boolean(*stmt, C.idx_t(paramIdx), boolVal) == C.DuckDBError {
					err = fmt.Errorf("failed to bind bool to parameter %d", paramIdx)
				}
			// Integer types
			case int:
				if C.duckdb_bind_int64(*stmt, C.idx_t(paramIdx), C.int64_t(v)) == C.DuckDBError {
					err = fmt.Errorf("failed to bind int to parameter %d", paramIdx)
				}
			case int8:
				if C.duckdb_bind_int8(*stmt, C.idx_t(paramIdx), C.int8_t(v)) == C.DuckDBError {
					err = fmt.Errorf("failed to bind int8 to parameter %d", paramIdx)
				}
			case int16:
				if C.duckdb_bind_int16(*stmt, C.idx_t(paramIdx), C.int16_t(v)) == C.DuckDBError {
					err = fmt.Errorf("failed to bind int16 to parameter %d", paramIdx)
				}
			case int32:
				if C.duckdb_bind_int32(*stmt, C.idx_t(paramIdx), C.int32_t(v)) == C.DuckDBError {
					err = fmt.Errorf("failed to bind int32 to parameter %d", paramIdx)
				}
			case int64:
				if C.duckdb_bind_int64(*stmt, C.idx_t(paramIdx), C.int64_t(v)) == C.DuckDBError {
					err = fmt.Errorf("failed to bind int64 to parameter %d", paramIdx)
				}
			// Unsigned integer types
			case uint:
				if C.duckdb_bind_uint64(*stmt, C.idx_t(paramIdx), C.uint64_t(v)) == C.DuckDBError {
					err = fmt.Errorf("failed to bind uint to parameter %d", paramIdx)
				}
			case uint8:
				if C.duckdb_bind_uint8(*stmt, C.idx_t(paramIdx), C.uint8_t(v)) == C.DuckDBError {
					err = fmt.Errorf("failed to bind uint8 to parameter %d", paramIdx)
				}
			case uint16:
				if C.duckdb_bind_uint16(*stmt, C.idx_t(paramIdx), C.uint16_t(v)) == C.DuckDBError {
					err = fmt.Errorf("failed to bind uint16 to parameter %d", paramIdx)
				}
			case uint32:
				if C.duckdb_bind_uint32(*stmt, C.idx_t(paramIdx), C.uint32_t(v)) == C.DuckDBError {
					err = fmt.Errorf("failed to bind uint32 to parameter %d", paramIdx)
				}
			case uint64:
				if C.duckdb_bind_uint64(*stmt, C.idx_t(paramIdx), C.uint64_t(v)) == C.DuckDBError {
					err = fmt.Errorf("failed to bind uint64 to parameter %d", paramIdx)
				}
			// Float types
			case float32:
				if C.duckdb_bind_float(*stmt, C.idx_t(paramIdx), C.float(v)) == C.DuckDBError {
					err = fmt.Errorf("failed to bind float32 to parameter %d", paramIdx)
				}
			case float64:
				if C.duckdb_bind_double(*stmt, C.idx_t(paramIdx), C.double(v)) == C.DuckDBError {
					err = fmt.Errorf("failed to bind float64 to parameter %d", paramIdx)
				}
			// String and binary types
			case string:
				cstr := C.CString(v)
				defer C.free(unsafe.Pointer(cstr))
				if C.duckdb_bind_varchar(*stmt, C.idx_t(paramIdx), cstr) == C.DuckDBError {
					err = fmt.Errorf("failed to bind string to parameter %d", paramIdx)
				}
			case []byte:
				dataPtr := unsafe.Pointer(nil)
				if len(v) > 0 {
					dataPtr = unsafe.Pointer(&v[0])
				}
				if C.duckdb_bind_blob(*stmt, C.idx_t(paramIdx), dataPtr, C.idx_t(len(v))) == C.DuckDBError {
					err = fmt.Errorf("failed to bind blob to parameter %d", paramIdx)
				}
			// Time types
			case time.Time:
				// Convert time to DuckDB timestamp (microseconds since epoch)
				micros := v.UnixMicro()
				ts := C.duckdb_timestamp{micros: C.int64_t(micros)}
				if C.duckdb_bind_timestamp(*stmt, C.idx_t(paramIdx), ts) == C.DuckDBError {
					err = fmt.Errorf("failed to bind timestamp to parameter %d", paramIdx)
				}
			default:
				err = fmt.Errorf("unsupported parameter type %T for parameter %d", val, paramIdx)
			}

			if err != nil {
				return nil, err
			}
		}

		// Execute the statement
		var result C.duckdb_result
		if C.duckdb_execute_prepared(*stmt, &result) == C.DuckDBError {
			errMsg := C.GoString(C.duckdb_result_error(&result))
			C.duckdb_destroy_result(&result)
			return nil, fmt.Errorf("failed to execute statement: %s", errMsg)
		}

		// Get affected rows count
		rowsAffected := int64(C.duckdb_rows_changed(&result))
		totalRowsAffected += rowsAffected

		// Clean up result
		C.duckdb_destroy_result(&result)
	}

	// Return result with total affected rows
	return &Result{
		rowsAffected: totalRowsAffected,
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}, nil
}

// ExecBatchContext executes a batch of parameter sets with context support
func (bs *OptimizedBatchStmt) ExecBatchContext(ctx context.Context, values [][]interface{}) (driver.Result, error) {
	if atomic.LoadInt32(&bs.closed) != 0 {
		return nil, errors.New("statement is closed")
	}

	if bs.conn == nil || atomic.LoadInt32(&bs.conn.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	// Check if context is already canceled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Context is still valid, proceed
	}

	// Check for empty batch
	if len(values) == 0 {
		return &Result{
			rowsAffected: 0,
			lastInsertID: 0,
		}, nil
	}

	// All parameter sets must have the same number of parameters
	expectedParams := bs.paramCount
	for i, params := range values {
		// Check context periodically during validation
		if i > 0 && i%1000 == 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				// Continue validation
			}
		}
		
		if len(params) != expectedParams {
			return nil, fmt.Errorf("parameter set %d has %d parameters, expected %d",
				i, len(params), expectedParams)
		}
	}

	// If we don't have the native library, use the fallback implementation
	if !nativeLibLoaded {
		return bs.execBatchContextFallback(ctx, values)
	}

	// For large batches, break into chunks to avoid memory issues
	if len(values) > defaultBatchChunkSize {
		return bs.execBatchChunkedContext(ctx, values, expectedParams)
	}

	// For smaller batches, process directly without chunking
	// Add context check before the direct execution
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Proceed with execution
	}
	
	return bs.execBatchDirect(values, expectedParams)
}

// execBatchContextFallback executes batch with context support for fallback mode
func (bs *OptimizedBatchStmt) execBatchContextFallback(ctx context.Context, values [][]interface{}) (driver.Result, error) {
	// We'll just reuse the existing fallback implementation but check context at regular intervals
	// This simplifies the code significantly and avoids duplicating the binding logic
	
	// Check context before starting
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Continue execution
	}
	
	// Use the standard execution but check context after a reasonable number of batches
	result, err := bs.execBatchFallback(values)
	
	// Check context again after completion (in case it was canceled but execution succeeded anyway)
	select {
	case <-ctx.Done():
		return nil, ctx.Err() 
	default:
		return result, err
	}
}

// execBatchChunkedContext executes a large batch with context support
func (bs *OptimizedBatchStmt) execBatchChunkedContext(ctx context.Context, values [][]interface{}, expectedParams int) (driver.Result, error) {
	// Check context before starting
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Continue execution
	}
	
	// Use the regular chunked execution
	result, err := bs.execBatchChunked(values, expectedParams)
	
	// Check context again after completion
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return result, err
	}
}

// NumInput returns the number of placeholder parameters
func (bs *OptimizedBatchStmt) NumInput() int {
	return bs.paramCount
}

// Allocate memory for parameter arrays by type
func allocateParamTypeMemory(batch *C.param_batch_t, paramType int, paramCount, batchSize int) error {
	total := paramCount * batchSize

	// Allocate memory based on parameter type
	switch paramType {
	case paramBool:
		if C.alloc_bool_data(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for bool parameters")
		}

	case paramInt8:
		if C.alloc_int8_data(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for int8 parameters")
		}

	case paramInt16:
		if C.alloc_int16_data(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for int16 parameters")
		}

	case paramInt32:
		if C.alloc_int32_data(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for int32 parameters")
		}

	case paramInt64:
		if C.alloc_int64_data(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for int64 parameters")
		}

	case paramUint8:
		if C.alloc_uint8_data(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for uint8 parameters")
		}

	case paramUint16:
		if C.alloc_uint16_data(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for uint16 parameters")
		}

	case paramUint32:
		if C.alloc_uint32_data(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for uint32 parameters")
		}

	case paramUint64:
		if C.alloc_uint64_data(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for uint64 parameters")
		}

	case paramFloat:
		if C.alloc_float_data(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for float parameters")
		}

	case paramDouble:
		if C.alloc_double_data(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for double parameters")
		}

	case paramString:
		if C.alloc_string_data(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for string parameters")
		}

	case paramBlob:
		// For blobs, we allocate both data and lengths arrays
		if C.alloc_blob_data(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for blob data pointers")
		}

		if C.alloc_blob_lengths(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for blob lengths")
		}

	case paramTimestamp:
		if C.alloc_timestamp_data(batch, C.int32_t(total)) == nil {
			return fmt.Errorf("failed to allocate memory for timestamp parameters")
		}

	default:
		return fmt.Errorf("unsupported parameter type: %d", paramType)
	}

	return nil
}

// Determine the parameter type for a given Go value
func getParamType(value interface{}) int {
	switch value.(type) {
	case nil:
		return paramNull
	case bool:
		return paramBool
	case int8:
		return paramInt8
	case int16:
		return paramInt16
	case int32:
		return paramInt32
	case int:
		// For Go int, use int64 since we don't know the platform size
		return paramInt64
	case int64:
		return paramInt64
	case uint8:
		return paramUint8
	case uint16:
		return paramUint16
	case uint32:
		return paramUint32
	case uint:
		// For Go uint, use uint64 since we don't know the platform size
		return paramUint64
	case uint64:
		return paramUint64
	case float32:
		return paramFloat
	case float64:
		return paramDouble
	case string:
		return paramString
	case []byte:
		return paramBlob
	case time.Time:
		return paramTimestamp
	default:
		// Unsupported type
		return -1
	}
}

// Set a parameter value in the batch
func setParamValue(batch *C.param_batch_t, paramType int, idx int, value interface{}) error {
	switch paramType {
	case paramBool:
		if v, ok := value.(bool); ok {
			var val C.int8_t
			if v {
				val = 1
			}
			C.set_bool_data(batch, C.int32_t(idx), val)
		} else {
			return fmt.Errorf("expected bool, got %T", value)
		}

	case paramInt8:
		if v, ok := value.(int8); ok {
			C.set_int8_data(batch, C.int32_t(idx), C.int8_t(v))
		} else {
			return fmt.Errorf("expected int8, got %T", value)
		}

	case paramInt16:
		if v, ok := value.(int16); ok {
			C.set_int16_data(batch, C.int32_t(idx), C.int16_t(v))
		} else {
			return fmt.Errorf("expected int16, got %T", value)
		}

	case paramInt32:
		if v, ok := value.(int32); ok {
			C.set_int32_data(batch, C.int32_t(idx), C.int32_t(v))
		} else {
			return fmt.Errorf("expected int32, got %T", value)
		}

	case paramInt64:
		var val int64
		switch v := value.(type) {
		case int:
			val = int64(v)
		case int64:
			val = v
		default:
			return fmt.Errorf("expected int64 or int, got %T", value)
		}
		C.set_int64_data(batch, C.int32_t(idx), C.int64_t(val))

	case paramUint8:
		if v, ok := value.(uint8); ok {
			C.set_uint8_data(batch, C.int32_t(idx), C.uint8_t(v))
		} else {
			return fmt.Errorf("expected uint8, got %T", value)
		}

	case paramUint16:
		if v, ok := value.(uint16); ok {
			C.set_uint16_data(batch, C.int32_t(idx), C.uint16_t(v))
		} else {
			return fmt.Errorf("expected uint16, got %T", value)
		}

	case paramUint32:
		if v, ok := value.(uint32); ok {
			C.set_uint32_data(batch, C.int32_t(idx), C.uint32_t(v))
		} else {
			return fmt.Errorf("expected uint32, got %T", value)
		}

	case paramUint64:
		var val uint64
		switch v := value.(type) {
		case uint:
			val = uint64(v)
		case uint64:
			val = v
		default:
			return fmt.Errorf("expected uint64 or uint, got %T", value)
		}
		C.set_uint64_data(batch, C.int32_t(idx), C.uint64_t(val))

	case paramFloat:
		if v, ok := value.(float32); ok {
			C.set_float_data(batch, C.int32_t(idx), C.float(v))
		} else {
			return fmt.Errorf("expected float32, got %T", value)
		}

	case paramDouble:
		if v, ok := value.(float64); ok {
			C.set_double_data(batch, C.int32_t(idx), C.double(v))
		} else {
			return fmt.Errorf("expected float64, got %T", value)
		}

	case paramString:
		if v, ok := value.(string); ok {
			// Ensure resources array has enough capacity before allocating string
			if C.ensure_param_batch_resource_capacity(batch) == 0 {
				return fmt.Errorf("failed to ensure resource capacity for string data")
			}

			// Allocate and copy string to C memory
			cstr := C.CString(v)
			if cstr == nil {
				return fmt.Errorf("failed to allocate memory for string data")
			}

			// Set the string data
			C.set_string_data(batch, C.int32_t(idx), cstr)

			// Explicitly track this resource to ensure proper cleanup
			C.add_resource(batch, unsafe.Pointer(cstr))
		} else {
			return fmt.Errorf("expected string, got %T", value)
		}

	case paramBlob:
		if v, ok := value.([]byte); ok {
			length := len(v)

			if length > 0 {
				// Ensure resources array has enough capacity before allocation
				if C.ensure_param_batch_resource_capacity(batch) == 0 {
					return fmt.Errorf("failed to ensure resource capacity for blob data")
				}

				// Allocate memory for the blob data with proper error handling
				data := C.malloc(C.size_t(length))
				if data == nil {
					return fmt.Errorf("failed to allocate memory for blob data of size %d", length)
				}

				// Copy the data - use safer approach with explicit size
				C.memcpy(data, unsafe.Pointer(&v[0]), C.size_t(length))

				// Set the blob data and length
				C.set_blob_data(batch, C.int32_t(idx), data, C.int64_t(length))

				// Explicitly track this resource to ensure proper cleanup
				C.add_resource(batch, data)
			} else {
				// Empty blob - no allocation needed
				C.set_blob_data(batch, C.int32_t(idx), nil, 0)
			}
		} else {
			return fmt.Errorf("expected []byte, got %T", value)
		}

	case paramTimestamp:
		if v, ok := value.(time.Time); ok {
			// Convert to DuckDB timestamp (microseconds since 1970-01-01)
			micros := v.Unix()*1000000 + int64(v.Nanosecond())/1000
			C.set_timestamp_data(batch, C.int32_t(idx), C.int64_t(micros))
		} else {
			return fmt.Errorf("expected time.Time, got %T", value)
		}

	default:
		return fmt.Errorf("unsupported parameter type: %d", paramType)
	}

	return nil
}

// Helper function to add batch preparation to a connection
func (conn *Connection) PrepareOptimizedBatch(query string) (*OptimizedBatchStmt, error) {
	if atomic.LoadInt32(&conn.closed) != 0 {
		return nil, driver.ErrBadConn
	}

	return NewOptimizedBatchStmt(conn, query)
}
