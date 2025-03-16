package duckdb

/*
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "duckdb.h"
#include "duckdb_go_adapter.h"
*/
import "C"

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"runtime"
	"sync"
	"unsafe"
)

// Fast implementation using our custom C adapter

// FastResultBuffer wraps the C result_buffer_t structure
type FastResultBuffer struct {
	buffer         C.result_buffer_t
	stringCache    StringCacher
	closed         bool
	releaseOnClose bool
}

// NewFastResultBuffer creates a new result buffer
func NewFastResultBuffer() *FastResultBuffer {
	buffer := &FastResultBuffer{
		stringCache:    NewOptimizedStringCache(100),
		releaseOnClose: true,
	}

	// Set finalizer to ensure C memory is freed
	runtime.SetFinalizer(buffer, (*FastResultBuffer).Close)

	return buffer
}

// Close releases all resources used by the result buffer
func (rb *FastResultBuffer) Close() {
	if rb.closed {
		return
	}

	// Free C resources
	if rb.releaseOnClose {
		C.free_result_buffer(&rb.buffer)
	}

	// Clear string cache
	if rb.stringCache != nil {
		rb.stringCache.Reset()
	}

	rb.closed = true
}

// FastRows implements driver.Rows interface for high-performance access
type FastRows struct {
	buffer     *FastResultBuffer
	currentRow int64
	rowCount   int64
	colCount   int32
	columns    []string
	types      []int32
	nulls      [][]bool
	data       []interface{}
	closed     bool
	mutex      sync.Mutex
}

// ExecuteFast executes a query with our high-performance C adapter
func (conn *Connection) ExecuteFast(query string) (*FastRows, error) {
	if conn.conn == nil {
		return nil, errors.New("connection is closed")
	}

	// Create a new result buffer
	rb := NewFastResultBuffer()

	// Convert query to C string
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	// Execute the query using our C adapter
	success := C.execute_query_vectorized(*conn.conn, cQuery, &rb.buffer)

	// Check for errors
	if success == 0 {
		errMsg := ""
		if rb.buffer.error_message != nil {
			errMsg = C.GoString(rb.buffer.error_message)
		} else {
			errMsg = "Unknown error executing query"
		}
		rb.Close()
		return nil, errors.New(errMsg)
	}

	// Create rows object
	rows := &FastRows{
		buffer:     rb,
		currentRow: 0,
		rowCount:   int64(rb.buffer.row_count),
		colCount:   int32(rb.buffer.column_count),
	}

	// Extract column information
	if rows.colCount > 0 {
		rows.columns = make([]string, rows.colCount)
		rows.types = make([]int32, rows.colCount)
		rows.nulls = make([][]bool, rows.colCount)
		rows.data = make([]interface{}, rows.colCount)

		// Extract column metadata
		for i := int32(0); i < rows.colCount; i++ {
			// Get column name
			colMeta := (*C.column_meta_t)(unsafe.Pointer(
				uintptr(unsafe.Pointer(rb.buffer.columns)) +
					uintptr(i)*unsafe.Sizeof(C.column_meta_t{})))

			if colMeta.name != nil {
				rows.columns[i] = C.GoString(colMeta.name)
			}

			// Get column type
			rows.types[i] = int32(colMeta._type)

			// Extract nulls array
			nullsPtrs := (*[1 << 30]unsafe.Pointer)(unsafe.Pointer(rb.buffer.nulls_ptrs))[:rows.colCount:rows.colCount]
			if nullsPtrs[i] != nil {
				nullsPtr := nullsPtrs[i]
				nulls := make([]bool, rows.rowCount)
				for j := int64(0); j < rows.rowCount; j++ {
					nulls[j] = *(*int8)(unsafe.Pointer(uintptr(nullsPtr) + uintptr(j))) != 0
				}
				rows.nulls[i] = nulls
			} else {
				// No nulls array - create a default one with all false
				nulls := make([]bool, rows.rowCount)
				rows.nulls[i] = nulls
			}

			// Extract data based on type
			dataPtrs := (*[1 << 30]unsafe.Pointer)(unsafe.Pointer(rb.buffer.data_ptrs))[:rows.colCount:rows.colCount]
			if dataPtrs[i] != nil {
				dataPtr := dataPtrs[i]

				switch rows.types[i] {
				case C.DUCKDB_TYPE_BOOLEAN:
					// For booleans, we store an array of int8/bool
					data := make([]bool, rows.rowCount)
					for j := int64(0); j < rows.rowCount; j++ {
						if !rows.nulls[i][j] {
							data[j] = *(*int8)(unsafe.Pointer(uintptr(dataPtr) + uintptr(j))) != 0
						}
					}
					rows.data[i] = data

				case C.DUCKDB_TYPE_TINYINT:
					// For tinyint, we store an array of int8
					data := make([]int8, rows.rowCount)
					for j := int64(0); j < rows.rowCount; j++ {
						if !rows.nulls[i][j] {
							data[j] = *(*int8)(unsafe.Pointer(uintptr(dataPtr) + uintptr(j)))
						}
					}
					rows.data[i] = data

				case C.DUCKDB_TYPE_SMALLINT:
					// For smallint, we store an array of int16
					data := make([]int16, rows.rowCount)
					for j := int64(0); j < rows.rowCount; j++ {
						if !rows.nulls[i][j] {
							data[j] = *(*int16)(unsafe.Pointer(uintptr(dataPtr) + uintptr(j)*2))
						}
					}
					rows.data[i] = data

				case C.DUCKDB_TYPE_INTEGER:
					// For integer, we store an array of int32
					data := make([]int32, rows.rowCount)
					for j := int64(0); j < rows.rowCount; j++ {
						if !rows.nulls[i][j] {
							data[j] = *(*int32)(unsafe.Pointer(uintptr(dataPtr) + uintptr(j)*4))
						}
					}
					rows.data[i] = data

				case C.DUCKDB_TYPE_BIGINT:
					// For bigint, we store an array of int64
					data := make([]int64, rows.rowCount)
					for j := int64(0); j < rows.rowCount; j++ {
						if !rows.nulls[i][j] {
							data[j] = *(*int64)(unsafe.Pointer(uintptr(dataPtr) + uintptr(j)*8))
						}
					}
					rows.data[i] = data

				case C.DUCKDB_TYPE_FLOAT:
					// For float, we store an array of float32
					data := make([]float32, rows.rowCount)
					for j := int64(0); j < rows.rowCount; j++ {
						if !rows.nulls[i][j] {
							data[j] = *(*float32)(unsafe.Pointer(uintptr(dataPtr) + uintptr(j)*4))
						}
					}
					rows.data[i] = data

				case C.DUCKDB_TYPE_DOUBLE:
					// For double, we store an array of float64
					data := make([]float64, rows.rowCount)
					for j := int64(0); j < rows.rowCount; j++ {
						if !rows.nulls[i][j] {
							data[j] = *(*float64)(unsafe.Pointer(uintptr(dataPtr) + uintptr(j)*8))
						}
					}
					rows.data[i] = data

				case C.DUCKDB_TYPE_VARCHAR:
					// For strings, we store offsets into a string buffer
					if rows.buffer.buffer.string_buffer != nil {
						// Get the array of offsets
						offsets := (*[1 << 30]int64)(dataPtr)[:rows.rowCount:rows.rowCount]

						// Get the string buffer base pointer
						stringBuffer := unsafe.Pointer(rows.buffer.buffer.string_buffer)

						// Create a string cache for this column
						colCache := rb.stringCache

						// For each string, extract it from the buffer
						data := make([]string, rows.rowCount)
						for j := int64(0); j < rows.rowCount; j++ {
							if !rows.nulls[i][j] && offsets[j] >= 0 {
								// Get pointer to the string in the buffer
								strPtr := (*C.char)(unsafe.Pointer(uintptr(stringBuffer) + uintptr(offsets[j])))

								// Convert to Go string
								str := C.GoString(strPtr)

								// Store in string cache to reduce allocations
								data[j] = colCache.GetDedupString(str)
							}
						}
						rows.data[i] = data
					}

				default:
					// For other types, we also store as strings
					if rows.buffer.buffer.string_buffer != nil {
						// Get the array of offsets
						offsets := (*[1 << 30]int64)(dataPtr)[:rows.rowCount:rows.rowCount]

						// Get the string buffer base pointer
						stringBuffer := unsafe.Pointer(rows.buffer.buffer.string_buffer)

						// Create a string cache for this column
						colCache := rb.stringCache

						// For each string, extract it from the buffer
						data := make([]string, rows.rowCount)
						for j := int64(0); j < rows.rowCount; j++ {
							if !rows.nulls[i][j] && offsets[j] >= 0 {
								// Get pointer to the string in the buffer
								strPtr := (*C.char)(unsafe.Pointer(uintptr(stringBuffer) + uintptr(offsets[j])))

								// Convert to Go string
								str := C.GoString(strPtr)

								// Store in string cache to reduce allocations
								data[j] = colCache.GetDedupString(str)
							}
						}
						rows.data[i] = data
					}
				}
			}
		}
	}

	return rows, nil
}

// Columns returns the names of the columns
func (r *FastRows) Columns() []string {
	return r.columns
}

// Close releases resources associated with the rows
func (r *FastRows) Close() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if r.closed {
		return nil
	}

	// Close the result buffer
	if r.buffer != nil {
		r.buffer.Close()
		r.buffer = nil
	}

	// Clear references to help GC
	r.columns = nil
	r.types = nil
	r.nulls = nil
	r.data = nil
	r.closed = true

	return nil
}

// Next populates the given slice with the next row's values
// This is much faster than the standard driver as we've already
// processed all the data in a single pass
func (r *FastRows) Next(dest []driver.Value) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// Check if rows are closed
	if r.closed {
		return io.EOF
	}

	// Check if we've reached the end
	if r.currentRow >= r.rowCount {
		return io.EOF
	}

	// Extract values for current row
	for i := 0; i < len(dest) && i < int(r.colCount); i++ {
		// Check for NULL
		if r.nulls[i][r.currentRow] {
			dest[i] = nil
			continue
		}

		// Extract value based on type
		switch r.types[i] {
		case C.DUCKDB_TYPE_BOOLEAN:
			data := r.data[i].([]bool)
			dest[i] = data[r.currentRow]

		case C.DUCKDB_TYPE_TINYINT:
			data := r.data[i].([]int8)
			dest[i] = data[r.currentRow]

		case C.DUCKDB_TYPE_SMALLINT:
			data := r.data[i].([]int16)
			dest[i] = data[r.currentRow]

		case C.DUCKDB_TYPE_INTEGER:
			data := r.data[i].([]int32)
			dest[i] = data[r.currentRow]

		case C.DUCKDB_TYPE_BIGINT:
			data := r.data[i].([]int64)
			dest[i] = data[r.currentRow]

		case C.DUCKDB_TYPE_FLOAT:
			data := r.data[i].([]float32)
			dest[i] = data[r.currentRow]

		case C.DUCKDB_TYPE_DOUBLE:
			data := r.data[i].([]float64)
			dest[i] = data[r.currentRow]

		case C.DUCKDB_TYPE_VARCHAR:
			data := r.data[i].([]string)
			dest[i] = data[r.currentRow]

		default:
			// For other types, also stored as strings
			data := r.data[i].([]string)
			dest[i] = data[r.currentRow]
		}
	}

	// Move to next row
	r.currentRow++

	return nil
}

// RowsColumnTypeScanType implements RowsColumnTypeScanType
func (r *FastRows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= int(r.colCount) {
		return nil
	}

	switch r.types[index] {
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
	case C.DUCKDB_TYPE_FLOAT:
		return reflect.TypeOf(float32(0))
	case C.DUCKDB_TYPE_DOUBLE:
		return reflect.TypeOf(float64(0))
	case C.DUCKDB_TYPE_VARCHAR:
		return reflect.TypeOf(string(""))
	default:
		return reflect.TypeOf(string(""))
	}
}

// FastQuery executes a query using the high-performance C adapter
// This is the primary entry point for the fast query implementation
func (conn *Connection) FastQuery(query string) (driver.Rows, error) {
	return conn.ExecuteFast(query)
}

// FastQueryContext executes a query with context using the high-performance adapter
func (conn *Connection) FastQueryContext(ctx interface{}, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		// Use prepared statement with parameter binding
		stmt, err := conn.FastPrepare(query)
		if err != nil {
			return nil, err
		}
		defer stmt.Close()

		// Convert named args to positional args
		params := make([]interface{}, len(args))
		for i, arg := range args {
			params[i] = arg.Value
		}

		// Execute the prepared statement
		return stmt.ExecuteFast(params...)
	}

	return conn.ExecuteFast(query)
}

// FastExecContext executes a statement without returning rows using the high-performance adapter
func (conn *Connection) FastExecContext(ctx interface{}, query string, args []driver.NamedValue) (driver.Result, error) {
	// First use the fast query implementation to execute the statement
	rows, err := conn.FastQueryContext(ctx, query, args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	// For Exec operations, we return a Result with RowsAffected
	// DuckDB doesn't provide a way to get affected rows through the C API for fast driver
	// so we'll return a default value for now
	return &Result{
		rowsAffected: 0,
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}, nil
}

// FastStmt is a prepared statement with the high-performance C adapter
type FastStmt struct {
	conn       *Connection
	stmt       *C.duckdb_prepared_statement
	query      string
	paramCount int
	closed     bool
}

// FastPrepare prepares a statement for execution with the high-performance adapter
func (conn *Connection) FastPrepare(query string) (*FastStmt, error) {
	if conn.conn == nil {
		return nil, errors.New("connection is closed")
	}

	// Convert query to C string
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	// Prepare the statement
	var stmt C.duckdb_prepared_statement
	if err := C.duckdb_prepare(*conn.conn, cQuery, &stmt); err == C.DuckDBError {
		errMsg := C.GoString(C.duckdb_prepare_error(stmt))
		return nil, fmt.Errorf("failed to prepare statement: %s", errMsg)
	}

	// Get parameter count
	paramCount := int(C.duckdb_nparams(stmt))

	// Create prepared statement object
	return &FastStmt{
		conn:       conn,
		stmt:       &stmt,
		query:      query,
		paramCount: paramCount,
	}, nil
}

// Close releases resources associated with the statement
func (stmt *FastStmt) Close() error {
	if stmt.closed {
		return nil
	}

	// Clean up C resources
	if stmt.stmt != nil {
		C.duckdb_destroy_prepare(stmt.stmt)
		stmt.stmt = nil
	}

	stmt.closed = true
	return nil
}

// ExecuteFast executes the prepared statement with the given parameters
func (stmt *FastStmt) ExecuteFast(args ...interface{}) (*FastRows, error) {
	if stmt.closed {
		return nil, errors.New("statement is closed")
	}

	// Get parameter count
	paramCount := int(C.duckdb_nparams(*stmt.stmt))
	if len(args) != paramCount {
		return nil, fmt.Errorf("expected %d parameters, got %d", paramCount, len(args))
	}

	// Bind parameters
	if len(args) > 0 {
		// Bind each parameter by position
		for i, arg := range args {
			paramIdx := C.idx_t(i + 1) // Parameters are 1-indexed in DuckDB C API
			
			// Handle nil values
			if arg == nil {
				if err := C.duckdb_bind_null(*stmt.stmt, paramIdx); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind NULL parameter at position %d", i+1)
				}
				continue
			}

			// Bind based on Go type
			switch v := arg.(type) {
			case bool:
				cVal := boolToInt8(v)
				if err := C.duckdb_bind_boolean(*stmt.stmt, paramIdx, C.bool(cVal != 0)); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind boolean parameter at position %d", i+1)
				}
			case int8:
				if err := C.duckdb_bind_int8(*stmt.stmt, paramIdx, C.int8_t(v)); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind int8 parameter at position %d", i+1)
				}
			case int16:
				if err := C.duckdb_bind_int16(*stmt.stmt, paramIdx, C.int16_t(v)); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind int16 parameter at position %d", i+1)
				}
			case int32:
				if err := C.duckdb_bind_int32(*stmt.stmt, paramIdx, C.int32_t(v)); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind int32 parameter at position %d", i+1)
				}
			case int:
				if err := C.duckdb_bind_int64(*stmt.stmt, paramIdx, C.int64_t(v)); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind int parameter at position %d", i+1)
				}
			case int64:
				if err := C.duckdb_bind_int64(*stmt.stmt, paramIdx, C.int64_t(v)); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind int64 parameter at position %d", i+1)
				}
			case uint8:
				if err := C.duckdb_bind_uint8(*stmt.stmt, paramIdx, C.uint8_t(v)); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind uint8 parameter at position %d", i+1)
				}
			case uint16:
				if err := C.duckdb_bind_uint16(*stmt.stmt, paramIdx, C.uint16_t(v)); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind uint16 parameter at position %d", i+1)
				}
			case uint32:
				if err := C.duckdb_bind_uint32(*stmt.stmt, paramIdx, C.uint32_t(v)); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind uint32 parameter at position %d", i+1)
				}
			case uint:
				if err := C.duckdb_bind_uint64(*stmt.stmt, paramIdx, C.uint64_t(v)); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind uint parameter at position %d", i+1)
				}
			case uint64:
				if err := C.duckdb_bind_uint64(*stmt.stmt, paramIdx, C.uint64_t(v)); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind uint64 parameter at position %d", i+1)
				}
			case float32:
				if err := C.duckdb_bind_float(*stmt.stmt, paramIdx, C.float(v)); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind float32 parameter at position %d", i+1)
				}
			case float64:
				if err := C.duckdb_bind_double(*stmt.stmt, paramIdx, C.double(v)); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind float64 parameter at position %d", i+1)
				}
			case string:
				cstr := C.CString(v)
				defer C.free(unsafe.Pointer(cstr))
				if err := C.duckdb_bind_varchar(*stmt.stmt, paramIdx, cstr); err == C.DuckDBError {
					return nil, fmt.Errorf("failed to bind string parameter at position %d", i+1)
				}
			case []byte:
				if len(v) == 0 {
					// Empty blob case
					if err := C.duckdb_bind_blob(*stmt.stmt, paramIdx, nil, 0); err == C.DuckDBError {
						return nil, fmt.Errorf("failed to bind empty blob parameter at position %d", i+1)
					}
				} else {
					// Non-empty blob case
					if err := C.duckdb_bind_blob(*stmt.stmt, paramIdx, unsafe.Pointer(&v[0]), C.idx_t(len(v))); err == C.DuckDBError {
						return nil, fmt.Errorf("failed to bind blob parameter at position %d", i+1)
					}
				}
			default:
				return nil, fmt.Errorf("unsupported parameter type %T at position %d", v, i+1)
			}
		}
	}

	// Create a new result buffer
	rb := NewFastResultBuffer()

	// Execute the prepared statement using our C adapter (parameters are already bound)
	success := C.execute_prepared_vectorized(*stmt.stmt, &rb.buffer)

	// Check for errors
	if success == 0 {
		errMsg := ""
		if rb.buffer.error_message != nil {
			errMsg = C.GoString(rb.buffer.error_message)
		} else {
			errMsg = "Unknown error executing prepared statement"
		}
		rb.Close()
		return nil, errors.New(errMsg)
	}

	// Create rows object
	rows := &FastRows{
		buffer:     rb,
		currentRow: 0,
		rowCount:   int64(rb.buffer.row_count),
		colCount:   int32(rb.buffer.column_count),
	}

	// Extract column information
	if rows.colCount > 0 {
		rows.columns = make([]string, rows.colCount)
		rows.types = make([]int32, rows.colCount)
		rows.nulls = make([][]bool, rows.colCount)
		rows.data = make([]interface{}, rows.colCount)
		
		// Extract column metadata
		for i := int32(0); i < rows.colCount; i++ {
			// Get column name
			colMeta := (*C.column_meta_t)(unsafe.Pointer(
				uintptr(unsafe.Pointer(rb.buffer.columns)) +
					uintptr(i)*unsafe.Sizeof(C.column_meta_t{})))
			
			if colMeta.name != nil {
				rows.columns[i] = C.GoString(colMeta.name)
			}
			
			// Get column type
			rows.types[i] = int32(colMeta._type)
			
			// Extract nulls array
			nullsPtrs := (*[1 << 30]unsafe.Pointer)(unsafe.Pointer(rb.buffer.nulls_ptrs))[:rows.colCount:rows.colCount]
			if nullsPtrs[i] != nil {
				nullsPtr := nullsPtrs[i]
				nulls := make([]bool, rows.rowCount)
				for j := int64(0); j < rows.rowCount; j++ {
					nulls[j] = *(*int8)(unsafe.Pointer(uintptr(nullsPtr) + uintptr(j))) != 0
				}
				rows.nulls[i] = nulls
			} else {
				// No nulls array - create a default one with all false
				nulls := make([]bool, rows.rowCount)
				rows.nulls[i] = nulls
			}
			
			// Extract data based on type
			dataPtrs := (*[1 << 30]unsafe.Pointer)(unsafe.Pointer(rb.buffer.data_ptrs))[:rows.colCount:rows.colCount]
			if dataPtrs[i] != nil {
				dataPtr := dataPtrs[i]
				
				switch rows.types[i] {
				case C.DUCKDB_TYPE_BOOLEAN:
					// For booleans, we store an array of int8/bool
					data := make([]bool, rows.rowCount)
					for j := int64(0); j < rows.rowCount; j++ {
						if !rows.nulls[i][j] {
							data[j] = *(*int8)(unsafe.Pointer(uintptr(dataPtr) + uintptr(j))) != 0
						}
					}
					rows.data[i] = data
					
				case C.DUCKDB_TYPE_TINYINT:
					// For tinyint, we store an array of int8
					data := make([]int8, rows.rowCount)
					for j := int64(0); j < rows.rowCount; j++ {
						if !rows.nulls[i][j] {
							data[j] = *(*int8)(unsafe.Pointer(uintptr(dataPtr) + uintptr(j)))
						}
					}
					rows.data[i] = data
					
				case C.DUCKDB_TYPE_SMALLINT:
					// For smallint, we store an array of int16
					data := make([]int16, rows.rowCount)
					for j := int64(0); j < rows.rowCount; j++ {
						if !rows.nulls[i][j] {
							data[j] = *(*int16)(unsafe.Pointer(uintptr(dataPtr) + uintptr(j)*2))
						}
					}
					rows.data[i] = data
					
				case C.DUCKDB_TYPE_INTEGER:
					// For integer, we store an array of int32
					data := make([]int32, rows.rowCount)
					for j := int64(0); j < rows.rowCount; j++ {
						if !rows.nulls[i][j] {
							data[j] = *(*int32)(unsafe.Pointer(uintptr(dataPtr) + uintptr(j)*4))
						}
					}
					rows.data[i] = data
					
				case C.DUCKDB_TYPE_BIGINT:
					// For bigint, we store an array of int64
					data := make([]int64, rows.rowCount)
					for j := int64(0); j < rows.rowCount; j++ {
						if !rows.nulls[i][j] {
							data[j] = *(*int64)(unsafe.Pointer(uintptr(dataPtr) + uintptr(j)*8))
						}
					}
					rows.data[i] = data
					
				case C.DUCKDB_TYPE_FLOAT:
					// For float, we store an array of float32
					data := make([]float32, rows.rowCount)
					for j := int64(0); j < rows.rowCount; j++ {
						if !rows.nulls[i][j] {
							data[j] = *(*float32)(unsafe.Pointer(uintptr(dataPtr) + uintptr(j)*4))
						}
					}
					rows.data[i] = data
					
				case C.DUCKDB_TYPE_DOUBLE:
					// For double, we store an array of float64
					data := make([]float64, rows.rowCount)
					for j := int64(0); j < rows.rowCount; j++ {
						if !rows.nulls[i][j] {
							data[j] = *(*float64)(unsafe.Pointer(uintptr(dataPtr) + uintptr(j)*8))
						}
					}
					rows.data[i] = data
					
				case C.DUCKDB_TYPE_VARCHAR:
					// For strings, we store offsets into a string buffer
					if rows.buffer.buffer.string_buffer != nil {
						// Get the array of offsets
						offsets := (*[1 << 30]int64)(dataPtr)[:rows.rowCount:rows.rowCount]
						
						// Get the string buffer base pointer
						stringBuffer := unsafe.Pointer(rows.buffer.buffer.string_buffer)
						
						// Create a string cache for this column
						colCache := rb.stringCache
						
						// For each string, extract it from the buffer
						data := make([]string, rows.rowCount)
						for j := int64(0); j < rows.rowCount; j++ {
							if !rows.nulls[i][j] && offsets[j] >= 0 {
								// Get pointer to the string in the buffer
								strPtr := (*C.char)(unsafe.Pointer(uintptr(stringBuffer) + uintptr(offsets[j])))
								
								// Convert to Go string
								str := C.GoString(strPtr)
								
								// Store in string cache to reduce allocations
								data[j] = colCache.GetDedupString(str)
							}
						}
						rows.data[i] = data
					}
					
				default:
					// For other types, we also store as strings
					if rows.buffer.buffer.string_buffer != nil {
						// Get the array of offsets
						offsets := (*[1 << 30]int64)(dataPtr)[:rows.rowCount:rows.rowCount]
						
						// Get the string buffer base pointer
						stringBuffer := unsafe.Pointer(rows.buffer.buffer.string_buffer)
						
						// Create a string cache for this column
						colCache := rb.stringCache
						
						// For each string, extract it from the buffer
						data := make([]string, rows.rowCount)
						for j := int64(0); j < rows.rowCount; j++ {
							if !rows.nulls[i][j] && offsets[j] >= 0 {
								// Get pointer to the string in the buffer
								strPtr := (*C.char)(unsafe.Pointer(uintptr(stringBuffer) + uintptr(offsets[j])))
								
								// Convert to Go string
								str := C.GoString(strPtr)
								
								// Store in string cache to reduce allocations
								data[j] = colCache.GetDedupString(str)
							}
						}
						rows.data[i] = data
					}
				}
			}
		}
	}

	return rows, nil
}

// DirectQuery is a simplified API that returns all result rows at once
// This is optimized for cases where you want all results in memory
func (conn *Connection) DirectQuery(query string, args ...interface{}) ([][]interface{}, []string, error) {
	var rows *FastRows
	var err error

	if len(args) > 0 {
		// If we have parameters, use a prepared statement
		stmt, err := conn.FastPrepare(query)
		if err != nil {
			return nil, nil, err
		}
		defer stmt.Close()
		
		rows, err = stmt.ExecuteFast(args...)
	} else {
		// For simple queries without parameters, use direct execution
		rows, err = conn.ExecuteFast(query)
	}
	
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	// If there are no columns, return an empty result
	if len(rows.columns) == 0 {
		return [][]interface{}{}, []string{}, nil
	}

	// Preallocate result arrays
	result := make([][]interface{}, rows.rowCount)
	for i := range result {
		result[i] = make([]interface{}, rows.colCount)
	}

	// Copy data directly from the arrays we already have
	for col := 0; col < int(rows.colCount); col++ {
		// For each column, copy values directly
		for row := int64(0); row < rows.rowCount; row++ {
			if rows.nulls[col][row] {
				// NULL value
				result[row][col] = nil
				continue
			}

			// Copy value based on type
			switch rows.types[col] {
			case C.DUCKDB_TYPE_BOOLEAN:
				data := rows.data[col].([]bool)
				result[row][col] = data[row]

			case C.DUCKDB_TYPE_TINYINT:
				data := rows.data[col].([]int8)
				result[row][col] = data[row]

			case C.DUCKDB_TYPE_SMALLINT:
				data := rows.data[col].([]int16)
				result[row][col] = data[row]

			case C.DUCKDB_TYPE_INTEGER:
				data := rows.data[col].([]int32)
				result[row][col] = data[row]

			case C.DUCKDB_TYPE_BIGINT:
				data := rows.data[col].([]int64)
				result[row][col] = data[row]

			case C.DUCKDB_TYPE_FLOAT:
				data := rows.data[col].([]float32)
				result[row][col] = data[row]

			case C.DUCKDB_TYPE_DOUBLE:
				data := rows.data[col].([]float64)
				result[row][col] = data[row]

			case C.DUCKDB_TYPE_VARCHAR:
				data := rows.data[col].([]string)
				result[row][col] = data[row]

			default:
				// For other types, also stored as strings
				data := rows.data[col].([]string)
				result[row][col] = data[row]
			}
		}
	}

	return result, rows.columns, nil
}
