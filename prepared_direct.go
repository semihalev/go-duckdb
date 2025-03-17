package duckdb

/*
#include <stdlib.h>
#include <duckdb.h>
*/
import "C"
import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// PreparedDirectStatement represents a prepared statement with direct native optimization.
// This is a high-performance implementation for parameterized queries.
type PreparedDirectStatement struct {
	conn       *Connection
	stmt       *C.duckdb_prepared_statement
	closed     int32
	paramCount int
	mu         sync.Mutex
}

// Close frees resources associated with the prepared statement.
func (ps *PreparedDirectStatement) Close() error {
	if !atomic.CompareAndSwapInt32(&ps.closed, 0, 1) {
		return nil // Already closed
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.stmt != nil {
		C.duckdb_destroy_prepare(ps.stmt)
		ps.stmt = nil
	}

	runtime.SetFinalizer(ps, nil)
	return nil
}

// ParameterCount returns the number of parameters in the prepared statement.
func (ps *PreparedDirectStatement) ParameterCount() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.stmt == nil {
		return 0
	}

	// Use cached value if available
	if ps.paramCount > 0 {
		return ps.paramCount
	}

	// Get parameter count from DuckDB
	count := int(C.duckdb_nparams(*ps.stmt))
	ps.paramCount = count
	return count
}

// Bind binds parameters to the prepared statement.
// Parameters must be provided in the same order as they appear in the query.
func (ps *PreparedDirectStatement) Bind(args ...interface{}) error {
	if atomic.LoadInt32(&ps.closed) != 0 {
		return ErrStatementClosed
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	paramCount := int(C.duckdb_nparams(*ps.stmt))
	if len(args) != paramCount {
		return fmt.Errorf("wrong number of parameters: expected %d, got %d", paramCount, len(args))
	}

	// Reset bindings for clean state
	if err := C.duckdb_clear_bindings(*ps.stmt); err == C.DuckDBError {
		errMsg := C.GoString(C.duckdb_prepare_error(*ps.stmt))
		return NewError(ErrBind, errMsg)
	}

	// Bind each parameter
	for i, arg := range args {
		idx := C.idx_t(i + 1) // DuckDB parameters are 1-indexed
		var err C.duckdb_state

		// Handle different types
		switch v := arg.(type) {
		case nil:
			err = C.duckdb_bind_null(*ps.stmt, idx)

		case bool:
			val := C.bool(v)
			err = C.duckdb_bind_boolean(*ps.stmt, idx, val)

		case int:
			val := C.int64_t(v)
			err = C.duckdb_bind_int64(*ps.stmt, idx, val)

		case int8:
			val := C.int8_t(v)
			err = C.duckdb_bind_int8(*ps.stmt, idx, val)

		case int16:
			val := C.int16_t(v)
			err = C.duckdb_bind_int16(*ps.stmt, idx, val)

		case int32:
			val := C.int32_t(v)
			err = C.duckdb_bind_int32(*ps.stmt, idx, val)

		case int64:
			val := C.int64_t(v)
			err = C.duckdb_bind_int64(*ps.stmt, idx, val)

		case uint8:
			val := C.uint8_t(v)
			err = C.duckdb_bind_uint8(*ps.stmt, idx, val)

		case uint16:
			val := C.uint16_t(v)
			err = C.duckdb_bind_uint16(*ps.stmt, idx, val)

		case uint32:
			val := C.uint32_t(v)
			err = C.duckdb_bind_uint32(*ps.stmt, idx, val)

		case uint64:
			val := C.uint64_t(v)
			err = C.duckdb_bind_uint64(*ps.stmt, idx, val)

		case float32:
			val := C.float(v)
			err = C.duckdb_bind_float(*ps.stmt, idx, val)

		case float64:
			val := C.double(v)
			err = C.duckdb_bind_double(*ps.stmt, idx, val)

		case string:
			cstr := C.CString(v)
			defer C.free(unsafe.Pointer(cstr))
			err = C.duckdb_bind_varchar(*ps.stmt, idx, cstr)

		case []byte:
			if len(v) == 0 {
				// Empty blob
				err = C.duckdb_bind_blob(*ps.stmt, idx, nil, 0)
			} else {
				// Non-empty blob
				err = C.duckdb_bind_blob(*ps.stmt, idx, unsafe.Pointer(&v[0]), C.idx_t(len(v)))
			}

		case time.Time:
			// Convert to DuckDB timestamp (microseconds since epoch)
			micros := v.Unix()*1000000 + int64(v.Nanosecond())/1000
			var ts C.duckdb_timestamp
			ts.micros = C.int64_t(micros)
			err = C.duckdb_bind_timestamp(*ps.stmt, idx, ts)

		default:
			return fmt.Errorf("unsupported parameter type: %T", arg)
		}

		if err == C.DuckDBError {
			errMsg := C.GoString(C.duckdb_prepare_error(*ps.stmt))
			return NewError(ErrBind, errMsg)
		}
	}

	return nil
}

// Execute executes the prepared statement with the bound parameters and
// returns a DirectResult for optimized access to the result set.
func (ps *PreparedDirectStatement) Execute() (*DirectResult, error) {
	if atomic.LoadInt32(&ps.closed) != 0 {
		return nil, ErrStatementClosed
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	var result C.duckdb_result
	if err := C.duckdb_execute_prepared(*ps.stmt, &result); err == C.DuckDBError {
		errMsg := C.GoString(C.duckdb_result_error(&result))
		C.duckdb_destroy_result(&result)
		return nil, NewError(ErrExec, errMsg)
	}

	// Create a DirectResult for high-performance access
	return NewDirectResult(&result), nil
}

// ExecuteColumnar executes the prepared statement and returns the result in columnar format,
// which is more efficient for analytics workloads.
func (ps *PreparedDirectStatement) ExecuteColumnar() (*ColumnarResult, error) {
	// First execute the query and get the direct result
	directResult, err := ps.Execute()
	if err != nil {
		return nil, err
	}
	defer directResult.Close()

	// Create a columnar result to hold the data
	result := &ColumnarResult{
		RowCount:    int(directResult.RowCount()),
		ColumnCount: directResult.ColumnCount(),
		ColumnNames: directResult.ColumnNames(),
		ColumnTypes: make([]string, directResult.ColumnCount()),
		Columns:     make([]interface{}, directResult.ColumnCount()),
		NullMasks:   make([][]bool, directResult.ColumnCount()),
	}

	// Extract type information
	duckDBTypes := directResult.ColumnTypes()
	for i := 0; i < result.ColumnCount; i++ {
		// Store the type name for user reference
		switch duckDBTypes[i] {
		case C.DUCKDB_TYPE_BOOLEAN:
			result.ColumnTypes[i] = "BOOLEAN"
		case C.DUCKDB_TYPE_TINYINT:
			result.ColumnTypes[i] = "TINYINT"
		case C.DUCKDB_TYPE_SMALLINT:
			result.ColumnTypes[i] = "SMALLINT"
		case C.DUCKDB_TYPE_INTEGER:
			result.ColumnTypes[i] = "INTEGER"
		case C.DUCKDB_TYPE_BIGINT:
			result.ColumnTypes[i] = "BIGINT"
		case C.DUCKDB_TYPE_UTINYINT:
			result.ColumnTypes[i] = "UTINYINT"
		case C.DUCKDB_TYPE_USMALLINT:
			result.ColumnTypes[i] = "USMALLINT"
		case C.DUCKDB_TYPE_UINTEGER:
			result.ColumnTypes[i] = "UINTEGER"
		case C.DUCKDB_TYPE_UBIGINT:
			result.ColumnTypes[i] = "UBIGINT"
		case C.DUCKDB_TYPE_FLOAT:
			result.ColumnTypes[i] = "FLOAT"
		case C.DUCKDB_TYPE_DOUBLE:
			result.ColumnTypes[i] = "DOUBLE"
		case C.DUCKDB_TYPE_VARCHAR:
			result.ColumnTypes[i] = "VARCHAR"
		case C.DUCKDB_TYPE_BLOB:
			result.ColumnTypes[i] = "BLOB"
		case C.DUCKDB_TYPE_TIMESTAMP:
			result.ColumnTypes[i] = "TIMESTAMP"
		case C.DUCKDB_TYPE_DATE:
			result.ColumnTypes[i] = "DATE"
		case C.DUCKDB_TYPE_TIME:
			result.ColumnTypes[i] = "TIME"
		default:
			result.ColumnTypes[i] = "UNKNOWN"
		}

		// Extract the column data using the appropriate method based on type
		var err error
		switch duckDBTypes[i] {
		case C.DUCKDB_TYPE_BOOLEAN:
			result.Columns[i], result.NullMasks[i], err = directResult.ExtractBoolColumn(i)
		case C.DUCKDB_TYPE_TINYINT:
			// Handle via int8
			vals, nulls, err := directResult.ExtractInt8Column(i)
			if err == nil {
				result.Columns[i] = vals
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_SMALLINT:
			// Handle via int16
			vals, nulls, err := directResult.ExtractInt16Column(i)
			if err == nil {
				result.Columns[i] = vals
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_INTEGER:
			result.Columns[i], result.NullMasks[i], err = directResult.ExtractInt32Column(i)
		case C.DUCKDB_TYPE_BIGINT:
			result.Columns[i], result.NullMasks[i], err = directResult.ExtractInt64Column(i)
		case C.DUCKDB_TYPE_UTINYINT:
			// Handle via uint8
			vals, nulls, err := directResult.ExtractUint8Column(i)
			if err == nil {
				result.Columns[i] = vals
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_USMALLINT:
			// Handle via uint16
			vals, nulls, err := directResult.ExtractUint16Column(i)
			if err == nil {
				result.Columns[i] = vals
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_UINTEGER:
			// Handle via uint32
			vals, nulls, err := directResult.ExtractUint32Column(i)
			if err == nil {
				result.Columns[i] = vals
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_UBIGINT:
			// Handle via uint64
			vals, nulls, err := directResult.ExtractUint64Column(i)
			if err == nil {
				result.Columns[i] = vals
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_FLOAT:
			// Handle via float32
			vals, nulls, err := directResult.ExtractFloat32Column(i)
			if err == nil {
				result.Columns[i] = vals
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_DOUBLE:
			result.Columns[i], result.NullMasks[i], err = directResult.ExtractFloat64Column(i)
		case C.DUCKDB_TYPE_VARCHAR:
			result.Columns[i], result.NullMasks[i], err = directResult.ExtractStringColumn(i)
		case C.DUCKDB_TYPE_BLOB:
			result.Columns[i], result.NullMasks[i], err = directResult.ExtractBlobColumn(i)
		case C.DUCKDB_TYPE_TIMESTAMP:
			vals, nulls, err := directResult.ExtractTimestampColumn(i)
			if err == nil {
				// Convert from microseconds to time.Time
				times := make([]time.Time, len(vals))
				for j, micros := range vals {
					if !nulls[j] {
						times[j] = time.Unix(micros/1000000, (micros%1000000)*1000)
					}
				}
				result.Columns[i] = times
				result.NullMasks[i] = nulls
			}
		case C.DUCKDB_TYPE_DATE:
			vals, nulls, err := directResult.ExtractDateColumn(i)
			if err == nil {
				// Convert from days to time.Time
				dates := make([]time.Time, len(vals))
				for j, days := range vals {
					if !nulls[j] {
						dates[j] = time.Unix(int64(days)*24*60*60, 0).UTC()
					}
				}
				result.Columns[i] = dates
				result.NullMasks[i] = nulls
			}
		}

		if err != nil {
			return nil, fmt.Errorf("error extracting column %d (%s): %v", i, result.ColumnNames[i], err)
		}
	}

	return result, nil
}
