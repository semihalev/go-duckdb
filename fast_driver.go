// Package duckdb provides low-level, high-performance SQL driver for DuckDB in Go.
package duckdb

/*
#cgo CFLAGS: -I${SRCDIR}/include
#include <stdlib.h>
#include <string.h>
#include <duckdb.h>
#include "duckdb_go_adapter.h"
*/
import "C"
import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"runtime"
	"time"
	"unsafe"
)

// FastExec executes a query without returning any rows.
// This is the high-performance implementation that uses the C adapter.
func (conn *Connection) FastExec(query string, args ...driver.Value) (driver.Result, error) {
	if len(args) == 0 {
		// Direct query execution for simple statements
		return conn.fastExecDirect(query)
	}

	// For queries with parameters, use a prepared statement
	stmt, err := conn.FastPrepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	// Execute with parameters
	return stmt.ExecuteWithResult(args...)
}

// fastExecDirect executes a simple query without parameters using the C adapter.
func (conn *Connection) fastExecDirect(query string) (driver.Result, error) {
	// Prepare query string for C
	cQuery := cString(query)
	defer freeString(cQuery)

	// Get buffer from pool
	buffer := GetBuffer()
	defer PutBuffer(buffer)

	// Execute query with our vectorized shim function
	result := C.execute_query_vectorized(*conn.conn, cQuery, buffer)

	if result == 0 {
		return nil, fmt.Errorf("failed to execute query: %s", C.GoString(buffer.error_message))
	}

	// Extract affected rows information
	rowsAffected := int64(buffer.rows_affected)

	return &QueryResult{
		rowsAffected: rowsAffected,
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}, nil
}

// FastExecContext executes a query without returning any rows, with context and named parameters.
func (conn *Connection) FastExecContext(ctx context.Context, query string, args []driver.Value) (driver.Result, error) {
	// First check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Proceed if context is still valid
	}

	// If no parameters, use direct execution
	if len(args) == 0 {
		return conn.fastExecDirect(query)
	}

	// For queries with parameters, use a prepared statement
	stmt, err := conn.FastPrepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	// Check context again before proceeding
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Continue if still valid
	}

	// Execute with parameters and respect context cancellation
	return stmt.ExecContext(ctx, args)
}

// FastQueryContext executes a query that returns rows, with context and named parameters.
func (conn *Connection) FastQueryContext(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {
	// First check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Proceed if context is still valid
	}

	// If no parameters, use direct query
	if len(args) == 0 {
		return conn.FastQuery(query)
	}

	// Prepare the statement
	stmt, err := conn.FastPrepare(query)
	if err != nil {
		return nil, err
	}

	// Check context again before proceeding
	select {
	case <-ctx.Done():
		stmt.Close() // Clean up resources
		return nil, ctx.Err()
	default:
		// Continue if still valid
	}

	// Execute the query with context support
	return stmt.QueryContext(ctx, args)
}

// FastQuery executes a direct query with the fast driver.
func (conn *Connection) FastQuery(query string) (driver.Rows, error) {
	// Prepare query string for C
	cQuery := cString(query)
	defer freeString(cQuery)

	// Get buffer from pool
	buffer := GetBuffer()

	// Execute query with our vectorized shim function
	result := C.execute_query_vectorized(*conn.conn, cQuery, buffer)
	if result == 0 {
		// Return buffer to pool on error
		PutBuffer(buffer)
		return nil, fmt.Errorf("failed to execute query: %s", C.GoString(buffer.error_message))
	}

	// Create FastRows from buffer and return
	// The buffer's ownership is transferred to FastRows
	return newFastRowsFromBuffer(buffer), nil
}

// FastStmt is a prepared statement for the fast driver.
type FastStmt struct {
	conn         *Connection
	query        string
	stmt         *C.duckdb_prepared_statement
	paramCount   int
	standardStmt driver.Stmt // For stub implementation
}

// FastPrepare prepares a statement with the fast driver.
func (conn *Connection) FastPrepare(query string) (*FastStmt, error) {
	// Prepare query string for C
	cQuery := cString(query)
	defer freeString(cQuery)

	// Prepare statement
	var stmt C.duckdb_prepared_statement
	if err := C.duckdb_prepare(*conn.conn, cQuery, &stmt); err == C.DuckDBError {
		return nil, fmt.Errorf("failed to prepare statement: %s", goString(C.duckdb_prepare_error(stmt)))
	}

	// Get parameter count
	paramCount := int(C.duckdb_nparams(stmt))

	return &FastStmt{
		conn:       conn,
		query:      query,
		stmt:       &stmt,
		paramCount: paramCount,
	}, nil
}

// NumInput returns the number of placeholder parameters
func (stmt *FastStmt) NumInput() int {
	return stmt.paramCount
}

// Close closes the prepared statement.
func (stmt *FastStmt) Close() error {
	if stmt.stmt != nil {
		C.duckdb_destroy_prepare(stmt.stmt)
		stmt.stmt = nil
	}

	// Close standard statement if it exists (for stub implementation)
	if stmt.standardStmt != nil {
		stmt.standardStmt.Close()
		stmt.standardStmt = nil
	}

	return nil
}

// Exec implements the driver.Stmt interface.
func (stmt *FastStmt) Exec(args []driver.Value) (driver.Result, error) {
	return stmt.ExecuteWithResult(args...)
}

// ExecContext implements the driver.StmtExecContext interface.
func (stmt *FastStmt) ExecContext(ctx context.Context, args []driver.Value) (driver.Result, error) {
	// Check if statement is closed
	if stmt.stmt == nil {
		return nil, fmt.Errorf("statement is closed")
	}

	// Check for context cancellation first
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Proceed if context still valid
	}

	// Bind parameters
	if err := stmt.bindParameters(args); err != nil {
		return nil, err
	}

	// Check context again before executing
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Proceed with execution
	}

	// Get buffer from pool
	buffer := GetBuffer()
	defer PutBuffer(buffer)

	// Execute prepared statement with vectorized C adapter
	result := C.execute_prepared_vectorized(*stmt.stmt, buffer)

	if result == 0 {
		return nil, fmt.Errorf("failed to execute statement: %s", C.GoString(buffer.error_message))
	}

	// Extract affected rows information
	rowsAffected := int64(buffer.rows_affected)

	return &QueryResult{
		rowsAffected: rowsAffected,
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}, nil
}

// Query implements the driver.Stmt interface.
func (stmt *FastStmt) Query(args []driver.Value) (driver.Rows, error) {
	return stmt.ExecuteFast(args...)
}

// QueryContext implements the driver.StmtQueryContext interface.
func (stmt *FastStmt) QueryContext(ctx context.Context, args []driver.Value) (driver.Rows, error) {
	// Check if statement is closed
	if stmt.stmt == nil {
		return nil, fmt.Errorf("statement is closed")
	}

	// Check for context cancellation first
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Proceed if context still valid
	}

	// Bind parameters
	if err := stmt.bindParameters(args); err != nil {
		return nil, err
	}

	// Check context again before executing
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// Proceed with execution
	}

	// Get buffer from pool
	buffer := GetBuffer()

	// Execute prepared statement with vectorized C adapter
	result := C.execute_prepared_vectorized(*stmt.stmt, buffer)
	if result == 0 {
		// Return buffer to pool on error
		PutBuffer(buffer)
		return nil, fmt.Errorf("failed to execute statement: %s", C.GoString(buffer.error_message))
	}

	// Create FastRows from buffer and return
	// The buffer's ownership is transferred to FastRows
	return newFastRowsFromBuffer(buffer), nil
}

// ExecuteFast executes the prepared statement with the fast driver.
func (stmt *FastStmt) ExecuteFast(args ...driver.Value) (driver.Rows, error) {
	if stmt.stmt == nil {
		return nil, fmt.Errorf("statement is closed")
	}

	// Bind parameters
	if err := stmt.bindParameters(args); err != nil {
		return nil, err
	}

	// Get buffer from pool
	buffer := GetBuffer()

	// Execute prepared statement with vectorized C adapter
	result := C.execute_prepared_vectorized(*stmt.stmt, buffer)
	if result == 0 {
		// Return buffer to pool on error
		PutBuffer(buffer)
		return nil, fmt.Errorf("failed to execute statement: %s", C.GoString(buffer.error_message))
	}

	// Create FastRows from buffer and return
	// The buffer's ownership is transferred to FastRows
	return newFastRowsFromBuffer(buffer), nil
}

// ExecuteWithResult executes the prepared statement and returns a Result with affected rows.
func (stmt *FastStmt) ExecuteWithResult(args ...driver.Value) (driver.Result, error) {
	if stmt.stmt == nil {
		return nil, fmt.Errorf("statement is closed")
	}

	namedArgs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		namedArgs[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}

	// If statement is backed by the standard driver, delegate to it (for stubs)
	if stmt.standardStmt != nil {
		// Check if standardStmt implements StmtExecContext
		if execer, ok := stmt.standardStmt.(driver.StmtExecContext); ok {
			// Convert to NamedValue
			return execer.ExecContext(context.Background(), namedArgs)
		}

		// Fall back to Exec if StmtExecContext is not implemented
		return stmt.standardStmt.Exec(args)
	}

	// Bind parameters
	if err := stmt.bindParameters(args); err != nil {
		return nil, err
	}

	// Get buffer from pool
	buffer := GetBuffer()
	defer PutBuffer(buffer)

	// Execute prepared statement with vectorized C adapter
	result := C.execute_prepared_vectorized(*stmt.stmt, buffer)

	if result == 0 {
		return nil, fmt.Errorf("failed to execute statement: %s", C.GoString(buffer.error_message))
	}

	// Extract affected rows information
	rowsAffected := int64(buffer.rows_affected)

	return &QueryResult{
		rowsAffected: rowsAffected,
		lastInsertID: 0, // DuckDB doesn't support last insert ID
	}, nil
}

// bindParameters binds parameters to the prepared statement.
func (stmt *FastStmt) bindParameters(args []driver.Value) error {
	if len(args) != stmt.paramCount {
		return fmt.Errorf("expected %d parameters, got %d", stmt.paramCount, len(args))
	}

	// DuckDB automatically resets bindings on execute, no need to clear them manually

	// Bind each parameter
	for i, arg := range args {
		idx := C.idx_t(i + 1) // Parameters are 1-indexed in DuckDB

		if arg == nil {
			if err := C.duckdb_bind_null(*stmt.stmt, idx); err == C.DuckDBError {
				return fmt.Errorf("failed to bind NULL parameter at index %d", i+1)
			}
			continue
		}

		// Bind based on type
		switch v := arg.(type) {
		case bool:
			val := C.int8_t(0)
			if v {
				val = C.int8_t(1)
			}
			if err := C.duckdb_bind_int8(*stmt.stmt, idx, val); err == C.DuckDBError {
				return fmt.Errorf("failed to bind boolean parameter at index %d", i+1)
			}

		case int8:
			if err := C.duckdb_bind_int8(*stmt.stmt, idx, C.int8_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind int8 parameter at index %d", i+1)
			}

		case int16:
			if err := C.duckdb_bind_int16(*stmt.stmt, idx, C.int16_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind int16 parameter at index %d", i+1)
			}

		case int32:
			if err := C.duckdb_bind_int32(*stmt.stmt, idx, C.int32_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind int32 parameter at index %d", i+1)
			}

		case int:
			if err := C.duckdb_bind_int64(*stmt.stmt, idx, C.int64_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind int parameter at index %d", i+1)
			}

		case int64:
			if err := C.duckdb_bind_int64(*stmt.stmt, idx, C.int64_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind int64 parameter at index %d", i+1)
			}

		case uint8:
			if err := C.duckdb_bind_uint8(*stmt.stmt, idx, C.uint8_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind uint8 parameter at index %d", i+1)
			}

		case uint16:
			if err := C.duckdb_bind_uint16(*stmt.stmt, idx, C.uint16_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind uint16 parameter at index %d", i+1)
			}

		case uint32:
			if err := C.duckdb_bind_uint32(*stmt.stmt, idx, C.uint32_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind uint32 parameter at index %d", i+1)
			}

		case uint:
			if err := C.duckdb_bind_uint64(*stmt.stmt, idx, C.uint64_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind uint parameter at index %d", i+1)
			}

		case uint64:
			if err := C.duckdb_bind_uint64(*stmt.stmt, idx, C.uint64_t(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind uint64 parameter at index %d", i+1)
			}

		case float32:
			if err := C.duckdb_bind_float(*stmt.stmt, idx, C.float(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind float32 parameter at index %d", i+1)
			}

		case float64:
			if err := C.duckdb_bind_double(*stmt.stmt, idx, C.double(v)); err == C.DuckDBError {
				return fmt.Errorf("failed to bind float64 parameter at index %d", i+1)
			}

		case string:
			cStr := cString(v)
			defer freeString(cStr)
			if err := C.duckdb_bind_varchar(*stmt.stmt, idx, cStr); err == C.DuckDBError {
				return fmt.Errorf("failed to bind string parameter at index %d", i+1)
			}

		case []byte:
			if len(v) == 0 {
				if err := C.duckdb_bind_blob(*stmt.stmt, idx, nil, C.idx_t(0)); err == C.DuckDBError {
					return fmt.Errorf("failed to bind empty blob parameter at index %d", i+1)
				}
			} else {
				if err := C.duckdb_bind_blob(*stmt.stmt, idx, unsafe.Pointer(&v[0]), C.idx_t(len(v))); err == C.DuckDBError {
					return fmt.Errorf("failed to bind blob parameter at index %d", i+1)
				}
			}

		case time.Time:
			// Convert to DuckDB timestamp (microseconds since 1970-01-01)
			micros := v.Unix()*1000000 + int64(v.Nanosecond())/1000
			ts := C.duckdb_timestamp{micros: C.int64_t(micros)}
			if err := C.duckdb_bind_timestamp(*stmt.stmt, idx, ts); err == C.DuckDBError {
				return fmt.Errorf("failed to bind timestamp parameter at index %d", i+1)
			}

		default:
			return fmt.Errorf("unsupported parameter type %T at index %d", v, i+1)
		}
	}

	return nil
}

// FastRows represents rows returned by the fast driver.
// It implements the driver.Rows interface for fast query results.
type FastRows struct {
	buffer      *C.result_buffer_t
	columnCount int32
	rowCount    int64
	currentRow  int64
	columnNames []string
	closed      bool
}

// newFastRowsFromBuffer creates a new FastRows from a result buffer.
func newFastRowsFromBuffer(buffer *C.result_buffer_t) *FastRows {
	// Get basic metadata
	columnCount := int32(buffer.column_count)
	rowCount := int64(buffer.row_count)

	// Extract column names
	columnNames := make([]string, columnCount)
	for i := int32(0); i < columnCount; i++ {
		colPtr := unsafe.Pointer(uintptr(unsafe.Pointer(buffer.columns)) + uintptr(i)*unsafe.Sizeof(C.column_meta_t{}))
		colInfo := (*C.column_meta_t)(colPtr)
		columnNames[i] = C.GoString(colInfo.name)
	}

	// Buffer already has a reference count of 1 from GetBuffer()

	// Create rows and setup finalizer to ensure cleanup
	rows := &FastRows{
		buffer:      buffer,
		columnCount: columnCount,
		rowCount:    rowCount,
		currentRow:  0,
		columnNames: columnNames,
	}

	// Add runtime finalizer to ensure buffer cleanup even if Close() is not called
	runtime.SetFinalizer(rows, finalizeRows)

	return rows
}

// finalizeRows is called by the garbage collector to ensure the buffer is released
func finalizeRows(rows *FastRows) {
	// First close the rows
	rows.Close()

	// Then release buffer resources - we do this as a separate step to avoid use-after-free
	// during actual use
	rows.releaseBufferResources()
}

// Columns returns the names of the columns.
func (r *FastRows) Columns() []string {
	return r.columnNames
}

// Close closes the rows and releases resources.
func (r *FastRows) Close() error {
	if r.closed {
		return nil
	}

	// Mark as closed to prevent further Next() calls
	r.closed = true

	// We'll hold onto our buffer but mark ourselves as closed
	// This ensures any in-progress Scan operations can complete
	// The buffer will be released when the finalizer runs or when
	// explicitly released later

	// Remove finalizer since we're manually handling cleanup
	runtime.SetFinalizer(r, nil)

	// Instead of immediately freeing the buffer, we'll hold onto it
	// This prevents use-after-free when Close() is called during row scan
	// We can implement a more sophisticated buffer release strategy if needed

	return nil
}

// releaseBufferResources is a helper to actually release the buffer when safe
func (r *FastRows) releaseBufferResources() {
	if r.buffer != nil {
		// Increase reference count before passing to PutBuffer
		// This ensures the buffer isn't freed during other operations
		C.increase_buffer_ref(r.buffer)
		PutBuffer(r.buffer)
		r.buffer = nil
	}
}

// Next advances to the next row.
func (r *FastRows) Next(dest []driver.Value) error {
	if r.closed {
		return fmt.Errorf("rows are closed")
	}

	if r.currentRow >= r.rowCount {
		// At the end of the result set, we can safely release resources
		// This helps with early cleanup in normal usage patterns
		r.releaseBufferResources()
		return io.EOF
	}

	// Extract values for each column
	for i := int32(0); i < r.columnCount && int(i) < len(dest); i++ {
		// Get column metadata
		colPtr := unsafe.Pointer(uintptr(unsafe.Pointer(r.buffer.columns)) + uintptr(i)*unsafe.Sizeof(C.column_meta_t{}))
		colInfo := (*C.column_meta_t)(colPtr)

		// Get nulls pointer for this column
		nullsPtrPtr := (*unsafe.Pointer)(unsafe.Pointer(uintptr(unsafe.Pointer(r.buffer.nulls_ptrs)) + uintptr(i)*unsafe.Sizeof(unsafe.Pointer(nil))))
		if *nullsPtrPtr == nil {
			dest[i] = nil
			continue
		}
		nullsArray := (*[1 << 30]C.int8_t)(unsafe.Pointer(*nullsPtrPtr))

		// Check if value is NULL
		if nullsArray[r.currentRow] != 0 {
			dest[i] = nil
			continue
		}

		// Get data pointer for this column
		dataPtr := (*unsafe.Pointer)(unsafe.Pointer(uintptr(unsafe.Pointer(r.buffer.data_ptrs)) + uintptr(i)*unsafe.Sizeof(unsafe.Pointer(nil))))

		// Extract value based on column type
		switch colInfo._type {
		case C.DUCKDB_TYPE_BOOLEAN:
			// Boolean values are stored as int8_t (0 or 1)
			boolArray := (*[1 << 30]C.int8_t)(unsafe.Pointer(*dataPtr))
			dest[i] = boolArray[r.currentRow] != 0

		case C.DUCKDB_TYPE_TINYINT:
			int8Array := (*[1 << 30]C.int8_t)(unsafe.Pointer(*dataPtr))
			dest[i] = int8(int8Array[r.currentRow])

		case C.DUCKDB_TYPE_SMALLINT:
			int16Array := (*[1 << 30]C.int16_t)(unsafe.Pointer(*dataPtr))
			dest[i] = int16(int16Array[r.currentRow])

		case C.DUCKDB_TYPE_INTEGER:
			int32Array := (*[1 << 30]C.int32_t)(unsafe.Pointer(*dataPtr))
			dest[i] = int32(int32Array[r.currentRow])

		case C.DUCKDB_TYPE_BIGINT:
			int64Array := (*[1 << 30]C.int64_t)(unsafe.Pointer(*dataPtr))
			dest[i] = int64(int64Array[r.currentRow])

		case C.DUCKDB_TYPE_FLOAT:
			floatArray := (*[1 << 30]C.float)(unsafe.Pointer(*dataPtr))
			dest[i] = float32(floatArray[r.currentRow])

		case C.DUCKDB_TYPE_DOUBLE:
			doubleArray := (*[1 << 30]C.double)(unsafe.Pointer(*dataPtr))
			dest[i] = float64(doubleArray[r.currentRow])

		case C.DUCKDB_TYPE_VARCHAR:
			// For strings, we have an array of offsets into the string buffer
			offsetArray := (*[1 << 30]C.int64_t)(unsafe.Pointer(*dataPtr))
			offset := offsetArray[r.currentRow]

			// Check for NULL (offset -1)
			if offset == -1 {
				dest[i] = nil
				continue
			}

			// Get string from buffer at offset
			strPtr := (*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(r.buffer.string_buffer)) + uintptr(offset)))
			dest[i] = C.GoString(strPtr)

		case C.DUCKDB_TYPE_DATE:
			// Check if we have enhanced date data
			if r.buffer.temporal_data != nil && r.buffer.temporal_data.has_date_data != 0 {
				// Get date data pointer (seconds since epoch)
				int64Array := (*[1 << 30]C.int64_t)(unsafe.Pointer(r.buffer.temporal_data.date_data))
				unix_seconds := int64(int64Array[r.currentRow])

				// Convert Unix time to Go time.Time (UTC)
				dest[i] = time.Unix(unix_seconds, 0).UTC()
			} else {
				// Fallback to string method
				offsetArray := (*[1 << 30]C.int64_t)(unsafe.Pointer(*dataPtr))
				offset := offsetArray[r.currentRow]

				if offset == -1 {
					dest[i] = nil
					continue
				}

				// Get string from buffer at offset
				strPtr := (*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(r.buffer.string_buffer)) + uintptr(offset)))
				dateStr := C.GoString(strPtr)

				// Parse date and convert to time.Time
				// Use standard date parsing logic: days since 1970-01-01
				if t, err := time.Parse("2006-01-02", dateStr); err == nil {
					// Convert to Unix time at UTC midnight
					year, month, day := t.Date()
					dest[i] = time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
				} else {
					// If parsing fails, fall back to string
					dest[i] = dateStr
				}
			}

		case C.DUCKDB_TYPE_TIMESTAMP, C.DUCKDB_TYPE_TIMESTAMP_S, C.DUCKDB_TYPE_TIMESTAMP_MS, C.DUCKDB_TYPE_TIMESTAMP_NS:
			// Check if we have enhanced timestamp data
			if r.buffer.temporal_data != nil && r.buffer.temporal_data.has_timestamp_data != 0 {
				// Get timestamp data pointers
				secondsArray := (*[1 << 30]C.int64_t)(unsafe.Pointer(r.buffer.temporal_data.timestamp_seconds))
				nanosArray := (*[1 << 30]C.int32_t)(unsafe.Pointer(r.buffer.temporal_data.timestamp_nanos))

				// Convert to Go time.Time
				unix_seconds := int64(secondsArray[r.currentRow])
				nanos := int64(nanosArray[r.currentRow])

				dest[i] = time.Unix(unix_seconds, nanos).UTC()
			} else {
				// Fallback to string method
				offsetArray := (*[1 << 30]C.int64_t)(unsafe.Pointer(*dataPtr))
				offset := offsetArray[r.currentRow]

				if offset == -1 {
					dest[i] = nil
					continue
				}

				// Get string from buffer at offset
				strPtr := (*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(r.buffer.string_buffer)) + uintptr(offset)))
				timeStr := C.GoString(strPtr)

				// Parse timestamp string to time.Time
				var t time.Time
				var err error

				// Try different timestamp formats
				for _, layout := range []string{
					"2006-01-02 15:04:05.999999",
					"2006-01-02 15:04:05",
					"2006-01-02T15:04:05.999999",
					"2006-01-02T15:04:05",
				} {
					if t, err = time.Parse(layout, timeStr); err == nil {
						// Successfully parsed
						dest[i] = t
						break
					}
				}

				// If all parsing attempts failed, fall back to string
				if err != nil {
					dest[i] = timeStr
				}
			}

		default:
			// For other types, convert to string
			// This is not optimal but handles all remaining types for now
			offsetArray := (*[1 << 30]C.int64_t)(unsafe.Pointer(*dataPtr))
			offset := offsetArray[r.currentRow]

			// Check for NULL (offset -1)
			if offset == -1 {
				dest[i] = nil
				continue
			}

			// Get string from buffer at offset
			strPtr := (*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(r.buffer.string_buffer)) + uintptr(offset)))
			dest[i] = C.GoString(strPtr)
		}
	}

	r.currentRow++
	return nil
}

// DirectQuery executes a query and returns the full result set as nested slices.
// This is primarily used for direct access patterns that don't use the sql.Rows interface.
func (conn *Connection) DirectQuery(query string, args ...driver.Value) ([][]driver.Value, []string, error) {
	var rows driver.Rows
	var err error

	// Execute query based on whether we have parameters
	if len(args) == 0 {
		rows, err = conn.FastQuery(query)
	} else {
		// Prepare and execute with parameters
		stmt, err := conn.FastPrepare(query)
		if err != nil {
			return nil, nil, err
		}
		defer stmt.Close()

		rows, _ = stmt.ExecuteFast(args...)
	}

	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	// Get column names
	columns := rows.Columns()

	// Prepare result set
	var result [][]driver.Value

	// Iterate through rows
	for {
		values := make([]driver.Value, len(columns))
		err := rows.Next(values)
		if err != nil {
			break
		}

		result = append(result, values)
	}

	return result, columns, nil
}

// FastStmtWrapper wraps a FastStmt to implement the standard driver.Stmt interface
type FastStmtWrapper struct {
	conn  *Connection
	stmt  *FastStmt
	query string
}

// Close closes the statement.
func (w *FastStmtWrapper) Close() error {
	if w.stmt != nil {
		return w.stmt.Close()
	}
	return nil
}

// NumInput returns the number of placeholder parameters.
func (w *FastStmtWrapper) NumInput() int {
	if w.stmt != nil {
		return w.stmt.paramCount
	}
	return -1 // Unknown
}

// Exec executes a query that doesn't return rows, like INSERT or UPDATE.
func (w *FastStmtWrapper) Exec(args []driver.Value) (driver.Result, error) {
	if w.stmt == nil {
		return nil, fmt.Errorf("statement is closed")
	}

	return w.stmt.ExecuteWithResult(args...)
}

// ExecContext executes a query with context that doesn't return rows.
func (w *FastStmtWrapper) ExecContext(ctx context.Context, args []driver.Value) (driver.Result, error) {
	if w.stmt == nil {
		return nil, fmt.Errorf("statement is closed")
	}

	return w.stmt.ExecContext(ctx, args)
}

// Query executes a query that may return rows, such as a SELECT.
func (w *FastStmtWrapper) Query(args []driver.Value) (driver.Rows, error) {
	if w.stmt == nil {
		return nil, fmt.Errorf("statement is closed")
	}

	return w.stmt.ExecuteFast(args...)
}

// QueryContext executes a query with context that may return rows.
func (w *FastStmtWrapper) QueryContext(ctx context.Context, args []driver.Value) (driver.Rows, error) {
	if w.stmt == nil {
		return nil, fmt.Errorf("statement is closed")
	}

	return w.stmt.QueryContext(ctx, args)
}
