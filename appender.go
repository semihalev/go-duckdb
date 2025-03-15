package duckdb

/*
// Standard includes
#include <stdlib.h>
#include <duckdb.h>

// Version-specific compatibility code
#ifndef DUCKDB_VERSION_MAJOR
#define DUCKDB_VERSION_MAJOR 0
#endif

#ifndef DUCKDB_VERSION_MINOR
#define DUCKDB_VERSION_MINOR 0
#endif

// In DuckDB 1.2.0+, the appender API changed to use connection handle
#if (DUCKDB_VERSION_MAJOR > 1) || (DUCKDB_VERSION_MAJOR == 1 && DUCKDB_VERSION_MINOR >= 2)
#define DUCKDB_APPENDER_NEW_API 1
#else
#define DUCKDB_APPENDER_NEW_API 0
#endif

// Compatibility wrapper for creating appenders
static duckdb_state duckdb_appender_create_compat(duckdb_connection connection, 
                                               const char* schema, 
                                               const char* table, 
                                               duckdb_appender* out_appender) {
#if DUCKDB_APPENDER_NEW_API
    // Use the new API with connection for DuckDB 1.2.0+
    return duckdb_appender_create(connection, schema, table, out_appender);
#else
    // Use the old API without connection for older versions
    duckdb_database db;  // We can't easily get the db from the connection
    return duckdb_appender_create(db, schema, table, out_appender);
#endif
}
*/
import "C"
import (
	"database/sql/driver"
	"errors"
	"reflect"
	"sync"
	"unsafe"
)

// Appender provides optimized bulk-insert functionality for DuckDB
type Appender struct {
	conn      *conn
	appender  *C.duckdb_appender
	schema    string
	table     string
	closed    bool
	mu        sync.Mutex
}

// NewAppender creates a new appender for the given table
func NewAppender(conn *conn, schema, table string) (*Appender, error) {
	cSchema := C.CString(schema)
	cTable := C.CString(table)
	defer C.free(unsafe.Pointer(cSchema))
	defer C.free(unsafe.Pointer(cTable))

	var appender *C.duckdb_appender
	
	if rc := C.duckdb_appender_create_compat(conn.conn, cSchema, cTable, &appender); rc != C.DuckDBSuccess {
		return nil, conn.lastError()
	}

	return &Appender{
		conn:     conn,
		appender: appender,
		schema:   schema,
		table:    table,
	}, nil
}

// AppendRow appends a row to the appender
func (a *Appender) AppendRow(values ...interface{}) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return errors.New("appender is closed")
	}

	for i, val := range values {
		if err := a.bindValue(C.idx_t(i), val); err != nil {
			return err
		}
	}

	if rc := C.duckdb_appender_end_row(a.appender); rc != C.DuckDBSuccess {
		return errors.New("failed to end row")
	}

	return nil
}

// Flush flushes the appender
func (a *Appender) Flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return errors.New("appender is closed")
	}

	if rc := C.duckdb_appender_flush(a.appender); rc != C.DuckDBSuccess {
		return errors.New("failed to flush appender")
	}

	return nil
}

// Close closes the appender and flushes any remaining data
func (a *Appender) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return nil
	}

	a.closed = true
	
	if rc := C.duckdb_appender_close(a.appender); rc != C.DuckDBSuccess {
		return errors.New("failed to close appender")
	}

	return nil
}

// bindValue binds a value to the appender
func (a *Appender) bindValue(idx C.idx_t, value interface{}) error {
	var rc C.duckdb_state

	switch v := value.(type) {
	case nil:
		rc = C.duckdb_append_null(a.appender)
	case bool:
		boolVal := C.bool(v)
		rc = C.duckdb_append_bool(a.appender, boolVal)
	case int64:
		rc = C.duckdb_append_int64(a.appender, C.int64_t(v))
	case int:
		rc = C.duckdb_append_int64(a.appender, C.int64_t(v))
	case int32:
		rc = C.duckdb_append_int32(a.appender, C.int32_t(v))
	case float64:
		rc = C.duckdb_append_double(a.appender, C.double(v))
	case string:
		cstr := C.CString(v)
		defer C.free(unsafe.Pointer(cstr))
		rc = C.duckdb_append_varchar(a.appender, cstr)
	case []byte:
		if len(v) == 0 {
			rc = C.duckdb_append_null(a.appender)
		} else {
			rc = C.duckdb_append_blob(a.appender, unsafe.Pointer(&v[0]), C.idx_t(len(v)))
		}
	default:
		return errors.New("unsupported type: " + reflect.TypeOf(value).String())
	}

	if rc != C.DuckDBSuccess {
		return errors.New("failed to append value")
	}

	return nil
}

// AppendMultipleRows appends multiple rows at once
func (a *Appender) AppendMultipleRows(rows [][]interface{}) error {
	for _, row := range rows {
		if err := a.AppendRow(row...); err != nil {
			return err
		}
	}
	return nil
}

// AppendValues appends values from a driver.Value slice
func (a *Appender) AppendValues(values []driver.Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		return errors.New("appender is closed")
	}

	for i, val := range values {
		if err := a.bindValue(C.idx_t(i), val); err != nil {
			return err
		}
	}

	if rc := C.duckdb_appender_end_row(a.appender); rc != C.DuckDBSuccess {
		return errors.New("failed to end row")
	}

	return nil
}