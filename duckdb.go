// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

/*
#cgo CFLAGS: -I${SRCDIR}/include
#cgo darwin,amd64 LDFLAGS: -L${SRCDIR}/lib/darwin/amd64 -lduckdb -lstdc++
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/lib/darwin/arm64 -lduckdb -lstdc++
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/lib/linux/amd64 -lduckdb -lstdc++
#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/lib/linux/arm64 -lduckdb -lstdc++
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/lib/windows/amd64 -lduckdb -lstdc++
#cgo windows,arm64 LDFLAGS: -L${SRCDIR}/lib/windows/arm64 -lduckdb -lstdc++

#include <stdlib.h>
#include <duckdb.h>
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"sync"
	"unsafe"
)

func init() {
	sql.Register("duckdb", &Driver{})
}

// Utility functions for string conversions
func cString(s string) *C.char {
	return C.CString(s)
}

func freeString(s *C.char) {
	C.free(unsafe.Pointer(s))
}

func goString(s *C.char) string {
	return C.GoString(s)
}

// Convert Go bool to int8 for use with DuckDB C API
func boolToInt8(b bool) C.int8_t {
	if b {
		return C.int8_t(1)
	}
	return C.int8_t(0)
}

// Convert DuckDB C API bool result to Go bool
func cBoolToGo(b C.bool) bool {
	// Use unsafe pointer to reinterpret without direct conversion
	// Since Go can't directly convert between C bool and Go bool
	ptr := unsafe.Pointer(&b)
	// Any non-zero value is considered true
	return *(*C.char)(ptr) != 0
}

// ResultBufferPool provides a pooled set of resources for query results
// to minimize allocations during query execution.
type ResultBufferPool struct {
	// StringCachePool holds a pool of string caches that can be reused
	// This significantly reduces allocations for string-heavy queries
	stringCachePool sync.Pool

	// ColumnNamesPool holds a pool of slices used for column names
	// Reusing these slices reduces allocations for repeated queries
	columnNamesPool sync.Pool
	
	// ColumnTypesPool holds a pool of slices used for column types
	// Reusing these slices reduces allocations for repeated queries
	columnTypesPool sync.Pool
	
	// NamedArgsPool holds a pool of slices used for parameter binding
	// Reusing these slices reduces allocations for prepared statements
	namedArgsPool sync.Pool
}

// GetStringCache retrieves a StringCache from the pool or creates a new one.
// The initialCapacity parameter specifies the initial number of columns.
func (p *ResultBufferPool) GetStringCache(initialCapacity int) *StringCache {
	if cache, ok := p.stringCachePool.Get().(*StringCache); ok {
		// Ensure the cache has sufficient capacity
		if len(cache.columnValues) < initialCapacity {
			cache.columnValues = make([]string, initialCapacity)
		}
		return cache
	}
	
	// Create new cache if none in pool
	return NewStringCache(initialCapacity)
}

// PutStringCache returns a StringCache to the pool for reuse.
func (p *ResultBufferPool) PutStringCache(cache *StringCache) {
	if cache == nil {
		return
	}
	
	// Reset the cache to prevent holding onto too much memory
	if len(cache.internMap) > 1000 {
		cache.Reset()
	}
	
	p.stringCachePool.Put(cache)
}

// GetColumnNamesBuffer retrieves a slice for column names from the pool or creates a new one.
func (p *ResultBufferPool) GetColumnNamesBuffer(capacity int) []string {
	if buf, ok := p.columnNamesPool.Get().([]string); ok {
		if cap(buf) >= capacity {
			return buf[:capacity]
		}
	}
	
	// Create new buffer if none in pool or too small
	return make([]string, capacity)
}

// PutColumnNamesBuffer returns a column names buffer to the pool for reuse.
func (p *ResultBufferPool) PutColumnNamesBuffer(buf []string) {
	if buf != nil && cap(buf) > 0 {
		p.columnNamesPool.Put(buf[:0]) // Clear slice but keep capacity
	}
}

// GetColumnTypesBuffer retrieves a slice for column types from the pool or creates a new one.
func (p *ResultBufferPool) GetColumnTypesBuffer(capacity int) []C.duckdb_type {
	if buf, ok := p.columnTypesPool.Get().([]C.duckdb_type); ok {
		if cap(buf) >= capacity {
			return buf[:capacity]
		}
	}
	
	// Create new buffer if none in pool or too small
	return make([]C.duckdb_type, capacity)
}

// PutColumnTypesBuffer returns a column types buffer to the pool for reuse.
func (p *ResultBufferPool) PutColumnTypesBuffer(buf []C.duckdb_type) {
	if buf != nil && cap(buf) > 0 {
		p.columnTypesPool.Put(buf[:0]) // Clear slice but keep capacity
	}
}

// GetNamedArgsBuffer retrieves a slice for named arguments from the pool or creates a new one.
func (p *ResultBufferPool) GetNamedArgsBuffer(capacity int) []driver.NamedValue {
	if buf, ok := p.namedArgsPool.Get().([]driver.NamedValue); ok {
		if cap(buf) >= capacity {
			return buf[:capacity]
		}
	}
	
	// Create new buffer if none in pool or too small
	return make([]driver.NamedValue, capacity)
}

// PutNamedArgsBuffer returns a named arguments buffer to the pool for reuse.
func (p *ResultBufferPool) PutNamedArgsBuffer(buf []driver.NamedValue) {
	if buf != nil && cap(buf) > 0 {
		p.namedArgsPool.Put(buf[:0]) // Clear slice but keep capacity
	}
}

// Global shared buffer pool for all connections
var globalBufferPool = &ResultBufferPool{}

// Driver implements the database/sql/driver.Driver interface.
type Driver struct{}

// Open opens a new connection to the database using memory database as default.
func (d *Driver) Open(name string) (driver.Conn, error) {
	if name == "" {
		name = ":memory:"
	}

	return NewConnection(name)
}