// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

// This file contains the main driver implementation and shared utilities

/*
// Central definition of CGO flags - these should only be defined once in the entire package
// to avoid duplicate library linking warnings
#cgo CFLAGS: -I${SRCDIR}/include
#cgo darwin,amd64 LDFLAGS: -L${SRCDIR}/lib/darwin/amd64 -lduckdb -lstdc++
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/lib/darwin/arm64 -lduckdb -lstdc++
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/lib/linux/amd64 -lduckdb -lstdc++
#cgo linux,arm64 LDFLAGS: -L${SRCDIR}/lib/linux/arm64 -lduckdb -lstdc++
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/lib/windows/amd64 -lduckdb -lstdc++
#cgo windows,arm64 LDFLAGS: -L${SRCDIR}/lib/windows/arm64 -lduckdb -lstdc++

#include <stdlib.h>
#include <string.h>
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
	
	// BlobBufferPool holds a pool of byte slices used for BLOB data
	// Reusing these buffers significantly reduces allocations for BLOB-heavy queries
	blobBufferPool sync.Pool
	
	// StringBufferPool holds a pool of byte slices used for string conversions
	// This provides multiple buffers for string processing to reduce contention
	stringBufferPool sync.Pool
	
	// ResultSetPool holds a pool of result set wrappers
	// This allows complete reuse of result set structures across queries
	resultSetPool sync.Pool
	
	// SharedStringMap is a global intern map for string deduplication across all queries
	// This allows strings to be reused between queries, greatly reducing allocations
	// for common values like column names, repeated values, etc.
	sharedStringMap     map[string]string
	sharedStringMapLock sync.RWMutex
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

// GetBlobBuffer retrieves a byte slice for BLOB data from the pool or creates a new one.
// The minimumCapacity parameter specifies the minimum required size of the buffer.
func (p *ResultBufferPool) GetBlobBuffer(minimumCapacity int) []byte {
	// Round up capacity to the nearest power of 2 to reduce reallocation frequency
	// This is a common pattern for buffer sizing to amortize growth costs
	capacity := 1
	for capacity < minimumCapacity {
		capacity *= 2
	}
	
	// Cap at a reasonable maximum to prevent excessive memory usage
	// 16MB should be more than enough for most BLOB use cases
	const maxBlobSize = 16 * 1024 * 1024
	if capacity > maxBlobSize {
		capacity = maxBlobSize
	}
	
	// Try to get a buffer from the pool
	if buf, ok := p.blobBufferPool.Get().([]byte); ok {
		// If the buffer from pool is large enough, return it
		if cap(buf) >= minimumCapacity {
			return buf[:minimumCapacity]
		}
		// If buffer is too small, we'll create a new one and let the old one be GC'd
	}
	
	// Create a new buffer with the calculated capacity
	// Return a slice with the exact size requested, but with larger capacity
	return make([]byte, minimumCapacity, capacity)
}

// PutBlobBuffer returns a BLOB buffer to the pool for reuse.
// Only reasonably sized buffers are returned to the pool to prevent
// memory bloat with extremely large blobs that are rarely used.
func (p *ResultBufferPool) PutBlobBuffer(buf []byte) {
	if buf == nil {
		return
	}
	
	// Only keep reasonably sized buffers in the pool
	// Very small buffers aren't worth pooling, and very large ones would waste memory
	const minPoolableSize = 64      // Don't pool buffers smaller than 64 bytes
	const maxPoolableSize = 1 << 20 // Don't pool buffers larger than 1MB
	
	if cap(buf) >= minPoolableSize && cap(buf) <= maxPoolableSize {
		// Return a zero-length slice but preserve capacity
		p.blobBufferPool.Put(buf[:0])
	}
	// For buffers outside our desired size range, let them be garbage collected
}

// GetStringBuffer retrieves a byte slice for string conversions from the pool or creates a new one.
// This method is similar to GetBlobBuffer but optimized for string handling.
func (p *ResultBufferPool) GetStringBuffer(minimumCapacity int) []byte {
	// Round up capacity to the nearest power of 2 to reduce reallocation frequency
	capacity := 1024 // Start with at least 1KB for reasonable string efficiency
	for capacity < minimumCapacity {
		capacity *= 2
	}
	
	// Cap at a reasonable maximum to prevent excessive memory usage
	const maxStringSize = 8 * 1024 * 1024 // 8MB is more than enough for most strings
	if capacity > maxStringSize {
		capacity = maxStringSize
	}
	
	// Try to get a buffer from the pool
	if buf, ok := p.stringBufferPool.Get().([]byte); ok {
		// If the buffer from pool is large enough, return it
		if cap(buf) >= minimumCapacity {
			return buf[:minimumCapacity]
		}
		// If buffer is too small, we'll create a new one and let the old one be GC'd
	}
	
	// Create a new buffer with the calculated capacity
	return make([]byte, minimumCapacity, capacity)
}

// PutStringBuffer returns a string buffer to the pool for reuse.
func (p *ResultBufferPool) PutStringBuffer(buf []byte) {
	if buf == nil {
		return
	}
	
	// Only keep reasonably sized buffers in the pool
	const minPoolableSize = 64         // Don't pool buffers smaller than 64 bytes
	const maxPoolableSize = 1024 * 1024 // Don't pool buffers larger than 1MB
	
	if cap(buf) >= minPoolableSize && cap(buf) <= maxPoolableSize {
		// Return a zero-length slice but preserve capacity
		p.stringBufferPool.Put(buf[:0])
	}
	// For buffers outside our desired size range, let them be garbage collected
}

// GetSharedString attempts to find an existing string in the shared map
// or adds the new string to the map if it's not found.
// This provides cross-query string deduplication, which is especially
// beneficial for repeating values like column names, common strings, etc.
func (p *ResultBufferPool) GetSharedString(s string) string {
	// Only intern strings under reasonable size to prevent memory bloat
	if len(s) == 0 {
		return ""
	}
	
	if len(s) > 1024 {
		// For longer strings, just return the original without interning
		return s
	}
	
	// Try read-only first for better performance in the common case
	p.sharedStringMapLock.RLock()
	if cached, ok := p.sharedStringMap[s]; ok {
		p.sharedStringMapLock.RUnlock()
		return cached
	}
	p.sharedStringMapLock.RUnlock()
	
	// Not found with read lock, take write lock
	p.sharedStringMapLock.Lock()
	defer p.sharedStringMapLock.Unlock()
	
	// Double-check after getting write lock
	if cached, ok := p.sharedStringMap[s]; ok {
		return cached
	}
	
	// Add to map
	p.sharedStringMap[s] = s
	return s
}

// PeriodicCleanup should be called occasionally to prevent unbounded growth
// of the shared string map. It's safe to call this function from a goroutine.
func (p *ResultBufferPool) PeriodicCleanup() {
	// Don't clean up if map is reasonably sized
	p.sharedStringMapLock.RLock()
	size := len(p.sharedStringMap)
	p.sharedStringMapLock.RUnlock()
	
	if size < 100000 {
		return
	}
	
	// Take write lock and rebuild the map with a smaller capacity
	p.sharedStringMapLock.Lock()
	defer p.sharedStringMapLock.Unlock()
	
	// Double-check after getting write lock
	if len(p.sharedStringMap) < 100000 {
		return
	}
	
	// Keep common and shorter strings, which are more likely to be reused
	newMap := make(map[string]string, 10000)
	count := 0
	for s, v := range p.sharedStringMap {
		if len(s) <= 64 { // Prioritize keeping shorter strings
			newMap[s] = v
			count++
			if count >= 10000 {
				break
			}
		}
	}
	
	p.sharedStringMap = newMap
}

// ResultSetWrapper is a reusable wrapper around C.duckdb_result
// It allows pooling of result set resources to avoid allocations
type ResultSetWrapper struct {
	result      C.duckdb_result // The actual result structure (not a pointer to avoid extra alloc)
	isAllocated bool            // Whether the result is currently allocated/valid
}

// GetResultSetWrapper retrieves a result set wrapper from the pool or creates a new one.
func (p *ResultBufferPool) GetResultSetWrapper() *ResultSetWrapper {
	if wrapper, ok := p.resultSetPool.Get().(*ResultSetWrapper); ok {
		// Reset the wrapper for reuse, preserving the underlying memory
		if wrapper.isAllocated {
			// Clear the previous result but don't free memory
			C.memset(unsafe.Pointer(&wrapper.result), 0, C.sizeof_duckdb_result)
		}
		wrapper.isAllocated = true
		return wrapper
	}
	
	// Create a new wrapper with zero-initialized result
	wrapper := &ResultSetWrapper{
		isAllocated: true,
	}
	// No need to allocate anything - we'll use the struct directly
	return wrapper
}

// PutResultSetWrapper returns a result set wrapper to the pool for reuse.
// It ensures proper cleanup of any allocated DuckDB result.
func (p *ResultBufferPool) PutResultSetWrapper(wrapper *ResultSetWrapper) {
	if wrapper == nil {
		return
	}
	
	// Clean up any DuckDB resources, but keep our structure
	if wrapper.isAllocated {
		C.duckdb_destroy_result(&wrapper.result)
		// Zero out the struct to avoid lingering pointers
		C.memset(unsafe.Pointer(&wrapper.result), 0, C.sizeof_duckdb_result)
	}
	
	// Return the wrapper to the pool
	p.resultSetPool.Put(wrapper)
}

// Global shared buffer pool for all connections
var globalBufferPool = &ResultBufferPool{
	sharedStringMap: make(map[string]string, 10000),
}

// Driver implements the database/sql/driver.Driver interface.
type Driver struct{}

// Open opens a new connection to the database using memory database as default.
func (d *Driver) Open(name string) (driver.Conn, error) {
	if name == "" {
		name = ":memory:"
	}

	return NewConnection(name)
}