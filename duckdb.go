// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
package duckdb

// This file contains the main driver implementation and shared utilities

/*
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
	"runtime"
	"sync"
	"unsafe"
)

// DuckDB type constants exported for use with PreallocateVectors
const (
	DUCKDB_TYPE_INVALID    = 0
	DUCKDB_TYPE_BOOLEAN    = 1
	DUCKDB_TYPE_TINYINT    = 2
	DUCKDB_TYPE_SMALLINT   = 3
	DUCKDB_TYPE_INTEGER    = 4
	DUCKDB_TYPE_BIGINT     = 5
	DUCKDB_TYPE_UTINYINT   = 6
	DUCKDB_TYPE_USMALLINT  = 7
	DUCKDB_TYPE_UINTEGER   = 8
	DUCKDB_TYPE_UBIGINT    = 9
	DUCKDB_TYPE_FLOAT      = 10
	DUCKDB_TYPE_DOUBLE     = 11
	DUCKDB_TYPE_TIMESTAMP  = 12
	DUCKDB_TYPE_DATE       = 13
	DUCKDB_TYPE_TIME       = 14
	DUCKDB_TYPE_VARCHAR    = 15
	DUCKDB_TYPE_BLOB       = 16
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

// SafeColumnVector provides safe pre-allocated column vector storage for result sets.
// It uses C-allocated memory to store values, avoiding CGO pointer safety issues
// and reducing allocations during query execution.
type SafeColumnVector struct {
	// Pointer to C-allocated memory to store values
	cData unsafe.Pointer
	
	// Size of the currently used elements (may be less than Capacity)
	Size int
	
	// Total capacity of allocated memory
	Capacity int
	
	// Size in bytes of a single element
	ElementSize int
	
	// DuckDB type of the column
	DuckDBType int
	
	// Track whether the vector has been manually freed
	freed bool
}

// AllocSafeVector allocates a new SafeColumnVector with the specified capacity and element size.
// The memory is allocated on the C heap to avoid CGO pointer safety issues.
func AllocSafeVector(capacity int, elementSize int, duckDBType int) *SafeColumnVector {
	if capacity <= 0 || elementSize <= 0 {
		return nil
	}
	
	// Allocate memory on the C heap
	totalSize := capacity * elementSize
	cData := C.malloc(C.size_t(totalSize))
	if cData == nil {
		return nil
	}
	
	// Zero-initialize the memory
	C.memset(cData, 0, C.size_t(totalSize))
	
	// Create the safe vector
	v := &SafeColumnVector{
		cData:       cData,
		Size:        0,
		Capacity:    capacity,
		ElementSize: elementSize,
		DuckDBType:  duckDBType,
		freed:       false,
	}
	
	// Set finalizer to ensure memory is freed when the Go object is garbage collected
	RegisterVectorFinalizer(v)
	
	return v
}

// RegisterVectorFinalizer sets a finalizer for the SafeColumnVector to ensure C memory is freed
func RegisterVectorFinalizer(v *SafeColumnVector) {
	// Add runtime finalizer to free C memory when the Go object is garbage collected
	runtime.SetFinalizer(v, func(v *SafeColumnVector) {
		v.Free()
	})
}

// Free explicitly frees the C-allocated memory.
// This should be called when the vector is no longer needed to prevent memory leaks.
func (v *SafeColumnVector) Free() {
	if v == nil || v.cData == nil || v.freed {
		return
	}
	
	// Free the C memory
	C.free(v.cData)
	v.cData = nil
	v.freed = true
}

// Reset resets the vector to be reused, keeping the allocated memory.
func (v *SafeColumnVector) Reset() {
	if v == nil || v.cData == nil || v.freed {
		return
	}
	
	// Zero out the memory
	C.memset(v.cData, 0, C.size_t(v.Capacity*v.ElementSize))
	v.Size = 0
}

// SetInt8 sets an int8 value at the specified index
func (v *SafeColumnVector) SetInt8(index int, value int8) {
	if v == nil || v.cData == nil || v.freed || index >= v.Capacity {
		return
	}
	
	// Calculate the offset for this index
	offset := uintptr(index)
	
	// Write directly to C memory
	*(*int8)(unsafe.Pointer(uintptr(v.cData) + offset)) = value
	
	// Update size if needed
	if index >= v.Size {
		v.Size = index + 1
	}
}

// GetInt8 gets an int8 value at the specified index
func (v *SafeColumnVector) GetInt8(index int) int8 {
	if v == nil || v.cData == nil || v.freed || index >= v.Size {
		return 0
	}
	
	// Calculate the offset for this index
	offset := uintptr(index)
	
	// Read directly from C memory
	return *(*int8)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetInt16 sets an int16 value at the specified index
func (v *SafeColumnVector) SetInt16(index int, value int16) {
	if v == nil || v.cData == nil || v.freed || index >= v.Capacity {
		return
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 2) // int16 is 2 bytes
	
	// Write directly to C memory
	*(*int16)(unsafe.Pointer(uintptr(v.cData) + offset)) = value
	
	// Update size if needed
	if index >= v.Size {
		v.Size = index + 1
	}
}

// GetInt16 gets an int16 value at the specified index
func (v *SafeColumnVector) GetInt16(index int) int16 {
	if v == nil || v.cData == nil || v.freed || index >= v.Size {
		return 0
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 2) // int16 is 2 bytes
	
	// Read directly from C memory
	return *(*int16)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetInt32 sets an int32 value at the specified index
func (v *SafeColumnVector) SetInt32(index int, value int32) {
	if v == nil || v.cData == nil || v.freed || index >= v.Capacity {
		return
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 4) // int32 is 4 bytes
	
	// Write directly to C memory
	*(*int32)(unsafe.Pointer(uintptr(v.cData) + offset)) = value
	
	// Update size if needed
	if index >= v.Size {
		v.Size = index + 1
	}
}

// GetInt32 gets an int32 value at the specified index
func (v *SafeColumnVector) GetInt32(index int) int32 {
	if v == nil || v.cData == nil || v.freed || index >= v.Size {
		return 0
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 4) // int32 is 4 bytes
	
	// Read directly from C memory
	return *(*int32)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetInt64 sets an int64 value at the specified index
func (v *SafeColumnVector) SetInt64(index int, value int64) {
	if v == nil || v.cData == nil || v.freed || index >= v.Capacity {
		return
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 8) // int64 is 8 bytes
	
	// Write directly to C memory
	*(*int64)(unsafe.Pointer(uintptr(v.cData) + offset)) = value
	
	// Update size if needed
	if index >= v.Size {
		v.Size = index + 1
	}
}

// GetInt64 gets an int64 value at the specified index
func (v *SafeColumnVector) GetInt64(index int) int64 {
	if v == nil || v.cData == nil || v.freed || index >= v.Size {
		return 0
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 8) // int64 is 8 bytes
	
	// Read directly from C memory
	return *(*int64)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetUint8 sets a uint8 value at the specified index
func (v *SafeColumnVector) SetUint8(index int, value uint8) {
	if v == nil || v.cData == nil || v.freed || index >= v.Capacity {
		return
	}
	
	// Calculate the offset for this index
	offset := uintptr(index)
	
	// Write directly to C memory
	*(*uint8)(unsafe.Pointer(uintptr(v.cData) + offset)) = value
	
	// Update size if needed
	if index >= v.Size {
		v.Size = index + 1
	}
}

// GetUint8 gets a uint8 value at the specified index
func (v *SafeColumnVector) GetUint8(index int) uint8 {
	if v == nil || v.cData == nil || v.freed || index >= v.Size {
		return 0
	}
	
	// Calculate the offset for this index
	offset := uintptr(index)
	
	// Read directly from C memory
	return *(*uint8)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetUint16 sets a uint16 value at the specified index
func (v *SafeColumnVector) SetUint16(index int, value uint16) {
	if v == nil || v.cData == nil || v.freed || index >= v.Capacity {
		return
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 2) // uint16 is 2 bytes
	
	// Write directly to C memory
	*(*uint16)(unsafe.Pointer(uintptr(v.cData) + offset)) = value
	
	// Update size if needed
	if index >= v.Size {
		v.Size = index + 1
	}
}

// GetUint16 gets a uint16 value at the specified index
func (v *SafeColumnVector) GetUint16(index int) uint16 {
	if v == nil || v.cData == nil || v.freed || index >= v.Size {
		return 0
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 2) // uint16 is 2 bytes
	
	// Read directly from C memory
	return *(*uint16)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetUint32 sets a uint32 value at the specified index
func (v *SafeColumnVector) SetUint32(index int, value uint32) {
	if v == nil || v.cData == nil || v.freed || index >= v.Capacity {
		return
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 4) // uint32 is 4 bytes
	
	// Write directly to C memory
	*(*uint32)(unsafe.Pointer(uintptr(v.cData) + offset)) = value
	
	// Update size if needed
	if index >= v.Size {
		v.Size = index + 1
	}
}

// GetUint32 gets a uint32 value at the specified index
func (v *SafeColumnVector) GetUint32(index int) uint32 {
	if v == nil || v.cData == nil || v.freed || index >= v.Size {
		return 0
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 4) // uint32 is 4 bytes
	
	// Read directly from C memory
	return *(*uint32)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetUint64 sets a uint64 value at the specified index
func (v *SafeColumnVector) SetUint64(index int, value uint64) {
	if v == nil || v.cData == nil || v.freed || index >= v.Capacity {
		return
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 8) // uint64 is 8 bytes
	
	// Write directly to C memory
	*(*uint64)(unsafe.Pointer(uintptr(v.cData) + offset)) = value
	
	// Update size if needed
	if index >= v.Size {
		v.Size = index + 1
	}
}

// GetUint64 gets a uint64 value at the specified index
func (v *SafeColumnVector) GetUint64(index int) uint64 {
	if v == nil || v.cData == nil || v.freed || index >= v.Size {
		return 0
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 8) // uint64 is 8 bytes
	
	// Read directly from C memory
	return *(*uint64)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetFloat32 sets a float32 value at the specified index
func (v *SafeColumnVector) SetFloat32(index int, value float32) {
	if v == nil || v.cData == nil || v.freed || index >= v.Capacity {
		return
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 4) // float32 is 4 bytes
	
	// Write directly to C memory
	*(*float32)(unsafe.Pointer(uintptr(v.cData) + offset)) = value
	
	// Update size if needed
	if index >= v.Size {
		v.Size = index + 1
	}
}

// GetFloat32 gets a float32 value at the specified index
func (v *SafeColumnVector) GetFloat32(index int) float32 {
	if v == nil || v.cData == nil || v.freed || index >= v.Size {
		return 0
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 4) // float32 is 4 bytes
	
	// Read directly from C memory
	return *(*float32)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetFloat64 sets a float64 value at the specified index
func (v *SafeColumnVector) SetFloat64(index int, value float64) {
	if v == nil || v.cData == nil || v.freed || index >= v.Capacity {
		return
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 8) // float64 is 8 bytes
	
	// Write directly to C memory
	*(*float64)(unsafe.Pointer(uintptr(v.cData) + offset)) = value
	
	// Update size if needed
	if index >= v.Size {
		v.Size = index + 1
	}
}

// GetFloat64 gets a float64 value at the specified index
func (v *SafeColumnVector) GetFloat64(index int) float64 {
	if v == nil || v.cData == nil || v.freed || index >= v.Size {
		return 0
	}
	
	// Calculate the offset for this index
	offset := uintptr(index * 8) // float64 is 8 bytes
	
	// Read directly from C memory
	return *(*float64)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetBool sets a bool value at the specified index
func (v *SafeColumnVector) SetBool(index int, value bool) {
	if v == nil || v.cData == nil || v.freed || index >= v.Capacity {
		return
	}
	
	// Convert bool to byte (0 or 1)
	var byteVal byte
	if value {
		byteVal = 1
	}
	
	// Calculate the offset for this index
	offset := uintptr(index)
	
	// Write directly to C memory
	*(*byte)(unsafe.Pointer(uintptr(v.cData) + offset)) = byteVal
	
	// Update size if needed
	if index >= v.Size {
		v.Size = index + 1
	}
}

// GetBool gets a bool value at the specified index
func (v *SafeColumnVector) GetBool(index int) bool {
	if v == nil || v.cData == nil || v.freed || index >= v.Size {
		return false
	}
	
	// Calculate the offset for this index
	offset := uintptr(index)
	
	// Read directly from C memory
	byteVal := *(*byte)(unsafe.Pointer(uintptr(v.cData) + offset))
	
	// Convert byte to bool
	return byteVal != 0
}

// Store vectors by column index and DuckDB type
var vectorsByType = make(map[int]map[int]*SafeColumnVector)
var vectorsLock sync.RWMutex

// PreallocateVectors pre-allocates column vectors for specific types and sizes.
// This reduces allocations during query execution.
// Parameters:
//   - rowCapacity: number of rows to allocate space for
//   - types: array of DuckDB column types
//   - sizes: array of element sizes for each type
func PreallocateVectors(rowCapacity int, types []int, sizes []int) {
	if rowCapacity <= 0 || len(types) != len(sizes) || len(types) == 0 {
		return
	}
	
	vectorsLock.Lock()
	defer vectorsLock.Unlock()
	
	// Allocate vectors for each type
	for i, duckDBType := range types {
		elementSize := sizes[i]
		if elementSize <= 0 {
			continue
		}
		
		// Create map for this type if it doesn't exist
		if vectorsByType[duckDBType] == nil {
			vectorsByType[duckDBType] = make(map[int]*SafeColumnVector)
		}
		
		// Either create a new vector or reset existing one
		vector := vectorsByType[duckDBType][elementSize]
		if vector == nil {
			// Allocate with some extra capacity to reduce future allocations
			effectiveCapacity := rowCapacity + 50
			vector = AllocSafeVector(effectiveCapacity, elementSize, duckDBType)
			if vector != nil {
				vectorsByType[duckDBType][elementSize] = vector
			}
		} else {
			// Reset existing vector for reuse
			vector.Reset()
		}
	}
	
	// Also pre-allocate for standard sizes to cover common cases
	standardSizes := map[int]int{
		DUCKDB_TYPE_BOOLEAN:  1,  // bool is 1 byte
		DUCKDB_TYPE_TINYINT:  1,  // int8 is 1 byte
		DUCKDB_TYPE_SMALLINT: 2,  // int16 is 2 bytes  
		DUCKDB_TYPE_INTEGER:  4,  // int32 is 4 bytes
		DUCKDB_TYPE_BIGINT:   8,  // int64 is 8 bytes
		DUCKDB_TYPE_FLOAT:    4,  // float32 is 4 bytes
		DUCKDB_TYPE_DOUBLE:   8,  // float64 is 8 bytes
	}
	
	// Ensure we have vectors for standard sizes
	for duckDBType, elementSize := range standardSizes {
		if vectorsByType[duckDBType] == nil {
			vectorsByType[duckDBType] = make(map[int]*SafeColumnVector)
		}
		
		if vectorsByType[duckDBType][elementSize] == nil {
			effectiveCapacity := rowCapacity + 50
			vector := AllocSafeVector(effectiveCapacity, elementSize, duckDBType)
			if vector != nil {
				vectorsByType[duckDBType][elementSize] = vector
			}
		} else {
			// Reset for reuse
			vectorsByType[duckDBType][elementSize].Reset()
		}
	}
}

// GetPreallocatedVector retrieves a pre-allocated vector for a specific type and element size.
// Returns nil if no vector is available.
func GetPreallocatedVector(duckDBType int, elementSize int) *SafeColumnVector {
	vectorsLock.RLock()
	defer vectorsLock.RUnlock()
	
	// Check if we have a vector for this type and size
	if typeMap, ok := vectorsByType[duckDBType]; ok {
		if vector, ok := typeMap[elementSize]; ok {
			return vector
		}
	}
	
	return nil
}

// storeInSafeVector stores a value in a safe column vector.
// This is used by the rows.Next method to store values in pre-allocated vectors.
func storeInSafeVector(safeVector *SafeColumnVector, rowIdx int, val interface{}) {
	if safeVector == nil || rowIdx < 0 {
		return
	}
	
	// Store value based on its type
	switch v := val.(type) {
	case int8:
		safeVector.SetInt8(rowIdx, v)
	case int16:
		safeVector.SetInt16(rowIdx, v)
	case int32:
		safeVector.SetInt32(rowIdx, v)
	case int64:
		safeVector.SetInt64(rowIdx, v)
	case uint8:
		safeVector.SetUint8(rowIdx, v)
	case uint16:
		safeVector.SetUint16(rowIdx, v)
	case uint32:
		safeVector.SetUint32(rowIdx, v)
	case uint64:
		safeVector.SetUint64(rowIdx, v)
	case float32:
		safeVector.SetFloat32(rowIdx, v)
	case float64:
		safeVector.SetFloat64(rowIdx, v)
	case bool:
		safeVector.SetBool(rowIdx, v)
	}
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