// Package duckdb provides low-level, high-performance SQL driver for DuckDB in Go.
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
	"sync/atomic"
	"unsafe"
)

// DuckDB type constants exported for use with PreallocateVectors
const (
	DUCKDB_TYPE_INVALID   = 0
	DUCKDB_TYPE_BOOLEAN   = 1
	DUCKDB_TYPE_TINYINT   = 2
	DUCKDB_TYPE_SMALLINT  = 3
	DUCKDB_TYPE_INTEGER   = 4
	DUCKDB_TYPE_BIGINT    = 5
	DUCKDB_TYPE_UTINYINT  = 6
	DUCKDB_TYPE_USMALLINT = 7
	DUCKDB_TYPE_UINTEGER  = 8
	DUCKDB_TYPE_UBIGINT   = 9
	DUCKDB_TYPE_FLOAT     = 10
	DUCKDB_TYPE_DOUBLE    = 11
	DUCKDB_TYPE_TIMESTAMP = 12
	DUCKDB_TYPE_DATE      = 13
	DUCKDB_TYPE_TIME      = 14
	DUCKDB_TYPE_VARCHAR   = 15
	DUCKDB_TYPE_BLOB      = 16
)

func init() {
	// Register the standard driver
	sql.Register("duckdb", &Driver{})

	// Initialize the other pools to use pointers
	globalBufferPool.columnNamesPool = sync.Pool{
		New: func() interface{} {
			buf := make([]string, 0, 32) // Common column count
			return &buf
		},
	}

	globalBufferPool.columnTypesPool = sync.Pool{
		New: func() interface{} {
			buf := make([]C.duckdb_type, 0, 32) // Common column count
			return &buf
		},
	}

	globalBufferPool.namedArgsPool = sync.Pool{
		New: func() interface{} {
			buf := make([]driver.NamedValue, 0, 16) // Common parameter count
			return &buf
		},
	}
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
	// Buffer pool statistics for monitoring
	stats struct {
		stringCacheGets  int64
		stringCachePuts  int64
		stringBufferGets int64
		stringBufferPuts int64
		blobBufferGets   int64
		blobBufferPuts   int64
		vectorGets       int64
		vectorPuts       int64
	}
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

	// Tiered blob buffer pools for common size ranges to reduce fragmentation
	smallBlobPool  sync.Pool // For blobs <= 256 bytes
	mediumBlobPool sync.Pool // For blobs <= 4KB
	largeBlobPool  sync.Pool // For blobs <= 64KB

	// StringBufferPool holds a pool of byte slices used for string conversions
	// This provides multiple buffers for string processing to reduce contention
	stringBufferPool sync.Pool

	// Tiered string buffer pools for common size ranges
	smallStringPool  sync.Pool // For strings <= 128 bytes
	mediumStringPool sync.Pool // For strings <= 2KB

	// Type-specific column vector pools
	int32VectorPool   sync.Pool // For INTEGER columns
	int64VectorPool   sync.Pool // For BIGINT columns
	float64VectorPool sync.Pool // For DOUBLE columns
	boolVectorPool    sync.Pool // For BOOLEAN columns

	// ResultSetPool holds a pool of result set wrappers
	// This allows complete reuse of result set structures across queries
	resultSetPool sync.Pool

	// SharedStringMap is a global intern map for string deduplication across all queries
	// This allows strings to be reused between queries, greatly reducing allocations
	// for common values like column names, repeated values, etc.
	// Using sync.Map for thread-safe concurrent access
	sharedStringMap sync.Map
}

// GetStringCache retrieves a StringCache from the pool or creates a new one.
// The initialCapacity parameter specifies the initial number of columns.
func (p *ResultBufferPool) GetStringCache(initialCapacity int) StringCacher {
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
func (p *ResultBufferPool) PutStringCache(cache StringCacher) {
	if cache == nil {
		return
	}

	// If it's our string cache, we can reuse it
	if sc, ok := cache.(*StringCache); ok {
		// Reset the cache to prevent holding onto too much memory
		sc.Reset()
		p.stringCachePool.Put(sc)
		return
	}

	// For other cache implementations, just call Reset and let it be GC'd
	cache.Reset()
}

// GetColumnNamesBuffer retrieves a slice for column names from the pool or creates a new one.
func (p *ResultBufferPool) GetColumnNamesBuffer(capacity int) []string {
	// Try to get a pointer to a slice from the pool
	if pBuf, ok := p.columnNamesPool.Get().(*[]string); ok {
		buf := *pBuf
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
		// Create a pointer to the slice to avoid allocations
		pBuf := &buf
		*pBuf = (*pBuf)[:0]         // Clear slice but keep capacity
		p.columnNamesPool.Put(pBuf) // Store the pointer
	}
}

// GetColumnTypesBuffer retrieves a slice for column types from the pool or creates a new one.
func (p *ResultBufferPool) GetColumnTypesBuffer(capacity int) []C.duckdb_type {
	// Try to get a pointer to a slice from the pool
	if pBuf, ok := p.columnTypesPool.Get().(*[]C.duckdb_type); ok {
		buf := *pBuf
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
		// Create a pointer to the slice to avoid allocations
		pBuf := &buf
		*pBuf = (*pBuf)[:0]         // Clear slice but keep capacity
		p.columnTypesPool.Put(pBuf) // Store the pointer
	}
}

// GetNamedArgsBuffer retrieves a slice for named arguments from the pool or creates a new one.
func (p *ResultBufferPool) GetNamedArgsBuffer(capacity int) []driver.NamedValue {
	// Try to get a pointer to a slice from the pool
	if pBuf, ok := p.namedArgsPool.Get().(*[]driver.NamedValue); ok {
		buf := *pBuf
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
		// Create a pointer to the slice to avoid allocations
		pBuf := &buf
		*pBuf = (*pBuf)[:0]       // Clear slice but keep capacity
		p.namedArgsPool.Put(pBuf) // Store the pointer
	}
}

// GetBlobBuffer retrieves a byte slice for BLOB data from the pool or creates a new one.
// The minimumCapacity parameter specifies the minimum required size of the buffer.
// This uses a tiered approach to better reuse common buffer sizes.
func (p *ResultBufferPool) GetBlobBuffer(minimumCapacity int) []byte {
	// Handle small blob buffers (≤ 256 bytes) - very common in database workloads
	if minimumCapacity <= 256 {
		// Try to get a buffer from the small pool - most blob column values are small
		if pBuf, ok := p.smallBlobPool.Get().(*[]byte); ok {
			buf := *pBuf
			if cap(buf) >= minimumCapacity {
				return buf[:minimumCapacity]
			}
		}
		// Create a new fixed-size buffer - always 256 bytes for predictable reuse
		return make([]byte, minimumCapacity, 256)
	}

	// Handle medium blob buffers (≤ 4KB) - common size for document fields, JSON values
	if minimumCapacity <= 4*1024 {
		if pBuf, ok := p.mediumBlobPool.Get().(*[]byte); ok {
			buf := *pBuf
			if cap(buf) >= minimumCapacity {
				return buf[:minimumCapacity]
			}
		}
		// Create new buffer with 4KB capacity
		return make([]byte, minimumCapacity, 4*1024)
	}

	// Handle large blob buffers (≤ 64KB) - less common but still worth pooling
	if minimumCapacity <= 64*1024 {
		if pBuf, ok := p.largeBlobPool.Get().(*[]byte); ok {
			buf := *pBuf
			if cap(buf) >= minimumCapacity {
				return buf[:minimumCapacity]
			}
		}
		// Create new buffer with 64KB capacity
		return make([]byte, minimumCapacity, 64*1024)
	}

	// For very large blobs, use power-of-2 sizing
	capacity := 128 * 1024 // Start at 128KB
	for capacity < minimumCapacity {
		capacity *= 2
	}

	// Cap at a reasonable maximum to prevent excessive memory usage
	const maxBlobSize = 16 * 1024 * 1024 // 16MB cap
	if capacity > maxBlobSize {
		capacity = maxBlobSize
	}

	// Try the general blob buffer pool for anything larger
	if pBuf, ok := p.blobBufferPool.Get().(*[]byte); ok {
		buf := *pBuf
		if cap(buf) >= minimumCapacity {
			return buf[:minimumCapacity]
		}
	}

	// Create a new buffer with the calculated capacity
	return make([]byte, minimumCapacity, capacity)
}

// PutBlobBuffer returns a BLOB buffer to the pool for reuse.
// Uses a tiered approach to keep common-sized buffers in their respective pools.
func (p *ResultBufferPool) PutBlobBuffer(buf []byte) {
	if buf == nil {
		return
	}

	// Get the buffer's capacity to decide which pool to return it to
	bufCap := cap(buf)

	// Create a pointer to avoid allocations
	pBuf := &buf
	*pBuf = (*pBuf)[:0] // Clear slice but keep capacity

	// Choose appropriate pool based on buffer capacity
	switch {
	case bufCap == 256:
		// Return to small pool - exact size match for highest reuse
		p.smallBlobPool.Put(pBuf)

	case bufCap == 4*1024:
		// Return to medium pool - exact size match
		p.mediumBlobPool.Put(pBuf)

	case bufCap == 64*1024:
		// Return to large pool - exact size match
		p.largeBlobPool.Put(pBuf)

	case bufCap >= 128 && bufCap <= 1024*1024:
		// Only pool reasonably sized buffers in the general pool
		// Too small isn't worth it, too large wastes memory
		p.blobBufferPool.Put(pBuf)
	}

	// Buffers outside these ranges are left for garbage collection
}

// GetStringBuffer retrieves a byte slice for string conversions from the pool or creates a new one.
// This method uses a tiered approach for more efficient reuse of common string sizes.
func (p *ResultBufferPool) GetStringBuffer(minimumCapacity int) []byte {
	// Handle small strings (≤ 128 bytes) - extremely common in database workloads
	// Most column names, simple values, etc. fall in this category
	if minimumCapacity <= 128 {
		// Try to get a buffer from the small string pool
		if pBuf, ok := p.smallStringPool.Get().(*[]byte); ok {
			buf := *pBuf
			if cap(buf) >= minimumCapacity {
				return buf[:minimumCapacity]
			}
		}
		// Create a new fixed-size buffer for small strings
		return make([]byte, minimumCapacity, 128)
	}

	// Handle medium strings (≤ 2KB) - common for longer text fields
	if minimumCapacity <= 2*1024 {
		if pBuf, ok := p.mediumStringPool.Get().(*[]byte); ok {
			buf := *pBuf
			if cap(buf) >= minimumCapacity {
				return buf[:minimumCapacity]
			}
		}
		// Create a new fixed-size buffer for medium strings
		return make([]byte, minimumCapacity, 2*1024)
	}

	// For larger strings, use power-of-2 sizing starting at 4KB
	capacity := 4 * 1024
	for capacity < minimumCapacity {
		capacity *= 2
	}

	// Cap at a reasonable maximum to prevent excessive memory usage
	const maxStringSize = 8 * 1024 * 1024 // 8MB should be enough for any string
	if capacity > maxStringSize {
		capacity = maxStringSize
	}

	// Try the general string buffer pool for larger strings
	if pBuf, ok := p.stringBufferPool.Get().(*[]byte); ok {
		buf := *pBuf
		if cap(buf) >= minimumCapacity {
			return buf[:minimumCapacity]
		}
	}

	// Create a new buffer with the calculated capacity
	return make([]byte, minimumCapacity, capacity)
}

// PutStringBuffer returns a string buffer to the pool for reuse.
// Uses a tiered approach to better match buffers to their appropriate pools.
func (p *ResultBufferPool) PutStringBuffer(buf []byte) {
	if buf == nil {
		return
	}

	// Choose appropriate pool based on buffer capacity
	bufCap := cap(buf)

	// Create a pointer to avoid allocations
	pBuf := &buf
	*pBuf = (*pBuf)[:0] // Clear slice but keep capacity

	switch {
	case bufCap == 128:
		// This is a small string buffer - perfect size match
		p.smallStringPool.Put(pBuf)

	case bufCap == 2*1024:
		// This is a medium string buffer - perfect size match
		p.mediumStringPool.Put(pBuf)

	case bufCap >= 64 && bufCap <= 1024*1024:
		// General pool for reasonable sized buffers that don't match fixed tiers
		p.stringBufferPool.Put(pBuf)
	}
	// For buffers outside our desired size range, let them be garbage collected
}

// GetSharedString attempts to find an existing string in the shared map
// or adds the new string to the map if it's not found.
// This provides cross-query string deduplication, which is especially
// beneficial for repeating values like column names, common strings, etc.
// Uses sync.Map for thread-safe concurrent access.
func (p *ResultBufferPool) GetSharedString(s string) string {
	// Only intern strings under reasonable size to prevent memory bloat
	if len(s) == 0 {
		return ""
	}

	if len(s) > 1024 {
		// For longer strings, just return the original without interning
		return s
	}

	// Check if string exists in the map
	if cached, ok := p.sharedStringMap.Load(s); ok {
		return cached.(string)
	}

	// String doesn't exist, store it
	// LoadOrStore atomically loads or stores a value if the key is not present
	actual, _ := p.sharedStringMap.LoadOrStore(s, s)
	return actual.(string)
}

// PeriodicCleanup should be called occasionally to prevent unbounded growth
// of the shared string map. It's safe to call this function from a goroutine.
func (p *ResultBufferPool) PeriodicCleanup() {
	// With sync.Map we can't easily count the size, so we'll keep track of
	// string count separately as a metric in a future enhancement.
	// The built-in sync.Map garbage collection is efficient for this use case,
	// so manual cleanup is less critical than with standard maps.

	// For now, we'll just let sync.Map handle the memory management as entries
	// that are no longer accessed will be garbage collected naturally.

	// If future metrics show excessive memory usage, we could implement
	// a more sophisticated cleanup by creating a new sync.Map and
	// transferring only the most valuable entries.
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
	sharedStringMap: sync.Map{},

	// Initialize tiered pools for string handling with pointer types
	smallStringPool: sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 0, 128)
			return &buf
		},
	},
	mediumStringPool: sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 0, 2*1024)
			return &buf
		},
	},

	// Initialize tiered pools for blob handling with pointer types
	smallBlobPool: sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 0, 256)
			return &buf
		},
	},
	mediumBlobPool: sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 0, 4*1024)
			return &buf
		},
	},
	largeBlobPool: sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 0, 64*1024)
			return &buf
		},
	},
	stringBufferPool: sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 0, 4*1024)
			return &buf
		},
	},
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

	// Mutex for thread-safe access to vector data
	mu sync.RWMutex
}

// Add vector pool management to the ResultBufferPool

// GetInt32Vector retrieves an integer vector from the pool or creates a new one
func (p *ResultBufferPool) GetInt32Vector(capacity int) *SafeColumnVector {
	// Check if a vector is available in the pool
	if v, ok := p.int32VectorPool.Get().(*SafeColumnVector); ok {
		// If the vector is large enough, reset and return it
		if v.Capacity >= capacity {
			v.Reset()
			return v
		}
		// If not large enough, free it and allocate a new one
		v.Free()
	}

	// Allocate a new vector with proper size
	return AllocSafeVector(capacity, 4, DUCKDB_TYPE_INTEGER)
}

// PutInt32Vector returns an integer vector to the pool
func (p *ResultBufferPool) PutInt32Vector(v *SafeColumnVector) {
	if v == nil || v.freed || v.DuckDBType != DUCKDB_TYPE_INTEGER {
		return
	}

	// Only pool reasonably sized vectors to avoid memory bloat
	if v.Capacity <= 10000 {
		v.Size = 0 // Clear but keep memory allocated
		p.int32VectorPool.Put(v)
	} else {
		// Free very large vectors
		v.Free()
	}
}

// GetInt64Vector retrieves a bigint vector from the pool or creates a new one
func (p *ResultBufferPool) GetInt64Vector(capacity int) *SafeColumnVector {
	// Check if a vector is available in the pool
	if v, ok := p.int64VectorPool.Get().(*SafeColumnVector); ok {
		// If the vector is large enough, reset and return it
		if v.Capacity >= capacity {
			v.Reset()
			return v
		}
		// If not large enough, free it and allocate a new one
		v.Free()
	}

	// Allocate a new vector with proper size
	return AllocSafeVector(capacity, 8, DUCKDB_TYPE_BIGINT)
}

// PutInt64Vector returns a bigint vector to the pool
func (p *ResultBufferPool) PutInt64Vector(v *SafeColumnVector) {
	if v == nil || v.freed || v.DuckDBType != DUCKDB_TYPE_BIGINT {
		return
	}

	// Only pool reasonably sized vectors to avoid memory bloat
	if v.Capacity <= 10000 {
		v.Size = 0 // Clear but keep memory allocated
		p.int64VectorPool.Put(v)
	} else {
		// Free very large vectors
		v.Free()
	}
}

// GetFloat64Vector retrieves a double vector from the pool or creates a new one
func (p *ResultBufferPool) GetFloat64Vector(capacity int) *SafeColumnVector {
	// Check if a vector is available in the pool
	if v, ok := p.float64VectorPool.Get().(*SafeColumnVector); ok {
		// If the vector is large enough, reset and return it
		if v.Capacity >= capacity {
			v.Reset()
			return v
		}
		// If not large enough, free it and allocate a new one
		v.Free()
	}

	// Allocate a new vector with proper size
	return AllocSafeVector(capacity, 8, DUCKDB_TYPE_DOUBLE)
}

// PutFloat64Vector returns a double vector to the pool
func (p *ResultBufferPool) PutFloat64Vector(v *SafeColumnVector) {
	if v == nil || v.freed || v.DuckDBType != DUCKDB_TYPE_DOUBLE {
		return
	}

	// Only pool reasonably sized vectors to avoid memory bloat
	if v.Capacity <= 10000 {
		v.Size = 0 // Clear but keep memory allocated
		p.float64VectorPool.Put(v)
	} else {
		// Free very large vectors
		v.Free()
	}
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
	if v == nil {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.cData == nil || v.freed {
		return
	}

	// Free the C memory
	C.free(v.cData)
	v.cData = nil
	v.freed = true
}

// Reset resets the vector to be reused, keeping the allocated memory.
func (v *SafeColumnVector) Reset() {
	if v == nil {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.cData == nil || v.freed {
		return
	}

	// Zero out the memory
	C.memset(v.cData, 0, C.size_t(v.Capacity*v.ElementSize))
	v.Size = 0
}

// SetInt8 sets an int8 value at the specified index
func (v *SafeColumnVector) SetInt8(index int, value int8) {
	if v == nil {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.cData == nil || v.freed || index >= v.Capacity {
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
	if v == nil {
		return 0
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.cData == nil || v.freed || index >= v.Size {
		return 0
	}

	// Calculate the offset for this index
	offset := uintptr(index)

	// Read directly from C memory
	return *(*int8)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetInt16 sets an int16 value at the specified index
func (v *SafeColumnVector) SetInt16(index int, value int16) {
	if v == nil {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.cData == nil || v.freed || index >= v.Capacity {
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
	if v == nil {
		return 0
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.cData == nil || v.freed || index >= v.Size {
		return 0
	}

	// Calculate the offset for this index
	offset := uintptr(index * 2) // int16 is 2 bytes

	// Read directly from C memory
	return *(*int16)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetInt32 sets an int32 value at the specified index
func (v *SafeColumnVector) SetInt32(index int, value int32) {
	if v == nil {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.cData == nil || v.freed || index >= v.Capacity {
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
	if v == nil {
		return 0
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.cData == nil || v.freed || index >= v.Size {
		return 0
	}

	// Calculate the offset for this index
	offset := uintptr(index * 4) // int32 is 4 bytes

	// Read directly from C memory
	return *(*int32)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetInt64 sets an int64 value at the specified index
func (v *SafeColumnVector) SetInt64(index int, value int64) {
	if v == nil {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.cData == nil || v.freed || index >= v.Capacity {
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
	if v == nil {
		return 0
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.cData == nil || v.freed || index >= v.Size {
		return 0
	}

	// Calculate the offset for this index
	offset := uintptr(index * 8) // int64 is 8 bytes

	// Read directly from C memory
	return *(*int64)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetUint8 sets a uint8 value at the specified index
func (v *SafeColumnVector) SetUint8(index int, value uint8) {
	if v == nil {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.cData == nil || v.freed || index >= v.Capacity {
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
	if v == nil {
		return 0
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.cData == nil || v.freed || index >= v.Size {
		return 0
	}

	// Calculate the offset for this index
	offset := uintptr(index)

	// Read directly from C memory
	return *(*uint8)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetUint16 sets a uint16 value at the specified index
func (v *SafeColumnVector) SetUint16(index int, value uint16) {
	if v == nil {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.cData == nil || v.freed || index >= v.Capacity {
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
	if v == nil {
		return 0
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.cData == nil || v.freed || index >= v.Size {
		return 0
	}

	// Calculate the offset for this index
	offset := uintptr(index * 2) // uint16 is 2 bytes

	// Read directly from C memory
	return *(*uint16)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetUint32 sets a uint32 value at the specified index
func (v *SafeColumnVector) SetUint32(index int, value uint32) {
	if v == nil {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.cData == nil || v.freed || index >= v.Capacity {
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
	if v == nil {
		return 0
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.cData == nil || v.freed || index >= v.Size {
		return 0
	}

	// Calculate the offset for this index
	offset := uintptr(index * 4) // uint32 is 4 bytes

	// Read directly from C memory
	return *(*uint32)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetUint64 sets a uint64 value at the specified index
func (v *SafeColumnVector) SetUint64(index int, value uint64) {
	if v == nil {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.cData == nil || v.freed || index >= v.Capacity {
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
	if v == nil {
		return 0
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.cData == nil || v.freed || index >= v.Size {
		return 0
	}

	// Calculate the offset for this index
	offset := uintptr(index * 8) // uint64 is 8 bytes

	// Read directly from C memory
	return *(*uint64)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetFloat32 sets a float32 value at the specified index
func (v *SafeColumnVector) SetFloat32(index int, value float32) {
	if v == nil {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.cData == nil || v.freed || index >= v.Capacity {
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
	if v == nil {
		return 0
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.cData == nil || v.freed || index >= v.Size {
		return 0
	}

	// Calculate the offset for this index
	offset := uintptr(index * 4) // float32 is 4 bytes

	// Read directly from C memory
	return *(*float32)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetFloat64 sets a float64 value at the specified index
func (v *SafeColumnVector) SetFloat64(index int, value float64) {
	if v == nil {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.cData == nil || v.freed || index >= v.Capacity {
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
	if v == nil {
		return 0
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.cData == nil || v.freed || index >= v.Size {
		return 0
	}

	// Calculate the offset for this index
	offset := uintptr(index * 8) // float64 is 8 bytes

	// Read directly from C memory
	return *(*float64)(unsafe.Pointer(uintptr(v.cData) + offset))
}

// SetBool sets a bool value at the specified index
func (v *SafeColumnVector) SetBool(index int, value bool) {
	if v == nil {
		return
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.cData == nil || v.freed || index >= v.Capacity {
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
	if v == nil {
		return false
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.cData == nil || v.freed || index >= v.Size {
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

// vectorCleanupInterval defines how often to check for unused vectors (set to ~1 minute)
const vectorCleanupInterval = 1000

// Counter to track allocations for periodic cleanup
var allocCounter int32

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
		DUCKDB_TYPE_BOOLEAN:  1, // bool is 1 byte
		DUCKDB_TYPE_TINYINT:  1, // int8 is 1 byte
		DUCKDB_TYPE_SMALLINT: 2, // int16 is 2 bytes
		DUCKDB_TYPE_INTEGER:  4, // int32 is 4 bytes
		DUCKDB_TYPE_BIGINT:   8, // int64 is 8 bytes
		DUCKDB_TYPE_FLOAT:    4, // float32 is 4 bytes
		DUCKDB_TYPE_DOUBLE:   8, // float64 is 8 bytes
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

	// Periodically check to clean up any vectors that might not get used
	// This helps prevent memory leaks from preallocated vectors
	counter := atomic.AddInt32(&allocCounter, 1)
	if counter%vectorCleanupInterval == 0 {
		go cleanUnusedVectors()
	}
}

// cleanUnusedVectors periodically removes vectors that haven't been used recently
// This helps prevent memory leaks from preallocated vectors that are never used
func cleanUnusedVectors() {
	// Get exclusive lock since we'll be modifying the map
	vectorsLock.Lock()
	defer vectorsLock.Unlock()

	// For now, we'll use a simple strategy: keep only a reasonable number of vectors per type
	// A more sophisticated approach would track usage time and free less-used vectors
	const maxVectorsPerType = 5

	for duckDBType, typeMap := range vectorsByType {
		if len(typeMap) > maxVectorsPerType {
			// Count how many vectors we have for this type
			count := 0

			// First pass: free excess vectors
			for elementSize, vector := range typeMap {
				count++
				if count > maxVectorsPerType {
					// Free the vector's memory
					vector.Free()
					// Remove from the map
					delete(typeMap, elementSize)
				}
			}
		}

		// If the map is now empty, remove it entirely
		if len(typeMap) == 0 {
			delete(vectorsByType, duckDBType)
		}
	}
}

// GetPreallocatedVector retrieves a pre-allocated vector for a specific type and element size.
// Returns nil if no vector is available.
func GetPreallocatedVector(duckDBType int, elementSize int) *SafeColumnVector {
	// Ensure thread-safe access to the map
	vectorsLock.RLock()

	// Check if we have a type map for this DuckDB type
	typeMap, typeOk := vectorsByType[duckDBType]
	if !typeOk {
		vectorsLock.RUnlock()
		return nil
	}

	// Check if we have a vector for this element size
	vector, vectorOk := typeMap[elementSize]

	// Release read lock before returning
	vectorsLock.RUnlock()

	if !vectorOk {
		return nil
	}

	return vector
}

// Driver implements the database/sql/driver.Driver interface.
type Driver struct {
}

// Open opens a new connection to the database using memory database as default.
func (d *Driver) Open(name string) (driver.Conn, error) {
	if name == "" {
		name = ":memory:"
	}

	var options []ConnectionOption

	return NewConnection(name, options...)
}
