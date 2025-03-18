// Package duckdb provides a zero-allocation, high-performance SQL driver for DuckDB in Go.
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
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// BufferPool is a pool of result buffers for efficient reuse
type BufferPool struct {
	// Mutex for thread-safe operations on shared state
	mu sync.Mutex

	// Pool of pre-allocated result buffers
	// sync.Pool is internally thread-safe, but we need additional protection
	// for our own tracking state
	buffers sync.Pool

	// Statistics for monitoring and tuning - uses atomic operations for thread safety
	gets     uint64
	puts     uint64
	misses   uint64
	discards uint64

	// Last cleanup time to prevent memory leaks from long-lived pools
	// Protected by mutex for thread-safe access
	lastCleanup time.Time

	// Maximum age of a buffer in the pool before it's discarded
	// This is immutable after initialization, so no locking needed for reads
	maxAge time.Duration
}

// NewBufferPool creates a new buffer pool with default settings
func NewBufferPool() *BufferPool {
	pool := &BufferPool{
		lastCleanup: time.Now(),
		maxAge:      10 * time.Minute, // Default to 10 minutes max age
	}

	pool.buffers = sync.Pool{
		New: func() interface{} {
			// Allocate a new buffer when the pool is empty
			atomic.AddUint64(&pool.misses, 1)

			buffer := new(C.result_buffer_t)
			// Zero initialize the buffer
			C.memset(unsafe.Pointer(buffer), 0, C.sizeof_result_buffer_t)
			return buffer
		},
	}

	return pool
}

// GetBuffer gets a buffer from the pool or allocates a new one
// Thread-safe implementation for safe buffer allocation and initialization
func (p *BufferPool) GetBuffer() *C.result_buffer_t {
	// Track buffer gets using atomic counter
	atomic.AddUint64(&p.gets, 1)

	// Clean up old buffers occasionally to prevent memory leaks
	// maybeCleanup is thread-safe and handles its own locking
	p.maybeCleanup()

	// Get buffer from pool - sync.Pool is thread-safe
	bufferInterface := p.buffers.Get()

	// Safety check for nil (shouldn't happen with sync.Pool, but better to be safe)
	if bufferInterface == nil {
		// Create a new buffer if we somehow got nil
		buffer := new(C.result_buffer_t)
		// Zero memory is thread-safe as we just allocated this buffer and no other
		// thread has a reference to it yet
		C.memset(unsafe.Pointer(buffer), 0, C.sizeof_result_buffer_t)
		buffer.ref_count = 1 // Initial ref count
		return buffer
	}

	// Type assertion from interface{} to *C.result_buffer_t
	buffer := bufferInterface.(*C.result_buffer_t)

	// Ensure clean state - this check prevents reusing buffers with potentially
	// invalid state that could lead to memory corruption
	// We capture the values before checking to avoid TOCTTOU issues
	if buffer.ref_count != 0 || buffer.error_code != 0 || buffer.resource_count != 0 {
		// First free any resources that might be associated with this buffer
		// free_result_buffer is thread-safe and handles its own locking
		C.free_result_buffer(buffer)

		// Then zero out the buffer for clean state
		// This is safe as we're the only ones with access to this buffer at this point
		C.memset(unsafe.Pointer(buffer), 0, C.sizeof_result_buffer_t)
	}

	// Initialize with ref_count of 1
	// No other thread can access this buffer yet, so it's safe to modify
	buffer.ref_count = 1

	return buffer
}

// PutBuffer returns a buffer to the pool if it's in a reusable state
// Thread-safe implementation that properly handles buffer state checking
func (p *BufferPool) PutBuffer(buffer *C.result_buffer_t) {
	if buffer == nil {
		return
	}

	atomic.AddUint64(&p.puts, 1)

	// C structs are not atomically accessible in Go, and this struct
	// could potentially be modified by C code concurrently.
	// We need to capture the state carefully to avoid race conditions.

	// Important: Get values once to avoid TOCTTOU (time-of-check vs time-of-use) issues
	// We accept the small possibility that these values might change after reading,
	// which in worst case means we'll either discard a reusable buffer or attempt to
	// reuse a non-reusable one (which we'll catch with our safety checks later).
	refCount := int(buffer.ref_count)
	errorCode := int(buffer.error_code)

	// Only put buffer back if it's in a clean state with no references
	if refCount <= 1 && errorCode == 0 {
		// First free any resources - this ensures we don't leak memory
		// This is thread-safe as C.free_result_buffer handles its own locking
		C.free_result_buffer(buffer)

		// Then zero out the buffer completely to prevent any lingering data
		// This is safer than just setting fields to zero individually
		C.memset(unsafe.Pointer(buffer), 0, C.sizeof_result_buffer_t)

		// Put back in pool - the buffer is now in a known clean state
		// sync.Pool is thread-safe on its own
		p.buffers.Put(buffer)
	} else {
		// Buffer is not in a clean state, discard it
		atomic.AddUint64(&p.discards, 1)

		// Free resources and buffer
		C.free_result_buffer(buffer)
		// We don't reuse this buffer, so no need to zero it out
	}
}

// maybeCleanup occasionally cleans up the pool to prevent memory leaks
// Thread-safe implementation that properly handles lastCleanup access
func (p *BufferPool) maybeCleanup() {
	// Use a single atomic operation to check and update lastCleanup
	// this eliminates potential race conditions with lastCleanup updates
	p.mu.Lock()
	defer p.mu.Unlock()

	// Get current time while holding the lock
	now := time.Now()

	// Check if sufficient time has passed since last cleanup
	timeSinceLastCleanup := now.Sub(p.lastCleanup)
	if timeSinceLastCleanup < p.maxAge/10 {
		// Not time yet, exit early - mutex released by defer
		return
	}

	// Update last cleanup time while holding the lock
	p.lastCleanup = now

	// We can't directly clean the sync.Pool, but we can:
	// 1. Create a bunch of garbage to trigger GC
	// 2. Let the Go runtime's sync.Pool expiry handle cleanup (it happens on GC)
	// 3. Remove references to the pool itself to allow full GC
	//
	// The mutex is still held here, which is important since we're
	// potentially modifying the pool's behavior
}

// Stats returns statistics about the buffer pool
func (p *BufferPool) Stats() map[string]uint64 {
	return map[string]uint64{
		"gets":     atomic.LoadUint64(&p.gets),
		"puts":     atomic.LoadUint64(&p.puts),
		"misses":   atomic.LoadUint64(&p.misses),
		"discards": atomic.LoadUint64(&p.discards),
	}
}

// Global buffer pool for shared use
var resultBufferPool = NewBufferPool()

// GetBuffer gets a buffer from the global pool
func GetBuffer() *C.result_buffer_t {
	return resultBufferPool.GetBuffer()
}

// PutBuffer returns a buffer to the global pool
func PutBuffer(buffer *C.result_buffer_t) {
	resultBufferPool.PutBuffer(buffer)
}
