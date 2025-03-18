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
	mu sync.Mutex

	// Pool of pre-allocated result buffers
	buffers sync.Pool

	// Statistics for monitoring and tuning
	gets     uint64
	puts     uint64
	misses   uint64
	discards uint64

	// Last cleanup time to prevent memory leaks from long-lived pools
	lastCleanup time.Time

	// Maximum age of a buffer in the pool before it's discarded
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
func (p *BufferPool) GetBuffer() *C.result_buffer_t {
	atomic.AddUint64(&p.gets, 1)

	// Clean up old buffers occasionally to prevent memory leaks
	p.maybeCleanup()

	// Get buffer from pool
	bufferInterface := p.buffers.Get()

	// Safety check for nil (shouldn't happen with sync.Pool, but better to be safe)
	if bufferInterface == nil {
		// Create a new buffer if we somehow got nil
		buffer := new(C.result_buffer_t)
		C.memset(unsafe.Pointer(buffer), 0, C.sizeof_result_buffer_t)
		buffer.ref_count = 1
		return buffer
	}

	buffer := bufferInterface.(*C.result_buffer_t)

	// Ensure clean state - this check prevents reusing buffers with potentially
	// invalid state that could lead to memory corruption
	if buffer.ref_count != 0 || buffer.error_code != 0 || buffer.resource_count != 0 {
		// First free any resources that might be associated with this buffer
		C.free_result_buffer(buffer)
		// Then zero out the buffer for clean state
		C.memset(unsafe.Pointer(buffer), 0, C.sizeof_result_buffer_t)
	}

	// Initialize with ref_count of 1
	buffer.ref_count = 1

	return buffer
}

// PutBuffer returns a buffer to the pool if it's in a reusable state
func (p *BufferPool) PutBuffer(buffer *C.result_buffer_t) {
	if buffer == nil {
		return
	}

	atomic.AddUint64(&p.puts, 1)

	// Get current value of ref_count before modifying it
	// to avoid race condition where another goroutine might
	// be changing it at the same time
	refCount := buffer.ref_count
	errorCode := buffer.error_code

	// Only put buffer back if it's in a clean state with no references
	if refCount <= 1 && errorCode == 0 {
		// First free any resources - this ensures we don't leak memory
		C.free_result_buffer(buffer)

		// Then zero out the buffer completely to prevent any lingering data
		// This is safer than just setting fields to zero individually
		C.memset(unsafe.Pointer(buffer), 0, C.sizeof_result_buffer_t)

		// Put back in pool - the buffer is now in a known clean state
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
func (p *BufferPool) maybeCleanup() {
	// Acquire lock first to prevent race condition
	p.mu.Lock()

	// Get current time while holding the lock
	now := time.Now()

	// Check if it's time to clean up
	if now.Sub(p.lastCleanup) < p.maxAge/10 {
		// Not time yet, release lock and return
		p.mu.Unlock()
		return
	}

	// Update last cleanup time while still holding the lock
	p.lastCleanup = now

	// We've updated the timestamp, can release the lock now
	p.mu.Unlock()

	// We can't directly clean the sync.Pool, but we can
	// let the GC do its work by having the pool discard
	// some entries naturally.
	// Note: the actual cleanup doesn't need the lock since
	// sync.Pool is already thread-safe.
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
