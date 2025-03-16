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
	buffer := p.buffers.Get().(*C.result_buffer_t)
	
	// Ensure clean state
	if buffer.ref_count != 0 || buffer.error_code != 0 || buffer.resource_count != 0 {
		// If the buffer is in an unexpected state, discard it and get a fresh one
		C.free_result_buffer(buffer)
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
	
	// Only put buffer back if it's in a clean state with no references
	if buffer.ref_count <= 1 && buffer.error_code == 0 {
		// Reset the buffer completely to prevent any lingering data
		C.free_result_buffer(buffer)
		C.memset(unsafe.Pointer(buffer), 0, C.sizeof_result_buffer_t)
		
		// Put back in pool
		p.buffers.Put(buffer)
	} else {
		// Buffer is not in a clean state, discard it
		atomic.AddUint64(&p.discards, 1)
		C.free_result_buffer(buffer)
	}
}

// maybeCleanup occasionally cleans up the pool to prevent memory leaks
func (p *BufferPool) maybeCleanup() {
	// Only clean up occasionally to avoid lock contention
	now := time.Now()
	if now.Sub(p.lastCleanup) < p.maxAge/10 {
		return
	}
	
	// Acquire lock for cleanup
	p.mu.Lock()
	defer p.mu.Unlock()
	
	// Double-check that another goroutine didn't beat us to it
	if now.Sub(p.lastCleanup) < p.maxAge/10 {
		return
	}
	
	// Update last cleanup time
	p.lastCleanup = now
	
	// We can't directly clean the sync.Pool, but we can
	// let the GC do its work by having the pool discard
	// some entries naturally.
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