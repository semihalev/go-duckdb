package duckdb

/*
#include <stdlib.h>
#include <string.h>
#include <duckdb.h>
*/
import "C"

import (
	"sync"
	"unsafe"
)

// StringCacher is the interface that all string cache implementations must satisfy
type StringCacher interface {
	Get(colIdx int, value string) string
	GetFromBytes(colIdx int, bytes []byte) string
	GetFromCString(colIdx int, cstr *C.char, length C.size_t) string
	GetDedupString(value string) string
	Reset()
}

// StringCache is a high-performance string cache implementation
// that balances CPU performance with memory optimization.
type StringCache struct {
	// Column-specific caches for quick access by column index
	columnValues []string

	// Simplified string intern map - used only for strings under the threshold
	// This dramatically reduces the map size while still capturing common strings
	internMap map[string]string

	// C string pointer cache - maps C string pointer addresses to Go strings
	// This avoids repeated conversions of the same C string
	cStringMap map[uintptr]string

	// Critical section mutex - only needed for complex operations
	// Most operations are lock-free for performance
	mu sync.Mutex

	// Single large buffer for string conversion to avoid allocations
	// Using a single buffer and focused copy operations is simpler/faster
	buffer []byte

	// Size thresholds for different handling paths
	// Small strings use direct interning, large use C.GoString
	smallStringThreshold int
	largeStringThreshold int

	// Global string map usage flag
	useSharedStringMap bool

	// Statistics for monitoring
	hits   int
	misses int
	cHits  int // C string cache hits
}

// NewStringCache creates a new optimized string cache with improved performance.
func NewStringCache(columns int) *StringCache {
	return &StringCache{
		columnValues:         make([]string, columns),
		internMap:            make(map[string]string, 2048),  // Smaller initial capacity
		cStringMap:           make(map[uintptr]string, 2048), // C string pointer cache
		buffer:               make([]byte, 4096),             // Single larger buffer
		smallStringThreshold: 64,                             // Fast path for common strings
		largeStringThreshold: 1024,                           // Threshold for full interning
		useSharedStringMap:   true,                           // Enable shared string map
	}
}

// Get returns a cached string for the column value, optimized for speed
func (sc *StringCache) Get(colIdx int, value string) string {
	// Ensure we have space in the column values
	if colIdx >= len(sc.columnValues) {
		newValues := make([]string, colIdx+1)
		copy(newValues, sc.columnValues)
		sc.columnValues = newValues
	}

	// Simple length check to avoid map lookups for empty strings
	if value == "" {
		sc.columnValues[colIdx] = ""
		return ""
	}

	// Small string optimization - for very common short strings
	if len(value) < sc.smallStringThreshold {
		// Check the local map first - this is the hottest path
		if cached, ok := sc.internMap[value]; ok {
			sc.columnValues[colIdx] = cached
			sc.hits++
			return cached
		}

		// For very small strings, we use the shared pool for massive deduplication
		if sc.useSharedStringMap {
			result := globalBufferPool.GetSharedString(value)

			// Cache locally too for future hits
			sc.internMap[value] = result

			sc.columnValues[colIdx] = result
			sc.misses++
			return result
		}

		// For non-shared mode, store in local map
		sc.internMap[value] = value
		sc.columnValues[colIdx] = value
		sc.misses++
		return value
	}

	// For medium strings, we selectively intern
	if len(value) < sc.largeStringThreshold {
		// Only use shared map for medium strings
		if sc.useSharedStringMap {
			result := globalBufferPool.GetSharedString(value)
			sc.columnValues[colIdx] = result
			sc.misses++
			return result
		}
	}

	// For large strings, no interning to avoid memory bloat
	sc.columnValues[colIdx] = value
	sc.misses++
	return value
}

// GetFromBytes converts a byte slice to a string with optimized performance
func (sc *StringCache) GetFromBytes(colIdx int, bytes []byte) string {
	// Ensure we have space in the column values
	if colIdx >= len(sc.columnValues) {
		newValues := make([]string, colIdx+1)
		copy(newValues, sc.columnValues)
		sc.columnValues = newValues
	}

	// Fast path for empty slices
	if len(bytes) == 0 {
		sc.columnValues[colIdx] = ""
		return ""
	}

	// Fast path for small strings
	if len(bytes) < sc.smallStringThreshold {
		// Create string from bytes
		s := string(bytes)

		// Check the local map first
		if cached, ok := sc.internMap[s]; ok {
			sc.columnValues[colIdx] = cached
			sc.hits++
			return cached
		}

		// For very small strings, use the shared pool
		if sc.useSharedStringMap {
			result := globalBufferPool.GetSharedString(s)

			// Cache locally too for future hits
			sc.internMap[s] = result

			sc.columnValues[colIdx] = result
			sc.misses++
			return result
		}

		// For non-shared mode, store in local map
		sc.internMap[s] = s
		sc.columnValues[colIdx] = s
		sc.misses++
		return s
	}

	// For medium strings
	if len(bytes) < sc.largeStringThreshold {
		s := string(bytes)

		// Only use shared map for medium strings
		if sc.useSharedStringMap {
			result := globalBufferPool.GetSharedString(s)
			sc.columnValues[colIdx] = result
			sc.misses++
			return result
		}

		// Otherwise just use the string directly
		sc.columnValues[colIdx] = s
		sc.misses++
		return s
	}

	// For large strings, just convert without caching
	s := string(bytes)
	sc.columnValues[colIdx] = s
	sc.misses++
	return s
}

// GetFromCString efficiently converts a C string to a Go string
// Optimized for both performance and memory usage with C string pointer caching
func (sc *StringCache) GetFromCString(colIdx int, cstr *C.char, length C.size_t) string {
	// Ensure we have space in the column values
	if colIdx >= len(sc.columnValues) {
		newValues := make([]string, colIdx+1)
		copy(newValues, sc.columnValues)
		sc.columnValues = newValues
	}

	// Fast path for nil or empty strings
	if cstr == nil || length == 0 {
		sc.columnValues[colIdx] = ""
		return ""
	}

	// Get C string address for cache lookup
	cstrAddr := uintptr(unsafe.Pointer(cstr))

	// Check C string pointer cache first - fastest path when same C string is reused
	// This avoids the expensive C.GoStringN conversion completely
	sc.mu.Lock()
	if cached, ok := sc.cStringMap[cstrAddr]; ok {
		sc.mu.Unlock()
		sc.columnValues[colIdx] = cached
		sc.cHits++
		return cached
	}
	sc.mu.Unlock()

	// Safety check - if length is unreasonably large, cap it
	actualLength := int(length)
	if actualLength > 1024*1024 { // Cap at 1MB for safety
		actualLength = 1024 * 1024
	}

	// Use direct GoString for small strings - more efficient than buffer copying
	if actualLength < sc.smallStringThreshold {
		s := C.GoStringN(cstr, C.int(actualLength))

		// Check local map for string value
		if cached, ok := sc.internMap[s]; ok {
			// Cache the C string pointer for future lookups
			sc.mu.Lock()
			sc.cStringMap[cstrAddr] = cached
			sc.mu.Unlock()

			sc.columnValues[colIdx] = cached
			sc.hits++
			return cached
		}

		// For very small strings, use the shared pool for massive deduplication
		if sc.useSharedStringMap {
			result := globalBufferPool.GetSharedString(s)

			// Cache locally for future hits
			sc.internMap[s] = result

			// Cache C string pointer too
			sc.mu.Lock()
			sc.cStringMap[cstrAddr] = result
			sc.mu.Unlock()

			sc.columnValues[colIdx] = result
			sc.misses++
			return result
		}

		// For non-shared mode, store in local map
		sc.internMap[s] = s

		// Cache C string pointer
		sc.mu.Lock()
		sc.cStringMap[cstrAddr] = s
		sc.mu.Unlock()

		sc.columnValues[colIdx] = s
		sc.misses++
		return s
	}

	// For medium strings, use buffer with synchronized access
	if actualLength < sc.largeStringThreshold {
		// Lock for buffer access
		sc.mu.Lock()

		// Ensure buffer is large enough
		if len(sc.buffer) < actualLength {
			sc.buffer = make([]byte, actualLength*2)
		}

		// Copy from C memory to our buffer
		for i := 0; i < actualLength; i++ {
			bytePtr := (*C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(cstr)) + uintptr(i)))
			if bytePtr != nil {
				sc.buffer[i] = byte(*bytePtr)
			} else {
				// Truncate if we hit a null pointer
				actualLength = i
				break
			}
		}

		// Create string from buffer
		s := string(sc.buffer[:actualLength])

		// Cache the C string pointer before unlocking
		sc.cStringMap[cstrAddr] = s

		// Unlock after string creation but before caching
		sc.mu.Unlock()

		// Only use shared map for medium strings
		if sc.useSharedStringMap {
			result := globalBufferPool.GetSharedString(s)

			// Update C string cache with the shared string
			sc.mu.Lock()
			sc.cStringMap[cstrAddr] = result
			sc.mu.Unlock()

			sc.columnValues[colIdx] = result
			sc.misses++
			return result
		}

		sc.columnValues[colIdx] = s
		sc.misses++
		return s
	}

	// For large strings, use direct C.GoString with minimal caching
	s := C.GoStringN(cstr, C.int(actualLength))

	// For large strings, we still cache the C string pointer
	// This is important because the same large string may be used repeatedly
	sc.mu.Lock()
	sc.cStringMap[cstrAddr] = s
	sc.mu.Unlock()

	sc.columnValues[colIdx] = s
	sc.misses++
	return s
}

// Reset clears the intern map to prevent unbounded growth
func (sc *StringCache) Reset() {
	// Only reset if the maps have grown significantly
	sc.mu.Lock()
	internMapSize := len(sc.internMap)
	cStringMapSize := len(sc.cStringMap)

	if internMapSize > 10000 || cStringMapSize > 10000 {
		// Just create new maps - faster than selective copying
		sc.internMap = make(map[string]string, 2048)
		sc.cStringMap = make(map[uintptr]string, 2048)

		// Reset stats
		sc.hits = 0
		sc.misses = 0
		sc.cHits = 0
	}
	sc.mu.Unlock()
}

// Stats returns cache hit/miss statistics
func (sc *StringCache) Stats() (hits, misses, cHits int) {
	return sc.hits, sc.misses, sc.cHits
}

// GetDedupString returns a deduplicated string from the cache
// It uses the shared string map for maximum deduplication across all strings
func (sc *StringCache) GetDedupString(value string) string {
	// Simple length check to avoid map lookups for empty strings
	if value == "" {
		return ""
	}

	// Small string optimization - for very common short strings
	if len(value) < sc.smallStringThreshold {
		// Check the local map first - this is the hottest path
		if cached, ok := sc.internMap[value]; ok {
			sc.hits++
			return cached
		}

		// For very small strings, use the shared pool for massive deduplication
		if sc.useSharedStringMap {
			result := globalBufferPool.GetSharedString(value)

			// Cache locally too for future hits
			sc.internMap[value] = result

			sc.misses++
			return result
		}

		// For non-shared mode, store in local map
		sc.internMap[value] = value
		sc.misses++
		return value
	}

	// Medium string optimization
	if len(value) < sc.largeStringThreshold {
		// Only use shared map for medium strings
		if sc.useSharedStringMap {
			result := globalBufferPool.GetSharedString(value)
			sc.misses++
			return result
		}
	}

	// For large strings, no interning to avoid memory bloat
	sc.misses++
	return value
}
