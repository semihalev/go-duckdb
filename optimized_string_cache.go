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
	Reset()
	TuneCache()
}

// OptimizedStringCache is a high-performance string cache implementation
// that balances CPU performance with memory optimization.
type OptimizedStringCache struct {
	// Column-specific caches for quick access by column index
	columnValues []string

	// Tiered caching strategy based on string length
	smallStrings  map[string]string // For strings < 64 bytes
	mediumStrings sync.Map          // For strings < 1024 bytes, thread-safe

	// Single reusable buffer for string conversion
	buffer []byte

	// Size thresholds for different handling paths
	smallStringThreshold  int
	mediumStringThreshold int

	// Flag indicating whether this cache uses the global string map
	useSharedStringMap bool

	// Statistics for monitoring
	hits   int
	misses int

	// Mutex for buffer access
	mu sync.Mutex
}

// NewOptimizedStringCache creates a new optimized string cache with improved performance.
func NewOptimizedStringCache(columns int) *OptimizedStringCache {
	return &OptimizedStringCache{
		columnValues:          make([]string, columns),
		smallStrings:          make(map[string]string, 1024), // Smaller initial capacity
		buffer:                make([]byte, 4096),            // Single larger buffer
		smallStringThreshold:  64,                            // Fast path for common strings
		mediumStringThreshold: 1024,                          // Threshold for sync.Map
		useSharedStringMap:    true,                          // Enable shared string map
	}
}

// Get returns a cached string for the column value, optimized for speed
func (sc *OptimizedStringCache) Get(colIdx int, value string) string {
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
		if cached, ok := sc.smallStrings[value]; ok {
			sc.columnValues[colIdx] = cached
			sc.hits++
			return cached
		}

		// For very small strings, use the shared pool for massive deduplication
		if sc.useSharedStringMap {
			result := globalBufferPool.GetSharedString(value)

			// Cache locally too for future hits
			sc.smallStrings[value] = result

			sc.columnValues[colIdx] = result
			sc.misses++
			return result
		}

		// For non-shared mode, store in local map
		sc.smallStrings[value] = value
		sc.columnValues[colIdx] = value
		sc.misses++
		return value
	}

	// For medium strings, use thread-safe map
	if len(value) < sc.mediumStringThreshold {
		// Check medium string cache
		if cached, ok := sc.mediumStrings.Load(value); ok {
			cachedStr := cached.(string)
			sc.columnValues[colIdx] = cachedStr
			sc.hits++
			return cachedStr
		}

		// Only use shared map for medium strings
		if sc.useSharedStringMap {
			result := globalBufferPool.GetSharedString(value)
			sc.mediumStrings.Store(value, result)
			sc.columnValues[colIdx] = result
			sc.misses++
			return result
		}

		// Store in medium string cache
		sc.mediumStrings.Store(value, value)
		sc.columnValues[colIdx] = value
		sc.misses++
		return value
	}

	// For large strings, no caching to avoid memory bloat
	sc.columnValues[colIdx] = value
	sc.misses++
	return value
}

// GetFromBytes converts a byte slice to a string with optimized performance
func (sc *OptimizedStringCache) GetFromBytes(colIdx int, bytes []byte) string {
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
		if cached, ok := sc.smallStrings[s]; ok {
			sc.columnValues[colIdx] = cached
			sc.hits++
			return cached
		}

		// For very small strings, use the shared pool
		if sc.useSharedStringMap {
			result := globalBufferPool.GetSharedString(s)

			// Cache locally too for future hits
			sc.smallStrings[s] = result

			sc.columnValues[colIdx] = result
			sc.misses++
			return result
		}

		// For non-shared mode, store in local map
		sc.smallStrings[s] = s
		sc.columnValues[colIdx] = s
		sc.misses++
		return s
	}

	// For medium strings
	if len(bytes) < sc.mediumStringThreshold {
		s := string(bytes)

		// Check medium string cache
		if cached, ok := sc.mediumStrings.Load(s); ok {
			cachedStr := cached.(string)
			sc.columnValues[colIdx] = cachedStr
			sc.hits++
			return cachedStr
		}

		// Only use shared map for medium strings
		if sc.useSharedStringMap {
			result := globalBufferPool.GetSharedString(s)
			sc.mediumStrings.Store(s, result)
			sc.columnValues[colIdx] = result
			sc.misses++
			return result
		}

		// Store in medium string cache
		sc.mediumStrings.Store(s, s)
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
// Optimized for both performance and memory usage
func (sc *OptimizedStringCache) GetFromCString(colIdx int, cstr *C.char, length C.size_t) string {
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

	// Safety check - if length is unreasonably large, cap it
	actualLength := int(length)
	if actualLength > 1024*1024 { // Cap at 1MB for safety
		actualLength = 1024 * 1024
	}

	// Use direct GoString for small strings - more efficient than buffer copying
	if actualLength < sc.smallStringThreshold {
		s := C.GoStringN(cstr, C.int(actualLength))

		// Check local map first for very small strings
		if cached, ok := sc.smallStrings[s]; ok {
			sc.columnValues[colIdx] = cached
			sc.hits++
			return cached
		}

		// Use shared map for small strings
		if sc.useSharedStringMap {
			result := globalBufferPool.GetSharedString(s)

			// Cache locally for future hits
			sc.smallStrings[s] = result

			sc.columnValues[colIdx] = result
			sc.misses++
			return result
		}

		// For non-shared mode, store in local map
		sc.smallStrings[s] = s
		sc.columnValues[colIdx] = s
		sc.misses++
		return s
	}

	// For medium strings, use buffer with synchronized access
	if actualLength < sc.mediumStringThreshold {
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

		// Unlock after string creation but before caching
		sc.mu.Unlock()

		// Check medium string cache
		if cached, ok := sc.mediumStrings.Load(s); ok {
			cachedStr := cached.(string)
			sc.columnValues[colIdx] = cachedStr
			sc.hits++
			return cachedStr
		}

		// Only use shared map for medium strings
		if sc.useSharedStringMap {
			result := globalBufferPool.GetSharedString(s)
			sc.mediumStrings.Store(s, result)
			sc.columnValues[colIdx] = result
			sc.misses++
			return result
		}

		// Store in medium string cache
		sc.mediumStrings.Store(s, s)
		sc.columnValues[colIdx] = s
		sc.misses++
		return s
	}

	// For large strings, use direct C.GoString without caching
	// This is simpler and avoids memory bloat for large strings
	s := C.GoStringN(cstr, C.int(actualLength))
	sc.columnValues[colIdx] = s
	sc.misses++
	return s
}

// Reset clears the caches to prevent unbounded growth
func (sc *OptimizedStringCache) Reset() {
	// Only reset if the maps have grown significantly
	smallMapSize := len(sc.smallStrings)
	if smallMapSize > 10000 {
		// Just create a new map - faster than selective copying
		sc.smallStrings = make(map[string]string, 1024)
	}

	// Always create a new medium map - it's cheaper than clearing
	sc.mediumStrings = sync.Map{}

	// Reset stats
	sc.hits = 0
	sc.misses = 0
}

// TuneCache optimizes cache parameters based on usage patterns
func (sc *OptimizedStringCache) TuneCache() {
	// Calculate hit rate
	totalOps := sc.hits + sc.misses
	if totalOps < 1000 {
		return // Not enough data to tune
	}

	hitRate := float64(sc.hits) / float64(totalOps)

	// Adjust thresholds based on hit rate
	if hitRate < 0.3 {
		// If hit rate is low, we might need to adjust our thresholds
		if sc.smallStringThreshold < 128 {
			sc.smallStringThreshold *= 2 // Try caching more strings as small
		}

		if sc.mediumStringThreshold < 2048 {
			sc.mediumStringThreshold *= 2 // Try caching more strings as medium
		}
	} else if hitRate > 0.8 {
		// If hit rate is very high, we might be caching too much
		if sc.smallStringThreshold > 32 {
			sc.smallStringThreshold /= 2 // Reduce small string threshold
		}

		if sc.mediumStringThreshold > 512 {
			sc.mediumStringThreshold /= 2 // Reduce medium string threshold
		}
	}

	// Reset stats for next tuning cycle
	sc.hits = 0
	sc.misses = 0
}
