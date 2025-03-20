package duckdb

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

func TestStringCache(t *testing.T) {
	// Create a new cache with 10 columns
	cache := NewStringCache(10)

	// Test Get method
	t.Run("Get", func(t *testing.T) {
		// Test empty string
		s := cache.Get(0, "")
		if s != "" {
			t.Errorf("Expected empty string, got %q", s)
		}

		// Test small string
		s = cache.Get(1, "small")
		if s != "small" {
			t.Errorf("Expected 'small', got %q", s)
		}

		// Test cache hit with same string
		s2 := cache.Get(2, "small")
		if s2 != "small" {
			t.Errorf("Expected 'small', got %q", s2)
		}

		// Verify hits increased
		if cache.hits < 1 {
			t.Errorf("Expected hits > 0, got %d", cache.hits)
		}

		// Test medium string
		mediumStr := generateString(100)
		s = cache.Get(3, mediumStr)
		if s != mediumStr {
			t.Errorf("Expected %q, got %q", mediumStr, s)
		}

		// Test large string
		largeStr := generateString(2000)
		s = cache.Get(4, largeStr)
		if s != largeStr {
			t.Errorf("Expected large string of length %d, got length %d", len(largeStr), len(s))
		}

		// Test column index expansion
		s = cache.Get(20, "expanded")
		if s != "expanded" {
			t.Errorf("Expected 'expanded', got %q", s)
		}
		if len(cache.columnValues) <= 10 {
			t.Errorf("Expected column values to expand beyond 10, got %d", len(cache.columnValues))
		}
	})

	t.Run("GetFromBytes", func(t *testing.T) {
		// Test empty bytes
		s := cache.GetFromBytes(0, []byte{})
		if s != "" {
			t.Errorf("Expected empty string, got %q", s)
		}

		// Test small bytes
		b := []byte("small bytes")
		s = cache.GetFromBytes(1, b)
		if s != "small bytes" {
			t.Errorf("Expected 'small bytes', got %q", s)
		}

		// Test cache hit
		s2 := cache.GetFromBytes(2, []byte("small bytes"))
		if s2 != "small bytes" {
			t.Errorf("Expected 'small bytes', got %q", s2)
		}

		// Test medium bytes
		mediumBytes := []byte(generateString(100))
		s = cache.GetFromBytes(3, mediumBytes)
		if s != string(mediumBytes) {
			t.Errorf("Expected string of length %d, got length %d", len(mediumBytes), len(s))
		}

		// Test large bytes
		largeBytes := []byte(generateString(2000))
		s = cache.GetFromBytes(4, largeBytes)
		if s != string(largeBytes) {
			t.Errorf("Expected string of length %d, got length %d", len(largeBytes), len(s))
		}
	})
}

func TestConcurrentStringCacheAccess(t *testing.T) {
	cache := NewStringCache(10)

	// Number of goroutines to test with
	goroutines := runtime.GOMAXPROCS(0) * 4
	iterations := 10000

	// Create a wait group to synchronize goroutines
	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Run the test with multiple goroutines
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()

			// Each goroutine will access a mix of shared and unique strings
			for i := 0; i < iterations; i++ {
				// Shared strings (all goroutines access the same strings)
				cache.Get(0, "shared1")
				cache.Get(1, "shared2")
				cache.Get(2, "shared"+generateStringWithNumber(i%10))

				// Unique strings per goroutine
				unique := fmt.Sprintf("unique-%d-%d", id, i)
				cache.Get(3, unique)

				// Mix of GetFromBytes calls
				cache.GetFromBytes(4, []byte("shared-bytes"))
				cache.GetFromBytes(5, []byte(unique))
			}
		}(g)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify the cache has hits
	hits := cache.hits
	misses := cache.misses
	cHits := cache.cHits
	t.Logf("Cache stats: hits=%d, misses=%d, cHits=%d", hits, misses, cHits)

	if hits <= 0 {
		t.Errorf("Expected cache hits > 0, got %d", hits)
	}
}

func BenchmarkStringCache(b *testing.B) {
	// Create string cache implementation
	cache := NewStringCache(10)

	// Create test data
	smallStrings := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		smallStrings[i] = generateStringWithNumber(i % 50) // Create some duplication
	}

	mediumStrings := make([]string, 500)
	for i := 0; i < 500; i++ {
		mediumStrings[i] = generateString(100) + generateStringWithNumber(i%20)
	}

	largeStrings := make([]string, 100)
	for i := 0; i < 100; i++ {
		largeStrings[i] = generateString(2000) + generateStringWithNumber(i)
	}

	// Simple high contention pattern - repeated lookups of same strings
	b.Run("StringCache_HighContention", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx := i % 10
			// Use the same few strings to create contention
			stringIndex := i % 5
			cache.Get(idx, smallStrings[stringIndex])
		}
	})

	// Benchmark global buffer pool GetSharedString with high contention
	b.Run("GlobalBufferPool_HighContention", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Use the same few strings to create contention
			stringIndex := i % 5
			globalBufferPool.GetSharedString(smallStrings[stringIndex])
		}
	})

	// More realistic mixed workload - mix of hits and misses
	b.Run("StringCache_MixedWorkload", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx := i % 10
			// Mix of different string types and hit/miss patterns
			switch i % 20 {
			case 0, 1, 2, 3, 4: // 25% - high-frequency small strings (high hit rate)
				cache.Get(idx, smallStrings[i%5])
			case 5, 6, 7, 8, 9: // 25% - medium-frequency small strings (medium hit rate)
				cache.Get(idx, smallStrings[5+(i%15)])
			case 10, 11, 12: // 15% - medium strings (lower hit rate)
				cache.Get(idx, mediumStrings[i%50])
			case 13: // 5% - large strings (very low hit rate)
				cache.Get(idx, largeStrings[i%100])
			default: // 30% - unique strings (all misses)
				cache.Get(idx, fmt.Sprintf("unique-string-%d", i))
			}
		}
	})

	// Same mixed workload for global buffer pool
	b.Run("GlobalBufferPool_MixedWorkload", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			// Mix of different string types and hit/miss patterns
			switch i % 20 {
			case 0, 1, 2, 3, 4: // 25% - high-frequency small strings (high hit rate)
				globalBufferPool.GetSharedString(smallStrings[i%5])
			case 5, 6, 7, 8, 9: // 25% - medium-frequency small strings (medium hit rate)
				globalBufferPool.GetSharedString(smallStrings[5+(i%15)])
			case 10, 11, 12: // 15% - medium strings (lower hit rate)
				globalBufferPool.GetSharedString(mediumStrings[i%50])
			case 13: // 5% - large strings (very low hit rate)
				globalBufferPool.GetSharedString(largeStrings[i%100])
			default: // 30% - unique strings (all misses)
				globalBufferPool.GetSharedString(fmt.Sprintf("unique-string-%d", i))
			}
		}
	})

	// Concurrent benchmarks with high contention
	const numGoroutines = 8 // Use fewer goroutines to prevent CPU spikes

	// Benchmark concurrent string cache with high contention
	b.Run("StringCache_Concurrent_HighContention", func(b *testing.B) {
		// Create a set of strings that all goroutines will fight over
		highContentionStrings := make([]string, 8)
		for i := 0; i < 8; i++ {
			highContentionStrings[i] = fmt.Sprintf("shared-string-%d", i)
		}

		b.ResetTimer()

		// Create wait group
		var wg sync.WaitGroup
		opsPerGoroutine := b.N / numGoroutines

		// Launch goroutines
		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for i := 0; i < opsPerGoroutine; i++ {
					idx := i % 10
					// All goroutines use the same set of strings
					stringIdx := i % len(highContentionStrings)
					cache.Get(idx, highContentionStrings[stringIdx])
				}
			}(g)
		}

		wg.Wait()
	})

	// Benchmark concurrent global buffer pool GetSharedString with high contention
	b.Run("GlobalBufferPool_Concurrent_HighContention", func(b *testing.B) {
		// Create a set of strings that all goroutines will fight over
		highContentionStrings := make([]string, 8)
		for i := 0; i < 8; i++ {
			highContentionStrings[i] = fmt.Sprintf("shared-string-%d", i)
		}

		b.ResetTimer()

		// Create wait group
		var wg sync.WaitGroup
		opsPerGoroutine := b.N / numGoroutines

		// Launch goroutines
		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for i := 0; i < opsPerGoroutine; i++ {
					// All goroutines use the same set of strings
					stringIdx := i % len(highContentionStrings)
					globalBufferPool.GetSharedString(highContentionStrings[stringIdx])
				}
			}(g)
		}

		wg.Wait()
	})

	// Concurrent mixed workload benchmark for StringCache
	b.Run("StringCache_Concurrent_MixedWorkload", func(b *testing.B) {
		b.ResetTimer()

		var wg sync.WaitGroup
		opsPerGoroutine := b.N / numGoroutines

		// Pre-generate unique strings to avoid fmt.Sprintf in hot path
		uniqueStrings := make([][]string, numGoroutines)
		for g := 0; g < numGoroutines; g++ {
			uniqueBase := g * 1000000
			uniqueStrings[g] = make([]string, opsPerGoroutine/2) // Only store the ones we'll use
			for i := 0; i < len(uniqueStrings[g]); i++ {
				uniqueStrings[g][i] = fmt.Sprintf("unique-%d-%d", uniqueBase, i)
			}
		}

		// Launch goroutines
		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				uniqueIdx := 0
				for i := 0; i < opsPerGoroutine; i++ {
					idx := i % 10
					// Mix of shared and unique strings
					switch i % 10 {
					case 0, 1, 2: // 30% - small shared strings (high contention)
						cache.Get(idx, smallStrings[i%5])
					case 3, 4: // 20% - medium shared strings
						cache.Get(idx, mediumStrings[i%10])
					case 5: // 10% - large shared strings
						cache.Get(idx, largeStrings[i%5])
					default: // 40% - unique strings per goroutine
						// Use pre-generated strings instead of fmt.Sprintf in hot path
						if uniqueIdx < len(uniqueStrings[id]) {
							cache.Get(idx, uniqueStrings[id][uniqueIdx])
							uniqueIdx++
						} else {
							// Fallback for any extra iterations
							cache.Get(idx, smallStrings[i%5])
						}
					}
				}
			}(g)
		}

		wg.Wait()
	})

	// Concurrent mixed workload for global buffer pool
	b.Run("GlobalBufferPool_Concurrent_MixedWorkload", func(b *testing.B) {
		// Simplified benchmark to avoid CPU spikes
		commonStrings := []string{
			"string1", "string2", "string3", "string4", "string5",
			"medium-string-1", "medium-string-2",
			"a-bit-longer-string-to-test-with",
		}

		b.ResetTimer()

		// Use reduced number of goroutines
		var wg sync.WaitGroup
		reducedGoroutines := 4
		opsPerGoroutine := b.N / reducedGoroutines

		// Launch goroutines
		for g := 0; g < reducedGoroutines; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// Simple pattern with low memory pressure
				for i := 0; i < opsPerGoroutine; i++ {
					stringIdx := i % len(commonStrings)
					globalBufferPool.GetSharedString(commonStrings[stringIdx])
				}
			}(g)
		}

		wg.Wait()
	})
}

// Helper to generate a random string of given length
func generateString(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)

	for i := 0; i < length; i++ {
		result[i] = chars[i%len(chars)]
	}

	return string(result)
}

// Helper to generate a string with a number
func generateStringWithNumber(n int) string {
	return generateString(5) + "-" + fmt.Sprintf("%d", n)
}
