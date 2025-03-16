package duckdb

import (
	"fmt"
	"testing"
)

func TestOptimizedStringCache(t *testing.T) {
	// Create a new cache with 10 columns
	cache := NewOptimizedStringCache(10)

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

	t.Run("Reset", func(t *testing.T) {
		// Fill cache with lots of entries
		for i := 0; i < 12000; i++ {
			key := generateString(10) + generateStringWithNumber(i)
			cache.Get(0, key)
		}

		// Verify cache is full
		if len(cache.smallStrings) < 10000 {
			t.Errorf("Expected smallStrings to have at least 10000 entries, got %d", len(cache.smallStrings))
		}

		// Reset the cache
		cache.Reset()

		// Verify cache was reset
		if len(cache.smallStrings) >= 10000 {
			t.Errorf("Expected smallStrings to be reset to < 10000, got %d", len(cache.smallStrings))
		}

		// Verify hits and misses reset
		if cache.hits != 0 || cache.misses != 0 {
			t.Errorf("Expected hits and misses to be reset to 0, got hits=%d, misses=%d", cache.hits, cache.misses)
		}
	})
}

func BenchmarkStringCache(b *testing.B) {
	// Create both cache implementations for comparison
	oldCache := NewStringCache(10)
	optimizedCache := NewOptimizedStringCache(10)

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

	// Benchmark original string cache
	b.Run("OriginalCache", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx := i % 10

			// Mix of different string sizes
			if i%100 < 70 {
				// 70% small strings
				strIdx := i % len(smallStrings)
				oldCache.Get(idx, smallStrings[strIdx])
			} else if i%100 < 90 {
				// 20% medium strings
				strIdx := i % len(mediumStrings)
				oldCache.Get(idx, mediumStrings[strIdx])
			} else {
				// 10% large strings
				strIdx := i % len(largeStrings)
				oldCache.Get(idx, largeStrings[strIdx])
			}

			// Periodic reset
			if i%10000 == 0 {
				oldCache.Reset()
			}
		}
	})

	// Benchmark optimized string cache
	b.Run("OptimizedCache", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			idx := i % 10

			// Mix of different string sizes
			if i%100 < 70 {
				// 70% small strings
				strIdx := i % len(smallStrings)
				optimizedCache.Get(idx, smallStrings[strIdx])
			} else if i%100 < 90 {
				// 20% medium strings
				strIdx := i % len(mediumStrings)
				optimizedCache.Get(idx, mediumStrings[strIdx])
			} else {
				// 10% large strings
				strIdx := i % len(largeStrings)
				optimizedCache.Get(idx, largeStrings[strIdx])
			}

			// Periodic reset
			if i%10000 == 0 {
				optimizedCache.Reset()
			}
		}
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
