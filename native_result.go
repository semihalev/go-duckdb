package duckdb

/*
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <duckdb.h>
#include "include/duckdb_native.h"
*/
import "C"
import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ZeroCopyResult holds data for true zero-copy extraction to manage memory lifecycle
type ZeroCopyResult struct {
	// Pointers to C strings that need to be released
	stringPtrs []unsafe.Pointer

	// Pointers to C blobs that need to be released
	blobPtrs []unsafe.Pointer
}

// Release releases all memory in this result
func (z *ZeroCopyResult) Release() {
	// Release all string pointers
	for _, ptr := range z.stringPtrs {
		globalStringTable.Release(ptr)
	}

	// Release all blob pointers
	for _, ptr := range z.blobPtrs {
		C.duckdb_free(ptr)
	}

	// Clear slices to allow GC to reclaim memory
	z.stringPtrs = nil
	z.blobPtrs = nil
}

// StringTable is a special structure for true zero-copy string handling
// It manages the lifecycle of strings extracted from DuckDB results
type StringTable struct {
	// Map of string data pointers to string values
	// This allows us to deduplicate and track strings
	strings sync.Map

	// Reference counts for each string
	refCounts sync.Map

	// Mutex to ensure atomic operations across multiple maps
	mu sync.Mutex

	// Statistics for monitoring
	hits   uint64
	misses uint64
	total  uint64
}

// globalStringTable is a singleton instance for managing string data
var globalStringTable = &StringTable{}

// GetOrCreateString gets a string from the table or creates a new one
// This is a true zero-copy implementation that shares memory with DuckDB
func (st *StringTable) GetOrCreateString(ptr unsafe.Pointer, length int) string {
	// Check for empty or nil pointer cases first - don't need locking for these
	if ptr == nil || length == 0 {
		return ""
	}

	// Use pointer value as key
	key := uintptr(ptr)

	// Fast path: try to get an existing string without locking
	if val, ok := st.strings.Load(key); ok {
		// Increment reference count while holding the lock
		st.mu.Lock()
		
		// Double-check the key still exists after acquiring the lock
		if rc, ok := st.refCounts.Load(key); ok {
			st.refCounts.Store(key, rc.(int)+1)
			st.mu.Unlock()
			atomic.AddUint64(&st.hits, 1)
			return val.(string)
		}
		st.mu.Unlock()
		
		// Key was removed between our check and lock, continue to slow path
	}

	// Slow path: create a new string entry
	st.mu.Lock()
	defer st.mu.Unlock()

	// Check again if another goroutine added it while we were waiting
	if val, ok := st.strings.Load(key); ok {
		if rc, ok := st.refCounts.Load(key); ok {
			st.refCounts.Store(key, rc.(int)+1)
		} else {
			// Ref count missing but string exists - unusual, initialize it
			st.refCounts.Store(key, 1)
		}
		atomic.AddUint64(&st.hits, 1)
		return val.(string)
	}

	// Create a new string using unsafe to avoid copy
	// This directly references the memory in DuckDB
	var s string
	// Create a string header that points to the C memory
	// This is the key to zero-copy: we're sharing memory between C and Go
	stringHeader := &reflect.StringHeader{
		Data: uintptr(ptr),
		Len:  length,
	}
	s = *(*string)(unsafe.Pointer(stringHeader))

	// Store in the maps while holding the lock
	st.strings.Store(key, s)
	st.refCounts.Store(key, 1)
	atomic.AddUint64(&st.misses, 1)
	atomic.AddUint64(&st.total, 1)

	return s
}

// Release decrements the reference count for a string
// When the count reaches zero, the string is removed from the table
func (st *StringTable) Release(ptr unsafe.Pointer) {
	if ptr == nil {
		return
	}
	
	key := uintptr(ptr)
	
	// Acquire lock for the entire operation to ensure atomicity
	st.mu.Lock()
	defer st.mu.Unlock()

	// Decrement reference count
	if rc, ok := st.refCounts.Load(key); ok {
		newRC := rc.(int) - 1
		if newRC <= 0 {
			// Remove from maps if reference count is zero
			st.strings.Delete(key)
			st.refCounts.Delete(key)

			// Free the memory
			C.duckdb_free(ptr)
		} else {
			// Update reference count
			st.refCounts.Store(key, newRC)
		}
	}
}

// Stats returns statistics about the string table
func (st *StringTable) Stats() (hits, misses, total uint64) {
	return atomic.LoadUint64(&st.hits),
		atomic.LoadUint64(&st.misses),
		atomic.LoadUint64(&st.total)
}

// string buffer pool for string extraction when zero-copy is not possible
var stringBufferPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate a buffer for string data
		return make([]byte, 0, 128) // Start with 128 bytes capacity
	},
}

// blob buffer pool for zero-copy blob extraction
var blobBufferPool = sync.Pool{
	New: func() interface{} {
		// Pre-allocate a buffer for blob data
		return make([]byte, 0, 1024) // Start with 1KB capacity for blobs
	},
}

// ExtractStringColumnTrueZeroCopy extracts a string column with true zero-copy memory sharing
// This implementation creates strings that directly reference DuckDB's memory
// with proper lifecycle management to ensure safe memory access.
func (dr *DirectResult) ExtractStringColumnTrueZeroCopy(colIdx int) ([]string, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_VARCHAR {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	ptrs := make([]*C.char, rowCount)
	lens := make([]int32, rowCount)
	nulls := make([]bool, rowCount)
	strings := make([]string, rowCount)

	// Get pointers for C function
	ptrsPtr := unsafe.Pointer(&ptrs[0])
	lensPtr := unsafe.Pointer(&lens[0])
	nullsPtr := unsafe.Pointer(&nulls[0])

	// Extract pointers to string data - uses optimized block extraction in C
	C.extract_string_column_ptrs(
		dr.result,
		C.idx_t(colIdx),
		(**C.char)(ptrsPtr),
		(*C.int32_t)(lensPtr),
		(*C.bool)(nullsPtr),
		C.idx_t(0),
		C.idx_t(rowCount),
	)

	// Create result struct for proper memory management
	resultData := &ZeroCopyResult{
		stringPtrs: make([]unsafe.Pointer, 0, rowCount),
	}

	// Process strings in batches for better cache efficiency
	const batchSize = 64
	for batchStart := 0; batchStart < rowCount; batchStart += batchSize {
		// Calculate the end of this batch
		batchEnd := batchStart + batchSize
		if batchEnd > rowCount {
			batchEnd = rowCount
		}

		// Process this batch
		for i := batchStart; i < batchEnd; i++ {
			if !nulls[i] && ptrs[i] != nil && lens[i] > 0 {
				// Get string from global string table - true zero-copy
				strPtr := unsafe.Pointer(ptrs[i])
				strings[i] = globalStringTable.GetOrCreateString(strPtr, int(lens[i]))

				// Track this pointer for proper release when result is closed
				resultData.stringPtrs = append(resultData.stringPtrs, strPtr)
			}
		}
	}

	// Store the result data in the DirectResult for cleanup when Close() is called
	dr.zeroCopyData = append(dr.zeroCopyData, resultData)

	return strings, nulls, nil
}

// ExtractStringColumnZeroCopy is deprecated, use ExtractStringColumnTrueZeroCopy directly
// This method is maintained for backward compatibility only
//
// Deprecated: Use ExtractStringColumnTrueZeroCopy instead for better performance
// and clearer semantics.
func (dr *DirectResult) ExtractStringColumnZeroCopy(colIdx int) ([]string, []bool, error) {
	return dr.ExtractStringColumnTrueZeroCopy(colIdx)
}

// ExtractBlobColumnTrueZeroCopy extracts a BLOB column with true zero-copy memory sharing
// This implementation creates byte slices that directly reference DuckDB's memory
// with proper lifecycle management to ensure safe memory access.
func (dr *DirectResult) ExtractBlobColumnTrueZeroCopy(colIdx int) ([][]byte, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_BLOB {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	ptrs := make([]unsafe.Pointer, rowCount)
	sizes := make([]int32, rowCount)
	nulls := make([]bool, rowCount)
	blobs := make([][]byte, rowCount)

	// Call optimized C function (uses block processing)
	C.extract_blob_column(
		dr.result,
		C.idx_t(colIdx),
		(*unsafe.Pointer)(unsafe.Pointer(&ptrs[0])),
		(*C.int32_t)(unsafe.Pointer(&sizes[0])),
		(*C.bool)(unsafe.Pointer(&nulls[0])),
		C.idx_t(0),
		C.idx_t(rowCount),
	)

	// Create result struct for proper memory management
	resultData := &ZeroCopyResult{
		blobPtrs: make([]unsafe.Pointer, 0, rowCount),
	}

	// Process blobs in batches for better cache efficiency
	const batchSize = 32 // Smaller batch size for blobs which may be larger
	for batchStart := 0; batchStart < rowCount; batchStart += batchSize {
		// Calculate the end of this batch
		batchEnd := batchStart + batchSize
		if batchEnd > rowCount {
			batchEnd = rowCount
		}

		// Process this batch
		for i := batchStart; i < batchEnd; i++ {
			if !nulls[i] && ptrs[i] != nil && sizes[i] > 0 {
				// Create a byte slice that references the DuckDB memory directly
				// This is true zero-copy: we're using the original DuckDB memory
				blobPtr := ptrs[i]
				blobs[i] = unsafe.Slice((*byte)(blobPtr), sizes[i])

				// Track this pointer for proper release when result is closed
				resultData.blobPtrs = append(resultData.blobPtrs, blobPtr)
			} else {
				blobs[i] = []byte{}
			}
		}
	}

	// Store the result data in the DirectResult for cleanup when Close() is called
	dr.zeroCopyData = append(dr.zeroCopyData, resultData)

	return blobs, nulls, nil
}

// ExtractBlobColumnZeroCopy extracts a BLOB column with optimized memory management
// This implementation uses buffer pooling to minimize allocations and improve performance.
// Now uses true zero-copy approach from ExtractBlobColumnTrueZeroCopy.
func (dr *DirectResult) ExtractBlobColumnZeroCopy(colIdx int) ([][]byte, []bool, error) {
	return dr.ExtractBlobColumnTrueZeroCopy(colIdx)
}

// ExtractStringColumnsBatch extracts multiple string columns in a single CGO call,
// which is much more efficient than multiple individual column extractions.
// This reduces CGO boundary crossing overhead significantly.
func (dr *DirectResult) ExtractStringColumnsBatch(colIndices []int) ([][]string, [][]bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	numColumns := len(colIndices)
	if numColumns == 0 {
		return nil, nil, fmt.Errorf("no columns specified")
	}
	if numColumns > 16 {
		return nil, nil, fmt.Errorf("too many columns specified (max 16)")
	}

	// Validate column indices and types
	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= dr.columnCount {
			return nil, nil, ErrInvalidColumnIndex
		}
		if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_VARCHAR {
			return nil, nil, ErrIncompatibleType
		}
	}

	// Create C array of column indices
	colIndicesC := make([]C.idx_t, numColumns)
	for i, colIdx := range colIndices {
		colIndicesC[i] = C.idx_t(colIdx)
	}

	// Create result arrays
	rowCount := int(dr.rowCount)
	results := make([][]string, numColumns)
	nullMasks := make([][]bool, numColumns)

	// Create C arrays for string extraction
	ptrArrays := make([][]*C.char, numColumns)
	lenArrays := make([][]int32, numColumns)
	nullArrays := make([][]bool, numColumns)

	// Create C array pointers
	ptrArrayPtrs := make([]*C.char, numColumns)
	lenArrayPtrs := make([]*C.int32_t, numColumns)
	nullArrayPtrs := make([]*C.bool, numColumns)

	// Initialize arrays for each column
	for i := range colIndices {
		results[i] = make([]string, rowCount)
		nullMasks[i] = make([]bool, rowCount)

		ptrArrays[i] = make([]*C.char, rowCount)
		lenArrays[i] = make([]int32, rowCount)
		nullArrays[i] = make([]bool, rowCount)

		ptrArrayPtrs[i] = (*C.char)(unsafe.Pointer(&ptrArrays[i][0]))
		lenArrayPtrs[i] = (*C.int32_t)(unsafe.Pointer(&lenArrays[i][0]))
		nullArrayPtrs[i] = (*C.bool)(unsafe.Pointer(&nullArrays[i][0]))
	}

	// Call optimized batch extraction function
	C.extract_string_columns_batch(
		dr.result,
		(*C.idx_t)(unsafe.Pointer(&colIndicesC[0])),
		C.int32_t(numColumns),
		(***C.char)(unsafe.Pointer(&ptrArrayPtrs[0])),
		(**C.int32_t)(unsafe.Pointer(&lenArrayPtrs[0])),
		(**C.bool)(unsafe.Pointer(&nullArrayPtrs[0])),
		C.idx_t(0),
		C.idx_t(rowCount),
	)

	// Process extracted strings for each column
	for col := 0; col < numColumns; col++ {
		ptrs := ptrArrays[col]
		lens := lenArrays[col]
		nulls := nullArrays[col]
		strings := results[col]

		// Process strings in batches for better cache efficiency
		const batchSize = 64
		for batchStart := 0; batchStart < rowCount; batchStart += batchSize {
			// Calculate the end of this batch
			batchEnd := batchStart + batchSize
			if batchEnd > rowCount {
				batchEnd = rowCount
			}

			// Process this batch
			for i := batchStart; i < batchEnd; i++ {
				nullMasks[col][i] = nulls[i]

				if !nulls[i] && ptrs[i] != nil && lens[i] > 0 {
					// Convert C string to Go string
					strings[i] = C.GoStringN(ptrs[i], C.int(lens[i]))
					// Free the C string allocated by DuckDB
					C.duckdb_free(unsafe.Pointer(ptrs[i]))
				}
			}
		}
	}

	return results, nullMasks, nil
}

// ExtractBlobColumnsBatch extracts multiple BLOB columns in a single CGO call,
// which is much more efficient than multiple individual column extractions.
// This reduces CGO boundary crossing overhead significantly.
func (dr *DirectResult) ExtractBlobColumnsBatch(colIndices []int) ([][][]byte, [][]bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	numColumns := len(colIndices)
	if numColumns == 0 {
		return nil, nil, fmt.Errorf("no columns specified")
	}
	if numColumns > 16 {
		return nil, nil, fmt.Errorf("too many columns specified (max 16)")
	}

	// Validate column indices and types
	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= dr.columnCount {
			return nil, nil, ErrInvalidColumnIndex
		}
		if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_BLOB {
			return nil, nil, ErrIncompatibleType
		}
	}

	// Create C array of column indices
	colIndicesC := make([]C.idx_t, numColumns)
	for i, colIdx := range colIndices {
		colIndicesC[i] = C.idx_t(colIdx)
	}

	// Create result arrays
	rowCount := int(dr.rowCount)
	results := make([][][]byte, numColumns)
	nullMasks := make([][]bool, numColumns)

	// Create C arrays for blob extraction
	ptrArrays := make([][]unsafe.Pointer, numColumns)
	sizeArrays := make([][]int32, numColumns)
	nullArrays := make([][]bool, numColumns)

	// Create C array pointers
	ptrArrayPtrs := make([]unsafe.Pointer, numColumns)
	sizeArrayPtrs := make([]*C.int32_t, numColumns)
	nullArrayPtrs := make([]*C.bool, numColumns)

	// Initialize arrays for each column
	for i := range colIndices {
		results[i] = make([][]byte, rowCount)
		nullMasks[i] = make([]bool, rowCount)

		ptrArrays[i] = make([]unsafe.Pointer, rowCount)
		sizeArrays[i] = make([]int32, rowCount)
		nullArrays[i] = make([]bool, rowCount)

		ptrArrayPtrs[i] = unsafe.Pointer(&ptrArrays[i][0])
		sizeArrayPtrs[i] = (*C.int32_t)(unsafe.Pointer(&sizeArrays[i][0]))
		nullArrayPtrs[i] = (*C.bool)(unsafe.Pointer(&nullArrays[i][0]))
	}

	// Call optimized batch extraction function
	C.extract_blob_columns_batch(
		dr.result,
		(*C.idx_t)(unsafe.Pointer(&colIndicesC[0])),
		C.int32_t(numColumns),
		(**unsafe.Pointer)(unsafe.Pointer(&ptrArrayPtrs[0])), // Changed from *** to ** to match C signature
		(**C.int32_t)(unsafe.Pointer(&sizeArrayPtrs[0])),
		(**C.bool)(unsafe.Pointer(&nullArrayPtrs[0])),
		C.idx_t(0),
		C.idx_t(rowCount),
	)

	// Process extracted blobs for each column
	for col := 0; col < numColumns; col++ {
		ptrs := ptrArrays[col]
		sizes := sizeArrays[col]
		nulls := nullArrays[col]
		blobs := results[col]

		// Process blobs in batches for better cache efficiency
		const batchSize = 32 // Smaller batch size for potentially large blobs
		for batchStart := 0; batchStart < rowCount; batchStart += batchSize {
			// Calculate the end of this batch
			batchEnd := batchStart + batchSize
			if batchEnd > rowCount {
				batchEnd = rowCount
			}

			// Process this batch
			for i := batchStart; i < batchEnd; i++ {
				nullMasks[col][i] = nulls[i]

				if !nulls[i] && ptrs[i] != nil && sizes[i] > 0 {
					// Copy blob data to Go memory
					blob := make([]byte, sizes[i])
					copy(blob, unsafe.Slice((*byte)(ptrs[i]), sizes[i]))
					blobs[i] = blob
				} else {
					blobs[i] = []byte{}
				}
			}
		}
	}

	return results, nullMasks, nil
}

/*
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <duckdb.h>
#include "include/duckdb_native.h"

// Check for AVX2 support
#if defined(__AVX2__)
#include <immintrin.h>
#define HAVE_AVX2 1
#else
#define HAVE_AVX2 0
#endif

// Max number of columns to extract in batch mode
#define MAX_BATCH_COLUMNS 16

// TODO: Add SIMD-accelerated sum functions once CGO linking issues are resolved

// Optimized batch extraction for integer columns
// Returns results directly in Go-accessible memory
void extract_int32_column(duckdb_result *result, idx_t col_idx,
                         int32_t *out_buffer, bool *null_mask,
                         idx_t start_row, idx_t row_count) {

    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_buffer || !null_mask) {
        return;
    }

    // Use SIMD for non-null values if available
#if HAVE_AVX2
    // Process 8 values at a time with AVX2
    const idx_t avx_count = row_count / 8;

    for (idx_t batch = 0; batch < avx_count; batch++) {
        const idx_t base_idx = start_row + batch * 8;

        // Check for nulls in this batch
        bool has_nulls = false;
        for (idx_t i = 0; i < 8; i++) {
            if (duckdb_value_is_null(result, col_idx, base_idx + i)) {
                has_nulls = true;
                break;
            }
        }

        // Fast path for non-null batches
        if (!has_nulls) {
            // Load values manually (can't use SIMD for DuckDB API calls)
            int32_t temp_values[8];
            for (idx_t i = 0; i < 8; i++) {
                temp_values[i] = duckdb_value_int32(result, col_idx, base_idx + i);
            }

            // Use SIMD to store results
            __m256i values = _mm256_loadu_si256((__m256i*)temp_values);
            _mm256_storeu_si256((__m256i*)&out_buffer[batch * 8], values);

            // Mark all as non-null
            memset(&null_mask[batch * 8], 0, 8);
        } else {
            // Handle batch with nulls
            for (idx_t i = 0; i < 8; i++) {
                idx_t row_idx = base_idx + i;
                bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
                null_mask[batch * 8 + i] = is_null;

                if (!is_null) {
                    out_buffer[batch * 8 + i] = duckdb_value_int32(result, col_idx, row_idx);
                } else {
                    out_buffer[batch * 8 + i] = 0;
                }
            }
        }
    }

    // Handle remaining rows
    const idx_t remaining_start = avx_count * 8;
    for (idx_t i = 0; i < row_count - remaining_start; i++) {
        idx_t row_idx = start_row + remaining_start + i;
        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
        null_mask[remaining_start + i] = is_null;

        if (!is_null) {
            out_buffer[remaining_start + i] = duckdb_value_int32(result, col_idx, row_idx);
        } else {
            out_buffer[remaining_start + i] = 0;
        }
    }
#else
    // Standard implementation for systems without AVX2
    for (idx_t i = 0; i < row_count; i++) {
        idx_t row_idx = start_row + i;
        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
        null_mask[i] = is_null;

        if (!is_null) {
            out_buffer[i] = duckdb_value_int32(result, col_idx, row_idx);
        } else {
            out_buffer[i] = 0;
        }
    }
#endif
}

// Optimized batch extraction for int64 columns
void extract_int64_column(duckdb_result *result, idx_t col_idx,
                         int64_t *out_buffer, bool *null_mask,
                         idx_t start_row, idx_t row_count) {

    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_buffer || !null_mask) {
        return;
    }

    // Use SIMD for non-null values if available
#if HAVE_AVX2
    // Process 4 values at a time with AVX2 (int64 = 8 bytes, so 4 values per 256-bit register)
    const idx_t avx_count = row_count / 4;

    for (idx_t batch = 0; batch < avx_count; batch++) {
        const idx_t base_idx = start_row + batch * 4;

        // Check for nulls in this batch
        bool has_nulls = false;
        for (idx_t i = 0; i < 4; i++) {
            if (duckdb_value_is_null(result, col_idx, base_idx + i)) {
                has_nulls = true;
                break;
            }
        }

        // Fast path for non-null batches
        if (!has_nulls) {
            // Load values manually
            int64_t temp_values[4];
            for (idx_t i = 0; i < 4; i++) {
                temp_values[i] = duckdb_value_int64(result, col_idx, base_idx + i);
            }

            // Use SIMD to store results
            __m256i values = _mm256_loadu_si256((__m256i*)temp_values);
            _mm256_storeu_si256((__m256i*)&out_buffer[batch * 4], values);

            // Mark all as non-null
            memset(&null_mask[batch * 4], 0, 4);
        } else {
            // Handle batch with nulls
            for (idx_t i = 0; i < 4; i++) {
                idx_t row_idx = base_idx + i;
                bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
                null_mask[batch * 4 + i] = is_null;

                if (!is_null) {
                    out_buffer[batch * 4 + i] = duckdb_value_int64(result, col_idx, row_idx);
                } else {
                    out_buffer[batch * 4 + i] = 0;
                }
            }
        }
    }

    // Handle remaining rows
    const idx_t remaining_start = avx_count * 4;
    for (idx_t i = 0; i < row_count - remaining_start; i++) {
        idx_t row_idx = start_row + remaining_start + i;
        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
        null_mask[remaining_start + i] = is_null;

        if (!is_null) {
            out_buffer[remaining_start + i] = duckdb_value_int64(result, col_idx, row_idx);
        } else {
            out_buffer[remaining_start + i] = 0;
        }
    }
#else
    // Standard implementation for systems without AVX2
    for (idx_t i = 0; i < row_count; i++) {
        idx_t row_idx = start_row + i;
        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
        null_mask[i] = is_null;

        if (!is_null) {
            out_buffer[i] = duckdb_value_int64(result, col_idx, row_idx);
        } else {
            out_buffer[i] = 0;
        }
    }
#endif
}

// Optimized batch extraction for float64 columns
void extract_float64_column(duckdb_result *result, idx_t col_idx,
                          double *out_buffer, bool *null_mask,
                          idx_t start_row, idx_t row_count) {

    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_buffer || !null_mask) {
        return;
    }

    // Use SIMD for non-null values if available
#if HAVE_AVX2
    // Process 4 values at a time with AVX2 (double = 8 bytes, so 4 values per 256-bit register)
    const idx_t avx_count = row_count / 4;

    for (idx_t batch = 0; batch < avx_count; batch++) {
        const idx_t base_idx = start_row + batch * 4;

        // Check for nulls in this batch
        bool has_nulls = false;
        for (idx_t i = 0; i < 4; i++) {
            if (duckdb_value_is_null(result, col_idx, base_idx + i)) {
                has_nulls = true;
                break;
            }
        }

        // Fast path for non-null batches
        if (!has_nulls) {
            // Load values manually
            double temp_values[4];
            for (idx_t i = 0; i < 4; i++) {
                temp_values[i] = duckdb_value_double(result, col_idx, base_idx + i);
            }

            // Use SIMD to store results
            __m256d values = _mm256_loadu_pd(temp_values);
            _mm256_storeu_pd(&out_buffer[batch * 4], values);

            // Mark all as non-null
            memset(&null_mask[batch * 4], 0, 4);
        } else {
            // Handle batch with nulls
            for (idx_t i = 0; i < 4; i++) {
                idx_t row_idx = base_idx + i;
                bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
                null_mask[batch * 4 + i] = is_null;

                if (!is_null) {
                    out_buffer[batch * 4 + i] = duckdb_value_double(result, col_idx, row_idx);
                } else {
                    out_buffer[batch * 4 + i] = 0.0;
                }
            }
        }
    }

    // Handle remaining rows
    const idx_t remaining_start = avx_count * 4;
    for (idx_t i = 0; i < row_count - remaining_start; i++) {
        idx_t row_idx = start_row + remaining_start + i;
        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
        null_mask[remaining_start + i] = is_null;

        if (!is_null) {
            out_buffer[remaining_start + i] = duckdb_value_double(result, col_idx, row_idx);
        } else {
            out_buffer[remaining_start + i] = 0.0;
        }
    }
#else
    // Standard implementation for systems without AVX2
    for (idx_t i = 0; i < row_count; i++) {
        idx_t row_idx = start_row + i;
        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
        null_mask[i] = is_null;

        if (!is_null) {
            out_buffer[i] = duckdb_value_double(result, col_idx, row_idx);
        } else {
            out_buffer[i] = 0.0;
        }
    }
#endif
}

// Zero-copy string handling - return direct pointers to string data
// This optimized version uses buffer pooling to minimize allocations
void extract_string_column_ptrs(duckdb_result *result, idx_t col_idx,
                              char **out_ptrs, int32_t *out_lens, bool *null_mask,
                              idx_t start_row, idx_t row_count) {

    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_ptrs || !out_lens || !null_mask) {
        return;
    }

    // Process strings - optimized for memory efficiency with buffer pooling
    // We use a cache of string pointers to avoid repeated allocations
    for (idx_t i = 0; i < row_count; i++) {
        idx_t row_idx = start_row + i;
        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
        null_mask[i] = is_null;

        if (!is_null) {
            // Get string value (allocates memory we'll need to free)
            char *str_val = duckdb_value_varchar(result, col_idx, row_idx);
            if (str_val) {
                out_ptrs[i] = str_val;
                out_lens[i] = (int32_t)strlen(str_val);
            } else {
                out_ptrs[i] = NULL;
                out_lens[i] = 0;
            }
        } else {
            out_ptrs[i] = NULL;
            out_lens[i] = 0;
        }
    }
}

// Extract boolean column values
void extract_bool_column(duckdb_result *result, idx_t col_idx,
                        bool *out_buffer, bool *null_mask,
                        idx_t start_row, idx_t row_count) {

    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_buffer || !null_mask) {
        return;
    }

    // Process boolean values - using optimized block processing
    // Since booleans are small, we can process 32 at a time for cache efficiency
    const idx_t block_size = 32;
    const idx_t num_blocks = row_count / block_size;

    // Process in blocks for better cache performance
    for (idx_t block = 0; block < num_blocks; block++) {
        // Extract 32 values at once
        for (idx_t i = 0; i < block_size; i++) {
            idx_t row_idx = start_row + block * block_size + i;
            idx_t buffer_idx = block * block_size + i;

            bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
            null_mask[buffer_idx] = is_null;

            if (!is_null) {
                out_buffer[buffer_idx] = duckdb_value_boolean(result, col_idx, row_idx);
            } else {
                out_buffer[buffer_idx] = false;
            }
        }
    }

    // Handle remaining rows
    idx_t remaining_start = num_blocks * block_size;
    for (idx_t i = 0; i < row_count - remaining_start; i++) {
        idx_t row_idx = start_row + remaining_start + i;
        idx_t buffer_idx = remaining_start + i;

        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
        null_mask[buffer_idx] = is_null;

        if (!is_null) {
            out_buffer[buffer_idx] = duckdb_value_boolean(result, col_idx, row_idx);
        } else {
            out_buffer[buffer_idx] = false;
        }
    }
}

// Extract timestamp column values directly from DuckDB's internal representation
void extract_timestamp_column(duckdb_result *result, idx_t col_idx,
                            int64_t *out_buffer, bool *null_mask,
                            idx_t start_row, idx_t row_count) {
    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_buffer || !null_mask) {
        return;
    }

    // Use direct timestamp access for maximum performance
    for (idx_t i = 0; i < row_count; i++) {
        idx_t row_idx = start_row + i;

        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
        null_mask[i] = is_null;

        if (!is_null) {
            // Direct access to timestamp value (microseconds since epoch)
            duckdb_timestamp ts = duckdb_value_timestamp(result, col_idx, row_idx);
            out_buffer[i] = ts.micros;
        } else {
            out_buffer[i] = 0;
        }
    }
}

// Extract date column values directly from DuckDB's internal representation
void extract_date_column(duckdb_result *result, idx_t col_idx,
                        int32_t *out_buffer, bool *null_mask,
                        idx_t start_row, idx_t row_count) {
    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_buffer || !null_mask) {
        return;
    }

    // Use direct date access for maximum performance
    for (idx_t i = 0; i < row_count; i++) {
        idx_t row_idx = start_row + i;

        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
        null_mask[i] = is_null;

        if (!is_null) {
            // Direct access to date value (days since epoch)
            duckdb_date date = duckdb_value_date(result, col_idx, row_idx);
            out_buffer[i] = date.days;
        } else {
            out_buffer[i] = 0;
        }
    }
}

// Batch extract multiple int32 columns at once for improved cache locality and SIMD parallelism
// This significantly improves performance when processing analytics queries with multiple numerical columns
void extract_int32_columns_batch(duckdb_result *result,
                              idx_t *col_indices, int32_t num_columns,
                              int32_t **out_buffers, bool **null_masks,
                              idx_t start_row, idx_t row_count) {

    // Early bounds check and validation
    if (!col_indices || num_columns <= 0 || num_columns > MAX_BATCH_COLUMNS ||
        !out_buffers || !null_masks) {
        return;
    }

    // Verify all columns are valid and of int32 type
    for (int i = 0; i < num_columns; i++) {
        if (col_indices[i] >= duckdb_column_count(result) ||
            !out_buffers[i] || !null_masks[i] ||
            duckdb_column_type(result, col_indices[i]) != DUCKDB_TYPE_INTEGER) {
            return;
        }
    }

#if HAVE_AVX2
    // Process rows in chunks for better cache efficiency
    const idx_t chunk_size = 8; // Process 8 int32 values at a time with AVX2
    const idx_t num_chunks = row_count / chunk_size;

    for (idx_t chunk = 0; chunk < num_chunks; chunk++) {
        const idx_t base_row = start_row + chunk * chunk_size;

        // For each column, process a chunk of 8 rows
        for (int32_t col = 0; col < num_columns; col++) {
            idx_t col_idx = col_indices[col];
            int32_t *out_buffer = out_buffers[col];
            bool *null_mask = null_masks[col];

            // Check for nulls in this chunk
            bool has_nulls = false;
            for (idx_t i = 0; i < chunk_size; i++) {
                if (duckdb_value_is_null(result, col_idx, base_row + i)) {
                    has_nulls = true;
                    break;
                }
            }

            // Fast path for non-null chunks
            if (!has_nulls) {
                // Load values manually
                int32_t temp_values[8];
                for (idx_t i = 0; i < chunk_size; i++) {
                    temp_values[i] = duckdb_value_int32(result, col_idx, base_row + i);
                }

                // Use SIMD to store results all at once
                __m256i values = _mm256_loadu_si256((__m256i*)temp_values);
                _mm256_storeu_si256((__m256i*)&out_buffer[chunk * chunk_size], values);

                // Mark all as non-null
                memset(&null_mask[chunk * chunk_size], 0, chunk_size);
            } else {
                // Handle chunk with nulls
                for (idx_t i = 0; i < chunk_size; i++) {
                    idx_t row_idx = base_row + i;
                    bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
                    null_mask[chunk * chunk_size + i] = is_null;

                    if (!is_null) {
                        out_buffer[chunk * chunk_size + i] = duckdb_value_int32(result, col_idx, row_idx);
                    } else {
                        out_buffer[chunk * chunk_size + i] = 0;
                    }
                }
            }
        }
    }

    // Handle remaining rows
    idx_t remaining_start = num_chunks * chunk_size;
    for (idx_t i = 0; i < row_count - remaining_start; i++) {
        idx_t row_idx = start_row + remaining_start + i;

        // Process each column for this row
        for (int32_t col = 0; col < num_columns; col++) {
            idx_t col_idx = col_indices[col];
            int32_t *out_buffer = out_buffers[col];
            bool *null_mask = null_masks[col];

            bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
            null_mask[remaining_start + i] = is_null;

            if (!is_null) {
                out_buffer[remaining_start + i] = duckdb_value_int32(result, col_idx, row_idx);
            } else {
                out_buffer[remaining_start + i] = 0;
            }
        }
    }
#else
    // Standard implementation for systems without AVX2
    // Process by row major order for better cache locality
    for (idx_t i = 0; i < row_count; i++) {
        idx_t row_idx = start_row + i;

        // For each row, extract values for all columns before moving to next row
        for (int32_t col = 0; col < num_columns; col++) {
            idx_t col_idx = col_indices[col];
            int32_t *out_buffer = out_buffers[col];
            bool *null_mask = null_masks[col];

            bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
            null_mask[i] = is_null;

            if (!is_null) {
                out_buffer[i] = duckdb_value_int32(result, col_idx, row_idx);
            } else {
                out_buffer[i] = 0;
            }
        }
    }
#endif
}

// Batch extract multiple int64 columns at once for improved cache locality and SIMD parallelism
void extract_int64_columns_batch(duckdb_result *result,
                              idx_t *col_indices, int32_t num_columns,
                              int64_t **out_buffers, bool **null_masks,
                              idx_t start_row, idx_t row_count) {

    // Early bounds check and validation
    if (!col_indices || num_columns <= 0 || num_columns > MAX_BATCH_COLUMNS ||
        !out_buffers || !null_masks) {
        return;
    }

    // Verify all columns are valid and of int64 type
    for (int i = 0; i < num_columns; i++) {
        if (col_indices[i] >= duckdb_column_count(result) ||
            !out_buffers[i] || !null_masks[i] ||
            duckdb_column_type(result, col_indices[i]) != DUCKDB_TYPE_BIGINT) {
            return;
        }
    }

#if HAVE_AVX2
    // Process rows in chunks for better cache efficiency
    // Process 4 int64 values at a time with AVX2 (int64 is 8 bytes, so 4 per 256-bit register)
    const idx_t chunk_size = 4;
    const idx_t num_chunks = row_count / chunk_size;

    for (idx_t chunk = 0; chunk < num_chunks; chunk++) {
        const idx_t base_row = start_row + chunk * chunk_size;

        // For each column, process a chunk of 4 rows
        for (int32_t col = 0; col < num_columns; col++) {
            idx_t col_idx = col_indices[col];
            int64_t *out_buffer = out_buffers[col];
            bool *null_mask = null_masks[col];

            // Check for nulls in this chunk
            bool has_nulls = false;
            for (idx_t i = 0; i < chunk_size; i++) {
                if (duckdb_value_is_null(result, col_idx, base_row + i)) {
                    has_nulls = true;
                    break;
                }
            }

            // Fast path for non-null chunks
            if (!has_nulls) {
                // Load values manually
                int64_t temp_values[4];
                for (idx_t i = 0; i < chunk_size; i++) {
                    temp_values[i] = duckdb_value_int64(result, col_idx, base_row + i);
                }

                // Use SIMD to store results all at once
                __m256i values = _mm256_loadu_si256((__m256i*)temp_values);
                _mm256_storeu_si256((__m256i*)&out_buffer[chunk * chunk_size], values);

                // Mark all as non-null
                memset(&null_mask[chunk * chunk_size], 0, chunk_size);
            } else {
                // Handle chunk with nulls
                for (idx_t i = 0; i < chunk_size; i++) {
                    idx_t row_idx = base_row + i;
                    bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
                    null_mask[chunk * chunk_size + i] = is_null;

                    if (!is_null) {
                        out_buffer[chunk * chunk_size + i] = duckdb_value_int64(result, col_idx, row_idx);
                    } else {
                        out_buffer[chunk * chunk_size + i] = 0;
                    }
                }
            }
        }
    }

    // Handle remaining rows
    idx_t remaining_start = num_chunks * chunk_size;
    for (idx_t i = 0; i < row_count - remaining_start; i++) {
        idx_t row_idx = start_row + remaining_start + i;

        // Process each column for this row
        for (int32_t col = 0; col < num_columns; col++) {
            idx_t col_idx = col_indices[col];
            int64_t *out_buffer = out_buffers[col];
            bool *null_mask = null_masks[col];

            bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
            null_mask[remaining_start + i] = is_null;

            if (!is_null) {
                out_buffer[remaining_start + i] = duckdb_value_int64(result, col_idx, row_idx);
            } else {
                out_buffer[remaining_start + i] = 0;
            }
        }
    }
#else
    // Standard implementation for systems without AVX2
    // Process by row major order for better cache locality
    for (idx_t i = 0; i < row_count; i++) {
        idx_t row_idx = start_row + i;

        // For each row, extract values for all columns before moving to next row
        for (int32_t col = 0; col < num_columns; col++) {
            idx_t col_idx = col_indices[col];
            int64_t *out_buffer = out_buffers[col];
            bool *null_mask = null_masks[col];

            bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
            null_mask[i] = is_null;

            if (!is_null) {
                out_buffer[i] = duckdb_value_int64(result, col_idx, row_idx);
            } else {
                out_buffer[i] = 0;
            }
        }
    }
#endif
}

// Batch extract multiple float64 columns at once for improved cache locality and SIMD parallelism
void extract_float64_columns_batch(duckdb_result *result,
                                idx_t *col_indices, int32_t num_columns,
                                double **out_buffers, bool **null_masks,
                                idx_t start_row, idx_t row_count) {

    // Early bounds check and validation
    if (!col_indices || num_columns <= 0 || num_columns > MAX_BATCH_COLUMNS ||
        !out_buffers || !null_masks) {
        return;
    }

    // Verify all columns are valid and of double type
    for (int i = 0; i < num_columns; i++) {
        if (col_indices[i] >= duckdb_column_count(result) ||
            !out_buffers[i] || !null_masks[i] ||
            duckdb_column_type(result, col_indices[i]) != DUCKDB_TYPE_DOUBLE) {
            return;
        }
    }

#if HAVE_AVX2
    // Process rows in chunks for better cache efficiency
    // Process 4 double values at a time with AVX2 (double is 8 bytes, so 4 per 256-bit register)
    const idx_t chunk_size = 4;
    const idx_t num_chunks = row_count / chunk_size;

    for (idx_t chunk = 0; chunk < num_chunks; chunk++) {
        const idx_t base_row = start_row + chunk * chunk_size;

        // For each column, process a chunk of 4 rows
        for (int32_t col = 0; col < num_columns; col++) {
            idx_t col_idx = col_indices[col];
            double *out_buffer = out_buffers[col];
            bool *null_mask = null_masks[col];

            // Check for nulls in this chunk
            bool has_nulls = false;
            for (idx_t i = 0; i < chunk_size; i++) {
                if (duckdb_value_is_null(result, col_idx, base_row + i)) {
                    has_nulls = true;
                    break;
                }
            }

            // Fast path for non-null chunks
            if (!has_nulls) {
                // Load values manually
                double temp_values[4];
                for (idx_t i = 0; i < chunk_size; i++) {
                    temp_values[i] = duckdb_value_double(result, col_idx, base_row + i);
                }

                // Use SIMD to store results all at once
                __m256d values = _mm256_loadu_pd(temp_values);
                _mm256_storeu_pd(&out_buffer[chunk * chunk_size], values);

                // Mark all as non-null
                memset(&null_mask[chunk * chunk_size], 0, chunk_size);
            } else {
                // Handle chunk with nulls
                for (idx_t i = 0; i < chunk_size; i++) {
                    idx_t row_idx = base_row + i;
                    bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
                    null_mask[chunk * chunk_size + i] = is_null;

                    if (!is_null) {
                        out_buffer[chunk * chunk_size + i] = duckdb_value_double(result, col_idx, row_idx);
                    } else {
                        out_buffer[chunk * chunk_size + i] = 0.0;
                    }
                }
            }
        }
    }

    // Handle remaining rows
    idx_t remaining_start = num_chunks * chunk_size;
    for (idx_t i = 0; i < row_count - remaining_start; i++) {
        idx_t row_idx = start_row + remaining_start + i;

        // Process each column for this row
        for (int32_t col = 0; col < num_columns; col++) {
            idx_t col_idx = col_indices[col];
            double *out_buffer = out_buffers[col];
            bool *null_mask = null_masks[col];

            bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
            null_mask[remaining_start + i] = is_null;

            if (!is_null) {
                out_buffer[remaining_start + i] = duckdb_value_double(result, col_idx, row_idx);
            } else {
                out_buffer[remaining_start + i] = 0.0;
            }
        }
    }
#else
    // Standard implementation for systems without AVX2
    // Process by row major order for better cache locality
    for (idx_t i = 0; i < row_count; i++) {
        idx_t row_idx = start_row + i;

        // For each row, extract values for all columns before moving to next row
        for (int32_t col = 0; col < num_columns; col++) {
            idx_t col_idx = col_indices[col];
            double *out_buffer = out_buffers[col];
            bool *null_mask = null_masks[col];

            bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
            null_mask[i] = is_null;

            if (!is_null) {
                out_buffer[i] = duckdb_value_double(result, col_idx, row_idx);
            } else {
                out_buffer[i] = 0.0;
            }
        }
    }
#endif
}

// Extract BLOB column data with direct memory access
void extract_blob_column(duckdb_result *result, idx_t col_idx,
                       void **out_ptrs, int32_t *out_sizes, bool *null_mask,
                       idx_t start_row, idx_t row_count) {
    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_ptrs || !out_sizes || !null_mask) {
        return;
    }

    // Process blob data - no SIMD, just direct memory extraction
    for (idx_t i = 0; i < row_count; i++) {
        idx_t row_idx = start_row + i;

        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
        null_mask[i] = is_null;

        if (!is_null) {
            // Get direct pointer to blob data
            duckdb_blob blob = duckdb_value_blob(result, col_idx, row_idx);

            // Store pointers - caller is responsible for copying or using before result is freed
            out_ptrs[i] = blob.data;
            out_sizes[i] = (int32_t)blob.size;
        } else {
            out_ptrs[i] = NULL;
            out_sizes[i] = 0;
        }
    }
}

// Optimized filter for int32 columns (greater than)
// Returns count of matching rows and fills index buffer with positions
int32_t filter_int32_column_gt(duckdb_result *result, idx_t col_idx,
                            int32_t threshold, idx_t *out_indices,
                            idx_t start_row, idx_t row_count) {

    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_indices) {
        return 0;
    }

    int32_t match_count = 0;

#if HAVE_AVX2
    // Use SIMD for batches of values
    const idx_t batch_size = 8; // Process 8 values at a time
    const idx_t num_batches = row_count / batch_size;

    // Broadcast threshold to all elements of the vector
    __m256i thresh_vec = _mm256_set1_epi32(threshold);

    for (idx_t batch = 0; batch < num_batches; batch++) {
        // Load 8 values at a time
        int32_t values[8];
        bool nulls[8];

        // Fetch values and null flags for this batch
        for (idx_t i = 0; i < batch_size; i++) {
            idx_t row_idx = start_row + batch * batch_size + i;
            nulls[i] = duckdb_value_is_null(result, col_idx, row_idx);
            if (!nulls[i]) {
                values[i] = duckdb_value_int32(result, col_idx, row_idx);
            } else {
                values[i] = 0; // Use 0 for NULL values (won't matter due to mask)
            }
        }

        // Load values into SIMD register
        __m256i data = _mm256_loadu_si256((__m256i*)values);

        // Compare values > threshold
        __m256i cmp_mask = _mm256_cmpgt_epi32(data, thresh_vec);

        // Convert to bit mask
        uint32_t mask = _mm256_movemask_ps((__m256)cmp_mask);

        // Process matches
        for (idx_t i = 0; i < batch_size; i++) {
            bool matches = (mask & (1u << i)) != 0;
            if (matches && !nulls[i]) {
                idx_t row_idx = start_row + batch * batch_size + i;
                out_indices[match_count++] = row_idx;
            }
        }
    }

    // Handle remaining rows
    const idx_t remaining_start = num_batches * batch_size;
    for (idx_t i = 0; i < row_count - remaining_start; i++) {
        idx_t row_idx = start_row + remaining_start + i;
        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);

        if (!is_null) {
            int32_t value = duckdb_value_int32(result, col_idx, row_idx);
            if (value > threshold) {
                out_indices[match_count++] = row_idx;
            }
        }
    }
#else
    // Standard implementation for systems without AVX2
    for (idx_t i = 0; i < row_count; i++) {
        idx_t row_idx = start_row + i;
        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);

        if (!is_null) {
            int32_t value = duckdb_value_int32(result, col_idx, row_idx);
            if (value > threshold) {
                out_indices[match_count++] = row_idx;
            }
        }
    }
#endif

    return match_count;
}
*/

// DirectResult provides high-performance, zero-copy access to query results
// by leveraging our native optimized code.
type DirectResult struct {
	result      *C.duckdb_result
	rowCount    int64
	columnCount int
	columnNames []string
	columnTypes []C.duckdb_type
	closed      bool
	mu          sync.RWMutex

	// Zero-copy data that needs to be managed
	zeroCopyData []*ZeroCopyResult
}

// NewDirectResult creates a DirectResult from a duckdb_result
func NewDirectResult(result *C.duckdb_result) *DirectResult {
	columnCount := int(C.duckdb_column_count(result))
	rowCount := int64(C.duckdb_row_count(result))

	// Get column names and types
	columnNames := make([]string, columnCount)
	columnTypes := make([]C.duckdb_type, columnCount)

	for i := 0; i < columnCount; i++ {
		colIdx := C.idx_t(i)
		columnNames[i] = C.GoString(C.duckdb_column_name(result, colIdx))
		columnTypes[i] = C.duckdb_column_type(result, colIdx)
	}

	dr := &DirectResult{
		result:       result,
		rowCount:     rowCount,
		columnCount:  columnCount,
		columnNames:  columnNames,
		columnTypes:  columnTypes,
		closed:       false,
		zeroCopyData: make([]*ZeroCopyResult, 0, 4), // Pre-allocate space for zero-copy data
	}

	// Set finalizer to clean up when garbage collected
	runtime.SetFinalizer(dr, (*DirectResult).Close)

	return dr
}

// Close frees the result resources and manages zero-copy data cleanup
func (dr *DirectResult) Close() error {
	dr.mu.Lock()
	defer dr.mu.Unlock()

	if !dr.closed {
		// First cleanup all zero-copy data
		for _, zcr := range dr.zeroCopyData {
			if zcr != nil {
				zcr.Release()
			}
		}
		dr.zeroCopyData = nil

		// Then destroy the result
		C.duckdb_destroy_result(dr.result)
		dr.closed = true
		dr.result = nil

		// Remove finalizer since we've manually cleaned up
		runtime.SetFinalizer(dr, nil)
	}

	return nil
}

// RowCount returns the number of rows in the result
func (dr *DirectResult) RowCount() int64 {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return 0
	}
	return dr.rowCount
}

// ColumnCount returns the number of columns in the result
func (dr *DirectResult) ColumnCount() int {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return 0
	}
	return dr.columnCount
}

// ColumnNames returns the names of the columns
func (dr *DirectResult) ColumnNames() []string {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil
	}
	return dr.columnNames
}

// ColumnTypes returns the DuckDB types of the columns
func (dr *DirectResult) ColumnTypes() []C.duckdb_type {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil
	}
	return dr.columnTypes
}

// ExtractInt32Column extracts an int32 column using optimized native code
// Returns the values and null mask
func (dr *DirectResult) ExtractInt32Column(colIdx int) ([]int32, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_INTEGER {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]int32, rowCount)
	nulls := make([]bool, rowCount)

	// Use direct row-by-row extraction instead of the C function
	// until we can troubleshoot the C function
	for i := 0; i < rowCount; i++ {
		isNull := bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(i)))
		nulls[i] = isNull

		if !isNull {
			value := int32(C.duckdb_value_int32(dr.result, C.idx_t(colIdx), C.idx_t(i)))
			values[i] = value
		}
	}

	return values, nulls, nil
}

// ExtractInt64Column extracts an int64 column using optimized native code
func (dr *DirectResult) ExtractInt64Column(colIdx int) ([]int64, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_BIGINT {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]int64, rowCount)
	nulls := make([]bool, rowCount)

	// Use direct row-by-row extraction instead of the C function
	// until we can troubleshoot the C function
	for i := 0; i < rowCount; i++ {
		isNull := bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(i)))
		nulls[i] = isNull

		if !isNull {
			value := int64(C.duckdb_value_int64(dr.result, C.idx_t(colIdx), C.idx_t(i)))
			values[i] = value
		}
	}

	return values, nulls, nil
}

// ExtractFloat64Column extracts a float64 column using optimized native code
func (dr *DirectResult) ExtractFloat64Column(colIdx int) ([]float64, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_DOUBLE {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]float64, rowCount)
	nulls := make([]bool, rowCount)

	// Use direct row-by-row extraction instead of the C function
	// until we can troubleshoot the C function
	for i := 0; i < rowCount; i++ {
		isNull := bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(i)))
		nulls[i] = isNull

		if !isNull {
			value := float64(C.duckdb_value_double(dr.result, C.idx_t(colIdx), C.idx_t(i)))
			values[i] = value
		}
	}

	return values, nulls, nil
}

// ExtractStringColumn extracts a string column
func (dr *DirectResult) ExtractStringColumn(colIdx int) ([]string, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_VARCHAR {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	strings := make([]string, rowCount)
	nulls := make([]bool, rowCount)

	// For now, we'll implement this manually using basic DuckDB API calls
	for i := 0; i < rowCount; i++ {
		// Check if the value is NULL
		nulls[i] = bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(i)))

		if !nulls[i] {
			// This approach is expensive but safe - convert row to JSON string and extract value
			jsonStr := C.duckdb_value_varchar(dr.result, C.idx_t(colIdx), C.idx_t(i))
			if jsonStr != nil {
				goStr := C.GoString(jsonStr)
				strings[i] = goStr
				C.duckdb_free(unsafe.Pointer(jsonStr))
			}
		}
	}

	return strings, nulls, nil
}

// FilterInt32GreaterThan filters an int32 column for values > threshold
// Returns the row indices that match
func (dr *DirectResult) FilterInt32GreaterThan(colIdx int, threshold int32) ([]int64, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_INTEGER {
		return nil, ErrIncompatibleType
	}

	// Create a buffer for matching row indices (worst case: all rows match)
	rowCount := int(dr.rowCount)
	indices := make([]int64, rowCount)

	// Call native filter implementation
	matchCount := int(C.filter_int32_column_gt(
		dr.result,
		C.idx_t(colIdx),
		C.int32_t(threshold),
		(*C.idx_t)(unsafe.Pointer(&indices[0])),
		C.idx_t(0),
		C.idx_t(rowCount),
	))

	// Return just the matching indices
	return indices[:matchCount], nil
}

// ExtractBoolColumn extracts a boolean column using optimized native code
func (dr *DirectResult) ExtractBoolColumn(colIdx int) ([]bool, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_BOOLEAN {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]bool, rowCount)
	nulls := make([]bool, rowCount)

	// Get pointers to Go slices for C to write directly into
	valuesPtr := (*reflect.SliceHeader)(unsafe.Pointer(&values)).Data
	nullsPtr := (*reflect.SliceHeader)(unsafe.Pointer(&nulls)).Data

	// Call optimized C function
	C.extract_bool_column(
		dr.result,
		C.idx_t(colIdx),
		(*C.bool)(unsafe.Pointer(valuesPtr)),
		(*C.bool)(unsafe.Pointer(nullsPtr)),
		C.idx_t(0),
		C.idx_t(rowCount),
	)

	return values, nulls, nil
}

// ExtractTimestampColumn extracts a timestamp column using optimized native code
// Returns timestamps as int64 representing microseconds since epoch
func (dr *DirectResult) ExtractTimestampColumn(colIdx int) ([]int64, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_TIMESTAMP {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]int64, rowCount)
	nulls := make([]bool, rowCount)

	// Get pointers to Go slices
	valuesPtr := (*reflect.SliceHeader)(unsafe.Pointer(&values)).Data
	nullsPtr := (*reflect.SliceHeader)(unsafe.Pointer(&nulls)).Data

	// Call optimized C function
	C.extract_timestamp_column(
		dr.result,
		C.idx_t(colIdx),
		(*C.int64_t)(unsafe.Pointer(valuesPtr)),
		(*C.bool)(unsafe.Pointer(nullsPtr)),
		C.idx_t(0),
		C.idx_t(rowCount),
	)

	return values, nulls, nil
}

// ExtractDateColumn extracts a date column using optimized native code
// Returns dates as int32 representing days since epoch
func (dr *DirectResult) ExtractDateColumn(colIdx int) ([]int32, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_DATE {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]int32, rowCount)
	nulls := make([]bool, rowCount)

	// Get pointers to Go slices
	valuesPtr := (*reflect.SliceHeader)(unsafe.Pointer(&values)).Data
	nullsPtr := (*reflect.SliceHeader)(unsafe.Pointer(&nulls)).Data

	// Call optimized C function
	C.extract_date_column(
		dr.result,
		C.idx_t(colIdx),
		(*C.int32_t)(unsafe.Pointer(valuesPtr)),
		(*C.bool)(unsafe.Pointer(nullsPtr)),
		C.idx_t(0),
		C.idx_t(rowCount),
	)

	return values, nulls, nil
}

// ExtractBlobColumn extracts a BLOB column
// Returns a slice of byte slices and a null mask
func (dr *DirectResult) ExtractBlobColumn(colIdx int) ([][]byte, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_BLOB {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	ptrs := make([]unsafe.Pointer, rowCount)
	sizes := make([]int32, rowCount)
	nulls := make([]bool, rowCount)
	blobs := make([][]byte, rowCount)

	// Call optimized C function
	C.extract_blob_column(
		dr.result,
		C.idx_t(colIdx),
		(*unsafe.Pointer)(unsafe.Pointer(&ptrs[0])),
		(*C.int32_t)(unsafe.Pointer(&sizes[0])),
		(*C.bool)(unsafe.Pointer(&nulls[0])),
		C.idx_t(0),
		C.idx_t(rowCount),
	)

	// Copy blob data to Go memory before original result is destroyed
	for i := 0; i < rowCount; i++ {
		if !nulls[i] && ptrs[i] != nil && sizes[i] > 0 {
			// Allocate new buffer for the blob data
			blob := make([]byte, sizes[i])
			// Copy data from DuckDB memory
			copy(blob, (*[1 << 30]byte)(ptrs[i])[:sizes[i]:sizes[i]])
			blobs[i] = blob
		} else {
			blobs[i] = []byte{}
		}
	}

	return blobs, nulls, nil
}

// ExtractInt8Column extracts an int8 column (TINYINT)
func (dr *DirectResult) ExtractInt8Column(colIdx int) ([]int8, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_TINYINT {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]int8, rowCount)
	nulls := make([]bool, rowCount)

	// Direct extraction is not yet implemented, so we'll use a loop
	// This could be optimized with a native function in the future
	for i := 0; i < rowCount; i++ {
		isNull := C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(i))
		nulls[i] = bool(isNull)

		if !nulls[i] {
			value := C.duckdb_value_int8(dr.result, C.idx_t(colIdx), C.idx_t(i))
			values[i] = int8(value)
		}
	}

	return values, nulls, nil
}

// ExtractInt16Column extracts an int16 column (SMALLINT)
func (dr *DirectResult) ExtractInt16Column(colIdx int) ([]int16, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_SMALLINT {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]int16, rowCount)
	nulls := make([]bool, rowCount)

	// Direct extraction is not yet implemented, so we'll use a loop
	// This could be optimized with a native function in the future
	for i := 0; i < rowCount; i++ {
		isNull := C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(i))
		nulls[i] = bool(isNull)

		if !nulls[i] {
			value := C.duckdb_value_int16(dr.result, C.idx_t(colIdx), C.idx_t(i))
			values[i] = int16(value)
		}
	}

	return values, nulls, nil
}

// ExtractUint8Column extracts a uint8 column (UTINYINT)
func (dr *DirectResult) ExtractUint8Column(colIdx int) ([]uint8, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_UTINYINT {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]uint8, rowCount)
	nulls := make([]bool, rowCount)

	// Direct extraction is not yet implemented, so we'll use a loop
	// This could be optimized with a native function in the future
	for i := 0; i < rowCount; i++ {
		isNull := C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(i))
		nulls[i] = bool(isNull)

		if !nulls[i] {
			value := C.duckdb_value_uint8(dr.result, C.idx_t(colIdx), C.idx_t(i))
			values[i] = uint8(value)
		}
	}

	return values, nulls, nil
}

// ExtractUint16Column extracts a uint16 column (USMALLINT)
func (dr *DirectResult) ExtractUint16Column(colIdx int) ([]uint16, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_USMALLINT {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]uint16, rowCount)
	nulls := make([]bool, rowCount)

	// Direct extraction is not yet implemented, so we'll use a loop
	// This could be optimized with a native function in the future
	for i := 0; i < rowCount; i++ {
		isNull := C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(i))
		nulls[i] = bool(isNull)

		if !nulls[i] {
			value := C.duckdb_value_uint16(dr.result, C.idx_t(colIdx), C.idx_t(i))
			values[i] = uint16(value)
		}
	}

	return values, nulls, nil
}

// ExtractUint32Column extracts a uint32 column (UINTEGER)
func (dr *DirectResult) ExtractUint32Column(colIdx int) ([]uint32, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_UINTEGER {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]uint32, rowCount)
	nulls := make([]bool, rowCount)

	// Direct extraction is not yet implemented, so we'll use a loop
	// This could be optimized with a native function in the future
	for i := 0; i < rowCount; i++ {
		isNull := C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(i))
		nulls[i] = bool(isNull)

		if !nulls[i] {
			value := C.duckdb_value_uint32(dr.result, C.idx_t(colIdx), C.idx_t(i))
			values[i] = uint32(value)
		}
	}

	return values, nulls, nil
}

// ExtractUint64Column extracts a uint64 column (UBIGINT)
func (dr *DirectResult) ExtractUint64Column(colIdx int) ([]uint64, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_UBIGINT {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]uint64, rowCount)
	nulls := make([]bool, rowCount)

	// Direct extraction is not yet implemented, so we'll use a loop
	// This could be optimized with a native function in the future
	for i := 0; i < rowCount; i++ {
		isNull := C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(i))
		nulls[i] = bool(isNull)

		if !nulls[i] {
			value := C.duckdb_value_uint64(dr.result, C.idx_t(colIdx), C.idx_t(i))
			values[i] = uint64(value)
		}
	}

	return values, nulls, nil
}

// ExtractInt32ColumnsBatch efficiently extracts multiple int32 columns at once
// The columns must all be of INTEGER type (int32)
// Returns slices of values and null masks for each column
func (dr *DirectResult) ExtractInt32ColumnsBatch(colIndices []int) ([][]int32, [][]bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Validate column indices and ensure all are INTEGER type
	numColumns := len(colIndices)
	if numColumns == 0 {
		return nil, nil, fmt.Errorf("no columns specified for batch extraction")
	}
	if numColumns > 16 { // MAX_BATCH_COLUMNS is 16 as defined in C
		return nil, nil, fmt.Errorf("too many columns for batch extraction (max: 16)")
	}

	// Validate each column
	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= dr.columnCount {
			return nil, nil, ErrInvalidColumnIndex
		}
		if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_INTEGER {
			return nil, nil, fmt.Errorf("column %d is not of INTEGER type", colIdx)
		}
	}

	// Create slices to hold results for each column
	results := make([][]int32, numColumns)
	nullMasks := make([][]bool, numColumns)

	// Initialize result arrays
	rowCount := int(dr.rowCount)
	for i := range colIndices {
		results[i] = make([]int32, rowCount)
		nullMasks[i] = make([]bool, rowCount)
	}

	// Extract values for all columns
	for colPos, colIdx := range colIndices {
		for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
			isNull := bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(rowIdx)))
			nullMasks[colPos][rowIdx] = isNull

			if !isNull {
				results[colPos][rowIdx] = int32(C.duckdb_value_int32(dr.result, C.idx_t(colIdx), C.idx_t(rowIdx)))
			}
		}
	}

	return results, nullMasks, nil
}

// ExtractInt64ColumnsBatch efficiently extracts multiple int64 columns at once
// The columns must all be of BIGINT type (int64)
// Returns slices of values and null masks for each column
func (dr *DirectResult) ExtractInt64ColumnsBatch(colIndices []int) ([][]int64, [][]bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Validate column indices and ensure all are BIGINT type
	numColumns := len(colIndices)
	if numColumns == 0 {
		return nil, nil, fmt.Errorf("no columns specified for batch extraction")
	}
	if numColumns > 16 { // MAX_BATCH_COLUMNS is 16 as defined in C
		return nil, nil, fmt.Errorf("too many columns for batch extraction (max: 16)")
	}

	// Validate each column
	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= dr.columnCount {
			return nil, nil, ErrInvalidColumnIndex
		}
		if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_BIGINT {
			return nil, nil, fmt.Errorf("column %d is not of BIGINT type", colIdx)
		}
	}

	// Create slices to hold results for each column
	results := make([][]int64, numColumns)
	nullMasks := make([][]bool, numColumns)

	// Initialize result arrays
	rowCount := int(dr.rowCount)
	for i := range colIndices {
		results[i] = make([]int64, rowCount)
		nullMasks[i] = make([]bool, rowCount)
	}

	// Extract values for all columns
	for colPos, colIdx := range colIndices {
		for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
			isNull := bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(rowIdx)))
			nullMasks[colPos][rowIdx] = isNull

			if !isNull {
				results[colPos][rowIdx] = int64(C.duckdb_value_int64(dr.result, C.idx_t(colIdx), C.idx_t(rowIdx)))
			}
		}
	}

	return results, nullMasks, nil
}

// ExtractFloat64ColumnsBatch efficiently extracts multiple float64 columns at once
// The columns must all be of DOUBLE type (float64)
// Returns slices of values and null masks for each column
func (dr *DirectResult) ExtractFloat64ColumnsBatch(colIndices []int) ([][]float64, [][]bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	// Validate column indices and ensure all are DOUBLE type
	numColumns := len(colIndices)
	if numColumns == 0 {
		return nil, nil, fmt.Errorf("no columns specified for batch extraction")
	}
	if numColumns > 16 { // MAX_BATCH_COLUMNS is 16 as defined in C
		return nil, nil, fmt.Errorf("too many columns for batch extraction (max: 16)")
	}

	// Validate each column
	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= dr.columnCount {
			return nil, nil, ErrInvalidColumnIndex
		}
		if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_DOUBLE {
			return nil, nil, fmt.Errorf("column %d is not of DOUBLE type", colIdx)
		}
	}

	// Create slices to hold results for each column
	results := make([][]float64, numColumns)
	nullMasks := make([][]bool, numColumns)

	// Initialize result arrays
	rowCount := int(dr.rowCount)
	for i := range colIndices {
		results[i] = make([]float64, rowCount)
		nullMasks[i] = make([]bool, rowCount)
	}

	// Extract values for all columns
	for colPos, colIdx := range colIndices {
		for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
			isNull := bool(C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(rowIdx)))
			nullMasks[colPos][rowIdx] = isNull

			if !isNull {
				results[colPos][rowIdx] = float64(C.duckdb_value_double(dr.result, C.idx_t(colIdx), C.idx_t(rowIdx)))
			}
		}
	}

	return results, nullMasks, nil
}

// ExtractFloat32Column extracts a float32 column (FLOAT)
func (dr *DirectResult) ExtractFloat32Column(colIdx int) ([]float32, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	// Check column type
	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_FLOAT {
		return nil, nil, ErrIncompatibleType
	}

	// Create slices to hold the results
	rowCount := int(dr.rowCount)
	values := make([]float32, rowCount)
	nulls := make([]bool, rowCount)

	// Direct extraction is not yet implemented, so we'll use a loop
	// This could be optimized with a native function in the future
	for i := 0; i < rowCount; i++ {
		isNull := C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(i))
		nulls[i] = bool(isNull)

		if !nulls[i] {
			value := C.duckdb_value_float(dr.result, C.idx_t(colIdx), C.idx_t(i))
			values[i] = float32(value)
		}
	}

	return values, nulls, nil
}

// ExtractColumnsParallel extracts multiple columns of the same type in parallel
// using multiple goroutines for better performance.
//
// This is a generic method to extract any column types in parallel.
// The extractFn parameter is a function that extracts a single column.
func extractColumnsParallel[T any](dr *DirectResult, colIndices []int,
	extractFn func(int) ([]T, []bool, error)) ([][]T, [][]bool, error) {

	numColumns := len(colIndices)
	if numColumns == 0 {
		return nil, nil, fmt.Errorf("no columns specified for parallel extraction")
	}

	// Allocate slices to hold results and error
	results := make([][]T, numColumns)
	nullMasks := make([][]bool, numColumns)
	errChan := make(chan error, numColumns)

	// Use a WaitGroup to wait for all goroutines to complete
	var wg sync.WaitGroup
	wg.Add(numColumns)

	// Launch a goroutine for each column extraction
	for i, colIdx := range colIndices {
		// Capture loop variables
		localI, localColIdx := i, colIdx

		go func() {
			defer wg.Done()

			// Extract the column using the provided extraction function
			values, nulls, err := extractFn(localColIdx)
			if err != nil {
				errChan <- fmt.Errorf("failed to extract column %d: %w", localColIdx, err)
				return
			}

			// Store the results
			results[localI] = values
			nullMasks[localI] = nulls
		}()
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Check if any errors occurred - collect all errors
	var errors []error
drain:
	for {
		select {
		case err := <-errChan:
			errors = append(errors, err)
		default:
			break drain
		}
	}

	// If we have any errors, return them all
	if len(errors) > 0 {
		// Create a combined error message
		errMsg := fmt.Sprintf("%d errors occurred during parallel extraction. First error: %v", 
			len(errors), errors[0])
		
		// For debugging, include all errors
		if len(errors) > 1 {
			errMsg += fmt.Sprintf(". Additional errors: %v", errors[1:])
		}
		
		return nil, nil, fmt.Errorf(errMsg)
	}

	return results, nullMasks, nil
}

// extractInt32ColumnThreadSafe is a wrapper for ExtractInt32Column that is mutex protected
func (dr *DirectResult) extractInt32ColumnThreadSafe(colIdx int) ([]int32, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	rowCount := int(dr.rowCount)
	values := make([]int32, rowCount)
	nulls := make([]bool, rowCount)

	// Check column type and bounds
	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_INTEGER {
		return nil, nil, ErrIncompatibleType
	}

	// Extract values directly to avoid CGO memory issues
	for i := 0; i < rowCount; i++ {
		isNull := C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(i))
		nulls[i] = bool(isNull)

		if !nulls[i] {
			values[i] = int32(C.duckdb_value_int32(dr.result, C.idx_t(colIdx), C.idx_t(i)))
		}
	}

	return values, nulls, nil
}

// extractInt64ColumnThreadSafe is a wrapper for ExtractInt64Column that is mutex protected
func (dr *DirectResult) extractInt64ColumnThreadSafe(colIdx int) ([]int64, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	rowCount := int(dr.rowCount)
	values := make([]int64, rowCount)
	nulls := make([]bool, rowCount)

	// Check column type and bounds
	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_BIGINT {
		return nil, nil, ErrIncompatibleType
	}

	// Extract values directly to avoid CGO memory issues
	for i := 0; i < rowCount; i++ {
		isNull := C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(i))
		nulls[i] = bool(isNull)

		if !nulls[i] {
			values[i] = int64(C.duckdb_value_int64(dr.result, C.idx_t(colIdx), C.idx_t(i)))
		}
	}

	return values, nulls, nil
}

// extractFloat64ColumnThreadSafe is a wrapper for ExtractFloat64Column that is mutex protected
func (dr *DirectResult) extractFloat64ColumnThreadSafe(colIdx int) ([]float64, []bool, error) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()

	if dr.closed {
		return nil, nil, ErrResultClosed
	}

	rowCount := int(dr.rowCount)
	values := make([]float64, rowCount)
	nulls := make([]bool, rowCount)

	// Check column type and bounds
	if colIdx < 0 || colIdx >= dr.columnCount {
		return nil, nil, ErrInvalidColumnIndex
	}

	if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_DOUBLE {
		return nil, nil, ErrIncompatibleType
	}

	// Extract values directly to avoid CGO memory issues
	for i := 0; i < rowCount; i++ {
		isNull := C.duckdb_value_is_null(dr.result, C.idx_t(colIdx), C.idx_t(i))
		nulls[i] = bool(isNull)

		if !nulls[i] {
			values[i] = float64(C.duckdb_value_double(dr.result, C.idx_t(colIdx), C.idx_t(i)))
		}
	}

	return values, nulls, nil
}

// ExtractInt32ColumnsParallel extracts multiple int32 columns in parallel
// using multiple goroutines for better performance.
func (dr *DirectResult) ExtractInt32ColumnsParallel(colIndices []int) ([][]int32, [][]bool, error) {
	// Initial validation - don't hold the lock for the whole operation
	dr.mu.RLock()
	if dr.closed {
		dr.mu.RUnlock()
		return nil, nil, ErrResultClosed
	}

	// Validate column indices and ensure all are INTEGER type
	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= dr.columnCount {
			dr.mu.RUnlock()
			return nil, nil, ErrInvalidColumnIndex
		}
		if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_INTEGER {
			dr.mu.RUnlock()
			return nil, nil, fmt.Errorf("column %d is not of INTEGER type", colIdx)
		}
	}
	dr.mu.RUnlock()

	// Create an extraction function for int32 columns
	extractFn := func(colIdx int) ([]int32, []bool, error) {
		return dr.extractInt32ColumnThreadSafe(colIdx)
	}

	// Use the generic parallel extraction helper
	return extractColumnsParallel(dr, colIndices, extractFn)
}

// ExtractInt64ColumnsParallel extracts multiple int64 columns in parallel
// using multiple goroutines for better performance.
func (dr *DirectResult) ExtractInt64ColumnsParallel(colIndices []int) ([][]int64, [][]bool, error) {
	// Initial validation - don't hold the lock for the whole operation
	dr.mu.RLock()
	if dr.closed {
		dr.mu.RUnlock()
		return nil, nil, ErrResultClosed
	}

	// Validate column indices and ensure all are BIGINT type
	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= dr.columnCount {
			dr.mu.RUnlock()
			return nil, nil, ErrInvalidColumnIndex
		}
		if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_BIGINT {
			dr.mu.RUnlock()
			return nil, nil, fmt.Errorf("column %d is not of BIGINT type", colIdx)
		}
	}
	dr.mu.RUnlock()

	// Create an extraction function for int64 columns
	extractFn := func(colIdx int) ([]int64, []bool, error) {
		return dr.extractInt64ColumnThreadSafe(colIdx)
	}

	// Use the generic parallel extraction helper
	return extractColumnsParallel(dr, colIndices, extractFn)
}

// ExtractFloat64ColumnsParallel extracts multiple float64 columns in parallel
// using multiple goroutines for better performance.
func (dr *DirectResult) ExtractFloat64ColumnsParallel(colIndices []int) ([][]float64, [][]bool, error) {
	// Initial validation - don't hold the lock for the whole operation
	dr.mu.RLock()
	if dr.closed {
		dr.mu.RUnlock()
		return nil, nil, ErrResultClosed
	}

	// Validate column indices and ensure all are DOUBLE type
	for _, colIdx := range colIndices {
		if colIdx < 0 || colIdx >= dr.columnCount {
			dr.mu.RUnlock()
			return nil, nil, ErrInvalidColumnIndex
		}
		if dr.columnTypes[colIdx] != C.DUCKDB_TYPE_DOUBLE {
			dr.mu.RUnlock()
			return nil, nil, fmt.Errorf("column %d is not of DOUBLE type", colIdx)
		}
	}
	dr.mu.RUnlock()

	// Create an extraction function for float64 columns
	extractFn := func(colIdx int) ([]float64, []bool, error) {
		return dr.extractFloat64ColumnThreadSafe(colIdx)
	}

	// Use the generic parallel extraction helper
	return extractColumnsParallel(dr, colIndices, extractFn)
}
