#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <duckdb.h>
#include "../include/duckdb_native.h"

// SIMD Compiler Detection
// Check for AVX2 support (x86_64)
#if defined(__AVX2__)
#include <immintrin.h>
#define HAVE_AVX2 1
#else
#define HAVE_AVX2 0
#endif

// Check for ARM NEON support (ARM64)
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#define HAVE_NEON 1
#else
#define HAVE_NEON 0
#endif

// Optimized batch extraction for integer columns
// Returns results directly in Go-accessible memory
void extract_int32_column(duckdb_result *result, idx_t col_idx, 
                         int32_t *out_buffer, bool *null_mask, 
                         idx_t start_row, idx_t row_count) {
    
    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_buffer || !null_mask) {
        return;
    }
    
#if HAVE_AVX2
    // Process 8 values at a time with AVX2 (x86_64)
    const idx_t batch_size = 8;
    const idx_t batch_count = row_count / batch_size;
    
    for (idx_t batch = 0; batch < batch_count; batch++) {
        const idx_t base_idx = start_row + batch * batch_size;
        
        // Check for nulls in this batch
        bool has_nulls = false;
        for (idx_t i = 0; i < batch_size; i++) {
            if (duckdb_value_is_null(result, col_idx, base_idx + i)) {
                has_nulls = true;
                break;
            }
        }
        
        // Fast path for non-null batches
        if (!has_nulls) {
            // Load values manually (can't use SIMD for DuckDB API calls)
            int32_t temp_values[8];
            for (idx_t i = 0; i < batch_size; i++) {
                temp_values[i] = duckdb_value_int32(result, col_idx, base_idx + i);
            }
            
            // Use SIMD to store results
            __m256i values = _mm256_loadu_si256((__m256i*)temp_values);
            _mm256_storeu_si256((__m256i*)&out_buffer[batch * batch_size], values);
            
            // Mark all as non-null
            memset(&null_mask[batch * batch_size], 0, batch_size);
        } else {
            // Handle batch with nulls
            for (idx_t i = 0; i < batch_size; i++) {
                idx_t row_idx = base_idx + i;
                bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
                null_mask[batch * batch_size + i] = is_null;
                
                if (!is_null) {
                    out_buffer[batch * batch_size + i] = duckdb_value_int32(result, col_idx, row_idx);
                } else {
                    out_buffer[batch * batch_size + i] = 0;
                }
            }
        }
    }
    
    // Handle remaining rows
    const idx_t remaining_start = batch_count * batch_size;

#elif HAVE_NEON
    // Process 4 values at a time with NEON (ARM64)
    const idx_t batch_size = 4; // NEON can process 4 int32 values at once
    const idx_t batch_count = row_count / batch_size;
    
    for (idx_t batch = 0; batch < batch_count; batch++) {
        const idx_t base_idx = start_row + batch * batch_size;
        
        // Check for nulls in this batch
        bool has_nulls = false;
        for (idx_t i = 0; i < batch_size; i++) {
            if (duckdb_value_is_null(result, col_idx, base_idx + i)) {
                has_nulls = true;
                break;
            }
        }
        
        // Fast path for non-null batches
        if (!has_nulls) {
            // Load values manually (can't use SIMD for DuckDB API calls)
            int32_t temp_values[4];
            for (idx_t i = 0; i < batch_size; i++) {
                temp_values[i] = duckdb_value_int32(result, col_idx, base_idx + i);
            }
            
            // Use NEON to store results - load and store directly
            int32x4_t values = vld1q_s32(temp_values);
            vst1q_s32(&out_buffer[batch * batch_size], values);
            
            // Mark all as non-null
            memset(&null_mask[batch * batch_size], 0, batch_size);
        } else {
            // Handle batch with nulls
            for (idx_t i = 0; i < batch_size; i++) {
                idx_t row_idx = base_idx + i;
                bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
                null_mask[batch * batch_size + i] = is_null;
                
                if (!is_null) {
                    out_buffer[batch * batch_size + i] = duckdb_value_int32(result, col_idx, row_idx);
                } else {
                    out_buffer[batch * batch_size + i] = 0;
                }
            }
        }
    }
    
    // Handle remaining rows
    const idx_t remaining_start = batch_count * batch_size;
#else
    // Standard implementation for systems without SIMD support
    // Process everything row by row
    const idx_t remaining_start = 0;
#endif

    // Process any remaining rows (common code for all implementations)
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
}

// Optimized batch extraction for int64 columns
void extract_int64_column(duckdb_result *result, idx_t col_idx, 
                         int64_t *out_buffer, bool *null_mask, 
                         idx_t start_row, idx_t row_count) {
    
    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_buffer || !null_mask) {
        return;
    }
    
#if HAVE_AVX2
    // Process 4 values at a time with AVX2 (int64 = 8 bytes, so 4 values per 256-bit register)
    const idx_t batch_size = 4;
    const idx_t batch_count = row_count / batch_size;
    
    for (idx_t batch = 0; batch < batch_count; batch++) {
        const idx_t base_idx = start_row + batch * batch_size;
        
        // Check for nulls in this batch
        bool has_nulls = false;
        for (idx_t i = 0; i < batch_size; i++) {
            if (duckdb_value_is_null(result, col_idx, base_idx + i)) {
                has_nulls = true;
                break;
            }
        }
        
        // Fast path for non-null batches
        if (!has_nulls) {
            // Load values manually
            int64_t temp_values[4];
            for (idx_t i = 0; i < batch_size; i++) {
                temp_values[i] = duckdb_value_int64(result, col_idx, base_idx + i);
            }
            
            // Use SIMD to store results
            __m256i values = _mm256_loadu_si256((__m256i*)temp_values);
            _mm256_storeu_si256((__m256i*)&out_buffer[batch * batch_size], values);
            
            // Mark all as non-null
            memset(&null_mask[batch * batch_size], 0, batch_size);
        } else {
            // Handle batch with nulls
            for (idx_t i = 0; i < batch_size; i++) {
                idx_t row_idx = base_idx + i;
                bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
                null_mask[batch * batch_size + i] = is_null;
                
                if (!is_null) {
                    out_buffer[batch * batch_size + i] = duckdb_value_int64(result, col_idx, row_idx);
                } else {
                    out_buffer[batch * batch_size + i] = 0;
                }
            }
        }
    }
    
    // Handle remaining rows
    const idx_t remaining_start = batch_count * batch_size;

#elif HAVE_NEON
    // Process 2 values at a time with NEON (int64 = 8 bytes, so 2 values per 128-bit register)
    const idx_t batch_size = 2;
    const idx_t batch_count = row_count / batch_size;
    
    for (idx_t batch = 0; batch < batch_count; batch++) {
        const idx_t base_idx = start_row + batch * batch_size;
        
        // Check for nulls in this batch
        bool has_nulls = false;
        for (idx_t i = 0; i < batch_size; i++) {
            if (duckdb_value_is_null(result, col_idx, base_idx + i)) {
                has_nulls = true;
                break;
            }
        }
        
        // Fast path for non-null batches
        if (!has_nulls) {
            // Load values manually
            int64_t temp_values[2];
            for (idx_t i = 0; i < batch_size; i++) {
                temp_values[i] = duckdb_value_int64(result, col_idx, base_idx + i);
            }
            
            // Use NEON to store results
            int64x2_t values = vld1q_s64(temp_values);
            vst1q_s64(&out_buffer[batch * batch_size], values);
            
            // Mark all as non-null
            memset(&null_mask[batch * batch_size], 0, batch_size);
        } else {
            // Handle batch with nulls
            for (idx_t i = 0; i < batch_size; i++) {
                idx_t row_idx = base_idx + i;
                bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
                null_mask[batch * batch_size + i] = is_null;
                
                if (!is_null) {
                    out_buffer[batch * batch_size + i] = duckdb_value_int64(result, col_idx, row_idx);
                } else {
                    out_buffer[batch * batch_size + i] = 0;
                }
            }
        }
    }
    
    // Handle remaining rows
    const idx_t remaining_start = batch_count * batch_size;

#else
    // Standard implementation for systems without SIMD support
    const idx_t remaining_start = 0;
#endif

    // Process any remaining rows (common code for all implementations)
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
}

// Optimized batch extraction for float64 columns
void extract_float64_column(duckdb_result *result, idx_t col_idx, 
                          double *out_buffer, bool *null_mask, 
                          idx_t start_row, idx_t row_count) {
    
    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_buffer || !null_mask) {
        return;
    }
    
#if HAVE_AVX2
    // Process 4 values at a time with AVX2 (double = 8 bytes, so 4 values per 256-bit register)
    const idx_t batch_size = 4;
    const idx_t batch_count = row_count / batch_size;
    
    for (idx_t batch = 0; batch < batch_count; batch++) {
        const idx_t base_idx = start_row + batch * batch_size;
        
        // Check for nulls in this batch
        bool has_nulls = false;
        for (idx_t i = 0; i < batch_size; i++) {
            if (duckdb_value_is_null(result, col_idx, base_idx + i)) {
                has_nulls = true;
                break;
            }
        }
        
        // Fast path for non-null batches
        if (!has_nulls) {
            // Load values manually
            double temp_values[4];
            for (idx_t i = 0; i < batch_size; i++) {
                temp_values[i] = duckdb_value_double(result, col_idx, base_idx + i);
            }
            
            // Use SIMD to store results
            __m256d values = _mm256_loadu_pd(temp_values);
            _mm256_storeu_pd(&out_buffer[batch * batch_size], values);
            
            // Mark all as non-null
            memset(&null_mask[batch * batch_size], 0, batch_size);
        } else {
            // Handle batch with nulls
            for (idx_t i = 0; i < batch_size; i++) {
                idx_t row_idx = base_idx + i;
                bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
                null_mask[batch * batch_size + i] = is_null;
                
                if (!is_null) {
                    out_buffer[batch * batch_size + i] = duckdb_value_double(result, col_idx, row_idx);
                } else {
                    out_buffer[batch * batch_size + i] = 0.0;
                }
            }
        }
    }
    
    // Handle remaining rows
    const idx_t remaining_start = batch_count * batch_size;

#elif HAVE_NEON
    // Process 2 values at a time with NEON (double = 8 bytes, so 2 values per 128-bit register)
    const idx_t batch_size = 2;
    const idx_t batch_count = row_count / batch_size;
    
    for (idx_t batch = 0; batch < batch_count; batch++) {
        const idx_t base_idx = start_row + batch * batch_size;
        
        // Check for nulls in this batch
        bool has_nulls = false;
        for (idx_t i = 0; i < batch_size; i++) {
            if (duckdb_value_is_null(result, col_idx, base_idx + i)) {
                has_nulls = true;
                break;
            }
        }
        
        // Fast path for non-null batches
        if (!has_nulls) {
            // Load values manually
            double temp_values[2];
            for (idx_t i = 0; i < batch_size; i++) {
                temp_values[i] = duckdb_value_double(result, col_idx, base_idx + i);
            }
            
            // Use NEON to store results
            float64x2_t values = vld1q_f64(temp_values);
            vst1q_f64(&out_buffer[batch * batch_size], values);
            
            // Mark all as non-null
            memset(&null_mask[batch * batch_size], 0, batch_size);
        } else {
            // Handle batch with nulls
            for (idx_t i = 0; i < batch_size; i++) {
                idx_t row_idx = base_idx + i;
                bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
                null_mask[batch * batch_size + i] = is_null;
                
                if (!is_null) {
                    out_buffer[batch * batch_size + i] = duckdb_value_double(result, col_idx, row_idx);
                } else {
                    out_buffer[batch * batch_size + i] = 0.0;
                }
            }
        }
    }
    
    // Handle remaining rows
    const idx_t remaining_start = batch_count * batch_size;

#else
    // Standard implementation for systems without SIMD support
    const idx_t remaining_start = 0;
#endif

    // Process any remaining rows (common code for all implementations)
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
}

// Zero-copy string handling with improved memory management
// This implementation optimizes for memory efficiency by using DuckDB's string pointers directly
void extract_string_column_ptrs(duckdb_result *result, idx_t col_idx,
                              char **out_ptrs, int32_t *out_lens, bool *null_mask,
                              idx_t start_row, idx_t row_count) {
    
    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_ptrs || !out_lens || !null_mask) {
        return;
    }
    
    // Process strings in larger blocks for better cache locality
    const idx_t block_size = 64; // Process in blocks for better cache performance
    const idx_t num_blocks = row_count / block_size;
    
    // Process in blocks for better cache performance
    for (idx_t block = 0; block < num_blocks; block++) {
        const idx_t base_idx = start_row + block * block_size;
        
        for (idx_t i = 0; i < block_size; i++) {
            idx_t row_idx = base_idx + i;
            idx_t buffer_idx = block * block_size + i;
            
            bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
            null_mask[buffer_idx] = is_null;
            
            if (!is_null) {
                // Get string value directly from DuckDB's memory
                // This still creates a copy, but we'll manage it optimally on the Go side
                char *str_val = duckdb_value_varchar(result, col_idx, row_idx);
                if (str_val) {
                    out_ptrs[buffer_idx] = str_val;
                    // Pre-calculate the length to avoid repeated strlen calls
                    out_lens[buffer_idx] = (int32_t)strlen(str_val);
                } else {
                    out_ptrs[buffer_idx] = NULL;
                    out_lens[buffer_idx] = 0;
                }
            } else {
                out_ptrs[buffer_idx] = NULL;
                out_lens[buffer_idx] = 0;
            }
        }
    }
    
    // Handle remaining rows
    const idx_t remaining_start = num_blocks * block_size;
    for (idx_t i = 0; i < row_count - remaining_start; i++) {
        idx_t row_idx = start_row + remaining_start + i;
        idx_t buffer_idx = remaining_start + i;
        
        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
        null_mask[buffer_idx] = is_null;
        
        if (!is_null) {
            char *str_val = duckdb_value_varchar(result, col_idx, row_idx);
            if (str_val) {
                out_ptrs[buffer_idx] = str_val;
                out_lens[buffer_idx] = (int32_t)strlen(str_val);
            } else {
                out_ptrs[buffer_idx] = NULL;
                out_lens[buffer_idx] = 0;
            }
        } else {
            out_ptrs[buffer_idx] = NULL;
            out_lens[buffer_idx] = 0;
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

// Extract timestamp column values (microseconds since epoch) using direct method
void extract_timestamp_column(duckdb_result *result, idx_t col_idx,
                             int64_t *out_buffer, bool *null_mask,
                             idx_t start_row, idx_t row_count) {
    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_buffer || !null_mask) {
        return;
    }
    
    // Use SIMD-friendly processing with larger blocks
    const idx_t block_size = 64;
    const idx_t num_blocks = row_count / block_size;
    
    // Process blocks of timestamps
    for (idx_t block = 0; block < num_blocks; block++) {
        for (idx_t i = 0; i < block_size; i++) {
            idx_t row_idx = start_row + block * block_size + i;
            idx_t buffer_idx = block * block_size + i;
            
            bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
            null_mask[buffer_idx] = is_null;
            
            if (!is_null) {
                // Direct access to timestamp value (microseconds since epoch)
                // duckdb_timestamp is a struct with a single field 'micros'
                duckdb_timestamp ts = duckdb_value_timestamp(result, col_idx, row_idx);
                out_buffer[buffer_idx] = ts.micros;
            } else {
                out_buffer[buffer_idx] = 0;
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
            duckdb_timestamp ts = duckdb_value_timestamp(result, col_idx, row_idx);
            out_buffer[buffer_idx] = ts.micros;
        } else {
            out_buffer[buffer_idx] = 0;
        }
    }
}

// Extract date column values (days since epoch) using direct method
void extract_date_column(duckdb_result *result, idx_t col_idx,
                        int32_t *out_buffer, bool *null_mask,
                        idx_t start_row, idx_t row_count) {
    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_buffer || !null_mask) {
        return;
    }
    
    // Use SIMD-friendly processing with larger blocks
    const idx_t block_size = 64;
    const idx_t num_blocks = row_count / block_size;
    
    // Process blocks of dates
    for (idx_t block = 0; block < num_blocks; block++) {
        for (idx_t i = 0; i < block_size; i++) {
            idx_t row_idx = start_row + block * block_size + i;
            idx_t buffer_idx = block * block_size + i;
            
            bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
            null_mask[buffer_idx] = is_null;
            
            if (!is_null) {
                // Direct access to date value (days since epoch)
                // duckdb_date is a struct with a single field 'days'
                duckdb_date date = duckdb_value_date(result, col_idx, row_idx);
                out_buffer[buffer_idx] = date.days;
            } else {
                out_buffer[buffer_idx] = 0;
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
            duckdb_date date = duckdb_value_date(result, col_idx, row_idx);
            out_buffer[buffer_idx] = date.days;
        } else {
            out_buffer[buffer_idx] = 0;
        }
    }
}

// Extract BLOB column data with optimized memory access and block processing
void extract_blob_column(duckdb_result *result, idx_t col_idx,
                       void **out_ptrs, int32_t *out_sizes, bool *null_mask,
                       idx_t start_row, idx_t row_count) {
    // Early bounds check
    if (col_idx >= duckdb_column_count(result) || !out_ptrs || !out_sizes || !null_mask) {
        return;
    }
    
    // Process blobs in blocks for better cache performance
    const idx_t block_size = 32; // Smaller block size for potentially large blobs
    const idx_t num_blocks = row_count / block_size;
    
    // Process in blocks for improved cache locality
    for (idx_t block = 0; block < num_blocks; block++) {
        const idx_t base_idx = start_row + block * block_size;
        
        for (idx_t i = 0; i < block_size; i++) {
            idx_t row_idx = base_idx + i;
            idx_t buffer_idx = block * block_size + i;
            
            bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
            null_mask[buffer_idx] = is_null;
            
            if (!is_null) {
                // Get direct pointer to blob data
                duckdb_blob blob = duckdb_value_blob(result, col_idx, row_idx);
                
                // Since duckdb_value_blob returns a copy, we need to use this copy
                // and free it later in the Go code
                out_ptrs[buffer_idx] = blob.data;
                out_sizes[buffer_idx] = (int32_t)blob.size;
            } else {
                out_ptrs[buffer_idx] = NULL;
                out_sizes[buffer_idx] = 0;
            }
        }
    }
    
    // Handle remaining rows
    const idx_t remaining_start = num_blocks * block_size;
    for (idx_t i = 0; i < row_count - remaining_start; i++) {
        idx_t row_idx = start_row + remaining_start + i;
        idx_t buffer_idx = remaining_start + i;
        
        bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
        null_mask[buffer_idx] = is_null;
        
        if (!is_null) {
            duckdb_blob blob = duckdb_value_blob(result, col_idx, row_idx);
            out_ptrs[buffer_idx] = blob.data;
            out_sizes[buffer_idx] = (int32_t)blob.size;
        } else {
            out_ptrs[buffer_idx] = NULL;
            out_sizes[buffer_idx] = 0;
        }
    }
}

// Batch extract multiple string columns for improved performance
// This reduces CGO overhead by extracting multiple columns in a single call
void extract_string_columns_batch(duckdb_result *result,
                               idx_t *col_indices, int32_t num_columns,
                               char ***out_ptrs_array, int32_t **out_lens_array, bool **null_masks_array,
                               idx_t start_row, idx_t row_count) {
    // Early bounds check and validation
    if (!col_indices || num_columns <= 0 || num_columns > 16 ||
        !out_ptrs_array || !out_lens_array || !null_masks_array) {
        return;
    }
    
    // Verify all columns are valid and of string type
    for (int i = 0; i < num_columns; i++) {
        if (col_indices[i] >= duckdb_column_count(result) ||
            !out_ptrs_array[i] || !out_lens_array[i] || !null_masks_array[i] ||
            duckdb_column_type(result, col_indices[i]) != DUCKDB_TYPE_VARCHAR) {
            return;
        }
    }
    
    // Process in blocks for better cache efficiency
    const idx_t block_size = 64; // Process 64 rows at a time
    const idx_t num_blocks = row_count / block_size;
    
    // Extract all columns
    for (int32_t col = 0; col < num_columns; col++) {
        idx_t col_idx = col_indices[col];
        char **out_ptrs = out_ptrs_array[col];
        int32_t *out_lens = out_lens_array[col];
        bool *null_mask = null_masks_array[col];
        
        // Process blocks of rows for this column
        for (idx_t block = 0; block < num_blocks; block++) {
            const idx_t base_idx = start_row + block * block_size;
            
            for (idx_t i = 0; i < block_size; i++) {
                idx_t row_idx = base_idx + i;
                idx_t buffer_idx = block * block_size + i;
                
                bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
                null_mask[buffer_idx] = is_null;
                
                if (!is_null) {
                    // Get string value directly from DuckDB's memory
                    char *str_val = duckdb_value_varchar(result, col_idx, row_idx);
                    if (str_val) {
                        out_ptrs[buffer_idx] = str_val;
                        // Pre-calculate the length to avoid repeated strlen calls
                        out_lens[buffer_idx] = (int32_t)strlen(str_val);
                    } else {
                        out_ptrs[buffer_idx] = NULL;
                        out_lens[buffer_idx] = 0;
                    }
                } else {
                    out_ptrs[buffer_idx] = NULL;
                    out_lens[buffer_idx] = 0;
                }
            }
        }
        
        // Handle remaining rows
        const idx_t remaining_start = num_blocks * block_size;
        for (idx_t i = 0; i < row_count - remaining_start; i++) {
            idx_t row_idx = start_row + remaining_start + i;
            idx_t buffer_idx = remaining_start + i;
            
            bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
            null_mask[buffer_idx] = is_null;
            
            if (!is_null) {
                char *str_val = duckdb_value_varchar(result, col_idx, row_idx);
                if (str_val) {
                    out_ptrs[buffer_idx] = str_val;
                    out_lens[buffer_idx] = (int32_t)strlen(str_val);
                } else {
                    out_ptrs[buffer_idx] = NULL;
                    out_lens[buffer_idx] = 0;
                }
            } else {
                out_ptrs[buffer_idx] = NULL;
                out_lens[buffer_idx] = 0;
            }
        }
    }
}

// Batch extract multiple blob columns for improved performance
// This reduces CGO overhead by extracting multiple columns in a single call
void extract_blob_columns_batch(duckdb_result *result,
                            idx_t *col_indices, int32_t num_columns,
                            void ***out_ptrs_array, int32_t **out_sizes_array, bool **null_masks_array,
                            idx_t start_row, idx_t row_count) {
    // Early bounds check and validation
    if (!col_indices || num_columns <= 0 || num_columns > 16 ||
        !out_ptrs_array || !out_sizes_array || !null_masks_array) {
        return;
    }
    
    // Verify all columns are valid and of blob type
    for (int i = 0; i < num_columns; i++) {
        if (col_indices[i] >= duckdb_column_count(result) ||
            !out_ptrs_array[i] || !out_sizes_array[i] || !null_masks_array[i] ||
            duckdb_column_type(result, col_indices[i]) != DUCKDB_TYPE_BLOB) {
            return;
        }
    }
    
    // Process in blocks for better cache efficiency
    const idx_t block_size = 32; // Process 32 rows at a time (smaller for blobs)
    const idx_t num_blocks = row_count / block_size;
    
    // Extract all columns
    for (int32_t col = 0; col < num_columns; col++) {
        idx_t col_idx = col_indices[col];
        void **out_ptrs = out_ptrs_array[col];
        int32_t *out_sizes = out_sizes_array[col];
        bool *null_mask = null_masks_array[col];
        
        // Process blocks of rows for this column
        for (idx_t block = 0; block < num_blocks; block++) {
            const idx_t base_idx = start_row + block * block_size;
            
            for (idx_t i = 0; i < block_size; i++) {
                idx_t row_idx = base_idx + i;
                idx_t buffer_idx = block * block_size + i;
                
                bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
                null_mask[buffer_idx] = is_null;
                
                if (!is_null) {
                    // Get blob data
                    duckdb_blob blob = duckdb_value_blob(result, col_idx, row_idx);
                    
                    // Store pointers and sizes
                    out_ptrs[buffer_idx] = blob.data;
                    out_sizes[buffer_idx] = (int32_t)blob.size;
                } else {
                    out_ptrs[buffer_idx] = NULL;
                    out_sizes[buffer_idx] = 0;
                }
            }
        }
        
        // Handle remaining rows
        const idx_t remaining_start = num_blocks * block_size;
        for (idx_t i = 0; i < row_count - remaining_start; i++) {
            idx_t row_idx = start_row + remaining_start + i;
            idx_t buffer_idx = remaining_start + i;
            
            bool is_null = duckdb_value_is_null(result, col_idx, row_idx);
            null_mask[buffer_idx] = is_null;
            
            if (!is_null) {
                duckdb_blob blob = duckdb_value_blob(result, col_idx, row_idx);
                out_ptrs[buffer_idx] = blob.data;
                out_sizes[buffer_idx] = (int32_t)blob.size;
            } else {
                out_ptrs[buffer_idx] = NULL;
                out_sizes[buffer_idx] = 0;
            }
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




// Optimized direct-to-struct batch extraction
// Extracts multiple columns at once into a pre-defined struct layout
void extract_row_batch(duckdb_result *result, void *out_buffer,
                     idx_t *col_offsets, idx_t *col_types, idx_t col_count,
                     idx_t start_row, idx_t row_count, idx_t row_size) {
    
    // Early bounds check
    if (!out_buffer || !col_offsets || !col_types || col_count == 0 || 
        col_count > duckdb_column_count(result)) {
        return;
    }
    
    // Extract each row
    char *buffer = (char *)out_buffer;
    
    for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
        idx_t current_row = start_row + row_idx;
        char *row_ptr = buffer + (row_idx * row_size);
        
        // Extract each column value for this row
        for (idx_t col = 0; col < col_count; col++) {
            idx_t col_type = col_types[col];
            idx_t offset = col_offsets[col];
            
            // Check for NULL
            bool is_null = duckdb_value_is_null(result, col, current_row);
            
            // Skip if NULL - leave as zero value (from memset)
            if (is_null) {
                continue;
            }
            
            // Based on column type, extract value to correct position
            switch (col_type) {
                case DUCKDB_TYPE_BOOLEAN: {
                    bool val = duckdb_value_boolean(result, col, current_row);
                    *(bool *)(row_ptr + offset) = val;
                    break;
                }
                case DUCKDB_TYPE_TINYINT: {
                    int8_t val = duckdb_value_int8(result, col, current_row);
                    *(int8_t *)(row_ptr + offset) = val;
                    break;
                }
                case DUCKDB_TYPE_SMALLINT: {
                    int16_t val = duckdb_value_int16(result, col, current_row);
                    *(int16_t *)(row_ptr + offset) = val;
                    break;
                }
                case DUCKDB_TYPE_INTEGER: {
                    int32_t val = duckdb_value_int32(result, col, current_row);
                    *(int32_t *)(row_ptr + offset) = val;
                    break;
                }
                case DUCKDB_TYPE_BIGINT: {
                    int64_t val = duckdb_value_int64(result, col, current_row);
                    *(int64_t *)(row_ptr + offset) = val;
                    break;
                }
                case DUCKDB_TYPE_FLOAT: {
                    float val = duckdb_value_float(result, col, current_row);
                    *(float *)(row_ptr + offset) = val;
                    break;
                }
                case DUCKDB_TYPE_DOUBLE: {
                    double val = duckdb_value_double(result, col, current_row);
                    *(double *)(row_ptr + offset) = val;
                    break;
                }
                // String values need to be handled carefully - will be added in future version
                default:
                    // Unsupported types - skip for now
                    break;
            }
        }
    }
}