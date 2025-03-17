// This is a shim file to satisfy the CGO linker requirements
// These stub implementations of native functions are not meant to be used directly
// Instead, the actual implementations will be loaded dynamically at runtime via purego

#ifndef DUCKDB_NATIVE_SHIM_C
#define DUCKDB_NATIVE_SHIM_C

#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include "../include/duckdb.h"
#include "../include/duckdb_native.h"

// Stub implementations for all functions declared in duckdb_native.h

// Optimized batch extraction for integer columns
void extract_int32_column(duckdb_result *result, idx_t col_idx, 
                         int32_t *out_buffer, bool *null_mask, 
                         idx_t start_row, idx_t row_count) {
    // Stub implementation - will be overridden by dynamic loading
}

// Optimized batch extraction for int64 columns
void extract_int64_column(duckdb_result *result, idx_t col_idx, 
                         int64_t *out_buffer, bool *null_mask, 
                         idx_t start_row, idx_t row_count) {
    // Stub implementation
}

// Optimized batch extraction for float64 columns
void extract_float64_column(duckdb_result *result, idx_t col_idx, 
                          double *out_buffer, bool *null_mask, 
                          idx_t start_row, idx_t row_count) {
    // Stub implementation
}

// Zero-copy string handling - return direct pointers to string data
void extract_string_column_ptrs(duckdb_result *result, idx_t col_idx,
                              char **out_ptrs, int32_t *out_lens, bool *null_mask,
                              idx_t start_row, idx_t row_count) {
    // Stub implementation
}

// Extract boolean column values
void extract_bool_column(duckdb_result *result, idx_t col_idx, 
                        bool *out_buffer, bool *null_mask, 
                        idx_t start_row, idx_t row_count) {
    // Stub implementation
}

// Extract timestamp column values (converted to int64_t microseconds since epoch)
void extract_timestamp_column(duckdb_result *result, idx_t col_idx,
                             int64_t *out_buffer, bool *null_mask,
                             idx_t start_row, idx_t row_count) {
    // Stub implementation
}

// Extract date column values (converted to int32_t days since epoch)
void extract_date_column(duckdb_result *result, idx_t col_idx,
                        int32_t *out_buffer, bool *null_mask,
                        idx_t start_row, idx_t row_count) {
    // Stub implementation
}

// Extract BLOB column data with direct memory access
void extract_blob_column(duckdb_result *result, idx_t col_idx,
                       void **out_ptrs, int32_t *out_sizes, bool *null_mask,
                       idx_t start_row, idx_t row_count) {
    // Stub implementation
}

// Batch extract multiple string columns at once (reduces CGO overhead)
void extract_string_columns_batch(duckdb_result *result,
                               idx_t *col_indices, int32_t num_columns,
                               char ***out_ptrs_array, int32_t **out_lens_array, bool **null_masks_array,
                               idx_t start_row, idx_t row_count) {
    // Stub implementation
}

// Batch extract multiple blob columns at once (reduces CGO overhead)
void extract_blob_columns_batch(duckdb_result *result,
                            idx_t *col_indices, int32_t num_columns,
                            void ***out_ptrs_array, int32_t **out_sizes_array, bool **null_masks_array,
                            idx_t start_row, idx_t row_count) {
    // Stub implementation
}

// Optimized filter for int32 columns (greater than)
int32_t filter_int32_column_gt(duckdb_result *result, idx_t col_idx,
                            int32_t threshold, idx_t *out_indices,
                            idx_t start_row, idx_t row_count) {
    // Stub implementation
    return 0;
}


// Optimized direct-to-struct batch extraction
void extract_row_batch(duckdb_result *result, void *out_buffer,
                     idx_t *col_offsets, idx_t *col_types, idx_t col_count,
                     idx_t start_row, idx_t row_count, idx_t row_size) {
    // Stub implementation
}

// Reserved for future optimizations

#endif // DUCKDB_NATIVE_SHIM_C