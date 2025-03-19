/**
 * duckdb_go_adapter.h - Header for high performance C adapter for DuckDB Go driver
 */

#ifndef DUCKDB_GO_ADAPTER_H
#define DUCKDB_GO_ADAPTER_H

#include <stdint.h>
#include "include/duckdb.h"

// String cache structure for efficient string handling
typedef struct {
    char** entries;         // Array of string pointers
    int32_t* hashes;        // Hash values for quick lookup
    int32_t capacity;       // Total capacity of the cache
    int32_t size;           // Current number of entries
    int32_t* ref_counts;    // Reference counting for entries
} string_cache_t;

// Column metadata structure
typedef struct {
    char* name;
    int32_t _type;     // Using _type to avoid conflict with C++ keyword
    int32_t physical_type;
    int32_t nullable;
} column_meta_t;

// Vector extraction functions for efficient batch processing
// These functions extract an entire column of values with a single call
// to minimize CGO boundary crossings

// Extract bool values and nulls from a result column
void extract_vector_bool(duckdb_result *result, idx_t col_idx, idx_t offset, 
                        idx_t batch_size, bool *values, bool *nulls);

// Extract int8 values and nulls from a result column
void extract_vector_int8(duckdb_result *result, idx_t col_idx, idx_t offset, 
                        idx_t batch_size, int8_t *values, bool *nulls);

// Extract int16 values and nulls from a result column
void extract_vector_int16(duckdb_result *result, idx_t col_idx, idx_t offset, 
                         idx_t batch_size, int16_t *values, bool *nulls);

// Extract int32 values and nulls from a result column
void extract_vector_int32(duckdb_result *result, idx_t col_idx, idx_t offset, 
                         idx_t batch_size, int32_t *values, bool *nulls);

// Extract int64 values and nulls from a result column
void extract_vector_int64(duckdb_result *result, idx_t col_idx, idx_t offset, 
                         idx_t batch_size, int64_t *values, bool *nulls);

// Extract uint8 values and nulls from a result column
void extract_vector_uint8(duckdb_result *result, idx_t col_idx, idx_t offset, 
                         idx_t batch_size, uint8_t *values, bool *nulls);

// Extract uint16 values and nulls from a result column
void extract_vector_uint16(duckdb_result *result, idx_t col_idx, idx_t offset, 
                          idx_t batch_size, uint16_t *values, bool *nulls);

// Extract uint32 values and nulls from a result column
void extract_vector_uint32(duckdb_result *result, idx_t col_idx, idx_t offset, 
                          idx_t batch_size, uint32_t *values, bool *nulls);

// Extract uint64 values and nulls from a result column
void extract_vector_uint64(duckdb_result *result, idx_t col_idx, idx_t offset, 
                          idx_t batch_size, uint64_t *values, bool *nulls);

// Extract float32 values and nulls from a result column
void extract_vector_float32(duckdb_result *result, idx_t col_idx, idx_t offset, 
                           idx_t batch_size, float *values, bool *nulls);

// Extract float64 values and nulls from a result column
void extract_vector_float64(duckdb_result *result, idx_t col_idx, idx_t offset, 
                           idx_t batch_size, double *values, bool *nulls);

// Extract string values and nulls from a result column
// Returns 1 on success, 0 on failure
int extract_vector_string(duckdb_result *result, idx_t col_idx, idx_t offset, 
                         idx_t batch_size, char **values, idx_t *lengths, bool *nulls);

// Extract blob values and nulls from a result column
// Returns 1 on success, 0 on failure
int extract_vector_blob(duckdb_result *result, idx_t col_idx, idx_t offset, 
                       idx_t batch_size, char **values, idx_t *lengths, bool *nulls);

// Extract timestamp values and nulls from a result column
void extract_vector_timestamp(duckdb_result *result, idx_t col_idx, idx_t offset, 
                             idx_t batch_size, int64_t *values, bool *nulls);

// Extract date values and nulls from a result column
void extract_vector_date(duckdb_result *result, idx_t col_idx, idx_t offset, 
                        idx_t batch_size, int32_t *values, bool *nulls);

// Temporal data structure for date/timestamp handling
typedef struct {
    // Date data (seconds since epoch for each date value)
    int64_t* date_data;
    
    // Timestamp data (seconds and nanoseconds parts for each timestamp value)
    int64_t* timestamp_seconds;
    int32_t* timestamp_nanos;
    
    // Flags to indicate whether these arrays were allocated
    int8_t has_date_data;
    int8_t has_timestamp_data;
} temporal_data_t;

// Buffer metadata structure
typedef struct {
    int64_t row_count;
    int32_t column_count;
    int64_t error_code;
    char* error_message;
    column_meta_t* columns;
    
    // Pointers to column data
    void** data_ptrs;
    int8_t** nulls_ptrs;
    
    // Keep track of string data for cleanup
    char* string_buffer;
    int64_t string_buffer_size;
    
    // For tracking affected rows in DML statements
    int64_t rows_affected;
    
    // Temporal data storage (date/timestamp)
    temporal_data_t* temporal_data;
    
    // For memory cleanup
    void** resources;
    int32_t resource_count;
    
    // Reference counting for shared buffer management
    int ref_count;
} result_buffer_t;

// Parameter batch data structure for efficient batch binding
typedef struct {
    int32_t param_count;     // Number of parameters
    int32_t batch_size;      // Number of parameter sets in the batch
    
    // Flag arrays for NULL values (param_count * batch_size)
    int8_t* null_flags;
    
    // Parameter type information
    int32_t* param_types;    // Array of parameter types
    
    // Data arrays for each type (pointers to param data for each type)
    int8_t* bool_data;       // For BOOLEAN
    int8_t* int8_data;       // For TINYINT
    int16_t* int16_data;     // For SMALLINT
    int32_t* int32_data;     // For INTEGER
    int64_t* int64_data;     // For BIGINT
    uint8_t* uint8_data;     // For UTINYINT
    uint16_t* uint16_data;   // For USMALLINT
    uint32_t* uint32_data;   // For UINTEGER
    uint64_t* uint64_data;   // For UBIGINT
    float* float_data;       // For FLOAT
    double* double_data;     // For DOUBLE
    
    // Variable-length data (string and blob)
    char** string_data;      // Array of string pointers
    void** blob_data;        // Array of blob data pointers
    int64_t* blob_lengths;   // Array of blob lengths
    
    // Timestamp data
    int64_t* timestamp_data; // For TIMESTAMP (microseconds)
    
    // Resources for cleanup
    void** resources;        // Pointers to allocated resources
    int32_t resource_count;  // Count of resources
    int32_t resource_capacity; // Capacity of resources array
} param_batch_t;

// Execute a query and store the entire result set in a single operation
int execute_query_vectorized(duckdb_connection connection, const char* query, result_buffer_t* buffer);

// Simplified version of execute_query_vectorized that's embedded directly in Go code
int execute_query_vectorized_shim(duckdb_connection connection, const char* query, result_buffer_t* buffer);

// Execute a prepared statement and store the result set
// Parameters are bound directly to the statement before calling this function
int execute_prepared_vectorized(duckdb_prepared_statement statement, 
                               result_buffer_t* buffer);

// Batch bind parameters to a prepared statement and execute it
// This reduces the number of CGO crossings for multiple parameter sets
int bind_and_execute_batch(duckdb_prepared_statement statement, 
                          param_batch_t* params,
                          result_buffer_t* buffer);

// Initialize a parameter batch structure
param_batch_t* create_param_batch(int32_t param_count, int32_t batch_size);

// Free all resources associated with a parameter batch
void free_param_batch(param_batch_t* params);

// Clean up all resources associated with a result buffer
void free_result_buffer(result_buffer_t* buffer);

// Reference counting functions for safer resource management
void increase_buffer_ref(result_buffer_t* buffer);
void decrease_buffer_ref(result_buffer_t* buffer);

// String cache management functions
string_cache_t* create_string_cache(int32_t capacity);
void free_string_cache(string_cache_t* cache);

// Optimized batch string extraction
int extract_string_column_batch(duckdb_result* result, 
                               int32_t col_idx,
                               int64_t start_row,
                               int64_t batch_size,
                               void* null_map,
                               char*** string_data,
                               string_cache_t* string_cache);

#endif /* DUCKDB_GO_ADAPTER_H */