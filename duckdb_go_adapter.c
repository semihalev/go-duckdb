/**
 * duckdb_go_adapter.c - High performance C adapter for DuckDB Go driver
 * 
 * This adapter drastically reduces CGO overhead by:
 * 1. Processing entire result sets in a single operation
 * 2. Using vectorized operations for column data
 * 3. Minimizing memory allocations and copying
 * 4. Providing zero-copy access where possible
 * 5. Supporting batch parameter binding for prepared statements
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include "include/duckdb.h"

// Include our adapter header
#include "duckdb_go_adapter.h"

// Forward declarations
static void destroy_result_buffer(result_buffer_t* buffer);
static int grow_string_buffer(result_buffer_t* buffer, int64_t additional_size);
static int process_column_data(duckdb_result* result, result_buffer_t* buffer);
static int64_t copy_strings_to_buffer(duckdb_result* result, int32_t col, result_buffer_t* buffer);

// Implementation of helper functions for DuckDB prepared statements
const char* duckdb_prepare_error_message(duckdb_prepared_statement prepared_statement) {
    return duckdb_prepare_error(prepared_statement);
}

duckdb_prepared_statement* duckdb_prepared_statement_unsafe_ptr(duckdb_prepared_statement prepared_statement) {
    // This just returns the prepared statement pointer for direct access
    // Used to bypass CGO overhead when we need the raw pointer in Go
    duckdb_prepared_statement* ptr = malloc(sizeof(duckdb_prepared_statement));
    if (ptr) {
        *ptr = prepared_statement;
    }
    return ptr;
}

// Create a parameter batch for efficient binding
param_batch_t* create_param_batch(int32_t param_count, int32_t batch_size) {
    if (param_count <= 0 || batch_size <= 0) {
        return NULL;
    }
    
    // Calculate total number of parameters
    int32_t total_params = param_count * batch_size;
    
    // Allocate the param_batch structure
    param_batch_t* batch = (param_batch_t*)calloc(1, sizeof(param_batch_t));
    if (batch == NULL) {
        return NULL;
    }
    
    // Initialize basic fields
    batch->param_count = param_count;
    batch->batch_size = batch_size;
    batch->resource_count = 0;
    batch->resource_capacity = 16; // Initial capacity for resources
    
    // Allocate arrays for parameters
    batch->null_flags = (int8_t*)calloc(total_params, sizeof(int8_t));
    batch->param_types = (int32_t*)calloc(total_params, sizeof(int32_t));
    
    // Allocate arrays for each data type
    batch->bool_data = (int8_t*)calloc(total_params, sizeof(int8_t));
    batch->int8_data = (int8_t*)calloc(total_params, sizeof(int8_t));
    batch->int16_data = (int16_t*)calloc(total_params, sizeof(int16_t));
    batch->int32_data = (int32_t*)calloc(total_params, sizeof(int32_t));
    batch->int64_data = (int64_t*)calloc(total_params, sizeof(int64_t));
    batch->uint8_data = (uint8_t*)calloc(total_params, sizeof(uint8_t));
    batch->uint16_data = (uint16_t*)calloc(total_params, sizeof(uint16_t));
    batch->uint32_data = (uint32_t*)calloc(total_params, sizeof(uint32_t));
    batch->uint64_data = (uint64_t*)calloc(total_params, sizeof(uint64_t));
    batch->float_data = (float*)calloc(total_params, sizeof(float));
    batch->double_data = (double*)calloc(total_params, sizeof(double));
    
    // Allocate arrays for variable-length data
    batch->string_data = (char**)calloc(total_params, sizeof(char*));
    batch->blob_data = (void**)calloc(total_params, sizeof(void*));
    batch->blob_lengths = (int64_t*)calloc(total_params, sizeof(int64_t));
    
    // Allocate timestamp data
    batch->timestamp_data = (int64_t*)calloc(total_params, sizeof(int64_t));
    
    // Allocate resources array for tracking memory
    batch->resources = (void**)calloc(batch->resource_capacity, sizeof(void*));
    
    // Check if any allocation failed
    if (batch->null_flags == NULL || batch->param_types == NULL ||
        batch->bool_data == NULL || batch->int8_data == NULL || 
        batch->int16_data == NULL || batch->int32_data == NULL || 
        batch->int64_data == NULL || batch->uint8_data == NULL || 
        batch->uint16_data == NULL || batch->uint32_data == NULL || 
        batch->uint64_data == NULL || batch->float_data == NULL || 
        batch->double_data == NULL || batch->string_data == NULL || 
        batch->blob_data == NULL || batch->blob_lengths == NULL ||
        batch->timestamp_data == NULL || batch->resources == NULL) {
        
        // Free any allocated memory
        free_param_batch(batch);
        return NULL;
    }
    
    return batch;
}

// Free all resources associated with a parameter batch
void free_param_batch(param_batch_t* batch) {
    if (batch == NULL) {
        return;
    }
    
    // Free all tracked resources
    for (int32_t i = 0; i < batch->resource_count; i++) {
        if (batch->resources[i] != NULL) {
            free(batch->resources[i]);
        }
    }
    
    // Free all arrays
    free(batch->null_flags);
    free(batch->param_types);
    free(batch->bool_data);
    free(batch->int8_data);
    free(batch->int16_data);
    free(batch->int32_data);
    free(batch->int64_data);
    free(batch->uint8_data);
    free(batch->uint16_data);
    free(batch->uint32_data);
    free(batch->uint64_data);
    free(batch->float_data);
    free(batch->double_data);
    free(batch->string_data);
    free(batch->blob_data);
    free(batch->blob_lengths);
    free(batch->timestamp_data);
    free(batch->resources);
    
    // Free the batch itself
    free(batch);
}

// Ensure the resources array in param_batch has enough capacity
int ensure_param_batch_resource_capacity(param_batch_t* batch) {
    if (batch->resource_count >= batch->resource_capacity) {
        int32_t new_capacity = batch->resource_capacity * 2;
        if (new_capacity == 0) {
            new_capacity = 16; // Initial capacity
        }
        
        void** new_resources = (void**)realloc(batch->resources, new_capacity * sizeof(void*));
        if (new_resources == NULL) {
            return 0; // Allocation failed
        }
        
        batch->resources = new_resources;
        batch->resource_capacity = new_capacity;
    }
    
    return 1; // Success
}

// Bind parameters in a batch and execute a prepared statement
int bind_and_execute_batch(duckdb_prepared_statement statement, 
                          param_batch_t* params,
                          result_buffer_t* buffer) {
    if (statement == NULL || params == NULL || buffer == NULL) {
        return 0; // Failure due to invalid parameters
    }
    
    // Zero initialize the buffer
    memset(buffer, 0, sizeof(result_buffer_t));
    
    // Initialize reference count to 1
    buffer->ref_count = 1;
    
    // Check parameter counts
    idx_t param_count = duckdb_nparams(statement);
    if ((idx_t)params->param_count != param_count) {
        buffer->error_code = 50;
        buffer->error_message = strdup("Parameter count mismatch");
        return 0;
    }

    // Allocate resource tracking array
    buffer->resources = calloc(params->batch_size + 10, sizeof(void*)); // Space for resources
    buffer->resource_count = 0;
    if (!buffer->resources) {
        buffer->error_code = 51;
        buffer->error_message = strdup("Failed to allocate resource tracking memory");
        return 0;
    }
    
    int64_t total_rows_affected = 0;
    
    // Process each batch set
    for (int32_t batch_idx = 0; batch_idx < params->batch_size; batch_idx++) {
        // First, reset the parameter bindings
        duckdb_clear_bindings(statement);
        
        // Bind parameters for this batch entry
        for (int32_t param_idx = 0; param_idx < params->param_count; param_idx++) {
            // Calculate the flat index in the batch arrays
            int32_t flat_idx = batch_idx * params->param_count + param_idx;
            
            // Get parameter index (1-based in DuckDB API)
            idx_t duckdb_param_idx = param_idx + 1;
            
            // Check if parameter is NULL
            if (params->null_flags[flat_idx]) {
                if (duckdb_bind_null(statement, duckdb_param_idx) == DuckDBError) {
                    buffer->error_code = 52;
                    buffer->error_message = strdup("Failed to bind NULL parameter");
                    return 0;
                }
                continue;
            }
            
            // Bind based on parameter type
            int32_t param_type = params->param_types[param_idx];
            switch (param_type) {
                case PARAM_BOOL: {
                    int8_t* bool_ptr = &params->bool_data[flat_idx];
                    if (duckdb_bind_boolean(statement, duckdb_param_idx, *bool_ptr ? 1 : 0) == DuckDBError) {
                        buffer->error_code = 53;
                        buffer->error_message = strdup("Failed to bind boolean parameter");
                        return 0;
                    }
                    break;
                }
                
                case PARAM_INT8: {
                    int8_t* int8_ptr = &params->int8_data[flat_idx];
                    if (duckdb_bind_int8(statement, duckdb_param_idx, *int8_ptr) == DuckDBError) {
                        buffer->error_code = 54;
                        buffer->error_message = strdup("Failed to bind int8 parameter");
                        return 0;
                    }
                    break;
                }
                
                case PARAM_INT16: {
                    int16_t* int16_ptr = &params->int16_data[flat_idx];
                    if (duckdb_bind_int16(statement, duckdb_param_idx, *int16_ptr) == DuckDBError) {
                        buffer->error_code = 55;
                        buffer->error_message = strdup("Failed to bind int16 parameter");
                        return 0;
                    }
                    break;
                }
                
                case PARAM_INT32: {
                    int32_t* int32_ptr = &params->int32_data[flat_idx];
                    if (duckdb_bind_int32(statement, duckdb_param_idx, *int32_ptr) == DuckDBError) {
                        buffer->error_code = 56;
                        buffer->error_message = strdup("Failed to bind int32 parameter");
                        return 0;
                    }
                    break;
                }
                
                case PARAM_INT64: {
                    int64_t* int64_ptr = &params->int64_data[flat_idx];
                    if (duckdb_bind_int64(statement, duckdb_param_idx, *int64_ptr) == DuckDBError) {
                        buffer->error_code = 57;
                        buffer->error_message = strdup("Failed to bind int64 parameter");
                        return 0;
                    }
                    break;
                }
                
                case PARAM_UINT8: {
                    uint8_t* uint8_ptr = &params->uint8_data[flat_idx];
                    if (duckdb_bind_uint8(statement, duckdb_param_idx, *uint8_ptr) == DuckDBError) {
                        buffer->error_code = 58;
                        buffer->error_message = strdup("Failed to bind uint8 parameter");
                        return 0;
                    }
                    break;
                }
                
                case PARAM_UINT16: {
                    uint16_t* uint16_ptr = &params->uint16_data[flat_idx];
                    if (duckdb_bind_uint16(statement, duckdb_param_idx, *uint16_ptr) == DuckDBError) {
                        buffer->error_code = 59;
                        buffer->error_message = strdup("Failed to bind uint16 parameter");
                        return 0;
                    }
                    break;
                }
                
                case PARAM_UINT32: {
                    uint32_t* uint32_ptr = &params->uint32_data[flat_idx];
                    if (duckdb_bind_uint32(statement, duckdb_param_idx, *uint32_ptr) == DuckDBError) {
                        buffer->error_code = 60;
                        buffer->error_message = strdup("Failed to bind uint32 parameter");
                        return 0;
                    }
                    break;
                }
                
                case PARAM_UINT64: {
                    uint64_t* uint64_ptr = &params->uint64_data[flat_idx];
                    if (duckdb_bind_uint64(statement, duckdb_param_idx, *uint64_ptr) == DuckDBError) {
                        buffer->error_code = 61;
                        buffer->error_message = strdup("Failed to bind uint64 parameter");
                        return 0;
                    }
                    break;
                }
                
                case PARAM_FLOAT: {
                    float* float_ptr = &params->float_data[flat_idx];
                    if (duckdb_bind_float(statement, duckdb_param_idx, *float_ptr) == DuckDBError) {
                        buffer->error_code = 62;
                        buffer->error_message = strdup("Failed to bind float parameter");
                        return 0;
                    }
                    break;
                }
                
                case PARAM_DOUBLE: {
                    double* double_ptr = &params->double_data[flat_idx];
                    if (duckdb_bind_double(statement, duckdb_param_idx, *double_ptr) == DuckDBError) {
                        buffer->error_code = 63;
                        buffer->error_message = strdup("Failed to bind double parameter");
                        return 0;
                    }
                    break;
                }
                
                case PARAM_STRING: {
                    char** string_ptr = &params->string_data[flat_idx];
                    if (*string_ptr != NULL) {
                        if (duckdb_bind_varchar(statement, duckdb_param_idx, *string_ptr) == DuckDBError) {
                            buffer->error_code = 64;
                            buffer->error_message = strdup("Failed to bind string parameter");
                            return 0;
                        }
                    } else {
                        // Empty string
                        if (duckdb_bind_varchar(statement, duckdb_param_idx, "") == DuckDBError) {
                            buffer->error_code = 65;
                            buffer->error_message = strdup("Failed to bind empty string parameter");
                            return 0;
                        }
                    }
                    break;
                }
                
                case PARAM_BLOB: {
                    void** blob_ptr = &params->blob_data[flat_idx];
                    int64_t* length_ptr = &params->blob_lengths[flat_idx];
                    
                    if (*blob_ptr != NULL && *length_ptr > 0) {
                        if (duckdb_bind_blob(statement, duckdb_param_idx, *blob_ptr, *length_ptr) == DuckDBError) {
                            buffer->error_code = 66;
                            buffer->error_message = strdup("Failed to bind blob parameter");
                            return 0;
                        }
                    } else {
                        // Empty blob
                        if (duckdb_bind_blob(statement, duckdb_param_idx, NULL, 0) == DuckDBError) {
                            buffer->error_code = 67;
                            buffer->error_message = strdup("Failed to bind empty blob parameter");
                            return 0;
                        }
                    }
                    break;
                }
                
                case PARAM_TIMESTAMP: {
                    int64_t* timestamp_ptr = &params->timestamp_data[flat_idx];
                    duckdb_timestamp ts;
                    ts.micros = *timestamp_ptr;
                    if (duckdb_bind_timestamp(statement, duckdb_param_idx, ts) == DuckDBError) {
                        buffer->error_code = 68;
                        buffer->error_message = strdup("Failed to bind timestamp parameter");
                        return 0;
                    }
                    break;
                }
                
                default: {
                    buffer->error_code = 69;
                    buffer->error_message = strdup("Unsupported parameter type");
                    return 0;
                }
            }
        }
        
        // Execute the statement
        duckdb_result result;
        if (duckdb_execute_prepared(statement, &result) == DuckDBError) {
            buffer->error_code = 70;
            buffer->error_message = strdup(duckdb_result_error(&result));
            duckdb_destroy_result(&result);
            return 0;
        }
        
        // Add to the total rows affected count
        total_rows_affected += duckdb_rows_changed(&result);
        
        // Clean up the result
        duckdb_destroy_result(&result);
    }
    
    // Store the total rows affected
    buffer->rows_affected = total_rows_affected;
    
    return 1; // Success
}

// Helper functions for boolean conversion - these avoid CGO type conversion issues
// Use int8_t consistently for all boolean values in the Go interface
static inline int8_t duckdb_bool_to_int8(bool value) {
    return value ? 1 : 0;
}

// Create specialized conversion functions for each bool usage context
// This helps with CGO type conversion issues by being explicit about the context
static inline int8_t convert_null_value(bool is_null) {
    return is_null ? 1 : 0;
}

static inline int8_t convert_boolean_value(bool value) {
    return value ? 1 : 0;
}

// Convert DuckDB's boolean return value to int8_t for consistent handling
static inline int8_t convert_duckdb_boolean(bool value) {
    return value ? 1 : 0;
}

/**
 * Date and timestamp helper functions
 * These provide efficient conversion between DuckDB's date/timestamp representation and Unix time
 */

// Convert DuckDB date (days since 1970-01-01) to Unix timestamp at UTC midnight
static inline int64_t duckdb_date_to_unix_seconds(duckdb_date date) {
    // Convert days to seconds (86400 seconds per day)
    return (int64_t)date.days * 86400;
}

// Convert DuckDB timestamp (microseconds since 1970-01-01) to Unix timestamp with microsecond precision
static inline int64_t duckdb_timestamp_to_unix_micros(duckdb_timestamp ts) {
    return ts.micros;
}

// Convert DuckDB timestamp_s (seconds since 1970-01-01) to Unix timestamp with microsecond precision
static inline int64_t duckdb_timestamp_s_to_unix_micros(duckdb_timestamp_s ts) {
    return (int64_t)ts.seconds * 1000000; // Convert seconds to microseconds
}

// Convert DuckDB timestamp_ms (milliseconds since 1970-01-01) to Unix timestamp with microsecond precision
static inline int64_t duckdb_timestamp_ms_to_unix_micros(duckdb_timestamp_ms ts) {
    return (int64_t)ts.millis * 1000; // Convert milliseconds to microseconds
}

// Convert DuckDB timestamp_ns (nanoseconds since 1970-01-01) to Unix timestamp with microsecond precision
static inline int64_t duckdb_timestamp_ns_to_unix_micros(duckdb_timestamp_ns ts) {
    return ts.nanos / 1000; // Convert nanoseconds to microseconds
}

// Extract date and time components from a DuckDB timestamp
static inline void duckdb_extract_timestamp_components(duckdb_timestamp ts, int64_t* unix_seconds, int32_t* nanos) {
    *unix_seconds = ts.micros / 1000000;
    *nanos = (ts.micros % 1000000) * 1000;
}

/**
 * Vector extraction functions for efficient batch processing
 * These functions significantly reduce CGO boundary crossings by extracting
 * multiple values at once instead of making individual calls per value.
 */

// Extract boolean values in batch
void extract_vector_bool(duckdb_result *result, idx_t col_idx, idx_t offset, 
                        idx_t batch_size, bool *values, bool *nulls) {
    idx_t i;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            values[i] = duckdb_value_boolean(result, col_idx, offset + i);
        }
    }
}

// Extract int8 values in batch
void extract_vector_int8(duckdb_result *result, idx_t col_idx, idx_t offset, 
                        idx_t batch_size, int8_t *values, bool *nulls) {
    idx_t i;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            values[i] = duckdb_value_int8(result, col_idx, offset + i);
        }
    }
}

// Extract int16 values in batch
void extract_vector_int16(duckdb_result *result, idx_t col_idx, idx_t offset, 
                         idx_t batch_size, int16_t *values, bool *nulls) {
    idx_t i;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            values[i] = duckdb_value_int16(result, col_idx, offset + i);
        }
    }
}

// Extract int32 values in batch
void extract_vector_int32(duckdb_result *result, idx_t col_idx, idx_t offset, 
                         idx_t batch_size, int32_t *values, bool *nulls) {
    idx_t i;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            values[i] = duckdb_value_int32(result, col_idx, offset + i);
        }
    }
}

// Extract int64 values in batch
void extract_vector_int64(duckdb_result *result, idx_t col_idx, idx_t offset, 
                         idx_t batch_size, int64_t *values, bool *nulls) {
    idx_t i;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            values[i] = duckdb_value_int64(result, col_idx, offset + i);
        }
    }
}

// Extract uint8 values in batch
void extract_vector_uint8(duckdb_result *result, idx_t col_idx, idx_t offset, 
                         idx_t batch_size, uint8_t *values, bool *nulls) {
    idx_t i;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            values[i] = duckdb_value_uint8(result, col_idx, offset + i);
        }
    }
}

// Extract uint16 values in batch
void extract_vector_uint16(duckdb_result *result, idx_t col_idx, idx_t offset, 
                          idx_t batch_size, uint16_t *values, bool *nulls) {
    idx_t i;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            values[i] = duckdb_value_uint16(result, col_idx, offset + i);
        }
    }
}

// Extract uint32 values in batch
void extract_vector_uint32(duckdb_result *result, idx_t col_idx, idx_t offset, 
                          idx_t batch_size, uint32_t *values, bool *nulls) {
    idx_t i;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            values[i] = duckdb_value_uint32(result, col_idx, offset + i);
        }
    }
}

// Extract uint64 values in batch
void extract_vector_uint64(duckdb_result *result, idx_t col_idx, idx_t offset, 
                          idx_t batch_size, uint64_t *values, bool *nulls) {
    idx_t i;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            values[i] = duckdb_value_uint64(result, col_idx, offset + i);
        }
    }
}

// Extract float32 values in batch
void extract_vector_float32(duckdb_result *result, idx_t col_idx, idx_t offset, 
                           idx_t batch_size, float *values, bool *nulls) {
    idx_t i;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            values[i] = duckdb_value_float(result, col_idx, offset + i);
        }
    }
}

// Extract float64 values in batch
void extract_vector_float64(duckdb_result *result, idx_t col_idx, idx_t offset, 
                           idx_t batch_size, double *values, bool *nulls) {
    idx_t i;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            values[i] = duckdb_value_double(result, col_idx, offset + i);
        }
    }
}

// Extract timestamp values in batch
void extract_vector_timestamp(duckdb_result *result, idx_t col_idx, idx_t offset, 
                             idx_t batch_size, int64_t *values, bool *nulls) {
    idx_t i;
    duckdb_timestamp ts;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            ts = duckdb_value_timestamp(result, col_idx, offset + i);
            values[i] = ts.micros;
        }
    }
}

// Extract date values in batch
void extract_vector_date(duckdb_result *result, idx_t col_idx, idx_t offset, 
                        idx_t batch_size, int32_t *values, bool *nulls) {
    idx_t i;
    duckdb_date date;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            date = duckdb_value_date(result, col_idx, offset + i);
            values[i] = date.days;
        }
    }
}

// Extract string values in batch - this is more complex because we need to allocate memory for each string
int extract_vector_string(duckdb_result *result, idx_t col_idx, idx_t offset, 
                         idx_t batch_size, char **values, idx_t *lengths, bool *nulls) {
    idx_t i;
    char *str;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            // Get the string from DuckDB
            str = duckdb_value_varchar(result, col_idx, offset + i);
            
            if (str != NULL) {
                // Store the string pointer for Go to access
                values[i] = str;
                
                // Calculate and store the length
                lengths[i] = strlen(str);
            } else {
                values[i] = NULL;
                lengths[i] = 0;
            }
        } else {
            values[i] = NULL;
            lengths[i] = 0;
        }
    }
    
    return 1; // Success
}

// Extract blob values in batch - similar to strings but with explicit length management
int extract_vector_blob(duckdb_result *result, idx_t col_idx, idx_t offset, 
                       idx_t batch_size, char **values, idx_t *lengths, bool *nulls) {
    idx_t i;
    duckdb_blob blob;
    
    // First extract all nulls in a single pass
    for (i = 0; i < batch_size; i++) {
        nulls[i] = duckdb_value_is_null(result, col_idx, offset + i);
    }
    
    // Then extract all non-null values
    for (i = 0; i < batch_size; i++) {
        if (!nulls[i]) {
            // Get the blob from DuckDB
            blob = duckdb_value_blob(result, col_idx, offset + i);
            
            if (blob.data != NULL) {
                // Store the blob data pointer for Go to access
                values[i] = (char*)blob.data;
                
                // Store the size
                lengths[i] = blob.size;
            } else {
                values[i] = NULL;
                lengths[i] = 0;
            }
        } else {
            values[i] = NULL;
            lengths[i] = 0;
        }
    }
    
    return 1; // Success
}

// We already declared temporal_data_t in the header file

/**
 * Execute a query and store the entire result set in a single operation
 * This dramatically reduces CGO boundary crossings
 */
// Process date column data and store as seconds since epoch
static int process_date_column(duckdb_result* result, int32_t col, int64_t row_count, result_buffer_t* buffer) {
    // Allocate array for date data (int64_t for seconds since epoch)
    int64_t* date_data = calloc(row_count, sizeof(int64_t));
    if (!date_data) {
        buffer->error_code = 20;
        buffer->error_message = strdup("Failed to allocate date data memory");
        return 0;
    }
    
    // Add to resources for cleanup
    buffer->resources[buffer->resource_count++] = date_data;
    
    // Get date values for each row
    for (int64_t row = 0; row < row_count; row++) {
        // Skip NULL values (already handled in nulls bitmap)
        if (buffer->nulls_ptrs[col][row]) {
            continue;
        }
        
        // Get date value and convert to seconds since epoch
        duckdb_date date = duckdb_value_date(result, col, row);
        date_data[row] = duckdb_date_to_unix_seconds(date);
    }
    
    // Store date data in temporal data structure
    if (!buffer->temporal_data) {
        buffer->temporal_data = calloc(1, sizeof(temporal_data_t));
        if (!buffer->temporal_data) {
            buffer->error_code = 21;
            buffer->error_message = strdup("Failed to allocate temporal data structure");
            return 0;
        }
        buffer->resources[buffer->resource_count++] = buffer->temporal_data;
    }
    
    buffer->temporal_data->date_data = date_data;
    buffer->temporal_data->has_date_data = 1;
    
    // Store the date data pointer in the data_ptrs array as well
    buffer->data_ptrs[col] = date_data;
    
    return 1;
}

// Process timestamp column data and store as seconds + nanoseconds
static int process_timestamp_column(duckdb_result* result, int32_t col, int64_t row_count, result_buffer_t* buffer) {
    // Allocate arrays for timestamp data
    int64_t* seconds_data = calloc(row_count, sizeof(int64_t));
    int32_t* nanos_data = calloc(row_count, sizeof(int32_t));
    
    if (!seconds_data || !nanos_data) {
        if (seconds_data) free(seconds_data);
        if (nanos_data) free(nanos_data);
        buffer->error_code = 22;
        buffer->error_message = strdup("Failed to allocate timestamp data memory");
        return 0;
    }
    
    // Add to resources for cleanup
    buffer->resources[buffer->resource_count++] = seconds_data;
    buffer->resources[buffer->resource_count++] = nanos_data;
    
    // Get timestamp values for each row
    for (int64_t row = 0; row < row_count; row++) {
        // Skip NULL values (already handled in nulls bitmap)
        if (buffer->nulls_ptrs[col][row]) {
            continue;
        }
        
        // Get timestamp value and extract components
        duckdb_timestamp ts = duckdb_value_timestamp(result, col, row);
        int64_t unix_seconds;
        int32_t nanos;
        duckdb_extract_timestamp_components(ts, &unix_seconds, &nanos);
        
        seconds_data[row] = unix_seconds;
        nanos_data[row] = nanos;
    }
    
    // Store timestamp data in temporal data structure
    if (!buffer->temporal_data) {
        buffer->temporal_data = calloc(1, sizeof(temporal_data_t));
        if (!buffer->temporal_data) {
            buffer->error_code = 21;
            buffer->error_message = strdup("Failed to allocate temporal data structure");
            return 0;
        }
        buffer->resources[buffer->resource_count++] = buffer->temporal_data;
    }
    
    buffer->temporal_data->timestamp_seconds = seconds_data;
    buffer->temporal_data->timestamp_nanos = nanos_data;
    buffer->temporal_data->has_timestamp_data = 1;
    
    // Store the timestamp seconds data pointer in the data_ptrs array
    // The nanos data is accessible through the temporal_data structure
    buffer->data_ptrs[col] = seconds_data;
    
    return 1;
}

/**
 * Reference counting functions for safer resource management
 */
void increase_buffer_ref(result_buffer_t* buffer) {
    if (!buffer) return;
    buffer->ref_count++;
}

void decrease_buffer_ref(result_buffer_t* buffer) {
    if (!buffer) return;
    if (buffer->ref_count > 0) {
        buffer->ref_count--;
        if (buffer->ref_count == 0) {
            free_result_buffer(buffer);
        }
    }
}

int execute_query_vectorized(duckdb_connection connection, const char* query, result_buffer_t* buffer) {
    if (!connection || !query || !buffer) {
        return 0; // Invalid parameters
    }
    
    // Zero initialize the buffer
    memset(buffer, 0, sizeof(result_buffer_t));
    
    // Initialize reference count to 1
    buffer->ref_count = 1;
    
    // Execute the query
    duckdb_result result;
    if (duckdb_query(connection, query, &result) == DuckDBError) {
        buffer->error_code = 1;
        buffer->error_message = strdup(duckdb_result_error(&result));
        duckdb_destroy_result(&result);
        return 0;
    }
    
    // Get basic metadata
    buffer->row_count = duckdb_row_count(&result);
    buffer->column_count = duckdb_column_count(&result);
    
    // Get affected rows count for DML statements
    buffer->rows_affected = duckdb_rows_changed(&result);
    
    // Early return for empty result sets
    if (buffer->row_count == 0 || buffer->column_count == 0) {
        duckdb_destroy_result(&result);
        return 1;
    }
    
    // Allocate column metadata array
    buffer->columns = calloc(buffer->column_count, sizeof(column_meta_t));
    if (!buffer->columns) {
        buffer->error_code = 2;
        buffer->error_message = strdup("Failed to allocate column metadata memory");
        duckdb_destroy_result(&result);
        return 0;
    }
    
    // Allocate data pointers array
    buffer->data_ptrs = calloc(buffer->column_count, sizeof(void*));
    buffer->nulls_ptrs = calloc(buffer->column_count, sizeof(int8_t*));
    if (!buffer->data_ptrs || !buffer->nulls_ptrs) {
        buffer->error_code = 3;
        buffer->error_message = strdup("Failed to allocate data pointers memory");
        destroy_result_buffer(buffer);
        duckdb_destroy_result(&result);
        return 0;
    }
    
    // Allocate resource tracking array
    buffer->resources = calloc(buffer->column_count * 2 + 10, sizeof(void*)); // Extra space for misc allocations
    buffer->resource_count = 0;
    if (!buffer->resources) {
        buffer->error_code = 4;
        buffer->error_message = strdup("Failed to allocate resource tracking memory");
        destroy_result_buffer(buffer);
        duckdb_destroy_result(&result);
        return 0;
    }
    
    // Store column metadata
    for (int32_t i = 0; i < buffer->column_count; i++) {
        // Get column name - we'll need to free this memory later
        buffer->columns[i].name = strdup(duckdb_column_name(&result, i));
        buffer->resources[buffer->resource_count++] = buffer->columns[i].name;
        
        // Get column type information
        buffer->columns[i]._type = duckdb_column_type(&result, i);
        buffer->columns[i].nullable = 1; // Assume all columns are nullable for now
    }
    
    // Process column data - this does the heavy lifting
    if (!process_column_data(&result, buffer)) {
        destroy_result_buffer(buffer);
        duckdb_destroy_result(&result);
        return 0;
    }
    
    // Clean up DuckDB result
    duckdb_destroy_result(&result);
    return 1;
}

/**
 * Process all column data in vectorized fashion
 * This is the core optimization that makes this adapter fast
 */
static int process_column_data(duckdb_result* result, result_buffer_t* buffer) {
    int64_t row_count = buffer->row_count;
    
    // Process each column
    for (int32_t col = 0; col < buffer->column_count; col++) {
        int32_t col_type = buffer->columns[col]._type;
        
        // Allocate NULL bitmap for this column (1 byte per row)
        buffer->nulls_ptrs[col] = calloc(row_count, sizeof(int8_t));
        if (!buffer->nulls_ptrs[col]) {
            buffer->error_code = 5;
            buffer->error_message = strdup("Failed to allocate NULL bitmap memory");
            return 0;
        }
        buffer->resources[buffer->resource_count++] = buffer->nulls_ptrs[col];
        
        // Extract NULL information first - convert bool to int8_t for CGO compatibility
        for (int64_t row = 0; row < row_count; row++) {
            // Convert from C bool to int8_t (0 or 1) for CGO compatibility
            bool is_null = duckdb_value_is_null(result, col, row);
            buffer->nulls_ptrs[col][row] = convert_null_value(is_null);
        }
        
        // Process based on column type
        switch (col_type) {
            case DUCKDB_TYPE_BOOLEAN: {
                // Allocate memory for boolean values (1 byte per value)
                int8_t* data = calloc(row_count, sizeof(int8_t));
                if (!data) {
                    buffer->error_code = 6;
                    buffer->error_message = strdup("Failed to allocate boolean column memory");
                    return 0;
                }
                buffer->resources[buffer->resource_count++] = data;
                buffer->data_ptrs[col] = data;
                
                // Extract all values at once - convert bool to int8_t for CGO compatibility
                for (int64_t row = 0; row < row_count; row++) {
                    if (!buffer->nulls_ptrs[col][row]) {
                        bool value = duckdb_value_boolean(result, col, row);
                        data[row] = convert_boolean_value(value);
                    }
                }
                break;
            }
            
            case DUCKDB_TYPE_DATE: {
                // Use specialized handler for date type
                if (!process_date_column(result, col, row_count, buffer)) {
                    return 0;
                }
                break;
            }
            
            case DUCKDB_TYPE_TIMESTAMP:
            case DUCKDB_TYPE_TIMESTAMP_S:
            case DUCKDB_TYPE_TIMESTAMP_MS:
            case DUCKDB_TYPE_TIMESTAMP_NS: {
                // Use specialized handler for timestamp types
                if (!process_timestamp_column(result, col, row_count, buffer)) {
                    return 0;
                }
                break;
            }
            
            case DUCKDB_TYPE_TINYINT: {
                // Allocate memory for int8 values
                int8_t* data = calloc(row_count, sizeof(int8_t));
                if (!data) {
                    buffer->error_code = 7;
                    buffer->error_message = strdup("Failed to allocate tinyint column memory");
                    return 0;
                }
                buffer->resources[buffer->resource_count++] = data;
                buffer->data_ptrs[col] = data;
                
                // Extract all values at once
                for (int64_t row = 0; row < row_count; row++) {
                    if (!buffer->nulls_ptrs[col][row]) {
                        data[row] = duckdb_value_int8(result, col, row);
                    }
                }
                break;
            }
            
            case DUCKDB_TYPE_SMALLINT: {
                // Allocate memory for int16 values
                int16_t* data = calloc(row_count, sizeof(int16_t));
                if (!data) {
                    buffer->error_code = 8;
                    buffer->error_message = strdup("Failed to allocate smallint column memory");
                    return 0;
                }
                buffer->resources[buffer->resource_count++] = data;
                buffer->data_ptrs[col] = data;
                
                // Extract all values at once
                for (int64_t row = 0; row < row_count; row++) {
                    if (!buffer->nulls_ptrs[col][row]) {
                        data[row] = duckdb_value_int16(result, col, row);
                    }
                }
                break;
            }
            
            case DUCKDB_TYPE_INTEGER: {
                // Allocate memory for int32 values
                int32_t* data = calloc(row_count, sizeof(int32_t));
                if (!data) {
                    buffer->error_code = 9;
                    buffer->error_message = strdup("Failed to allocate integer column memory");
                    return 0;
                }
                buffer->resources[buffer->resource_count++] = data;
                buffer->data_ptrs[col] = data;
                
                // Extract all values at once
                for (int64_t row = 0; row < row_count; row++) {
                    if (!buffer->nulls_ptrs[col][row]) {
                        data[row] = duckdb_value_int32(result, col, row);
                    }
                }
                break;
            }
            
            case DUCKDB_TYPE_BIGINT: {
                // Allocate memory for int64 values
                int64_t* data = calloc(row_count, sizeof(int64_t));
                if (!data) {
                    buffer->error_code = 10;
                    buffer->error_message = strdup("Failed to allocate bigint column memory");
                    return 0;
                }
                buffer->resources[buffer->resource_count++] = data;
                buffer->data_ptrs[col] = data;
                
                // Extract all values at once
                for (int64_t row = 0; row < row_count; row++) {
                    if (!buffer->nulls_ptrs[col][row]) {
                        data[row] = duckdb_value_int64(result, col, row);
                    }
                }
                break;
            }
            
            case DUCKDB_TYPE_FLOAT: {
                // Allocate memory for float values
                float* data = calloc(row_count, sizeof(float));
                if (!data) {
                    buffer->error_code = 11;
                    buffer->error_message = strdup("Failed to allocate float column memory");
                    return 0;
                }
                buffer->resources[buffer->resource_count++] = data;
                buffer->data_ptrs[col] = data;
                
                // Extract all values at once
                for (int64_t row = 0; row < row_count; row++) {
                    if (!buffer->nulls_ptrs[col][row]) {
                        data[row] = duckdb_value_float(result, col, row);
                    }
                }
                break;
            }
            
            case DUCKDB_TYPE_DOUBLE: {
                // Allocate memory for double values
                double* data = calloc(row_count, sizeof(double));
                if (!data) {
                    buffer->error_code = 12;
                    buffer->error_message = strdup("Failed to allocate double column memory");
                    return 0;
                }
                buffer->resources[buffer->resource_count++] = data;
                buffer->data_ptrs[col] = data;
                
                // Extract all values at once
                for (int64_t row = 0; row < row_count; row++) {
                    if (!buffer->nulls_ptrs[col][row]) {
                        data[row] = duckdb_value_double(result, col, row);
                    }
                }
                break;
            }
            
            case DUCKDB_TYPE_VARCHAR: {
                // Handle strings specially - these are tricky because of variable length
                // We'll use offset pointers into a single string buffer
                
                // First pass: Calculate total string buffer size needed
                int64_t string_size = copy_strings_to_buffer(result, col, buffer);
                if (string_size < 0) {
                    // Error occurred
                    return 0;
                }
                break;
            }
            
            default: {
                // For unsupported types, convert to strings
                int64_t string_size = copy_strings_to_buffer(result, col, buffer);
                if (string_size < 0) {
                    // Error occurred
                    return 0;
                }
                break;
            }
        }
    }
    
    return 1;
}

/**
 * Copy strings to a unified buffer to minimize allocations
 * Returns total string size or -1 on error
 */
static int64_t copy_strings_to_buffer(duckdb_result* result, int32_t col, result_buffer_t* buffer) {
    int64_t row_count = buffer->row_count;
    
    // First calculate total string size needed
    int64_t total_size = 0;
    int64_t* offsets = calloc(row_count, sizeof(int64_t));
    if (!offsets) {
        buffer->error_code = 13;
        buffer->error_message = strdup("Failed to allocate string offsets memory");
        return -1;
    }
    buffer->resources[buffer->resource_count++] = offsets;
    
    // First pass: calculate total size and create offset table
    for (int64_t row = 0; row < row_count; row++) {
        if (buffer->nulls_ptrs[col][row]) {
            offsets[row] = -1; // Special marker for NULL
            continue;
        }
        
        char* str = duckdb_value_varchar(result, col, row);
        if (str) {
            int64_t len = strlen(str);
            offsets[row] = total_size;
            total_size += len + 1; // Include null terminator
            free(str); // Free immediately since we're just measuring
        } else {
            offsets[row] = total_size;
            total_size += 1; // Just a null terminator for empty string
        }
    }
    
    // Allocate or expand the string buffer if needed
    if (buffer->string_buffer == NULL) {
        buffer->string_buffer = malloc(total_size);
        buffer->string_buffer_size = total_size;
        if (!buffer->string_buffer) {
            buffer->error_code = 14;
            buffer->error_message = strdup("Failed to allocate string buffer memory");
            return -1;
        }
        buffer->resources[buffer->resource_count++] = buffer->string_buffer;
    } else if (buffer->string_buffer_size < total_size) {
        char* new_buffer = realloc(buffer->string_buffer, buffer->string_buffer_size + total_size);
        if (!new_buffer) {
            buffer->error_code = 15;
            buffer->error_message = strdup("Failed to expand string buffer memory");
            return -1;
        }
        buffer->string_buffer = new_buffer;
        buffer->string_buffer_size += total_size;
        buffer->resources[buffer->resource_count - 1] = buffer->string_buffer; // Update resource pointer
    }
    
    // Second pass: copy strings into the buffer
    for (int64_t row = 0; row < row_count; row++) {
        if (offsets[row] == -1) {
            continue; // Skip NULL values
        }
        
        char* str = duckdb_value_varchar(result, col, row);
        if (str) {
            int64_t len = strlen(str);
            memcpy(buffer->string_buffer + offsets[row], str, len + 1);
            free(str);
        } else {
            // Empty string
            buffer->string_buffer[offsets[row]] = '\0';
        }
    }
    
    // Store the offset array as column data
    buffer->data_ptrs[col] = offsets;
    
    return total_size;
}

/**
 * Free all resources associated with a result buffer
 */
static void destroy_result_buffer(result_buffer_t* buffer) {
    if (!buffer) return;
    
    // Free all tracked resources
    for (int32_t i = 0; i < buffer->resource_count; i++) {
        if (buffer->resources[i]) {
            free(buffer->resources[i]);
        }
    }
    
    // Free the columns array if it exists
    if (buffer->columns) {
        free(buffer->columns);
    }
    
    // Free data pointers array if it exists
    if (buffer->data_ptrs) {
        free(buffer->data_ptrs);
    }
    
    // Free nulls pointers array if it exists
    if (buffer->nulls_ptrs) {
        free(buffer->nulls_ptrs);
    }
    
    // Free resources array if it exists
    if (buffer->resources) {
        free(buffer->resources);
    }
    
    // Free error message if it exists
    if (buffer->error_message) {
        free(buffer->error_message);
    }
    
    // Reset the buffer
    memset(buffer, 0, sizeof(result_buffer_t));
}

/**
 * Execute a prepared statement and store the result set
 * This is similar to execute_query_vectorized but for prepared statements
 * Parameters are bound directly to the statement before calling this function
 */
int execute_prepared_vectorized(duckdb_prepared_statement statement, 
                               result_buffer_t* buffer) {
    if (!statement || !buffer) {
        return 0; // Invalid parameters
    }
    
    // Zero initialize the buffer
    memset(buffer, 0, sizeof(result_buffer_t));
    
    // Initialize reference count to 1
    buffer->ref_count = 1;
    
    // Execute the prepared statement (parameters are already bound)
    duckdb_result result;
    if (duckdb_execute_prepared(statement, &result) == DuckDBError) {
        buffer->error_code = 1;
        buffer->error_message = strdup(duckdb_result_error(&result));
        duckdb_destroy_result(&result);
        return 0;
    }
    
    // Get basic metadata
    buffer->row_count = duckdb_row_count(&result);
    buffer->column_count = duckdb_column_count(&result);
    
    // Get affected rows count for DML statements
    buffer->rows_affected = duckdb_rows_changed(&result);
    
    // Early return for empty result sets
    if (buffer->row_count == 0 || buffer->column_count == 0) {
        duckdb_destroy_result(&result);
        return 1;
    }
    
    // Allocate column metadata array
    buffer->columns = calloc(buffer->column_count, sizeof(column_meta_t));
    if (!buffer->columns) {
        buffer->error_code = 2;
        buffer->error_message = strdup("Failed to allocate column metadata memory");
        duckdb_destroy_result(&result);
        return 0;
    }
    
    // Allocate data pointers array
    buffer->data_ptrs = calloc(buffer->column_count, sizeof(void*));
    buffer->nulls_ptrs = calloc(buffer->column_count, sizeof(int8_t*));
    if (!buffer->data_ptrs || !buffer->nulls_ptrs) {
        buffer->error_code = 3;
        buffer->error_message = strdup("Failed to allocate data pointers memory");
        destroy_result_buffer(buffer);
        duckdb_destroy_result(&result);
        return 0;
    }
    
    // Allocate resource tracking array
    buffer->resources = calloc(buffer->column_count * 2 + 10, sizeof(void*)); // Extra space for misc allocations
    buffer->resource_count = 0;
    if (!buffer->resources) {
        buffer->error_code = 4;
        buffer->error_message = strdup("Failed to allocate resource tracking memory");
        destroy_result_buffer(buffer);
        duckdb_destroy_result(&result);
        return 0;
    }
    
    // Store column metadata
    for (int32_t i = 0; i < buffer->column_count; i++) {
        // Get column name - we'll need to free this memory later
        buffer->columns[i].name = strdup(duckdb_column_name(&result, i));
        buffer->resources[buffer->resource_count++] = buffer->columns[i].name;
        
        // Get column type information
        buffer->columns[i]._type = duckdb_column_type(&result, i);
        buffer->columns[i].nullable = 1; // Assume all columns are nullable for now
    }
    
    // Process column data - this does the heavy lifting
    if (!process_column_data(&result, buffer)) {
        destroy_result_buffer(buffer);
        duckdb_destroy_result(&result);
        return 0;
    }
    
    // Clean up DuckDB result
    duckdb_destroy_result(&result);
    return 1;
}

/**
 * Clean up all resources associated with a result buffer
 * This should be called when the result set is no longer needed
 * With reference counting, it now delegates to decrease_buffer_ref
 */
void free_result_buffer(result_buffer_t* buffer) {
    if (!buffer) return;
    
    // If reference count is disabled (0) or this is the last reference,
    // actually destroy the buffer
    if (buffer->ref_count <= 1) {
        destroy_result_buffer(buffer);
    } else {
        // Otherwise just decrease the reference count
        buffer->ref_count--;
    }
}

// Function has already been implemented earlier in the file - removing duplicate definition