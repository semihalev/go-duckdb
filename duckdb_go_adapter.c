/**
 * duckdb_go_adapter.c - High performance C adapter for DuckDB Go driver
 * 
 * This adapter drastically reduces CGO overhead by:
 * 1. Processing entire result sets in a single operation
 * 2. Using vectorized operations for column data
 * 3. Minimizing memory allocations and copying
 * 4. Providing zero-copy access where possible
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