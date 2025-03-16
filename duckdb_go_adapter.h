/**
 * duckdb_go_adapter.h - Header for high performance C adapter for DuckDB Go driver
 */

#ifndef DUCKDB_GO_ADAPTER_H
#define DUCKDB_GO_ADAPTER_H

#include <stdint.h>
#include "include/duckdb.h"

// Column metadata structure
typedef struct {
    char* name;
    int32_t _type;     // Using _type to avoid conflict with C++ keyword
    int32_t physical_type;
    int32_t nullable;
} column_meta_t;

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

// Execute a query and store the entire result set in a single operation
int execute_query_vectorized(duckdb_connection connection, const char* query, result_buffer_t* buffer);

// Execute a prepared statement and store the result set
// Parameters are bound directly to the statement before calling this function
int execute_prepared_vectorized(duckdb_prepared_statement statement, 
                               result_buffer_t* buffer);

// Clean up all resources associated with a result buffer
void free_result_buffer(result_buffer_t* buffer);

// Reference counting functions for safer resource management
void increase_buffer_ref(result_buffer_t* buffer);
void decrease_buffer_ref(result_buffer_t* buffer);

#endif /* DUCKDB_GO_ADAPTER_H */