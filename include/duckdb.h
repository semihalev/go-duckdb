// DuckDB Header - v1.2.0
// This is a placeholder for the actual DuckDB header file.
// The actual header file should be downloaded from the DuckDB repository
// and placed here before building the project.

#ifndef DUCKDB_H
#define DUCKDB_H

// We'll include the basic definitions here to make it compile

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

// DuckDB version information
#define DUCKDB_VERSION_MAJOR 1
#define DUCKDB_VERSION_MINOR 2
#define DUCKDB_VERSION_PATCH 0
#define DUCKDB_SOURCE_ID "v1.2.0"

// Basic DuckDB types
typedef uint64_t idx_t;
typedef uint8_t uint8_t;
typedef uint32_t uint32_t;
typedef uint64_t uint64_t;
typedef int32_t int32_t;
typedef int64_t int64_t;

// DuckDB state
typedef enum {
	DuckDBSuccess = 0,
	DuckDBError = 1
} duckdb_state;

// DuckDB types
typedef enum {
	DUCKDB_TYPE_INVALID = 0,
	DUCKDB_TYPE_BOOLEAN = 1,
	DUCKDB_TYPE_TINYINT = 2,
	DUCKDB_TYPE_SMALLINT = 3,
	DUCKDB_TYPE_INTEGER = 4,
	DUCKDB_TYPE_BIGINT = 5,
	DUCKDB_TYPE_FLOAT = 6,
	DUCKDB_TYPE_DOUBLE = 7,
	DUCKDB_TYPE_VARCHAR = 8,
	DUCKDB_TYPE_BLOB = 9,
	DUCKDB_TYPE_TIMESTAMP = 10
} duckdb_type;

// Forward declarations of DuckDB types
typedef struct duckdb_database_struct duckdb_database;
typedef struct duckdb_connection_struct duckdb_connection;
typedef struct duckdb_prepared_statement_struct duckdb_prepared_statement;
typedef struct duckdb_appender_struct duckdb_appender;
typedef struct duckdb_result_struct duckdb_result;

// Timestamp struct
typedef struct {
	int64_t micros;
} duckdb_timestamp;

// Function declarations - to be implemented in the actual library
duckdb_state duckdb_open(const char *path, duckdb_database *out_database);
duckdb_state duckdb_connect(duckdb_database database, duckdb_connection *out_connection);
void duckdb_disconnect(duckdb_connection *connection);
void duckdb_close(duckdb_database *database);
duckdb_state duckdb_prepare(duckdb_connection connection, const char *query, duckdb_prepared_statement *out_prepared_statement);
duckdb_state duckdb_execute_prepared(duckdb_prepared_statement prepared_statement, duckdb_result *out_result);
duckdb_state duckdb_nparams(duckdb_prepared_statement prepared_statement);
duckdb_state duckdb_bind_null(duckdb_prepared_statement prepared_statement, idx_t param_idx);
duckdb_state duckdb_bind_boolean(duckdb_prepared_statement prepared_statement, idx_t param_idx, bool value);
duckdb_state duckdb_bind_int32(duckdb_prepared_statement prepared_statement, idx_t param_idx, int32_t value);
duckdb_state duckdb_bind_int64(duckdb_prepared_statement prepared_statement, idx_t param_idx, int64_t value);
duckdb_state duckdb_bind_double(duckdb_prepared_statement prepared_statement, idx_t param_idx, double value);
duckdb_state duckdb_bind_varchar(duckdb_prepared_statement prepared_statement, idx_t param_idx, const char *value);
duckdb_state duckdb_bind_blob(duckdb_prepared_statement prepared_statement, idx_t param_idx, void *data, idx_t length);
duckdb_state duckdb_bind_timestamp(duckdb_prepared_statement prepared_statement, idx_t param_idx, duckdb_timestamp value);
void duckdb_destroy_prepare(duckdb_prepared_statement *prepared_statement);
void duckdb_destroy_result(duckdb_result *result);
duckdb_state duckdb_query(duckdb_connection connection, const char *query, duckdb_result *out_result);
const char *duckdb_column_name(duckdb_result *result, idx_t col);
idx_t duckdb_column_count(duckdb_result *result);
idx_t duckdb_row_count(duckdb_result *result);
duckdb_type duckdb_column_type(duckdb_result *result, idx_t col);
bool duckdb_value_is_null(duckdb_result *result, idx_t col, idx_t row);
bool duckdb_value_boolean(duckdb_result *result, idx_t col, idx_t row);
int32_t duckdb_value_int32(duckdb_result *result, idx_t col, idx_t row);
int64_t duckdb_value_int64(duckdb_result *result, idx_t col, idx_t row);
double duckdb_value_double(duckdb_result *result, idx_t col, idx_t row);
char *duckdb_value_string(duckdb_result *result, idx_t col, idx_t row);
void *duckdb_value_blob(duckdb_result *result, idx_t col, idx_t row, idx_t *out_size);
duckdb_timestamp duckdb_value_timestamp(duckdb_result *result, idx_t col, idx_t row);
const char *duckdb_connection_error(duckdb_connection connection);

// Appender functions
duckdb_state duckdb_appender_create(duckdb_connection connection, const char *schema, const char *table, duckdb_appender *out_appender);
duckdb_state duckdb_appender_flush(duckdb_appender appender);
duckdb_state duckdb_appender_close(duckdb_appender appender);
duckdb_state duckdb_appender_end_row(duckdb_appender appender);
duckdb_state duckdb_append_null(duckdb_appender appender);
duckdb_state duckdb_append_bool(duckdb_appender appender, bool value);
duckdb_state duckdb_append_int32(duckdb_appender appender, int32_t value);
duckdb_state duckdb_append_int64(duckdb_appender appender, int64_t value);
duckdb_state duckdb_append_double(duckdb_appender appender, double value);
duckdb_state duckdb_append_varchar(duckdb_appender appender, const char *value);
duckdb_state duckdb_append_blob(duckdb_appender appender, void *data, idx_t length);

#ifdef __cplusplus
}
#endif

#endif // DUCKDB_H