# DuckDB 1.2.1 Update TODO

This file tracks the changes needed to fully support DuckDB v1.2.1.

## Known Issues

1. **Type compatibility with DuckDB C API**
   - The C API changed several types in DuckDB 1.2.1
   - Current pointer type casts are no longer compatible
   - Need to update all C type references to match new API

2. **Missing `duckdb_connection_error` function**
   - This function was removed in DuckDB 1.2.1
   - Need to implement a replacement using SQL query

3. **Appender API changes**
   - The Appender API structure changed in DuckDB 1.2.1
   - Need to update the Appender implementation

## Tasks

- [x] Build DuckDB v1.2.1 for darwin/arm64
- [x] Update header file and static library
- [x] Add IsAtLeast121 version check function
- [x] Create basic version check example
- [ ] Fix all C type references in connection.go
- [ ] Fix all C type references in statement.go
- [ ] Fix all C type references in rows.go
- [ ] Update lastError function to use SQL query instead of duckdb_connection_error
- [ ] Implement new Appender API for DuckDB 1.2.1
- [ ] Update all tests to work with DuckDB 1.2.1
- [ ] Update documentation with DuckDB 1.2.1 compatibility info

## Notes

1. Current progress:
   - DuckDB v1.2.1 library and header files have been updated
   - IsAtLeast121 version check function has been added
   - Initial compatibility with the core API needs type fixes

2. Strategy:
   - Fix core connection and statement functionality first
   - Update appender API second
   - Ensure all tests pass with DuckDB 1.2.1
   - Document API changes and compatibility

## References

- [DuckDB v1.2.1 Release](https://github.com/duckdb/duckdb/releases/tag/v1.2.1)
- [DuckDB C API Documentation](https://duckdb.org/docs/api/c/overview.html)
