# DuckDB 1.2.1 Compatibility TODO

This document tracks the tasks needed to make the go-duckdb driver fully compatible with DuckDB 1.2.1.

## Current Limitations

1. **Prepared Statement Parameter Binding**: There seems to be an issue with parameter binding in prepared statements in DuckDB 1.2.1. This requires further investigation and potentially a workaround until fixed in a future DuckDB release.

2. **BLOB Handling**: BLOB handling is simplified in the current implementation. We need to enhance this to fully leverage DuckDB 1.2.1's BLOB capabilities.

3. **Named Parameters**: Named parameters are not yet supported in prepared statements.

## Tasks

- [ ] Investigate and fix parameter binding issues with prepared statements
  - [ ] Add tests to verify parameter binding is working correctly
  - [ ] Consider implementing a workaround by rewriting queries and direct execution

- [ ] Enhance BLOB handling
  - [ ] Implement zero-copy BLOB transfers where possible
  - [ ] Add proper length tracking for BLOB data

- [ ] Add support for named parameters
  - [ ] Implement parameter name resolution
  - [ ] Add support for driver.NamedValue parsing

- [ ] Update API for 1.2.1-specific extensions
  - [ ] Investigate and implement any new API methods in DuckDB 1.2.1
  - [ ] Add tests for the new features

- [ ] Validate compatibility against all DuckDB 1.2.1 features
  - [ ] Create comprehensive compatibility test suite
  - [ ] Test all data types and edge cases
  - [ ] Verify handling of errors and edge cases

## Future Enhancements

1. **Performance Benchmarking**: Add benchmarks for comparison against other drivers and DuckDB versions.

2. **Memory Management**: Optimize memory usage and allocation patterns.

3. **Connection Pooling**: Implement connection pooling optimizations for better concurrent performance.

## Resources

- [DuckDB 1.2.1 Release Notes](https://github.com/duckdb/duckdb/releases/tag/v1.2.1)
- [DuckDB C API Documentation](https://duckdb.org/docs/api/c/overview.html)