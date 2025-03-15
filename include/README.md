# DuckDB Headers

This directory contains header files for DuckDB v1.2.0 required for compiling the Go driver.

## Files

- `duckdb.h`: The main DuckDB C API header file

## Updating Headers

To update the headers for a new version of DuckDB:

1. Clone the DuckDB repository:
   ```
   git clone https://github.com/duckdb/duckdb.git
   cd duckdb
   ```

2. Checkout the desired version:
   ```
   git checkout v1.2.0
   ```

3. Copy the header files:
   ```
   cp src/include/duckdb.h /path/to/go-duckdb/include/
   ```

## Version Compatibility

The current headers are compatible with DuckDB v1.2.0. If you update to a newer version, make sure to update the static libraries as well to match the version.