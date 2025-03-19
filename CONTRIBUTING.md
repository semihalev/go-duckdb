# Contributing to Go-DuckDB

Thank you for your interest in contributing to Go-DuckDB! This document provides guidelines and instructions for contributing.

## Development Environment Setup

1. **Fork the repository** and clone it locally
2. **Install Go 1.24+** if you haven't already
3. **Set up your environment**:
   ```bash
   git clone https://github.com/yourusername/go-duckdb.git
   cd go-duckdb
   make test # Verify your environment is working
   ```

## Development Process

1. **Create a new branch** for your changes
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following the code style guidelines below

3. **Run tests** to ensure everything works correctly
   ```bash
   make test
   ```

4. **Run benchmarks** to check for performance regressions
   ```bash
   make bench
   ```

5. **Submit a pull request** with a clear description of your changes

## Code Style Guidelines

- **Follow Go best practices**:
  - Use `gofmt` to format your code
  - Follow the [Effective Go](https://golang.org/doc/effective_go) guidelines
  - Aim for high test coverage

- **Optimization focus**:
  - Minimize memory allocations where possible
  - Be mindful of CGO boundary crossings
  - Use buffer pooling for frequently allocated objects
  - Add benchmarks for performance-critical code

- **Documentation**:
  - Document all exported functions and types
  - Include examples where appropriate
  - Explain complex algorithms or optimizations

## Performance Considerations

- Minimize CGO crossings to improve performance
- Use batch operations where possible
- Implement column-wise extraction instead of value-by-value extraction
- Use object pooling for frequently allocated objects
- Prefer preallocated buffers over dynamic allocations in hot paths

## Testing

- Write unit tests for all new functionality
- Include both positive and negative test cases
- Test edge cases such as NULL values and extreme inputs
- Add benchmarks for performance-critical code

## Pull Request Process

1. Update documentation as necessary
2. Add or update tests as appropriate
3. Ensure all tests and benchmarks pass
4. Make sure your code follows the style guidelines
5. Keep pull requests focused on a single change
6. Respond to feedback and be willing to make changes

## Reporting Bugs

When reporting bugs, please include:

- A clear description of the issue
- Steps to reproduce
- Expected vs actual behavior
- Go version and operating system information
- Sample code that demonstrates the issue, if possible

## License

By contributing to Go-DuckDB, you agree that your contributions will be licensed under the project's MIT License.

Thank you for your contributions to making Go-DuckDB better!