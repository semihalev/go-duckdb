package duckdb

import (
	"fmt"
	"reflect"
	"time"
)

// ColumnarResult provides a columnar data structure for analytics and data science workloads.
// Instead of row-by-row access, it extracts all data at once in a column-oriented format,
// which is much more efficient for analytical processing, especially with vector operations.
type ColumnarResult struct {
	// Basic metadata
	RowCount    int
	ColumnCount int
	ColumnNames []string
	ColumnTypes []string

	// The actual column data in native Go types
	// Each element is a slice of a specific type ([]int32, []float64, []string, etc.)
	Columns []interface{}

	// Boolean masks indicating which values are NULL in each column
	NullMasks [][]bool
}

// GetInt32Column returns a column as []int32 and its null mask.
// Returns an error if the column index is invalid or the column is not of INT32 type.
func (cr *ColumnarResult) GetInt32Column(colIdx int) ([]int32, []bool, error) {
	if err := cr.validateColumnIndex(colIdx); err != nil {
		return nil, nil, err
	}

	if cr.ColumnTypes[colIdx] != "INTEGER" {
		return nil, nil, fmt.Errorf("column %d is not an INTEGER", colIdx)
	}

	values, ok := cr.Columns[colIdx].([]int32)
	if !ok {
		return nil, nil, fmt.Errorf("internal type error: column %d is not []int32", colIdx)
	}

	return values, cr.NullMasks[colIdx], nil
}

// GetInt64Column returns a column as []int64 and its null mask.
// Returns an error if the column index is invalid or the column is not of INT64 type.
func (cr *ColumnarResult) GetInt64Column(colIdx int) ([]int64, []bool, error) {
	if err := cr.validateColumnIndex(colIdx); err != nil {
		return nil, nil, err
	}

	if cr.ColumnTypes[colIdx] != "BIGINT" {
		return nil, nil, fmt.Errorf("column %d is not a BIGINT", colIdx)
	}

	values, ok := cr.Columns[colIdx].([]int64)
	if !ok {
		return nil, nil, fmt.Errorf("internal type error: column %d is not []int64", colIdx)
	}

	return values, cr.NullMasks[colIdx], nil
}

// GetFloat64Column returns a column as []float64 and its null mask.
// Returns an error if the column index is invalid or the column is not of DOUBLE type.
func (cr *ColumnarResult) GetFloat64Column(colIdx int) ([]float64, []bool, error) {
	if err := cr.validateColumnIndex(colIdx); err != nil {
		return nil, nil, err
	}

	if cr.ColumnTypes[colIdx] != "DOUBLE" {
		return nil, nil, fmt.Errorf("column %d is not a DOUBLE", colIdx)
	}

	values, ok := cr.Columns[colIdx].([]float64)
	if !ok {
		return nil, nil, fmt.Errorf("internal type error: column %d is not []float64", colIdx)
	}

	return values, cr.NullMasks[colIdx], nil
}

// GetTimestampColumn returns a column as []time.Time and its null mask.
// Returns an error if the column index is invalid or the column is not of TIMESTAMP type.
func (cr *ColumnarResult) GetTimestampColumn(colIdx int) ([]time.Time, []bool, error) {
	if err := cr.validateColumnIndex(colIdx); err != nil {
		return nil, nil, err
	}

	if cr.ColumnTypes[colIdx] != "TIMESTAMP" {
		return nil, nil, fmt.Errorf("column %d is not a TIMESTAMP", colIdx)
	}

	values, ok := cr.Columns[colIdx].([]time.Time)
	if !ok {
		return nil, nil, fmt.Errorf("internal type error: column %d is not []time.Time", colIdx)
	}

	return values, cr.NullMasks[colIdx], nil
}

// GetDateColumn returns a column as []time.Time and its null mask.
// Returns an error if the column index is invalid or the column is not of DATE type.
func (cr *ColumnarResult) GetDateColumn(colIdx int) ([]time.Time, []bool, error) {
	if err := cr.validateColumnIndex(colIdx); err != nil {
		return nil, nil, err
	}

	if cr.ColumnTypes[colIdx] != "DATE" {
		return nil, nil, fmt.Errorf("column %d is not a DATE", colIdx)
	}

	values, ok := cr.Columns[colIdx].([]time.Time)
	if !ok {
		return nil, nil, fmt.Errorf("internal type error: column %d is not []time.Time", colIdx)
	}

	return values, cr.NullMasks[colIdx], nil
}

// GetStringColumn returns a column as []string and its null mask.
// Returns an error if the column index is invalid or the column is not of VARCHAR type.
func (cr *ColumnarResult) GetStringColumn(colIdx int) ([]string, []bool, error) {
	if err := cr.validateColumnIndex(colIdx); err != nil {
		return nil, nil, err
	}

	if cr.ColumnTypes[colIdx] != "VARCHAR" {
		return nil, nil, fmt.Errorf("column %d is not a VARCHAR", colIdx)
	}

	values, ok := cr.Columns[colIdx].([]string)
	if !ok {
		return nil, nil, fmt.Errorf("internal type error: column %d is not []string", colIdx)
	}

	return values, cr.NullMasks[colIdx], nil
}

// GetBoolColumn returns a column as []bool and its null mask.
// Returns an error if the column index is invalid or the column is not of BOOLEAN type.
func (cr *ColumnarResult) GetBoolColumn(colIdx int) ([]bool, []bool, error) {
	if err := cr.validateColumnIndex(colIdx); err != nil {
		return nil, nil, err
	}

	if cr.ColumnTypes[colIdx] != "BOOLEAN" {
		return nil, nil, fmt.Errorf("column %d is not a BOOLEAN", colIdx)
	}

	values, ok := cr.Columns[colIdx].([]bool)
	if !ok {
		return nil, nil, fmt.Errorf("internal type error: column %d is not []bool", colIdx)
	}

	return values, cr.NullMasks[colIdx], nil
}

// GetBlobColumn returns a column as [][]byte and its null mask.
// Returns an error if the column index is invalid or the column is not of BLOB type.
func (cr *ColumnarResult) GetBlobColumn(colIdx int) ([][]byte, []bool, error) {
	if err := cr.validateColumnIndex(colIdx); err != nil {
		return nil, nil, err
	}

	if cr.ColumnTypes[colIdx] != "BLOB" {
		return nil, nil, fmt.Errorf("column %d is not a BLOB", colIdx)
	}

	values, ok := cr.Columns[colIdx].([][]byte)
	if !ok {
		return nil, nil, fmt.Errorf("internal type error: column %d is not [][]byte", colIdx)
	}

	return values, cr.NullMasks[colIdx], nil
}

// GetColumnByName returns the column index for a given column name.
// Returns an error if the column name is not found.
func (cr *ColumnarResult) GetColumnByName(name string) (int, error) {
	for i, colName := range cr.ColumnNames {
		if colName == name {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column not found: %s", name)
}

// GetColumnType returns the Go type of a column.
// This is useful for dynamic type handling.
func (cr *ColumnarResult) GetColumnType(colIdx int) (reflect.Type, error) {
	if err := cr.validateColumnIndex(colIdx); err != nil {
		return nil, err
	}

	return reflect.TypeOf(cr.Columns[colIdx]).Elem(), nil
}

// Helper method to validate column indices
func (cr *ColumnarResult) validateColumnIndex(colIdx int) error {
	if colIdx < 0 || colIdx >= cr.ColumnCount {
		return fmt.Errorf("column index out of range: %d (valid range: 0-%d)",
			colIdx, cr.ColumnCount-1)
	}
	return nil
}
