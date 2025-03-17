package duckdb

import "sync"

// ParallelExtractor aggregation methods - operate on multiple columns concurrently

// SumInt32Columns calculates the sum of multiple int32 columns in parallel
func (pe *ParallelExtractor) SumInt32Columns(colIndices []int) (map[int]int64, error) {
	if len(colIndices) == 0 {
		return nil, nil
	}

	// Results map to store sums
	results := make(map[int]int64)
	var mu sync.Mutex

	// Use the parallel processing framework we already have
	err := pe.ProcessInt32Columns(colIndices, func(colIdx int, values []int32, nulls []bool) error {
		// Calculate sum directly
		var sum int64
		for i, val := range values {
			if !nulls[i] {
				sum += int64(val)
			}
		}

		// Store the result in a thread-safe manner
		mu.Lock()
		results[colIdx] = sum
		mu.Unlock()

		return nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}

// SumInt64Columns calculates the sum of multiple int64 columns in parallel
func (pe *ParallelExtractor) SumInt64Columns(colIndices []int) (map[int]int64, error) {
	if len(colIndices) == 0 {
		return nil, nil
	}

	// Results map to store sums
	results := make(map[int]int64)
	var mu sync.Mutex

	// Use the parallel processing framework we already have
	err := pe.ProcessInt64Columns(colIndices, func(colIdx int, values []int64, nulls []bool) error {
		// Calculate sum directly
		var sum int64
		for i, val := range values {
			if !nulls[i] {
				sum += val
			}
		}

		// Store the result in a thread-safe manner
		mu.Lock()
		results[colIdx] = sum
		mu.Unlock()

		return nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}

// SumFloat64Columns calculates the sum of multiple float64 columns in parallel
func (pe *ParallelExtractor) SumFloat64Columns(colIndices []int) (map[int]float64, error) {
	if len(colIndices) == 0 {
		return nil, nil
	}

	// Results map to store sums
	results := make(map[int]float64)
	var mu sync.Mutex

	// Use the parallel processing framework we already have
	err := pe.ProcessFloat64Columns(colIndices, func(colIdx int, values []float64, nulls []bool) error {
		// Calculate sum directly
		var sum float64
		for i, val := range values {
			if !nulls[i] {
				sum += val
			}
		}

		// Store the result in a thread-safe manner
		mu.Lock()
		results[colIdx] = sum
		mu.Unlock()

		return nil
	})

	if err != nil {
		return nil, err
	}

	return results, nil
}
