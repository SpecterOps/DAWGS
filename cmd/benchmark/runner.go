// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/specterops/dawgs/graph"
)

// ExplainFunc captures a plan for the exact scenario values that were timed.
// Passing the Scenario prevents parameterized endpoint plans from silently
// explaining an empty query instead of the workload under measurement.
type ExplainFunc func(ctx context.Context, tx graph.Transaction, scenario Scenario) (*ExplainResult, error)

type RunOptions struct {
	Explain          ExplainFunc
	WarmupIterations int
	Workers          int
}

// Stats holds computed timing statistics for a scenario.
type Stats struct {
	Median time.Duration `json:"median"`
	P95    time.Duration `json:"p95"`
	Max    time.Duration `json:"max"`
}

// Result is one row in the report.
type Result struct {
	Section           string          `json:"section"`
	Dataset           string          `json:"dataset"`
	Label             string          `json:"label"`
	RowCount          int64           `json:"row_count"`
	DistinctRowCount  *int64          `json:"distinct_row_count,omitempty"`
	DuplicateRowCount *int64          `json:"duplicate_row_count,omitempty"`
	Explain           *ExplainResult  `json:"explain,omitempty"`
	Stats             Stats           `json:"stats"`
	Samples           []time.Duration `json:"samples,omitempty"`
}

// runScenario executes a scenario N times and returns timing stats.
func runScenario(ctx context.Context, db graph.Database, s Scenario, iterations int, options RunOptions) (Result, error) {
	if err := validateIterations(iterations); err != nil {
		return Result{}, err
	}
	if options.Workers == 0 {
		options.Workers = 1
	}
	if err := validateRunOptions(options); err != nil {
		return Result{}, err
	}

	measurement, durations, err := runScenarioSamples(iterations, options.WarmupIterations, options.Workers, func() (Measurement, error) {
		return runScenarioOnce(ctx, db, s)
	})
	if err != nil {
		return Result{}, err
	}

	result := Result{
		Section:           s.Section,
		Dataset:           s.Dataset,
		Label:             s.Label,
		RowCount:          measurement.RowCount,
		DistinctRowCount:  measurement.DistinctRowCount,
		DuplicateRowCount: measurement.DuplicateRowCount,
		Stats:             computeStats(append([]time.Duration(nil), durations...)),
		Samples:           durations,
	}

	if options.Explain != nil && s.Cypher != "" {
		if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
			explain, err := options.Explain(ctx, tx, s)
			result.Explain = explain
			return err
		}); err != nil {
			return Result{}, err
		}
	}

	return result, nil
}

// runScenarioSamples executes one logical benchmark scenario across workers.
// Every worker warms its own leased connection before it contributes timed
// samples, which makes connection-local cache behavior observable without
// mixing warm-up latency into the reported distribution.
func runScenarioSamples(iterations, warmupIterations, workers int, run func() (Measurement, error)) (Measurement, []time.Duration, error) {
	if err := validateIterations(iterations); err != nil {
		return Measurement{}, nil, err
	}
	if err := validateBenchmarkConcurrency(warmupIterations, workers); err != nil {
		return Measurement{}, nil, err
	}

	type workerResult struct {
		measurement Measurement
		durations   []time.Duration
		err         error
	}

	results := make(chan workerResult, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			var measurement Measurement
			for range warmupIterations {
				next, err := run()
				if err != nil {
					results <- workerResult{err: err}
					return
				}
				measurement = next
			}

			durations := make([]time.Duration, iterations)
			for index := range iterations {
				started := time.Now()
				next, err := run()
				if err != nil {
					results <- workerResult{err: err}
					return
				}
				if warmupIterations == 0 && index == 0 {
					measurement = next
				}
				durations[index] = time.Since(started)
			}
			results <- workerResult{measurement: measurement, durations: durations}
		}()
	}
	group.Wait()
	close(results)

	durations := make([]time.Duration, 0, iterations*workers)
	var measurement Measurement
	for result := range results {
		if result.err != nil {
			return Measurement{}, nil, result.err
		}
		measurement = result.measurement
		durations = append(durations, result.durations...)
	}

	return measurement, durations, nil
}

func validateIterations(iterations int) error {
	if iterations < 1 {
		return fmt.Errorf("iterations must be at least 1")
	}

	return nil
}

func validateRunOptions(options RunOptions) error {
	return validateBenchmarkConcurrency(options.WarmupIterations, options.Workers)
}

func validateBenchmarkConcurrency(warmupIterations, workers int) error {
	if warmupIterations < 0 {
		return fmt.Errorf("warm-up iterations must not be negative: %d", warmupIterations)
	}
	if workers < 1 {
		return fmt.Errorf("workers must be at least 1: %d", workers)
	}
	return nil
}

func runScenarioOnce(ctx context.Context, db graph.Database, s Scenario) (Measurement, error) {
	var measurement Measurement
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		nextMeasurement, err := s.Query(tx)
		if err != nil {
			return err
		}

		measurement = nextMeasurement
		return validateScenarioRows(s, nextMeasurement.RowCount)
	}); err != nil {
		return Measurement{}, err
	}

	return measurement, nil
}

func validateScenarioRows(s Scenario, actualRows int64) error {
	if s.ExpectedRows == nil || *s.ExpectedRows == actualRows {
		return nil
	}

	return fmt.Errorf("%s/%s on %s expected %d rows, got %d", s.Section, s.Label, s.Dataset, *s.ExpectedRows, actualRows)
}

func computeStats(durations []time.Duration) Stats {
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	n := len(durations)

	return Stats{
		Median: durations[n/2],
		P95:    durations[n*95/100],
		Max:    durations[n-1],
	}
}
